"""
Re-populate NH House seat_terms from Ballotpedia data.

NH's floterial district system causes OpenStates data to mismatch our DB structure.
This script uses Ballotpedia's authoritative member list to fill all 400 seats.

Strategy:
1. Delete existing NH House seat_terms + orphaned candidates
2. Parse Ballotpedia HTML (downloaded to /tmp/nh_house_bp.html)
3. First pass: assign BP members to matching DB districts (up to num_seats)
4. Second pass: assign overflow members to empty seats in same county (floterial overlap)
5. Insert candidates + seat_terms + update cache

Usage:
    python3 scripts/populate_seat_terms_nh_house.py
    python3 scripts/populate_seat_terms_nh_house.py --dry-run
"""
import sys
import re
import httpx
import html as htmlmod
from collections import defaultdict

def run_sql(query, exit_on_error=True):
    resp = httpx.post(
        f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
        json={'query': query},
        timeout=120
    )
    if resp.status_code != 201:
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None
    return resp.json()

def esc(s):
    if s is None:
        return ''
    return str(s).replace("'", "''")

PARTY_MAP = {
    'Republican': 'R',
    'Democratic': 'D',
    'Independent': 'I',
    'Libertarian': 'L',
}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    # ── Step 0: Parse Ballotpedia HTML ───────────────────────────
    print("=" * 60)
    print("STEP 0: Parse Ballotpedia data")
    print("=" * 60)

    try:
        with open('/tmp/nh_house_bp.html', 'r', encoding='utf-8') as f:
            html = f.read()
    except FileNotFoundError:
        print("  ERROR: /tmp/nh_house_bp.html not found.")
        print("  Download with: curl -s -L -H 'User-Agent: Mozilla/5.0' "
              "'https://ballotpedia.org/New_Hampshire_House_of_Representatives' "
              "-o /tmp/nh_house_bp.html")
        sys.exit(1)

    rows = re.findall(
        r'<tr>\s*<td[^>]*><a[^>]*District_([^"]+)"[^>]*>[^<]*</a></td>\s*'
        r'<td[^>]*>(?:<a[^>]*>([^<]*)</a>|([^<]*))</td>\s*'
        r'<td\s+class="partytd\s+([^"]*)"',
        html, re.DOTALL
    )

    bp_by_district = defaultdict(list)
    total_bp = 0
    vacancies = 0
    for r in rows:
        district = r[0].replace('_', '-')
        name = htmlmod.unescape((r[1] or r[2]).strip())
        party_raw = r[3].strip()
        if name == 'Vacant':
            vacancies += 1
            continue
        party = PARTY_MAP.get(party_raw, party_raw[:3] if party_raw else 'U')
        bp_by_district[district].append((name, party))
        total_bp += 1

    print(f"  Ballotpedia members: {total_bp} + {vacancies} vacancies = {total_bp + vacancies}")
    print(f"  Ballotpedia districts: {len(bp_by_district)}")

    # ── Step 1: Delete existing NH House seat_terms ──────────────
    print("\n" + "=" * 60)
    print("STEP 1: Check existing NH House data")
    print("=" * 60)

    existing = run_sql("""
        SELECT COUNT(*) as cnt FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
          AND st.end_date IS NULL
    """)
    print(f"  Existing NH House seat_terms: {existing[0]['cnt']}")

    if not args.dry_run and existing[0]['cnt'] > 0:
        print("  Deleting existing NH House seat_terms...")

        # Clear cache columns first
        run_sql("""
            UPDATE seats SET current_holder = NULL, current_holder_party = NULL,
                current_holder_caucus = NULL
            WHERE id IN (
                SELECT se.id FROM seats se
                JOIN districts d ON se.district_id = d.id
                JOIN states s ON d.state_id = s.id
                WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
            )
        """)

        # Get candidate IDs to delete
        cand_ids = run_sql("""
            SELECT DISTINCT st.candidate_id FROM seat_terms st
            JOIN seats se ON st.seat_id = se.id
            JOIN districts d ON se.district_id = d.id
            JOIN states s ON d.state_id = s.id
            WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
        """)

        # Delete seat_terms
        run_sql("""
            DELETE FROM seat_terms WHERE seat_id IN (
                SELECT se.id FROM seats se
                JOIN districts d ON se.district_id = d.id
                JOIN states s ON d.state_id = s.id
                WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
            )
        """)

        # Delete orphaned candidates (only if they have no other seat_terms)
        if cand_ids:
            id_list = ','.join(str(r['candidate_id']) for r in cand_ids)
            run_sql(f"""
                DELETE FROM candidates WHERE id IN ({id_list})
                AND id NOT IN (SELECT candidate_id FROM seat_terms)
            """)

        verify = run_sql("""
            SELECT COUNT(*) as cnt FROM seat_terms st
            JOIN seats se ON st.seat_id = se.id
            JOIN districts d ON se.district_id = d.id
            JOIN states s ON d.state_id = s.id
            WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
        """)
        print(f"  After deletion: {verify[0]['cnt']} NH House seat_terms")

    # ── Step 2: Load DB seats ────────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 2: Load NH House seats from DB")
    print("=" * 60)

    db_seats = run_sql("""
        SELECT se.id as seat_id, se.seat_designator,
               d.district_number, d.num_seats, d.is_floterial
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
        ORDER BY d.district_number, se.seat_designator
    """)

    db_by_district = defaultdict(list)
    for s in db_seats:
        db_by_district[s['district_number']].append(s)

    print(f"  DB districts: {len(db_by_district)}")
    print(f"  DB seats: {len(db_seats)}")

    # ── Step 3: Match members to seats ───────────────────────────
    print("\n" + "=" * 60)
    print("STEP 3: Match Ballotpedia members to seats")
    print("=" * 60)

    assigned = []   # (seat_id, name, party)
    overflow = []   # (district, name, party)

    # First pass: direct district match
    for district in sorted(bp_by_district.keys()):
        members = bp_by_district[district]
        seats = db_by_district.get(district, [])

        members_sorted = sorted(members, key=lambda x: x[0].split()[-1].lower())
        seats_sorted = sorted(seats, key=lambda x: x['seat_designator'] or '')

        for i, (name, party) in enumerate(members_sorted):
            if i < len(seats_sorted):
                assigned.append((seats_sorted[i]['seat_id'], name, party))
            else:
                overflow.append((district, name, party))

    print(f"  First pass: {len(assigned)} assigned, {len(overflow)} overflow")

    # Second pass: county-based overflow assignment
    assigned_seat_ids = {a[0] for a in assigned}
    empty_by_county = defaultdict(list)
    for s in db_seats:
        if s['seat_id'] not in assigned_seat_ids:
            county = s['district_number'].rsplit('-', 1)[0]
            empty_by_county[county].append(s)

    second_pass = []
    still_overflow = []

    for district, name, party in overflow:
        county = district.rsplit('-', 1)[0]
        if empty_by_county[county]:
            seat = empty_by_county[county].pop(0)
            assigned.append((seat['seat_id'], name, party))
            second_pass.append((seat['seat_id'], name, party, district, seat['district_number']))
        else:
            still_overflow.append((district, name, party))

    print(f"  Second pass (county overflow): {len(second_pass)} assigned")
    if still_overflow:
        print(f"  Still unmatched: {len(still_overflow)}")
        for d, n, p in still_overflow:
            print(f"    {d}: {n} ({p})")

    total_assigned = len(assigned)
    print(f"\n  Total assigned: {total_assigned} / 400")
    print(f"  Vacancies: {400 - total_assigned}")

    # Verify no duplicate seat assignments
    seat_ids = [a[0] for a in assigned]
    if len(seat_ids) != len(set(seat_ids)):
        print("  ERROR: Duplicate seat assignments!")
        from collections import Counter
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL
        dupes = {k: v for k, v in Counter(seat_ids).items() if v > 1}
        print(f"  Duplicates: {dupes}")
        sys.exit(1)
    print("  No duplicate assignments!")

    if args.dry_run:
        print("\n  DRY RUN — no database changes.")
        print(f"\n  Second pass details:")
        for sid, name, party, from_d, to_d in second_pass[:15]:
            print(f"    {name} ({party}): {from_d} → {to_d}")
        if len(second_pass) > 15:
            print(f"    ... and {len(second_pass) - 15} more")
        return

    # ── Step 4: Insert candidates ────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 4: Insert candidates")
    print("=" * 60)

    values = []
    for seat_id, name, party in assigned:
        values.append(f"('{esc(name)}', NULL, NULL, NULL)")

    sql = (
        "INSERT INTO candidates (full_name, first_name, last_name, gender) VALUES\n"
        + ",\n".join(values)
        + "\nRETURNING id;"
    )
    result = run_sql(sql)
    cand_ids = [r['id'] for r in result]
    print(f"  Inserted {len(cand_ids)} candidates")

    # ── Step 5: Insert seat_terms ────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 5: Insert seat_terms")
    print("=" * 60)

    values = []
    for i, (seat_id, name, party) in enumerate(assigned):
        values.append(
            f"({seat_id}, {cand_ids[i]}, '{esc(party)}', '2025-01-01', NULL, "
            f"'elected', '{esc(party)}', NULL)"
        )

    sql = (
        "INSERT INTO seat_terms (seat_id, candidate_id, party, start_date, end_date, "
        "start_reason, caucus, election_id) VALUES\n"
        + ",\n".join(values)
        + "\nRETURNING id;"
    )
    result = run_sql(sql)
    print(f"  Inserted {len(result)} seat_terms")

    # ── Step 6: Update seats cache ───────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 6: Update seats cache")
    print("=" * 60)

    run_sql("""
        UPDATE seats
        SET current_holder = c.full_name,
            current_holder_party = st.party,
            current_holder_caucus = st.caucus
        FROM seat_terms st
        JOIN candidates c ON st.candidate_id = c.id
        WHERE seats.id = st.seat_id
          AND st.end_date IS NULL
          AND seats.id IN (
              SELECT se.id FROM seats se
              JOIN districts d ON se.district_id = d.id
              JOIN states s ON d.state_id = s.id
              WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
          );
    """)

    filled = run_sql("""
        SELECT COUNT(*) as cnt FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
          AND se.current_holder IS NOT NULL
    """)
    print(f"  NH House seats with holder: {filled[0]['cnt']}")

    # ── Step 7: Verification ─────────────────────────────────────
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    counts = run_sql("""
        SELECT
            (SELECT COUNT(*) FROM candidates) as total_cand,
            (SELECT COUNT(*) FROM seat_terms) as total_st,
            (SELECT COUNT(*) FROM seat_terms st
             JOIN seats se ON st.seat_id = se.id
             JOIN districts d ON se.district_id = d.id
             JOIN states s ON d.state_id = s.id
             WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
               AND st.end_date IS NULL) as nh_house_st,
            (SELECT COUNT(*) FROM seats WHERE office_level = 'Legislative'
             AND current_holder IS NOT NULL) as leg_filled
    """)
    c = counts[0]
    print(f"  Total candidates: {c['total_cand']}")
    print(f"  Total seat_terms: {c['total_st']}")
    print(f"  NH House seat_terms: {c['nh_house_st']}")
    print(f"  Legislative seats filled: {c['leg_filled']} / 7386")

    # Party
    print("\n  NH House party distribution:")
    party = run_sql("""
        SELECT st.party, COUNT(*) as cnt
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
          AND st.end_date IS NULL
        GROUP BY st.party ORDER BY cnt DESC
    """)
    for r in party:
        print(f"    {r['party']}: {r['cnt']}")

    # Duplicate check
    dupes = run_sql("""
        SELECT seat_id, COUNT(*) as cnt FROM seat_terms
        WHERE end_date IS NULL AND seat_id IN (
            SELECT se.id FROM seats se
            JOIN districts d ON se.district_id = d.id
            JOIN states s ON d.state_id = s.id
            WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
        )
        GROUP BY seat_id HAVING COUNT(*) > 1
    """)
    if dupes:
        print(f"\n  WARNING: {len(dupes)} duplicate seat_terms!")
    else:
        print("\n  No duplicate seat_terms!")

    # Spot checks
    print("\n  Spot checks:")
    spots = run_sql("""
        SELECT se.seat_label, c.full_name, st.party
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN candidates c ON st.candidate_id = c.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
          AND st.end_date IS NULL
        ORDER BY RANDOM() LIMIT 10
    """)
    for r in spots:
        print(f"    {r['seat_label']}: {r['full_name']} ({r['party']})")

    print("\nDone!")

if __name__ == '__main__':
    main()
