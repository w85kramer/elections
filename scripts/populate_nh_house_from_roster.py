#!/usr/bin/env python3
"""
Populate NH House seat_terms from Ballotpedia text roster.

Parses a plain-text roster (copy-pasted from BP member table) and matches
members to DB seats, handling floterial district overflow by county.

Usage:
    python3 scripts/populate_nh_house_from_roster.py --dry-run
    python3 scripts/populate_nh_house_from_roster.py
"""
import sys
import os
import re
import time
import argparse
from collections import defaultdict

import httpx
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF

PARTY_MAP = {
    'Republican': 'R',
    'Democratic': 'D',
    'Independent': 'I',
    'Libertarian': 'L',
}

def run_sql(query, max_retries=5):
    for attempt in range(max_retries):
        try:
            resp = httpx.post(
                f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
                headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
                json={'query': query},
                timeout=120,
            )
            if resp.status_code == 201:
                return resp.json()
            if resp.status_code == 429:
                wait = 5 * (attempt + 1)
                print(f'  Rate limited, waiting {wait}s...')
                time.sleep(wait)
                continue
            print(f'SQL ERROR ({resp.status_code}): {resp.text[:500]}')
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
                continue
            return None
        except (httpx.ConnectError, httpx.ReadError, httpx.WriteError,
                httpx.ReadTimeout, httpx.WriteTimeout) as e:
            wait = 5 * (attempt + 1)
            print(f'  Connection error: {e}, retrying in {wait}s...')
            time.sleep(wait)
            continue
    return None


def esc(s):
    if s is None:
        return ''
    return str(s).replace("'", "''")


def parse_roster(filepath):
    """Parse BP text roster into {district: [(name, party)]}."""
    with open(filepath, 'r') as f:
        text = f.read()

    prefix = 'New Hampshire House of Representatives '
    entries = text.split(prefix)

    bp_by_district = defaultdict(list)
    vacancies = 0
    total = 0

    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue

        # Extract county and number
        m = re.match(r'(\w+)\s+(\d+)(.*)', entry)
        if not m:
            print(f'  WARN: Could not parse entry: {entry[:60]}')
            continue

        county = m.group(1)
        number = m.group(2)
        rest = m.group(3)
        district = f'{county}-{number}'

        # Check for vacancy
        if rest.strip() == 'Vacant' or rest.strip().startswith('Vacant'):
            vacancies += 1
            continue

        # Extract name and party
        party_found = None
        name = None
        for party_str in ['Republican', 'Democratic', 'Independent', 'Libertarian']:
            if party_str in rest:
                idx = rest.index(party_str)
                name = rest[:idx].strip()
                party_found = PARTY_MAP[party_str]
                break

        if name and party_found:
            bp_by_district[district].append((name, party_found))
            total += 1
        else:
            print(f'  WARN: Could not parse name/party: {district} -> {rest[:60]}')

    return bp_by_district, total, vacancies


def main():
    parser = argparse.ArgumentParser(description='Populate NH House seat_terms from BP text roster')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--roster', default='/tmp/nh_house_roster.txt',
                        help='Path to roster text file')
    args = parser.parse_args()

    # ── Step 0: Parse roster ───────────────────────────────────
    print('=' * 60)
    print('STEP 0: Parse Ballotpedia roster')
    print('=' * 60)

    bp_by_district, total_bp, vacancies = parse_roster(args.roster)
    print(f'  Members: {total_bp}')
    print(f'  Vacancies: {vacancies}')
    print(f'  Districts in roster: {len(bp_by_district)}')
    print(f'  Expected total: {total_bp + vacancies} (should be ~400)')

    # ── Step 1: Load DB seats ──────────────────────────────────
    print('\n' + '=' * 60)
    print('STEP 1: Load NH House seats from DB')
    print('=' * 60)

    db_seats = run_sql("""
        SELECT se.id as seat_id, se.seat_designator,
               d.district_number, d.num_seats, d.is_floterial
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
          AND d.redistricting_cycle = '2022'
        ORDER BY d.district_number, se.seat_designator
    """)

    db_by_district = defaultdict(list)
    for s in db_seats:
        db_by_district[s['district_number']].append(s)

    print(f'  DB districts: {len(db_by_district)}')
    print(f'  DB seats: {len(db_seats)}')

    # ── Step 2: Match members to seats ─────────────────────────
    print('\n' + '=' * 60)
    print('STEP 2: Match members to seats')
    print('=' * 60)

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

    print(f'  First pass: {len(assigned)} assigned, {len(overflow)} overflow')

    # Second pass: county-based overflow assignment (floterial logic)
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

    print(f'  Second pass (county overflow): {len(second_pass)} assigned')
    if still_overflow:
        print(f'  Still unmatched: {len(still_overflow)}')
        for d, n, p in still_overflow:
            print(f'    {d}: {n} ({p})')

    total_assigned = len(assigned)
    empty_seats = len(db_seats) - total_assigned
    print(f'\n  Total assigned: {total_assigned} / {len(db_seats)}')
    print(f'  Empty seats (vacancies): {empty_seats}')

    # Party breakdown
    dem = sum(1 for _, _, p in assigned if p == 'D')
    rep = sum(1 for _, _, p in assigned if p == 'R')
    other = sum(1 for _, _, p in assigned if p not in ('D', 'R'))
    print(f'  Party: {dem}D, {rep}R, {other} other')

    # Verify no duplicate seat assignments
    seat_ids = [a[0] for a in assigned]
    if len(seat_ids) != len(set(seat_ids)):
        print('  ERROR: Duplicate seat assignments!')
        from collections import Counter
        dupes = {k: v for k, v in Counter(seat_ids).items() if v > 1}
        print(f'  Duplicates: {dupes}')
        sys.exit(1)
    print('  No duplicate assignments!')

    if args.dry_run:
        print('\n  DRY RUN — no database changes.')
        print(f'\n  Second pass details:')
        for sid, name, party, from_d, to_d in second_pass[:20]:
            print(f'    {name} ({party}): {from_d} -> {to_d}')
        if len(second_pass) > 20:
            print(f'    ... and {len(second_pass) - 20} more')
        return

    # ── Step 3: Clear existing NH House seat_terms ─────────────
    print('\n' + '=' * 60)
    print('STEP 3: Clear existing NH House seat_terms')
    print('=' * 60)

    # Clear cache columns
    run_sql("""
        UPDATE seats SET current_holder = NULL, current_holder_party = NULL,
            current_holder_caucus = NULL
        WHERE id IN (
            SELECT se.id FROM seats se
            JOIN districts d ON se.district_id = d.id
            JOIN states s ON d.state_id = s.id
            WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
              AND d.redistricting_cycle = '2022'
        )
    """)

    # Get candidate IDs to potentially clean up
    cand_ids = run_sql("""
        SELECT DISTINCT st.candidate_id FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
          AND d.redistricting_cycle = '2022'
    """)

    # Delete seat_terms
    run_sql("""
        DELETE FROM seat_terms WHERE seat_id IN (
            SELECT se.id FROM seats se
            JOIN districts d ON se.district_id = d.id
            JOIN states s ON d.state_id = s.id
            WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
              AND d.redistricting_cycle = '2022'
        )
    """)

    # Delete orphaned candidates
    if cand_ids:
        id_list = ','.join(str(r['candidate_id']) for r in cand_ids)
        run_sql(f"""
            DELETE FROM candidates WHERE id IN ({id_list})
            AND id NOT IN (SELECT candidate_id FROM seat_terms)
            AND id NOT IN (SELECT candidate_id FROM candidacies)
        """)

    print('  Cleared existing data')

    # ── Step 4: Insert candidates ──────────────────────────────
    print('\n' + '=' * 60)
    print('STEP 4: Insert candidates')
    print('=' * 60)

    # Batch insert candidates
    batch_size = 100
    all_cand_ids = []
    for i in range(0, len(assigned), batch_size):
        batch = assigned[i:i+batch_size]
        values = []
        for seat_id, name, party in batch:
            values.append(f"('{esc(name)}', NULL, NULL, NULL)")
        sql = (
            "INSERT INTO candidates (full_name, first_name, last_name, gender) VALUES\n"
            + ",\n".join(values)
            + "\nRETURNING id;"
        )
        result = run_sql(sql)
        if result:
            all_cand_ids.extend([r['id'] for r in result])
        else:
            print(f'  ERROR: Candidate insert failed at batch {i}')
            return
        if i + batch_size < len(assigned):
            time.sleep(1)

    print(f'  Inserted {len(all_cand_ids)} candidates')

    # ── Step 5: Insert seat_terms ──────────────────────────────
    print('\n' + '=' * 60)
    print('STEP 5: Insert seat_terms')
    print('=' * 60)

    all_st_ids = []
    for i in range(0, len(assigned), batch_size):
        batch_assigned = assigned[i:i+batch_size]
        batch_cand_ids = all_cand_ids[i:i+batch_size]
        values = []
        for j, (seat_id, name, party) in enumerate(batch_assigned):
            values.append(
                f"({seat_id}, {batch_cand_ids[j]}, '{esc(party)}', '2025-01-01', NULL, "
                f"'elected', '{esc(party)}', NULL)"
            )
        sql = (
            "INSERT INTO seat_terms (seat_id, candidate_id, party, start_date, end_date, "
            "start_reason, caucus, election_id) VALUES\n"
            + ",\n".join(values)
            + "\nRETURNING id;"
        )
        result = run_sql(sql)
        if result:
            all_st_ids.extend([r['id'] for r in result])
        else:
            print(f'  ERROR: seat_terms insert failed at batch {i}')
            return
        if i + batch_size < len(assigned):
            time.sleep(1)

    print(f'  Inserted {len(all_st_ids)} seat_terms')

    # ── Step 6: Update seats cache ─────────────────────────────
    print('\n' + '=' * 60)
    print('STEP 6: Update seats cache')
    print('=' * 60)

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
                AND d.redistricting_cycle = '2022'
          );
    """)
    print('  Cache updated')

    # ── Step 7: Verification ───────────────────────────────────
    print('\n' + '=' * 60)
    print('VERIFICATION')
    print('=' * 60)

    counts = run_sql("""
        SELECT
            (SELECT COUNT(*) FROM seats se
             JOIN districts d ON se.district_id = d.id
             JOIN states s ON d.state_id = s.id
             WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
               AND d.redistricting_cycle = '2022'
               AND se.current_holder IS NOT NULL) as filled,
            (SELECT COUNT(*) FROM seats se
             JOIN districts d ON se.district_id = d.id
             JOIN states s ON d.state_id = s.id
             WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
               AND d.redistricting_cycle = '2022'
               AND se.current_holder IS NULL) as vacant
    """)
    c = counts[0]
    print(f'  NH House seats filled: {c["filled"]}')
    print(f'  NH House seats vacant: {c["vacant"]}')

    # Party breakdown
    party = run_sql("""
        SELECT se.current_holder_caucus as caucus, COUNT(*) as cnt
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
          AND d.redistricting_cycle = '2022' AND se.current_holder IS NOT NULL
        GROUP BY se.current_holder_caucus ORDER BY cnt DESC
    """)
    print('  Party breakdown:')
    for r in party:
        print(f"    {r['caucus']}: {r['cnt']}")

    # Spot checks
    spots = run_sql("""
        SELECT se.seat_label, se.current_holder, se.current_holder_caucus
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NH' AND se.office_type = 'State House'
          AND d.redistricting_cycle = '2022' AND se.current_holder IS NOT NULL
        ORDER BY RANDOM() LIMIT 10
    """)
    print('\n  Spot checks:')
    for r in spots:
        print(f"    {r['seat_label']}: {r['current_holder']} ({r['current_holder_caucus']})")

    print('\nDone!')


if __name__ == '__main__':
    main()
