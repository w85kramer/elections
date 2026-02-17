"""
Populate seat_terms for Massachusetts state legislators.

MA requires special handling because OpenStates uses named county-based districts
(e.g., "7th Hampden", "3rd Suffolk") while our DB uses sequential numbers (1-160
for House, 1-40 for Senate). This script builds the name→number mapping and
inserts candidates + seat_terms.

Data sources:
- OpenStates bulk CSV: https://data.openstates.org/people/current/ma.csv
- Ballotpedia for canonical district list (hardcoded below)

Usage:
    python3 scripts/populate_seat_terms_ma.py
    python3 scripts/populate_seat_terms_ma.py --dry-run
"""
import sys
import csv
import io
import httpx

OPENSTATES_URL = 'https://data.openstates.org/people/current/ma.csv'

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

# ══════════════════════════════════════════════════════════════════
# CANONICAL DISTRICT LISTS (from Ballotpedia, all 160 House + 40 Senate)
# Sorted alphabetically to assign to DB district numbers 1-N.
# ══════════════════════════════════════════════════════════════════

# All 160 MA House district names (Ballotpedia format)
MA_HOUSE_DISTRICTS_BP = [
    "1st Barnstable", "1st Berkshire", "1st Bristol", "1st Essex", "1st Franklin",
    "1st Hampden", "1st Hampshire", "1st Middlesex", "1st Norfolk", "1st Plymouth",
    "1st Suffolk", "1st Worcester",
    "2nd Barnstable", "2nd Berkshire", "2nd Bristol", "2nd Essex", "2nd Franklin",
    "2nd Hampden", "2nd Hampshire", "2nd Middlesex", "2nd Norfolk", "2nd Plymouth",
    "2nd Suffolk", "2nd Worcester",
    "3rd Barnstable", "3rd Berkshire", "3rd Bristol", "3rd Essex",
    "3rd Hampden", "3rd Hampshire", "3rd Middlesex", "3rd Norfolk", "3rd Plymouth",
    "3rd Suffolk", "3rd Worcester",
    "4th Barnstable", "4th Bristol", "4th Essex", "4th Hampden",
    "4th Middlesex", "4th Norfolk", "4th Plymouth", "4th Suffolk", "4th Worcester",
    "5th Barnstable", "5th Bristol", "5th Essex", "5th Hampden",
    "5th Middlesex", "5th Norfolk", "5th Plymouth", "5th Suffolk", "5th Worcester",
    "6th Bristol", "6th Essex", "6th Hampden", "6th Middlesex",
    "6th Norfolk", "6th Plymouth", "6th Suffolk", "6th Worcester",
    "7th Bristol", "7th Essex", "7th Hampden", "7th Middlesex",
    "7th Norfolk", "7th Plymouth", "7th Suffolk", "7th Worcester",
    "8th Bristol", "8th Essex", "8th Hampden", "8th Middlesex",
    "8th Norfolk", "8th Plymouth", "8th Suffolk", "8th Worcester",
    "9th Bristol", "9th Essex", "9th Hampden", "9th Middlesex",
    "9th Norfolk", "9th Plymouth", "9th Suffolk", "9th Worcester",
    "10th Bristol", "10th Essex", "10th Hampden", "10th Middlesex",
    "10th Norfolk", "10th Plymouth", "10th Suffolk", "10th Worcester",
    "11th Bristol", "11th Essex", "11th Hampden", "11th Middlesex",
    "11th Norfolk", "11th Plymouth", "11th Suffolk", "11th Worcester",
    "12th Bristol", "12th Essex", "12th Hampden", "12th Middlesex",
    "12th Norfolk", "12th Plymouth", "12th Suffolk", "12th Worcester",
    "13th Bristol", "13th Essex", "13th Middlesex", "13th Norfolk",
    "13th Suffolk", "13th Worcester",
    "14th Bristol", "14th Essex", "14th Middlesex", "14th Norfolk",
    "14th Suffolk", "14th Worcester",
    "15th Essex", "15th Middlesex", "15th Norfolk", "15th Suffolk", "15th Worcester",
    "16th Essex", "16th Middlesex", "16th Suffolk", "16th Worcester",
    "17th Essex", "17th Middlesex", "17th Suffolk", "17th Worcester",
    "18th Essex", "18th Middlesex", "18th Suffolk", "18th Worcester",
    "19th Middlesex", "19th Suffolk", "19th Worcester",
    "20th Middlesex", "21st Middlesex", "22nd Middlesex", "23rd Middlesex",
    "24th Middlesex", "25th Middlesex", "26th Middlesex", "27th Middlesex",
    "28th Middlesex", "29th Middlesex", "30th Middlesex", "31st Middlesex",
    "32nd Middlesex", "33rd Middlesex", "34th Middlesex", "35th Middlesex",
    "36th Middlesex", "37th Middlesex",
    "Barnstable, Dukes, and Nantucket",
]

# All 40 MA Senate district names (Ballotpedia format)
MA_SENATE_DISTRICTS_BP = [
    "1st Bristol and Plymouth", "1st Essex", "1st Essex and Middlesex",
    "1st Middlesex", "1st Plymouth and Norfolk", "1st Suffolk", "1st Worcester",
    "2nd Bristol and Plymouth", "2nd Essex", "2nd Essex and Middlesex",
    "2nd Middlesex", "2nd Plymouth and Norfolk", "2nd Suffolk", "2nd Worcester",
    "3rd Bristol and Plymouth", "3rd Essex", "3rd Middlesex", "3rd Suffolk",
    "4th Middlesex", "5th Middlesex",
    "Berkshire, Hampden, Franklin, and Hampshire",
    "Bristol and Norfolk", "Cape and Islands",
    "Hampden", "Hampden and Hampshire", "Hampden, Hampshire, and Worcester",
    "Hampshire, Franklin, and Worcester",
    "Middlesex and Norfolk", "Middlesex and Suffolk", "Middlesex and Worcester",
    "Norfolk and Middlesex", "Norfolk and Plymouth", "Norfolk and Suffolk",
    "Norfolk, Plymouth, and Bristol", "Norfolk, Worcester, and Middlesex",
    "Plymouth and Barnstable",
    "Suffolk and Middlesex",
    "Worcester and Hampden", "Worcester and Hampshire", "Worcester and Middlesex",
]

def bp_to_os_name(name, chamber):
    """Convert Ballotpedia district name to OpenStates format."""
    result = name
    if chamber == 'upper':
        # Senate: "1st" → "First", "2nd" → "Second", etc.
        ordinal_map = {
            "1st ": "First ", "2nd ": "Second ", "3rd ": "Third ",
            "4th ": "Fourth ", "5th ": "Fifth ",
        }
        for abbr, word in ordinal_map.items():
            if result.startswith(abbr):
                result = word + result[len(abbr):]
                break
    # Both chambers: remove Oxford comma
    result = result.replace(", and ", " and ")
    return result

def build_name_to_number_maps():
    """
    Sort district names alphabetically and assign to sequential DB numbers.
    Returns (house_map, senate_map) where each is {openstates_name: db_district_number}.
    """
    # Convert to OpenStates format and sort
    house_os_names = sorted(bp_to_os_name(n, 'lower') for n in MA_HOUSE_DISTRICTS_BP)
    senate_os_names = sorted(bp_to_os_name(n, 'upper') for n in MA_SENATE_DISTRICTS_BP)

    assert len(house_os_names) == 160, f"Expected 160 House, got {len(house_os_names)}"
    assert len(senate_os_names) == 40, f"Expected 40 Senate, got {len(senate_os_names)}"

    house_map = {name: str(i + 1) for i, name in enumerate(house_os_names)}
    senate_map = {name: str(i + 1) for i, name in enumerate(senate_os_names)}

    return house_map, senate_map

# ══════════════════════════════════════════════════════════════════
# PARTY MAPPING
# ══════════════════════════════════════════════════════════════════
PARTY_MAP = {
    'Republican': 'R',
    'Democratic': 'D',
    'Independent': 'I',
    'Unenrolled': 'I',  # MA term for independent
}

def map_party(os_party):
    code = PARTY_MAP.get(os_party, os_party[:3] if os_party else 'U')
    return code, code

# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    # ── Step 0: Precondition checks ──────────────────────────────
    print("=" * 60)
    print("STEP 0: Precondition checks")
    print("=" * 60)

    # Check MA has no legislative seat_terms yet
    ma_st = run_sql("""
        SELECT COUNT(*) as cnt FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'MA' AND se.office_level = 'Legislative'
    """)
    if ma_st[0]['cnt'] > 0:
        print(f"  ERROR: MA already has {ma_st[0]['cnt']} legislative seat_terms!")
        print("  Aborting to prevent duplicates.")
        sys.exit(1)
    print(f"  MA legislative seat_terms: 0 (clean)")

    # Check MA legislative seats exist
    ma_seats = run_sql("""
        SELECT COUNT(*) as cnt FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'MA' AND se.office_level = 'Legislative'
    """)
    print(f"  MA legislative seats: {ma_seats[0]['cnt']} (expected 200)")

    # ── Step 1: Build name→number maps ───────────────────────────
    print("\n" + "=" * 60)
    print("STEP 1: Build district name → number mapping")
    print("=" * 60)

    house_map, senate_map = build_name_to_number_maps()
    print(f"  House mapping: {len(house_map)} districts")
    print(f"  Senate mapping: {len(senate_map)} districts")

    # Show a few examples
    print("\n  House examples:")
    for name in list(sorted(house_map.keys()))[:5]:
        print(f"    '{name}' → district {house_map[name]}")
    print("\n  Senate examples:")
    for name in list(sorted(senate_map.keys()))[:5]:
        print(f"    '{name}' → district {senate_map[name]}")

    # ── Step 2: Load seat IDs from DB ────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 2: Load MA seat IDs from DB")
    print("=" * 60)

    seat_rows = run_sql("""
        SELECT se.id as seat_id, se.office_type, d.district_number
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'MA' AND se.office_level = 'Legislative'
        ORDER BY se.office_type, d.district_number
    """)

    # Build (office_type, district_number) → seat_id
    seat_map = {}
    for r in seat_rows:
        seat_map[(r['office_type'], r['district_number'])] = r['seat_id']

    print(f"  Loaded {len(seat_map)} seat mappings")

    # ── Step 3: Download and parse OpenStates ────────────────────
    print("\n" + "=" * 60)
    print("STEP 3: Download OpenStates MA data")
    print("=" * 60)

    resp = httpx.get(OPENSTATES_URL, timeout=30, follow_redirects=True)
    if resp.status_code != 200:
        print(f"  ERROR: HTTP {resp.status_code}")
        sys.exit(1)

    reader = csv.DictReader(io.StringIO(resp.text))
    legislators = list(reader)
    print(f"  Downloaded {len(legislators)} legislators")

    # ── Step 4: Match legislators to seats ───────────────────────
    print("\n" + "=" * 60)
    print("STEP 4: Match legislators to seats")
    print("=" * 60)

    matched = []
    unmatched = []

    for leg in legislators:
        os_chamber = leg.get('current_chamber', '').strip()
        os_district = leg.get('current_district', '').strip()
        os_party = leg.get('current_party', '').strip()

        # Determine office_type and name→number map
        if os_chamber == 'lower':
            office_type = 'State House'
            name_map = house_map
        elif os_chamber == 'upper':
            office_type = 'State Senate'
            name_map = senate_map
        else:
            unmatched.append(leg)
            continue

        # Look up district number from name
        district_number = name_map.get(os_district)
        if district_number is None:
            unmatched.append(leg)
            print(f"    No mapping for {os_chamber} district: {os_district!r}")
            continue

        # Look up seat_id
        seat_id = seat_map.get((office_type, district_number))
        if seat_id is None:
            unmatched.append(leg)
            print(f"    No seat for {office_type} district {district_number} (from {os_district!r})")
            continue

        party, caucus = map_party(os_party)
        gender = (leg.get('gender', '') or '').strip()[:1] or None

        matched.append({
            'seat_id': seat_id,
            'full_name': leg.get('name', '').strip(),
            'first_name': leg.get('given_name', '').strip(),
            'last_name': leg.get('family_name', '').strip(),
            'gender': gender,
            'party': party,
            'caucus': caucus,
            'district_name': os_district,
            'office_type': office_type,
        })

    print(f"\n  Matched: {len(matched)}")
    print(f"  Unmatched: {len(unmatched)}")

    # Verify no duplicate seat assignments
    seat_ids_used = [m['seat_id'] for m in matched]
    if len(seat_ids_used) != len(set(seat_ids_used)):
        print("  ERROR: Duplicate seat assignments!")
        from collections import Counter
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL
        dupes = {k: v for k, v in Counter(seat_ids_used).items() if v > 1}
        for sid, cnt in dupes.items():
            legs = [m for m in matched if m['seat_id'] == sid]
            print(f"    seat_id={sid}: {[l['full_name'] for l in legs]}")
        sys.exit(1)
    print("  No duplicate seat assignments!")

    if args.dry_run:
        print("\n  DRY RUN — showing mapping:")
        house_matched = [m for m in matched if m['office_type'] == 'State House']
        senate_matched = [m for m in matched if m['office_type'] == 'State Senate']
        print(f"  House: {len(house_matched)} matched")
        print(f"  Senate: {len(senate_matched)} matched")
        print("\n  Sample matches:")
        for m in matched[:10]:
            print(f"    seat={m['seat_id']} {m['office_type']} → {m['full_name']} ({m['party']}) [{m['district_name']}]")
        return

    # ── Step 5: Insert candidates ────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 5: Insert candidates")
    print("=" * 60)

    values = []
    for m in matched:
        gender_val = f"'{m['gender']}'" if m['gender'] else 'NULL'
        values.append(
            f"('{esc(m['full_name'])}', '{esc(m['first_name'])}', "
            f"'{esc(m['last_name'])}', {gender_val})"
        )

    sql = (
        "INSERT INTO candidates (full_name, first_name, last_name, gender) VALUES\n"
        + ",\n".join(values)
        + "\nRETURNING id;"
    )
    result = run_sql(sql)
    cand_ids = [r['id'] for r in result]
    print(f"  Inserted {len(cand_ids)} candidates")

    if len(cand_ids) != len(matched):
        print(f"  ERROR: Expected {len(matched)}, got {len(cand_ids)}")
        sys.exit(1)

    # ── Step 6: Insert seat_terms ────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 6: Insert seat_terms")
    print("=" * 60)

    values = []
    for i, m in enumerate(matched):
        values.append(
            f"({m['seat_id']}, {cand_ids[i]}, '{esc(m['party'])}', '2025-01-01', NULL, "
            f"'elected', '{esc(m['caucus'])}', NULL)"
        )

    sql = (
        "INSERT INTO seat_terms (seat_id, candidate_id, party, start_date, end_date, "
        "start_reason, caucus, election_id) VALUES\n"
        + ",\n".join(values)
        + "\nRETURNING id;"
    )
    result = run_sql(sql)
    print(f"  Inserted {len(result)} seat_terms")

    # ── Step 7: Update seats cache ───────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 7: Update seats cache columns")
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
          AND seats.office_level = 'Legislative'
          AND seats.id IN (
              SELECT se.id FROM seats se
              JOIN districts d ON se.district_id = d.id
              JOIN states s ON d.state_id = s.id
              WHERE s.abbreviation = 'MA'
          );
    """)

    updated = run_sql("""
        SELECT COUNT(*) as cnt FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'MA'
          AND se.office_level = 'Legislative'
          AND se.current_holder IS NOT NULL
    """)
    print(f"  MA seats with current_holder: {updated[0]['cnt']}")

    # ── Step 8: Verification ─────────────────────────────────────
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    # Counts
    counts = run_sql("""
        SELECT
            (SELECT COUNT(*) FROM candidates) as total_candidates,
            (SELECT COUNT(*) FROM seat_terms) as total_seat_terms,
            (SELECT COUNT(*) FROM seat_terms st
             JOIN seats se ON st.seat_id = se.id
             JOIN districts d ON se.district_id = d.id
             JOIN states s ON d.state_id = s.id
             WHERE s.abbreviation = 'MA' AND se.office_level = 'Legislative') as ma_leg_terms
    """)
    c = counts[0]
    print(f"  Total candidates: {c['total_candidates']}")
    print(f"  Total seat_terms: {c['total_seat_terms']}")
    print(f"  MA legislative seat_terms: {c['ma_leg_terms']}")

    # Party distribution for MA
    print("\n  MA legislative party distribution:")
    party = run_sql("""
        SELECT st.party, COUNT(*) as cnt
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'MA' AND se.office_level = 'Legislative'
        GROUP BY st.party ORDER BY cnt DESC
    """)
    for r in party:
        print(f"    {r['party']}: {r['cnt']}")

    # Coverage
    coverage = run_sql("""
        SELECT se.office_type,
               COUNT(se.id) as total,
               COUNT(st.id) as filled
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        LEFT JOIN seat_terms st ON se.id = st.seat_id AND st.end_date IS NULL
        WHERE s.abbreviation = 'MA' AND se.office_level = 'Legislative'
        GROUP BY se.office_type
    """)
    print("\n  MA coverage by chamber:")
    for r in coverage:
        pct = r['filled'] / r['total'] * 100
        print(f"    {r['office_type']}: {r['filled']}/{r['total']} ({pct:.1f}%)")

    # Spot checks
    print("\n  Spot checks:")
    spots = run_sql("""
        SELECT se.seat_label, c.full_name, st.party
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        JOIN candidates c ON st.candidate_id = c.id
        WHERE s.abbreviation = 'MA' AND se.office_level = 'Legislative'
          AND st.end_date IS NULL
        ORDER BY RANDOM()
        LIMIT 10
    """)
    for r in spots:
        print(f"    {r['seat_label']}: {r['full_name']} ({r['party']})")

    # Duplicate check
    dupes = run_sql("""
        SELECT seat_id, COUNT(*) as cnt
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'MA' AND se.office_level = 'Legislative'
          AND st.end_date IS NULL
        GROUP BY seat_id HAVING COUNT(*) > 1
    """)
    if dupes:
        print(f"\n  WARNING: {len(dupes)} seats with duplicate active terms!")
    else:
        print("\n  No duplicate seat_terms!")

    # Overall DB summary
    print("\n  Overall database summary:")
    overall = run_sql("""
        SELECT
            (SELECT COUNT(*) FROM candidates) as candidates,
            (SELECT COUNT(*) FROM seat_terms) as seat_terms,
            (SELECT COUNT(*) FROM seats WHERE office_level = 'Legislative'
             AND current_holder IS NOT NULL) as leg_filled,
            (SELECT COUNT(*) FROM seats WHERE office_level = 'Legislative') as leg_total
    """)
    o = overall[0]
    print(f"    candidates: {o['candidates']}")
    print(f"    seat_terms: {o['seat_terms']}")
    print(f"    legislative seats filled: {o['leg_filled']}/{o['leg_total']} ({o['leg_filled']/o['leg_total']*100:.1f}%)")

    print("\nDone!")

if __name__ == '__main__':
    main()
