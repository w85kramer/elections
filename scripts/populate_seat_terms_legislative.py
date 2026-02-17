"""
Populate seat_terms for current state legislators (Phase 2).

Downloads OpenStates bulk CSV data for all 50 states, matches legislators
to seats in the database, and inserts candidates + seat_terms records.

Requires: Phase 1 complete (278 statewide candidates/seat_terms).

Usage:
    python3 scripts/populate_seat_terms_legislative.py
    python3 scripts/populate_seat_terms_legislative.py --dry-run
    python3 scripts/populate_seat_terms_legislative.py --state CA
"""
import sys
import os
import csv
import io
import time
import argparse

import httpx
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

OPENSTATES_URL = 'https://data.openstates.org/people/current/{state}.csv'

BATCH_SIZE = 500

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
    """Escape single quotes for SQL."""
    if s is None:
        return ''
    return str(s).replace("'", "''")

# ══════════════════════════════════════════════════════════════════
# STATE ABBREVIATIONS
# ══════════════════════════════════════════════════════════════════
ALL_STATES = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
]

# ══════════════════════════════════════════════════════════════════
# CHAMBER MAPPING: OpenStates → our office_type
# ══════════════════════════════════════════════════════════════════
# State-specific lower chamber names
LOWER_CHAMBER_NAMES = {
    'CA': 'Assembly', 'NV': 'Assembly', 'NY': 'Assembly',
    'WI': 'Assembly', 'NJ': 'Assembly',
    'MD': 'House of Delegates', 'VA': 'House of Delegates', 'WV': 'House of Delegates',
}

def get_office_type(state, os_chamber):
    """Convert OpenStates chamber to our office_type."""
    if os_chamber == 'upper':
        return 'State Senate'
    elif os_chamber == 'lower':
        return 'State House'
    elif os_chamber == 'legislature':
        return 'State Legislature'  # NE only
    return None

def get_db_chamber(state, os_chamber):
    """Convert OpenStates chamber to our DB chamber value (for districts)."""
    if os_chamber == 'upper':
        return 'Senate'
    elif os_chamber == 'lower':
        if state in LOWER_CHAMBER_NAMES:
            return LOWER_CHAMBER_NAMES[state]
        return 'House'
    elif os_chamber == 'legislature':
        return 'Legislature'
    return None

# ══════════════════════════════════════════════════════════════════
# PARTY MAPPING
# ══════════════════════════════════════════════════════════════════
PARTY_MAP = {
    'Republican': 'R',
    'Democratic': 'D',
    'Democratic-Farmer-Labor': 'D',                          # MN
    'Democratic/Progressive': 'D',                            # VT dual-endorsed
    'Democratic/Working Families': 'D',                       # NY fusion
    'Democratic/Independence/Working Families': 'D',          # NY fusion
    'Republican/Conservative': 'R',                           # NY fusion
    'Republican/Conservative/Independence': 'R',              # NY fusion
    'Republican/Conservative/Independence/Libertarian': 'R',  # NY fusion
    'Republican/Conservative/Independence/Reform': 'R',       # NY fusion
    'Independent': 'I',
    'Nonpartisan': 'NP',                                      # NE
    'Progressive': 'P',                                       # VT
    'Forward': 'F',                                           # Forward Party
    'Libertarian': 'L',
    'Green': 'G',
    'Working Families Party': 'WFP',
}

# Parties that caucus with Democrats
D_CAUCUS_PARTIES = {'P', 'WFP', 'G'}

# Independents that caucus with a party (can add specifics as needed)
# Forward party members may caucus either way — default to their own code

def map_party(os_party):
    """Map OpenStates party to our code. Returns (party, caucus)."""
    code = PARTY_MAP.get(os_party)
    if code is None:
        # Fallback: check if party starts with a known prefix
        if os_party and os_party.startswith('Democratic'):
            code = 'D'
        elif os_party and os_party.startswith('Republican'):
            code = 'R'
        else:
            code = os_party[:3] if os_party else 'U'
    if code in D_CAUCUS_PARTIES:
        return code, 'D'
    return code, code

# ══════════════════════════════════════════════════════════════════
# AK SENATE LETTER-TO-NUMBER MAPPING
# ══════════════════════════════════════════════════════════════════
AK_SENATE_MAP = {chr(65 + i): str(i + 1) for i in range(20)}  # A→1, B→2, ..., T→20

# ══════════════════════════════════════════════════════════════════
# VT SENATE: OpenStates name → DB district_number
# "Essex" and "Orleans" are separate in OpenStates but combined as "Essex-Orleans" in DB
# "Grand Isle" has a space in both OpenStates and DB (no hyphen conversion needed)
# Directional suffixes: "Chittenden North" → "Chittenden-North"
# ══════════════════════════════════════════════════════════════════
VT_SENATE_MAP = {
    'Essex': 'Essex-Orleans',
    'Orleans': 'Essex-Orleans',
}

# ══════════════════════════════════════════════════════════════════
# MA: Skip entirely — OpenStates uses named county districts
# ("7th Hampden", "3rd Suffolk") while our DB uses sequential numbers.
# Needs a complete mapping table built separately.
# ══════════════════════════════════════════════════════════════════
SKIP_STATES = {'MA'}

# ══════════════════════════════════════════════════════════════════
# DISTRICT TRANSLATION
# ══════════════════════════════════════════════════════════════════
def translate_district(state, os_chamber, os_district):
    """
    Translate OpenStates district name to our DB district_number.
    Returns (district_number, extracted_seat_designator_or_None).
    """
    if os_district is None:
        return None, None

    district = os_district.strip()

    # AK Senate: letter → number
    if state == 'AK' and os_chamber == 'upper':
        if district in AK_SENATE_MAP:
            return AK_SENATE_MAP[district], None

    # NH House: "Rockingham 30" → "Rockingham-30"
    if state == 'NH' and os_chamber == 'lower':
        parts = district.rsplit(' ', 1)
        if len(parts) == 2 and parts[1].isdigit():
            return f"{parts[0]}-{parts[1]}", None

    # VT Senate: handle name mappings
    if state == 'VT' and os_chamber == 'upper':
        # Check explicit mapping first (Essex→Essex-Orleans, etc.)
        if district in VT_SENATE_MAP:
            return VT_SENATE_MAP[district], None
        # "Grand Isle" stays as-is (DB has "Grand Isle" with space, not hyphen)
        if district == 'Grand Isle':
            return district, None
        # Directional suffixes: "Chittenden North" → "Chittenden-North"
        if ' ' in district:
            return district.replace(' ', '-'), None
        # Single-name districts (Addison, Bennington, etc.) → return as-is
        return district, None

    # MN House: "62A" → district "123", "62B" → district "124"
    # MN has 67 Senate districts, each split into House sub-districts A and B.
    # Our DB numbers them sequentially 1-134: senate_num*2-1 = A, senate_num*2 = B
    if state == 'MN' and os_chamber == 'lower':
        if district and district[-1].isalpha() and district[:-1].isdigit():
            senate_num = int(district[:-1])
            suffix = district[-1].upper()
            if suffix == 'A':
                return str(senate_num * 2 - 1), None
            elif suffix == 'B':
                return str(senate_num * 2), None

    # SD House: "26A" → district "26" with seat designator "A"
    if state == 'SD' and os_chamber == 'lower':
        if district and district[-1].isalpha() and district[:-1].isdigit():
            return district[:-1], district[-1].upper()

    # ND House: some districts have suffixes like "4A", "4B"
    if state == 'ND' and os_chamber == 'lower':
        if district and district[-1].isalpha() and district[:-1].isdigit():
            return district[:-1], district[-1].upper()

    # ID House: similar potential suffix format
    if state == 'ID' and os_chamber == 'lower':
        if district and district[-1].isalpha() and district[:-1].isdigit():
            return district[:-1], district[-1].upper()

    # General: return as-is
    return district, None

# ══════════════════════════════════════════════════════════════════
# MAIN SCRIPT
# ══════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description='Populate legislative seat_terms')
    parser.add_argument('--dry-run', action='store_true', help='Parse and match only, no inserts')
    parser.add_argument('--state', type=str, help='Process only this state (e.g., CA)')
    args = parser.parse_args()

    states_to_process = [args.state.upper()] if args.state else ALL_STATES

    # ══════════════════════════════════════════════════════════════
    # STEP 0: Precondition checks
    # ══════════════════════════════════════════════════════════════
    print("=" * 60)
    print("STEP 0: Precondition checks")
    print("=" * 60)

    # Check Phase 1 complete
    sw_count = run_sql("SELECT COUNT(*) as cnt FROM seat_terms st JOIN seats se ON st.seat_id = se.id WHERE se.office_level = 'Statewide'")
    print(f"  Statewide seat_terms: {sw_count[0]['cnt']} (expected 278)")
    if sw_count[0]['cnt'] != 278:
        print("  ERROR: Phase 1 not complete! Expected 278 statewide seat_terms.")
        sys.exit(1)

    # Check no legislative seat_terms yet
    leg_st = run_sql("SELECT COUNT(*) as cnt FROM seat_terms st JOIN seats se ON st.seat_id = se.id WHERE se.office_level = 'Legislative'")
    if leg_st[0]['cnt'] > 0:
        print(f"  ERROR: {leg_st[0]['cnt']} legislative seat_terms already exist!")
        print("  Aborting to prevent duplicates.")
        sys.exit(1)
    print(f"  Legislative seat_terms: 0 (clean, good)")

    # Count legislative seats
    leg_seats = run_sql("SELECT COUNT(*) as cnt FROM seats WHERE office_level = 'Legislative'")
    print(f"  Legislative seats in DB: {leg_seats[0]['cnt']} (expected 7386)")

    print("  Preconditions OK!\n")

    # ══════════════════════════════════════════════════════════════
    # STEP 1: Download OpenStates CSVs
    # ══════════════════════════════════════════════════════════════
    print("=" * 60)
    print("STEP 1: Download OpenStates CSVs")
    print("=" * 60)

    all_legislators = []
    download_failures = []
    skipped_states = []

    for state in states_to_process:
        if state in SKIP_STATES:
            print(f"  {state}: SKIPPED (incompatible district naming)")
            skipped_states.append(state)
            continue
        url = OPENSTATES_URL.format(state=state.lower())
        try:
            resp = httpx.get(url, timeout=30, follow_redirects=True)
            if resp.status_code != 200:
                print(f"  {state}: HTTP {resp.status_code} - FAILED")
                download_failures.append(state)
                continue

            reader = csv.DictReader(io.StringIO(resp.text))
            count = 0
            for row in reader:
                row['_state'] = state
                all_legislators.append(row)
                count += 1
            print(f"  {state}: {count} legislators")

        except Exception as e:
            print(f"  {state}: ERROR - {e}")
            download_failures.append(state)

        time.sleep(0.3)

    print(f"\n  Total legislators downloaded: {len(all_legislators)}")
    if download_failures:
        print(f"  Download failures: {download_failures}")
    if skipped_states:
        print(f"  Skipped states: {skipped_states}")
    print()

    # ══════════════════════════════════════════════════════════════
    # STEP 2: Build seat lookup maps from DB
    # ══════════════════════════════════════════════════════════════
    print("=" * 60)
    print("STEP 2: Build seat lookup maps from DB")
    print("=" * 60)

    seat_rows = run_sql("""
        SELECT se.id as seat_id, se.office_type, se.seat_designator,
               d.district_number, d.num_seats, d.chamber,
               s.abbreviation as state
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE se.office_level = 'Legislative'
        ORDER BY s.abbreviation, d.chamber, d.district_number, se.seat_designator
    """)

    # Single-member map: (state, office_type, district_number) → seat_id
    single_map = {}
    # Multi-member map: (state, office_type, district_number) → [(seat_id, seat_designator), ...]
    multi_map = {}

    for row in seat_rows:
        key = (row['state'], row['office_type'], row['district_number'])
        if row['num_seats'] == 1:
            single_map[key] = row['seat_id']
        else:
            if key not in multi_map:
                multi_map[key] = []
            multi_map[key].append((row['seat_id'], row['seat_designator']))

    # Sort multi-member seat lists by designator
    for key in multi_map:
        multi_map[key].sort(key=lambda x: x[1] or '')

    print(f"  Single-member entries: {len(single_map)}")
    print(f"  Multi-member district groups: {len(multi_map)}")
    print(f"  Total seats covered: {len(single_map) + sum(len(v) for v in multi_map.values())}")
    print()

    # ══════════════════════════════════════════════════════════════
    # STEP 3-4: Translate and match legislators to seats
    # ══════════════════════════════════════════════════════════════
    print("=" * 60)
    print("STEP 3-4: Match legislators to seats")
    print("=" * 60)

    # Group legislators by (state, office_type, district_number) for multi-member handling
    # First pass: translate all legislators
    translated = []
    skipped_chamber = []

    for leg in all_legislators:
        state = leg['_state']
        os_chamber = leg.get('current_chamber', '').strip()
        os_district = leg.get('current_district', '').strip()
        os_party = leg.get('current_party', '').strip()

        office_type = get_office_type(state, os_chamber)
        if not office_type:
            skipped_chamber.append((state, leg.get('name', '?'), os_chamber))
            continue

        district_number, extracted_designator = translate_district(state, os_chamber, os_district)

        party, caucus = map_party(os_party)

        translated.append({
            'state': state,
            'office_type': office_type,
            'district_number': district_number,
            'extracted_designator': extracted_designator,
            'full_name': leg.get('name', '').strip(),
            'first_name': leg.get('given_name', '').strip(),
            'last_name': leg.get('family_name', '').strip(),
            'gender': (leg.get('gender', '') or '').strip()[:1] if leg.get('gender') else None,
            'party': party,
            'caucus': caucus,
        })

    if skipped_chamber:
        print(f"  Skipped {len(skipped_chamber)} legislators with unknown chamber")
        for s, n, c in skipped_chamber[:5]:
            print(f"    {s}: {n} (chamber={c})")

    # Now match to seats
    matched = []       # list of (legislator_dict, seat_id)
    unmatched = []     # list of legislator_dict

    # Track already-assigned single-member seats to prevent duplicates
    assigned_single = set()

    multi_candidates = {}  # key → [legislator_dict, ...]

    for leg in translated:
        key = (leg['state'], leg['office_type'], leg['district_number'])

        # Check if legislator has an extracted designator for direct multi-member match
        if leg['extracted_designator']:
            # Try to find the specific seat in multi_map
            if key in multi_map:
                seats_in_district = multi_map[key]
                found = False
                for seat_id, seat_desg in seats_in_district:
                    if seat_desg == leg['extracted_designator']:
                        matched.append((leg, seat_id))
                        found = True
                        break
                if not found:
                    unmatched.append(leg)
                continue

        # Single-member direct match (first legislator wins)
        if key in single_map:
            seat_id = single_map[key]
            if seat_id not in assigned_single:
                matched.append((leg, seat_id))
                assigned_single.add(seat_id)
            else:
                # NH floterial overlap: multiple legislators claim same single-member district
                # Try multi_map as fallback (they might belong to a floterial district)
                if key in multi_map:
                    if key not in multi_candidates:
                        multi_candidates[key] = []
                    multi_candidates[key].append(leg)
                else:
                    unmatched.append(leg)
        elif key in multi_map:
            # Multi-member: collect for group assignment
            if key not in multi_candidates:
                multi_candidates[key] = []
            multi_candidates[key].append(leg)
        else:
            unmatched.append(leg)

    # Assign multi-member seats alphabetically by last name
    multi_matched = 0
    multi_overflow = 0
    for key, legs in multi_candidates.items():
        seats = multi_map.get(key, [])
        if not seats:
            unmatched.extend(legs)
            continue

        # Sort legislators by last_name, then first_name
        legs.sort(key=lambda l: (l['last_name'].lower(), l['first_name'].lower()))

        for i, leg in enumerate(legs):
            if i < len(seats):
                matched.append((leg, seats[i][0]))
                multi_matched += 1
            else:
                # More legislators than seats
                unmatched.append(leg)
                multi_overflow += 1
                if multi_overflow <= 10:
                    print(f"    Overflow: {leg['state']} {leg['office_type']} d={leg['district_number']}: "
                          f"{leg['full_name']} (have {len(legs)} legislators, {len(seats)} seats)")

    print(f"\n  Translated legislators: {len(translated)}")
    print(f"  Direct single-member matches: {len(matched) - multi_matched}")
    print(f"  Multi-member matches (alphabetical): {multi_matched}")
    if multi_overflow:
        print(f"  Multi-member overflow (extra legislators): {multi_overflow}")
    print(f"  Unmatched: {len(unmatched)}")

    # Report unmatched by state
    if unmatched:
        unmatched_by_state = {}
        for leg in unmatched:
            st = leg['state']
            if st not in unmatched_by_state:
                unmatched_by_state[st] = []
            unmatched_by_state[st].append(leg)

        print(f"\n  Unmatched by state:")
        for st in sorted(unmatched_by_state.keys()):
            legs = unmatched_by_state[st]
            print(f"    {st}: {len(legs)} unmatched")
            for l in legs[:3]:
                print(f"      - {l['full_name']} ({l['office_type']}, district={l['district_number']})")
            if len(legs) > 3:
                print(f"      ... and {len(legs) - 3} more")

    print(f"\n  Total matched: {len(matched)} legislators → seats")

    if args.dry_run:
        print("\n  DRY RUN — no database changes made.")
        # Print per-state summary
        state_summary = {}
        for leg, seat_id in matched:
            st = leg['state']
            state_summary[st] = state_summary.get(st, 0) + 1
        print("\n  Per-state match counts:")
        for st in sorted(state_summary.keys()):
            print(f"    {st}: {state_summary[st]}")
        return

    # ══════════════════════════════════════════════════════════════
    # STEP 5: Insert candidates (batch)
    # ══════════════════════════════════════════════════════════════
    print("\n" + "=" * 60)
    print("STEP 5: Insert candidates")
    print("=" * 60)

    # Build candidate data in matched order
    all_cand_ids = []
    total_inserted = 0

    for batch_start in range(0, len(matched), BATCH_SIZE):
        batch = matched[batch_start:batch_start + BATCH_SIZE]
        values = []
        for leg, seat_id in batch:
            gender_val = f"'{leg['gender']}'" if leg['gender'] else 'NULL'
            values.append(
                f"('{esc(leg['full_name'])}', '{esc(leg['first_name'])}', "
                f"'{esc(leg['last_name'])}', {gender_val})"
            )

        sql = (
            "INSERT INTO candidates (full_name, first_name, last_name, gender) VALUES\n"
            + ",\n".join(values)
            + "\nRETURNING id;"
        )

        # Retry once on failure
        result = run_sql(sql, exit_on_error=False)
        if result is None:
            print(f"  Batch {batch_start}-{batch_start+len(batch)} failed, retrying in 2s...")
            time.sleep(2)
            result = run_sql(sql)

        for row in result:
            all_cand_ids.append(row['id'])
        total_inserted += len(result)
        print(f"  Batch {batch_start}-{batch_start+len(batch)}: {len(result)} candidates inserted")

    print(f"  Total candidates inserted: {total_inserted}")

    if total_inserted != len(matched):
        print(f"  ERROR: Expected {len(matched)}, got {total_inserted}")
        sys.exit(1)

    # ══════════════════════════════════════════════════════════════
    # STEP 6: Insert seat_terms (batch)
    # ══════════════════════════════════════════════════════════════
    print("\n" + "=" * 60)
    print("STEP 6: Insert seat_terms")
    print("=" * 60)

    total_st_inserted = 0

    for batch_start in range(0, len(matched), BATCH_SIZE):
        batch = matched[batch_start:batch_start + BATCH_SIZE]
        values = []
        for i, (leg, seat_id) in enumerate(batch):
            cand_id = all_cand_ids[batch_start + i]
            party = leg['party']
            caucus = leg['caucus']
            values.append(
                f"({seat_id}, {cand_id}, '{esc(party)}', '2025-01-01', NULL, "
                f"'elected', '{esc(caucus)}', NULL)"
            )

        sql = (
            "INSERT INTO seat_terms (seat_id, candidate_id, party, start_date, end_date, "
            "start_reason, caucus, election_id) VALUES\n"
            + ",\n".join(values)
            + "\nRETURNING id;"
        )

        result = run_sql(sql, exit_on_error=False)
        if result is None:
            print(f"  Batch {batch_start}-{batch_start+len(batch)} failed, retrying in 2s...")
            time.sleep(2)
            result = run_sql(sql)

        total_st_inserted += len(result)
        print(f"  Batch {batch_start}-{batch_start+len(batch)}: {len(result)} seat_terms inserted")

    print(f"  Total seat_terms inserted: {total_st_inserted}")

    if total_st_inserted != len(matched):
        print(f"  ERROR: Expected {len(matched)}, got {total_st_inserted}")
        sys.exit(1)

    # ══════════════════════════════════════════════════════════════
    # STEP 7: Update seats cache columns
    # ══════════════════════════════════════════════════════════════
    print("\n" + "=" * 60)
    print("STEP 7: Update seats cache columns")
    print("=" * 60)

    sql = """
    UPDATE seats
    SET current_holder = c.full_name,
        current_holder_party = st.party,
        current_holder_caucus = st.caucus
    FROM seat_terms st
    JOIN candidates c ON st.candidate_id = c.id
    WHERE seats.id = st.seat_id
      AND st.end_date IS NULL
      AND seats.office_level = 'Legislative';
    """
    run_sql(sql)

    updated = run_sql("""
        SELECT COUNT(*) as cnt FROM seats
        WHERE office_level = 'Legislative'
          AND current_holder IS NOT NULL
    """)
    print(f"  Legislative seats with current_holder: {updated[0]['cnt']}")

    # ══════════════════════════════════════════════════════════════
    # STEP 8: Verification
    # ══════════════════════════════════════════════════════════════
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    # 1. Total counts
    print("\n1. Record counts:")
    counts = run_sql("""
        SELECT
            (SELECT COUNT(*) FROM candidates) as candidates,
            (SELECT COUNT(*) FROM seat_terms) as seat_terms,
            (SELECT COUNT(*) FROM seat_terms WHERE end_date IS NULL) as current_terms,
            (SELECT COUNT(*) FROM seats WHERE office_level = 'Legislative'
             AND current_holder IS NOT NULL) as leg_seats_filled,
            (SELECT COUNT(*) FROM seats WHERE office_level = 'Statewide'
             AND current_holder IS NOT NULL) as sw_seats_filled
    """)
    c = counts[0]
    print(f"   Total candidates: {c['candidates']}")
    print(f"   Total seat_terms: {c['seat_terms']}")
    print(f"   Current terms (end_date IS NULL): {c['current_terms']}")
    print(f"   Legislative seats filled: {c['leg_seats_filled']} / 7386")
    print(f"   Statewide seats filled: {c['sw_seats_filled']} (should be 278)")

    # 2. Party distribution
    print("\n2. Party distribution (legislative seat_terms):")
    party_dist = run_sql("""
        SELECT st.party, COUNT(*) as cnt
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        WHERE se.office_level = 'Legislative'
        GROUP BY st.party
        ORDER BY cnt DESC
    """)
    for row in party_dist:
        print(f"   {row['party']}: {row['cnt']}")

    # 3. Per-state coverage
    print("\n3. Per-state coverage (states < 90% filled):")
    coverage = run_sql("""
        SELECT s.abbreviation,
               COUNT(se.id) as total_seats,
               COUNT(st.id) as filled_seats
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        LEFT JOIN seat_terms st ON se.id = st.seat_id AND st.end_date IS NULL
        WHERE se.office_level = 'Legislative'
        GROUP BY s.abbreviation
        ORDER BY s.abbreviation
    """)
    low_coverage = []
    for row in coverage:
        total = row['total_seats']
        filled = row['filled_seats']
        pct = (filled / total * 100) if total > 0 else 0
        if pct < 90:
            low_coverage.append((row['abbreviation'], filled, total, pct))
            print(f"   {row['abbreviation']}: {filled}/{total} ({pct:.1f}%)")
    if not low_coverage:
        print("   All states >= 90% filled!")
    else:
        print(f"   {len(low_coverage)} states below 90%")

    # 4. Full state coverage table
    print("\n4. Full state coverage:")
    for row in coverage:
        total = row['total_seats']
        filled = row['filled_seats']
        pct = (filled / total * 100) if total > 0 else 0
        marker = " *" if pct < 90 else ""
        print(f"   {row['abbreviation']}: {filled}/{total} ({pct:.1f}%){marker}")

    # 5. Duplicate check
    print("\n5. Duplicate check (seats with multiple active seat_terms):")
    dupes = run_sql("""
        SELECT seat_id, COUNT(*) as cnt
        FROM seat_terms
        WHERE end_date IS NULL
        GROUP BY seat_id
        HAVING COUNT(*) > 1
        LIMIT 10
    """)
    if dupes:
        print(f"   WARNING: {len(dupes)} seats with duplicates!")
        for d in dupes:
            print(f"     seat_id={d['seat_id']}: {d['cnt']} active terms")
    else:
        print("   No duplicates found!")

    # 6. Spot checks
    print("\n6. Spot checks:")
    spot_checks_sql = """
        SELECT s.abbreviation as state, se.office_type, se.seat_label,
               c.full_name, st.party
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        JOIN candidates c ON st.candidate_id = c.id
        WHERE st.end_date IS NULL
          AND se.office_level = 'Legislative'
        ORDER BY RANDOM()
        LIMIT 10
    """
    spots = run_sql(spot_checks_sql)
    for r in spots:
        print(f"   {r['state']} {r['seat_label']}: {r['full_name']} ({r['party']})")

    # 7. NH special report
    print("\n7. NH coverage:")
    nh = run_sql("""
        SELECT se.office_type,
               COUNT(se.id) as total,
               COUNT(st.id) as filled
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        LEFT JOIN seat_terms st ON se.id = st.seat_id AND st.end_date IS NULL
        WHERE s.abbreviation = 'NH' AND se.office_level = 'Legislative'
        GROUP BY se.office_type
    """)
    for r in nh:
        print(f"   {r['office_type']}: {r['filled']}/{r['total']}")

    # 8. Overall DB summary
    print("\n8. Overall database summary:")
    overall = run_sql("""
        SELECT
            (SELECT COUNT(*) FROM states) as states,
            (SELECT COUNT(*) FROM districts) as districts,
            (SELECT COUNT(*) FROM seats) as seats,
            (SELECT COUNT(*) FROM elections) as elections,
            (SELECT COUNT(*) FROM candidates) as candidates,
            (SELECT COUNT(*) FROM seat_terms) as seat_terms,
            (SELECT COUNT(*) FROM candidacies) as candidacies
    """)
    o = overall[0]
    print(f"   states: {o['states']}")
    print(f"   districts: {o['districts']}")
    print(f"   seats: {o['seats']}")
    print(f"   elections: {o['elections']}")
    print(f"   candidates: {o['candidates']}")
    print(f"   seat_terms: {o['seat_terms']}")
    print(f"   candidacies: {o['candidacies']}")

    print("\nDone!")

if __name__ == '__main__':
    main()
