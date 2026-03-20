#!/usr/bin/env python3
"""
Create historical redistricting-era districts and seats for Massachusetts,
then migrate elections to the correct era.

Massachusetts has had these redistricting cycles:
  2022 (current): 2022-present, 40 Senate + 160 House
  2012: 2012-2020, 40 Senate + 160 House
  2002: 2002-2010, 40 Senate + 160 House (House revised 2004 VRA)
  1994: 1994-2000, 40 Senate + 160 House
  1988: 1988-1992, 40 Senate + 160 House
  1978: 1978-1986, 40 Senate + 160 House
  1974: 1974-1976, 40 Senate + 240 House (multi-member, House skipped)
  1972: 1970-1972, 40 Senate + 240 House (multi-member, House skipped)

District names from SoS use spelled-out ordinals ("First Middlesex") while
our DB uses abbreviated ordinals ("1st Middlesex").

Usage:
    python3 scripts/create_ma_redistricting_eras.py --dry-run
    python3 scripts/create_ma_redistricting_eras.py --cycle 2012 --dry-run
    python3 scripts/create_ma_redistricting_eras.py --phase 1  # Create districts/seats only
    python3 scripts/create_ma_redistricting_eras.py --phase 2  # Move elections only
    python3 scripts/create_ma_redistricting_eras.py             # Full run
"""
import os
import sys
import json
import time
import re
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

import httpx

MAX_RETRIES = 8
BATCH_SIZE = 50
ELECTION_BATCH_SIZE = 200

# ══════════════════════════════════════════════════════════════════════
# Redistricting cycle definitions
# ══════════════════════════════════════════════════════════════════════

# Each cycle: (cycle_year, first_election_year, last_election_year)
# Elections in [first, last] belong to this cycle
CYCLES = [
    ('2012', 2012, 2020),
    ('2002', 2002, 2010),
    ('1994', 1994, 2000),
    ('1988', 1988, 1992),
    ('1978', 1978, 1986),
    ('1974', 1974, 1976),
    ('1972', 1970, 1972),
]

# House had 240 multi-member districts in 1974 and 1972 — skip those for House
SKIP_HOUSE_CYCLES = {'1974', '1972'}

# ══════════════════════════════════════════════════════════════════════
# Ordinal conversion: spelled-out → abbreviated
# (Same logic as populate_ma_sos_elections.py)
# ══════════════════════════════════════════════════════════════════════

ORDINAL_MAP = {}

_ONES = ['', 'First', 'Second', 'Third', 'Fourth', 'Fifth', 'Sixth',
         'Seventh', 'Eighth', 'Ninth', 'Tenth', 'Eleventh', 'Twelfth',
         'Thirteenth', 'Fourteenth', 'Fifteenth', 'Sixteenth',
         'Seventeenth', 'Eighteenth', 'Nineteenth']
_TENS = ['', '', 'Twenty', 'Thirty', 'Forty', 'Fifty', 'Sixty']
_TENS_ORDINAL = ['', '', 'Twentieth', 'Thirtieth', 'Fortieth', 'Fiftieth', 'Sixtieth']
_SUFFIXES = {1: 'st', 2: 'nd', 3: 'rd'}


def _ordinal_suffix(n):
    """Return abbreviated ordinal like '1st', '2nd', '23rd', '11th'."""
    if 11 <= (n % 100) <= 13:
        return f'{n}th'
    return f'{n}{_SUFFIXES.get(n % 10, "th")}'


# Build 1-19
for i in range(1, 20):
    ORDINAL_MAP[_ONES[i].lower()] = _ordinal_suffix(i)

# Build 20-60
for tens_digit in range(2, 7):
    tens_word = _TENS[tens_digit]
    ORDINAL_MAP[tens_word.lower()] = _ordinal_suffix(tens_digit * 10)
    ORDINAL_MAP[_TENS_ORDINAL[tens_digit].lower()] = _ordinal_suffix(tens_digit * 10)
    for ones_digit in range(1, 10):
        compound = f'{tens_word}-{_ONES[ones_digit]}'
        n = tens_digit * 10 + ones_digit
        ORDINAL_MAP[compound.lower()] = _ordinal_suffix(n)


def normalize_district_name(sos_name):
    """Convert SoS district name to our DB format.

    Examples:
        'First Middlesex' → '1st Middlesex'
        'Twenty-Third Worcester' → '23rd Worcester'
        'Berkshire, Hampshire and Franklin' → unchanged (multi-county)
    """
    if not sos_name:
        return None

    name = sos_name.strip()

    # Try to match the leading ordinal word(s)
    m = re.match(r'^([A-Za-z]+(?:-[A-Za-z]+)?)\s+(.+)$', name)
    if m:
        ordinal_word = m.group(1).lower()
        rest = m.group(2)
        if ordinal_word in ORDINAL_MAP:
            return f'{ORDINAL_MAP[ordinal_word]} {rest}'

    # No ordinal prefix — return as-is
    return name


def normalize_for_matching(name):
    """Normalize a district name for fuzzy comparison.

    Strips punctuation, normalizes ampersands/and, sorts words so
    'Berkshire, Hampden, Franklin & Hampshire' matches
    'Berkshire, Hampden, Hampshire and Franklin'.
    """
    if not name:
        return ''
    n = name.lower()
    # Normalize ampersands
    n = n.replace('&', 'and')
    # Remove commas
    n = re.sub(r'[,]', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    # Sort words for order-independent matching
    words = sorted(n.split())
    return ' '.join(words)


# ══════════════════════════════════════════════════════════════════════
# Database helpers
# ══════════════════════════════════════════════════════════════════════

def run_sql(query, exit_on_error=False):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = httpx.post(
                API_URL,
                headers={'Authorization': f'Bearer {TOKEN}',
                         'Content-Type': 'application/json'},
                json={'query': query},
                timeout=120
            )
            if resp.status_code == 201:
                return resp.json()
            elif resp.status_code in (429, 500, 503):
                wait = 10 * attempt
                print(f'  Rate limited ({resp.status_code}), waiting {wait}s...')
                time.sleep(wait)
                continue
            else:
                print(f'  SQL ERROR ({resp.status_code}): {resp.text[:500]}')
                if exit_on_error:
                    sys.exit(1)
                if attempt < MAX_RETRIES:
                    time.sleep(5 * attempt)
                    continue
                return None
        except Exception as e:
            print(f'  HTTP error: {e}')
            if attempt < MAX_RETRIES:
                time.sleep(5 * attempt)
                continue
            if exit_on_error:
                sys.exit(1)
            return None
    return None


def esc(s):
    """Escape single quotes for SQL."""
    if s is None:
        return ''
    return str(s).replace("'", "''")


# ══════════════════════════════════════════════════════════════════════
# Phase 1: Create old-era districts and seats
# ══════════════════════════════════════════════════════════════════════

def deduplicate_district_names(names):
    """Remove duplicate district names that differ only in punctuation/word order.

    E.g., 'Suffolk, Essex and Middlesex' and 'Middlesex, Suffolk and Essex'
    would have the same normalized form.
    """
    seen = {}  # normalized → original name (keep first occurrence)
    unique = []
    for name in names:
        norm = normalize_for_matching(name)
        if norm not in seen:
            seen[norm] = name
            unique.append(name)
    return unique


def create_districts_and_seats(ma_state_id, cycle_year, chamber, district_names, dry_run):
    """Create districts and seats for one chamber in one redistricting cycle.

    Returns the number of districts and seats created.
    """
    office_type = 'State Senate' if chamber == 'Senate' else 'State House'
    term_length = 2  # Both chambers have 2-year terms in MA

    # Normalize district names from SoS format to our DB format
    normalized_names = []
    for raw_name in district_names:
        norm = normalize_district_name(raw_name)
        if norm:
            normalized_names.append(norm)

    # Deduplicate (some eras list variants of the same district)
    normalized_names = deduplicate_district_names(normalized_names)

    # Sort alphabetically and assign district numbers
    normalized_names.sort()

    print(f'\n  {chamber} cycle={cycle_year}: {len(normalized_names)} districts')

    # Check for existing districts in this cycle
    existing = run_sql(f"""
        SELECT district_name FROM districts
        WHERE state_id = {ma_state_id} AND chamber = '{chamber}'
          AND redistricting_cycle = '{cycle_year}'
    """)
    existing_names = set()
    if existing:
        existing_names = {r['district_name'] for r in existing}
        print(f'    {len(existing_names)} already exist, will skip those')

    # Filter out already-existing districts
    to_create = [(i+1, name) for i, name in enumerate(normalized_names)
                 if name not in existing_names]

    if not to_create:
        print(f'    All districts already exist — nothing to create')
        return 0, 0

    print(f'    Will create {len(to_create)} districts + {len(to_create)} seats')

    if dry_run:
        for num, name in to_create[:10]:
            print(f'      #{num}: {name}')
        if len(to_create) > 10:
            print(f'      ... and {len(to_create) - 10} more')
        return len(to_create), len(to_create)

    # Insert districts in batches
    dist_values = []
    for num, name in to_create:
        dist_values.append(
            f"({ma_state_id}, 'Legislative', '{chamber}', "
            f"'{num}', '{esc(name)}', "
            f"1, false, '{cycle_year}')"
        )

    inserted_districts = 0
    for batch_start in range(0, len(dist_values), BATCH_SIZE):
        batch = dist_values[batch_start:batch_start + BATCH_SIZE]
        sql = (
            "INSERT INTO districts "
            "(state_id, office_level, chamber, district_number, district_name, "
            "num_seats, is_floterial, redistricting_cycle) "
            "VALUES " + ",\n".join(batch) +
            "\nON CONFLICT DO NOTHING "
            "\nRETURNING id, district_name;"
        )
        result = run_sql(sql)
        if result is not None:
            inserted_districts += len(result)
            print(f'    Districts batch: +{len(result)} (total: {inserted_districts})')
        else:
            print(f'    ERROR: District insert failed!')
            sys.exit(1)
        time.sleep(8)

    # Now load all districts in this cycle to get their IDs for seat creation
    all_cycle_districts = run_sql(f"""
        SELECT id, district_name FROM districts
        WHERE state_id = {ma_state_id} AND chamber = '{chamber}'
          AND redistricting_cycle = '{cycle_year}'
    """)
    if not all_cycle_districts:
        print(f'    ERROR: Could not load inserted districts')
        sys.exit(1)

    time.sleep(8)

    # Check which districts already have seats
    existing_seats = run_sql(f"""
        SELECT d.district_name FROM seats s
        JOIN districts d ON s.district_id = d.id
        WHERE d.state_id = {ma_state_id} AND d.chamber = '{chamber}'
          AND d.redistricting_cycle = '{cycle_year}'
    """)
    districts_with_seats = set()
    if existing_seats:
        districts_with_seats = {r['district_name'] for r in existing_seats}

    time.sleep(8)

    # Create seats for districts that don't have them yet
    seat_values = []
    for d in all_cycle_districts:
        if d['district_name'] in districts_with_seats:
            continue
        label = f"MA {chamber} {d['district_name']}"
        seat_values.append(
            f"({d['id']}, 'Legislative', '{office_type}', "
            f"'{esc(label)}', NULL, {term_length}, NULL)"
        )

    if not seat_values:
        print(f'    All seats already exist')
        return inserted_districts, 0

    inserted_seats = 0
    for batch_start in range(0, len(seat_values), BATCH_SIZE):
        batch = seat_values[batch_start:batch_start + BATCH_SIZE]
        sql = (
            "INSERT INTO seats "
            "(district_id, office_level, office_type, seat_label, "
            "seat_designator, term_length_years, next_regular_election_year) "
            "VALUES " + ",\n".join(batch) +
            "\nON CONFLICT DO NOTHING "
            "\nRETURNING id;"
        )
        result = run_sql(sql)
        if result is not None:
            inserted_seats += len(result)
            print(f'    Seats batch: +{len(result)} (total: {inserted_seats})')
        else:
            print(f'    ERROR: Seat insert failed!')
            sys.exit(1)
        time.sleep(8)

    return inserted_districts, inserted_seats


def phase1_create(ma_state_id, cycle_data, cycles_to_process, dry_run):
    """Phase 1: Create old-era districts and seats for specified cycles."""
    print('\n' + '=' * 60)
    print('PHASE 1: Create old-era districts and seats')
    print('=' * 60)

    total_districts = 0
    total_seats = 0

    for cycle_year, first_year, last_year in CYCLES:
        if cycles_to_process and cycle_year not in cycles_to_process:
            continue

        print(f'\n--- Cycle {cycle_year} (elections {first_year}-{last_year}) ---')

        # Senate — always process (40 single-member districts in all eras)
        senate_key = f'Senate_{cycle_year}'
        if senate_key in cycle_data:
            d, s = create_districts_and_seats(
                ma_state_id, cycle_year, 'Senate',
                cycle_data[senate_key], dry_run
            )
            total_districts += d
            total_seats += s
        else:
            print(f'  No Senate data for cycle {cycle_year}')

        # House — skip 1974 and 1972 (240 multi-member seats)
        if cycle_year in SKIP_HOUSE_CYCLES:
            print(f'  Skipping House for cycle {cycle_year} (240 multi-member seats)')
        else:
            house_key = f'House_{cycle_year}'
            if house_key in cycle_data:
                d, s = create_districts_and_seats(
                    ma_state_id, cycle_year, 'House',
                    cycle_data[house_key], dry_run
                )
                total_districts += d
                total_seats += s
            else:
                print(f'  No House data for cycle {cycle_year}')

    print(f'\nPhase 1 summary: {total_districts} districts, {total_seats} seats')
    if dry_run:
        print('  (DRY RUN — no changes made)')


# ══════════════════════════════════════════════════════════════════════
# Phase 2: Move elections to correct era
# ══════════════════════════════════════════════════════════════════════

def get_cycle_for_year(election_year):
    """Determine which redistricting cycle an election year belongs to."""
    for cycle_year, first_year, last_year in CYCLES:
        if first_year <= election_year <= last_year:
            return cycle_year
    return None  # 2022+ or too old


def phase2_move_elections(ma_state_id, cycles_to_process, dry_run):
    """Phase 2: Move pre-2022 elections from current seats to old-era seats."""
    print('\n' + '=' * 60)
    print('PHASE 2: Move elections to correct redistricting era')
    print('=' * 60)

    for chamber in ['Senate', 'House']:
        office_type = 'State Senate' if chamber == 'Senate' else 'State House'
        print(f'\n--- {chamber} ---')

        # Load current-cycle (2022) seats with district info
        print(f'  Loading current (2022-cycle) {chamber} seats...')
        current_seats = run_sql(f"""
            SELECT s.id as seat_id, d.district_name
            FROM seats s
            JOIN districts d ON s.district_id = d.id
            WHERE d.state_id = {ma_state_id} AND d.chamber = '{chamber}'
              AND d.redistricting_cycle = '2022'
        """, exit_on_error=True)
        print(f'    {len(current_seats)} current seats')

        time.sleep(8)

        # Build lookup: normalized district name → current seat_id
        current_by_norm = {}
        for s in current_seats:
            norm = normalize_for_matching(s['district_name'])
            current_by_norm[norm] = s

        # Load old-era seats for all historical cycles
        print(f'  Loading old-era {chamber} seats...')
        old_seats = run_sql(f"""
            SELECT s.id as seat_id, d.district_name, d.redistricting_cycle
            FROM seats s
            JOIN districts d ON s.district_id = d.id
            WHERE d.state_id = {ma_state_id} AND d.chamber = '{chamber}'
              AND d.redistricting_cycle != '2022'
            ORDER BY d.redistricting_cycle, d.district_name
        """, exit_on_error=True)

        if not old_seats:
            print(f'    No old-era seats found — run Phase 1 first')
            continue

        print(f'    {len(old_seats)} old-era seats')

        time.sleep(8)

        # Build lookup: (cycle, normalized_name) → old seat_id
        old_lookup = {}
        for s in old_seats:
            norm = normalize_for_matching(s['district_name'])
            old_lookup[(s['redistricting_cycle'], norm)] = s['seat_id']

        # Load pre-2022 elections on current-cycle seats
        print(f'  Loading pre-2022 elections on current {chamber} seats...')
        elections = run_sql(f"""
            SELECT e.id as election_id, e.seat_id, e.election_year,
                   e.election_type, d.district_name
            FROM elections e
            JOIN seats s ON e.seat_id = s.id
            JOIN districts d ON s.district_id = d.id
            WHERE d.state_id = {ma_state_id} AND d.chamber = '{chamber}'
              AND d.redistricting_cycle = '2022'
              AND e.election_year < 2022
            ORDER BY e.election_year, d.district_name
        """, exit_on_error=True)

        if not elections:
            print(f'    No pre-2022 elections to migrate')
            continue

        print(f'    {len(elections)} pre-2022 elections to process')

        time.sleep(8)

        # Build migration plan
        migrations = []    # (election_id, new_seat_id)
        unmatched = []     # Elections with no matching old-era seat
        skipped_cycles = []  # Elections in cycles we didn't create (House 1974/1972)

        for e in elections:
            cycle = get_cycle_for_year(e['election_year'])

            if cycle is None:
                # Election year doesn't map to any known cycle (shouldn't happen)
                unmatched.append(e)
                continue

            if cycles_to_process and cycle not in cycles_to_process:
                continue

            # Skip House elections in 1974/1972 cycles (multi-member, not modeled)
            if chamber == 'House' and cycle in SKIP_HOUSE_CYCLES:
                skipped_cycles.append(e)
                continue

            # Find matching old-era seat by normalized district name
            norm_name = normalize_for_matching(e['district_name'])
            old_seat_id = old_lookup.get((cycle, norm_name))

            if old_seat_id:
                migrations.append((e['election_id'], old_seat_id))
            else:
                unmatched.append(e)

        print(f'\n  Migration plan for {chamber}:')
        print(f'    Will migrate: {len(migrations)} elections')
        if skipped_cycles:
            print(f'    Skipped (House 1974/1972): {len(skipped_cycles)} elections')
        if unmatched:
            print(f'    Unmatched (no old-era seat): {len(unmatched)} elections')

        # Report unmatched elections for review
        if unmatched:
            print(f'\n  Unmatched elections (district name not in old era):')
            # Group by district name and cycle for clarity
            by_district = {}
            for e in unmatched:
                cycle = get_cycle_for_year(e['election_year']) or '???'
                key = (cycle, e['district_name'])
                by_district.setdefault(key, []).append(e['election_year'])

            for (cycle, dist_name), years in sorted(by_district.items()):
                years_str = ', '.join(str(y) for y in sorted(set(years)))
                print(f'    cycle={cycle} "{dist_name}": years {years_str}')
                # Show what the normalized form is, and what old-era districts exist
                norm = normalize_for_matching(dist_name)
                avail = [k for k in old_lookup if k[0] == cycle]
                close = [k for k in avail if len(set(norm.split()) & set(k[1].split())) > 1]
                if close:
                    print(f'      Possible matches: {[old_lookup[k] for k in close[:3]]}')

        if dry_run:
            # Show sample
            if migrations:
                print(f'\n  Sample migrations:')
                for eid, new_sid in migrations[:10]:
                    orig = next(e for e in elections if e['election_id'] == eid)
                    cycle = get_cycle_for_year(orig['election_year'])
                    print(f'    Election {eid} ({orig["district_name"]} '
                          f'{orig["election_year"]}) → cycle {cycle} seat {new_sid}')
            continue

        if not migrations:
            print(f'  Nothing to migrate for {chamber}.')
            continue

        # Save backup before migrating
        backup_path = f'/tmp/ma_{chamber.lower()}_pre2022_election_backup.json'
        with open(backup_path, 'w') as f:
            json.dump(elections, f, indent=2)
        print(f'  Backup saved to {backup_path}')

        # Execute migrations in batches using CASE statements
        print(f'\n  Migrating {len(migrations)} {chamber} elections...')
        total_migrated = 0

        for batch_start in range(0, len(migrations), ELECTION_BATCH_SIZE):
            batch = migrations[batch_start:batch_start + ELECTION_BATCH_SIZE]

            case_parts = []
            ids = []
            for eid, new_sid in batch:
                case_parts.append(f"WHEN {eid} THEN {new_sid}")
                ids.append(str(eid))

            sql = (
                f"UPDATE elections SET seat_id = CASE id\n"
                + "\n".join(f"  {p}" for p in case_parts)
                + f"\nEND\nWHERE id IN ({','.join(ids)});"
            )

            result = run_sql(sql)
            if result is not None:
                total_migrated += len(batch)
                print(f'    Batch: +{len(batch)} (total: {total_migrated})')
            else:
                print(f'    ERROR: Migration batch failed at offset {batch_start}!')
                print(f'    {total_migrated} elections already migrated.')
                print(f'    Use backup at {backup_path} to rollback if needed.')
                sys.exit(1)
            time.sleep(8)

        print(f'  Done! Migrated {total_migrated} {chamber} elections.')

        # Verify
        time.sleep(8)
        print(f'\n  Verifying {chamber}...')
        check = run_sql(f"""
            SELECT d.redistricting_cycle, COUNT(*) as cnt
            FROM elections e
            JOIN seats s ON e.seat_id = s.id
            JOIN districts d ON s.district_id = d.id
            WHERE d.state_id = {ma_state_id} AND d.chamber = '{chamber}'
            GROUP BY d.redistricting_cycle
            ORDER BY d.redistricting_cycle
        """)
        if check:
            for r in check:
                print(f"    cycle={r['redistricting_cycle']}: {r['cnt']} elections")

    if dry_run:
        print('\n  (DRY RUN — no changes made)')


# ══════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='Create MA historical redistricting-era districts/seats and migrate elections'
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview only, no DB changes')
    parser.add_argument('--cycle', type=str, action='append',
                        help='Only process specific cycle(s), e.g. --cycle 2012 --cycle 2002')
    parser.add_argument('--phase', type=int, choices=[1, 2],
                        help='Run only a specific phase (1=create districts/seats, 2=move elections)')
    args = parser.parse_args()

    cycles_to_process = set(args.cycle) if args.cycle else None

    # Validate cycle values
    valid_cycles = {c[0] for c in CYCLES}
    if cycles_to_process:
        invalid = cycles_to_process - valid_cycles
        if invalid:
            print(f'ERROR: Invalid cycle(s): {invalid}')
            print(f'Valid cycles: {sorted(valid_cycles)}')
            sys.exit(1)

    # Load cycle district data
    json_path = '/tmp/ma_cycle_districts.json'
    if not os.path.exists(json_path):
        print(f'ERROR: {json_path} not found.')
        print('This file should contain the mapping of cycle → district names from the SoS data.')
        sys.exit(1)

    with open(json_path) as f:
        cycle_data = json.load(f)

    print(f'Loaded {len(cycle_data)} cycle/chamber groups from {json_path}')
    for key in sorted(cycle_data.keys()):
        print(f'  {key}: {len(cycle_data[key])} districts')

    # Get MA state_id
    ma_state = run_sql("SELECT id FROM states WHERE abbreviation = 'MA'", exit_on_error=True)
    ma_state_id = ma_state[0]['id']
    print(f'MA state_id: {ma_state_id}')

    # Run phases
    run_phase1 = args.phase is None or args.phase == 1
    run_phase2 = args.phase is None or args.phase == 2

    if run_phase1:
        phase1_create(ma_state_id, cycle_data, cycles_to_process, args.dry_run)

    if run_phase2:
        phase2_move_elections(ma_state_id, cycles_to_process, args.dry_run)

    print('\n' + '=' * 60)
    print('All done!')
    if args.dry_run:
        print('(DRY RUN — no changes were made)')
    print('=' * 60)


if __name__ == '__main__':
    main()
