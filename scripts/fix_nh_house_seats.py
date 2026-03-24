#!/usr/bin/env python3
"""
Fix NH House district seat counts for the 2022 redistricting cycle.

The populate script used pre-redistricting (2012) seat counts when creating
2022-cycle districts. This script corrects num_seats, adds/removes seat records,
and cleans up linked elections/candidacies. Seat_terms must be repopulated
separately after this script runs.

Uses the 2024 NH SoS Excel files (in tmp/) as the source of truth.

Usage:
    python3 scripts/fix_nh_house_seats.py --dry-run   # Preview changes
    python3 scripts/fix_nh_house_seats.py              # Execute
"""

import os
import re
import sys
import glob
import time
import argparse
from collections import defaultdict

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF

TMP_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'tmp')
BATCH_SIZE = 50
DESIGNATORS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

NH_COUNTIES = [
    'belknap', 'carroll', 'cheshire', 'coos', 'grafton',
    'hillsborough', 'merrimack', 'rockingham', 'strafford', 'sullivan',
]

try:
    import xlrd
except ImportError:
    xlrd = None

try:
    import openpyxl
except ImportError:
    openpyxl = None


# ═══════════════════════════════════════════════════════════════
# DB HELPERS
# ═══════════════════════════════════════════════════════════════

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
        except (httpx.ConnectError, httpx.ReadError, httpx.WriteError) as e:
            wait = 5 * (attempt + 1)
            print(f'  Connection error: {e}, retrying in {wait}s...')
            time.sleep(wait)
            continue
    return None


def esc(s):
    if s is None:
        return 'NULL'
    return "'" + str(s).replace("'", "''") + "'"


# ═══════════════════════════════════════════════════════════════
# SoS EXCEL PARSING (from audit_nh_seats.py)
# ═══════════════════════════════════════════════════════════════

def read_excel(filepath):
    if filepath.endswith('.xls'):
        wb = xlrd.open_workbook(filepath)
        ws = wb.sheet_by_index(0)
        return [[ws.cell_value(r, c) for c in range(ws.ncols)] for r in range(ws.nrows)]
    else:
        wb = openpyxl.load_workbook(filepath, data_only=True)
        ws = wb.active
        return [list(row) for row in ws.iter_rows(values_only=True)]


def get_sos_seat_counts():
    """Parse 2024 SoS Excel files to get correct seat counts per district."""
    regular = {}
    floterial = {}
    for county in NH_COUNTIES:
        pattern = os.path.join(TMP_DIR, f'2024-ge-house-{county}*')
        matches = glob.glob(pattern)
        if not matches:
            print(f"  WARNING: No 2024 SoS file for {county}")
            continue
        rows = read_excel(matches[0])
        for row in rows:
            col0 = str(row[0]).strip() if row[0] is not None else ''
            if 'RECOUNT' in col0.upper():
                continue
            m = re.match(
                r'District(?:\s+No\.?)?\s*(\d+)\s*\((\d+)\)\s*(FL|F)?',
                col0, re.IGNORECASE
            )
            if m:
                dist_name = f"{county.title()}-{m.group(1)}"
                num_seats = int(m.group(2))
                if m.group(3):
                    floterial[dist_name] = num_seats
                else:
                    regular[dist_name] = num_seats
    return regular, floterial


# ═══════════════════════════════════════════════════════════════
# PHASE 0: AUDIT
# ═══════════════════════════════════════════════════════════════

def audit(sos_regular):
    """Compare SoS seat counts against DB. Returns (shrink, grow, floterial_fixes)."""
    db_districts = run_sql("""
        SELECT d.id, d.district_name, d.num_seats, d.is_floterial
        FROM districts d
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NH' AND d.chamber = 'House'
          AND d.redistricting_cycle = '2022'
        ORDER BY d.district_name
    """)

    db_lookup = {}
    for d in db_districts:
        db_lookup[d['district_name']] = d

    shrink = []  # (district_id, district_name, db_seats, correct_seats)
    grow = []    # (district_id, district_name, db_seats, correct_seats)
    floterial_fixes = []

    for dist_name, correct in sorted(sos_regular.items()):
        d = db_lookup.get(dist_name)
        if not d:
            continue
        if d['is_floterial']:
            # This district should be non-floterial per SoS but is marked floterial in DB
            continue
        if d['num_seats'] > correct:
            shrink.append((d['id'], dist_name, d['num_seats'], correct))
        elif d['num_seats'] < correct:
            grow.append((d['id'], dist_name, d['num_seats'], correct))

    # Check for non-floterial districts in DB that are actually floterial in SoS
    for dist_name, d in db_lookup.items():
        if not d['is_floterial'] and dist_name not in sos_regular:
            floterial_fixes.append((d['id'], dist_name))

    return shrink, grow, floterial_fixes


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Fix NH House 2022-cycle seat counts')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes only')
    args = parser.parse_args()

    print('=' * 60)
    print('NH House Seat Count Fix — 2022 Redistricting Cycle')
    print('=' * 60)

    # ── Phase 0: Audit ──
    print('\nPhase 0: Parsing SoS files and auditing DB...')
    sos_regular, sos_floterial = get_sos_seat_counts()
    sos_total = sum(sos_regular.values()) + sum(sos_floterial.values())
    print(f'  SoS: {len(sos_regular)} regular + {len(sos_floterial)} floterial = '
          f'{len(sos_regular) + len(sos_floterial)} districts, {sos_total} seats')

    shrink, grow, floterial_fixes = audit(sos_regular)

    total_excess = sum(db - correct for _, _, db, correct in shrink)
    total_needed = sum(correct - db for _, _, db, correct in grow)

    print(f'\n  Districts to SHRINK ({len(shrink)}, removing {total_excess} seats):')
    for did, name, db, correct in shrink:
        print(f'    {name}: {db} → {correct} (remove {db - correct})')

    print(f'\n  Districts to GROW ({len(grow)}, adding {total_needed} seats):')
    for did, name, db, correct in grow:
        print(f'    {name}: {db} → {correct} (add {correct - db})')

    if floterial_fixes:
        print(f'\n  Floterial flag fixes ({len(floterial_fixes)}):')
        for did, name in floterial_fixes:
            print(f'    {name} (id={did}): is_floterial → true')

    # Current vs target totals
    db_total = run_sql("""
        SELECT SUM(d.num_seats) as total FROM districts d
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NH' AND d.chamber = 'House'
          AND d.redistricting_cycle = '2022'
    """)
    current_total = db_total[0]['total'] if db_total else '?'
    print(f'\n  Current DB total: {current_total} seats')
    print(f'  Target total: {sos_total} seats (should be 400)')
    print(f'  Net change: {total_needed - total_excess:+d}')

    if not shrink and not grow and not floterial_fixes:
        print('\n  All seat counts already correct!')
        return

    if args.dry_run:
        print('\nDRY RUN — no changes made.')
        return

    # ── Phase 1: Fix floterial flags ──
    if floterial_fixes:
        print('\n\nPhase 1: Fixing is_floterial flags...')
        for did, name in floterial_fixes:
            run_sql(f"UPDATE districts SET is_floterial = true WHERE id = {did}")
            print(f'  {name} → is_floterial = true')
    else:
        print('\n\nPhase 1: No floterial flag fixes needed.')

    # ── Phase 2: Clear seat_terms ──
    print('\nPhase 2: Clearing NH House seat_terms...')

    # Only clear seat_terms for affected districts
    affected_ids = [did for did, _, _, _ in shrink] + [did for did, _, _, _ in grow]
    if affected_ids:
        id_list = ','.join(str(i) for i in affected_ids)

        # Clear cache columns
        run_sql(f"""
            UPDATE seats SET current_holder = NULL, current_holder_party = NULL,
                current_holder_caucus = NULL
            WHERE district_id IN ({id_list})
        """)

        # Delete seat_terms
        result = run_sql(f"""
            DELETE FROM seat_terms WHERE seat_id IN (
                SELECT id FROM seats WHERE district_id IN ({id_list})
            )
        """)
        print(f'  Cleared seat_terms for {len(affected_ids)} affected districts')
    time.sleep(1)

    # ── Phase 3: Shrink districts ──
    if shrink:
        print(f'\nPhase 3: Shrinking {len(shrink)} districts...')
        for did, name, db_seats, correct_seats in shrink:
            # Get all seats sorted by designator
            seats = run_sql(f"""
                SELECT id, seat_designator, seat_label FROM seats
                WHERE district_id = {did}
                ORDER BY seat_designator NULLS FIRST
            """)
            if not seats:
                print(f'  WARNING: No seats found for {name}')
                continue

            # Keep first N seats, delete the rest
            keep = seats[:correct_seats]
            delete = seats[correct_seats:]
            delete_ids = [s['id'] for s in delete]

            if delete_ids:
                id_list = ','.join(str(i) for i in delete_ids)
                # CASCADE will handle elections + candidacies
                run_sql(f"DELETE FROM seats WHERE id IN ({id_list})")

            # Update num_seats
            run_sql(f"UPDATE districts SET num_seats = {correct_seats} WHERE id = {did}")
            print(f'  {name}: {db_seats} → {correct_seats} (deleted {len(delete_ids)} seats)')
            time.sleep(1)
    else:
        print('\nPhase 3: No districts to shrink.')

    # ── Phase 4: Grow districts ──
    if grow:
        print(f'\nPhase 4: Growing {len(grow)} districts...')
        for did, name, db_seats, correct_seats in grow:
            # Get existing Seat A to copy properties
            seat_a = run_sql(f"""
                SELECT id, office_level, office_type, term_length_years,
                       election_class, next_regular_election_year
                FROM seats WHERE district_id = {did}
                ORDER BY seat_designator NULLS FIRST
                LIMIT 1
            """)
            if not seat_a:
                print(f'  WARNING: No existing seat for {name}')
                continue
            a = seat_a[0]
            seat_a_id = a['id']

            # Create new seats
            new_seats_needed = correct_seats - db_seats
            new_seat_ids = []
            for i in range(db_seats, correct_seats):
                desig = DESIGNATORS[i]
                label = f"NH House {name} Seat {desig}"
                ec = esc(a['election_class'])
                nrey = a['next_regular_election_year'] or 2026

                result = run_sql(f"""
                    INSERT INTO seats (district_id, office_level, office_type,
                        seat_label, seat_designator, term_length_years,
                        election_class, next_regular_election_year)
                    VALUES ({did}, 'Legislative', {esc(a['office_type'])},
                        {esc(label)}, '{desig}', {a['term_length_years']},
                        {ec}, {nrey})
                    RETURNING id
                """)
                if result:
                    new_seat_ids.append(result[0]['id'])

            print(f'  {name}: created {len(new_seat_ids)} new seats')

            # Get Seat A elections to copy
            elections_a = run_sql(f"""
                SELECT id, election_date, election_year, election_type,
                       result_status, total_votes_cast
                FROM elections WHERE seat_id = {seat_a_id}
                ORDER BY election_date
            """)

            if elections_a and new_seat_ids:
                total_elections = 0
                total_candidacies = 0
                for new_sid in new_seat_ids:
                    for ea in elections_a:
                        # Create matching election
                        rs = esc(ea['result_status'])
                        tvc = ea['total_votes_cast'] if ea['total_votes_cast'] is not None else 'NULL'
                        new_e = run_sql(f"""
                            INSERT INTO elections (seat_id, election_date, election_year,
                                election_type, result_status, total_votes_cast)
                            VALUES ({new_sid}, '{ea['election_date']}', {ea['election_year']},
                                {esc(ea['election_type'])}, {rs}, {tvc})
                            RETURNING id
                        """)
                        if not new_e:
                            continue
                        new_eid = new_e[0]['id']
                        total_elections += 1

                        # Copy candidacies from Seat A election
                        cands = run_sql(f"""
                            SELECT candidate_id, party, caucus, candidate_status,
                                   is_incumbent, is_write_in, votes_received,
                                   vote_percentage, result, is_major
                            FROM candidacies WHERE election_id = {ea['id']}
                        """)
                        if cands:
                            values = []
                            for c in cands:
                                values.append(
                                    f"({new_eid}, {c['candidate_id']}, "
                                    f"{esc(c['party'])}, {esc(c['caucus'])}, "
                                    f"{esc(c['candidate_status'])}, "
                                    f"{c['is_incumbent'] if c['is_incumbent'] is not None else 'false'}, "
                                    f"{c['is_write_in'] if c['is_write_in'] is not None else 'false'}, "
                                    f"{c['votes_received'] if c['votes_received'] is not None else 'NULL'}, "
                                    f"{c['vote_percentage'] if c['vote_percentage'] is not None else 'NULL'}, "
                                    f"{esc(c['result'])}, "
                                    f"{c['is_major'] if c['is_major'] is not None else 'NULL'})"
                                )
                            # Batch insert
                            for batch_start in range(0, len(values), BATCH_SIZE):
                                batch = values[batch_start:batch_start + BATCH_SIZE]
                                run_sql(
                                    "INSERT INTO candidacies "
                                    "(election_id, candidate_id, party, caucus, "
                                    "candidate_status, is_incumbent, is_write_in, "
                                    "votes_received, vote_percentage, result, is_major) "
                                    "VALUES\n" + ",\n".join(batch)
                                )
                                total_candidacies += len(batch)
                    time.sleep(1)

                print(f'    Copied {total_elections} elections, {total_candidacies} candidacies')

            # Update num_seats
            run_sql(f"UPDATE districts SET num_seats = {correct_seats} WHERE id = {did}")
            time.sleep(1)
    else:
        print('\nPhase 4: No districts to grow.')

    # ── Phase 5: Fix seat labels ──
    print('\nPhase 5: Fixing seat labels...')
    label_fixes = 0

    # Districts that went from multi to single: remove "Seat A"
    for did, name, db_seats, correct_seats in shrink:
        if correct_seats == 1 and db_seats > 1:
            plain_label = f"NH House {name}"
            run_sql(f"""
                UPDATE seats SET seat_label = {esc(plain_label)},
                    seat_designator = NULL
                WHERE district_id = {did}
            """)
            label_fixes += 1
            print(f'  {name}: "Seat A" → plain label')

    # Districts that went from single to multi: add "Seat A"
    for did, name, db_seats, correct_seats in grow:
        if db_seats == 1 and correct_seats > 1:
            seat_a_label = f"NH House {name} Seat A"
            run_sql(f"""
                UPDATE seats SET seat_label = {esc(seat_a_label)},
                    seat_designator = 'A'
                WHERE district_id = {did} AND seat_designator IS NULL
            """)
            label_fixes += 1
            print(f'  {name}: plain label → "Seat A"')

    if label_fixes == 0:
        print('  No label fixes needed.')

    # ── Phase 7: Verification ──
    print('\n\nPhase 7: Verification...')

    # Total seat count
    total = run_sql("""
        SELECT SUM(d.num_seats) as total,
               COUNT(DISTINCT d.id) as districts,
               (SELECT COUNT(*) FROM seats s
                JOIN districts d2 ON s.district_id = d2.id
                JOIN states st2 ON d2.state_id = st2.id
                WHERE st2.abbreviation = 'NH' AND d2.chamber = 'House'
                  AND d2.redistricting_cycle = '2022') as seat_records
        FROM districts d
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NH' AND d.chamber = 'House'
          AND d.redistricting_cycle = '2022'
    """)
    if total:
        t = total[0]
        print(f'  Districts: {t["districts"]}')
        print(f'  num_seats total: {t["total"]}')
        print(f'  Seat records: {t["seat_records"]}')
        match = '✓' if t['total'] == t['seat_records'] else '✗ MISMATCH'
        print(f'  num_seats == seat_records: {match}')
        target = '✓' if t['total'] == 400 else f'✗ (expected 400)'
        print(f'  Total == 400: {target}')

    # Per-county breakdown
    county_totals = run_sql("""
        SELECT
            SPLIT_PART(d.district_name, '-', 1) as county,
            SUM(d.num_seats) as seats,
            COUNT(*) as districts
        FROM districts d
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NH' AND d.chamber = 'House'
          AND d.redistricting_cycle = '2022'
          AND d.is_floterial = false
        GROUP BY SPLIT_PART(d.district_name, '-', 1)
        ORDER BY 1
    """)
    if county_totals:
        print('\n  Regular districts by county:')
        for c in county_totals:
            print(f"    {c['county']}: {c['districts']} districts, {c['seats']} seats")

    print('\n' + '=' * 60)
    print('NEXT STEPS:')
    print('  1. Download BP page:')
    print('     curl -L "https://ballotpedia.org/New_Hampshire_House_of_Representatives" \\')
    print('       -o /tmp/nh_house_bp.html')
    print('  2. Re-run seat_terms:')
    print('     python3 scripts/populate_seat_terms_nh_house.py')
    print('  3. Fix multi-seat candidacies:')
    print('     python3 scripts/fix_nh_multiseat_candidacies.py')
    print('  4. Re-export NH:')
    print('     python3 scripts/export_site_data.py --state NH')
    print('     python3 scripts/export_district_data.py --state NH')
    print('=' * 60)


if __name__ == '__main__':
    main()
