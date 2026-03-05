#!/usr/bin/env python3
"""
Fix General election dates across all states.

Many Ballotpedia imports recorded "General" elections with non-November dates
(filing deadlines, primary dates, convention dates). This script:
  1. Finds all General elections with dates outside Oct/Nov (since 2010)
  2. Identifies duplicates (wrong-date General + matching November General for same seat/year)
  3. Deletes duplicates (candidacies cascade via FK)
  4. Fixes dates on non-duplicates to the correct November Election Day

Usage:
    python3 scripts/fix_general_election_dates.py --dry-run
    python3 scripts/fix_general_election_dates.py --state CT --dry-run
    python3 scripts/fix_general_election_dates.py --state CT
    python3 scripts/fix_general_election_dates.py
"""

import sys
import os
import time
import argparse
from collections import defaultdict

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF

# US Election Day: Tuesday after the first Monday in November
NOVEMBER_ELECTION_DATES = {
    2010: '2010-11-02',
    2011: '2011-11-08',
    2012: '2012-11-06',
    2013: '2013-11-05',
    2014: '2014-11-04',
    2015: '2015-11-03',
    2016: '2016-11-08',
    2017: '2017-11-07',
    2018: '2018-11-06',
    2019: '2019-11-05',
    2020: '2020-11-03',
    2021: '2021-11-02',
    2022: '2022-11-08',
    2023: '2023-11-07',
    2024: '2024-11-05',
}


# ══════════════════════════════════════════════════════════════════════
# DB HELPERS
# ══════════════════════════════════════════════════════════════════════

def run_sql(query, max_retries=5):
    for attempt in range(max_retries):
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
        sys.exit(1)
    print(f'SQL FAILED after {max_retries} retries')
    sys.exit(1)


def run_sql_batch(statements, dry_run=False):
    """Execute a list of SQL statements in batches."""
    if not statements or dry_run:
        return
    batch_size = 30
    for i in range(0, len(statements), batch_size):
        batch = statements[i:i + batch_size]
        combined = 'BEGIN;\n' + '\n'.join(batch) + '\nCOMMIT;'
        run_sql(combined)
        if i + batch_size < len(statements):
            time.sleep(1)


# ══════════════════════════════════════════════════════════════════════
# MAIN LOGIC
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Fix General election dates')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without executing')
    parser.add_argument('--state', type=str, help='Filter to a single state (e.g., CT)')
    args = parser.parse_args()

    print('=== Fix General Election Dates ===\n')

    # Step 1: Load all affected elections (General with non-Oct/Nov date)
    state_filter = ''
    if args.state:
        state_filter = f"AND s.abbreviation = '{args.state.upper()}'"

    print('Loading affected elections...')
    affected = run_sql(f"""
        SELECT e.id, e.seat_id, e.election_year, e.election_date::text,
               e.total_votes_cast,
               s.abbreviation AS state, st.office_type, d.district_number
        FROM elections e
        JOIN seats st ON e.seat_id = st.id
        JOIN districts d ON st.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE e.election_type = 'General'
          AND e.election_date IS NOT NULL
          AND EXTRACT(MONTH FROM e.election_date) NOT IN (10, 11)
          AND e.election_date >= '2010-01-01'
          {state_filter}
        ORDER BY s.abbreviation, e.election_year, st.office_type, d.district_number;
    """)
    print(f'  {len(affected)} found\n')

    if not affected:
        print('No affected elections found. Nothing to do.')
        return

    # Step 2: Load November generals to identify duplicates
    print('Loading November generals for duplicate detection...')
    november_generals = run_sql(f"""
        SELECT e.id, e.seat_id, e.election_year
        FROM elections e
        JOIN seats st ON e.seat_id = st.id
        JOIN districts d ON st.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE e.election_type = 'General'
          AND e.election_date IS NOT NULL
          AND EXTRACT(MONTH FROM e.election_date) IN (10, 11)
          AND e.election_year >= 2010
          {state_filter};
    """)
    # Build set of (seat_id, election_year) that have a November general
    nov_index = set()
    for row in november_generals:
        nov_index.add((row['seat_id'], row['election_year']))
    print(f'  {len(november_generals)} November generals indexed\n')

    # Step 3: Classify and process
    duplicates = []       # elections to DELETE (have Nov counterpart)
    date_fixes = []       # elections to UPDATE date

    # Group by state for reporting
    by_state = defaultdict(lambda: {'delete': [], 'fix': []})

    for elec in affected:
        key = (elec['seat_id'], elec['election_year'])
        state = elec['state']

        if key in nov_index:
            duplicates.append(elec)
            by_state[state]['delete'].append(elec)
        else:
            date_fixes.append(elec)
            by_state[state]['fix'].append(elec)

    # Print per-state summary
    affected_states = sorted(by_state.keys())
    for state in affected_states:
        info = by_state[state]
        print(f'--- {state} ---')
        if info['delete']:
            print(f'  DELETE: {len(info["delete"])} duplicates (have Nov counterpart)')
        if info['fix']:
            # Group fixes by target year
            by_year = defaultdict(int)
            for e in info['fix']:
                by_year[e['election_year']] += 1
            year_parts = ', '.join(
                f'{NOVEMBER_ELECTION_DATES.get(y, f"{y}-11-??")}: {c}'
                for y, c in sorted(by_year.items())
            )
            print(f'  FIX DATE: {len(info["fix"])} elections → {year_parts}')
        print()

    print(f'TOTAL: {len(duplicates)} deletions, {len(date_fixes)} date fixes')
    print()

    if args.dry_run:
        print('DRY RUN — no changes written')
        return

    # Step 4: Execute deletions
    if duplicates:
        print(f'Deleting {len(duplicates)} duplicate elections...')
        delete_ids = [str(e['id']) for e in duplicates]
        # Batch deletions (30 IDs per statement is fine since DELETE ... IN (...) is one statement)
        batch_size = 100
        for i in range(0, len(delete_ids), batch_size):
            batch = delete_ids[i:i + batch_size]
            ids_str = ','.join(batch)
            run_sql(f'DELETE FROM elections WHERE id IN ({ids_str});')
            if i + batch_size < len(delete_ids):
                time.sleep(1)
        print(f'  Done.\n')

    # Step 5: Execute date fixes
    if date_fixes:
        print(f'Fixing dates on {len(date_fixes)} elections...')
        update_stmts = []
        skipped = 0
        for elec in date_fixes:
            year = elec['election_year']
            nov_date = NOVEMBER_ELECTION_DATES.get(year)
            if not nov_date:
                print(f'  WARNING: No November date mapping for year {year} '
                      f'(election {elec["id"]}, {elec["state"]}), skipping')
                skipped += 1
                continue
            update_stmts.append(
                f"UPDATE elections SET election_date = '{nov_date}' WHERE id = {elec['id']};"
            )
        run_sql_batch(update_stmts)
        print(f'  Done. ({len(update_stmts)} updated, {skipped} skipped)\n')

    # Print affected states for re-export
    print(f'Affected states ({len(affected_states)}): {" ".join(affected_states)}')
    print('Re-export these states with:')
    print(f'  for st in {" ".join(affected_states)}; do')
    print(f'    python3 scripts/export_district_data.py --state $st')
    print(f'    python3 scripts/export_site_data.py --state $st')
    print(f'  done')


if __name__ == '__main__':
    main()
