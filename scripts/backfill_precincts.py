#!/usr/bin/env python3
"""
Backfill precincts_reporting and precincts_total for NC and AR 2026 primaries.

Uses:
  - /tmp/nc_primary_races.json (already has per-race precinct data)
  - elections/tmp/AR_2026_Preferential_Primary_State Senate.csv
  - elections/tmp/AR_2026_Preferential_Primary_State Representative.csv

TX is skipped — the raw data files only have statewide precinct totals,
not per-district counts.

Usage:
    python3 scripts/backfill_precincts.py               # Backfill NC + AR
    python3 scripts/backfill_precincts.py --dry-run      # Show SQL without executing
"""

import sys
import os
import json
import csv
import time
import argparse
from collections import defaultdict

import httpx
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF

ELECTIONS_TMP = os.path.join(os.path.dirname(__file__), '..', 'tmp')


def run_sql(query, retries=5):
    for attempt in range(retries):
        resp = httpx.post(
            f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429 and attempt < retries - 1:
            wait = 10 * (attempt + 1)
            print(f'  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        sys.exit(1)


def load_nc_data():
    """Load NC precinct data from processed primary races JSON."""
    path = '/tmp/nc_primary_races.json'
    if not os.path.exists(path):
        print(f'  NC data not found at {path}, skipping')
        return []

    with open(path) as f:
        races = json.load(f)

    results = []
    for race in races:
        pr = race.get('precincts_reporting')
        pt = race.get('precincts_total')
        if pr is None or pt is None:
            continue
        chamber = race['chamber']
        district = race['district']
        etype = race['election_type']
        results.append({
            'state': 'NC',
            'chamber': chamber,
            'district': district,
            'election_type': etype,
            'precincts_reporting': pr,
            'precincts_total': pt,
        })
    return results


def load_ar_data():
    """Load AR precinct data from CSV files, aggregating county rows per contest."""
    results = []

    for filename, chamber in [
        ('AR_2026_Preferential_Primary_State Senate.csv', 'Senate'),
        ('AR_2026_Preferential_Primary_State Representative.csv', 'House'),
    ]:
        path = os.path.join(ELECTIONS_TMP, filename)
        if not os.path.exists(path):
            print(f'  AR data not found at {path}, skipping')
            continue

        # Group by Contest ID, summing precincts across counties
        contests = defaultdict(lambda: {
            'name': '', 'total_precincts': 0, 'precincts_reporting': 0,
        })

        with open(path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                cid = row['Contest ID']
                contests[cid]['name'] = row['Contest Name']
                contests[cid]['total_precincts'] += int(row['Total Precincts'])
                contests[cid]['precincts_reporting'] += int(row['Precincts Reporting'])

        for cid, data in contests.items():
            name = data['name']  # e.g., "REP State Senate District 10"
            # Parse party and district from contest name
            if name.startswith('REP '):
                party = 'R'
                rest = name[4:]
            elif name.startswith('DEM '):
                party = 'D'
                rest = name[4:]
            else:
                continue

            # Extract district number
            # "State Senate District 10" or "State Representative Dist. 01"
            parts = rest.split()
            dist_num = parts[-1].lstrip('0') or '0'

            etype = f'Primary_{party}'
            results.append({
                'state': 'AR',
                'chamber': chamber,
                'district': dist_num,
                'election_type': etype,
                'precincts_reporting': data['precincts_reporting'],
                'precincts_total': data['total_precincts'],
            })

    return results


def backfill(dry_run=False):
    nc_data = load_nc_data()
    ar_data = load_ar_data()
    all_data = nc_data + ar_data

    print(f'  NC: {len(nc_data)} races with precinct data')
    print(f'  AR: {len(ar_data)} races with precinct data')

    if not all_data:
        print('  No data to backfill.')
        return

    # Fetch election IDs for NC and AR 2026 primaries
    elections = run_sql("""
        SELECT e.id, e.election_type, e.total_votes_cast,
               st.abbreviation as state,
               d.chamber,
               d.district_number
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE e.election_year = 2026
          AND e.election_type LIKE 'Primary%'
          AND st.abbreviation IN ('NC', 'AR')
          AND s.office_level = 'Legislative'
          AND COALESCE(d.redistricting_cycle, '2022') = '2022'
    """)

    # Index by (state, chamber, district, election_type)
    elec_map = {}
    for e in elections:
        key = (e['state'], e['chamber'], e['district_number'], e['election_type'])
        elec_map[key] = e

    # Build UPDATE statements
    updates = []
    matched = 0
    unmatched = []

    for race in all_data:
        # Map House chamber names
        chamber_db = race['chamber']
        if race['state'] == 'AR' and chamber_db == 'House':
            chamber_db = 'House'  # AR uses "House" in DB

        key = (race['state'], chamber_db, race['district'], race['election_type'])
        elec = elec_map.get(key)

        if not elec:
            unmatched.append(key)
            continue

        matched += 1
        updates.append(
            f"UPDATE elections SET precincts_reporting = {race['precincts_reporting']}, "
            f"precincts_total = {race['precincts_total']} "
            f"WHERE id = {elec['id']}"
        )

    print(f'\n  Matched: {matched} elections')
    if unmatched:
        print(f'  Unmatched: {len(unmatched)} races (no DB election found)')
        for u in unmatched[:5]:
            print(f'    {u}')
        if len(unmatched) > 5:
            print(f'    ... and {len(unmatched) - 5} more')

    if dry_run:
        print(f'\n  Would execute {len(updates)} UPDATE statements')
        for u in updates[:5]:
            print(f'    {u}')
        if len(updates) > 5:
            print(f'    ... and {len(updates) - 5} more')
        return

    if not updates:
        print('  No updates to execute.')
        return

    # Batch updates into a single SQL statement
    batch_size = 50
    total_updated = 0
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        sql = ';\n'.join(batch) + ';'
        run_sql(sql)
        total_updated += len(batch)
        print(f'    Updated {total_updated}/{len(updates)}...')

    print(f'\n  Done! Updated {total_updated} elections with precinct data.')


def main():
    parser = argparse.ArgumentParser(description='Backfill precinct reporting data')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    print('Backfilling precinct reporting data for NC and AR 2026 primaries...')
    backfill(dry_run=args.dry_run)
    print('\nDone.')


if __name__ == '__main__':
    main()
