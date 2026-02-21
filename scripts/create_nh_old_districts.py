#!/usr/bin/env python3
"""
Create NH House 2012-cycle (old-era) district and seat records.

Reads the district list extracted from 2020 SoS files and inserts:
- District records with redistricting_cycle='2012'
- Seat records (A, B, C, ... based on num_seats)

Usage:
    python3 scripts/create_nh_old_districts.py --dry-run   # Preview only
    python3 scripts/create_nh_old_districts.py              # Insert into DB
"""
import os
import sys
import json
import time
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

import httpx

MAX_RETRIES = 5
BATCH_SIZE = 50  # Smaller batches for complex INSERTs


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
            elif resp.status_code == 429:
                wait = 5 * attempt
                print(f'  Rate limited (429), waiting {wait}s...')
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
    if s is None:
        return ''
    return str(s).replace("'", "''")


def main():
    parser = argparse.ArgumentParser(description='Create NH 2012-cycle districts and seats')
    parser.add_argument('--dry-run', action='store_true', help='Preview only, no DB changes')
    args = parser.parse_args()

    # Load the extracted district list
    json_path = '/tmp/nh_2012_districts.json'
    if not os.path.exists(json_path):
        print(f'ERROR: {json_path} not found. Run extract_nh_2012_districts.py --json first.')
        sys.exit(1)

    with open(json_path) as f:
        data = json.load(f)

    old_districts = data['old_districts']
    print(f'Loaded {len(old_districts)} old-era districts from {json_path}')

    # Get NH state_id
    nh_state = run_sql("SELECT id FROM states WHERE abbreviation = 'NH'", exit_on_error=True)
    nh_state_id = nh_state[0]['id']
    print(f'NH state_id: {nh_state_id}')

    # Check for existing 2012-cycle districts (idempotency)
    existing = run_sql(f"""
        SELECT district_number FROM districts
        WHERE state_id = {nh_state_id} AND chamber = 'House'
          AND redistricting_cycle = '2012'
    """)
    if existing:
        print(f'WARNING: {len(existing)} districts with cycle=2012 already exist!')
        existing_nums = set(r['district_number'] for r in existing)
        old_districts = [d for d in old_districts if d['district_number'] not in existing_nums]
        print(f'  Will insert {len(old_districts)} remaining districts')
        if not old_districts:
            print('  Nothing to do.')
            return

    total_seats = sum(d['num_seats'] for d in old_districts)
    print(f'\nWill create:')
    print(f'  {len(old_districts)} districts')
    print(f'  {total_seats} seats')

    if args.dry_run:
        print('\nDRY RUN — sample districts:')
        for d in sorted(old_districts, key=lambda x: x['district_number'])[:15]:
            fl = ' FL' if d['is_floterial'] else ''
            desigs = ', '.join(chr(65 + i) for i in range(d['num_seats']))
            print(f"  {d['district_number']}: {d['num_seats']} seats ({desigs}){fl}")
        if len(old_districts) > 15:
            print(f'  ... and {len(old_districts) - 15} more')
        return

    # Insert districts in batches
    print(f'\nInserting {len(old_districts)} districts...')
    dist_values = []
    for d in old_districts:
        fl = 'true' if d['is_floterial'] else 'false'
        dist_values.append(
            f"({nh_state_id}, 'Legislative', 'House', "
            f"'{esc(d['district_number'])}', NULL, "
            f"{d['num_seats']}, {fl}, '2012')"
        )

    inserted_districts = 0
    for batch_start in range(0, len(dist_values), BATCH_SIZE):
        batch = dist_values[batch_start:batch_start + BATCH_SIZE]
        sql = (
            "INSERT INTO districts "
            "(state_id, office_level, chamber, district_number, district_name, "
            "num_seats, is_floterial, redistricting_cycle) "
            "VALUES " + ",\n".join(batch) +
            "\nRETURNING id, district_number, num_seats, is_floterial;"
        )
        result = run_sql(sql)
        if result:
            inserted_districts += len(result)
            print(f'  Batch: +{len(result)} districts (total: {inserted_districts})')
        else:
            print('  ERROR: District insert failed!')
            sys.exit(1)
        time.sleep(2)

    # Now load all 2012-cycle districts to get their IDs
    print(f'\nLoading inserted districts...')
    all_old = run_sql(f"""
        SELECT id, district_number, num_seats, is_floterial
        FROM districts
        WHERE state_id = {nh_state_id} AND chamber = 'House'
          AND redistricting_cycle = '2012'
        ORDER BY district_number
    """)
    if not all_old:
        print('ERROR: Could not load inserted districts')
        sys.exit(1)

    print(f'  Found {len(all_old)} 2012-cycle districts')

    # Insert seats for each district
    print(f'\nInserting seats...')
    seat_values = []
    for d in all_old:
        for i in range(d['num_seats']):
            desig = chr(65 + i)  # A, B, C, ...
            label = f"NH House {d['district_number']} Seat {desig}"
            seat_values.append(
                f"({d['id']}, 'Legislative', 'State House', "
                f"'{esc(label)}', '{desig}', 2, NULL)"
            )

    inserted_seats = 0
    for batch_start in range(0, len(seat_values), BATCH_SIZE):
        batch = seat_values[batch_start:batch_start + BATCH_SIZE]
        sql = (
            "INSERT INTO seats "
            "(district_id, office_level, office_type, seat_label, "
            "seat_designator, term_length_years, next_regular_election_year) "
            "VALUES " + ",\n".join(batch) +
            "\nRETURNING id;"
        )
        result = run_sql(sql)
        if result:
            inserted_seats += len(result)
            print(f'  Batch: +{len(result)} seats (total: {inserted_seats})')
        else:
            print('  ERROR: Seat insert failed!')
            sys.exit(1)
        time.sleep(2)

    print(f'\nDone! Created {inserted_districts} districts and {inserted_seats} seats '
          f'with redistricting_cycle=2012')


if __name__ == '__main__':
    main()
