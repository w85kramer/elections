#!/usr/bin/env python3
"""
Migrate NH House pre-2022 elections from current (2022-cycle) seats to old-era (2012-cycle) seats.

Elections before 2022 should be on 2012-cycle districts/seats since they used the old map.
The mapping is by district_number + seat_designator:
  - Current NULL designator (single-seat) → old-era 'A'
  - Current 'A' → old-era 'A'
  - Current 'B' → old-era 'B', etc.

If the old-era district doesn't have enough seats (e.g., old district has 2 seats but
current has 3), elections on excess seats are flagged (shouldn't happen since BP data
was limited by old seat counts, but we check anyway).

Usage:
    python3 scripts/migrate_nh_pre2022_elections.py --dry-run   # Preview only
    python3 scripts/migrate_nh_pre2022_elections.py              # Execute migration
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
BATCH_SIZE = 200


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


def main():
    parser = argparse.ArgumentParser(description='Migrate NH pre-2022 elections to old-era seats')
    parser.add_argument('--dry-run', action='store_true', help='Preview only')
    args = parser.parse_args()

    # Load current-cycle seats
    print('Loading current (2022-cycle) NH House seats...')
    current_seats = run_sql("""
        SELECT s.id as seat_id, s.seat_designator, d.district_number
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NH' AND d.chamber = 'House'
          AND d.redistricting_cycle = '2022'
        ORDER BY d.district_number, s.seat_designator
    """, exit_on_error=True)
    print(f'  {len(current_seats)} current seats')

    # Load old-era seats
    print('Loading old-era (2012-cycle) NH House seats...')
    old_seats = run_sql("""
        SELECT s.id as seat_id, s.seat_designator, d.district_number
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NH' AND d.chamber = 'House'
          AND d.redistricting_cycle = '2012'
        ORDER BY d.district_number, s.seat_designator
    """, exit_on_error=True)
    print(f'  {len(old_seats)} old-era seats')

    # Build old-era lookup: (district_number, designator) → seat_id
    old_lookup = {}
    for s in old_seats:
        key = (s['district_number'], s['seat_designator'])
        old_lookup[key] = s['seat_id']

    # Load pre-2022 elections on current seats
    print('\nLoading pre-2022 elections on current-cycle seats...')
    elections = run_sql("""
        SELECT e.id as election_id, e.seat_id, e.election_year, e.election_type,
               s.seat_designator, d.district_number
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NH' AND d.chamber = 'House'
          AND d.redistricting_cycle = '2022'
          AND e.election_year < 2022
        ORDER BY d.district_number, s.seat_designator, e.election_year
    """, exit_on_error=True)
    print(f'  {len(elections)} pre-2022 elections to migrate')

    # Save backup
    backup_path = '/tmp/nh_pre2022_election_backup.json'
    with open(backup_path, 'w') as f:
        json.dump(elections, f, indent=2)
    print(f'  Backup saved to {backup_path}')

    # Build migration map
    migrations = []  # (election_id, old_seat_id)
    unmapped = []

    for e in elections:
        dist_num = e['district_number']
        # Map NULL designator to 'A' for old-era lookup
        desig = e['seat_designator'] if e['seat_designator'] else 'A'

        old_seat_id = old_lookup.get((dist_num, desig))
        if old_seat_id:
            migrations.append((e['election_id'], old_seat_id))
        else:
            unmapped.append(e)

    print(f'\nMigration plan:')
    print(f'  Will migrate: {len(migrations)} elections')
    print(f'  Unmapped: {len(unmapped)} elections')

    if unmapped:
        print(f'\n  Unmapped elections (no matching old-era seat):')
        for e in unmapped[:20]:
            print(f"    {e['district_number']} Seat {e['seat_designator']} "
                  f"year={e['election_year']} type={e['election_type']}")
        if len(unmapped) > 20:
            print(f'    ... and {len(unmapped) - 20} more')

    if args.dry_run:
        # Show sample migrations
        print(f'\n  Sample migrations:')
        for eid, new_sid in migrations[:10]:
            orig = next(e for e in elections if e['election_id'] == eid)
            print(f"    Election {eid} ({orig['district_number']} "
                  f"Seat {orig['seat_designator']} {orig['election_year']}) "
                  f"→ old seat {new_sid}")
        print('\nDRY RUN — no changes made.')
        return

    if not migrations:
        print('Nothing to migrate.')
        return

    # Execute migrations in batches using CASE statements
    print(f'\nMigrating {len(migrations)} elections...')
    total_migrated = 0

    for batch_start in range(0, len(migrations), BATCH_SIZE):
        batch = migrations[batch_start:batch_start + BATCH_SIZE]

        # Build CASE statement for batch UPDATE
        case_parts = []
        ids = []
        for eid, new_sid in batch:
            case_parts.append(f"WHEN {eid} THEN {new_sid}")
            ids.append(str(eid))

        sql = (
            f"UPDATE elections SET seat_id = CASE id\n"
            + "\n".join(f"  {p}" for p in case_parts) +
            f"\nEND\nWHERE id IN ({','.join(ids)});"
        )

        result = run_sql(sql)
        if result is not None:
            total_migrated += len(batch)
            print(f'  Batch: +{len(batch)} (total: {total_migrated})')
        else:
            print(f'  ERROR: Migration batch failed at offset {batch_start}!')
            print(f'  {total_migrated} elections already migrated.')
            print(f'  Use backup at {backup_path} to rollback if needed.')
            sys.exit(1)
        time.sleep(2)

    print(f'\nDone! Migrated {total_migrated} elections to 2012-cycle seats.')

    # Verify
    print('\nVerifying...')
    check = run_sql("""
        SELECT d.redistricting_cycle, COUNT(*) as cnt
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NH' AND d.chamber = 'House'
          AND e.election_year < 2022
        GROUP BY d.redistricting_cycle
    """)
    if check:
        for r in check:
            print(f"  cycle={r['redistricting_cycle']}: {r['cnt']} pre-2022 elections")


if __name__ == '__main__':
    main()
