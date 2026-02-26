#!/usr/bin/env python3
"""
Fix NH multi-member district candidacies (Seats B+).

NH House has 98 multi-member districts where all candidates run in one pool
(bloc voting). Every seat's election should have the same full candidate list.
The parse_nh_sos.py script garbled Seats B+ data — this script replaces all
Seats B+ candidacies with copies of Seat A's candidacies.

Usage:
    python3 scripts/fix_nh_multiseat_candidacies.py --dry-run
    python3 scripts/fix_nh_multiseat_candidacies.py
"""

import sys
import os
import time
import argparse
from collections import defaultdict

import httpx
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF


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
        return None
    return None


def esc(s):
    if s is None:
        return 'NULL'
    return "'" + str(s).replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser(description='Fix NH multi-member district candidacies')
    parser.add_argument('--dry-run', action='store_true', help='Show what would change without modifying data')
    args = parser.parse_args()

    print('Step 1: Fetching all NH multi-member district election data...')
    rows = run_sql("""
        SELECT d.id as district_id, COALESCE(d.district_name, d.district_number) as district_name, d.num_seats,
               se.id as seat_id, se.seat_designator,
               e.id as election_id, e.election_year,
               cy.id as candidacy_id, cy.candidate_id, cy.party, cy.caucus,
               cy.votes_received, cy.vote_percentage, cy.result,
               cy.is_incumbent, cy.is_write_in
        FROM districts d
        JOIN states st ON d.state_id = st.id
        JOIN seats se ON se.district_id = d.id
        JOIN elections e ON e.seat_id = se.id
          AND e.election_type = 'General'
          AND e.election_year IN (2016, 2018, 2020, 2022, 2024)
        LEFT JOIN candidacies cy ON cy.election_id = e.id
        WHERE st.abbreviation = 'NH' AND d.chamber = 'House' AND d.num_seats > 1
        ORDER BY d.id, e.election_year, se.seat_designator
    """)

    if rows is None:
        print('ERROR: Failed to fetch data')
        return
    print(f'  Got {len(rows)} rows')

    # Organize: {(district_id, year): {seat_designator: {election_id, candidacies: [...]}}}
    district_years = defaultdict(lambda: defaultdict(lambda: {'election_id': None, 'candidacies': []}))
    district_names = {}

    for r in rows:
        key = (r['district_id'], r['election_year'])
        desig = r['seat_designator']
        district_names[r['district_id']] = r['district_name']

        entry = district_years[key][desig]
        entry['election_id'] = r['election_id']
        if r['candidacy_id'] is not None:
            entry['candidacies'].append({
                'candidate_id': r['candidate_id'],
                'party': r['party'],
                'caucus': r['caucus'],
                'votes_received': r['votes_received'],
                'vote_percentage': r['vote_percentage'],
                'result': r['result'],
                'is_incumbent': r['is_incumbent'],
                'is_write_in': r['is_write_in'],
            })

    # Count what we'll be working with
    total_seats_b_plus = 0
    total_delete = 0
    total_insert = 0
    warnings = []
    inserts = []  # list of (election_id, candidacy_dict) tuples
    delete_election_ids = set()

    for (district_id, year), seats in sorted(district_years.items()):
        dist_name = district_names[district_id]
        seat_a = seats.get('A')

        if not seat_a or seat_a['election_id'] is None:
            warnings.append(f'  WARN: {dist_name} (id={district_id}) {year} — no Seat A election')
            continue

        seat_a_cands = seat_a['candidacies']
        if len(seat_a_cands) == 0:
            warnings.append(f'  WARN: {dist_name} (id={district_id}) {year} — Seat A has 0 candidacies (skip)')
            continue

        # Process each Seat B+
        for desig in sorted(seats.keys()):
            if desig == 'A':
                continue
            seat_x = seats[desig]
            if seat_x['election_id'] is None:
                continue

            total_seats_b_plus += 1
            total_delete += len(seat_x['candidacies'])
            total_insert += len(seat_a_cands)

            delete_election_ids.add(seat_x['election_id'])
            for cand in seat_a_cands:
                inserts.append((seat_x['election_id'], cand))

    print(f'\nStep 2: Analysis')
    print(f'  Seats B+ elections to fix: {total_seats_b_plus}')
    print(f'  Candidacies to delete: {total_delete}')
    print(f'  Candidacies to insert: {total_insert}')

    if warnings:
        print(f'\n  Warnings ({len(warnings)}):')
        for w in warnings:
            print(w)

    if args.dry_run:
        print('\nDRY RUN — no changes made')
        return

    # Step 3: Delete all Seats B+ candidacies in one query
    if delete_election_ids:
        print(f'\nStep 3: Deleting candidacies on {len(delete_election_ids)} Seats B+ elections...')
        id_list = ','.join(str(eid) for eid in sorted(delete_election_ids))
        result = run_sql(f"DELETE FROM candidacies WHERE election_id IN ({id_list})")
        if result is None:
            print('ERROR: Delete failed — aborting')
            return
        print(f'  Deleted')

    # Step 4: Insert copies in batches
    if inserts:
        print(f'\nStep 4: Inserting {len(inserts)} candidacies in batches...')
        batch_size = 50
        inserted = 0
        for i in range(0, len(inserts), batch_size):
            batch = inserts[i:i+batch_size]
            values = []
            for election_id, c in batch:
                vals = (
                    f"({election_id}, {c['candidate_id']}, "
                    f"{esc(c['party'])}, {esc(c['caucus'])}, "
                    f"{c['votes_received'] if c['votes_received'] is not None else 'NULL'}, "
                    f"{c['vote_percentage'] if c['vote_percentage'] is not None else 'NULL'}, "
                    f"{esc(c['result'])}, "
                    f"{c['is_incumbent'] if c['is_incumbent'] is not None else 'false'}, "
                    f"{c['is_write_in'] if c['is_write_in'] is not None else 'false'})"
                )
                values.append(vals)
            sql = (
                "INSERT INTO candidacies "
                "(election_id, candidate_id, party, caucus, votes_received, "
                "vote_percentage, result, is_incumbent, is_write_in) VALUES\n"
                + ",\n".join(values)
            )
            result = run_sql(sql)
            if result is not None:
                inserted += len(batch)
            else:
                print(f'  WARN: batch insert failed at index {i}')
            if i + batch_size < len(inserts):
                time.sleep(1)  # pace API calls

        print(f'  Inserted {inserted} candidacies')

    # Step 5: Verification
    print('\nStep 5: Verification...')
    verify = run_sql("""
        SELECT se.seat_designator,
               COUNT(cy.id) as cand_count
        FROM districts d
        JOIN states st ON d.state_id = st.id
        JOIN seats se ON se.district_id = d.id
        JOIN elections e ON e.seat_id = se.id
          AND e.election_type = 'General'
          AND e.election_year IN (2016, 2018, 2020, 2022, 2024)
        LEFT JOIN candidacies cy ON cy.election_id = e.id
        WHERE st.abbreviation = 'NH' AND d.chamber = 'House' AND d.num_seats > 1
        GROUP BY se.seat_designator
        ORDER BY se.seat_designator
    """)
    if verify:
        print('  Candidacies by seat designator:')
        for r in verify:
            print(f"    Seat {r['seat_designator']}: {r['cand_count']} candidacies")

    # Spot check: pick a district and compare Seat A vs Seat B
    spot = run_sql("""
        WITH district_check AS (
            SELECT d.district_name, se.seat_designator, e.election_year,
                   c.full_name, cy.votes_received, cy.result
            FROM districts d
            JOIN states st ON d.state_id = st.id
            JOIN seats se ON se.district_id = d.id
            JOIN elections e ON e.seat_id = se.id
              AND e.election_type = 'General' AND e.election_year = 2024
            JOIN candidacies cy ON cy.election_id = e.id
            JOIN candidates c ON cy.candidate_id = c.id
            WHERE st.abbreviation = 'NH' AND d.chamber = 'House'
              AND d.district_name = 'Hillsborough-37'
            ORDER BY se.seat_designator, cy.votes_received DESC NULLS LAST
        )
        SELECT * FROM district_check
    """)
    if spot:
        print('\n  Spot check — Hillsborough-37, 2024:')
        for r in spot:
            print(f"    Seat {r['seat_designator']}: {r['full_name']} — {r['votes_received']} votes, {r['result']}")

    print('\nDone.')


if __name__ == '__main__':
    main()
