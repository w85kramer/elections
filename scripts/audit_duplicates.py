#!/usr/bin/env python3
"""
Audit for potential duplicate candidates.

Lightweight check that can be run periodically (e.g., as part of the Monday
election monitoring routine). Queries for candidates with similar names in
the same state and reports any potential duplicates.

Usage:
    python3 scripts/audit_duplicates.py              # Check all states
    python3 scripts/audit_duplicates.py --state NH   # Check one state
    python3 scripts/audit_duplicates.py --recent 7   # Only candidates added in last N days
"""

import argparse
import sys
import os
import time

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from db_config import TOKEN, API_URL

MAX_RETRIES = 5


def run_sql(sql):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = httpx.post(
                API_URL,
                headers={'Authorization': f'Bearer {TOKEN}',
                         'Content-Type': 'application/json'},
                json={'query': sql},
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
                raise RuntimeError(f'SQL error {resp.status_code}: {resp.text[:200]}')
        except httpx.TimeoutException:
            if attempt < MAX_RETRIES:
                print(f'  Timeout, retrying ({attempt}/{MAX_RETRIES})...')
                time.sleep(5 * attempt)
            else:
                raise
    raise RuntimeError(f'Failed after {MAX_RETRIES} retries')


def check_exact_name_dupes(state_filter='', recent_days=None):
    """Find candidates with exact same name (after normalization) in same state."""
    where_clauses = []
    if state_filter:
        where_clauses.append(f"st.abbreviation = '{state_filter}'")
    if recent_days:
        where_clauses.append(
            f"(c1.created_at >= NOW() - INTERVAL '{recent_days} days' "
            f"OR c2.created_at >= NOW() - INTERVAL '{recent_days} days')")

    extra_where = ('AND ' + ' AND '.join(where_clauses)) if where_clauses else ''

    sql = f"""
    WITH candidate_states AS (
      SELECT DISTINCT ca.candidate_id, d.state_id
      FROM candidacies ca JOIN elections e ON ca.election_id = e.id
      JOIN seats s ON e.seat_id = s.id JOIN districts d ON s.district_id = d.id
      UNION
      SELECT DISTINCT stm.candidate_id, d.state_id
      FROM seat_terms stm JOIN seats s ON stm.seat_id = s.id JOIN districts d ON s.district_id = d.id
    )
    SELECT c1.id as id1, c1.full_name as name1,
           c2.id as id2, c2.full_name as name2,
           st.abbreviation as state
    FROM candidates c1
    JOIN candidates c2 ON c1.id < c2.id
      AND LOWER(TRIM(c1.full_name)) = LOWER(TRIM(c2.full_name))
      AND c1.full_name IS NOT NULL AND c1.full_name != ''
    JOIN candidate_states cs1 ON cs1.candidate_id = c1.id
    JOIN candidate_states cs2 ON cs2.candidate_id = c2.id AND cs1.state_id = cs2.state_id
    JOIN states st ON cs1.state_id = st.id
    {extra_where}
    ORDER BY st.abbreviation, c1.full_name
    """
    return run_sql(sql)


def check_name_variation_dupes(state_filter='', recent_days=None):
    """Find candidates with name variations (prefix match, 3+ chars) in same state."""
    where_clauses = []
    if state_filter:
        where_clauses.append(f"st.abbreviation = '{state_filter}'")
    if recent_days:
        where_clauses.append(
            f"(c1.created_at >= NOW() - INTERVAL '{recent_days} days' "
            f"OR c2.created_at >= NOW() - INTERVAL '{recent_days} days')")

    extra_where = ('AND ' + ' AND '.join(where_clauses)) if where_clauses else ''

    sql = f"""
    WITH candidate_states AS (
      SELECT DISTINCT ca.candidate_id, d.state_id
      FROM candidacies ca JOIN elections e ON ca.election_id = e.id
      JOIN seats s ON e.seat_id = s.id JOIN districts d ON s.district_id = d.id
      UNION
      SELECT DISTINCT stm.candidate_id, d.state_id
      FROM seat_terms stm JOIN seats s ON stm.seat_id = s.id JOIN districts d ON s.district_id = d.id
    ),
    -- Only check candidates that share a seat (more likely to be dupes)
    candidate_seats AS (
      SELECT ca.candidate_id, e.seat_id
      FROM candidacies ca JOIN elections e ON ca.election_id = e.id
      UNION
      SELECT stm.candidate_id, stm.seat_id FROM seat_terms stm
    )
    SELECT DISTINCT c1.id as id1, c1.full_name as name1,
           c2.id as id2, c2.full_name as name2,
           st.abbreviation as state, c1.last_name as sort_last
    FROM candidates c1
    JOIN candidates c2 ON c1.id < c2.id
      AND LOWER(c1.last_name) = LOWER(c2.last_name)
      AND c1.last_name IS NOT NULL AND c1.last_name != ''
      AND LOWER(TRIM(c1.full_name)) != LOWER(TRIM(c2.full_name))
      AND (
        LOWER(c1.first_name) LIKE LOWER(c2.first_name) || '%'
        OR LOWER(c2.first_name) LIKE LOWER(c1.first_name) || '%'
      )
      AND LENGTH(LEAST(c1.first_name, c2.first_name)) >= 3
    JOIN candidate_seats cs1 ON cs1.candidate_id = c1.id
    JOIN candidate_seats cs2 ON cs2.candidate_id = c2.id AND cs1.seat_id = cs2.seat_id
    JOIN candidate_states cst1 ON cst1.candidate_id = c1.id
    JOIN states st ON cst1.state_id = st.id
    {extra_where}
    ORDER BY st.abbreviation, sort_last
    """
    return run_sql(sql)


def main():
    parser = argparse.ArgumentParser(description='Audit for duplicate candidates')
    parser.add_argument('--state', help='Check only this state (abbreviation)')
    parser.add_argument('--recent', type=int,
                        help='Only check candidates added in the last N days')
    args = parser.parse_args()

    state = args.state.upper() if args.state else ''

    scope = f' for {state}' if state else ''
    if args.recent:
        scope += f' (last {args.recent} days)'

    print(f'Checking for duplicate candidates{scope}...\n')

    # Exact name matches
    print('Exact name duplicates (same name, same state):')
    exact = check_exact_name_dupes(state, args.recent)
    if exact:
        for r in exact:
            print(f'  {r["state"]}: #{r["id1"]} "{r["name1"]}" ↔ #{r["id2"]} "{r["name2"]}"')
        print(f'  → {len(exact)} pairs found\n')
    else:
        print('  None found ✓\n')

    # Name variation matches (shared seat)
    print('Name variation duplicates (similar name + shared seat):')
    variations = check_name_variation_dupes(state, args.recent)
    if variations:
        for r in variations:
            print(f'  {r["state"]}: #{r["id1"]} "{r["name1"]}" ↔ #{r["id2"]} "{r["name2"]}"')
        print(f'  → {len(variations)} pairs found\n')
    else:
        print('  None found ✓\n')

    total = len(exact) + len(variations)
    if total == 0:
        print('No duplicates detected.')
    else:
        print(f'Total: {total} potential duplicate pairs.')
        print('Run scripts/dedup_candidates.py to merge them.')


if __name__ == '__main__':
    main()
