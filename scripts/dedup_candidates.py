#!/usr/bin/env python3
"""
Deduplicate candidate records in the elections database.

Tier 1 (auto-merge): Exact same full_name + shared seat → merge automatically
Tier 2 (review): Exact same full_name + same state, different seats → output for review
Tier 3 (review): Name variations (prefix match) + same state → output for review

For each merge, the "canonical" record is chosen by:
  1. Prefer the record referenced by seat_terms (officeholder history)
  2. Prefer the record with more candidacies
  3. Prefer lower ID (older record)

Usage:
  python3 scripts/dedup_candidates.py --dry-run        # Preview changes
  python3 scripts/dedup_candidates.py                   # Execute merges
  python3 scripts/dedup_candidates.py --review-file /tmp/dedup_review.csv
"""

import argparse
import csv
import sys
import os
import time

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from db_config import TOKEN, API_URL

MAX_RETRIES = 5


def run_sql(sql):
    """Execute SQL via Supabase Management API with retry."""
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


def find_tier1_duplicates():
    """Exact name + shared seat — safe to auto-merge."""
    sql = """
    WITH candidate_seats AS (
      SELECT ca.candidate_id, e.seat_id
      FROM candidacies ca JOIN elections e ON ca.election_id = e.id
      UNION
      SELECT st.candidate_id, st.seat_id FROM seat_terms st
    ),
    dupes AS (
      SELECT DISTINCT c1.id as id1, c1.full_name as name1,
             c2.id as id2, c2.full_name as name2
      FROM candidates c1
      JOIN candidates c2 ON c1.id < c2.id
        AND LOWER(TRIM(c1.full_name)) = LOWER(TRIM(c2.full_name))
        AND c1.full_name IS NOT NULL AND c1.full_name != ''
      JOIN candidate_seats cs1 ON cs1.candidate_id = c1.id
      JOIN candidate_seats cs2 ON cs2.candidate_id = c2.id AND cs1.seat_id = cs2.seat_id
    )
    SELECT DISTINCT d.id1, d.name1, d.id2, d.name2,
      (SELECT COUNT(*) FROM seat_terms WHERE candidate_id = d.id1) as st_count1,
      (SELECT COUNT(*) FROM seat_terms WHERE candidate_id = d.id2) as st_count2,
      (SELECT COUNT(*) FROM candidacies WHERE candidate_id = d.id1) as ca_count1,
      (SELECT COUNT(*) FROM candidacies WHERE candidate_id = d.id2) as ca_count2
    FROM dupes d
    ORDER BY d.id1
    """
    return run_sql(sql)


def find_tier2_duplicates():
    """Exact name + same state, no shared seat."""
    sql = """
    WITH candidate_seats AS (
      SELECT ca.candidate_id, e.seat_id
      FROM candidacies ca JOIN elections e ON ca.election_id = e.id
      UNION
      SELECT st.candidate_id, st.seat_id FROM seat_terms st
    ),
    candidate_states AS (
      SELECT DISTINCT ca.candidate_id, d.state_id
      FROM candidacies ca JOIN elections e ON ca.election_id = e.id
      JOIN seats s ON e.seat_id = s.id JOIN districts d ON s.district_id = d.id
      UNION
      SELECT DISTINCT st.candidate_id, d.state_id
      FROM seat_terms st JOIN seats s ON st.seat_id = s.id JOIN districts d ON s.district_id = d.id
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
    WHERE NOT EXISTS (
      SELECT 1 FROM candidate_seats x1
      JOIN candidate_seats x2 ON x1.seat_id = x2.seat_id
      WHERE x1.candidate_id = c1.id AND x2.candidate_id = c2.id
    )
    ORDER BY st.abbreviation, c1.last_name
    """
    return run_sql(sql)


def find_tier3_duplicates():
    """Name variations (first name prefix match, 3+ chars) + same state."""
    sql = """
    WITH candidate_states AS (
      SELECT DISTINCT ca.candidate_id, d.state_id
      FROM candidacies ca JOIN elections e ON ca.election_id = e.id
      JOIN seats s ON e.seat_id = s.id JOIN districts d ON s.district_id = d.id
      UNION
      SELECT DISTINCT st.candidate_id, d.state_id
      FROM seat_terms st JOIN seats s ON st.seat_id = s.id JOIN districts d ON s.district_id = d.id
    )
    SELECT c1.id as id1, c1.full_name as name1,
           c2.id as id2, c2.full_name as name2,
           st.abbreviation as state
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
    JOIN candidate_states cs1 ON cs1.candidate_id = c1.id
    JOIN candidate_states cs2 ON cs2.candidate_id = c2.id AND cs1.state_id = cs2.state_id
    JOIN states st ON cs1.state_id = st.id
    ORDER BY st.abbreviation, c1.last_name
    """
    return run_sql(sql)


def pick_canonical(row):
    """Choose which record to keep. Returns (keep_id, merge_id)."""
    id1, id2 = row['id1'], row['id2']
    st1 = row.get('st_count1', 0)
    st2 = row.get('st_count2', 0)
    ca1 = row.get('ca_count1', 0)
    ca2 = row.get('ca_count2', 0)

    # Prefer record with seat_terms (officeholder history)
    if st1 > 0 and st2 == 0:
        return id1, id2
    if st2 > 0 and st1 == 0:
        return id2, id1
    # Prefer more candidacies
    if ca1 > ca2:
        return id1, id2
    if ca2 > ca1:
        return id2, id1
    # Prefer lower ID (older)
    return id1, id2


def merge_sql(keep_id, merge_id):
    """Return SQL statements to merge merge_id into keep_id."""
    return f"""
    -- Reassign candidacies (skip duplicates for same election)
    UPDATE candidacies SET candidate_id = {keep_id}
    WHERE candidate_id = {merge_id}
      AND election_id NOT IN (
        SELECT election_id FROM candidacies WHERE candidate_id = {keep_id}
      );
    -- Reassign seat_terms (skip duplicates for same seat + start_date)
    UPDATE seat_terms SET candidate_id = {keep_id}
    WHERE candidate_id = {merge_id}
      AND id NOT IN (
        SELECT st2.id FROM seat_terms st1
        JOIN seat_terms st2 ON st1.seat_id = st2.seat_id
          AND st1.start_date = st2.start_date
        WHERE st1.candidate_id = {keep_id} AND st2.candidate_id = {merge_id}
      );
    -- Delete orphaned references
    DELETE FROM candidacies WHERE candidate_id = {merge_id};
    DELETE FROM seat_terms WHERE candidate_id = {merge_id};
    -- Delete the duplicate candidate
    DELETE FROM candidates WHERE id = {merge_id};
    """


def execute_batch(pairs):
    """Execute a batch of merges in a single API call."""
    sql = "\n".join(merge_sql(keep, merge) for keep, merge in pairs)
    run_sql(sql)


def main():
    parser = argparse.ArgumentParser(description='Deduplicate candidate records')
    parser.add_argument('--dry-run', action='store_true', help='Preview without making changes')
    parser.add_argument('--review-file', default='/tmp/dedup_review.csv',
                        help='Output path for Tier 2/3 review CSV')
    parser.add_argument('--merge-tier2', action='store_true',
                        help='Merge Tier 2 pairs from the review CSV')
    args = parser.parse_args()

    if args.dry_run:
        print('DRY RUN — no database changes will be made.\n')

    # ── Merge Tier 2 from review CSV ──
    if args.merge_tier2:
        print(f'Reading Tier 2 pairs from {args.review_file}...')
        with open(args.review_file) as f:
            reader = csv.DictReader(f)
            tier2_pairs = [(int(row['id1']), int(row['id2']), row['name1'], row['name2'])
                           for row in reader if row['tier'] == '2']
        print(f'  Found {len(tier2_pairs)} Tier 2 pairs')

        # Get seat_term and candidacy counts for canonical selection
        all_ids = set()
        for id1, id2, _, _ in tier2_pairs:
            all_ids.add(id1)
            all_ids.add(id2)
        id_list = ','.join(str(i) for i in all_ids)
        counts = run_sql(f"""
            SELECT c.id,
              (SELECT COUNT(*) FROM seat_terms WHERE candidate_id = c.id) as st_count,
              (SELECT COUNT(*) FROM candidacies WHERE candidate_id = c.id) as ca_count
            FROM candidates c WHERE c.id IN ({id_list})
        """)
        count_map = {r['id']: (r['st_count'], r['ca_count']) for r in counts}

        merged = 0
        already_merged = set()
        batch = []
        BATCH_SIZE = 20

        for id1, id2, name1, name2 in tier2_pairs:
            if id1 in already_merged or id2 in already_merged:
                continue
            st1, ca1 = count_map.get(id1, (0, 0))
            st2, ca2 = count_map.get(id2, (0, 0))
            row = {'id1': id1, 'id2': id2, 'st_count1': st1, 'st_count2': st2,
                   'ca_count1': ca1, 'ca_count2': ca2}
            keep_id, merge_id = pick_canonical(row)

            if args.dry_run:
                print(f'  MERGE: #{keep_id} "{name1 if keep_id == id1 else name2}" '
                      f'← #{merge_id} "{name2 if merge_id == id2 else name1}" '
                      f'(st:{st1}/{st2}, ca:{ca1}/{ca2})')
            else:
                batch.append((keep_id, merge_id))
                if len(batch) >= BATCH_SIZE:
                    execute_batch(batch)
                    print(f'  Merged batch of {len(batch)} (total: {merged + len(batch)})')
                    merged += len(batch)
                    batch = []
                    time.sleep(2)

            already_merged.add(merge_id)
            if args.dry_run:
                merged += 1

        if batch:
            execute_batch(batch)
            print(f'  Merged final batch of {len(batch)} (total: {merged + len(batch)})')
            merged += len(batch)

        print(f'  {"Would merge" if args.dry_run else "Merged"}: {merged} Tier 2 pairs\n')
        print('Done.')
        return

    # ── Tier 1: Auto-merge ──
    print('Finding Tier 1 duplicates (exact name + shared seat)...')
    tier1 = find_tier1_duplicates()
    print(f'  Found {len(tier1)} duplicate pairs')

    merged = 0
    already_merged = set()  # Track IDs we've already merged away
    batch = []
    BATCH_SIZE = 20

    for row in tier1:
        keep_id, merge_id = pick_canonical(row)
        if merge_id in already_merged or keep_id in already_merged:
            continue  # Skip if one side was already merged in a prior iteration

        if args.dry_run:
            print(f'  MERGE: #{keep_id} "{row["name1"]}" ← #{merge_id} "{row["name2"]}" '
                  f'(st:{row["st_count1"]}/{row["st_count2"]}, ca:{row["ca_count1"]}/{row["ca_count2"]})')
        else:
            batch.append((keep_id, merge_id))
            if len(batch) >= BATCH_SIZE:
                execute_batch(batch)
                print(f'  Merged batch of {len(batch)} (total: {merged + len(batch)})')
                merged += len(batch)
                batch = []
                time.sleep(2)  # Brief pause between batches

        already_merged.add(merge_id)
        if args.dry_run:
            merged += 1

    # Flush remaining batch
    if batch:
        execute_batch(batch)
        print(f'  Merged final batch of {len(batch)} (total: {merged + len(batch)})')
        merged += len(batch)

    print(f'  {"Would merge" if args.dry_run else "Merged"}: {merged} pairs\n')

    # ── Tier 2 & 3: Review file ──
    print('Finding Tier 2 duplicates (exact name + same state, different seats)...')
    tier2 = find_tier2_duplicates()
    print(f'  Found {len(tier2)} pairs')

    print('Finding Tier 3 duplicates (name variations + same state)...')
    tier3 = find_tier3_duplicates()
    print(f'  Found {len(tier3)} pairs')

    review_rows = []
    for row in tier2:
        review_rows.append({
            'tier': 2,
            'state': row['state'],
            'id1': row['id1'],
            'name1': row['name1'],
            'id2': row['id2'],
            'name2': row['name2'],
        })
    for row in tier3:
        review_rows.append({
            'tier': 3,
            'state': row['state'],
            'id1': row['id1'],
            'name1': row['name1'],
            'id2': row['id2'],
            'name2': row['name2'],
        })

    if review_rows:
        with open(args.review_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['tier', 'state', 'id1', 'name1', 'id2', 'name2'])
            writer.writeheader()
            writer.writerows(review_rows)
        print(f'\n  Review file: {args.review_file} ({len(review_rows)} pairs)')

    print('\nDone.')


if __name__ == '__main__':
    main()
