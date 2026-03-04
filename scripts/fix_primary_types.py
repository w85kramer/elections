#!/usr/bin/env python3
"""
Fix mislabeled primary election types across all states.

The download_district_history.py parser sometimes assigns the wrong election_type
to primary elections (e.g., Primary_D when the candidates are Republicans). This
script fixes those mismatches and backfills NULL party on candidacies in partisan
primaries.

Pass 1: Fix election_type mismatches (~630 elections)
  - Primary_D with R winner → swap to Primary_R (if no conflict)
  - Primary_R with D winner → swap to Primary_D (if no conflict)
  - Primary with L/I/G/NP winner → change to generic Primary
  - Conflicts (swap would duplicate) → change to generic Primary

Pass 2: Backfill NULL party on candidacies (~11,500)
  - Candidacies in Primary_D → party = 'D'
  - Candidacies in Primary_R → party = 'R'

Pass 3: Summary report

Usage:
    python3 scripts/fix_primary_types.py --dry-run          # preview all changes
    python3 scripts/fix_primary_types.py --dry-run --state AR  # preview one state
    python3 scripts/fix_primary_types.py                     # execute all
    python3 scripts/fix_primary_types.py --state AR          # execute one state
"""

import sys
import os
import time
import argparse
from collections import defaultdict

import httpx
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF


# ─────────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────────

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
            wait = 10 * (attempt + 1)
            print(f'    Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR ({resp.status_code}): {resp.text[:500]}')
        return None
    print('    Max retries exceeded')
    return None


def main():
    parser = argparse.ArgumentParser(description='Fix mislabeled primary election types')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without applying')
    parser.add_argument('--state', type=str, help='Only fix a specific state (abbreviation)')
    args = parser.parse_args()

    state_filter = ''
    if args.state:
        state_filter = f"AND st.abbreviation = '{args.state.upper()}'"
        print(f'Filtering to state: {args.state.upper()}')

    # ─────────────────────────────────────────
    # PASS 1: Fix election_type mismatches
    # ─────────────────────────────────────────
    print('\n═══ PASS 1: Fix election_type mismatches ═══\n')

    # Find Primary_D/Primary_R elections where the winner's party contradicts the type
    mismatch_sql = f"""
        SELECT
            e.id AS election_id,
            e.election_type,
            e.election_year,
            st.abbreviation AS state,
            d.district_name AS district,
            s.id AS seat_id,
            cy.party AS winner_party,
            c.full_name AS winner_name
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidacies cy ON cy.election_id = e.id
        JOIN candidates c ON cy.candidate_id = c.id
        WHERE e.election_type IN ('Primary_D', 'Primary_R')
          AND cy.result = 'Won'
          AND cy.party IS NOT NULL
          AND (
              (e.election_type = 'Primary_D' AND cy.party != 'D')
              OR (e.election_type = 'Primary_R' AND cy.party != 'R')
          )
          {state_filter}
        ORDER BY st.abbreviation, e.election_year, d.district_name
    """

    print('Querying mismatched elections...')
    mismatches = run_sql(mismatch_sql)
    if mismatches is None:
        print('ERROR: Failed to query mismatches')
        return

    print(f'Found {len(mismatches)} mismatched elections')

    if not mismatches:
        print('No mismatches found.')
    else:
        # For each mismatch, check if swapping would conflict with an existing primary
        # Build a set of (seat_id, year, election_type) for existing primaries
        existing_sql = f"""
            SELECT e.seat_id, e.election_year, e.election_type
            FROM elections e
            JOIN seats s ON e.seat_id = s.id
            JOIN districts d ON s.district_id = d.id
            JOIN states st ON d.state_id = st.id
            WHERE e.election_type IN ('Primary_D', 'Primary_R')
              {state_filter}
        """
        existing_rows = run_sql(existing_sql)
        existing_primaries = set()
        if existing_rows:
            for r in existing_rows:
                existing_primaries.add((r['seat_id'], r['election_year'], r['election_type']))

        # Categorize fixes
        swap_to_r = []       # Primary_D → Primary_R
        swap_to_d = []       # Primary_R → Primary_D
        to_generic = []      # → Primary (conflicts or third-party winners)
        conflicts = []       # logged for review

        state_counts = defaultdict(lambda: {'swap': 0, 'generic': 0, 'conflict': 0})

        for m in mismatches:
            eid = m['election_id']
            etype = m['election_type']
            winner_party = m['winner_party']
            seat_id = m['seat_id']
            year = m['election_year']
            state = m['state']

            # Determine target type
            if etype == 'Primary_D' and winner_party == 'R':
                target = 'Primary_R'
            elif etype == 'Primary_R' and winner_party == 'D':
                target = 'Primary_D'
            else:
                # Third-party winner (L, I, G, NP, etc.) → generic Primary
                to_generic.append(m)
                state_counts[state]['generic'] += 1
                continue

            # Check for conflict
            if (seat_id, year, target) in existing_primaries:
                # Swap would create a duplicate — use generic Primary
                to_generic.append(m)
                conflicts.append(m)
                state_counts[state]['conflict'] += 1
            else:
                if target == 'Primary_R':
                    swap_to_r.append(m)
                else:
                    swap_to_d.append(m)
                state_counts[state]['swap'] += 1

        print(f'\n  Swap to Primary_R: {len(swap_to_r)}')
        print(f'  Swap to Primary_D: {len(swap_to_d)}')
        print(f'  Change to Primary (generic): {len(to_generic)}')
        print(f'    (of which {len(conflicts)} are conflict cases)')

        # Show per-state breakdown
        print('\n  Per-state breakdown:')
        for state in sorted(state_counts.keys()):
            c = state_counts[state]
            parts = []
            if c['swap']:
                parts.append(f"{c['swap']} swapped")
            if c['generic']:
                parts.append(f"{c['generic']} generic")
            if c['conflict']:
                parts.append(f"{c['conflict']} conflicts")
            print(f'    {state}: {", ".join(parts)}')

        # Show sample mismatches
        print('\n  Sample mismatches:')
        for m in mismatches[:15]:
            print(f"    {m['state']} {m['district']} {m['election_year']}: "
                  f"{m['election_type']} but winner={m['winner_name']} ({m['winner_party']})")
        if len(mismatches) > 15:
            print(f'    ... and {len(mismatches) - 15} more')

        # Show conflict cases
        if conflicts:
            print(f'\n  Conflict cases ({len(conflicts)}):')
            for m in conflicts[:10]:
                print(f"    {m['state']} {m['district']} {m['election_year']}: "
                      f"{m['election_type']} → Primary (conflict)")
            if len(conflicts) > 10:
                print(f'    ... and {len(conflicts) - 10} more')

        if not args.dry_run:
            print('\n  Applying election_type fixes...')
            total_fixed = 0

            # Batch swap to Primary_R
            if swap_to_r:
                ids = ','.join(str(m['election_id']) for m in swap_to_r)
                result = run_sql(f"UPDATE elections SET election_type = 'Primary_R' WHERE id IN ({ids})")
                if result is not None:
                    total_fixed += len(swap_to_r)
                    print(f'    Updated {len(swap_to_r)} elections to Primary_R')
                else:
                    print(f'    ERROR: Failed to update Primary_R batch')

            # Batch swap to Primary_D
            if swap_to_d:
                ids = ','.join(str(m['election_id']) for m in swap_to_d)
                result = run_sql(f"UPDATE elections SET election_type = 'Primary_D' WHERE id IN ({ids})")
                if result is not None:
                    total_fixed += len(swap_to_d)
                    print(f'    Updated {len(swap_to_d)} elections to Primary_D')
                else:
                    print(f'    ERROR: Failed to update Primary_D batch')

            # Batch change to generic Primary
            if to_generic:
                ids = ','.join(str(m['election_id']) for m in to_generic)
                result = run_sql(f"UPDATE elections SET election_type = 'Primary' WHERE id IN ({ids})")
                if result is not None:
                    total_fixed += len(to_generic)
                    print(f'    Updated {len(to_generic)} elections to Primary')
                else:
                    print(f'    ERROR: Failed to update Primary batch')

            print(f'\n  Total elections fixed: {total_fixed}')

    # ─────────────────────────────────────────
    # PASS 2: Backfill NULL party on candidacies
    # ─────────────────────────────────────────
    print('\n═══ PASS 2: Backfill NULL party on candidacies ═══\n')

    # Count NULL-party candidacies in Primary_D elections
    null_d_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE e.election_type = 'Primary_D'
          AND cy.party IS NULL
          {state_filter}
    """
    null_r_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE e.election_type = 'Primary_R'
          AND cy.party IS NULL
          {state_filter}
    """

    d_count = run_sql(null_d_sql)
    r_count = run_sql(null_r_sql)

    d_num = d_count[0]['cnt'] if d_count else 0
    r_num = r_count[0]['cnt'] if r_count else 0

    print(f'  NULL-party candidacies in Primary_D: {d_num}')
    print(f'  NULL-party candidacies in Primary_R: {r_num}')
    print(f'  Total to backfill: {d_num + r_num}')

    if not args.dry_run and (d_num + r_num) > 0:
        print('\n  Applying party backfill...')

        if d_num > 0:
            update_d_sql = f"""
                UPDATE candidacies
                SET party = 'D'
                WHERE party IS NULL
                  AND election_id IN (
                      SELECT e.id FROM elections e
                      JOIN seats s ON e.seat_id = s.id
                      JOIN districts d ON s.district_id = d.id
                      JOIN states st ON d.state_id = st.id
                      WHERE e.election_type = 'Primary_D'
                        {state_filter}
                  )
            """
            result = run_sql(update_d_sql)
            if result is not None:
                print(f'    Set party=D on {d_num} candidacies')
            else:
                print(f'    ERROR: Failed to update Primary_D candidacies')

        if r_num > 0:
            update_r_sql = f"""
                UPDATE candidacies
                SET party = 'R'
                WHERE party IS NULL
                  AND election_id IN (
                      SELECT e.id FROM elections e
                      JOIN seats s ON e.seat_id = s.id
                      JOIN districts d ON s.district_id = d.id
                      JOIN states st ON d.state_id = st.id
                      WHERE e.election_type = 'Primary_R'
                        {state_filter}
                  )
            """
            result = run_sql(update_r_sql)
            if result is not None:
                print(f'    Set party=R on {r_num} candidacies')
            else:
                print(f'    ERROR: Failed to update Primary_R candidacies')

    # ─────────────────────────────────────────
    # PASS 3: Summary report
    # ─────────────────────────────────────────
    print('\n═══ PASS 3: Summary ═══\n')

    # Per-state counts of remaining NULL-party candidacies in partisan primaries
    remaining_sql = f"""
        SELECT st.abbreviation AS state, COUNT(*) AS cnt
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE e.election_type IN ('Primary_D', 'Primary_R')
          AND cy.party IS NULL
          {state_filter}
        GROUP BY st.abbreviation
        ORDER BY cnt DESC
    """
    remaining = run_sql(remaining_sql)
    if remaining:
        print(f'  Remaining NULL-party candidacies in partisan primaries: {sum(r["cnt"] for r in remaining)}')
        for r in remaining[:10]:
            print(f'    {r["state"]}: {r["cnt"]}')
    else:
        print('  No remaining NULL-party candidacies in partisan primaries')

    # Remaining mismatches (should be 0 after fix)
    remaining_mismatch_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidacies cy ON cy.election_id = e.id
        WHERE e.election_type IN ('Primary_D', 'Primary_R')
          AND cy.result = 'Won'
          AND cy.party IS NOT NULL
          AND (
              (e.election_type = 'Primary_D' AND cy.party != 'D')
              OR (e.election_type = 'Primary_R' AND cy.party != 'R')
          )
          {state_filter}
    """
    remaining_mm = run_sql(remaining_mismatch_sql)
    mm_cnt = remaining_mm[0]['cnt'] if remaining_mm else '?'
    print(f'  Remaining winner-type mismatches: {mm_cnt}')

    if args.dry_run:
        print('\n  DRY RUN — no changes were made')

    print('\nDone.')


if __name__ == '__main__':
    main()
