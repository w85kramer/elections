#!/usr/bin/env python3
"""Fix elections with vote percentages summing to >100%.

Four phases:
1. Delete 'Tot' party candidacies (CT/NY fusion voting totals)
2. Fix elections with same candidate appearing multiple times (NJ, VA, MS, etc.)
3. Fix same-person duplicate candidacies (AR name variants, ND governor, etc.)
4. Clean up remaining >110% elections (CT/NY fusion name-matching, governor misc)

Usage:
    python3 scripts/fix_vote_percentages.py --dry-run              # Preview all phases
    python3 scripts/fix_vote_percentages.py --phase 1 --dry-run    # Preview phase 1
    python3 scripts/fix_vote_percentages.py --phase 2              # Execute phase 2
    python3 scripts/fix_vote_percentages.py                        # Execute all phases
"""

import argparse
import sys
import os
import time
from collections import defaultdict

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF

API_URL = f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query'


def run_sql(query, max_retries=5):
    """Execute SQL via Supabase Management API with exponential backoff."""
    for attempt in range(max_retries):
        resp = httpx.post(
            API_URL,
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120,
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f'    Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'    SQL ERROR ({resp.status_code}): {resp.text[:500]}')
        return None
    print('    Max retries exceeded')
    return None


def calc_implied_total(votes, pct):
    """Calculate implied total votes from a candidacy's votes and percentage."""
    if not votes or not pct or float(pct) <= 0:
        return None
    return votes / (float(pct) / 100)


def pct_diff(a, b):
    """Fractional difference between two values."""
    if not b or b == 0:
        return float('inf')
    return abs(a - b) / b


def normalize_name(name):
    """Strip Ballotpedia-style quotes, incumbency markers, and formatting."""
    import re
    n = name.replace("''", "").replace("'", "").strip()
    # Remove parenthetical suffixes like (inc.), (Incumbent), (inc)
    n = re.sub(r'\s*\((?:inc\.?|incumbent)\)\s*', ' ', n, flags=re.IGNORECASE).strip()
    return n


def delete_candidacies(ids, dry_run, batch_size=50):
    """Delete candidacies by ID list, batched to avoid oversized queries."""
    if not ids or dry_run:
        return
    id_list = list(ids)
    for i in range(0, len(id_list), batch_size):
        batch = id_list[i:i + batch_size]
        run_sql(f'DELETE FROM candidacies WHERE id IN ({",".join(str(x) for x in batch)});')
        if i + batch_size < len(id_list):
            time.sleep(0.5)


def get_problem_elections(threshold=110):
    """Get elections where candidacy vote_percentage sums exceed threshold."""
    rows = run_sql(f"""
        SELECT e.id as election_id, e.election_type, e.election_year,
               e.total_votes_cast, s.seat_label, st.abbreviation as state,
               COUNT(c.id) as num_candidacies,
               ROUND(SUM(c.vote_percentage)::numeric, 2) as total_pct
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        LEFT JOIN candidacies c ON c.election_id = e.id
        WHERE c.vote_percentage IS NOT NULL
        GROUP BY e.id, e.election_type, e.election_year, e.total_votes_cast,
                 s.seat_label, st.abbreviation
        HAVING SUM(c.vote_percentage) > {threshold}
        ORDER BY SUM(c.vote_percentage) DESC
    """)
    return rows or []


# ═══════════════════════════════════════════════════════════
# PHASE 1: Delete "Tot" party candidacies
# ═══════════════════════════════════════════════════════════

def phase_1(dry_run):
    print('\n' + '=' * 60)
    print(' PHASE 1: Delete "Tot" party candidacies')
    print('=' * 60 + '\n')

    rows = run_sql("""
        SELECT c.id, c.election_id, c.votes_received, c.vote_percentage,
               ca.full_name, e.election_year, s.seat_label, st.abbreviation as state
        FROM candidacies c
        JOIN candidates ca ON c.candidate_id = ca.id
        JOIN elections e ON c.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE c.party = 'Tot'
        ORDER BY st.abbreviation, e.election_year
    """)

    if not rows:
        print('  No "Tot" candidacies found.')
        return

    print(f'  Found {len(rows)} "Tot" candidacies to delete:\n')
    for r in rows:
        print(f'    {r["state"]} {r["seat_label"]} {r["election_year"]}: '
              f'{r["full_name"]} -- {r["votes_received"]:,} votes ({r["vote_percentage"]}%)')

    if dry_run:
        print('\n  DRY RUN -- no changes made')
        return

    ids = [r['id'] for r in rows]
    delete_candidacies(ids, dry_run)
    print(f'\n  Deleted {len(rows)} "Tot" candidacies')


# ═══════════════════════════════════════════════════════════
# PHASE 2: Fix duplicate candidate entries
# Same candidate_id appears multiple times with different votes
# Only processes elections currently >110%
# Distinguishes multi-race mixing from fusion voting
# ═══════════════════════════════════════════════════════════

def phase_2(dry_run):
    print('\n' + '=' * 60)
    print(' PHASE 2: Fix duplicate candidate entries')
    print('=' * 60 + '\n')

    # Step 1: Get all >110% elections
    problems = get_problem_elections(110)
    if not problems:
        print('  No >110% elections found.')
        return
    problem_map = {p['election_id']: p for p in problems}
    problem_eids = set(problem_map.keys())
    print(f'  {len(problems)} elections currently >110%')

    # Step 2: Find duplicate candidate_ids in those elections (bulk query)
    eid_list = ','.join(str(e) for e in problem_eids)
    dupes = run_sql(f"""
        SELECT c.election_id, c.candidate_id, COUNT(*) as cnt
        FROM candidacies c
        WHERE c.election_id IN ({eid_list})
          AND c.votes_received IS NOT NULL AND c.votes_received > 0
        GROUP BY c.election_id, c.candidate_id
        HAVING COUNT(*) > 1
    """)

    if not dupes:
        print('  No duplicate candidate entries in >110% elections.')
        return

    dupe_eids = sorted(set(d['election_id'] for d in dupes))
    print(f'  {len(dupe_eids)} of those have duplicate candidate entries')

    # Step 3: Bulk-fetch all candidacies for affected elections
    dupe_eid_list = ','.join(str(e) for e in dupe_eids)
    all_cands = run_sql(f"""
        SELECT c.id, c.election_id, c.candidate_id, c.party, c.votes_received,
               c.vote_percentage, c.result, ca.full_name
        FROM candidacies c
        JOIN candidates ca ON c.candidate_id = ca.id
        WHERE c.election_id IN ({dupe_eid_list})
        ORDER BY c.election_id, c.votes_received DESC NULLS LAST
    """)
    if not all_cands:
        print('  Failed to fetch candidacies.')
        return

    cands_by_eid = defaultdict(list)
    for c in all_cands:
        cands_by_eid[c['election_id']].append(c)

    # Step 4: Process each election
    print(f'\n  Processing {len(dupe_eids)} elections...\n')
    all_delete_ids = set()
    TOLERANCE = 0.08

    for eid in dupe_eids:
        ei = problem_map[eid]
        total_votes = ei['total_votes_cast']
        cands = cands_by_eid.get(eid, [])
        if not cands:
            continue

        by_cid = defaultdict(list)
        for c in cands:
            by_cid[c['candidate_id']].append(c)

        delete_ids = set()
        is_multi_race = False

        for cid, entries in by_cid.items():
            if len(entries) <= 1:
                continue

            if total_votes and total_votes > 0:
                # Compute implied_total spread to distinguish patterns
                imp_totals = []
                for e in entries:
                    it = calc_implied_total(e['votes_received'], e['vote_percentage'])
                    if it and it > 0:
                        imp_totals.append(it)

                spread = 1.0
                if len(imp_totals) >= 2 and min(imp_totals) > 0:
                    spread = max(imp_totals) / min(imp_totals)

                if spread > 1.15:
                    # Multi-race mixing: entries come from different races
                    # Keep entry whose implied_total best matches total_votes_cast
                    is_multi_race = True
                    best = min(entries, key=lambda e: pct_diff(
                        calc_implied_total(e['votes_received'], e['vote_percentage']) or 0,
                        total_votes))
                    for e in entries:
                        if e['id'] != best['id']:
                            delete_ids.add(e['id'])
                else:
                    # Fusion voting: all entries from same election
                    # Keep highest-voted entry
                    sorted_entries = sorted(entries,
                                            key=lambda x: x['votes_received'] or 0,
                                            reverse=True)
                    for e in sorted_entries[1:]:
                        delete_ids.add(e['id'])
            else:
                # No total_votes_cast: keep highest-voted
                sorted_entries = sorted(entries,
                                        key=lambda x: x['votes_received'] or 0,
                                        reverse=True)
                for e in sorted_entries[1:]:
                    delete_ids.add(e['id'])

        # For multi-race mixing: also remove single-entry candidates from wrong races
        if is_multi_race and total_votes and total_votes > 0:
            for cid, entries in by_cid.items():
                if len(entries) != 1:
                    continue
                e = entries[0]
                if e['id'] in delete_ids:
                    continue
                it = calc_implied_total(e['votes_received'], e['vote_percentage'])
                if it and pct_diff(it, total_votes) > TOLERANCE:
                    delete_ids.add(e['id'])

        if delete_ids:
            deleted = [c for c in cands if c['id'] in delete_ids]
            kept = [c for c in cands if c['id'] not in delete_ids]
            mix_type = 'multi-race' if is_multi_race else 'fusion'

            # Only warn if ALL Won candidacies are being deleted
            any_kept_won = any(k['result'] == 'Won' for k in kept)
            any_del_won = any(d['result'] == 'Won' for d in deleted)

            print(f'  {ei["state"]} {ei["seat_label"]} {ei["election_year"]} '
                  f'{ei["election_type"]} [{mix_type}]:')
            if any_del_won and not any_kept_won:
                print(f'    WARNING: No "Won" candidacy remaining after cleanup!')

            for d in deleted:
                print(f'    DEL: {d["full_name"]} ({d["party"]}, '
                      f'{d["votes_received"]:,})')
            for k in kept:
                print(f'    KEEP: {k["full_name"]} ({k["party"]}, '
                      f'{k["votes_received"]:,})')

            all_delete_ids.update(delete_ids)

    # Step 5: Batch delete
    print(f'\n  Total: {len(all_delete_ids)} candidacies '
          f'{"to delete" if dry_run else "deleted"}')
    if dry_run:
        print('  DRY RUN -- no changes made')
    else:
        delete_candidacies(all_delete_ids, dry_run)
        print('  Phase 2 complete.')


# ═══════════════════════════════════════════════════════════
# PHASE 3: Fix same-person duplicate candidacies
# Different candidate_id but same person (same party + same votes)
# (AR name variants, ND governor, etc.)
# ═══════════════════════════════════════════════════════════

def names_match(name_a, name_b):
    """Check if two candidate names plausibly refer to the same person.

    Returns True if they share a last name AND have a matching first initial or
    one first name is a prefix/expansion of the other (e.g. Fred/Fredrick).
    """
    def parse(name):
        parts = normalize_name(name).lower().split()
        if len(parts) >= 2:
            # Handle suffixes like Jr., Sr., III, IV
            suffixes = {'jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv', 'v'}
            last = parts[-1]
            if last in suffixes and len(parts) >= 3:
                last = parts[-2]
            return parts[0], last
        return (parts[0] if parts else '', '')

    first_a, last_a = parse(name_a)
    first_b, last_b = parse(name_b)

    if not last_a or not last_b or last_a != last_b:
        return False

    # Same last name — check first names
    if first_a == first_b:
        return True
    if first_a and first_b and first_a[0] == first_b[0]:
        # Same first initial: check if one is a prefix of the other
        # e.g., Fred/Fredrick, Josh/Joshua, Jim/James
        if first_a.startswith(first_b[:3]) or first_b.startswith(first_a[:3]):
            return True
    return False


def phase_3(dry_run):
    print('\n' + '=' * 60)
    print(' PHASE 3: Fix same-person duplicate candidacies')
    print('=' * 60 + '\n')

    # Only look at elections currently >110%
    problems = get_problem_elections(110)
    if not problems:
        print('  No >110% elections found.')
        return
    problem_eids = set(p['election_id'] for p in problems)
    eid_list = ','.join(str(e) for e in problem_eids)
    print(f'  Checking {len(problems)} elections currently >110%')

    pairs = run_sql(f"""
        SELECT c1.id as id_a, c2.id as id_b,
               c1.election_id, c1.candidate_id as cand_a, c2.candidate_id as cand_b,
               c1.party, c1.votes_received, c1.vote_percentage,
               c1.result as result_a, c2.result as result_b,
               ca1.full_name as name_a, ca2.full_name as name_b,
               e.election_year, s.seat_label, st.abbreviation as state,
               (SELECT COUNT(*) FROM candidacies WHERE candidate_id = c1.candidate_id) as total_a,
               (SELECT COUNT(*) FROM candidacies WHERE candidate_id = c2.candidate_id) as total_b
        FROM candidacies c1
        JOIN candidacies c2 ON c1.election_id = c2.election_id
            AND c1.party = c2.party
            AND c1.votes_received = c2.votes_received
            AND c1.votes_received > 0
            AND c1.id < c2.id
            AND c1.candidate_id != c2.candidate_id
        JOIN candidates ca1 ON c1.candidate_id = ca1.id
        JOIN candidates ca2 ON c2.candidate_id = ca2.id
        JOIN elections e ON c1.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE c1.election_id IN ({eid_list})
        ORDER BY st.abbreviation, e.election_year
    """)

    if not pairs:
        print('  No same-person duplicate candidacies found.')
        return

    # Filter to pairs where names actually match (same person, different spelling)
    matched_pairs = [p for p in pairs if names_match(p['name_a'], p['name_b'])]
    skipped = len(pairs) - len(matched_pairs)
    print(f'  Found {len(pairs)} same-party/same-votes pairs, '
          f'{len(matched_pairs)} with matching names ({skipped} skipped)\n')

    if not matched_pairs:
        print('  No name-matched duplicates found.')
        return

    delete_ids = set()
    orphan_cand_ids = []

    for p in matched_pairs:
        # Decide which to keep: prefer "Won", then more total candidacies
        if p['result_a'] == 'Won' and p['result_b'] != 'Won':
            keep_name, del_name = p['name_a'], p['name_b']
            del_id = p['id_b']
            del_cand_id, del_total = p['cand_b'], p['total_b']
        elif p['result_b'] == 'Won' and p['result_a'] != 'Won':
            keep_name, del_name = p['name_b'], p['name_a']
            del_id = p['id_a']
            del_cand_id, del_total = p['cand_a'], p['total_a']
        elif p['total_a'] >= p['total_b']:
            keep_name, del_name = p['name_a'], p['name_b']
            del_id = p['id_b']
            del_cand_id, del_total = p['cand_b'], p['total_b']
        else:
            keep_name, del_name = p['name_b'], p['name_a']
            del_id = p['id_a']
            del_cand_id, del_total = p['cand_a'], p['total_a']

        if del_id in delete_ids:
            continue  # Already marked from a previous pair

        print(f'    {p["state"]} {p["seat_label"]} {p["election_year"]} ({p["party"]}):')
        print(f'      Keep: {keep_name} ({p["votes_received"]:,} votes)')
        print(f'      Delete: {del_name}')

        delete_ids.add(del_id)
        if del_total <= 1:
            orphan_cand_ids.append(del_cand_id)

    print(f'\n  Candidacies to delete: {len(delete_ids)}')
    print(f'  Orphan candidates to clean up: {len(orphan_cand_ids)}')

    if dry_run:
        print('\n  DRY RUN -- no changes made')
        return

    delete_candidacies(delete_ids, dry_run)
    print(f'  Deleted {len(delete_ids)} duplicate candidacies')
    time.sleep(1)

    # Clean up orphan candidates (no remaining candidacies or seat_terms)
    cleaned = 0
    for cand_id in orphan_cand_ids:
        refs = run_sql(f"""
            SELECT (SELECT COUNT(*) FROM candidacies WHERE candidate_id = {cand_id}) as c,
                   (SELECT COUNT(*) FROM seat_terms WHERE candidate_id = {cand_id}) as t
        """)
        if refs and refs[0]['c'] == 0 and refs[0]['t'] == 0:
            run_sql(f'DELETE FROM candidates WHERE id = {cand_id};')
            cleaned += 1
        time.sleep(0.3)
    if cleaned:
        print(f'  Deleted {cleaned} orphan candidate records')


# ═══════════════════════════════════════════════════════════
# PHASE 4: Clean up remaining >110% elections
# Name-matching for CT/NY fusion, implied_total for others
# ═══════════════════════════════════════════════════════════

def phase_4(dry_run):
    print('\n' + '=' * 60)
    print(' PHASE 4: Clean up remaining >110% elections')
    print('=' * 60 + '\n')

    remaining = get_problem_elections(110)
    if not remaining:
        print('  All elections within normal range!')
        return

    print(f'  {len(remaining)} elections still >110%\n')

    # Bulk-fetch candidacies for all remaining problem elections
    eid_list = ','.join(str(r['election_id']) for r in remaining)
    all_cands = run_sql(f"""
        SELECT c.id, c.election_id, c.candidate_id, c.party, c.votes_received,
               c.vote_percentage, c.result, ca.full_name
        FROM candidacies c
        JOIN candidates ca ON c.candidate_id = ca.id
        WHERE c.election_id IN ({eid_list})
        ORDER BY c.election_id, c.votes_received DESC NULLS LAST
    """)
    if not all_cands:
        print('  Failed to fetch candidacies.')
        return

    cands_by_eid = defaultdict(list)
    for c in all_cands:
        cands_by_eid[c['election_id']].append(c)

    total_deleted = 0

    for r in remaining:
        eid = r['election_id']
        total_votes = r['total_votes_cast']
        cands = cands_by_eid.get(eid, [])
        if not cands:
            continue

        delete_ids = set()

        # Strategy 1: Name-based dedup
        by_norm = defaultdict(list)
        for c in cands:
            by_norm[normalize_name(c['full_name'])].append(c)

        for norm_name, entries in by_norm.items():
            if len(entries) <= 1:
                continue
            entries.sort(key=lambda x: x['votes_received'] or 0, reverse=True)
            for e in entries[1:]:
                delete_ids.add(e['id'])

        # Strategy 2: If no name dupes, try implied_total subset matching
        if not delete_ids and total_votes and total_votes > 0:
            matching = []
            non_matching = []
            for c in cands:
                it = calc_implied_total(c['votes_received'], c['vote_percentage'])
                if it and pct_diff(it, total_votes) <= 0.05:
                    matching.append(c)
                else:
                    non_matching.append(c)

            if matching and non_matching:
                match_pct = sum(float(c['vote_percentage']) for c in matching)
                if 90 <= match_pct <= 105:
                    for c in non_matching:
                        delete_ids.add(c['id'])

        print(f'  {r["state"]} {r["seat_label"]} {r["election_year"]} '
              f'{r["election_type"]}: {r["total_pct"]}%')

        if delete_ids:
            for c in cands:
                tag = 'DEL' if c['id'] in delete_ids else 'KEEP'
                print(f'    {tag}: {c["full_name"]} ({c["party"]}, '
                      f'{c["votes_received"]:,} votes)')

            any_kept_won = any(c['result'] == 'Won' for c in cands
                               if c['id'] not in delete_ids)
            any_del_won = any(c['result'] == 'Won' for c in cands
                              if c['id'] in delete_ids)
            if any_del_won and not any_kept_won:
                print(f'    WARNING: No "Won" candidacy remaining!')

            if not dry_run:
                delete_candidacies(delete_ids, dry_run)
                time.sleep(0.3)
            total_deleted += len(delete_ids)
        else:
            for c in cands:
                it = calc_implied_total(c['votes_received'], c['vote_percentage'])
                diff_pct = f'{pct_diff(it, total_votes)*100:.1f}%' if it else '?'
                print(f'    {c["full_name"]} ({c["party"]}, '
                      f'{c["votes_received"]:,}, impl_diff={diff_pct})')
            print(f'    -> No automated fix. Manual review needed.')

    print(f'\n  Total: {total_deleted} candidacies '
          f'{"to delete" if dry_run else "deleted"}')
    if dry_run:
        print('  DRY RUN -- no changes made')


# ═══════════════════════════════════════════════════════════

def show_verification():
    """Show final count of remaining >110% elections."""
    print('\n' + '=' * 60)
    print(' VERIFICATION')
    print('=' * 60 + '\n')

    remaining = get_problem_elections(110)
    if not remaining:
        print('  All elections <= 110%. Database is clean!')
    else:
        print(f'  {len(remaining)} elections still >110%:\n')
        for r in remaining:
            print(f'    {r["state"]} {r["seat_label"]} {r["election_year"]} '
                  f'{r["election_type"]}: {r["total_pct"]}% ({r["num_candidacies"]} cands)')


def main():
    parser = argparse.ArgumentParser(
        description='Fix elections with vote percentages summing to >100%%')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without executing')
    parser.add_argument('--phase', type=int, choices=[1, 2, 3, 4],
                        help='Run a specific phase only (1-4)')
    args = parser.parse_args()

    phases = [
        (1, phase_1),
        (2, phase_2),
        (3, phase_3),
        (4, phase_4),
    ]

    if args.phase:
        for num, fn in phases:
            if num == args.phase:
                fn(args.dry_run)
                break
    else:
        for num, fn in phases:
            fn(args.dry_run)

    if not args.dry_run:
        show_verification()

    print()


if __name__ == '__main__':
    main()
