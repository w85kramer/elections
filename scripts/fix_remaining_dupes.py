#!/usr/bin/env python3
"""Fix 20 remaining elections where vote percentages sum to >110%.

All are name-variant duplicates or party-name entries requiring manual SQL fixes.
Three groups:
  A) Same person, different name (15 elections) - delete the duplicate candidacy
  B) Party-name duplicates (4 elections, 5 deletions) - delete party-name entry
  C) Multi-race mixing (2 VA Senate elections) - delete entries from wrong race

Usage:
    python3 scripts/fix_remaining_dupes.py --dry-run   # Preview all fixes
    python3 scripts/fix_remaining_dupes.py              # Execute all fixes
"""

import argparse
import sys
import os
import time

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


def find_candidacy(candidate_name, state_abbr, seat_label_pattern, election_year,
                   election_type=None, party=None, votes=None):
    """Find a candidacy by candidate name, state, seat, and year.

    Returns dict with candidacy id, candidate_id, and other details, or None.
    """
    where_clauses = [
        f"ca.full_name = '{candidate_name.replace(chr(39), chr(39)+chr(39))}'",
        f"st.abbreviation = '{state_abbr}'",
        f"s.seat_label LIKE '{seat_label_pattern}'",
        f"e.election_year = {election_year}",
    ]
    if election_type:
        where_clauses.append(f"e.election_type = '{election_type}'")
    if party is not None:
        if party == '':
            where_clauses.append("(c.party IS NULL OR c.party = '')")
        else:
            where_clauses.append(f"c.party = '{party}'")
    if votes is not None:
        where_clauses.append(f"c.votes_received = {votes}")

    where = ' AND '.join(where_clauses)
    rows = run_sql(f"""
        SELECT c.id as candidacy_id, c.candidate_id, c.votes_received,
               c.vote_percentage, c.party, ca.full_name,
               e.id as election_id, e.election_year, e.election_type,
               s.seat_label, st.abbreviation as state
        FROM candidacies c
        JOIN candidates ca ON c.candidate_id = ca.id
        JOIN elections e ON c.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE {where}
        LIMIT 1
    """)
    if rows and len(rows) > 0:
        return rows[0]
    return None


def delete_candidacy(candidacy_id, dry_run):
    """Delete a single candidacy by ID."""
    if dry_run:
        return True
    result = run_sql(f'DELETE FROM candidacies WHERE id = {candidacy_id};')
    return result is not None


def cleanup_orphan_candidate(candidate_id, dry_run):
    """Delete a candidate if they have no remaining candidacies or seat_terms."""
    refs = run_sql(f"""
        SELECT (SELECT COUNT(*) FROM candidacies WHERE candidate_id = {candidate_id}) as c,
               (SELECT COUNT(*) FROM seat_terms WHERE candidate_id = {candidate_id}) as t
    """)
    if refs and refs[0]['c'] == 0 and refs[0]['t'] == 0:
        if dry_run:
            print(f'      Would delete orphan candidate id={candidate_id}')
            return True
        run_sql(f'DELETE FROM candidates WHERE id = {candidate_id};')
        print(f'      Deleted orphan candidate id={candidate_id}')
        return True
    else:
        if refs:
            print(f'      Candidate id={candidate_id} has {refs[0]["c"]} other candidacies, '
                  f'{refs[0]["t"]} seat_terms -- keeping')
        return False


def process_fix(fix, dry_run):
    """Process a single fix case. Returns True if successful."""
    desc = fix['desc']
    delete_name = fix['delete_name']
    state = fix['state']
    seat_pattern = fix['seat_pattern']
    year = fix['year']
    election_type = fix.get('election_type')
    party = fix.get('party')
    votes = fix.get('votes')

    print(f'\n  [{fix["group"]}] {desc}')

    # Find the candidacy to delete
    row = find_candidacy(delete_name, state, seat_pattern, year,
                         election_type=election_type, party=party, votes=votes)
    if not row:
        print(f'    ERROR: Could not find candidacy for "{delete_name}" '
              f'in {state} {seat_pattern} {year}')
        return False

    print(f'    Found: candidacy id={row["candidacy_id"]}, candidate_id={row["candidate_id"]}, '
          f'"{row["full_name"]}" ({row["party"]}, {row["votes_received"]:,} votes)')

    # Delete the candidacy
    if delete_candidacy(row['candidacy_id'], dry_run):
        action = 'Would delete' if dry_run else 'Deleted'
        print(f'    {action} candidacy id={row["candidacy_id"]}')
    else:
        print(f'    ERROR: Failed to delete candidacy id={row["candidacy_id"]}')
        return False

    # Check for orphan candidate
    if not dry_run:
        time.sleep(0.3)
    cleanup_orphan_candidate(row['candidate_id'], dry_run)

    if not dry_run:
        time.sleep(0.5)

    return True


def get_group_a_fixes():
    """Group A: Same person, different name - delete the duplicate candidacy."""
    return [
        {
            'group': 'A', 'num': 1,
            'desc': 'ND Governor 2008 - Delete "John H. Reed (Incumbent)" (dupe of John Hoeven)',
            'delete_name': 'John H. Reed (Incumbent)',
            'state': 'ND', 'seat_pattern': 'ND Governor', 'year': 2008,
            'election_type': 'General', 'party': 'R', 'votes': 235009,
        },
        {
            'group': 'A', 'num': 2,
            'desc': 'ND Governor 2004 - Delete "John H. Reed (Incumbent)" (dupe of John Hoeven)',
            'delete_name': 'John H. Reed (Incumbent)',
            'state': 'ND', 'seat_pattern': 'ND Governor', 'year': 2004,
            'election_type': 'General', 'party': 'R', 'votes': 220803,
        },
        {
            'group': 'A', 'num': 3,
            'desc': 'AK Governor 1970 - Delete "Bill Egan" (dupe of William A. Egan)',
            'delete_name': 'Bill Egan',
            'state': 'AK', 'seat_pattern': 'AK Governor', 'year': 1970,
            'party': 'D', 'votes': 42309,
        },
        {
            'group': 'A', 'num': 4,
            'desc': 'AK Governor 1962 - Delete "Bill Egan" (dupe of William A. Egan)',
            'delete_name': 'Bill Egan',
            'state': 'AK', 'seat_pattern': 'AK Governor', 'year': 1962,
            'party': 'D', 'votes': 29627,
        },
        {
            'group': 'A', 'num': 5,
            'desc': 'AK Governor 1974 - Delete "Bill Egan" (dupe of William A. Egan)',
            'delete_name': 'Bill Egan',
            'state': 'AK', 'seat_pattern': 'AK Governor', 'year': 1974,
            'party': 'D', 'votes': 45553,
        },
        {
            'group': 'A', 'num': 6,
            'desc': 'AR House 42 2016 - Delete "J.P. Bob Johnson" (dupe of Bob Johnson)',
            'delete_name': 'J.P. Bob Johnson',
            'state': 'AR', 'seat_pattern': 'AR House 42', 'year': 2016,
            'party': 'D', 'votes': 5100,
        },
        {
            'group': 'A', 'num': 7,
            'desc': 'SD Governor 1986 - Delete "Ralph Herseth" (dupe of Lars Herseth)',
            'delete_name': 'Ralph Herseth',
            'state': 'SD', 'seat_pattern': 'SD Governor', 'year': 1986,
            'party': 'D', 'votes': 141898,
        },
        {
            'group': 'A', 'num': 8,
            'desc': 'WA Governor 1964 - Delete "Albert D. Rosellini (Incumbent)" (dupe of Albert Rosellini)',
            'delete_name': 'Albert D. Rosellini (Incumbent)',
            'state': 'WA', 'seat_pattern': 'WA Governor', 'year': 1964,
        },
        {
            'group': 'A', 'num': 9,
            'desc': 'AR Senate 27 2022 - Delete "Becky Ward" (dupe of Rebecca Ward)',
            'delete_name': 'Becky Ward',
            'state': 'AR', 'seat_pattern': 'AR Senate 27', 'year': 2022,
            'party': 'D', 'votes': 6741,
        },
        {
            'group': 'A', 'num': 10,
            'desc': 'AR House 97 2014 - Delete "Charles Sonny Carter" (dupe of Sonny Carter)',
            'delete_name': 'Charles Sonny Carter',
            'state': 'AR', 'seat_pattern': 'AR House 97', 'year': 2014,
            'party': 'D', 'votes': 3384,
        },
        {
            'group': 'A', 'num': 11,
            'desc': 'AR House 83 2018 Primary_R - Delete "Sheriff Keith Slape" (dupe of Keith Slape)',
            'delete_name': 'Sheriff Keith Slape',
            'state': 'AR', 'seat_pattern': 'AR House 83', 'year': 2018,
            'election_type': 'Primary_R', 'party': 'R', 'votes': 1865,
        },
        {
            'group': 'A', 'num': 12,
            'desc': 'AR House 56 2024 Primary_D - Delete "Queen Lakeslia Mosley" (dupe of Lakeslia Mosley)',
            'delete_name': 'Queen Lakeslia Mosley',
            'state': 'AR', 'seat_pattern': 'AR House 56', 'year': 2024,
            'election_type': 'Primary_D', 'party': 'D', 'votes': 318,
        },
        {
            'group': 'A', 'num': 13,
            'desc': 'AR House 35 2022 Primary_D - Delete "Johnson, Demetris Jr." (dupe of Demetris Johnson Jr.)',
            'delete_name': 'Johnson, Demetris Jr.',
            'state': 'AR', 'seat_pattern': 'AR House 35', 'year': 2022,
            'election_type': 'Primary_D', 'party': 'D', 'votes': 327,
        },
        {
            'group': 'A', 'num': 14,
            'desc': 'AR Senate 18 2022 - Delete "Nicholas Cartwright" (fewer votes than Nick Cartwright)',
            'delete_name': 'Nicholas Cartwright',
            'state': 'AR', 'seat_pattern': 'AR Senate 18', 'year': 2022,
            'party': 'D', 'votes': 4383,
        },
        {
            'group': 'A', 'num': 15,
            'desc': 'AR Senate 20 2014 - Delete "Robert Thompson" (data entry error, dupe of Robert Johnson)',
            'delete_name': 'Robert Thompson',
            'state': 'AR', 'seat_pattern': 'AR Senate 20', 'year': 2014,
            'party': 'D', 'votes': 10405,
        },
    ]


def get_group_b_fixes():
    """Group B: Party-name duplicates - delete the party-name candidacy entry."""
    return [
        {
            'group': 'B', 'num': 16,
            'desc': 'HI Governor 1994 - Delete "Best Party of Hawaii" (dupe of Frank Fasi)',
            'delete_name': 'Best Party of Hawaii',
            'state': 'HI', 'seat_pattern': 'HI Governor', 'year': 1994,
            'votes': 113158,
        },
        {
            'group': 'B', 'num': 17,
            'desc': 'HI Governor 1982 - Delete "Independent Democrat" (dupe of Frank Fasi)',
            'delete_name': 'Independent Democrat',
            'state': 'HI', 'seat_pattern': 'HI Governor', 'year': 1982,
            'votes': 89303,
        },
        {
            'group': 'B', 'num': 18,
            'desc': 'AK Governor 1998 - Delete "Republican Write-in" (dupe of Robin L. Taylor)',
            'delete_name': 'Republican Write-in',
            'state': 'AK', 'seat_pattern': 'AK Governor', 'year': 1998,
            'votes': 40209,
        },
        {
            'group': 'B', 'num': '18b',
            'desc': 'AK Governor 1998 - Delete "Republican Moderate" (dupe of Ray Metcalfe)',
            'delete_name': 'Republican Moderate',
            'state': 'AK', 'seat_pattern': 'AK Governor', 'year': 1998,
            'votes': 13540,
        },
        {
            'group': 'B', 'num': 19,
            'desc': 'AK Governor 1970 - Delete "American Independent" (dupe of Ralph Anderson)',
            'delete_name': 'American Independent',
            'state': 'AK', 'seat_pattern': 'AK Governor', 'year': 1970,
            'votes': 1206,
        },
    ]


def process_group_c(dry_run):
    """Group C: VA Senate multi-race mixing - use implied_total to determine which
    candidacies belong to the election's total_votes_cast and delete the rest."""
    print('\n' + '=' * 60)
    print(' GROUP C: VA Senate multi-race mixing (implied_total)')
    print('=' * 60)

    va_cases = [
        {'seat_pattern': 'VA Senate 1', 'year': 2011},
        {'seat_pattern': 'VA Senate 35', 'year': 2011},
    ]

    total_deleted = 0
    total_orphans = 0

    for case in va_cases:
        seat = case['seat_pattern']
        year = case['year']
        print(f'\n  {seat} {year}:')

        # Get the election details and candidacies
        rows = run_sql(f"""
            SELECT e.id as election_id, e.total_votes_cast,
                   c.id as candidacy_id, c.candidate_id, c.votes_received,
                   c.vote_percentage, c.party, c.result,
                   ca.full_name
            FROM elections e
            JOIN seats s ON e.seat_id = s.id
            JOIN districts d ON s.district_id = d.id
            JOIN states st ON d.state_id = st.id
            JOIN candidacies c ON c.election_id = e.id
            JOIN candidates ca ON c.candidate_id = ca.id
            WHERE st.abbreviation = 'VA'
              AND s.seat_label = '{seat}'
              AND e.election_year = {year}
              AND e.election_type = 'General'
            ORDER BY c.votes_received DESC NULLS LAST
        """)

        if not rows:
            print(f'    ERROR: No candidacies found for {seat} {year}')
            continue

        total_votes = rows[0]['total_votes_cast']
        print(f'    total_votes_cast = {total_votes:,}')
        print(f'    Candidacies:')

        # For each candidacy, compute implied_total
        keep_ids = set()
        delete_ids = set()

        for r in rows:
            votes = r['votes_received'] or 0
            pct = float(r['vote_percentage']) if r['vote_percentage'] else 0
            if pct > 0:
                implied_total = votes / (pct / 100)
            else:
                implied_total = None

            # Check if implied_total is close to total_votes_cast (within 5%)
            if implied_total and total_votes and total_votes > 0:
                diff = abs(implied_total - total_votes) / total_votes
                matches = diff <= 0.05
            else:
                matches = False

            tag = 'MATCH' if matches else 'MISMATCH'
            impl_str = f'{implied_total:,.0f}' if implied_total else '?'
            diff_str = f'{diff*100:.1f}%' if implied_total and total_votes else '?'
            print(f'      {r["full_name"]} ({r["party"]}, {votes:,} votes, '
                  f'{pct}%) implied={impl_str} diff={diff_str} [{tag}]')

            if matches:
                keep_ids.add(r['candidacy_id'])
            else:
                delete_ids.add(r['candidacy_id'])

        # Verify: the kept candidacies should sum to ~100%
        kept_pct = sum(float(r['vote_percentage']) for r in rows
                       if r['candidacy_id'] in keep_ids and r['vote_percentage'])
        print(f'    Kept candidacies sum to {kept_pct:.1f}%')

        if kept_pct < 90 or kept_pct > 105:
            print(f'    WARNING: Kept sum looks wrong ({kept_pct:.1f}%), skipping!')
            continue

        if not delete_ids:
            print(f'    No candidacies to delete.')
            continue

        for r in rows:
            if r['candidacy_id'] in delete_ids:
                action = 'Would delete' if dry_run else 'Deleting'
                print(f'    {action}: candidacy id={r["candidacy_id"]} '
                      f'"{r["full_name"]}" ({r["party"]}, {r["votes_received"]:,})')

                if not dry_run:
                    run_sql(f'DELETE FROM candidacies WHERE id = {r["candidacy_id"]};')
                    time.sleep(0.3)
                total_deleted += 1

                # Clean up orphan candidate
                if not dry_run:
                    time.sleep(0.3)
                if cleanup_orphan_candidate(r['candidate_id'], dry_run):
                    total_orphans += 1

                if not dry_run:
                    time.sleep(0.5)

    return total_deleted, total_orphans


def show_verification():
    """Show count of remaining >110% elections."""
    print('\n' + '=' * 60)
    print(' VERIFICATION')
    print('=' * 60 + '\n')

    rows = run_sql("""
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
        HAVING SUM(c.vote_percentage) > 110
        ORDER BY SUM(c.vote_percentage) DESC
    """)

    if not rows:
        print('  All elections <= 110%. Database is clean!')
    else:
        print(f'  {len(rows)} elections still >110%:\n')
        for r in rows:
            print(f'    {r["state"]} {r["seat_label"]} {r["election_year"]} '
                  f'{r["election_type"]}: {r["total_pct"]}% ({r["num_candidacies"]} cands)')


def main():
    parser = argparse.ArgumentParser(
        description='Fix 20 remaining elections with >110%% vote percentages')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without executing')
    args = parser.parse_args()

    print('=' * 60)
    print(' Fix Remaining Duplicate Candidacies (20 elections)')
    print('=' * 60)
    if args.dry_run:
        print(' ** DRY RUN MODE **')

    # ── Group A: Same person, different name ──
    print('\n' + '=' * 60)
    print(' GROUP A: Same person, different name (15 elections)')
    print('=' * 60)

    group_a = get_group_a_fixes()
    a_success = 0
    a_fail = 0
    for fix in group_a:
        if process_fix(fix, args.dry_run):
            a_success += 1
        else:
            a_fail += 1

    print(f'\n  Group A: {a_success} succeeded, {a_fail} failed')

    # ── Group B: Party-name duplicates ──
    print('\n' + '=' * 60)
    print(' GROUP B: Party-name duplicates (4 elections, 5 deletions)')
    print('=' * 60)

    group_b = get_group_b_fixes()
    b_success = 0
    b_fail = 0
    for fix in group_b:
        if process_fix(fix, args.dry_run):
            b_success += 1
        else:
            b_fail += 1

    print(f'\n  Group B: {b_success} succeeded, {b_fail} failed')

    # ── Group C: VA Senate multi-race mixing ──
    c_deleted, c_orphans = process_group_c(args.dry_run)

    # ── Summary ──
    print('\n' + '=' * 60)
    print(' SUMMARY')
    print('=' * 60)
    total = a_success + b_success + c_deleted
    print(f'  Group A: {a_success}/{len(group_a)} fixes applied')
    print(f'  Group B: {b_success}/{len(group_b)} fixes applied')
    print(f'  Group C: {c_deleted} candidacies removed, {c_orphans} orphans cleaned')
    print(f'  Total candidacies {"to delete" if args.dry_run else "deleted"}: '
          f'{total}')

    if args.dry_run:
        print('\n  DRY RUN -- no changes were made')
    else:
        show_verification()

    print()


if __name__ == '__main__':
    main()
