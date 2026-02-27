#!/usr/bin/env python3
"""
Fill missing governor General elections (2015–2024) and populate from Wikipedia.

Most states have historical governor elections through ~2014 and then jump to 2026.
This script creates the missing election records and populates them with Wikipedia
vote data (candidates, votes, percentages).

Usage:
    python3 scripts/fill_missing_governor_elections.py --dry-run
    python3 scripts/fill_missing_governor_elections.py --state VT
    python3 scripts/fill_missing_governor_elections.py --min-year 2020
    python3 scripts/fill_missing_governor_elections.py
"""

import sys
import os
import re
import time
import argparse

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from scripts.backfill_governor_votes import (
    STATE_NAMES, PARTY_MAP, run_sql, esc, split_name, clean_name,
    parse_election_box_templates, parse_wikitable_results,
    find_candidate, name_match,
    WP_API, WP_HEADERS,
)

# Election day: Tuesday after first Monday in November
ELECTION_DATES = {
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


PARTY_CLEANUP = {
    'democratic': 'D', 'democrat': 'D', 'dem': 'D',
    'republican': 'R', 'rep': 'R', 'gop': 'R',
    'libertarian': 'L', 'lib': 'L',
    'green': 'G', 'gre': 'G',
    'independent': 'I', 'ind': 'I',
    'constitution': 'Constitution',
    'no party preference': 'NP', 'nonpartisan': 'NP',
    'progressive': 'Progressive', 'pro': 'Progressive',
    'ver': 'R',  # Vermont Republican Party truncated
    'cal': '',   # California jungle primary — party unknown from truncation
    'was': '',   # Washington top-two primary
    'col': 'R',  # Colorado Republican/Democratic truncated — ambiguous
    'ore': '',   # Oregon truncated
}


def clean_party(party_str, candidate_name=''):
    """Normalize party abbreviation. Handles wikitable parser's raw party strings."""
    if not party_str:
        return ''
    p = party_str.strip()
    # Already a clean abbreviation
    if p in ('D', 'R', 'L', 'G', 'I', 'NP', 'Constitution', 'Progressive', 'Reform'):
        return p
    # Check cleanup map
    lower = p.lower().strip('[]')
    if lower in PARTY_CLEANUP:
        return PARTY_CLEANUP[lower]
    # Substring matching for state-specific party names
    if 'democrat' in lower:
        return 'D'
    if 'republican' in lower:
        return 'R'
    if 'libertarian' in lower:
        return 'L'
    if 'green' in lower:
        return 'G'
    if 'independent' in lower:
        return 'I'
    if 'progressive' in lower:
        return 'Progressive'
    if 'constitution' in lower:
        return 'Constitution'
    # Strip wiki link artifacts
    if '[[' in p or ']]' in p or len(p) > 15:
        return ''
    return p


def clean_wiki_candidate_name(name):
    """Extra cleanup for garbled Wikipedia names (CSS artifacts, ticket concatenation)."""
    # Strip CSS style blocks (e.g. .mw-parser-output ... { ... })
    name = re.sub(r'\.mw-parser-output[^}]*\}[^A-Z]*', '', name)
    # Strip remaining HTML/CSS fragments
    name = re.sub(r'<[^>]+>', '', name)
    # Handle governor/lt.gov ticket concatenation (e.g. "Daniel CameronRobby Mills")
    # Look for lowercase letter immediately followed by uppercase (no space)
    name = re.sub(r'([a-z])([A-Z])', r'\1 \2', name)
    # Now split: take only governor name (first two or three words before running mate)
    # Running mates are typically the 3rd+ name in a ticket
    parts = name.strip().split()
    if len(parts) > 3:
        # Keep first + last name (2 words), or first + middle + last (3 words w/ suffix)
        suffixes = {'jr.', 'sr.', 'ii', 'iii', 'iv', 'jr', 'sr', 'v'}
        if len(parts) > 2 and parts[2].lower().rstrip('.') in suffixes:
            name = ' '.join(parts[:3])
        else:
            name = ' '.join(parts[:2])
    return name.strip()


def fetch_wiki_governor(state_abbrev, year):
    """Fetch governor election from Wikipedia with improved validation.

    Tries Election Box parser first, validates the result (must look like a real
    general election), then falls back to wikitable parser if needed.
    """
    state_name = STATE_NAMES[state_abbrev]
    title = f'{year}_{state_name}_gubernatorial_election'

    try:
        resp = requests.get(WP_API + title, headers=WP_HEADERS, timeout=30)
    except requests.RequestException as e:
        print(f'network error: {e}')
        return None

    if resp.status_code == 404:
        title2 = f'{year}_{state_name}_governor%27s_race'
        try:
            resp = requests.get(WP_API + title2, headers=WP_HEADERS, timeout=30)
        except requests.RequestException:
            pass
        if resp.status_code != 200:
            return None

    if resp.status_code != 200:
        return None

    html = resp.text

    # Try Election Box first
    result = parse_election_box_templates(html)

    # Validate: must have decent total votes, 2+ candidates, and top candidate
    # must have a reasonable share of votes (>10% of total) — catches cases where
    # the parser found the right total but wrong candidate list (e.g. write-in section)
    if result and result['total'] and result['total'] > 50000 and len(result['candidates']) >= 2:
        top_votes = max(c['votes'] for c in result['candidates'])
        if top_votes > result['total'] * 0.10:
            for c in result['candidates']:
                c['name'] = clean_wiki_candidate_name(c['name'])
            return result

    # Fallback to wikitable parser
    result = parse_wikitable_results(html)
    if result and result['candidates']:
        for c in result['candidates']:
            c['name'] = clean_wiki_candidate_name(c['name'])
        return result

    return None


def get_governor_seats():
    """Get all governor seats with their state info and election cycle."""
    rows = run_sql("""
        SELECT se.id as seat_id, s.abbreviation as state,
               se.term_length_years as term_length, se.next_regular_election_year
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE se.office_type = 'Governor'
        ORDER BY s.abbreviation
    """)
    if not rows:
        print('ERROR: Failed to fetch governor seats')
        sys.exit(1)
    return {r['state']: r for r in rows}


def get_existing_election_years():
    """Get all existing governor General election years per state."""
    rows = run_sql("""
        SELECT s.abbreviation as state, e.election_year
        FROM elections e
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE se.office_type = 'Governor' AND e.election_type = 'General'
        ORDER BY s.abbreviation, e.election_year
    """)
    if rows is None:
        print('ERROR: Failed to fetch existing elections')
        sys.exit(1)
    result = {}
    for r in rows:
        result.setdefault(r['state'], set()).add(r['election_year'])
    return result


def compute_missing_years(seats, existing_years, min_year=2014, max_year=2024):
    """Compute which governor election years are missing per state."""
    missing = []
    for state, seat in sorted(seats.items()):
        existing = existing_years.get(state, set())
        term = seat['term_length']
        next_yr = seat['next_regular_election_year']

        # Walk backward from the most recent cycle before 2026
        # to find all expected election years
        year = next_yr
        while year > 2026:
            year -= term
        # Now year is the most recent election year <= 2026
        # But we want the one before 2026 (the most recent past election)
        if year == 2026:
            year -= term

        expected = []
        while year >= min_year:
            if year <= max_year and year not in existing:
                expected.append(year)
            year -= term

        for y in sorted(expected):
            missing.append((state, y, seat['seat_id']))

    return missing


def load_candidate_cache():
    """Pre-load all candidates for name matching."""
    rows = run_sql("SELECT id, full_name FROM candidates ORDER BY id")
    if not rows:
        print('ERROR: Failed to load candidates')
        sys.exit(1)
    cache = {}
    for r in rows:
        cache[r['full_name'].lower().strip()] = {
            'id': r['id'], 'full_name': r['full_name']
        }
    return cache


def main():
    parser = argparse.ArgumentParser(
        description='Fill missing governor elections (2015-2024) from Wikipedia')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    parser.add_argument('--state', type=str, help='Process only this state (e.g. VT)')
    parser.add_argument('--min-year', type=int, default=2014,
                        help='Only fill from this year onward (default: 2014)')
    args = parser.parse_args()

    # Step 1: Get governor seats and existing elections
    print('Loading governor seats...')
    seats = get_governor_seats()
    print(f'  {len(seats)} governor seats found')

    print('Loading existing governor elections...')
    existing = get_existing_election_years()

    # Step 2: Compute missing elections
    missing = compute_missing_years(seats, existing, min_year=args.min_year)
    if args.state:
        missing = [(s, y, sid) for s, y, sid in missing if s == args.state.upper()]

    print(f'\n{len(missing)} missing elections to create:')
    for state, year, _ in missing:
        print(f'  {state} {year}')
    print()

    if not missing:
        print('Nothing to do!')
        return

    # Step 3: Load candidate cache
    print('Loading candidate cache...')
    candidate_cache = load_candidate_cache()
    print(f'  {len(candidate_cache)} candidates cached\n')

    # Step 4: Process each missing election
    elections_created = 0
    candidates_created = 0
    candidacies_created = 0
    wiki_failures = []

    for state, year, seat_id in missing:
        date = ELECTION_DATES.get(year)
        if not date:
            print(f'  {state} {year}: SKIP — no election date mapping')
            wiki_failures.append(f'{state} {year} (no date)')
            continue

        print(f'  {state} {year}...', end=' ', flush=True)

        # Fetch Wikipedia data first — skip if no data
        wiki = fetch_wiki_governor(state, year)
        time.sleep(1)

        if wiki is None or not wiki['candidates']:
            print('no Wikipedia data')
            wiki_failures.append(f'{state} {year}')
            continue

        total_votes = wiki['total']

        # Sanity checks — filter bad parses
        # 1. Total votes too low = probably parsed wrong section (e.g. LtGov primary)
        if total_votes and total_votes < 50000:
            print(f'SKIP — total votes too low ({total_votes:,}), likely wrong section')
            wiki_failures.append(f'{state} {year} (low votes: {total_votes:,})')
            continue

        # 2. Filter out minor candidates (<1%) and garbled names
        wiki_candidates = [
            c for c in wiki['candidates']
            if len(c['name']) >= 3
            and '}}' not in c['name']
            and '{' not in c['name']
            and (c['pct'] is None or c['pct'] >= 1.0)
        ]

        if not wiki_candidates:
            print('no valid candidates after filtering')
            wiki_failures.append(f'{state} {year} (no valid candidates)')
            continue

        # 3. Must have at least 2 candidates for a general election
        if len(wiki_candidates) < 2:
            print(f'SKIP — only {len(wiki_candidates)} candidate(s)')
            wiki_failures.append(f'{state} {year} (only {len(wiki_candidates)} candidate)')
            continue

        print(f'{len(wiki_candidates)} candidates, {total_votes:,} total votes')

        if args.dry_run:
            elections_created += 1
            for wc in wiki_candidates:
                cand_id = find_candidate(wc['name'], candidate_cache)
                if cand_id is None:
                    candidates_created += 1
                candidacies_created += 1
            continue

        # Create election record
        result = run_sql(f"""
            INSERT INTO elections (seat_id, election_date, election_year, election_type, result_status)
            VALUES ({seat_id}, '{date}', {year}, 'General', 'Certified')
            RETURNING id
        """)
        if not result or len(result) == 0:
            print(f'    FAILED to create election for {state} {year}')
            wiki_failures.append(f'{state} {year} (insert failed)')
            continue

        election_id = result[0]['id']
        elections_created += 1

        # Process each candidate
        candidacy_values = []
        for wc in wiki_candidates:
            clean = wc['name'].strip()
            if not clean or len(clean) < 3:
                continue

            # Find or create candidate
            cand_id = find_candidate(clean, candidate_cache)
            if cand_id is None:
                first_name, last_name = split_name(clean)
                cand_result = run_sql(f"""
                    INSERT INTO candidates (full_name, last_name, first_name)
                    VALUES ('{esc(clean)}', '{esc(last_name)}', '{esc(first_name)}')
                    RETURNING id
                """)
                if cand_result and len(cand_result) > 0:
                    cand_id = cand_result[0]['id']
                    candidate_cache[clean.lower()] = {'id': cand_id, 'full_name': clean}
                    candidates_created += 1
                else:
                    print(f'    FAILED to create candidate: {clean}')
                    continue

            party = clean_party(wc['party'] or '', clean)
            caucus = party
            result_val = 'Won' if wc['is_winner'] else 'Lost'
            pct_val = str(wc['pct']) if wc['pct'] is not None else 'NULL'

            candidacy_values.append(
                f"({election_id}, {cand_id}, '{esc(party)}', '{esc(caucus)}', "
                f"{wc['votes']}, {pct_val}, '{result_val}', false, false)"
            )

        # Batch insert candidacies
        if candidacy_values:
            sql = f"""
                INSERT INTO candidacies
                (election_id, candidate_id, party, caucus, votes_received,
                 vote_percentage, result, is_incumbent, is_write_in)
                VALUES {', '.join(candidacy_values)}
            """
            run_sql(sql)
            candidacies_created += len(candidacy_values)

        # Update election total votes
        if total_votes:
            run_sql(f"""
                UPDATE elections SET total_votes_cast = {total_votes}
                WHERE id = {election_id}
            """)

        print(f'    -> election #{election_id}, {len(candidacy_values)} candidacies')
        time.sleep(1)

    # Summary
    print(f'\n{"DRY RUN — " if args.dry_run else ""}SUMMARY:')
    print(f'  Elections created: {elections_created}')
    print(f'  Candidates created: {candidates_created}')
    print(f'  Candidacies created: {candidacies_created}')
    if wiki_failures:
        print(f'  Wikipedia failures ({len(wiki_failures)}):')
        for f in wiki_failures:
            print(f'    {f}')


if __name__ == '__main__':
    main()
