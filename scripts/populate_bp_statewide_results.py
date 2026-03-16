#!/usr/bin/env python3
"""
Populate statewide election results from Ballotpedia scrape data.

Reads /tmp/bp_statewide_results.json and:
1. For elections with a winner candidacy but no votes — adds vote data
2. For elections missing opponent candidacies — adds them
3. Calculates vote percentages where missing

Usage:
    python3 scripts/populate_bp_statewide_results.py --dry-run
    python3 scripts/populate_bp_statewide_results.py
"""

import json
import os
import re
import sys
import time
import requests
from pathlib import Path

# Load env
env_path = Path(__file__).parent.parent / '.env'
env = {}
with open(env_path) as f:
    for line in f:
        line = line.strip()
        if '=' in line and not line.startswith('#'):
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()

TOKEN = env['SUPABASE_MANAGEMENT_TOKEN']
PROJECT_REF = 'pikcvwulzfxgwfcfssxc'
API_URL = f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query'
DRY_RUN = '--dry-run' in sys.argv


def run_sql(query, label=""):
    if DRY_RUN:
        print(f"[DRY RUN] {label}: {query[:150]}...")
        return None
    for attempt in range(1, 6):
        r = requests.post(API_URL,
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query})
        if r.status_code in (200, 201):
            try:
                return r.json()
            except:
                return []
        if r.status_code == 429:
            wait = 5 * attempt
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        print(f"  ERROR ({r.status_code}): {r.text[:300]}")
        return None
    print(f"  Failed after 5 retries: {label}")
    return None


def run_sql_read(query, label=""):
    for attempt in range(1, 6):
        r = requests.post(API_URL,
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query})
        if r.status_code in (200, 201):
            try:
                return r.json()
            except:
                return []
        if r.status_code == 429:
            time.sleep(5 * attempt)
            continue
        print(f"  ERROR ({r.status_code}): {r.text[:200]}")
        return None
    return None


def esc(s):
    return s.replace("'", "''")


def normalize_name(name):
    """Normalize name for matching: lowercase, strip common suffixes."""
    n = name.lower().strip()
    n = re.sub(r'\s+', ' ', n)
    n = n.replace(',', '').replace('.', '')
    for suf in [' jr', ' sr', ' ii', ' iii', ' iv']:
        if n.endswith(suf):
            n = n[:-len(suf)].strip()
    return n


def last_name(full_name):
    """Extract last name for matching."""
    parts = full_name.strip().split()
    if not parts:
        return ''
    # Skip suffixes
    suffixes = {'jr', 'sr', 'ii', 'iii', 'iv', 'jr.', 'sr.'}
    while parts and parts[-1].lower().rstrip(',') in suffixes:
        parts.pop()
    return parts[-1].lower() if parts else ''


def match_candidate(bp_name, db_candidates):
    """Try to match a BP candidate name to a DB candidate.

    Returns candidate_id or None.
    """
    bp_norm = normalize_name(bp_name)
    bp_last = last_name(bp_name)

    # Exact full name match
    for c in db_candidates:
        if normalize_name(c['full_name']) == bp_norm:
            return c['id']

    # Last name match (if unique)
    last_matches = [c for c in db_candidates if last_name(c['full_name']) == bp_last]
    if len(last_matches) == 1:
        return last_matches[0]['id']

    # Substring match
    for c in db_candidates:
        db_norm = normalize_name(c['full_name'])
        if bp_last and len(bp_last) >= 3 and (bp_last in db_norm or db_norm in bp_norm):
            return c['id']

    return None


def main():
    print(f"{'[DRY RUN] ' if DRY_RUN else ''}Populate BP Statewide Results")
    print("=" * 60)

    # Load BP results
    input_path = '/tmp/bp_statewide_results.json'
    if not os.path.exists(input_path):
        print(f"ERROR: {input_path} not found. Run download_bp_statewide_results.py first.")
        return

    with open(input_path) as f:
        bp_results = json.load(f)

    # Filter to elections with candidates
    bp_with_data = [r for r in bp_results if r.get('candidates')]
    bp_failed = [r for r in bp_results if not r.get('candidates') and not r.get('error')]
    print(f"BP results: {len(bp_results)} total, {len(bp_with_data)} with data, {len(bp_failed)} no data")

    # Get existing candidacies for these elections
    election_ids = [r['election_id'] for r in bp_with_data]
    if not election_ids:
        print("No elections to process")
        return

    # Process in chunks since there could be many
    existing_candidacies = {}  # election_id -> [candidacy rows]
    chunk_size = 100
    for i in range(0, len(election_ids), chunk_size):
        chunk = election_ids[i:i+chunk_size]
        ids_str = ','.join(str(x) for x in chunk)
        rows = run_sql_read(f"""
            SELECT cy.id as cy_id, cy.election_id, cy.candidate_id, cy.party,
                   cy.votes_received, cy.vote_percentage, cy.result,
                   c.full_name, c.first_name, c.last_name
            FROM candidacies cy
            JOIN candidates c ON cy.candidate_id = c.id
            WHERE cy.election_id IN ({ids_str})
        """)
        if rows:
            for row in rows:
                eid = row['election_id']
                if eid not in existing_candidacies:
                    existing_candidacies[eid] = []
                existing_candidacies[eid].append(row)

    print(f"Existing candidacies loaded for {len(existing_candidacies)} elections")

    # Process each election
    votes_updated = 0
    opponents_added = 0
    winners_added = 0
    candidates_created = 0
    errors = 0

    for bp in bp_with_data:
        eid = bp['election_id']
        state = bp['state']
        office = bp['office']
        year = bp['year']
        bp_candidates = bp['candidates']
        db_cands = existing_candidacies.get(eid, [])

        if not bp_candidates:
            continue

        # Calculate total votes for percentages
        total_votes = sum(c.get('votes') or 0 for c in bp_candidates)

        for bp_cand in bp_candidates:
            bp_name = bp_cand['name']

            # Skip placeholder/aggregate entries
            skip_names = {'other/write-in votes', 'submit photo', 'write-in', 'write-in votes',
                          'other candidates', 'scattered', 'none of these candidates'}
            if bp_name.lower().strip() in skip_names:
                continue

            bp_party = bp_cand.get('party')
            bp_votes = bp_cand.get('votes')
            bp_pct = bp_cand.get('pct')
            bp_won = bp_cand.get('won', False)

            # Calculate percentage if we have votes
            if bp_pct is None and bp_votes and total_votes:
                bp_pct = round(100.0 * bp_votes / total_votes, 1)

            # Try to match to existing candidacy
            matched_cy = None
            for cy in db_cands:
                if normalize_name(cy['full_name']) == normalize_name(bp_name):
                    matched_cy = cy
                    break
                if last_name(cy['full_name']) == last_name(bp_name):
                    matched_cy = cy
                    break

            if matched_cy:
                # Update votes if missing
                if bp_votes and not matched_cy.get('votes_received'):
                    pct_clause = f", vote_percentage = {bp_pct}" if bp_pct else ""
                    sql = f"UPDATE candidacies SET votes_received = {bp_votes}{pct_clause} WHERE id = {matched_cy['cy_id']}"
                    run_sql(sql, f"Update votes {state} {office} {year} {bp_name}")
                    votes_updated += 1
            else:
                # New candidate — need to find or create in candidates table
                # First search for existing candidate
                search_name = esc(bp_name)
                search_last = esc(last_name(bp_name))
                found = run_sql_read(f"""
                    SELECT id, full_name FROM candidates
                    WHERE lower(full_name) = lower('{search_name}')
                    OR (lower(last_name) = lower('{search_last}')
                        AND lower(first_name) = lower('{esc(bp_name.split()[0])}'))
                    LIMIT 5
                """)

                candidate_id = None
                if found:
                    candidate_id = found[0]['id']

                if not candidate_id:
                    # Create new candidate
                    parts = bp_name.split()
                    first = esc(parts[0]) if parts else ''
                    last_n = esc(' '.join(parts[1:])) if len(parts) > 1 else ''
                    full = esc(bp_name)

                    result = run_sql(f"""
                        INSERT INTO candidates (full_name, first_name, last_name)
                        VALUES ('{full}', '{first}', '{last_n}')
                        RETURNING id
                    """, f"Create candidate {bp_name}")
                    if result and isinstance(result, list) and result:
                        candidate_id = result[0]['id']
                        candidates_created += 1
                    else:
                        errors += 1
                        continue

                if candidate_id:
                    # Create candidacy
                    party_val = f"'{esc(bp_party)}'" if bp_party else "'Unknown'"
                    result_val = "'Won'" if bp_won else "'Lost'"
                    votes_clause = f", votes_received = {bp_votes}" if bp_votes else ""
                    pct_clause = f", vote_percentage = {bp_pct}" if bp_pct else ""

                    sql = f"""
                        INSERT INTO candidacies (candidate_id, election_id, party, candidate_status, result
                            {', votes_received' if bp_votes else ''}{', vote_percentage' if bp_pct else ''})
                        VALUES ({candidate_id}, {eid}, {party_val}, 'Active', {result_val}
                            {f', {bp_votes}' if bp_votes else ''}{f', {bp_pct}' if bp_pct else ''})
                    """
                    run_sql(sql, f"Add candidacy {state} {office} {year} {bp_name}")

                    if bp_won:
                        winners_added += 1
                    else:
                        opponents_added += 1

        # Rate limit
        time.sleep(0.5)

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"  Votes updated on existing candidacies: {votes_updated}")
    print(f"  Winner candidacies added: {winners_added}")
    print(f"  Opponent candidacies added: {opponents_added}")
    print(f"  New candidates created: {candidates_created}")
    print(f"  Errors: {errors}")
    if DRY_RUN:
        print(f"\n  [DRY RUN — no changes made]")


if __name__ == '__main__':
    main()
