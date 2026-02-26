#!/usr/bin/env python3
"""
Backfill governor election vote counts from Wikipedia.

Fetches Wikipedia pages for governor elections missing vote data,
parses the Election Box templates, and updates candidacies and elections
in Supabase.

Usage:
    python3 scripts/backfill_governor_votes.py --dry-run
    python3 scripts/backfill_governor_votes.py --state NH
    python3 scripts/backfill_governor_votes.py --min-year 2000
    python3 scripts/backfill_governor_votes.py
"""

import sys
import os
import re
import json
import time
import argparse

import requests
import httpx

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF

# Wikipedia state name mapping
STATE_NAMES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New_Hampshire', 'NJ': 'New_Jersey', 'NM': 'New_Mexico', 'NY': 'New_York',
    'NC': 'North_Carolina', 'ND': 'North_Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode_Island', 'SC': 'South_Carolina',
    'SD': 'South_Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West_Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming',
}

PARTY_MAP = {
    'Republican Party (United States)': 'R',
    'Democratic Party (United States)': 'D',
    'Libertarian Party (United States)': 'L',
    'Green Party (United States)': 'G',
    'Green Party of the United States': 'G',
    'Independent (politician)': 'I',
    'Reform Party of the United States of America': 'Reform',
    'Constitution Party (United States)': 'Constitution',
    'American Independent Party': 'AIP',
    'Natural Law Party (United States)': 'NLP',
    'Working Families Party': 'WFP',
    'Conservative Party of New York State': 'Conservative',
    'Progressive Party (Vermont)': 'Progressive',
    'Alaska Independence Party': 'AIP',
}

WP_API = 'https://en.wikipedia.org/api/rest_v1/page/html/'
WP_HEADERS = {'User-Agent': 'ElectionsBot/1.0 (https://github.com/w85kramer/elections; w85kramer@gmail.com)'}


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
        print(f'  SQL ERROR ({resp.status_code}): {resp.text[:300]}')
        return None
    return None


def clean_name(name):
    """Extract clean candidate name from Wikipedia wikitext."""
    # Remove [[links]] — extract display text
    name = re.sub(r'\[\[([^|\]]*\|)?([^\]]+)\]\]', r'\2', name)
    # Remove (incumbent), (write-in), etc.
    name = re.sub(r'\s*\((?:incumbent|incumbents?|write-in|politician)\)\s*', ' ', name)
    # Remove {{nowrap|...}} and similar templates
    name = re.sub(r'\{\{nowrap\|([^}]+)\}\}', r'\1', name)
    name = re.sub(r'\{\{[^}]*\}\}', '', name)
    # Remove ref tags
    name = re.sub(r'<ref[^>]*>.*?</ref>', '', name)
    name = re.sub(r'<ref[^/]*/>', '', name)
    # Remove Lt. Gov running mate (name/running_mate)
    name = re.sub(r'\s*/\s*.*$', '', name)
    # Remove HTML entities
    name = name.replace('&apos;', "'").replace('&amp;', '&')
    return name.strip()


def parse_votes(vote_str):
    """Parse vote string like '352,813' to int."""
    if not vote_str:
        return None
    cleaned = vote_str.replace(',', '').replace(' ', '').strip()
    try:
        return int(cleaned)
    except ValueError:
        return None


def parse_pct(pct_str):
    """Parse percentage string like '56.98%' to float."""
    if not pct_str:
        return None
    cleaned = pct_str.replace('%', '').replace(' ', '').strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_election_box_templates(html):
    """Parse Election box templates from data-mw attributes."""
    matches = re.findall(r"data-mw='({.*?})'", html)
    if not matches:
        matches = re.findall(r'data-mw="({.*?})"', html)

    sections = []
    current_section = None

    for m in matches:
        try:
            data = json.loads(m)
        except json.JSONDecodeError:
            continue

        parts = data.get('parts', [])
        for p in parts:
            if not isinstance(p, dict) or 'template' not in p:
                continue
            tpl = p['template']
            target = tpl.get('target', {}).get('wt', '')
            params = tpl.get('params', {})

            if 'Election box begin' in target:
                title_text = params.get('title', {}).get('wt', '')
                current_section = {'title': title_text, 'candidates': [], 'total': None}
                sections.append(current_section)

            elif 'Election box' in target and 'candidate' in target.lower() and current_section is not None:
                candidate = clean_name(params.get('candidate', {}).get('wt', ''))
                party_raw = params.get('party', {}).get('wt', '')
                votes = parse_votes(params.get('votes', {}).get('wt', ''))
                pct = parse_pct(params.get('percentage', {}).get('wt', ''))
                is_winner = 'winning' in target.lower()
                party = PARTY_MAP.get(party_raw, party_raw[:3] if party_raw else '')

                if candidate and votes is not None:
                    current_section['candidates'].append({
                        'name': candidate, 'party': party,
                        'votes': votes, 'pct': pct, 'is_winner': is_winner,
                    })

            elif 'Election box total' in target and current_section is not None:
                total = parse_votes(params.get('votes', {}).get('wt', ''))
                if total:
                    current_section['total'] = total

    # Find the general election section
    for s in sections:
        title_lower = s['title'].lower()
        if 'primary' in title_lower or 'runoff' in title_lower or 'convention' in title_lower:
            continue
        if s['candidates'] and s['total']:
            return s

    for s in reversed(sections):
        if s['candidates'] and s['total'] and 'primary' not in s['title'].lower():
            return s

    return None


def parse_wikitable_results(html):
    """Fallback: parse general election results from standard wikitable format.
    Looks for tables with columns like: Party | Candidate | Votes | % """
    tables = re.findall(r'<table[^>]*>.*?</table>', html, re.DOTALL)

    best = None
    best_score = 0

    for table in tables:
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table, re.DOTALL)
        if len(rows) < 3:
            continue

        # Check if this looks like a general election results table
        table_text = re.sub(r'<[^>]+>', ' ', table).lower()
        if 'primary' in table_text and 'general' not in table_text:
            continue

        candidates = []
        total_votes = None

        for row in rows:
            cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row, re.DOTALL)
            cleaned = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]

            if len(cleaned) < 3:
                continue

            # Look for "Turnout" or "Total" row
            row_text = ' '.join(cleaned).lower()
            if 'turnout' in row_text or ('total' in row_text and 'vote' in row_text):
                for c in cleaned:
                    v = parse_votes(c)
                    if v and v > 10000:
                        total_votes = v
                        break
                continue

            # Look for "Majority" row — skip it
            if 'majority' in row_text:
                continue

            # Try to extract candidate data
            # Common formats:
            #   [party_color, party_name, candidate_name, votes, pct, ...]
            #   [party_name, candidate_name, votes, pct]
            name = None
            party = None
            votes = None
            pct = None

            for i, c in enumerate(cleaned):
                v = parse_votes(c)
                p = parse_pct(c)

                if v and v > 1000 and votes is None:
                    votes = v
                elif p and p < 100 and pct is None and votes is not None:
                    pct = p
                elif not name and len(c) > 3 and not c.replace('.', '').replace(',', '').isdigit():
                    # Could be party or candidate
                    c_lower = c.lower().strip()
                    if c_lower in ('democratic', 'democrat'):
                        party = 'D'
                    elif c_lower in ('republican',):
                        party = 'R'
                    elif c_lower in ('libertarian',):
                        party = 'L'
                    elif c_lower in ('green',):
                        party = 'G'
                    elif c_lower in ('independent',):
                        party = 'I'
                    elif any(ch.isupper() for ch in c) and ' ' in c and len(c) > 5:
                        # Looks like a candidate name — but skip county names
                        if c.endswith('County') or c.endswith('City') or 'style=' in c:
                            continue
                        # Strip (inc.) etc., and running mate
                        name = re.sub(r'\s*\((?:inc\.|incumbent|incumbents?)\)\s*', ' ', c).strip()
                        name = re.sub(r'\s*/\s*.*$', '', name)  # remove running mate
                        name = clean_name(name)

            if name and votes:
                candidates.append({
                    'name': name, 'party': party or '',
                    'votes': votes, 'pct': pct, 'is_winner': False,
                })

        if not candidates:
            continue

        # If no total found, sum candidate votes
        if total_votes is None:
            total_votes = sum(c['votes'] for c in candidates)

        # Score this table — prefer tables with more candidates and larger vote totals
        score = len(candidates) * 10 + (total_votes or 0) / 100000
        # Prefer tables that look like general elections (bigger totals)
        if total_votes and total_votes > 50000:
            score += 50

        if score > best_score:
            best_score = score
            # Mark the first candidate as winner (highest votes)
            candidates.sort(key=lambda c: c['votes'], reverse=True)
            if candidates:
                candidates[0]['is_winner'] = True
            best = {'title': 'General election', 'candidates': candidates, 'total': total_votes}

    return best


def fetch_wiki_election(state_abbrev, year):
    """Fetch and parse a Wikipedia governor election page. Returns general election results."""
    state_name = STATE_NAMES[state_abbrev]
    title = f'{year}_{state_name}_gubernatorial_election'

    try:
        resp = requests.get(WP_API + title, headers=WP_HEADERS, timeout=30)
    except requests.RequestException as e:
        print(f'    Network error: {e}')
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

    # Try Election box templates first (modern pages)
    result = parse_election_box_templates(resp.text)
    if result:
        return result

    # Fallback to wikitable parsing (older pages)
    return parse_wikitable_results(resp.text)


def name_match(wiki_name, db_name):
    """Check if Wikipedia candidate name matches DB candidate name."""
    if not wiki_name or not db_name:
        return False

    wiki_lower = wiki_name.lower().strip()
    db_lower = db_name.lower().strip()

    # Exact match
    if wiki_lower == db_lower:
        return True

    # Last name match with first initial
    wiki_parts = wiki_lower.split()
    db_parts = db_lower.split()

    if not wiki_parts or not db_parts:
        return False

    # Last names must match
    if wiki_parts[-1] != db_parts[-1]:
        # Try without suffixes (Jr., Sr., III, etc.)
        suffixes = {'jr.', 'sr.', 'ii', 'iii', 'iv', 'jr', 'sr'}
        wiki_last = wiki_parts[-1].rstrip('.')
        db_last = db_parts[-1].rstrip('.')
        if wiki_last in suffixes and len(wiki_parts) > 2:
            wiki_last = wiki_parts[-2]
        if db_last in suffixes and len(db_parts) > 2:
            db_last = db_parts[-2]
        if wiki_last != db_last:
            return False

    # First name or initial match
    if len(wiki_parts[0]) >= 3 and len(db_parts[0]) >= 3:
        if wiki_parts[0][:3] == db_parts[0][:3]:
            return True

    return False


def main():
    parser = argparse.ArgumentParser(description='Backfill governor vote data from Wikipedia')
    parser.add_argument('--dry-run', action='store_true', help='Show what would change')
    parser.add_argument('--state', type=str, help='Process only this state')
    parser.add_argument('--min-year', type=int, default=0, help='Minimum election year')
    parser.add_argument('--max-year', type=int, default=2025, help='Maximum election year')
    args = parser.parse_args()

    # Step 1: Get all governor elections missing vote data
    print('Fetching governor elections missing vote data...')
    where_clause = "AND e.total_votes_cast IS NULL AND e.election_year <= " + str(args.max_year)
    if args.min_year:
        where_clause += f" AND e.election_year >= {args.min_year}"
    if args.state:
        where_clause += f" AND s.abbreviation = '{args.state}'"

    rows = run_sql(f"""
        SELECT s.abbreviation, e.election_year, e.id as election_id,
               cy.id as candidacy_id, cy.candidate_id,
               c.full_name, cy.party, cy.result
        FROM elections e
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        LEFT JOIN candidacies cy ON cy.election_id = e.id
        LEFT JOIN candidates c ON cy.candidate_id = c.id
        WHERE se.office_type = 'Governor' AND e.election_type = 'General'
        {where_clause}
        ORDER BY s.abbreviation, e.election_year
    """)

    if rows is None:
        print('ERROR: Failed to fetch data')
        return

    # Organize by election
    elections = {}  # (state, year) -> {election_id, candidacies: [...]}
    for r in rows:
        key = (r['abbreviation'], r['election_year'])
        if key not in elections:
            elections[key] = {'election_id': r['election_id'], 'candidacies': []}
        if r['candidacy_id']:
            elections[key]['candidacies'].append({
                'id': r['candidacy_id'],
                'candidate_id': r['candidate_id'],
                'name': r['full_name'],
                'party': r['party'],
                'result': r['result'],
            })

    print(f'  {len(elections)} elections to process\n')

    # Step 2: Process each election
    updated_elections = 0
    updated_candidacies = 0
    failed = []
    no_data = []

    for (state, year), election in sorted(elections.items()):
        print(f'  {state} {year}...', end=' ', flush=True)

        wiki = fetch_wiki_election(state, year)
        time.sleep(0.5)  # be nice to Wikipedia

        if wiki is None or not wiki['candidates']:
            print('no Wikipedia data')
            no_data.append(f'{state} {year}')
            continue

        # Match Wikipedia candidates to DB candidacies
        matched = 0
        updates = []

        for wiki_cand in wiki['candidates']:
            best_match = None
            for db_cand in election['candidacies']:
                if name_match(wiki_cand['name'], db_cand['name']):
                    best_match = db_cand
                    break

            if best_match:
                matched += 1
                updates.append({
                    'candidacy_id': best_match['id'],
                    'votes': wiki_cand['votes'],
                    'pct': wiki_cand['pct'],
                    'name': wiki_cand['name'],
                })
            # Don't worry about unmatched wiki candidates (write-ins, minor party)

        if matched == 0 and election['candidacies']:
            print(f'0 matches (wiki: {[c["name"] for c in wiki["candidates"][:3]]}, db: {[c["name"] for c in election["candidacies"][:3]]})')
            failed.append(f'{state} {year}')
            continue

        total_votes = wiki['total']

        if args.dry_run:
            print(f'{matched} matches, total={total_votes:,}')
            updated_elections += 1
            updated_candidacies += matched
            continue

        # Update candidacies
        for u in updates:
            pct_val = f"{u['pct']}" if u['pct'] is not None else 'NULL'
            run_sql(f"""
                UPDATE candidacies
                SET votes_received = {u['votes']}, vote_percentage = {pct_val}
                WHERE id = {u['candidacy_id']}
            """)

        # Update election total
        if total_votes:
            run_sql(f"""
                UPDATE elections SET total_votes_cast = {total_votes}
                WHERE id = {election['election_id']}
            """)

        updated_elections += 1
        updated_candidacies += matched
        print(f'{matched} matches, total={total_votes:,}')

    # Summary
    print(f'\n{"DRY RUN — " if args.dry_run else ""}SUMMARY:')
    print(f'  Elections updated: {updated_elections}/{len(elections)}')
    print(f'  Candidacies updated: {updated_candidacies}')
    if no_data:
        print(f'  No Wikipedia data ({len(no_data)}): {", ".join(no_data[:20])}{"..." if len(no_data) > 20 else ""}')
    if failed:
        print(f'  Name match failures ({len(failed)}): {", ".join(failed[:20])}{"..." if len(failed) > 20 else ""}')


if __name__ == '__main__':
    main()
