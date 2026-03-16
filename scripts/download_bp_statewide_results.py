#!/usr/bin/env python3
"""
Download Ballotpedia statewide election results for 2022-2024.

Fetches individual election pages, parses the general election results table,
and saves structured data to /tmp/bp_statewide_results.json.

Usage:
    python3 scripts/download_bp_statewide_results.py
    python3 scripts/download_bp_statewide_results.py --year 2022
    python3 scripts/download_bp_statewide_results.py --office "Attorney General"
"""

import json
import os
import re
import sys
import time
import requests
from bs4 import BeautifulSoup
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

# Ballotpedia URL patterns by office type
BP_SLUGS = {
    'Attorney General': '{state}_Attorney_General_election,_{year}',
    'Lt. Governor': '{state}_lieutenant_gubernatorial_election,_{year}',
    'Secretary of State': '{state}_Secretary_of_State_election,_{year}',
    'Treasurer': '{state}_State_Treasurer_election,_{year}',
    'Auditor': '{state}_State_Auditor_election,_{year}',
    'Controller': '{state}_Comptroller_election,_{year}',
    'Insurance Commissioner': '{state}_Insurance_Commissioner_election,_{year}',
    'Agriculture Commissioner': '{state}_Commissioner_of_Agriculture_election,_{year}',
    'Labor Commissioner': '{state}_Commissioner_of_Labor_election,_{year}',
    'Superintendent of Public Instruction': '{state}_Superintendent_of_Public_Instruction_election,_{year}',
}

# Alternate URL patterns to try if primary fails
BP_ALT_SLUGS = {
    'Treasurer': [
        '{state}_Treasurer_election,_{year}',
        '{state}_Chief_Financial_Officer_election,_{year}',  # FL
    ],
    'Auditor': [
        '{state}_Auditor_election,_{year}',
        '{state}_Auditor_General_election,_{year}',  # PA
        '{state}_Comptroller_election,_{year}',  # IN (renamed)
        '{state}_Auditor_of_Accounts_election,_{year}',  # DE historical
    ],
    'Controller': [
        '{state}_Controller_election,_{year}',
        '{state}_State_Controller_election,_{year}',
        '{state}_Comptroller_election,_{year}',
    ],
    'Agriculture Commissioner': [
        '{state}_Agriculture_Commissioner_election,_{year}',
        '{state}_Commissioner_of_Agriculture_and_Industries_election,_{year}',
    ],
    'Insurance Commissioner': [
        '{state}_Commissioner_of_Insurance_election,_{year}',
        '{state}_Commissioner_of_Securities_and_Insurance,_Auditor_election,_{year}',  # MT
        '{state}_Auditor_election,_{year}',  # MT (BP calls it State Auditor)
    ],
    'Labor Commissioner': [
        '{state}_Labor_Commissioner_election,_{year}',
    ],
    'Superintendent of Public Instruction': [
        '{state}_Superintendent_of_Education_election,_{year}',  # SC
    ],
}

# State name formatting for BP URLs
STATE_BP_NAMES = {
    'NH': 'New_Hampshire', 'NJ': 'New_Jersey', 'NM': 'New_Mexico',
    'NY': 'New_York', 'NC': 'North_Carolina', 'ND': 'North_Dakota',
    'SC': 'South_Carolina', 'SD': 'South_Dakota', 'WV': 'West_Virginia',
    'RI': 'Rhode_Island',
}


def run_sql_read(query):
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


def state_bp_name(state_name):
    """Convert state name to Ballotpedia URL format."""
    return state_name.replace(' ', '_')


def fetch_bp_page(url):
    """Fetch a Ballotpedia page. Returns HTML or None."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    }
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            return r.text
        return None
    except Exception as e:
        print(f"    Fetch error: {e}")
        return None


def parse_votes(text):
    """Parse vote count from text like '1,254,809' or '1254809'."""
    if not text:
        return None
    text = text.strip().replace(',', '').replace(' ', '')
    try:
        return int(text)
    except ValueError:
        return None


def parse_pct(text):
    """Parse percentage from text like '49.9%' or '49.9'."""
    if not text:
        return None
    text = text.strip().replace('%', '').replace(' ', '')
    try:
        return float(text)
    except ValueError:
        return None


def parse_party(text):
    """Normalize party abbreviation."""
    if not text:
        return None
    text = text.strip()
    mapping = {
        'Democratic': 'D', 'Republican': 'R', 'Libertarian': 'L',
        'Green': 'G', 'Independent': 'I', 'Constitution': 'CST',
        'No party preference': 'NP', 'Nonpartisan': 'NP',
        'Progressive': 'PRG', 'Democrat': 'D',
    }
    if text in mapping:
        return mapping[text]
    if len(text) <= 4:
        return text
    # Try first letter
    return text[0] if text else None


def extract_results(html, state_name, office, year):
    """Parse Ballotpedia HTML to extract general election results.

    BP uses 'results_table' / 'votebox' classes for election results.
    Each candidate row has class 'results_row' with cells for name, %, votes.
    Winner rows have class 'winner'.

    Returns list of dicts: [{name, party, votes, pct, won}, ...]
    """
    soup = BeautifulSoup(html, 'html.parser')
    results = []

    # === Strategy 1: BP votebox/results_table format ===
    # Find the general election results table (not primary)
    # Look for headings containing "General election" then find the next results table

    general_tables = []

    # Walk through all headings to find "General election" sections
    for heading in soup.find_all(['h2', 'h3', 'h4']):
        heading_text = heading.get_text(strip=True).lower()
        if 'general election' in heading_text and 'primary' not in heading_text:
            # Find the next results_table after this heading
            sibling = heading.find_next_sibling()
            while sibling:
                if sibling.name == 'table':
                    cls = ' '.join(sibling.get('class', []))
                    if 'results_table' in cls or 'votebox' in cls or 'election' in cls.lower():
                        general_tables.append(sibling)
                        break
                # Also check for table inside a div
                if sibling.name == 'div':
                    inner_table = sibling.find('table', class_=lambda c: c and ('results_table' in ' '.join(c) or 'votebox' in ' '.join(c)))
                    if inner_table:
                        general_tables.append(inner_table)
                        break
                # Stop if we hit another section heading
                if sibling.name in ('h2', 'h3') and sibling != heading:
                    break
                sibling = sibling.find_next_sibling()

    # If no table found near heading, search all tables with results_table class
    if not general_tables:
        for table in soup.find_all('table'):
            cls = ' '.join(table.get('class', []))
            if 'results_table' in cls or 'votebox' in cls:
                # Check it's not inside a primary section
                prev_heading = table.find_previous(['h2', 'h3', 'h4'])
                if prev_heading:
                    h_text = prev_heading.get_text(strip=True).lower()
                    if 'primary' in h_text and 'general' not in h_text:
                        continue
                general_tables.append(table)

    # Parse each candidate from the results table(s)
    for table in general_tables:
        rows = table.find_all('tr', class_=lambda c: c and 'results_row' in ' '.join(c)) if table else []

        # If no results_row class, try all tr elements
        if not rows:
            rows = table.find_all('tr')

        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue

            # Check if this is a winner row
            row_classes = ' '.join(row.get('class', []))
            is_winner = 'winner' in row_classes

            # Extract text from all cells
            cell_texts = [c.get_text(strip=True) for c in cells]

            # Find the candidate name — usually the cell with the longest text
            # that contains letters (not just numbers/percentages)
            name = None
            party = None
            votes = None
            pct = None

            for ct in cell_texts:
                # Skip empty, headers, totals
                if not ct or ct.lower() in ('candidate', 'party', '%', 'votes', 'total', 'total votes'):
                    continue
                if ct.lower().startswith('total'):
                    continue

                # Check if it's a percentage
                if re.match(r'^[\d.]+%$', ct):
                    pct = parse_pct(ct)
                    continue

                # Check if it's a vote count (all digits/commas)
                if re.match(r'^[\d,]+$', ct) and len(ct.replace(',', '')) >= 3:
                    votes = parse_votes(ct)
                    continue

                # Check if it's a party name
                if ct in ('Democratic', 'Republican', 'Libertarian', 'Green', 'Independent',
                          'Constitution', 'No party preference', 'Nonpartisan', 'Progressive',
                          'Democrat', 'Working Families', 'Conservative'):
                    party = parse_party(ct)
                    continue

                # Check if it's a checkmark/icon
                if ct in ('✔', '✓', 'X', '☑'):
                    is_winner = True
                    continue

                # Must be the candidate name
                if not name and len(ct) > 1 and re.search(r'[a-zA-Z]', ct):
                    # Clean — remove party in parens
                    paren_match = re.search(r'\(([^)]+)\)', ct)
                    if paren_match and not party:
                        party = parse_party(paren_match.group(1))
                    name = re.sub(r'\s*\([^)]*\)\s*', ' ', ct).strip()
                    # Remove newlines and extra whitespace
                    name = re.sub(r'\s+', ' ', name).strip()

            if name and len(name) > 1:
                results.append({
                    'name': name,
                    'party': party,
                    'votes': votes,
                    'pct': pct,
                    'won': is_winner,
                })

        # If we got results from the first general table, stop
        if results:
            break

    # === Strategy 2: Standard wikitable format (older pages) ===
    if not results:
        for table in soup.find_all('table', class_='wikitable'):
            table_text = table.get_text().lower()
            if 'general' not in table_text:
                continue
            if 'votes' not in table_text and '%' not in table_text:
                continue

            rows = table.find_all('tr')
            if not rows:
                continue

            # Parse header
            headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(['th', 'td'])]
            name_col = next((i for i, h in enumerate(headers) if 'candidate' in h or 'name' in h), 0)
            party_col = next((i for i, h in enumerate(headers) if h == 'party'), None)
            votes_col = next((i for i, h in enumerate(headers) if 'votes' in h), None)
            pct_col = next((i for i, h in enumerate(headers) if '%' in h or 'percent' in h), None)

            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue

                name_text = cells[name_col].get_text(strip=True) if name_col < len(cells) else ''
                if not name_text or 'total' in name_text.lower():
                    continue

                name = re.sub(r'\s*\([^)]*\)\s*', ' ', name_text).strip()
                name = re.sub(r'\s+', ' ', name).strip()

                p = parse_party(cells[party_col].get_text(strip=True)) if party_col and party_col < len(cells) else None
                v = parse_votes(cells[votes_col].get_text(strip=True)) if votes_col and votes_col < len(cells) else None
                pc = parse_pct(cells[pct_col].get_text(strip=True)) if pct_col and pct_col < len(cells) else None

                if name:
                    results.append({'name': name, 'party': p, 'votes': v, 'pct': pc, 'won': False})

            if results:
                break

    # Ensure winner is marked
    if results:
        has_winner = any(r.get('won') for r in results)
        if not has_winner and any(r.get('votes') for r in results):
            max_votes = max((r.get('votes') or 0) for r in results)
            if max_votes > 0:
                for r in results:
                    r['won'] = (r.get('votes') or 0) == max_votes

    return results


def main():
    year_filter = None
    office_filter = None
    for arg in sys.argv[1:]:
        if arg.startswith('--year'):
            year_filter = int(sys.argv[sys.argv.index(arg) + 1])
        if arg.startswith('--office'):
            office_filter = sys.argv[sys.argv.index(arg) + 1]

    print("Download Ballotpedia Statewide Results")
    print("=" * 60)

    # Get elections to scrape
    where_clauses = [
        "d.office_level = 'Statewide'",
        "e.election_type = 'General'",
        "e.election_year >= 2010",
        "s.office_type != 'Governor'",
    ]
    if year_filter:
        where_clauses.append(f"e.election_year = {year_filter}")
    if office_filter:
        where_clauses.append(f"s.office_type = '{office_filter}'")

    elections = run_sql_read(f"""
        SELECT e.id as election_id, e.election_year, st.abbreviation, st.state_name, s.office_type
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE {' AND '.join(where_clauses)}
        ORDER BY e.election_year, s.office_type, st.abbreviation
    """)

    if not elections:
        print("No elections found")
        return

    print(f"Elections to scrape: {len(elections)}")

    # Load existing results if any
    output_path = '/tmp/bp_statewide_results.json'
    existing = {}
    if os.path.exists(output_path):
        with open(output_path) as f:
            existing_list = json.load(f)
            for r in existing_list:
                existing[r['election_id']] = r
        print(f"Existing results loaded: {len(existing)}")

    results = list(existing.values())
    success = 0
    failed = 0
    skipped = 0

    for i, elec in enumerate(elections):
        eid = elec['election_id']
        year = elec['election_year']
        state = elec['state_name']
        abbr = elec['abbreviation']
        office = elec['office_type']

        # Skip if already have results
        if eid in existing and existing[eid].get('candidates'):
            skipped += 1
            continue

        state_slug = state_bp_name(state)

        # Build URL
        slug_template = BP_SLUGS.get(office)
        if not slug_template:
            print(f"  WARNING: No URL pattern for {office}")
            failed += 1
            continue

        urls_to_try = [slug_template.format(state=state_slug, year=year)]
        for alt in BP_ALT_SLUGS.get(office, []):
            urls_to_try.append(alt.format(state=state_slug, year=year))

        html = None
        used_url = None
        for url_slug in urls_to_try:
            url = f'https://ballotpedia.org/{url_slug}'
            html = fetch_bp_page(url)
            if html:
                used_url = url
                break
            time.sleep(1)

        if not html:
            print(f"  FAILED: {abbr} {office} {year} — no page found")
            results.append({
                'election_id': eid,
                'year': year,
                'state': abbr,
                'office': office,
                'candidates': [],
                'error': 'page_not_found',
            })
            failed += 1
            time.sleep(2)
            continue

        # Parse results
        candidates = extract_results(html, state, office, year)

        result = {
            'election_id': eid,
            'year': year,
            'state': abbr,
            'office': office,
            'candidates': candidates,
            'url': used_url,
        }
        results.append(result)

        if candidates:
            winner = next((c for c in candidates if c.get('won')), None)
            winner_str = f"{winner['name']} ({winner.get('party', '?')})" if winner else 'no winner'
            votes_str = f", {len(candidates)} candidates" if len(candidates) > 1 else ""
            has_votes = any(c.get('votes') for c in candidates)
            print(f"  [{i+1}/{len(elections)}] {abbr} {office} {year}: {winner_str}{votes_str} {'✓' if has_votes else '(no votes)'}")
            success += 1
        else:
            print(f"  [{i+1}/{len(elections)}] {abbr} {office} {year}: NO RESULTS PARSED")
            failed += 1

        # Rate limit: 1 request per 2 seconds
        time.sleep(2)

        # Save periodically
        if (i + 1) % 20 == 0:
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"  --- Saved {len(results)} results ---")

    # Final save
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"  Success: {success}")
    print(f"  Failed: {failed}")
    print(f"  Skipped (already have): {skipped}")
    print(f"  Total results: {len(results)}")
    print(f"  Saved to: {output_path}")


if __name__ == '__main__':
    main()
