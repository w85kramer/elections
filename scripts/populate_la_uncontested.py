#!/usr/bin/env python3
"""
Backfill uncontested LA legislative elections from Ballotpedia.

LA holds elections for ALL House (105) and Senate (39) seats every 4 years.
The SoS Excel files only include contested races, leaving uncontested seats
without election records. This script finds those gaps and fills them using
Ballotpedia district pages.

BP marks uncontested races with text like:
  "The primary election was canceled. [Name] ([Party]) won the election
   without appearing on the ballot."

Usage:
    python3 scripts/populate_la_uncontested.py --dry-run
    python3 scripts/populate_la_uncontested.py --year 2015
    python3 scripts/populate_la_uncontested.py --year-from 1983 --year-to 2015
    python3 scripts/populate_la_uncontested.py --no-cache
"""

import sys
import os
import re
import time
import argparse
import html as htmlmod

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL
from scripts.candidate_lookup import CandidateLookup

# ══════════════════════════════════════════════════════════════════════
# Constants
# ══════════════════════════════════════════════════════════════════════

CACHE_DIR = '/tmp/bp_la_uncontested'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html',
}

# LA election years (every 4 years, odd years)
LA_ELECTION_YEARS = list(range(1983, 2024, 4))

# Election dates by year (LA jungle primary dates)
ELECTION_DATES = {
    1983: '1983-10-22', 1987: '1987-10-24', 1991: '1991-10-19',
    1995: '1995-10-21', 1999: '1999-10-23', 2003: '2003-10-04',
    2007: '2007-10-20', 2011: '2011-10-22', 2015: '2015-10-24',
    2019: '2019-10-12', 2023: '2023-10-14',
}

PARTY_MAP = {
    'Republican': 'R', 'Democratic': 'D', 'Democrat': 'D',
    'Libertarian': 'L', 'Green': 'G', 'Independent': 'I',
    'No party preference': 'I', 'Nonpartisan': 'I',
    'R': 'R', 'D': 'D', 'L': 'L', 'G': 'G', 'I': 'I',
    'REP': 'R', 'DEM': 'D', 'LIB': 'L', 'GRN': 'G', 'IND': 'I',
}

BP_CHAMBER_NAMES = {
    'House': 'House_of_Representatives',
    'Senate': 'State_Senate',
}


# ══════════════════════════════════════════════════════════════════════
# SQL helpers
# ══════════════════════════════════════════════════════════════════════

def run_sql(query, retries=8, exit_on_error=True):
    """Execute SQL via Supabase Management API with retry/backoff."""
    for attempt in range(retries):
        resp = httpx.post(
            API_URL,
            headers={'Authorization': f'Bearer {TOKEN}',
                     'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code in (429, 500, 503) and attempt < retries - 1:
            wait = 10 * (attempt + 1)
            print(f'  Rate limited/error ({resp.status_code}), waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None
    print('Max retries exceeded')
    if exit_on_error:
        sys.exit(1)
    return None


def esc(s):
    if s is None:
        return 'NULL'
    return "'" + str(s).replace("'", "''") + "'"


# ══════════════════════════════════════════════════════════════════════
# HTTP fetching
# ══════════════════════════════════════════════════════════════════════

def fetch_page(url, cache_path, use_cache=True, max_retries=3):
    """Fetch a URL with caching and retry logic."""
    if use_cache and os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            return f.read()

    for attempt in range(max_retries):
        try:
            resp = httpx.get(url, headers=HEADERS, follow_redirects=True, timeout=30)
            if resp.status_code == 200:
                os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                with open(cache_path, 'w', encoding='utf-8') as f:
                    f.write(resp.text)
                return resp.text
            elif resp.status_code == 202:
                wait = 3 + attempt * 2
                print(f'    202 (CDN warming), retry in {wait}s...')
                time.sleep(wait)
                continue
            elif resp.status_code == 404:
                print(f'    404: {url}')
                return None
            else:
                print(f'    HTTP {resp.status_code}: {url}')
                return None
        except Exception as e:
            print(f'    Error (attempt {attempt+1}): {e}')
            if attempt < max_retries - 1:
                time.sleep(2)
    return None


# ══════════════════════════════════════════════════════════════════════
# BP page parsing
# ══════════════════════════════════════════════════════════════════════

def build_bp_url(chamber, district_number):
    """Build a Ballotpedia district page URL for LA."""
    chamber_name = BP_CHAMBER_NAMES[chamber]
    return f'https://ballotpedia.org/Louisiana_{chamber_name}_District_{district_number}'


def build_cache_path(chamber, district_number):
    """Build cache file path."""
    return os.path.join(CACHE_DIR, f'{chamber.lower()}_{district_number}.html')


def parse_uncontested_winners(html, missing_years):
    """
    Parse a BP district page for uncontested election winners.

    Looks for patterns like:
      "The primary election was canceled. Name (Party) won the election
       without appearing on the ballot."
      "Name (Party) won election outright in the primary"

    Also checks for "Assumed office" text to identify winners from
    structured data sections.

    Returns dict: {year: {'name': str, 'party': str}} for each found year.
    """
    if not html:
        return {}

    results = {}
    missing_set = set(missing_years)

    # Clean HTML for text extraction
    text = htmlmod.unescape(html)

    # Strategy 1: Find "canceled" / "without appearing" patterns near year headings
    # BP pages have <span id="YYYY"> or <h2/h3> with year numbers
    # Then uncontested text follows within ~2000 chars

    # Split the page into year sections
    # Look for year headings: <span class="mw-headline" id="2023"> or similar
    year_sections = []
    year_pattern = re.compile(
        r'(?:<span[^>]*id="(\d{4})"[^>]*>|<h[23][^>]*>\s*(\d{4})\s*</h[23]>)',
        re.IGNORECASE
    )
    year_positions = []
    for m in year_pattern.finditer(text):
        yr = int(m.group(1) or m.group(2))
        year_positions.append((yr, m.start()))

    # For each year, extract the section text until the next year
    for i, (yr, start) in enumerate(year_positions):
        if yr not in missing_set:
            continue
        end = year_positions[i + 1][1] if i + 1 < len(year_positions) else start + 10000
        section = text[start:end]

        # Look for uncontested winner patterns
        winner = extract_uncontested_from_section(section, yr)
        if winner:
            results[yr] = winner

    return results


def extract_uncontested_from_section(section, year):
    """Extract uncontested winner from a year section of BP HTML."""
    # Strip HTML tags for cleaner matching
    clean = re.sub(r'<[^>]+>', ' ', section)
    clean = re.sub(r'\s+', ' ', clean).strip()

    # Pattern 1: "canceled. NAME (PARTY) won the election without appearing"
    m = re.search(
        r'canceled\.\s*(.+?)\s*\((\w+(?:\s+\w+)?)\)\s*won\s+the\s+election\s+without',
        clean, re.IGNORECASE
    )
    if m:
        name = m.group(1).strip()
        party = normalize_party(m.group(2).strip())
        if name and party:
            return {'name': name, 'party': party}

    # Pattern 2: "Incumbent NAME (PARTY) was unopposed in [the/both] ..."
    # Covers: "unopposed in the October 24 blanket primary"
    #         "unopposed in both the general election and Republican primary"
    m = re.search(
        r'(?:Incumbent\s+)?(.+?)\s*\((\w+)\)\s*was\s+unopposed\s+in\s+(?:the|both)\b',
        clean, re.IGNORECASE
    )
    if m:
        name = m.group(1).strip()
        party = normalize_party(m.group(2).strip())
        if name and party:
            return {'name': name, 'party': party}

    # Pattern 3: "NAME (PARTY) won election outright in the primary"
    m = re.search(
        r'(?:Incumbent\s+)?(.+?)\s*\((\w+)\)\s*won\s+(?:the\s+)?election\s+outright',
        clean, re.IGNORECASE
    )
    if m:
        name = m.group(1).strip()
        party = normalize_party(m.group(2).strip())
        if name and party:
            return {'name': name, 'party': party}

    # Pattern 4: "NAME (PARTY) ran unopposed"
    m = re.search(
        r'(?:Incumbent\s+)?(.+?)\s*\((\w+)\)\s*ran\s+unopposed',
        clean, re.IGNORECASE
    )
    if m:
        name = m.group(1).strip()
        party = normalize_party(m.group(2).strip())
        if name and party:
            return {'name': name, 'party': party}

    # Pattern 4: Single candidate with "won" in votebox section
    # Look for a single results_row with "winner" class in original HTML
    winner_rows = re.findall(
        r'class="results_row\s*winner"[^>]*>.*?<a[^>]*>([^<]+)</a>.*?'
        r'(?:Republican|Democratic|Libertarian|Independent)',
        section, re.DOTALL
    )
    if len(winner_rows) == 1:
        # Only one candidate total = uncontested
        total_rows = re.findall(r'class="results_row', section)
        if len(total_rows) == 1:
            name = htmlmod.unescape(winner_rows[0].strip())
            # Get party
            party_m = re.search(
                r'class="results_row\s*winner".*?(Republican|Democratic|Libertarian|Independent)',
                section, re.DOTALL
            )
            if party_m:
                party = normalize_party(party_m.group(1))
                return {'name': name, 'party': party}

    return None


def normalize_party(raw):
    """Normalize party string to single-letter code."""
    return PARTY_MAP.get(raw, PARTY_MAP.get(raw.title(), 'I'))


# ══════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Backfill uncontested LA elections from BP')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--year', type=int, help='Process single year')
    parser.add_argument('--year-from', type=int, default=1983)
    parser.add_argument('--year-to', type=int, default=2023)
    parser.add_argument('--chamber', choices=['House', 'Senate'], help='Process single chamber')
    parser.add_argument('--no-cache', action='store_true', help='Re-fetch all pages')
    parser.add_argument('--fetch-delay', type=float, default=2.0,
                        help='Delay between BP page fetches (seconds)')
    args = parser.parse_args()

    if args.year:
        args.year_from = args.year
        args.year_to = args.year

    use_cache = not args.no_cache

    # ── Find missing elections ──
    print('Finding missing LA elections...')
    target_years = [y for y in LA_ELECTION_YEARS
                    if args.year_from <= y <= args.year_to]
    years_list = ','.join(str(y) for y in target_years)

    chamber_filter = f"AND d.chamber = '{args.chamber}'" if args.chamber else ''

    rows = run_sql(f"""
        SELECT s.id AS seat_id, d.chamber, d.district_number,
          array_agg(e.election_year) AS existing_years
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        LEFT JOIN elections e ON e.seat_id = s.id
          AND e.election_type = 'General'
          AND e.election_year IN ({years_list})
        WHERE st.abbreviation = 'LA'
          AND d.chamber IN ('House', 'Senate')
          {chamber_filter}
        GROUP BY s.id, d.chamber, d.district_number
    """)

    # Build missing map: (chamber, dist) → [missing years]
    missing_map = {}
    total_missing = 0
    for r in rows:
        existing = set(y for y in (r['existing_years'] or []) if y is not None)
        missing = [y for y in target_years if y not in existing]
        if missing:
            key = (r['chamber'], r['district_number'])
            missing_map[key] = {
                'seat_id': r['seat_id'],
                'missing_years': missing,
            }
            total_missing += len(missing)

    print(f'  {total_missing} missing elections across {len(missing_map)} districts')
    if total_missing == 0:
        print('Nothing to do!')
        return

    # ── Initialize candidate lookup ──
    print('Loading candidate data for LA...')
    candidate_lookup = CandidateLookup(run_sql)
    candidate_lookup.load_state('LA')

    # ── Fetch and parse BP pages ──
    print(f'\nFetching Ballotpedia pages (cache: {"on" if use_cache else "off"})...')

    found = {}  # (chamber, dist, year) → {name, party, seat_id}
    not_found = []  # (chamber, dist, year)
    fetched_count = 0

    for (chamber, dist_num), info in sorted(missing_map.items()):
        url = build_bp_url(chamber, dist_num)
        cache_path = build_cache_path(chamber, dist_num)

        needs_fetch = not use_cache or not os.path.exists(cache_path)
        if needs_fetch and fetched_count > 0:
            time.sleep(args.fetch_delay)

        html = fetch_page(url, cache_path, use_cache=use_cache)
        if needs_fetch:
            fetched_count += 1

        if not html:
            print(f'  SKIP {chamber} {dist_num}: could not fetch BP page')
            for yr in info['missing_years']:
                not_found.append((chamber, dist_num, yr))
            continue

        winners = parse_uncontested_winners(html, info['missing_years'])

        for yr in info['missing_years']:
            if yr in winners:
                found[(chamber, dist_num, yr)] = {
                    **winners[yr],
                    'seat_id': info['seat_id'],
                }
            else:
                not_found.append((chamber, dist_num, yr))

        found_str = ', '.join(str(y) for y in sorted(winners.keys()))
        missed_str = ', '.join(
            str(y) for y in info['missing_years'] if y not in winners)
        status = f'found: [{found_str}]' if found_str else ''
        if missed_str:
            status += f'  MISSED: [{missed_str}]'
        print(f'  {chamber} {dist_num}: {status}')

    print(f'\nFound {len(found)} uncontested winners, {len(not_found)} not found')

    if not found:
        print('No winners to insert.')
        return

    # ── Insert elections ──
    print(f'\nInserting {len(found)} elections...')
    elections_to_insert = []
    for (chamber, dist_num, yr), info in sorted(found.items()):
        elections_to_insert.append({
            'seat_id': info['seat_id'],
            'year': yr,
            'date': ELECTION_DATES.get(yr),
        })

    if not args.dry_run:
        batch_size = 100
        created_elections = {}
        for i in range(0, len(elections_to_insert), batch_size):
            batch = elections_to_insert[i:i + batch_size]
            values = []
            for e in batch:
                values.append(
                    f"({e['seat_id']}, {e['year']}, 'General', "
                    f"{esc(e['date'])}, 'Certified')"
                )
            sql = f"""
                INSERT INTO elections
                    (seat_id, election_year, election_type, election_date, result_status)
                VALUES {', '.join(values)}
                ON CONFLICT DO NOTHING
                RETURNING id, seat_id, election_year
            """
            result = run_sql(sql)
            if result:
                for r in result:
                    created_elections[(r['seat_id'], r['election_year'])] = r['id']
            if i + batch_size < len(elections_to_insert):
                time.sleep(3)
        print(f'  Created {len(created_elections)} elections')
    else:
        print('  (dry run — skipping)')
        created_elections = {(e['seat_id'], e['year']): -1
                            for e in elections_to_insert}

    # ── Insert candidacies ──
    print(f'Inserting candidacies...')
    candy_values = []
    candidates_created = 0
    candidates_reused = 0

    for (chamber, dist_num, yr), info in sorted(found.items()):
        election_id = created_elections.get((info['seat_id'], yr))
        if not election_id:
            continue

        name = info['name']
        party = info['party']

        if args.dry_run:
            existing = candidate_lookup.find_match(name, 'LA')
            if existing:
                candidates_reused += 1
            else:
                candidates_created += 1
            continue

        cand_id = candidate_lookup.find_or_create(
            full_name=name,
            state='LA',
            first_name=name.split()[0] if name.split() else '',
            last_name=name.split()[-1] if name.split() else '',
        )
        if candidate_lookup.find_match(name, 'LA') == cand_id:
            candidates_reused += 1
        else:
            candidates_created += 1

        is_major = party in ('D', 'R')
        candy_values.append(
            f"({election_id}, {cand_id}, {esc(party)}, 'Won', "
            f"{'TRUE' if is_major else 'FALSE'}, FALSE)"
        )

    if candy_values and not args.dry_run:
        batch_size = 200
        total_inserted = 0
        for i in range(0, len(candy_values), batch_size):
            batch = candy_values[i:i + batch_size]
            sql = f"""
                INSERT INTO candidacies
                    (election_id, candidate_id, party, result, is_major, is_write_in)
                VALUES {', '.join(batch)}
                ON CONFLICT DO NOTHING
            """
            result = run_sql(sql, exit_on_error=False)
            if result is not None:
                total_inserted += len(batch)
            if i + batch_size < len(candy_values):
                time.sleep(3)
        print(f'  Created {total_inserted} candidacies')

    # ── Summary ──
    print(f'\n{"="*60}')
    print('SUMMARY')
    print(f'{"="*60}')
    print(f'Found on BP:         {len(found)}')
    print(f'Not found on BP:     {len(not_found)}')
    print(f'Candidates reused:   {candidates_reused}')
    print(f'Candidates created:  {candidates_created}')

    if not_found:
        print(f'\nMissing (need manual research):')
        for chamber, dist, yr in sorted(not_found):
            print(f'  {chamber} {dist}, {yr}')


if __name__ == '__main__':
    main()
