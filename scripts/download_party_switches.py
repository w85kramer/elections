"""
Download state legislator party switch data from Ballotpedia.

Fetches the BP page listing state legislators who switched party affiliation,
parses the hierarchical list structure, and outputs JSON.

Source: https://ballotpedia.org/State_legislators_who_have_switched_political_party_affiliation

Output: /tmp/party_switches.json

Usage:
    python3 scripts/download_party_switches.py
    python3 scripts/download_party_switches.py --no-cache
"""
import sys
import os
import re
import json
import time
import argparse
import html as htmlmod

import httpx

CACHE_DIR = '/tmp/bp_party_switches'
CACHE_FILE = os.path.join(CACHE_DIR, 'party_switches.html')
OUTPUT_PATH = '/tmp/party_switches.json'

BP_URL = 'https://ballotpedia.org/State_legislators_who_have_switched_political_party_affiliation'

# Map BP party names to our abbreviations
PARTY_MAP = {
    'Republican': 'R',
    'Republicans': 'R',
    'Democratic': 'D',
    'Democrat': 'D',
    'Democrats': 'D',
    'Independent': 'I',
    'Independents': 'I',
    'Libertarian': 'L',
    'Green': 'G',
    'Nonpartisan': 'NP',
    'Progressive': 'Prog',
    'Working Families': 'WF',
    'Conservative': 'Con',
    'Constitution': 'Const',
    'No party preference': 'NP',
    'No Party Preference': 'NP',
}


def fetch_page(use_cache=True):
    """Fetch the BP party switches page, using cache if available."""
    os.makedirs(CACHE_DIR, exist_ok=True)

    if use_cache and os.path.exists(CACHE_FILE) and os.path.getsize(CACHE_FILE) > 1000:
        print(f'Using cached page: {CACHE_FILE}')
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            return f.read()

    print(f'Fetching {BP_URL}')
    resp = httpx.get(BP_URL, follow_redirects=True, timeout=30,
                     headers={'User-Agent': 'Mozilla/5.0 (compatible; ElectionsBot/1.0)'})
    if resp.status_code == 202:
        print('  Got 202, retrying in 5s...')
        time.sleep(5)
        resp = httpx.get(BP_URL, follow_redirects=True, timeout=30,
                         headers={'User-Agent': 'Mozilla/5.0 (compatible; ElectionsBot/1.0)'})
    resp.raise_for_status()
    html = resp.text

    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f'  Cached to {CACHE_FILE} ({len(html):,} bytes)')
    return html


def strip_html(text):
    """Remove HTML tags and decode entities."""
    text = re.sub(r'<[^>]+>', '', text)
    text = htmlmod.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def map_party(name):
    """Map a party name string to abbreviation code."""
    name = name.strip()
    if name in PARTY_MAP:
        return PARTY_MAP[name]
    # Try partial match
    for key, val in PARTY_MAP.items():
        if key.lower() in name.lower():
            return val
    return name  # Return as-is if no match


def parse_direction(text):
    """
    Parse a direction string like 'Democrats who switched to Republican' or
    'Republicans who switched to Independent'.
    Returns (from_party_code, to_party_code) or None.
    """
    # Pattern: "{Party} who switched to {Party}"
    m = re.match(r'(.+?)\s+who\s+switched\s+to\s+(.+)', text.strip(), re.IGNORECASE)
    if m:
        from_party = map_party(m.group(1))
        to_party = map_party(m.group(2))
        return from_party, to_party
    return None


def parse_h4_direction(text):
    """
    Parse an h4 heading like 'Senate Democrats who switched to Republican'
    or 'House Republican who switched to Independent'.

    Returns (chamber, from_party_code, to_party_code) or None.
    """
    text = text.strip()

    # Determine chamber from prefix
    chamber = None
    rest = text
    if text.startswith('Senate '):
        chamber = 'Senate'
        rest = text[7:]
    elif text.startswith('House '):
        chamber = 'House of Representatives'
        rest = text[6:]
    elif text.startswith('Senators '):
        chamber = 'Senate'
        rest = text[9:]  # "Senators who switched to minor parties"

    if not chamber:
        return None

    # Parse direction from the rest
    direction = parse_direction(rest)
    if direction:
        return chamber, direction[0], direction[1]

    # Handle special case: "Senators who switched to minor parties"
    m = re.match(r'who switched to (.+)', rest, re.IGNORECASE)
    if m:
        to_party = map_party(m.group(1))
        return chamber, '?', to_party  # unknown from_party

    return None


def parse_party_switches(html):
    """
    Parse the BP party switches page.

    Actual page structure:
    - h1 year headings (2025, 2024, ..., 1994)
    - Under each year, h4 headings with chamber+direction:
      "Senate Democrats who switched to Republican",
      "House Democrats who switched to Republican", etc.
    - Under each h4, <ul><li><a href="/Name">Name</a></li></ul>
    - Empty categories show "No officials have been added to this category."

    There's also a flat section (h1 "Senate" / h1 "House") without years,
    which we skip — we only parse the year-organized sections.

    Returns list of dicts with keys:
        name, chamber, from_party, to_party, year, bp_profile_url
    """
    records = []

    # We only want the year-organized sections (h1 with year IDs)
    # Scan for h1 headings and h4 headings + li items
    current_year = None
    current_chamber = None
    current_from_party = None
    current_to_party = None
    in_year_section = False  # True once we hit first year h1

    # Match h1, h2, h4 headings and li items
    tokens = re.finditer(
        r'<h1[^>]*>(.*?)</h1>|<h2[^>]*>(.*?)</h2>|<h4[^>]*>(.*?)</h4>|<li[^>]*>(.*?)</li>',
        html,
        re.DOTALL | re.IGNORECASE
    )

    for token in tokens:
        h1_text = token.group(1)
        h2_text = token.group(2)
        h4_text = token.group(3)
        li_text = token.group(4)

        if h1_text is not None:
            text = strip_html(h1_text).strip()
            # Check if it's a year heading
            year_match = re.match(r'^(\d{4})$', text)
            if year_match:
                current_year = int(year_match.group(1))
                in_year_section = True
                current_chamber = None
                current_from_party = None
                current_to_party = None
            else:
                # Non-year h1 — don't collect from flat sections
                if not in_year_section:
                    current_year = None

        elif h2_text is not None:
            text = strip_html(h2_text).strip()
            # Stop at "Chamber control" or "See also" sections
            if in_year_section and text in ('Chamber control', 'See also'):
                break

        elif h4_text is not None and in_year_section and current_year:
            text = strip_html(h4_text).strip()
            parsed = parse_h4_direction(text)
            if parsed:
                current_chamber, current_from_party, current_to_party = parsed
            else:
                # Unrecognized h4 — reset
                current_chamber = None

        elif li_text is not None and in_year_section and current_year and current_chamber and current_from_party:
            # Extract name and profile link from <li>
            # Links are relative: <a href="/Name_Here" title="Name Here">Name Here</a>
            link_match = re.search(
                r'<a[^>]+href="(/[^"]+)"[^>]*>(.*?)</a>',
                li_text,
                re.DOTALL | re.IGNORECASE
            )
            if not link_match:
                continue

            rel_path = link_match.group(1)
            name = strip_html(link_match.group(2))
            bp_url = f'https://ballotpedia.org{rel_path}'

            if not name or name.lower() in ('edit', 'source', ''):
                continue

            # Clean up name: remove parenthetical disambiguators from display
            # but keep them in the URL for uniqueness
            clean_name = re.sub(r'\s*\(.*?\)\s*', ' ', name).strip()
            clean_name = re.sub(r'\s+', ' ', clean_name)

            records.append({
                'name': clean_name,
                'chamber': current_chamber,
                'from_party': current_from_party,
                'to_party': current_to_party,
                'year': current_year,
                'bp_profile_url': bp_url,
            })

    return records


def main():
    parser = argparse.ArgumentParser(description='Download BP party switch data')
    parser.add_argument('--no-cache', action='store_true', help='Force re-download')
    args = parser.parse_args()

    use_cache = not args.no_cache
    html = fetch_page(use_cache=use_cache)
    print(f'Page size: {len(html):,} bytes')

    records = parse_party_switches(html)

    # Deduplicate (same name + year + direction)
    seen = set()
    deduped = []
    for r in records:
        key = (r['name'], r['year'], r['from_party'], r['to_party'])
        if key not in seen:
            seen.add(key)
            deduped.append(r)
        else:
            print(f'  Duplicate skipped: {r["name"]} ({r["year"]}, {r["from_party"]}->{r["to_party"]})')

    records = deduped

    # Write output
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(records, f, indent=2)

    # Summary
    print(f'\n{"=" * 60}')
    print(f'SUMMARY')
    print(f'{"=" * 60}')
    print(f'Total records: {len(records)}')

    # By direction
    from collections import Counter
    directions = Counter(f'{r["from_party"]}->{r["to_party"]}' for r in records)
    print(f'\nBy direction:')
    for d, count in directions.most_common():
        print(f'  {d}: {count}')

    # By chamber
    chambers = Counter(r['chamber'] for r in records)
    print(f'\nBy chamber:')
    for ch, count in chambers.most_common():
        print(f'  {ch}: {count}')

    # By year (recent first)
    years = Counter(r['year'] for r in records)
    print(f'\nBy year (recent):')
    for y in sorted(years.keys(), reverse=True)[:10]:
        print(f'  {y}: {years[y]}')

    print(f'\nOutput: {OUTPUT_PATH}')


if __name__ == '__main__':
    main()
