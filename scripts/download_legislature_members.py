"""
Download current state legislature member data from Ballotpedia.

Downloads ~99 Ballotpedia state legislature pages (one per chamber per state),
parses member tables, and outputs JSON for audit_seat_gaps.py.

Each page has a consistent "bptable gray" table with columns:
  Office | Name | Party | Date assumed office

Output: /tmp/legislature_members.json

Usage:
    python3 scripts/download_legislature_members.py
    python3 scripts/download_legislature_members.py --state TX
    python3 scripts/download_legislature_members.py --state NH --no-cache
"""
import sys
import os
import re
import json
import time
import argparse
import html as htmlmod

import httpx

CACHE_DIR = '/tmp/bp_legislatures'
OUTPUT_PATH = '/tmp/legislature_members.json'

# ══════════════════════════════════════════════════════════════════════
# STATE LEGISLATURE PAGES (99 pages: 49 senates + 49 houses + 1 NE unicameral)
# ══════════════════════════════════════════════════════════════════════
# Format: state_abbrev -> list of (bp_page_name, our_chamber_name)
# BP page name is used in URL: https://ballotpedia.org/{bp_page_name}
# Office column prefix on page: "{State_Name} {Chamber} District {N}"

STATE_PAGES = {
    'AL': [('Alabama_State_Senate', 'Senate'), ('Alabama_House_of_Representatives', 'House')],
    'AK': [('Alaska_State_Senate', 'Senate'), ('Alaska_House_of_Representatives', 'House')],
    'AZ': [('Arizona_State_Senate', 'Senate'), ('Arizona_House_of_Representatives', 'House')],
    'AR': [('Arkansas_State_Senate', 'Senate'), ('Arkansas_House_of_Representatives', 'House')],
    'CA': [('California_State_Senate', 'Senate'), ('California_State_Assembly', 'Assembly')],
    'CO': [('Colorado_State_Senate', 'Senate'), ('Colorado_House_of_Representatives', 'House')],
    'CT': [('Connecticut_State_Senate', 'Senate'), ('Connecticut_House_of_Representatives', 'House')],
    'DE': [('Delaware_State_Senate', 'Senate'), ('Delaware_House_of_Representatives', 'House')],
    'FL': [('Florida_State_Senate', 'Senate'), ('Florida_House_of_Representatives', 'House')],
    'GA': [('Georgia_State_Senate', 'Senate'), ('Georgia_House_of_Representatives', 'House')],
    'HI': [('Hawaii_State_Senate', 'Senate'), ('Hawaii_House_of_Representatives', 'House')],
    'ID': [('Idaho_State_Senate', 'Senate'), ('Idaho_House_of_Representatives', 'House')],
    'IL': [('Illinois_State_Senate', 'Senate'), ('Illinois_House_of_Representatives', 'House')],
    'IN': [('Indiana_State_Senate', 'Senate'), ('Indiana_House_of_Representatives', 'House')],
    'IA': [('Iowa_State_Senate', 'Senate'), ('Iowa_House_of_Representatives', 'House')],
    'KS': [('Kansas_State_Senate', 'Senate'), ('Kansas_House_of_Representatives', 'House')],
    'KY': [('Kentucky_State_Senate', 'Senate'), ('Kentucky_House_of_Representatives', 'House')],
    'LA': [('Louisiana_State_Senate', 'Senate'), ('Louisiana_House_of_Representatives', 'House')],
    'ME': [('Maine_State_Senate', 'Senate'), ('Maine_House_of_Representatives', 'House')],
    'MD': [('Maryland_State_Senate', 'Senate'), ('Maryland_House_of_Delegates', 'House of Delegates')],
    'MA': [('Massachusetts_State_Senate', 'Senate'), ('Massachusetts_House_of_Representatives', 'House')],
    'MI': [('Michigan_State_Senate', 'Senate'), ('Michigan_House_of_Representatives', 'House')],
    'MN': [('Minnesota_State_Senate', 'Senate'), ('Minnesota_House_of_Representatives', 'House')],
    'MS': [('Mississippi_State_Senate', 'Senate'), ('Mississippi_House_of_Representatives', 'House')],
    'MO': [('Missouri_State_Senate', 'Senate'), ('Missouri_House_of_Representatives', 'House')],
    'MT': [('Montana_State_Senate', 'Senate'), ('Montana_House_of_Representatives', 'House')],
    'NE': [('Nebraska_Legislature', 'Legislature')],
    'NV': [('Nevada_State_Senate', 'Senate'), ('Nevada_State_Assembly', 'Assembly')],
    'NH': [('New_Hampshire_State_Senate', 'Senate'), ('New_Hampshire_House_of_Representatives', 'House')],
    'NJ': [('New_Jersey_State_Senate', 'Senate'), ('New_Jersey_General_Assembly', 'Assembly')],
    'NM': [('New_Mexico_State_Senate', 'Senate'), ('New_Mexico_House_of_Representatives', 'House')],
    'NY': [('New_York_State_Senate', 'Senate'), ('New_York_State_Assembly', 'Assembly')],
    'NC': [('North_Carolina_State_Senate', 'Senate'), ('North_Carolina_House_of_Representatives', 'House')],
    'ND': [('North_Dakota_State_Senate', 'Senate'), ('North_Dakota_House_of_Representatives', 'House')],
    'OH': [('Ohio_State_Senate', 'Senate'), ('Ohio_House_of_Representatives', 'House')],
    'OK': [('Oklahoma_State_Senate', 'Senate'), ('Oklahoma_House_of_Representatives', 'House')],
    'OR': [('Oregon_State_Senate', 'Senate'), ('Oregon_House_of_Representatives', 'House')],
    'PA': [('Pennsylvania_State_Senate', 'Senate'), ('Pennsylvania_House_of_Representatives', 'House')],
    'RI': [('Rhode_Island_State_Senate', 'Senate'), ('Rhode_Island_House_of_Representatives', 'House')],
    'SC': [('South_Carolina_State_Senate', 'Senate'), ('South_Carolina_House_of_Representatives', 'House')],
    'SD': [('South_Dakota_State_Senate', 'Senate'), ('South_Dakota_House_of_Representatives', 'House')],
    'TN': [('Tennessee_State_Senate', 'Senate'), ('Tennessee_House_of_Representatives', 'House')],
    'TX': [('Texas_State_Senate', 'Senate'), ('Texas_House_of_Representatives', 'House')],
    'UT': [('Utah_State_Senate', 'Senate'), ('Utah_House_of_Representatives', 'House')],
    'VT': [('Vermont_State_Senate', 'Senate'), ('Vermont_House_of_Representatives', 'House')],
    'VA': [('Virginia_State_Senate', 'Senate'), ('Virginia_House_of_Delegates', 'House of Delegates')],
    'WA': [('Washington_State_Senate', 'Senate'), ('Washington_House_of_Representatives', 'House')],
    'WV': [('West_Virginia_State_Senate', 'Senate'), ('West_Virginia_House_of_Delegates', 'House of Delegates')],
    'WI': [('Wisconsin_State_Senate', 'Senate'), ('Wisconsin_State_Assembly', 'Assembly')],
    'WY': [('Wyoming_State_Senate', 'Senate'), ('Wyoming_House_of_Representatives', 'House')],
}

# Map BP party names to our abbreviations
PARTY_MAP = {
    'Republican': 'R',
    'Democratic': 'D',
    'Democrat': 'D',
    'Independent': 'I',
    'Libertarian': 'L',
    'Green': 'G',
    'Nonpartisan': 'NP',
    'Progressive': 'Prog',
    'Working Families': 'WF',
    'Conservative': 'Con',
    'Vermont Progressive': 'Prog',
    'Independence': 'Ind',
    'Liberal': 'Lib',
}

# ══════════════════════════════════════════════════════════════════════
# HTML PARSING
# ══════════════════════════════════════════════════════════════════════

def fetch_page(bp_page_name, use_cache=True):
    """Fetch a BP page, using cache if available."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_file = os.path.join(CACHE_DIR, f'{bp_page_name}.html')

    if use_cache and os.path.exists(cache_file):
        with open(cache_file, 'r', encoding='utf-8') as f:
            return f.read()

    url = f'https://ballotpedia.org/{bp_page_name}'
    print(f'  Fetching {url}')
    resp = httpx.get(url, follow_redirects=True, timeout=30,
                     headers={'User-Agent': 'Mozilla/5.0 (compatible; ElectionsBot/1.0)'})
    if resp.status_code == 202:
        # CDN warming, retry after delay
        print(f'    Got 202, retrying in 5s...')
        time.sleep(5)
        resp = httpx.get(url, follow_redirects=True, timeout=30,
                         headers={'User-Agent': 'Mozilla/5.0 (compatible; ElectionsBot/1.0)'})
    resp.raise_for_status()
    html = resp.text

    with open(cache_file, 'w', encoding='utf-8') as f:
        f.write(html)

    time.sleep(1.5)  # Rate limiting
    return html


def extract_district_number(office_text, state_abbrev, chamber):
    """
    Extract district number/name from the Office column text.

    BP format examples:
      "Texas State Senate District 1" -> "1"
      "Maryland House of Delegates District 1A" -> "1A"
      "New Hampshire House of Representatives Belknap 5" -> "Belknap-5"
      "Nebraska Legislature District 1" -> "1"
      "Vermont House of Representatives Addison-1" -> "Addison-1"
      "Vermont House of Representatives Grand Isle-Chittenden" -> "Grand Isle-Chittenden"
    """
    text = office_text.strip()

    # NH House: county-based names like "Belknap 5" or "Rockingham 12"
    # Pattern: after the chamber name, we have "County_name Number"
    if state_abbrev == 'NH' and chamber == 'House':
        # Remove the prefix "New Hampshire House of Representatives "
        prefix = 'New Hampshire House of Representatives '
        if text.startswith(prefix):
            remainder = text[len(prefix):]
            # Convert "Belknap 5" -> "Belknap-5"
            remainder = remainder.strip()
            # If it ends with a number, add hyphen: "Belknap 5" -> "Belknap-5"
            m2 = re.match(r'^(.+?)\s+(\d+)$', remainder)
            if m2:
                return f'{m2.group(1)}-{m2.group(2)}'
            return remainder

    # VT House: named districts like "Addison 1 District" -> "Addison-1"
    # Multi-county: "Addison Rutland 1 District" -> "Addison-Rutland-1"
    #   (audit script will handle mapping to DB name "Addison-Rutland" if needed)
    # Already hyphenated: "Caledonia-Essex District" -> "Caledonia-Essex"
    if state_abbrev == 'VT' and chamber == 'House':
        prefix = 'Vermont House of Representatives '
        if text.startswith(prefix):
            remainder = text[len(prefix):].strip()
            # Strip trailing " District"
            remainder = re.sub(r'\s+District$', '', remainder)
            # Replace spaces with hyphens (preserves existing hyphens)
            remainder = remainder.replace(' ', '-')
            return remainder

    # VT Senate: named districts "Addison District" -> "Addison"
    # Multi-word: "Chittenden Central District" -> "Chittenden-Central"
    if state_abbrev == 'VT' and chamber == 'Senate':
        prefix = 'Vermont State Senate '
        if text.startswith(prefix):
            remainder = text[len(prefix):].strip()
            # Strip trailing " District"
            remainder = re.sub(r'\s+District$', '', remainder)
            # Replace spaces with hyphens
            remainder = remainder.replace(' ', '-')
            return remainder

    # Most states: "Texas State Senate District 1" -> "1"
    m = re.search(r'District\s+(.+)$', text, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # Fallback: take everything after the last recognized chamber word
    for pattern in ['Senate ', 'House of Representatives ', 'House of Delegates ',
                    'State Assembly ', 'General Assembly ', 'Assembly ', 'Legislature ']:
        idx = text.rfind(pattern)
        if idx >= 0:
            remainder = text[idx + len(pattern):]
            remainder = re.sub(r'^District\s+', '', remainder)
            return remainder.strip()

    return text  # Last resort


def parse_member_table(html_content, state_abbrev, chamber):
    """
    Parse the member table from a BP state legislature page.

    The table has class "bptable gray" and columns:
    Office | Name | Party | Date assumed office

    Returns list of dicts with keys:
      state, chamber, district, name, party, assumed_office, is_vacant
    """
    members = []

    # Find all "bptable gray" tables
    # The member table is typically the first/main one with 4 columns
    table_pattern = r'<table[^>]*class="[^"]*bptable\s+gray[^"]*"[^>]*>(.*?)</table>'
    tables = re.findall(table_pattern, html_content, re.DOTALL | re.IGNORECASE)

    if not tables:
        # Try alternative: sortable wikitable
        table_pattern = r'<table[^>]*class="[^"]*wikitable\s+sortable[^"]*"[^>]*>(.*?)</table>'
        tables = re.findall(table_pattern, html_content, re.DOTALL | re.IGNORECASE)

    if not tables:
        print(f'    WARNING: No member table found for {state_abbrev} {chamber}')
        return members

    # Find the member table: should have "Office" and "Name" headers
    member_table = None
    for t in tables:
        if re.search(r'<th[^>]*>.*?Office.*?</th>', t, re.DOTALL | re.IGNORECASE) and \
           re.search(r'<th[^>]*>.*?Name.*?</th>', t, re.DOTALL | re.IGNORECASE):
            member_table = t
            break

    if not member_table:
        # Fallback: use first table with 4+ columns
        for t in tables:
            header_count = len(re.findall(r'<th', t, re.IGNORECASE))
            if header_count >= 4:
                member_table = t
                break

    if not member_table:
        print(f'    WARNING: Could not identify member table for {state_abbrev} {chamber}')
        return members

    # Extract rows
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', member_table, re.DOTALL | re.IGNORECASE)

    for row in rows:
        # Skip header rows
        if '<th' in row.lower():
            continue

        # Extract cells (td)
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
        if len(cells) < 3:
            continue

        # Parse Office (column 0)
        office_raw = strip_html(cells[0])

        # Parse Name (column 1)
        name_raw = strip_html(cells[1])

        # Parse Party (column 2)
        party_raw = strip_html(cells[2])

        # Parse Date assumed office (column 3, if present)
        assumed_office = ''
        if len(cells) >= 4:
            assumed_office = strip_html(cells[3])

        # Extract district number
        district = extract_district_number(office_raw, state_abbrev, chamber)
        if not district:
            continue

        # Determine vacancy
        is_vacant = False
        name_clean = name_raw.strip()
        if name_clean.lower() in ('vacant', '', '—', '-', 'n/a'):
            is_vacant = True
            name_clean = ''

        # Map party
        party = PARTY_MAP.get(party_raw.strip(), party_raw.strip())
        if is_vacant:
            party = ''

        members.append({
            'state': state_abbrev,
            'chamber': chamber,
            'district': district,
            'name': name_clean,
            'party': party,
            'assumed_office': assumed_office.strip(),
            'is_vacant': is_vacant,
        })

    return members


def strip_html(text):
    """Remove HTML tags and decode entities."""
    # Remove tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode entities
    text = htmlmod.unescape(text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Download BP legislature member data')
    parser.add_argument('--state', help='Process single state (e.g., TX)')
    parser.add_argument('--no-cache', action='store_true', help='Force re-download')
    args = parser.parse_args()

    use_cache = not args.no_cache

    if args.state:
        states_to_process = [args.state.upper()]
        if states_to_process[0] not in STATE_PAGES:
            print(f'ERROR: Unknown state {states_to_process[0]}')
            sys.exit(1)
    else:
        states_to_process = sorted(STATE_PAGES.keys())

    all_members = []
    state_counts = {}

    for state in states_to_process:
        pages = STATE_PAGES[state]
        state_total = 0
        state_vacant = 0

        for bp_page, chamber in pages:
            print(f'Processing {state} {chamber} ({bp_page})...')
            html = fetch_page(bp_page, use_cache=use_cache)
            members = parse_member_table(html, state, chamber)
            print(f'  Found {len(members)} members ({sum(1 for m in members if m["is_vacant"])} vacant)')

            all_members.extend(members)
            state_total += len(members)
            state_vacant += sum(1 for m in members if m['is_vacant'])

        state_counts[state] = {'total': state_total, 'vacant': state_vacant}
        print(f'  {state} total: {state_total} seats, {state_vacant} vacant')

    # If processing a single state and we already have data, merge
    if args.state and os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, 'r') as f:
            existing = json.load(f)
        # Remove old entries for this state
        existing = [m for m in existing if m['state'] != args.state.upper()]
        existing.extend(all_members)
        all_members = existing

    # Write output
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(all_members, f, indent=2)

    print(f'\n═══ SUMMARY ═══')
    total = sum(c['total'] for c in state_counts.values())
    vacant = sum(c['vacant'] for c in state_counts.values())
    print(f'States processed: {len(state_counts)}')
    print(f'Total members parsed: {total}')
    print(f'Vacant seats: {vacant}')
    print(f'Filled seats: {total - vacant}')
    print(f'Output: {OUTPUT_PATH}')


if __name__ == '__main__':
    main()
