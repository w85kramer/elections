"""
Download historical election results from Ballotpedia district pages.

Pipeline:
  Phase A — Fetch chamber index pages to get district URLs
  Phase B — Download individual district pages (cached)
  Phase C — Parse election history from cached HTML → JSON

Output: /tmp/district_history/{state_abbr}.json

Usage:
    python3 scripts/download_district_history.py --state AK
    python3 scripts/download_district_history.py --state AK --chamber Senate
    python3 scripts/download_district_history.py --state AK --parse-only
    python3 scripts/download_district_history.py --state AK --index-only
    python3 scripts/download_district_history.py --state AK --no-cache
"""
import sys
import os
import re
import json
import time
import argparse
import html as htmlmod
from datetime import datetime

import httpx

# ══════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════

INDEX_CACHE_DIR = '/tmp/bp_chamber_index'
DISTRICT_CACHE_DIR = '/tmp/bp_district_history'
OUTPUT_DIR = '/tmp/district_history'

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

# DB chamber → BP URL fragment for chamber index pages
CHAMBER_URL = {
    'Senate': 'State_Senate',
    'House': 'House_of_Representatives',
    'Assembly': 'State_Assembly',
    'House of Delegates': 'House_of_Delegates',
    'Legislature': 'State_Legislature',
}

# State-specific overrides for index page URLs
CHAMBER_URL_OVERRIDES = {
    ('NJ', 'Assembly'): 'General_Assembly',
}

# Which chambers exist per state (DB chamber name → list of states)
STATE_CHAMBERS = {}
# Build it: most states have Senate + House
for _st in STATE_NAMES:
    if _st == 'NE':
        STATE_CHAMBERS[_st] = ['Legislature']
    elif _st in ('CA', 'NV', 'WI', 'NY'):
        STATE_CHAMBERS[_st] = ['Senate', 'Assembly']
    elif _st in ('MD', 'VA', 'WV'):
        STATE_CHAMBERS[_st] = ['Senate', 'House of Delegates']
    elif _st == 'NJ':
        STATE_CHAMBERS[_st] = ['Senate', 'Assembly']
    else:
        STATE_CHAMBERS[_st] = ['Senate', 'House']

HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (compatible; ElectionsBot/1.0)'}

# Standard party abbreviation mapping
PARTY_MAP = {
    'Republican': 'R', 'Democratic': 'D', 'Democrat': 'D',
    'Libertarian': 'L', 'Green': 'G', 'Independent': 'I',
    'Constitution': 'C', 'Reform': 'Ref', 'Working Families': 'WF',
    'Conservative': 'Con', 'Nonpartisan': 'NP',
    'R': 'R', 'D': 'D', 'L': 'L', 'G': 'G', 'I': 'I',
}


# ══════════════════════════════════════════════════════════════════════
# HTTP FETCHING
# ══════════════════════════════════════════════════════════════════════

def fetch_page(url, cache_path, use_cache=True, max_retries=3):
    """Fetch a URL with caching and 202 retry logic."""
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


def strip_html(text):
    """Remove HTML tags and decode entities."""
    text = re.sub(r'<[^>]+>', '', text)
    text = htmlmod.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# ══════════════════════════════════════════════════════════════════════
# PHASE A — CHAMBER INDEX PAGES → DISTRICT URLS
# ══════════════════════════════════════════════════════════════════════

def build_index_url(state, chamber):
    """Build the BP chamber index page URL."""
    state_name = STATE_NAMES[state]
    key = (state, chamber)
    if key in CHAMBER_URL_OVERRIDES:
        fragment = CHAMBER_URL_OVERRIDES[key]
    else:
        fragment = CHAMBER_URL.get(chamber, chamber.replace(' ', '_'))
    return f'https://ballotpedia.org/{state_name}_{fragment}'


def parse_index_page(html, state, chamber):
    """
    Parse a chamber index page to extract district URLs.

    Returns list of dicts:
      {bp_url, bp_district_name, district_identifier, current_member, current_party}
    """
    districts = []

    # BP index pages use tables with links to individual district pages.
    # Look for all links matching district page URL patterns.
    state_name = STATE_NAMES[state]
    state_name_space = state_name.replace('_', ' ')

    # Build pattern for district page URLs
    # e.g., /Alaska_State_Senate_District_A, /Alaska_House_of_Representatives_District_1
    # For MA: /Massachusetts_House_of_Representatives_1st_Barnstable_District
    # For NH: /New_Hampshire_House_of_Representatives_Grafton_1
    # For NE: /Nebraska_State_Legislature_District_1 (but district pages use State_Senate)

    # Strategy: find all <a> links that point to district subpages of this chamber
    # Use a broad pattern and filter

    # Determine the chamber part of the URL
    key = (state, chamber)
    if key in CHAMBER_URL_OVERRIDES:
        chamber_fragment = CHAMBER_URL_OVERRIDES[key]
    else:
        chamber_fragment = CHAMBER_URL.get(chamber, chamber.replace(' ', '_'))

    # For NE, district pages use "State_Senate" not "State_Legislature"
    if state == 'NE':
        district_url_fragment = f'{state_name}_State_Senate'
    else:
        district_url_fragment = f'{state_name}_{chamber_fragment}'

    # Find all links to district pages
    # Pattern: href="/State_Chamber_District_X" or href="https://ballotpedia.org/State_Chamber_District_X"
    link_pattern = re.compile(
        r'href="(?:https?://ballotpedia\.org)?(/(' + re.escape(district_url_fragment) + r'[^"]*?))"',
        re.IGNORECASE
    )

    seen_urls = set()
    for m in link_pattern.finditer(html):
        path = m.group(1)
        full_url = f'https://ballotpedia.org{path}'

        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        # Skip links that are clearly not individual district pages
        # (e.g., links to elections, committees, etc.)
        path_lower = path.lower()
        if any(skip in path_lower for skip in [
            'election', 'primary', 'general', 'special', 'redistrict',
            'committee', 'leadership', 'history', 'members', 'caucus',
            'campaign', 'legislation', '#',
        ]):
            continue

        # Extract the district identifier from the URL
        district_id = extract_district_id(path, state, chamber, district_url_fragment)
        if not district_id:
            continue

        # Try to get the link text for the district name
        # Look for the <a> tag context to find member name/party
        bp_district_name = path.split('/')[-1].replace('_', ' ')

        districts.append({
            'bp_url': full_url,
            'bp_district_name': bp_district_name,
            'district_identifier': district_id,
        })

    # Deduplicate by district_identifier
    seen_ids = set()
    unique = []
    for d in districts:
        if d['district_identifier'] not in seen_ids:
            seen_ids.add(d['district_identifier'])
            unique.append(d)

    return unique


def extract_district_id(path, state, chamber, url_fragment):
    """Extract a district identifier from a BP URL path."""
    # Remove the base part to get the district-specific suffix
    slug = path.split('/')[-1]

    # For MA: "Massachusetts_House_of_Representatives_1st_Barnstable_District"
    if state == 'MA':
        # Extract the named district
        prefix = url_fragment + '_'
        if slug.startswith(prefix):
            name_part = slug[len(prefix):]
            # Remove trailing "_District" if present
            name_part = re.sub(r'_District$', '', name_part)
            return name_part.replace('_', ' ')
        return None

    # For NH: "New_Hampshire_House_of_Representatives_Grafton_7"
    # or "New_Hampshire_House_of_Representatives_District_Grafton_7"
    if state == 'NH' and chamber == 'House':
        prefix = url_fragment + '_'
        if slug.startswith(prefix):
            suffix = slug[len(prefix):]
            suffix = re.sub(r'^District_', '', suffix)
            return suffix.replace('_', ' ')
        return None

    # For VT: named districts
    if state == 'VT':
        prefix = url_fragment + '_'
        if slug.startswith(prefix):
            name_part = slug[len(prefix):]
            name_part = re.sub(r'^District_', '', name_part)
            return name_part.replace('_', ' ')
        return None

    # For NE: "Nebraska_State_Senate_District_1"
    if state == 'NE':
        m = re.search(r'District_(\d+)', slug)
        if m:
            return m.group(1)
        return None

    # Standard: look for "District_X" at the end
    m = re.search(r'District_(.+)$', slug)
    if m:
        district_part = m.group(1)
        # Could be numeric, letter (AK Senate), or sub-district (1A, 1B)
        return district_part.replace('_', ' ')

    return None


def fetch_district_urls(state, chamber, use_cache=True):
    """Phase A: fetch index page and extract district URLs for one chamber."""
    url = build_index_url(state, chamber)
    slug = f'{state}_{chamber.replace(" ", "_")}'
    cache_path = os.path.join(INDEX_CACHE_DIR, f'{slug}.html')

    print(f'  Fetching index: {url}')
    html = fetch_page(url, cache_path, use_cache=use_cache)
    if not html:
        print(f'    FAILED to download index page')
        return []

    districts = parse_index_page(html, state, chamber)
    print(f'    Found {len(districts)} district URLs')
    return districts


# ══════════════════════════════════════════════════════════════════════
# PHASE B — DOWNLOAD INDIVIDUAL DISTRICT PAGES
# ══════════════════════════════════════════════════════════════════════

def download_district_pages(state, chamber, districts, use_cache=True):
    """Phase B: download all district pages for a state/chamber."""
    chamber_slug = chamber.lower().replace(' ', '_')
    cache_dir = os.path.join(DISTRICT_CACHE_DIR, state, chamber_slug)
    os.makedirs(cache_dir, exist_ok=True)

    total = len(districts)
    downloaded = 0
    cached = 0

    for i, dist in enumerate(districts):
        url = dist['bp_url']
        # Build a safe filename from the district identifier
        safe_id = dist['district_identifier'].replace(' ', '_').replace('/', '_')
        cache_path = os.path.join(cache_dir, f'{safe_id}.html')

        if use_cache and os.path.exists(cache_path):
            cached += 1
            continue

        print(f'    [{i+1}/{total}] {url}')
        html = fetch_page(url, cache_path, use_cache=False)
        if html:
            downloaded += 1
        time.sleep(0.5)

    print(f'    Downloaded: {downloaded}, Cached: {cached}, Total: {total}')


# ══════════════════════════════════════════════════════════════════════
# PHASE C — PARSE ELECTION HISTORY FROM CACHED HTML
# ══════════════════════════════════════════════════════════════════════

def parse_district_elections(html, state, chamber, district_id):
    """
    Parse all historical elections from a BP district page.

    Returns list of election dicts, each with:
      year, election_type, election_date, total_votes, candidates[]
    """
    if not html:
        return []

    elections = []

    # Strategy: find all election result sections using three parsers:
    # 1. Modern votebox format (results_row + votebox-results-cell) — 2018+
    # 2. RCV tables (results_table{hash} with round data) — AK/ME 2022+
    # 3. Old collapsible tables (mw-collapsible with Party/Candidate/Vote%/Votes) — pre-2018

    elections.extend(parse_votebox_elections(html, state, chamber, district_id))
    elections.extend(parse_rcv_elections(html, state, chamber, district_id))
    elections.extend(parse_collapsible_elections(html, state, chamber, district_id))

    # Deduplicate: same year + type + date
    seen = set()
    unique = []
    for e in elections:
        key = (e['year'], e['election_type'], e.get('election_date', ''))
        if key not in seen:
            seen.add(key)
            unique.append(e)

    # Sort by year descending, then type
    type_order = {'General': 0, 'Primary': 1, 'Primary_D': 2, 'Primary_R': 3,
                  'Primary_Nonpartisan': 4, 'Special': 5}
    unique.sort(key=lambda e: (-e['year'], type_order.get(e['election_type'], 9)))

    return unique


def parse_votebox_elections(html, state, chamber, district_id):
    """Parse modern votebox-style election results with results_row class (2018+)."""
    elections = []

    # Find each votebox section: header text → results_table with results_row entries
    # We search for results_row entries and work backward to find context
    row_pattern = re.compile(
        r'<tr\s+class="results_row\s*(winner)?\s*"[^>]*>.*?</tr>',
        re.DOTALL
    )

    # Find all results_row positions
    row_positions = [(m.start(), m.end()) for m in row_pattern.finditer(html)]
    if not row_positions:
        return elections

    # Group rows that are close together (within same table)
    groups = []
    current_group = [row_positions[0]]
    for pos in row_positions[1:]:
        if pos[0] - current_group[-1][1] < 2000:
            current_group.append(pos)
        else:
            groups.append(current_group)
            current_group = [pos]
    groups.append(current_group)

    for group in groups:
        first_pos = group[0][0]
        last_pos = group[-1][1]

        # Get preceding text for election type identification
        before = html[max(0, first_pos - 3000):first_pos]
        election_info = identify_election_type(before, state, chamber)
        if not election_info:
            continue

        year, election_type, election_date = election_info
        if year > 2025 or year < 2010:
            continue

        # Parse candidates from the group of rows
        table_html = html[first_pos:last_pos + 500]
        candidates = parse_results_table_candidates(table_html)
        if not candidates:
            continue

        # Get total votes from after the table
        after = html[last_pos:last_pos + 1000]
        total_votes = extract_total_votes(after)
        if total_votes is None:
            vote_sum = sum(c['votes'] for c in candidates if c['votes'] is not None)
            if vote_sum > 0:
                total_votes = vote_sum

        elections.append({
            'year': year,
            'election_type': election_type,
            'election_date': election_date,
            'total_votes': total_votes,
            'candidates': candidates,
        })

    return elections


def parse_rcv_elections(html, state, chamber, district_id):
    """Parse RCV (ranked-choice voting) election results (AK 2022+, ME)."""
    elections = []

    # RCV tables use class like "results_table699520a613d25" (hash suffix)
    # They appear inside rcvresults_table_container divs
    # Context text: "General election for ... The ranked-choice voting election was won by ..."
    # NOTE: The year heading can be 10000+ chars before the table due to CSS/JS blocks

    rcv_pattern = re.compile(
        r'<table[^>]*class="(results_table[0-9a-f]+)"[^>]*>(.*?)</table>',
        re.DOTALL | re.IGNORECASE
    )

    for m in rcv_pattern.finditer(html):
        table_html = m.group(2)

        # Get preceding text — need to look MUCH farther back for RCV due to
        # large CSS/JS blocks between year headings and tables
        before = html[max(0, m.start() - 15000):m.start()]

        # First try standard identification
        election_info = identify_election_type(before, state, chamber)

        # If no year found, try extracting from section heading
        if not election_info:
            # Look for <span id="YYYY"> heading pattern
            heading_matches = re.findall(r'id="(20[12]\d)"', before)
            if heading_matches:
                year = int(heading_matches[-1])
                # Determine type from closer context
                close_before = before[-2000:]
                close_clean = strip_html(close_before).lower()
                if 'general election' in close_clean or 'ranked-choice' in close_clean:
                    election_type = 'General'
                elif 'primary' in close_clean:
                    election_type = 'Primary_Nonpartisan'
                else:
                    election_type = 'General'
                election_date = extract_election_date(strip_html(close_before), year)
                election_info = (year, election_type, election_date)

        if not election_info:
            continue

        year, election_type, election_date = election_info
        if year > 2025 or year < 2010:
            continue

        # Parse RCV candidates from the table
        # Columns: (check) | (image) | Candidate | % | Votes | Transfer | Round eliminated
        candidates = []
        for row in re.finditer(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL):
            row_html = row.group(1)
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL)
            if len(cells) < 4:
                continue

            # Find candidate name (cell with <a> link)
            name = None
            for cell in cells:
                name_m = re.search(r'<a[^>]*>([^<]+)</a>', cell)
                if name_m:
                    candidate_name = htmlmod.unescape(name_m.group(1).strip())
                    if candidate_name and len(candidate_name) > 2:
                        name = candidate_name
                        break

            if not name or name.lower() in ('total', 'write-in', 'other'):
                continue

            # Skip header rows
            if 'Candidate' in name:
                continue

            # Party from image class or context
            party = None
            party_img = re.search(r'class="[^"]*\b(Republican|Democratic|Libertarian|Green|Independent)\b', row_html)
            if party_img:
                party = PARTY_MAP.get(party_img.group(1))

            # Extract percentage and votes from cells
            pct = None
            votes = None
            for cell in cells:
                cell_text = strip_html(cell).strip()
                # Percentage
                pct_m = re.match(r'^([\d.]+)$', cell_text)
                if pct_m and pct is None:
                    val = float(pct_m.group(1))
                    if val <= 100:
                        pct = val
                        continue
                # Votes (with commas)
                votes_m = re.match(r'^([\d,]+)$', cell_text)
                if votes_m and votes is None:
                    votes = int(votes_m.group(1).replace(',', ''))

            # Winner detection: check the last cell (round eliminated) for "Won"
            is_winner = False
            if cells:
                last_cell_text = strip_html(cells[-1]).strip()
                if 'Won' in last_cell_text:
                    is_winner = True

            # Incumbent: bold+underline pattern
            is_incumbent = bool(re.search(r'<b><u><a', row_html))

            candidates.append({
                'name': name,
                'party': party,
                'votes': votes,
                'pct': pct,
                'winner': is_winner,
                'incumbent': is_incumbent,
            })

        if candidates:
            total_votes = sum(c['votes'] for c in candidates if c['votes'] is not None)
            elections.append({
                'year': year,
                'election_type': election_type,
                'election_date': election_date,
                'total_votes': total_votes if total_votes > 0 else None,
                'candidates': candidates,
            })

    return elections


def parse_collapsible_elections(html, state, chamber, district_id):
    """Parse older collapsible table election results (pre-2018)."""
    elections = []

    # These tables have class "mw-collapsible" and contain:
    # Header row: Party | Candidate | Vote% | Votes
    # Data rows with party name, candidate link, percentage, vote count
    # Title row: "Alaska State Senate, District A, General Election, 2014"
    # Table classes vary by year: "mw-collapsible" (2012), "bptable gray collapsible" (2016)

    table_pattern = re.compile(
        r'<table[^>]*class="[^"]*collapsible[^"]*"[^>]*>(.*?)</table>',
        re.DOTALL | re.IGNORECASE
    )

    for m in table_pattern.finditer(html):
        table_html = m.group(1)
        full_table = m.group(0)

        # Look for title text like "District X, General Election, 2014"
        title_m = re.search(
            r'(?:General|Primary|Special|Nonpartisan|Democratic|Republican)[^<]*(?:Election|election)[^<]*(\d{4})',
            full_table
        )
        if not title_m:
            # Try the text just before the table
            before = html[max(0, m.start()-500):m.start()]
            title_m = re.search(
                r'(?:General|Primary|Special|Nonpartisan|Democratic|Republican)[^<]*(?:Election|election)[^<]*(\d{4})',
                before
            )
        if not title_m:
            continue

        year = int(title_m.group(1))
        if year > 2025 or year < 2010:
            continue

        # Determine election type from title
        title_text = title_m.group(0).lower()
        if 'special' in title_text:
            election_type = 'Special'
        elif 'democratic primary' in title_text:
            election_type = 'Primary_D'
        elif 'republican primary' in title_text:
            election_type = 'Primary_R'
        elif 'nonpartisan' in title_text:
            election_type = 'Primary_Nonpartisan'
        elif 'primary' in title_text:
            election_type = 'Primary'
        elif 'general' in title_text:
            election_type = 'General'
        else:
            election_type = 'General'

        # Parse rows: look for <td> cells with party, name, percentage, votes
        candidates = []
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if len(cells) < 3:
                continue

            cell_texts = [strip_html(c).strip() for c in cells]

            # Skip header row
            if any(h in cell_texts for h in ['Party', 'Candidate', 'Vote\xa0%', 'Vote %']):
                continue
            # Skip total row
            if 'Total' in cell_texts[0] or 'Total Votes' in cell_texts[0]:
                continue

            party = None
            name = None
            pct = None
            votes = None
            is_winner = False

            for j, cell in enumerate(cells):
                cell_text = cell_texts[j]
                cell_html = cells[j] if j < len(cells) else ''

                # Check for green check mark (winner indicator)
                # Don't continue — the name may also be in this cell
                if 'Green_check_mark' in cell or 'check_mark' in cell:
                    is_winner = True

                # Empty/spacing cells
                if not cell_text or cell_text in ('', '\xa0', '&#160;'):
                    continue

                # Party names
                if cell_text in PARTY_MAP:
                    party = PARTY_MAP[cell_text]
                    continue

                # Percentage (e.g., "60.8%")
                pct_m = re.match(r'^([\d.]+)%?$', cell_text)
                if pct_m:
                    val = float(pct_m.group(1))
                    if val <= 100 and pct is None:
                        pct = val
                        continue

                # Vote count (e.g., "5,393")
                if re.match(r'^[\d,]+$', cell_text):
                    val = int(cell_text.replace(',', ''))
                    if val > 0 and votes is None:
                        votes = val
                        continue

                # Candidate name — look for link or text with "Incumbent" tag
                name_link = re.search(r'<a[^>]*>([^<]+)</a>', cells[j] if j < len(cells) else '')
                if name_link:
                    name = htmlmod.unescape(name_link.group(1).strip())
                elif len(cell_text) > 2 and not cell_text.replace(',', '').replace('.', '').replace('%', '').isdigit():
                    # Clean "Incumbent" suffix
                    cleaned = re.sub(r'\s*Incumbent\s*$', '', cell_text).strip()
                    if cleaned:
                        name = cleaned

            if name and name.lower() not in ('total', 'total votes', 'write-in', 'other'):
                is_incumbent = 'Incumbent' in str(cells) if cells else False
                candidates.append({
                    'name': name,
                    'party': party,
                    'votes': votes,
                    'pct': pct,
                    'winner': is_winner,
                    'incumbent': is_incumbent,
                })

        if candidates:
            election_date = extract_election_date(
                strip_html(html[max(0, m.start()-500):m.start()]) + ' ' + strip_html(full_table[:500]),
                year
            )
            total_votes = sum(c['votes'] for c in candidates if c['votes'] is not None)
            elections.append({
                'year': year,
                'election_type': election_type,
                'election_date': election_date,
                'total_votes': total_votes if total_votes > 0 else None,
                'candidates': candidates,
            })

    return elections


def identify_election_type(text_before, state, chamber):
    """
    Identify the election type, year, and date from text preceding a results table.

    Returns (year, election_type, election_date) or None.
    """
    # Clean HTML for easier matching
    clean = strip_html(text_before[-2000:])

    # Look for election header patterns
    # "General election for Alaska State Senate District A, 2022"
    # "Democratic primary for ... , 2022"
    # "Republican primary for ... , 2020"
    # "Nonpartisan primary for ... , 2022"
    # "Special general election for ..."
    # "General election, 2022"

    # Extract year — try several strategies
    year = None

    # 1. Look for year near the end of text (within election context)
    year_match = re.search(r'\b(20[12]\d)\b', clean[-500:])
    if year_match:
        year = int(year_match.group(1))
    else:
        # 2. Look for year in HTML attributes (span id="2022", etc.)
        raw_before = text_before[-3000:]
        id_match = re.search(r'id="(20[12]\d)"', raw_before)
        if id_match:
            year = int(id_match.group(1))
        else:
            # 3. Look for year in h2/h3 headings in the raw HTML
            heading_match = re.findall(r'<h[23][^>]*>.*?(20[12]\d).*?</h[23]>', raw_before, re.DOTALL)
            if heading_match:
                year = int(heading_match[-1])
            else:
                # 4. Look farther back in clean text
                year_match = re.search(r'\b(20[12]\d)\b', clean[-1500:])
                if year_match:
                    year = int(year_match.group(1))
    if not year:
        return None

    # Determine election type
    text_lower = clean.lower()

    # Check for special elections
    if 'special' in text_lower[-500:]:
        if 'primary' in text_lower[-300:]:
            election_type = 'Special_Primary'
        elif 'runoff' in text_lower[-300:]:
            election_type = 'Special_Runoff'
        else:
            election_type = 'Special'
    elif 'general election' in text_lower[-500:] or 'general election' in text_lower[-300:]:
        election_type = 'General'
    elif 'democratic primary' in text_lower[-500:]:
        election_type = 'Primary_D'
    elif 'republican primary' in text_lower[-500:]:
        election_type = 'Primary_R'
    elif 'nonpartisan' in text_lower[-500:] and 'primary' in text_lower[-500:]:
        election_type = 'Primary_Nonpartisan'
    elif 'primary' in text_lower[-300:]:
        # Generic primary — try to determine party
        election_type = 'Primary'
    elif 'general' in text_lower[-300:]:
        election_type = 'General'
    elif 'runoff' in text_lower[-300:]:
        election_type = 'General_Runoff'
    else:
        # Default to General if we can't determine
        election_type = 'General'

    # Handle jungle primary states — CA, WA, AK, LA
    # Their "primary" is really a nonpartisan primary
    if state in ('CA', 'WA') and election_type == 'Primary':
        election_type = 'Primary_Nonpartisan'

    # Extract date if present
    election_date = extract_election_date(clean[-500:], year)

    return (year, election_type, election_date)


def extract_election_date(text, year):
    """Try to extract an election date from text."""
    # "November 8, 2022" or "November 3, 2020" etc.
    date_pattern = r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})'
    m = re.search(date_pattern, text)
    if m:
        try:
            dt = datetime.strptime(f'{m.group(1)} {m.group(2)}, {m.group(3)}', '%B %d, %Y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass

    # Fall back to well-known election dates
    KNOWN_DATES = {
        2024: '2024-11-05', 2022: '2022-11-08', 2020: '2020-11-03',
        2018: '2018-11-06', 2016: '2016-11-08', 2014: '2014-11-04',
        2012: '2012-11-06', 2010: '2010-11-02',
    }
    return KNOWN_DATES.get(year)


def parse_results_table_candidates(table_html):
    """Parse candidates from a results_table votebox."""
    candidates = []

    for row_match in re.finditer(
        r'<tr\s+class="results_row\s*(winner)?\s*"[^>]*>(.*?)</tr>',
        table_html, re.DOTALL
    ):
        is_winner = row_match.group(1) == 'winner'
        row_html = row_match.group(2)

        # Get candidate name
        name_cell = re.search(
            r'class="votebox-results-cell--text"[^>]*>(.*?)</td>',
            row_html, re.DOTALL
        )
        if not name_cell:
            continue

        cell_html = name_cell.group(1)

        # Skip write-in aggregates
        if 'Other/Write-in' in cell_html or '>Write-in<' in cell_html:
            continue

        # Check if incumbent (bold + underline + link)
        is_incumbent = bool(re.search(r'<b><u><a', cell_html))

        # Extract name from link
        name_link = re.search(r'<a\s+href="[^"]*"[^>]*>(.*?)</a>', cell_html)
        if not name_link:
            # Try plain text
            name_text = strip_html(cell_html)
            if not name_text or len(name_text) < 2:
                continue
            name = name_text
        else:
            name = htmlmod.unescape(name_link.group(1).strip())

        # Extract party — look for "(R)", "(D)", etc. or a party cell
        party = None
        party_match = re.search(r'\(([A-Z])\)', cell_html)
        if party_match:
            party = party_match.group(1)
        else:
            # Check for party in a separate cell
            party_cell = re.search(
                r'class="votebox-results-cell"[^>]*>\s*(.*?)\s*</td>',
                row_html, re.DOTALL
            )
            if party_cell:
                party_text = strip_html(party_cell.group(1)).strip()
                if party_text in PARTY_MAP:
                    party = PARTY_MAP[party_text]
                elif len(party_text) == 1 and party_text.isalpha():
                    party = party_text

        # Extract vote percentage
        pct = None
        pct_match = re.search(r'class="percentage_number">([\d.]+)</div>', row_html)
        if pct_match:
            pct = float(pct_match.group(1))

        # Extract votes
        votes = None
        votes_matches = re.findall(
            r'class="votebox-results-cell--number">([\d,]+)</td>',
            row_html
        )
        if votes_matches:
            votes = int(votes_matches[-1].replace(',', ''))

        candidates.append({
            'name': name,
            'party': party,
            'votes': votes,
            'pct': pct,
            'winner': is_winner,
            'incumbent': is_incumbent,
        })

    return candidates


def extract_total_votes(text):
    """Extract total votes from text near a results table."""
    m = re.search(r'Total\s+votes:?\s*([\d,]+)', text, re.IGNORECASE)
    if m:
        return int(m.group(1).replace(',', ''))
    return None




def parse_all_district_elections(state, chamber, districts):
    """Phase C: parse election history from cached HTML for all districts."""
    chamber_slug = chamber.lower().replace(' ', '_')
    cache_dir = os.path.join(DISTRICT_CACHE_DIR, state, chamber_slug)

    results = []
    for dist in districts:
        safe_id = dist['district_identifier'].replace(' ', '_').replace('/', '_')
        cache_path = os.path.join(cache_dir, f'{safe_id}.html')

        html = None
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as f:
                html = f.read()

        elections = parse_district_elections(html, state, chamber, dist['district_identifier'])

        results.append({
            'bp_url': dist['bp_url'],
            'bp_district_name': dist['bp_district_name'],
            'chamber': chamber,
            'district_identifier': dist['district_identifier'],
            'elections': elections,
        })

    return results


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Download BP district election history')
    parser.add_argument('--state', required=True, help='State abbreviation (e.g., AK)')
    parser.add_argument('--chamber', help='Filter to one chamber (e.g., Senate, House)')
    parser.add_argument('--no-cache', action='store_true', help='Force re-download')
    parser.add_argument('--parse-only', action='store_true', help='Skip download, just re-parse cached HTML')
    parser.add_argument('--index-only', action='store_true', help='Only download/parse index pages')
    args = parser.parse_args()

    state = args.state.upper()
    if state not in STATE_NAMES:
        print(f'ERROR: Unknown state {state}')
        sys.exit(1)

    use_cache = not args.no_cache
    os.makedirs(INDEX_CACHE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    chambers = STATE_CHAMBERS[state]
    if args.chamber:
        # Normalize chamber name
        chamber_input = args.chamber.strip()
        matched = None
        for c in chambers:
            if c.lower() == chamber_input.lower():
                matched = c
                break
        if not matched:
            print(f'ERROR: Chamber "{chamber_input}" not found for {state}. Available: {chambers}')
            sys.exit(1)
        chambers = [matched]

    print(f'Processing {state} — chambers: {", ".join(chambers)}')
    print(f'{"═"*60}')

    all_districts = []

    for chamber in chambers:
        print(f'\n── {state} {chamber} ──')

        # Phase A: get district URLs
        print('Phase A: Fetching district URLs from index page...')
        districts = fetch_district_urls(state, chamber, use_cache=use_cache)

        if not districts:
            print(f'  WARNING: No districts found for {state} {chamber}')
            continue

        # Save URL map
        url_map_path = os.path.join(OUTPUT_DIR, f'url_map_{state}_{chamber.replace(" ", "_")}.json')
        with open(url_map_path, 'w') as f:
            json.dump(districts, f, indent=2)
        print(f'  Saved {len(districts)} district URLs to {url_map_path}')

        if args.index_only:
            continue

        # Phase B: download individual pages
        if not args.parse_only:
            print(f'Phase B: Downloading {len(districts)} district pages...')
            download_district_pages(state, chamber, districts, use_cache=use_cache)
            time.sleep(0.5)

        # Phase C: parse election history
        print(f'Phase C: Parsing election history...')
        parsed = parse_all_district_elections(state, chamber, districts)

        # Statistics
        total_elections = sum(len(d['elections']) for d in parsed)
        total_candidates = sum(
            sum(len(e['candidates']) for e in d['elections'])
            for d in parsed
        )
        districts_with_data = sum(1 for d in parsed if d['elections'])

        print(f'  Parsed: {districts_with_data}/{len(parsed)} districts with election data')
        print(f'  Total elections: {total_elections}')
        print(f'  Total candidate records: {total_candidates}')

        # Year breakdown
        year_counts = {}
        for d in parsed:
            for e in d['elections']:
                year_counts[e['year']] = year_counts.get(e['year'], 0) + 1
        if year_counts:
            print(f'  By year:')
            for yr in sorted(year_counts.keys(), reverse=True):
                print(f'    {yr}: {year_counts[yr]} elections')

        all_districts.extend(parsed)

    if args.index_only:
        print(f'\nIndex-only mode — done.')
        return

    # Write output JSON
    output = {
        'state': state,
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'districts': all_districts,
    }

    output_path = os.path.join(OUTPUT_DIR, f'{state}.json')
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2, default=str)

    print(f'\n{"═"*60}')
    print(f'OUTPUT SUMMARY — {state}')
    print(f'{"═"*60}')
    print(f'Districts: {len(all_districts)}')
    total_elections = sum(len(d['elections']) for d in all_districts)
    print(f'Elections: {total_elections}')
    total_cands = sum(sum(len(e['candidates']) for e in d['elections']) for d in all_districts)
    print(f'Candidate records: {total_cands}')
    print(f'Output: {output_path}')


if __name__ == '__main__':
    main()
