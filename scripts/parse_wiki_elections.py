"""
Parse Wikipedia state legislative election HTML pages and populate missing
election + candidacy records in the Supabase database.

Handles multiple Wikipedia HTML formats:
  Format A (Idaho-style): Per-district detail tables with explicit Seat A/B labels
    - Used by: ID, WA, VT House, VT Senate, WV Senate, SD House, ND House
  Format C (Oklahoma-style): Summary table with inline candidate lists
    - Used by: OK House, OK Senate, HI House

Usage:
    python3 scripts/parse_wiki_elections.py --file "2024 Idaho House of Representatives election - Wikipedia.html" --dry-run
    python3 scripts/parse_wiki_elections.py --file "2024 Idaho House of Representatives election - Wikipedia.html"
    python3 scripts/parse_wiki_elections.py --tier 1 --dry-run
    python3 scripts/parse_wiki_elections.py --tier 2 --dry-run
"""
import os
import re
import sys
import json
import time
import argparse
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 required. Install with: pip install beautifulsoup4")
    sys.exit(1)

import httpx

# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════

HTML_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'tmp')
BATCH_SIZE = 250
MAX_RETRIES = 5

DEBUG = False

PARTY_MAP = {
    'republican': 'R', 'democratic': 'D', 'democrat': 'D',
    'libertarian': 'L', 'independent': 'I', 'green': 'G',
    'constitution': 'Con', 'nonpartisan': 'NP', 'no party': 'NP',
    'progressive': 'Prog',
    # State-specific party names
    'idaho democratic': 'D', 'north dakota republican': 'R',
    'north dakota democratic-npl': 'D', 'democratic-npl': 'D',
    'democratic–npl': 'D',
    'vermont progressive': 'Prog',
    'vermont republican': 'R', 'vermont democratic': 'D',
    'write-in': 'W', 'write in': 'W',
}

PARTY_COLOR_MAP = {
    '#E81B23': 'R', '#e81b23': 'R', '#FF0000': 'R', '#ff0000': 'R',
    '#3333FF': 'D', '#3333ff': 'D', '#0000FF': 'D', '#0000ff': 'D',
    '#008000': 'G', '#fed000': 'L', '#FED000': 'L',
    '#DCDCDC': 'I', '#dcdcdc': 'I', '#C0C0C0': 'I',
    '#A356DE': 'L',  # sometimes used for Libertarian
    '#FF6600': 'I',  # Reform/other
}

# File → (state_abbr, chamber, year) mapping
FILE_MAP = {
    # Tier 1: Multi-member districts
    '2024 Idaho House of Representatives election - Wikipedia.html':
        ('ID', 'House', 2024),
    '2024 Washington House of Representatives election - Wikipedia.html':
        ('WA', 'House', 2024),
    '2024 Vermont House of Representatives election - Wikipedia.html':
        ('VT', 'House', 2024),
    '2024 Vermont Senate election - Wikipedia.html':
        ('VT', 'Senate', 2024),
    '2024 West Virginia Senate election - Wikipedia.html':
        ('WV', 'Senate', 2024),
    '2024 South Dakota House of Representatives election - Wikipedia.html':
        ('SD', 'House', 2024),
    '2024 North Dakota House of Representatives election - Wikipedia.html':
        ('ND', 'House', 2024),
    '2024 Arizona House of Representatives election - Wikipedia.html':
        ('AZ', 'House', 2024),
    '2024 New Hampshire House of Representatives election - Wikipedia.html':
        ('NH', 'House', 2024),

    # Tier 2: Uncontested races
    '2024 Oklahoma House of Representatives election - Wikipedia.html':
        ('OK', 'House', 2024),
    '2024 Oklahoma Senate election - Wikipedia.html':
        ('OK', 'Senate', 2024),
    '2024 Hawaii House of Representatives election - Wikipedia.html':
        ('HI', 'House', 2024),

    # Tier 3: Off-cycle NJ
    '2019 New Jersey General Assembly election - Wikipedia.html':
        ('NJ', 'Assembly', 2019),
    '2021 New Jersey General Assembly election - Wikipedia.html':
        ('NJ', 'Assembly', 2021),
    '2023 New Jersey General Assembly election - Wikipedia.html':
        ('NJ', 'Assembly', 2023),
}

TIER_FILES = {
    1: [f for f, (s, c, y) in FILE_MAP.items()
        if s in ('ID', 'WA', 'VT', 'WV', 'SD', 'ND', 'AZ', 'NH')],
    2: [f for f, (s, c, y) in FILE_MAP.items()
        if s in ('OK', 'HI')],
    3: [f for f, (s, c, y) in FILE_MAP.items()
        if s == 'NJ'],
}

# Map (state, chamber) → office_type in our DB
CHAMBER_TO_OFFICE = {
    ('ID', 'House'): 'State House',
    ('WA', 'House'): 'State House',
    ('VT', 'House'): 'State House',
    ('VT', 'Senate'): 'State Senate',
    ('WV', 'Senate'): 'State Senate',
    ('SD', 'House'): 'State House',
    ('ND', 'House'): 'State House',
    ('AZ', 'House'): 'State House',
    ('NH', 'House'): 'State House',
    ('OK', 'House'): 'State House',
    ('OK', 'Senate'): 'State Senate',
    ('HI', 'House'): 'State House',
    ('NJ', 'Assembly'): 'State House',
}

# Map (state, chamber) → districts.chamber value in DB
CHAMBER_DB_MAP = {
    ('ID', 'House'): 'House',
    ('WA', 'House'): 'House',
    ('VT', 'House'): 'House',
    ('VT', 'Senate'): 'Senate',
    ('WV', 'Senate'): 'Senate',
    ('SD', 'House'): 'House',
    ('ND', 'House'): 'House',
    ('AZ', 'House'): 'House',
    ('NH', 'House'): 'House',
    ('OK', 'House'): 'House',
    ('OK', 'Senate'): 'Senate',
    ('HI', 'House'): 'House',
    ('NJ', 'Assembly'): 'Assembly',
}


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def debug(msg):
    if DEBUG:
        print(f'  DEBUG: {msg}')


def clean_text(element):
    """Extract clean text from a BeautifulSoup element."""
    if element is None:
        return ''
    text = element.get_text(separator=' ')
    text = re.sub(r'\[.*?\]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def normalize_party(party_text):
    """Normalize party text to a standard code."""
    if not party_text:
        return None
    party_text = re.sub(r'\[.*?\]', '', party_text)
    party_text = party_text.replace('\n', ' ').replace('\xa0', ' ')
    party_text = re.sub(r'\s+', ' ', party_text).strip()
    party_text = re.sub(r'\s+Party\s*$', '', party_text, flags=re.IGNORECASE)
    key = party_text.lower().strip()
    if key in PARTY_MAP:
        return PARTY_MAP[key]
    for k, v in PARTY_MAP.items():
        if k and k in key:
            return v
    if key:
        debug(f'Unknown party: "{party_text}"')
    return party_text[:10] if party_text else None


def party_from_color(style_str):
    """Extract party from a background-color style."""
    if not style_str:
        return None
    m = re.search(r'background-color:\s*([#\w]+)', style_str)
    if m:
        color = m.group(1)
        return PARTY_COLOR_MAP.get(color)
    return None


def esc(s):
    """Escape a string for SQL."""
    if s is None:
        return ''
    return str(s).replace("'", "''")


def run_sql(query, exit_on_error=False):
    """Execute SQL via Supabase Management API with retry."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = httpx.post(
                API_URL,
                headers={'Authorization': f'Bearer {TOKEN}',
                         'Content-Type': 'application/json'},
                json={'query': query},
                timeout=120
            )
            if resp.status_code == 201:
                return resp.json()
            elif resp.status_code == 429:
                wait = 5 * attempt
                print(f'  Rate limited (429), waiting {wait}s (attempt {attempt}/{MAX_RETRIES})...')
                time.sleep(wait)
                continue
            else:
                print(f'  SQL ERROR ({resp.status_code}): {resp.text[:500]}')
                if exit_on_error:
                    sys.exit(1)
                if attempt < MAX_RETRIES:
                    time.sleep(5 * attempt)
                    continue
                return None
        except Exception as e:
            print(f'  HTTP error: {e}')
            if attempt < MAX_RETRIES:
                time.sleep(5 * attempt)
                continue
            if exit_on_error:
                sys.exit(1)
            return None
    return None


# ═══════════════════════════════════════════════════════════════
# FORMAT DETECTION
# ═══════════════════════════════════════════════════════════════

def _count_district_headings(soup):
    """Count headings that look like district sections."""
    count = 0
    for div in soup.find_all('div', class_='mw-heading'):
        h = div.find(['h2', 'h3'])
        if h and re.search(r'District\s+\d', clean_text(h)):
            count += 1
    if count == 0:
        # Bare headings
        for h in soup.find_all(['h2', 'h3']):
            if re.search(r'District\s+\d', clean_text(h)):
                count += 1
    return count


def detect_format(soup, state_abbr):
    """Detect which Wikipedia format this page uses."""
    district_heading_count = _count_district_headings(soup)

    # Count detail tables (plainrowheaders or wikitable with vcard rows)
    plainrow_tables = soup.find_all('table', class_='wikitable plainrowheaders')
    vcard_tables = [t for t in soup.find_all('table', class_='wikitable')
                    if t.find('tr', class_='vcard')]

    # Format A: per-district detail tables (with or without plainrowheaders class)
    if district_heading_count >= 3 and (len(plainrow_tables) >= 3 or len(vcard_tables) >= 3):
        return 'A'

    # Format A variant: county-name districts under "Detailed results" section
    # Detected by many plainrowheaders/vcard tables + a "Detailed results" heading
    if (len(plainrow_tables) >= 10 or len(vcard_tables) >= 10):
        for div in soup.find_all('div', class_='mw-heading'):
            h = div.find('h2')
            if h and 'detailed results' in clean_text(h).lower():
                return 'A'
        # VT Senate: county names are h2 headings with general/primary h3 subsections
        if state_abbr in ('VT',):
            return 'A'

    # Format C: summary table with inline candidate lists (Oklahoma-style)
    for table in soup.find_all('table', class_='wikitable'):
        if 'sortable' in (table.get('class') or []):
            headers = [clean_text(th).lower() for th in table.find_all('th')]
            if any('candidate' in h for h in headers):
                if '▌' in table.get_text():
                    return 'C'

    return None


# ═══════════════════════════════════════════════════════════════
# FORMAT A PARSER (Idaho-style detail tables)
# ═══════════════════════════════════════════════════════════════

def parse_format_a(soup, state_abbr, chamber, year):
    """
    Parse Format A pages: per-district sections with detail tables.

    Returns list of district dicts:
    [
        {
            'district_number': '1',
            'elections': [
                {
                    'election_type': 'General',
                    'seat_designator': 'A',  # or None
                    'candidates': [
                        {'name': ..., 'party': ..., 'votes': ..., 'pct': ...,
                         'winner': True/False, 'incumbent': True/False}
                    ]
                }
            ]
        }
    ]
    """
    results = []

    # Find all district sections by looking for headings
    district_sections = _find_district_sections(soup, state_abbr)

    for dist_num, section_elements in district_sections.items():
        district_data = {
            'district_number': dist_num,
            'elections': [],
        }

        # Within this section, find seat labels and tables
        current_seat = None
        in_general_section = False  # Track h4 "General election" sub-sections
        for elem in section_elements:
            # Check for seat designator: <dl><dt>Seat A</dt></dl>
            if elem.name == 'dl':
                dt = elem.find('dt')
                if dt:
                    dt_text = clean_text(dt)
                    m = re.match(r'Seat\s+([A-Z])', dt_text)
                    if m:
                        current_seat = m.group(1)
                        debug(f'District {dist_num}: found Seat {current_seat}')
                continue

            # Check for sub-section headings (h3/h4)
            if elem.name == 'div' and 'mw-heading' in (elem.get('class') or []):
                h = elem.find(['h3', 'h4'])
                if h:
                    heading_text = clean_text(h).lower()
                    # Position N headings (WA style: "Position 1" → Seat A)
                    pos_m = re.match(r'position\s+(\d+)', heading_text)
                    if pos_m:
                        pos_num = int(pos_m.group(1))
                        current_seat = chr(ord('A') + pos_num - 1)
                        debug(f'District {dist_num}: found Position {pos_num} → Seat {current_seat}')
                    # Track general/primary sub-sections (WV style)
                    if 'general election' in heading_text:
                        in_general_section = True
                    elif 'primary' in heading_text:
                        in_general_section = False
                continue

            # Process tables
            if elem.name == 'table' and 'wikitable' in (elem.get('class') or []):
                # WA-style: combined table with "Primary election" + "General election" sections
                if _is_combined_table(elem):
                    election = _parse_combined_table(elem, state_abbr, current_seat)
                else:
                    election = _parse_detail_table(elem, state_abbr, current_seat,
                                                   force_general=in_general_section)
                if election and election['candidates']:
                    district_data['elections'].append(election)

            # Also check for collapsible wrappers (NH style)
            if elem.name == 'div':
                for table in elem.find_all('table', class_='wikitable'):
                    if table.find('tr', class_='vcard'):
                        if _is_combined_table(table):
                            election = _parse_combined_table(table, state_abbr, current_seat)
                        else:
                            election = _parse_detail_table(table, state_abbr, current_seat,
                                                           force_general=in_general_section)
                        if election and election['candidates']:
                            district_data['elections'].append(election)

        if district_data['elections']:
            results.append(district_data)

    return results


def _find_district_sections(soup, state_abbr):
    """Find all district headings and collect elements belonging to each district section."""
    districts = {}

    # Find all district heading divs
    heading_divs = []
    for div in soup.find_all('div', class_='mw-heading'):
        h = div.find(['h2', 'h3'])
        if h:
            h_text = clean_text(h)
            m = re.search(r'District\s+(\d+[A-Za-z]?)', h_text)
            if m:
                heading_divs.append((div, m.group(1)))

    if not heading_divs:
        # Try bare headings (older wiki format)
        for h in soup.find_all(['h2', 'h3']):
            h_text = clean_text(h)
            m = re.search(r'District\s+(\d+[A-Za-z]?)', h_text)
            if m:
                heading_divs.append((h, m.group(1)))

    # County-based districts (VT, NH): use table captions or heading text
    if not heading_divs and state_abbr in ('VT', 'NH'):
        heading_divs = _find_county_district_sections(soup, state_abbr)

    debug(f'Found {len(heading_divs)} district headings')

    # For each heading, collect sibling elements until the next district heading
    heading_elements = set(id(div) for div, _ in heading_divs)

    for idx, (heading_div, dist_num) in enumerate(heading_divs):
        section_elems = []
        sibling = heading_div.find_next_sibling()

        while sibling:
            # Stop at the next district heading
            if id(sibling) in heading_elements:
                break

            # Stop at a major non-district section heading (h2)
            if sibling.name == 'div' and 'mw-heading' in (sibling.get('class') or []):
                h = sibling.find('h2')
                if h and not re.search(r'District\s+\d', clean_text(h)):
                    # Also don't break if this is a VT-style county heading
                    if id(sibling) not in heading_elements:
                        break

            if hasattr(sibling, 'name') and sibling.name is not None:
                section_elems.append(sibling)

            sibling = sibling.find_next_sibling()

        districts[dist_num] = section_elems

    return districts


def _find_county_district_sections(soup, state_abbr):
    """Find county-based district sections for VT and NH.

    VT House: h3 headings like "Addison-1", "Bennington-2" under "Detailed results"
    VT Senate: h2 headings like "Addison", "Bennington" with h3 sub-sections
    NH House: h3 "Belknap County" → h4 "1st District", etc. under "Detailed results"
    """
    heading_divs = []

    # Find "Detailed results" section
    detailed_section = None
    for div in soup.find_all('div', class_='mw-heading'):
        h = div.find('h2')
        if h and 'detailed results' in clean_text(h).lower():
            detailed_section = div
            break

    if detailed_section and state_abbr == 'NH':
        return _find_nh_district_sections(soup, detailed_section)

    if detailed_section:
        # VT House: each h3 sibling after "Detailed results" is a district heading
        sibling = detailed_section.find_next_sibling()
        while sibling:
            if sibling.name == 'div' and 'mw-heading' in (sibling.get('class') or []):
                h = sibling.find('h3')
                if h:
                    dist_name = clean_text(h).strip()
                    dist_name = re.sub(r'\s*\[edit\]\s*$', '', dist_name)
                    if dist_name and len(dist_name) > 1:
                        heading_divs.append((sibling, dist_name))
                # Stop at next h2 that isn't a district
                h2 = sibling.find('h2')
                if h2:
                    break
            sibling = sibling.find_next_sibling()
        return heading_divs

    # VT Senate: h2 headings that match county names after "Results"
    results_section = None
    for div in soup.find_all('div', class_='mw-heading'):
        h = div.find('h2')
        if h and clean_text(h).strip().lower() == 'results':
            results_section = div
            break

    if results_section:
        vt_counties = {
            'addison', 'bennington', 'caledonia', 'chittenden', 'essex',
            'franklin', 'grand isle', 'lamoille', 'orange', 'orleans',
            'rutland', 'washington', 'windham', 'windsor',
            'chittenden-central', 'chittenden-north', 'chittenden-southeast',
            'essex-orleans',
        }
        sibling = results_section.find_next_sibling()
        while sibling:
            if sibling.name == 'div' and 'mw-heading' in (sibling.get('class') or []):
                h = sibling.find('h2')
                if h:
                    dist_name = clean_text(h).strip()
                    dist_name = re.sub(r'\s*\[edit\]\s*$', '', dist_name)
                    if dist_name.lower() in vt_counties:
                        heading_divs.append((sibling, dist_name))
                    elif dist_name.lower() in ('see also', 'references', 'notes', 'external links'):
                        break
            sibling = sibling.find_next_sibling()

    return heading_divs


def _find_nh_district_sections(soup, detailed_section):
    """Find NH district sections: h3 county → h4 numbered district.

    NH uses: h3 "Belknap County" → h4 "1st District", "2nd District" etc.
    DB district_number is "Belknap-1", "Belknap-2", etc.
    """
    heading_divs = []
    current_county = None

    # Ordinal to number map
    ordinal_map = {
        '1st': '1', '2nd': '2', '3rd': '3', '4th': '4', '5th': '5',
        '6th': '6', '7th': '7', '8th': '8', '9th': '9', '10th': '10',
        '11th': '11', '12th': '12', '13th': '13', '14th': '14', '15th': '15',
        '16th': '16', '17th': '17', '18th': '18', '19th': '19', '20th': '20',
        '21st': '21', '22nd': '22', '23rd': '23', '24th': '24', '25th': '25',
    }

    sibling = detailed_section.find_next_sibling()
    while sibling:
        if sibling.name == 'div' and 'mw-heading' in (sibling.get('class') or []):
            h2 = sibling.find('h2')
            if h2:
                # Stop at non-county h2 sections
                break

            h3 = sibling.find('h3')
            if h3:
                h3_text = clean_text(h3).strip()
                h3_text = re.sub(r'\s*\[edit\]\s*$', '', h3_text)
                if 'county' in h3_text.lower():
                    # Extract county name: "Belknap County" → "Belknap"
                    current_county = re.sub(r'\s+County\s*$', '', h3_text, flags=re.IGNORECASE).strip()
                    # Fix typos (Wikipedia has "Rockinghams" instead of "Rockingham")
                    if current_county.endswith('s') and current_county[:-1].lower() in (
                        'rockingham', 'strafford', 'sullivan'
                    ):
                        current_county = current_county  # Keep as-is, will match DB
                    debug(f'NH county: {current_county}')

            h4 = sibling.find('h4')
            if h4 and current_county:
                h4_text = clean_text(h4).strip()
                h4_text = re.sub(r'\s*\[edit\]\s*$', '', h4_text)
                # "1st District" → "1"
                m = re.match(r'(\d+(?:st|nd|rd|th))\s+District', h4_text)
                if m:
                    ordinal = m.group(1)
                    num = ordinal_map.get(ordinal, ordinal.rstrip('stndrdth'))
                    dist_name = f'{current_county}-{num}'
                    heading_divs.append((sibling, dist_name))
                    debug(f'NH district: {dist_name}')

        sibling = sibling.find_next_sibling()

    return heading_divs


def _parse_detail_table(table, state_abbr, seat_designator, force_general=False):
    """Parse a single wikitable plainrowheaders election results table."""
    caption = table.find('caption')
    caption_text = clean_text(caption) if caption else ''

    # Determine election type from caption
    election_type = _election_type_from_caption(caption_text)

    # If we're in a "General election" sub-section, override Unknown type
    if force_general and election_type in ('Unknown', 'General'):
        election_type = 'General'

    # We only want General elections for gap-filling
    if election_type != 'General':
        return None

    candidates = []
    rows = table.find_all('tr', class_='vcard')

    for row in rows:
        cand = _parse_vcard_row(row)
        if cand:
            candidates.append(cand)

    return {
        'election_type': election_type,
        'seat_designator': seat_designator,
        'candidates': candidates,
        'caption': caption_text,
    }


def _election_type_from_caption(caption_text):
    """Determine election type from table caption text."""
    text = caption_text.lower()
    if 'general election' in text or 'general' in text.split(',')[0] if ',' in text else 'general' in text:
        # Check it's actually a general, not just containing the word
        if 'primary' not in text:
            return 'General'
    if 'republican primary' in text or 'democratic primary' in text:
        return 'Primary'
    if 'primary' in text:
        return 'Primary'
    if 'runoff' in text:
        return 'Runoff'
    # SD/ND style: "general election" in caption
    if 'general' in text:
        return 'General'
    return 'Unknown'


def _parse_vcard_row(row):
    """Parse a single vcard row from a detail table."""
    # Party: from <td class="org">
    org_cell = row.find('td', class_='org')
    party = None
    if org_cell:
        party = normalize_party(clean_text(org_cell))
    else:
        # Try to get party from first td's background color
        first_td = row.find('td')
        if first_td:
            party = party_from_color(first_td.get('style', ''))

    # Name: from <th class="fn">
    name_cell = row.find('th', class_='fn')
    if not name_cell:
        name_cell = row.find('th')
    if not name_cell:
        return None

    name_text = clean_text(name_cell)
    # Check if winner (bold)
    is_winner = name_cell.find('b') is not None

    # Check if incumbent
    is_incumbent = 'incumbent' in name_text.lower()

    # Clean name: remove "(incumbent)" and other parentheticals
    name = re.sub(r'\s*\(incumbent\)\s*', '', name_text, flags=re.IGNORECASE).strip()
    name = re.sub(r'\s*\(.*?\)\s*', '', name).strip()
    # Remove trailing whitespace artifacts
    name = name.strip()

    if not name or len(name) < 2:
        return None

    # Votes and percentage
    tds = row.find_all('td')
    votes = None
    pct = None

    # Parse vote cells — typically last two non-org td cells
    numeric_cells = []
    for td in tds:
        if td.get('class') and 'org' in td.get('class', []):
            continue
        td_text = clean_text(td).replace(',', '').replace('%', '').strip()
        if re.match(r'^[\d.]+$', td_text):
            numeric_cells.append(td_text)

    if len(numeric_cells) >= 2:
        try:
            votes = int(float(numeric_cells[-2]))
        except (ValueError, IndexError):
            pass
        try:
            pct = float(numeric_cells[-1])
        except (ValueError, IndexError):
            pass
    elif len(numeric_cells) == 1:
        # Might be just percentage
        try:
            val = float(numeric_cells[0])
            if val <= 100:
                pct = val
            else:
                votes = int(val)
        except ValueError:
            pass

    # Also check swatch width for winner detection
    color_td = row.find('td', style=lambda s: s and 'width:' in (s or '') and 'background' in (s or ''))
    if color_td:
        style = color_td.get('style', '')
        width_m = re.search(r'width:\s*(\d+)px', style)
        if width_m:
            width = int(width_m.group(1))
            if width >= 5:
                is_winner = True

    return {
        'name': name,
        'party': party,
        'votes': votes,
        'pct': pct,
        'winner': is_winner,
        'incumbent': is_incumbent,
    }


def _is_combined_table(table):
    """Check if this table has combined Primary + General sections (WA-style)."""
    for th in table.find_all('th'):
        text = clean_text(th).lower()
        if text == 'general election':
            return True
    return False


def _parse_combined_table(table, state_abbr, seat_designator):
    """Parse a combined primary/general election table (WA-style).

    These tables have <th colspan="5">Primary election</th> and
    <th colspan="5">General election</th> as section dividers.
    """
    # Find the "General election" divider row
    in_general = False
    candidates = []

    for row in table.find_all('tr'):
        # Check for section header
        th = row.find('th', colspan=True)
        if th:
            text = clean_text(th).lower()
            if 'general election' in text:
                in_general = True
                continue
            elif 'primary election' in text:
                in_general = False
                continue

        if not in_general:
            continue

        # Parse vcard rows in general section
        if 'vcard' in (row.get('class') or []):
            cand = _parse_vcard_row(row)
            if cand:
                candidates.append(cand)

    if not candidates:
        return None

    # Get caption for context
    caption = table.find('caption')
    caption_text = clean_text(caption) if caption else ''

    return {
        'election_type': 'General',
        'seat_designator': seat_designator,
        'candidates': candidates,
        'caption': caption_text,
    }


# ═══════════════════════════════════════════════════════════════
# FORMAT A: MULTI-WINNER HANDLING (SD, ND style)
# ═══════════════════════════════════════════════════════════════

def has_seat_labels(districts_data):
    """Check if any election has an explicit seat designator."""
    for d in districts_data:
        for e in d['elections']:
            if e.get('seat_designator'):
                return True
    return False


def assign_seats_to_multiwinner(districts_data, state_abbr, chamber):
    """
    For multi-winner states without seat labels (SD, ND), assign winners
    to seat designators based on vote order.

    SD/ND have multi-member House districts where top-N candidates win.
    The DB has seats with designators A, B, etc.
    We assign winners to seats in vote order (highest votes → Seat A).
    """
    for district in districts_data:
        generals = [e for e in district['elections'] if e['election_type'] == 'General']
        if not generals:
            continue

        # For SD/ND, there's typically one general election table with all candidates
        gen = generals[0]
        winners = sorted(
            [c for c in gen['candidates'] if c['winner']],
            key=lambda c: (c.get('votes') or 0),
            reverse=True
        )

        # If we have multiple winners, split into separate "elections" per seat
        if len(winners) <= 1:
            # Single winner — leave as-is, seat_designator stays None (single-member)
            continue

        designators = [chr(ord('A') + i) for i in range(len(winners))]

        # Replace the single general election with per-seat elections
        new_elections = []
        all_candidates = gen['candidates']
        losers = [c for c in all_candidates if not c['winner']]

        for i, winner in enumerate(winners):
            seat_elec = {
                'election_type': 'General',
                'seat_designator': designators[i],
                'candidates': [winner] + losers,  # Each seat gets winner + all losers
                'caption': gen.get('caption', ''),
            }
            new_elections.append(seat_elec)

        # Replace
        district['elections'] = [e for e in district['elections'] if e['election_type'] != 'General']
        district['elections'].extend(new_elections)


# ═══════════════════════════════════════════════════════════════
# FORMAT C PARSER (Oklahoma-style summary table)
# ═══════════════════════════════════════════════════════════════

def parse_format_c(soup, state_abbr, chamber, year):
    """
    Parse Format C pages: single summary table with all districts.

    Each row has district number, incumbent info, and candidates column
    with color-coded bars and inline percentages.
    """
    results = []

    # Find the summary elections table (after "Summary of elections" heading)
    # It's a wikitable sortable with columns: District, Incumbent..., Candidates
    target_table = _find_summary_table(soup)
    if not target_table:
        print(f'  WARNING: Could not find summary table for {state_abbr} {chamber}')
        return results

    rows = target_table.find_all('tr')
    if len(rows) < 3:
        return results

    # Parse each district row
    for row in rows[2:]:  # Skip 2 header rows
        cells = row.find_all(['td', 'th'])
        if not cells:
            continue

        # First cell: district number
        dist_text = clean_text(cells[0]).strip()
        m = re.match(r'(\d+)', dist_text)
        if not m:
            continue
        dist_num = m.group(1)

        # Last cell: candidates (ul with color bars)
        cand_cell = cells[-1]
        candidates = _parse_candidate_list(cand_cell)

        if not candidates:
            continue

        # Determine status from the Status cell (if it has one)
        status_text = ''
        if len(cells) >= 5:
            status_text = clean_text(cells[4]).lower() if len(cells) > 4 else ''

        # Mark winner: bold name, or sole candidate if "without opposition"
        _mark_winners(candidates, status_text)

        district_data = {
            'district_number': dist_num,
            'elections': [{
                'election_type': 'General',
                'seat_designator': None,
                'candidates': candidates,
                'caption': '',
            }],
        }
        results.append(district_data)

    return results


def _find_summary_table(soup):
    """Find the main summary elections table in OK/HI-style pages."""
    # Look for "Summary of elections" heading
    for div in soup.find_all('div', class_='mw-heading'):
        h = div.find(['h2'])
        if h and 'summary' in clean_text(h).lower():
            # Find next sibling table with Candidates column (skip party summary)
            sibling = div.find_next_sibling()
            while sibling:
                if sibling.name == 'table' and 'wikitable' in (sibling.get('class') or []):
                    headers = [clean_text(th).lower() for th in sibling.find_all('th')]
                    if any('candidate' in h for h in headers):
                        return sibling
                # Stop at next major section
                if sibling.name == 'div' and 'mw-heading' in (sibling.get('class') or []):
                    h2 = sibling.find('h2')
                    if h2:
                        break
                sibling = sibling.find_next_sibling()

    # Fallback: find any wikitable sortable with a Candidates column
    for table in soup.find_all('table', class_='wikitable'):
        if 'sortable' in (table.get('class') or []):
            headers = [clean_text(th).lower() for th in table.find_all('th')]
            if any('candidate' in h for h in headers):
                return table

    return None


def _parse_candidate_list(cell):
    """Parse a candidate list cell with color bars and percentages."""
    candidates = []

    # Find all <li> items in the cell
    items = cell.find_all('li')
    if not items:
        # Try parsing from raw text
        return candidates

    for li in items:
        li_text = clean_text(li)

        # Skip "Eliminated in Primary" and similar headers
        if re.match(r'^(eliminated|primary|runoff)', li_text.lower()):
            continue

        # Extract party from color span
        color_span = li.find('span', style=True)
        party = None
        if color_span:
            style = color_span.get('style', '')
            m = re.search(r'color:\s*([#\w]+)', style)
            if m:
                party = PARTY_COLOR_MAP.get(m.group(1))

        # Extract name and percentage
        # Pattern: "▌ Name (Party) - XX.X%" or "▌ Name - XX.X%"
        # Remove the color bar character
        text = li_text.replace('▌', '').strip()

        # Check for bold (winner indicator)
        is_winner = li.find('b') is not None

        # Extract percentage
        pct = None
        pct_match = re.search(r'[-–]\s*([\d.]+)%?$', text)
        if pct_match:
            try:
                pct = float(pct_match.group(1))
            except ValueError:
                pass
            text = text[:pct_match.start()].strip()

        # Extract party label from parenthetical
        party_match = re.search(r'\((Republican|Democratic|Democrat|Libertarian|Independent|Green|Constitution)\)', text, re.IGNORECASE)
        if party_match:
            if party is None:
                party = normalize_party(party_match.group(1))
            text = text[:party_match.start()].strip()

        # Clean up remaining text to get name
        name = text.strip().rstrip('-–').strip()
        if not name or len(name) < 2:
            continue

        # Check incumbent
        is_incumbent = False
        link = li.find('a')
        if link:
            # Names with Wikipedia links are often incumbents but not always
            # Check if name matches the "Member" column from same row
            pass

        candidates.append({
            'name': name,
            'party': party,
            'votes': None,
            'pct': pct,
            'winner': is_winner,
            'incumbent': is_incumbent,
        })

    return candidates


def _mark_winners(candidates, status_text=''):
    """Mark winners in a candidate list."""
    # If any are already bold-marked, trust that
    if any(c['winner'] for c in candidates):
        return

    # If "without opposition" or single candidate, mark as winner
    if 'without opposition' in status_text or len(candidates) == 1:
        if candidates:
            candidates[0]['winner'] = True
        return

    # Otherwise mark highest-percentage candidate
    if candidates:
        best = max(candidates, key=lambda c: c.get('pct') or 0)
        if best.get('pct') and best['pct'] > 0:
            best['winner'] = True


# ═══════════════════════════════════════════════════════════════
# DB LOOKUP + POPULATION
# ═══════════════════════════════════════════════════════════════

def load_db_context(state_abbr, chamber_db, year):
    """
    Load seats, elections, and candidates from DB for matching.

    Returns:
        seats: {(district_number, seat_designator) → seat_id}
        existing_elections: {seat_id → {election_type → election_id}}
        candidates_by_name: {normalized_last_name → [(candidate_id, full_name)]}
    """
    office_type_key = None
    for (s, c), ot in CHAMBER_TO_OFFICE.items():
        if s == state_abbr:
            ch = CHAMBER_DB_MAP.get((s, c))
            if ch == chamber_db:
                office_type_key = ot
                break

    if not office_type_key:
        print(f'  ERROR: No office_type mapping for {state_abbr} {chamber_db}')
        return {}, {}, {}

    # Load seats
    seats_data = run_sql(f"""
        SELECT se.id as seat_id, se.seat_designator, se.seat_label,
               d.district_number, d.num_seats
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{esc(state_abbr)}'
          AND se.office_type = '{esc(office_type_key)}'
          AND d.chamber = '{esc(chamber_db)}'
        ORDER BY d.district_number, se.seat_designator
    """)

    if not seats_data:
        print(f'  WARNING: No seats found for {state_abbr} {chamber_db}')
        return {}, {}, {}

    seats = {}
    for s in seats_data:
        key = (str(s['district_number']), s['seat_designator'])
        seats[key] = s['seat_id']
    debug(f'Loaded {len(seats)} seats')

    # Load existing elections for the year
    elections_data = run_sql(f"""
        SELECT e.id as election_id, e.seat_id, e.election_type
        FROM elections e
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{esc(state_abbr)}'
          AND e.election_year = {year}
          AND se.office_type = '{esc(office_type_key)}'
    """)

    existing_elections = defaultdict(dict)
    if elections_data:
        for e in elections_data:
            existing_elections[e['seat_id']][e['election_type']] = e['election_id']
    debug(f'Loaded {len(elections_data or [])} existing elections')

    # Load all candidates (for name matching)
    cands_data = run_sql("""
        SELECT id, full_name, last_name FROM candidates
    """)

    candidates_by_name = defaultdict(list)
    if cands_data:
        for c in cands_data:
            if c['last_name']:
                key = c['last_name'].lower().strip()
                candidates_by_name[key].append((c['id'], c['full_name']))
    debug(f'Loaded {len(cands_data or [])} candidates for matching')

    return seats, existing_elections, candidates_by_name


def find_candidate_id(name, candidates_by_name):
    """Find a candidate ID by name matching."""
    parts = name.split()
    if not parts:
        return None

    last_name = parts[-1].lower().strip()

    # Handle suffixes
    if last_name in ('jr.', 'jr', 'sr.', 'sr', 'ii', 'iii', 'iv'):
        if len(parts) >= 3:
            last_name = parts[-2].lower().strip()

    matches = candidates_by_name.get(last_name, [])
    if not matches:
        return None

    # Score each match
    best_id = None
    best_score = 0

    for cand_id, full_name in matches:
        score = _name_similarity(name, full_name)
        if score > best_score:
            best_score = score
            best_id = cand_id

    if best_score >= 0.7:
        return best_id
    return None


def _name_similarity(name1, name2):
    """Simple name similarity scoring."""
    def normalize(n):
        n = re.sub(r'\s+(Jr\.?|Sr\.?|III|IV|II)\s*$', '', n, flags=re.IGNORECASE)
        return n.strip().lower()

    n1 = normalize(name1)
    n2 = normalize(name2)
    if n1 == n2:
        return 1.0

    parts1 = n1.split()
    parts2 = n2.split()
    if not parts1 or not parts2:
        return 0.0

    last1, last2 = parts1[-1], parts2[-1]
    if last1 != last2:
        # Check if last name appears anywhere
        if last1 not in parts2 and last2 not in parts1:
            return 0.0

    first1, first2 = parts1[0], parts2[0]
    if first1 == first2:
        return 0.9
    if first1.startswith(first2) or first2.startswith(first1):
        return 0.8
    if first1[0] == first2[0] and len(first1) >= 3 and len(first2) >= 3:
        return 0.7

    return 0.3


def populate_elections(parsed_districts, state_abbr, chamber, year, dry_run=False):
    """
    Match parsed data to DB and insert missing elections + candidacies.

    Returns (elections_inserted, candidacies_inserted, candidates_created)
    """
    chamber_db = CHAMBER_DB_MAP.get((state_abbr, chamber))
    if not chamber_db:
        print(f'  ERROR: No chamber mapping for ({state_abbr}, {chamber})')
        return 0, 0, 0

    seats, existing_elections, candidates_by_name = load_db_context(
        state_abbr, chamber_db, year
    )

    if not seats:
        return 0, 0, 0

    # Determine election date for generals
    election_dates = {
        2024: '2024-11-05',
        2023: '2023-11-07',
        2021: '2021-11-02',
        2019: '2019-11-05',
    }
    election_date = election_dates.get(year, f'{year}-11-05')

    elections_to_insert = []  # [(seat_id, election_date, year, type)]
    candidacies_to_insert = []  # [(election_id_or_placeholder, candidate_name, party, votes, pct, winner, incumbent)]

    skipped_existing = 0
    skipped_no_seat = 0
    total_candidates = 0

    for district in parsed_districts:
        dist_num = district['district_number']

        for election in district['elections']:
            if election['election_type'] != 'General':
                continue

            seat_desig = election.get('seat_designator')

            # Find the matching seat
            seat_id = seats.get((dist_num, seat_desig))

            # Try without designator if not found
            if seat_id is None and seat_desig is None:
                # Single-member district — look for seat with NULL designator
                seat_id = seats.get((dist_num, None))

            if seat_id is None:
                # For states where the DB doesn't use designators but wiki does
                # Try just the district number with 'A' designator
                for key, sid in seats.items():
                    if key[0] == dist_num:
                        if seat_desig is None or key[1] == seat_desig:
                            seat_id = sid
                            break

            if seat_id is None:
                debug(f'No seat for district {dist_num} designator {seat_desig}')
                skipped_no_seat += 1
                continue

            # Check if election already exists
            if seat_id in existing_elections and 'General' in existing_elections[seat_id]:
                skipped_existing += 1
                continue

            # This is a gap — insert election
            elections_to_insert.append({
                'seat_id': seat_id,
                'election_date': election_date,
                'year': year,
                'election_type': 'General',
                'candidates': election['candidates'],
                'dist_num': dist_num,
                'seat_desig': seat_desig,
            })
            total_candidates += len(election['candidates'])

    print(f'  Elections to insert: {len(elections_to_insert)}')
    print(f'  Candidates to process: {total_candidates}')
    print(f'  Skipped (already exist): {skipped_existing}')
    print(f'  Skipped (no seat match): {skipped_no_seat}')

    if dry_run:
        # Print sample
        for e in elections_to_insert[:5]:
            print(f'    District {e["dist_num"]} Seat {e["seat_desig"]}: '
                  f'{len(e["candidates"])} candidates')
            for c in e['candidates']:
                w = ' *WINNER*' if c['winner'] else ''
                inc = ' (i)' if c['incumbent'] else ''
                print(f'      {c["name"]} ({c["party"]}) '
                      f'{c.get("votes", "?")} votes {c.get("pct", "?")}%{w}{inc}')
        if len(elections_to_insert) > 5:
            print(f'    ... and {len(elections_to_insert) - 5} more')
        return len(elections_to_insert), total_candidates, 0

    if not elections_to_insert:
        print('  Nothing to insert.')
        return 0, 0, 0

    # ── STEP 1: Insert elections ──
    print(f'\n  Inserting {len(elections_to_insert)} elections...')
    election_values = []
    for e in elections_to_insert:
        election_values.append(
            f"({e['seat_id']}, '{e['election_date']}', {e['year']}, "
            f"'General', NULL)"
        )

    seat_to_election_id = {}
    total_inserted = 0

    for batch_start in range(0, len(election_values), BATCH_SIZE):
        batch = election_values[batch_start:batch_start + BATCH_SIZE]
        sql = (
            "INSERT INTO elections "
            "(seat_id, election_date, election_year, election_type, related_election_id) "
            "VALUES " + ",\n".join(batch) +
            "\nRETURNING id, seat_id;"
        )
        result = run_sql(sql)
        if result:
            for row in result:
                seat_to_election_id[row['seat_id']] = row['id']
            total_inserted += len(result)
            print(f'    Batch: +{len(result)} elections (total: {total_inserted})')
        else:
            print(f'    ERROR: Election batch insert failed!')
            return total_inserted, 0, 0
        time.sleep(1)

    print(f'  Inserted {total_inserted} elections')

    # ── STEP 2: Create candidates + candidacies ──
    print(f'\n  Processing candidates...')
    new_candidates_count = 0
    candidacies_count = 0

    # Collect all candidacy records
    all_candidacies = []

    for e in elections_to_insert:
        election_id = seat_to_election_id.get(e['seat_id'])
        if not election_id:
            continue

        for cand in e['candidates']:
            # Try to find existing candidate
            cand_id = find_candidate_id(cand['name'], candidates_by_name)

            all_candidacies.append({
                'election_id': election_id,
                'candidate_id': cand_id,
                'name': cand['name'],
                'party': cand['party'],
                'votes': cand.get('votes'),
                'pct': cand.get('pct'),
                'winner': cand.get('winner', False),
                'incumbent': cand.get('incumbent', False),
            })

    # Insert new candidates first
    new_cands = [c for c in all_candidacies if c['candidate_id'] is None]
    if new_cands:
        print(f'  Creating {len(new_cands)} new candidates...')
        cand_values = []
        for c in new_cands:
            parts = c['name'].split()
            first = esc(parts[0]) if parts else ''
            last = esc(parts[-1]) if len(parts) > 1 else first
            full = esc(c['name'])
            cand_values.append(f"('{full}', '{first}', '{last}', NULL)")

        new_ids = []
        for batch_start in range(0, len(cand_values), BATCH_SIZE):
            batch = cand_values[batch_start:batch_start + BATCH_SIZE]
            sql = (
                "INSERT INTO candidates (full_name, first_name, last_name, gender) VALUES\n"
                + ",\n".join(batch)
                + "\nRETURNING id;"
            )
            result = run_sql(sql)
            if result:
                new_ids.extend(r['id'] for r in result)
                print(f'    Batch: +{len(result)} candidates')
            else:
                print(f'    ERROR: Candidate batch insert failed!')
                return total_inserted, 0, 0
            time.sleep(1)

        # Assign IDs
        for i, c in enumerate(new_cands):
            if i < len(new_ids):
                c['candidate_id'] = new_ids[i]
                # Add to lookup map
                parts = c['name'].split()
                last = parts[-1].lower().strip() if parts else ''
                candidates_by_name[last].append((new_ids[i], c['name']))

        new_candidates_count = len(new_ids)
        print(f'  Created {new_candidates_count} new candidates')

    # Insert candidacies
    ready = [c for c in all_candidacies if c['candidate_id'] is not None]
    if ready:
        print(f'  Inserting {len(ready)} candidacies...')
        cand_batch = []
        for c in ready:
            votes_sql = str(c['votes']) if c['votes'] is not None else 'NULL'
            pct_sql = str(c['pct']) if c['pct'] is not None else 'NULL'
            result_val = "'Won'" if c['winner'] else "'Lost'"
            party_sql = f"'{esc(c['party'])}'" if c['party'] else 'NULL'
            status = 'Active'
            cand_batch.append(
                f"({c['election_id']}, {c['candidate_id']}, {party_sql}, "
                f"'{status}', {str(c['incumbent']).lower()}, false, "
                f"NULL, NULL, {votes_sql}, {pct_sql}, "
                f"{result_val}, NULL, NULL)"
            )

        for batch_start in range(0, len(cand_batch), BATCH_SIZE):
            batch = cand_batch[batch_start:batch_start + BATCH_SIZE]
            sql = (
                "INSERT INTO candidacies "
                "(election_id, candidate_id, party, candidate_status, "
                "is_incumbent, is_write_in, filing_date, withdrawal_date, "
                "votes_received, vote_percentage, result, endorsements, notes) "
                "VALUES\n" + ",\n".join(batch) +
                "\nRETURNING id;"
            )
            result = run_sql(sql)
            if result:
                candidacies_count += len(result)
                print(f'    Batch: +{len(result)} candidacies (total: {candidacies_count})')
            else:
                print(f'    ERROR: Candidacy batch insert failed!')
            time.sleep(1)

    print(f'  Total: {total_inserted} elections, {candidacies_count} candidacies, '
          f'{new_candidates_count} new candidates')

    return total_inserted, candidacies_count, new_candidates_count


# ═══════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def process_file(filename, dry_run=False):
    """Process a single Wikipedia HTML file."""
    if filename not in FILE_MAP:
        print(f'ERROR: Unknown file "{filename}"')
        print(f'Known files:')
        for f in sorted(FILE_MAP.keys()):
            print(f'  {f}')
        return False

    state_abbr, chamber, year = FILE_MAP[filename]
    filepath = os.path.join(HTML_DIR, filename)

    if not os.path.exists(filepath):
        print(f'ERROR: File not found: {filepath}')
        return False

    print(f'\n{"=" * 60}')
    print(f'Processing: {filename}')
    print(f'  State: {state_abbr}, Chamber: {chamber}, Year: {year}')
    print(f'{"=" * 60}')

    with open(filepath, 'r', encoding='utf-8') as f:
        html = f.read()

    soup = BeautifulSoup(html, 'html.parser')

    # Detect format
    fmt = detect_format(soup, state_abbr)
    print(f'  Detected format: {fmt}')

    if fmt == 'A':
        districts = parse_format_a(soup, state_abbr, chamber, year)

        # Check if this is a multi-winner state without seat labels
        if districts and not has_seat_labels(districts):
            print(f'  Multi-winner without seat labels — assigning seats by vote order')
            assign_seats_to_multiwinner(districts, state_abbr, chamber)

    elif fmt == 'C':
        districts = parse_format_c(soup, state_abbr, chamber, year)
    else:
        print(f'  ERROR: Could not detect page format')
        return False

    if not districts:
        print(f'  WARNING: No districts parsed')
        return False

    # Summary of parsed data
    total_elections = sum(len(d['elections']) for d in districts)
    total_cands = sum(
        len(e['candidates'])
        for d in districts
        for e in d['elections']
    )
    generals = sum(
        1 for d in districts
        for e in d['elections']
        if e['election_type'] == 'General'
    )

    print(f'  Parsed: {len(districts)} districts, {total_elections} elections, '
          f'{total_cands} candidates')
    print(f'  General elections: {generals}')

    if DEBUG:
        for d in districts[:3]:
            print(f'  District {d["district_number"]}:')
            for e in d['elections']:
                print(f'    {e["election_type"]} (Seat {e.get("seat_designator", "?")}): '
                      f'{len(e["candidates"])} candidates')
                for c in e['candidates']:
                    w = '*' if c['winner'] else ' '
                    print(f'      {w} {c["name"]} ({c["party"]}) '
                          f'{c.get("votes", "?")} / {c.get("pct", "?")}%')

    # Populate DB
    e_count, c_count, new_c = populate_elections(
        districts, state_abbr, chamber, year, dry_run=dry_run
    )

    return True


def main():
    global DEBUG

    parser = argparse.ArgumentParser(
        description='Parse Wikipedia election HTML pages and populate DB gaps'
    )
    parser.add_argument('--file', type=str,
                        help='Single HTML filename to process')
    parser.add_argument('--tier', type=int, choices=[1, 2, 3],
                        help='Process all files in a tier')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and match only, no DB inserts')
    parser.add_argument('--debug', action='store_true',
                        help='Show debug output')
    args = parser.parse_args()

    DEBUG = args.debug

    if not args.file and args.tier is None:
        parser.error('Specify --file FILENAME or --tier N')

    if args.dry_run:
        print('DRY RUN MODE — no database changes will be made.\n')

    if args.file:
        process_file(args.file, dry_run=args.dry_run)
    elif args.tier is not None:
        files = TIER_FILES.get(args.tier, [])
        if not files:
            print(f'No files configured for tier {args.tier}')
            sys.exit(1)
        print(f'Processing tier {args.tier}: {len(files)} files\n')
        for filename in files:
            process_file(filename, dry_run=args.dry_run)
            time.sleep(2)

    print('\nDone!')


if __name__ == '__main__':
    main()
