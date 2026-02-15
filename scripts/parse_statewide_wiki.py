"""
Parse Wikipedia statewide officeholder pages to extract historical data.

Reads HTML files from elections/tmp/ directory (downloaded by download_statewide_wiki.py).
Outputs JSON with officeholder records.

Usage:
    python3 scripts/parse_statewide_wiki.py --office ag
    python3 scripts/parse_statewide_wiki.py --office ag --state CA --debug
    python3 scripts/parse_statewide_wiki.py --office ag --cutoff 1960
"""
import os
import re
import sys
import json
import argparse
from bs4 import BeautifulSoup

HTML_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'tmp')

STATE_NAMES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming'
}

ELECTED_STATES = {
    'ag': sorted(set(STATE_NAMES.keys()) - {'AK', 'HI', 'ME', 'NH', 'NJ', 'TN', 'WY'}),
    'lt_gov': sorted(set(STATE_NAMES.keys()) - {'AZ', 'ME', 'NH', 'OR', 'WY', 'TN', 'WV'}),
    'sos': sorted(set(STATE_NAMES.keys()) - {
        'AK', 'HI', 'UT', 'DE', 'FL', 'MD', 'MN', 'NJ', 'NY', 'OK', 'PA', 'TN', 'TX', 'VA', 'WI',
    }),
    'treasurer': sorted(set(STATE_NAMES.keys()) - {
        'NY', 'TX', 'AK', 'GA', 'HI', 'KY', 'MD', 'MI', 'MN', 'NJ', 'TN', 'VA', 'WA', 'WI',
    }),
}

OFFICE_LABELS = {
    'ag': 'Attorney General',
    'lt_gov': 'Lieutenant Governor',
    'sos': 'Secretary of State',
    'treasurer': 'Treasurer',
}

OFFICE_TYPES = {
    'ag': 'Attorney General',
    'lt_gov': 'Lt. Governor',
    'sos': 'Secretary of State',
    'treasurer': 'Treasurer',
}

PARTY_MAP = {
    'democratic': 'D', 'democrat': 'D', 'democratic party': 'D', 'dem': 'D', '(d)': 'D',
    'republican': 'R', 'republican party': 'R', 'rep': 'R', '(r)': 'R',
    'independent': 'I',
    'democratic-farmer-labor': 'D', 'democratic–farmer–labor': 'D',
    'dfl': 'D', 'minnesota democratic–farmer–labor': 'D',
    'democratic-republican': 'DR',
    'federalist': 'Fed',
    'whig': 'Whig',
    'know nothing': 'KN', 'know-nothing': 'KN',
    'national republican': 'NR',
    'anti-jacksonian': 'NR',
    'jacksonian': 'D',
    'union': 'Union', 'unionist': 'Union',
    'progressive': 'Prog',
    'nonpartisan': 'NP', 'non-partisan': 'NP',
    'libertarian': 'L',
    'green': 'G',
    'populist': 'Pop',
    'free soil': 'FS',
    'readjuster': 'Readj',
    'alaskan independence': 'I', 'alaskan independence party': 'I',
    'a connecticut party': 'I',
    'silver republican': 'SR',
    'national union': 'Union',
    'nullifier': 'Null',
    'none': None, '': None,
}

DEBUG = False


def debug(msg):
    if DEBUG:
        print(f'  DEBUG: {msg}')


def clean_text(element):
    """Extract clean text from a BeautifulSoup element, stripping footnotes."""
    if element is None:
        return ''
    text = element.get_text(separator=' ')
    text = re.sub(r'\[.*?\]', '', text)  # Remove footnote brackets
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def normalize_party(party_text):
    """Normalize party text to a standard code."""
    if not party_text:
        return None
    party_text = re.sub(r'\[.*?\]', '', party_text)
    party_text = party_text.replace('\n', ' ').replace('\xa0', ' ')
    party_text = re.sub(r'\s+', ' ', party_text).strip()
    # Remove "Party" suffix
    party_text = re.sub(r'\s+Party\s*$', '', party_text, flags=re.IGNORECASE)
    key = party_text.lower().strip()
    if key in PARTY_MAP:
        return PARTY_MAP[key]
    # Fuzzy match
    for k, v in PARTY_MAP.items():
        if k and k in key:
            return v
    if key:
        debug(f'Unknown party: "{party_text}" (key="{key}")')
    # Return None for clearly non-party text
    if len(key) > 20:
        return None
    return party_text.strip() if party_text else None


def extract_year(text):
    """Extract a 4-digit year from text. Returns int or None."""
    if not text:
        return None
    m = re.search(r'(\d{4})', str(text))
    return int(m.group(1)) if m else None


def extract_full_date(text):
    """Try to extract a full date (YYYY-MM-DD) from text. Returns string or None."""
    if not text:
        return None
    months = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12
    }
    m = re.search(r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', text)
    if m and m.group(1).lower() in months:
        return f'{int(m.group(3)):04d}-{months[m.group(1).lower()]:02d}-{int(m.group(2)):02d}'
    return None


def is_incumbent(text):
    """Check if term text indicates current officeholder."""
    if not text:
        return False
    t = text.lower().strip()
    if any(x in t for x in ['incumbent', 'present', 'current']):
        return True
    # Trailing hyphen/dash after year: "2019-", "2019–"
    if re.search(r'\d{4}\s*[–—-]\s*$', t):
        return True
    return False


def is_acting(text):
    """Check if an officeholder entry is acting/interim."""
    if not text:
        return False
    t = text.lower()
    return any(x in t for x in ['acting', 'interim', 'temporary'])


def clean_name(name):
    """Clean officeholder name — strip acting/interim suffixes, footnotes."""
    if not name:
        return None
    name = re.sub(r'\[.*?\]', '', name)
    # Remove "Acting", "Interim" suffixes (sometimes concatenated without space)
    name = re.sub(r'(?:Acting|Interim|acting|interim)\s*$', '', name).strip()
    # Handle "(Acting)" parenthetical
    name = re.sub(r'\s*\((?:Acting|Interim|acting|interim)\)\s*', '', name).strip()
    return name if name else None


def file_path(office, state_abbr):
    """Get the HTML file path for a given office and state."""
    state_name = STATE_NAMES[state_abbr]
    label = OFFICE_LABELS[office]
    return os.path.join(HTML_DIR, f'{label} of {state_name} - Wikipedia.html')


# ═══════════════════════════════════════════════════════════════
# TABLE FINDING
# ═══════════════════════════════════════════════════════════════

def find_officeholder_table(soup, office):
    """Find the wikitable containing the officeholder list."""
    office_label = OFFICE_LABELS[office].lower()
    tables = soup.find_all('table', class_='wikitable')

    if not tables:
        return None

    # Score each table
    candidates = []
    for table in tables:
        headers = get_header_texts(table)
        header_str = ' '.join(h.lower() for h in headers)

        # Skip election results tables
        if re.search(r'(year|election).*?(democratic|republican)', header_str):
            debug(f'  Skipping election results table: {headers[:4]}')
            continue
        # Skip party summary tables
        if len(headers) <= 2 and 'party' in header_str:
            debug(f'  Skipping party summary table: {headers}')
            continue

        # Must have a name-like column
        has_name = any(re.search(r'(?i)(name|attorney|lieutenant|secretary|treasurer|comptroller|auditor)',
                                 h) for h in headers)
        # Must have a term/date column
        has_term = any(re.search(r'(?i)(term|office|took|start|tenure|served|year)', h) for h in headers)
        # Or a party column
        has_party = any(re.search(r'(?i)part', h) for h in headers)

        if not has_name:
            debug(f'  Skipping table (no name column): {headers[:5]}')
            continue

        row_count = len(table.find_all('tr')) - 1  # Exclude header
        score = row_count
        if has_term:
            score += 100
        if has_party:
            score += 200  # Strongly prefer tables with party data

        candidates.append((score, table, headers))

    if not candidates:
        return None

    # Return highest-scoring table
    candidates.sort(key=lambda x: x[0], reverse=True)
    best = candidates[0]
    debug(f'  Selected table with score={best[0]}, headers={best[2][:6]}')
    return best[1]


def get_header_texts(table):
    """Get header text from the first row of a table."""
    first_row = table.find('tr')
    if not first_row:
        return []
    ths = first_row.find_all(['th'])
    if not ths:
        return []
    return [clean_text(th) for th in ths]


# ═══════════════════════════════════════════════════════════════
# GRID BUILDER (handles rowspan/colspan)
# ═══════════════════════════════════════════════════════════════

def build_grid(table):
    """Build a 2D grid accounting for rowspan/colspan.
    Returns list of rows, each row is list of (cell_element, is_header)."""
    rows = table.find_all('tr')
    if not rows:
        return []

    # Determine grid width
    max_cols = 0
    for row in rows:
        cols = sum(int(c.get('colspan', 1)) for c in row.find_all(['td', 'th']))
        max_cols = max(max_cols, cols)

    grid = [[None] * (max_cols + 5) for _ in range(len(rows))]

    for row_idx, row in enumerate(rows):
        col_idx = 0
        for cell in row.find_all(['td', 'th']):
            while col_idx < len(grid[row_idx]) and grid[row_idx][col_idx] is not None:
                col_idx += 1
            if col_idx >= len(grid[row_idx]):
                break

            # Handle malformed values like "2;" or "2 "
            try:
                rowspan = int(re.sub(r'[^0-9]', '', str(cell.get('rowspan', 1))) or 1)
            except ValueError:
                rowspan = 1
            try:
                colspan = int(re.sub(r'[^0-9]', '', str(cell.get('colspan', 1))) or 1)
            except ValueError:
                colspan = 1
            is_header = cell.name == 'th'

            for dr in range(rowspan):
                for dc in range(colspan):
                    r = row_idx + dr
                    c = col_idx + dc
                    if r < len(grid) and c < len(grid[r]):
                        grid[r][c] = (cell, is_header)

            col_idx += colspan

    return grid


def identify_columns(grid):
    """Identify column roles from the header row."""
    if not grid or not grid[0]:
        return {}

    layout = {}
    seen_cells = set()

    for col_idx, entry in enumerate(grid[0]):
        if entry is None:
            break
        cell, is_header = entry
        if not is_header:
            continue
        # Skip duplicate cells (same cell spanning multiple columns via colspan)
        if id(cell) in seen_cells:
            continue
        seen_cells.add(id(cell))

        text = clean_text(cell).lower()
        text = re.sub(r'\[.*?\]', '', text).strip()  # Remove footnote refs from headers

        colspan = int(cell.get('colspan', 1))

        if re.search(r'^#$|^no\.?$|^number$', text):
            layout['number'] = col_idx
        elif re.search(r'portrait|image|picture|photo', text):
            layout['image'] = col_idx
        elif re.search(r'attorney|lieutenant|secretary|treasurer|comptroller|auditor|name', text):
            # Name column — if colspan > 1, the actual name text is in the last sub-col
            if colspan > 1:
                layout['name'] = col_idx + colspan - 1  # Last sub-column has the text
                layout['name_image'] = col_idx  # First sub-column might be an image
            else:
                layout['name'] = col_idx
        elif re.search(r'^party$|political\s*party', text):
            if colspan > 1:
                # Party has a color cell + text cell
                layout['party_color'] = col_idx
                layout['party'] = col_idx + colspan - 1
            else:
                layout['party'] = col_idx
        elif re.search(r'took\s+office|start|assumed|began|^from$', text):
            layout['start'] = col_idx
        elif re.search(r'left\s+office|end(?:ed)?$|^to$', text):
            layout['end'] = col_idx
        elif re.search(r'term|office|served|tenure|in office', text):
            # Exclude "Years in office" (duration count, not date range)
            if not re.search(r'years?\s+in', text):
                if colspan > 1:
                    # colspan=2 means separate start/end sub-columns (e.g., WA, WV)
                    layout['start'] = col_idx
                    layout['end'] = col_idx + colspan - 1
                else:
                    layout['term'] = col_idx
        elif re.search(r'note|comment|source|county|school|experience|governor', text):
            layout.setdefault('extra', []).append(col_idx)

    return layout


# ═══════════════════════════════════════════════════════════════
# ROW PARSER
# ═══════════════════════════════════════════════════════════════

def parse_officeholder_rows(grid, layout, state_abbr, cutoff_year):
    """Parse data rows from the grid into officeholder records."""
    if 'name' not in layout:
        print(f'  WARNING {state_abbr}: No name column found in layout: {layout}')
        return []

    name_col = layout['name']
    party_col = layout.get('party')
    term_col = layout.get('term')
    start_col = layout.get('start')
    end_col = layout.get('end')

    records = []
    prev_name_cell = None

    for row_idx in range(1, len(grid)):
        row = grid[row_idx]

        # Get cells
        name_entry = row[name_col] if name_col < len(row) else None
        if name_entry is None:
            continue

        name_cell = name_entry[0]

        # Skip vacancy rows (large colspan)
        if name_cell and int(name_cell.get('colspan', 1)) > 2:
            vacancy_text = clean_text(name_cell).lower()
            if 'vacant' in vacancy_text or 'abolished' in vacancy_text:
                debug(f'  Skipping vacancy row: {clean_text(name_cell)[:60]}')
                continue

        # Skip duplicate rows (same cell from rowspan)
        if name_cell is prev_name_cell:
            continue
        prev_name_cell = name_cell

        # Extract name
        raw_name = extract_name_from_cell(name_cell)
        if not raw_name:
            continue

        name = clean_name(raw_name)
        if not name:
            continue

        # Skip garbage rows: footnotes, party names parsed as names, etc.
        name_lower = name.lower().strip()
        if any(name_lower.startswith(x) for x in ['notes:', 'note:', 'source:', 'reference']):
            debug(f'  Skipping garbage row: {name}')
            continue
        # Skip rows where "name" is actually a party name
        if name_lower in PARTY_MAP or name_lower.rstrip('s') in PARTY_MAP:
            debug(f'  Skipping party-as-name row: {name}')
            continue

        acting = is_acting(raw_name) or is_acting(clean_text(name_cell))

        # Extract party
        party = None
        if party_col is not None and party_col < len(row) and row[party_col]:
            party_cell = row[party_col][0]
            party_text = clean_text(party_cell)
            # Skip empty color cells — check if this is just a background-color cell
            if not party_text.strip() and party_cell.get('style', '') and 'background' in party_cell.get('style', ''):
                # Try next column
                if party_col + 1 < len(row) and row[party_col + 1]:
                    party_text = clean_text(row[party_col + 1][0])
            party = normalize_party(party_text)

        # Extract dates
        start_year = None
        end_year = None
        start_date = None
        end_date = None

        if term_col is not None and term_col < len(row) and row[term_col]:
            term_cell = row[term_col][0]
            term_text = clean_text(term_cell)

            # Try to extract full dates first
            dates_in_text = re.findall(
                r'(?:January|February|March|April|May|June|July|August|September|'
                r'October|November|December)\s+\d{1,2},?\s+\d{4}',
                term_text
            )
            if len(dates_in_text) >= 2:
                start_date = extract_full_date(dates_in_text[0])
                end_date = extract_full_date(dates_in_text[-1])
            elif len(dates_in_text) == 1:
                start_date = extract_full_date(dates_in_text[0])

            # Extract years from term range
            years = re.findall(r'\d{4}', term_text)
            if years:
                start_year = int(years[0])
                if len(years) >= 2 and not is_incumbent(term_text):
                    end_year = int(years[-1])

        elif start_col is not None:
            # Separate start/end columns
            if start_col < len(row) and row[start_col]:
                start_text = clean_text(row[start_col][0])
                start_date = extract_full_date(start_text)
                start_year = extract_year(start_text)
            if end_col is not None and end_col < len(row) and row[end_col]:
                end_text = clean_text(row[end_col][0])
                if not is_incumbent(end_text):
                    end_date = extract_full_date(end_text)
                    end_year = extract_year(end_text)

        # Skip entries with no date information at all
        if start_year is None and end_year is None:
            continue

        # Apply cutoff — skip entries entirely before cutoff
        if end_year and end_year < cutoff_year:
            continue
        # If end_year unknown but started long before cutoff, skip
        # (they can't still be serving if they started before 1960)
        if end_year is None and start_year and start_year < cutoff_year:
            # Unless explicitly marked as incumbent in the text
            term_text_check = ''
            if term_col is not None and term_col < len(row) and row[term_col]:
                term_text_check = clean_text(row[term_col][0])
            elif end_col is not None and end_col < len(row) and row[end_col]:
                term_text_check = clean_text(row[end_col][0])
            if not is_incumbent(term_text_check):
                continue

        record = {
            'state': state_abbr,
            'name': name,
            'party': party,
            'start_year': start_year,
            'end_year': end_year,
            'start_date': start_date or (f'{start_year}-01-01' if start_year else None),
            'end_date': end_date,
            'is_acting': acting,
            'is_incumbent': end_year is None and start_year is not None,
        }
        records.append(record)
        debug(f'  {name} ({party}) {start_year}-{end_year or "present"} acting={acting}')

    return records


def extract_name_from_cell(cell):
    """Extract officeholder name from a cell, preferring linked text."""
    if cell is None:
        return None

    # Try bold link first (common pattern)
    bold = cell.find('b')
    if bold:
        link = bold.find('a')
        if link:
            return link.get_text().strip()
        return bold.get_text().strip()

    # Try any link
    link = cell.find('a')
    if link:
        text = link.get_text().strip()
        if text and len(text) > 1:
            return text

    # Fall back to cell text
    text = clean_text(cell)
    if text and len(text) > 1:
        return text

    return None


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def parse_state(office, state_abbr, cutoff_year):
    """Parse a single state's Wikipedia page. Returns list of records."""
    path = file_path(office, state_abbr)
    if not os.path.exists(path):
        print(f'  {state_abbr}: HTML file not found — skipping')
        return []

    with open(path, 'r', encoding='utf-8') as f:
        html = f.read()

    soup = BeautifulSoup(html, 'html.parser')
    table = find_officeholder_table(soup, office)

    if not table:
        print(f'  {state_abbr}: No officeholder table found')
        return []

    grid = build_grid(table)
    layout = identify_columns(grid)
    debug(f'  Layout: {layout}')

    if 'name' not in layout:
        print(f'  {state_abbr}: Could not identify name column. Headers: {get_header_texts(table)}')
        return []

    records = parse_officeholder_rows(grid, layout, state_abbr, cutoff_year)

    # Deduplicate — some tables have repeat entries
    seen = set()
    unique = []
    for r in records:
        key = (r['name'], r['start_year'])
        if key not in seen:
            seen.add(key)
            unique.append(r)

    return unique


def main():
    global DEBUG

    parser = argparse.ArgumentParser(description='Parse Wikipedia statewide officeholder pages')
    parser.add_argument('--office', required=True, choices=OFFICE_LABELS.keys(),
                        help='Office type to parse')
    parser.add_argument('--state', type=str, help='Parse single state (abbreviation)')
    parser.add_argument('--cutoff', type=int, default=1960,
                        help='Only include officeholders serving after this year (default: 1960)')
    parser.add_argument('--debug', action='store_true', help='Show debug output')
    args = parser.parse_args()

    DEBUG = args.debug
    office = args.office
    label = OFFICE_LABELS[office]
    cutoff = args.cutoff

    if args.state:
        states = [args.state.upper()]
    else:
        states = ELECTED_STATES[office]

    print(f'\n{"=" * 60}')
    print(f'Parsing {label} officeholders (cutoff: {cutoff})')
    print(f'States: {len(states)}')
    print(f'{"=" * 60}\n')

    all_records = []
    state_counts = {}

    for abbr in states:
        state_name = STATE_NAMES[abbr]
        records = parse_state(office, abbr, cutoff)

        if records:
            # Separate acting vs regular
            regular = [r for r in records if not r['is_acting']]
            acting = [r for r in records if r['is_acting']]
            incumbents = [r for r in records if r['is_incumbent']]

            state_counts[abbr] = len(regular)
            all_records.extend(records)

            inc_name = incumbents[0]['name'] if incumbents else None
            print(f'  {abbr} ({state_name}): {len(regular)} officeholders'
                  f'{f" + {len(acting)} acting" if acting else ""}'
                  f'{f" (incumbent: {inc_name})" if inc_name else ""}')
        else:
            state_counts[abbr] = 0
            print(f'  {abbr} ({state_name}): 0 records')

    # Save output
    output_path = f'/tmp/statewide_{office}_history.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_records, f, indent=2, ensure_ascii=False)

    # Summary
    total_regular = sum(1 for r in all_records if not r['is_acting'])
    total_acting = sum(1 for r in all_records if r['is_acting'])
    total_incumbent = sum(1 for r in all_records if r['is_incumbent'])

    parties = {}
    for r in all_records:
        if not r['is_acting']:
            p = r['party'] or 'Unknown'
            parties[p] = parties.get(p, 0) + 1

    print(f'\n{"=" * 60}')
    print(f'SUMMARY: {label}')
    print(f'{"=" * 60}')
    print(f'  Total: {total_regular} officeholders + {total_acting} acting')
    print(f'  Incumbents: {total_incumbent}')
    print(f'  States with 0 records: {sum(1 for v in state_counts.values() if v == 0)}')
    print(f'  Party breakdown: {dict(sorted(parties.items(), key=lambda x: -x[1]))}')
    print(f'  Saved to: {output_path}')


if __name__ == '__main__':
    main()
