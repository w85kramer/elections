"""
Parse Wikipedia "List of governors of {State}" HTML pages to extract:
1. Governor timeline (all 50 states) - term dates, party, end reason
2. Electoral history (10 states) - vote counts, percentages, all candidates

Reads HTML files from elections/tmp/ directory.
Outputs /tmp/governor_history.json

Usage:
    python3 scripts/parse_governor_wiki.py
    python3 scripts/parse_governor_wiki.py --state AK
    python3 scripts/parse_governor_wiki.py --state CA --debug
"""
import os
import re
import sys
import json
import argparse
from bs4 import BeautifulSoup

HTML_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'tmp')
OUTPUT_PATH = '/tmp/governor_history.json'

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

ELECTORAL_HISTORY_STATES = {'AK', 'HI', 'ID', 'MT', 'NV', 'ND', 'OR', 'SD', 'WA', 'WY'}

PARTY_MAP = {
    'democratic': 'D', 'democrat': 'D',
    'republican': 'R',
    'independent': 'I',
    'democratic-farmer-labor': 'D', 'democratic–farmer–labor': 'D',
    'democratic- farmer-labor': 'D',  # with line break artifact
    'independent-republican': 'R', 'independent- republican': 'R',
    'reform/ independence': 'Reform', 'reform/independence': 'Reform',
    'independence': 'Reform',
    'alaskan independence': 'AIP',
    'libertarian': 'L',
    'green': 'G', 'pacific green': 'G',
    'nonpartisan': 'NP',
    'whig': 'Whig', 'know nothing': 'KN',
    'union': 'Union', 'progressive': 'Prog',
    'constitution': 'Con',
    'socialist labor': 'SL', 'socialist workers': 'SW',
    'independent american': 'IA',
    'none of these candidates': 'NOTA',
    'democratic-npl': 'D', 'democratic–npl': 'D',
    'other': 'Other',
    'a connecticut party': 'I',
}

CAUCUS_MAP = {
    'democratic-farmer-labor': 'D', 'democratic–farmer–labor': 'D',
    'democratic- farmer-labor': 'D',
    'independent-republican': 'R', 'independent- republican': 'R',
}

DEBUG = False


def debug(msg):
    if DEBUG:
        print(f'  DEBUG: {msg}')


def find_html_file(state_abbr):
    state_name = STATE_NAMES[state_abbr]
    fname = f'List of governors of {state_name} - Wikipedia.html'
    path = os.path.join(HTML_DIR, fname)
    if os.path.exists(path):
        return path
    fname2 = f'Governor of {state_name} - Wikipedia.html'
    path2 = os.path.join(HTML_DIR, fname2)
    if os.path.exists(path2):
        return path2
    return None


def clean_text(element):
    if element is None:
        return ''
    text = element.get_text(separator=' ')
    text = re.sub(r'\[.*?\]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def parse_date_from_sort_value(span):
    sv = span.get('data-sort-value', '')
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', sv)
    if m:
        return f'{m.group(1)}-{m.group(2)}-{m.group(3)}'
    return None


def parse_date_from_text(text):
    text = text.strip()
    months = {
        'January': 1, 'February': 2, 'March': 3, 'April': 4,
        'May': 5, 'June': 6, 'July': 7, 'August': 8,
        'September': 9, 'October': 10, 'November': 11, 'December': 12
    }
    m = re.match(r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', text)
    if m and m.group(1) in months:
        return f'{int(m.group(3)):04d}-{months[m.group(1)]:02d}-{int(m.group(2)):02d}'
    return None


def normalize_party(party_text):
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
        if k in key:
            return v
    debug(f'Unknown party: "{party_text}" (key="{key}")')
    return party_text[:10]


def get_caucus(party_text):
    if not party_text:
        return None
    key = party_text.lower().strip()
    key = re.sub(r'\s+party\s*$', '', key, flags=re.IGNORECASE)
    key = re.sub(r'\[.*?\]', '', key).strip()
    return CAUCUS_MAP.get(key)


def parse_end_reason(text):
    if not text:
        return None
    text = text.lower()
    if 'recalled' in text:
        return 'removed'
    if 'impeach' in text or 'removed' in text:
        return 'removed'
    if 'resign' in text:
        return 'resigned'
    if 'died' in text:
        return 'died'
    if 'lost election' in text or 'lost nomination' in text:
        return 'lost_election'
    if any(x in text for x in ['term-limited', 'term limited', 'did not run',
                                 'did not seek', 'retired', 'withdrew',
                                 'successor took office']):
        return 'term_expired'
    if 'appointed' in text or 'elected to' in text:
        return 'appointed_elsewhere'
    return None


def extract_governor_name(cell):
    bold = cell.find('b')
    if bold:
        link = bold.find('a')
        if link:
            return link.get_text().strip()
        return bold.get_text().strip()
    link = cell.find('a')
    if link:
        return link.get_text().strip()
    return clean_text(cell)


def find_governor_table(soup, state_name):
    for table in soup.find_all('table', class_='wikitable'):
        caption = table.find('caption')
        if caption:
            cap_text = clean_text(caption).lower()
            if 'governors of the state of' in cap_text and state_name.lower() in cap_text:
                return table
            if f'governors of {state_name.lower()}' in cap_text and 'territory' not in cap_text:
                return table
    for table in soup.find_all('table', class_='wikitable'):
        caption = table.find('caption')
        if caption:
            cap_text = clean_text(caption).lower()
            if 'governor' in cap_text and state_name.lower() in cap_text:
                if 'territory' not in cap_text and 'colonial' not in cap_text:
                    return table
    return None


# ══════════════════════════════════════════════════════════════════
# TIMELINE TABLE PARSER
# ══════════════════════════════════════════════════════════════════

def build_logical_grid(table):
    """Build a 2D grid of cells accounting for rowspan/colspan.
    Returns grid[row_idx][col_idx] = cell element."""
    rows = table.find_all('tr')
    if not rows:
        return []

    # First pass: determine grid dimensions
    max_cols = 0
    for row in rows:
        cols_in_row = sum(int(c.get('colspan', 1)) for c in row.find_all(['td', 'th']))
        max_cols = max(max_cols, cols_in_row)

    # Build grid with None placeholders
    grid = [[None] * (max_cols + 10) for _ in range(len(rows))]

    for row_idx, row in enumerate(rows):
        col_idx = 0
        for cell in row.find_all(['td', 'th']):
            # Find next available column
            while col_idx < len(grid[row_idx]) and grid[row_idx][col_idx] is not None:
                col_idx += 1
            if col_idx >= len(grid[row_idx]):
                break

            rowspan = int(cell.get('rowspan', 1))
            colspan = int(cell.get('colspan', 1))

            for dr in range(rowspan):
                for dc in range(colspan):
                    r = row_idx + dr
                    c = col_idx + dc
                    if r < len(grid) and c < len(grid[r]):
                        grid[r][c] = cell

            col_idx += colspan

    return grid


def identify_columns(grid):
    """Identify which logical column indices correspond to which fields."""
    if not grid or not grid[0]:
        return {}

    header_row = grid[0]
    layout = {}
    seen_texts = []

    for col_idx, cell in enumerate(header_row):
        if cell is None:
            break
        text = clean_text(cell).lower()
        # Skip duplicate cells (same cell spanning multiple columns)
        if col_idx > 0 and grid[0][col_idx] is grid[0][col_idx - 1]:
            continue
        seen_texts.append((col_idx, text))

    # Now map header groups to logical column ranges
    # The pattern is always: No. | Governor(colspan=3-4) | Term | [Duration] | Party | Election | [Lt.Gov(colspan=2-3)]
    for col_idx, text in seen_texts:
        if 'no.' in text or text.startswith('#'):
            layout['number'] = col_idx
        elif 'lt.' in text or 'lieutenant' in text:
            # Must check BEFORE 'governor' since "Lt. Governor" contains "governor"
            layout['lt_gov'] = col_idx
        elif 'governor' in text:
            colspan = int(grid[0][col_idx].get('colspan', 1))
            layout['governor_name'] = col_idx + colspan - 1
            layout['governor_start'] = col_idx
        elif 'term' in text:
            layout['term'] = col_idx
        elif 'duration' in text:
            layout['duration'] = col_idx
        elif 'party' in text:
            layout['party'] = col_idx
        elif 'election' in text:
            layout['election'] = col_idx

    return layout


def parse_timeline_table(table, state_abbr):
    """Parse the governor timeline table into structured records."""
    grid = build_logical_grid(table)
    if not grid:
        return []

    layout = identify_columns(grid)
    debug(f'Column layout: {layout}')

    if 'governor_name' not in layout or 'term' not in layout:
        print(f'  WARNING: Could not identify required columns. Layout: {layout}')
        return []

    name_col = layout['governor_name']
    term_col = layout['term']
    party_col = layout.get('party')
    election_col = layout.get('election')
    number_col = layout.get('number', 0)

    governors = []
    prev_name_cell = None

    for row_idx in range(1, len(grid)):
        name_cell = grid[row_idx][name_col]
        term_cell = grid[row_idx][term_col]
        party_cell = grid[row_idx][party_col] if party_col is not None else None
        election_cell = grid[row_idx][election_col] if election_col is not None else None
        number_cell = grid[row_idx][number_col]

        if name_cell is None or term_cell is None:
            continue

        # Is this a new governor (name cell different from previous row)?
        is_new = (name_cell is not prev_name_cell)

        if is_new:
            prev_name_cell = name_cell

            gov_name = extract_governor_name(name_cell)
            if not gov_name:
                continue

            # Parse term dates
            start_date = None
            end_date = None
            end_reason = None

            sort_span = term_cell.find('span', attrs={'data-sort-value': True})
            if sort_span:
                start_date = parse_date_from_sort_value(sort_span)

            term_text = clean_text(term_cell)

            # End reason from small text
            for span in term_cell.find_all('span', style=lambda s: s and '85%' in s):
                reason = parse_end_reason(clean_text(span))
                if reason:
                    end_reason = reason

            # End date
            if 'incumbent' not in term_text.lower():
                # Find all date strings in the cell
                all_html = term_cell.decode_contents()
                date_matches = re.findall(
                    r'(?:January|February|March|April|May|June|July|August|September|'
                    r'October|November|December)\s+\d{1,2},?\s+\d{4}',
                    all_html
                )
                if len(date_matches) >= 2:
                    end_date = parse_date_from_text(date_matches[-1])
                elif len(date_matches) == 1 and start_date:
                    # Only start date found, but governor has ended term
                    # Try parsing end date from text after dash
                    parts = re.split(r'[–—-]', term_text)
                    if len(parts) > 1:
                        ed = parse_date_from_text(parts[-1].strip())
                        if ed:
                            end_date = ed

            # Parse party
            party_text = clean_text(party_cell) if party_cell else ''
            party_text = re.sub(r'\[.*?\]', '', party_text).strip()
            party_code = normalize_party(party_text)
            caucus = get_caucus(party_text)

            # Governor number / acting check
            num_text = clean_text(number_cell).strip() if number_cell else ''
            is_acting = num_text in ('--', '—', '–', '')

            # Election info
            start_reason = 'elected'
            election_years = []

            if election_cell:
                elec_text = clean_text(election_cell)
                if 'succeeded' in elec_text.lower() or 'president of' in elec_text.lower():
                    start_reason = 'succeeded'
                elif is_acting:
                    start_reason = 'succeeded'
                else:
                    # Check for recall in election cell
                    is_recall = 'recall' in elec_text.lower()

                    # Extract years
                    for link in election_cell.find_all('a'):
                        lt = link.get_text().strip()
                        if re.match(r'^\d{4}$', lt):
                            yr = int(lt)
                            # Check if this specific link is a recall
                            href = link.get('href', '')
                            link_parent_text = clean_text(link.parent) if link.parent else ''
                            if 'recall' in href.lower() or 'recall' in link_parent_text.lower():
                                election_years.append({'year': yr, 'is_recall': True})
                            else:
                                election_years.append({'year': yr, 'is_recall': False})
                    if not election_years:
                        yr_matches = re.findall(r'\b(19\d{2}|20\d{2})\b', elec_text)
                        election_years = [{'year': int(y), 'is_recall': False} for y in yr_matches]

            current_gov = {
                'name': gov_name,
                'state': state_abbr,
                'start_date': start_date,
                'end_date': end_date,
                'party': party_code,
                'caucus': caucus,
                'party_raw': party_text,
                'start_reason': start_reason,
                'end_reason': end_reason,
                'is_acting': is_acting,
                'election_years': election_years,
            }
            governors.append(current_gov)
            debug(f'Governor: {gov_name} ({party_code}) {start_date} - {end_date} [{start_reason}] '
                  f'end={end_reason} elections={[e["year"] for e in election_years]}')

        else:
            # Additional election row for same governor
            if election_cell and governors:
                current_gov = governors[-1]
                elec_text = clean_text(election_cell)
                if 'succeeded' not in elec_text.lower() and 'president of' not in elec_text.lower():
                    for link in election_cell.find_all('a'):
                        lt = link.get_text().strip()
                        if re.match(r'^\d{4}$', lt):
                            yr = int(lt)
                            href = link.get('href', '')
                            link_parent_text = clean_text(link.parent) if link.parent else ''
                            is_recall = 'recall' in href.lower() or 'recall' in link_parent_text.lower()
                            if yr not in [e['year'] for e in current_gov['election_years']]:
                                current_gov['election_years'].append({'year': yr, 'is_recall': is_recall})
                                debug(f'  +election {yr} for {current_gov["name"]} (recall={is_recall})')
                    if not election_cell.find_all('a'):
                        yr_matches = re.findall(r'\b(19\d{2}|20\d{2})\b', elec_text)
                        for y in yr_matches:
                            yr = int(y)
                            if yr not in [e['year'] for e in current_gov['election_years']]:
                                current_gov['election_years'].append({'year': yr, 'is_recall': False})

    # Filter to 1959+
    filtered = []
    for g in governors:
        if g['start_date']:
            start_year = int(g['start_date'][:4])
            if start_year >= 1959:
                filtered.append(g)
            elif g['end_date']:
                end_year = int(g['end_date'][:4])
                if end_year >= 1959:
                    filtered.append(g)
            elif g['end_date'] is None:
                filtered.append(g)
    return filtered


# ══════════════════════════════════════════════════════════════════
# ELECTORAL HISTORY TABLE PARSER
# ══════════════════════════════════════════════════════════════════

def find_electoral_history_table(soup):
    """Find the electoral history table."""
    # Look for span with id containing 'Electoral_history'
    for span in soup.find_all('span', id=lambda x: x and 'lectoral_history' in x):
        # Walk up to the heading div, then look for next table sibling
        parent = span.parent
        while parent and parent.name not in ['div', 'h2', 'h3']:
            parent = parent.parent

        # If wrapped in div.mw-heading, get that div
        if parent and parent.name != 'div':
            wrapper = parent.find_parent('div', class_='mw-heading')
            if wrapper:
                parent = wrapper

        if parent:
            sibling = parent.find_next_sibling()
            while sibling:
                if sibling.name == 'table' and sibling.get('class') and 'wikitable' in sibling.get('class', []):
                    return sibling
                if sibling.name == 'div':
                    tbl = sibling.find('table', class_='wikitable')
                    if tbl:
                        return tbl
                if sibling.name in ['h2', 'h3']:
                    break
                if sibling.name == 'div' and 'mw-heading' in (sibling.get('class') or []):
                    break
                sibling = sibling.find_next_sibling()

    # Fallback: look for heading text
    for heading in soup.find_all(['h2', 'h3']):
        if 'electoral history' in clean_text(heading).lower():
            wrapper = heading.find_parent('div', class_='mw-heading')
            start = wrapper if wrapper else heading
            sibling = start.find_next_sibling()
            while sibling:
                if sibling.name == 'table' and 'wikitable' in (sibling.get('class') or []):
                    return sibling
                if sibling.name in ['h2', 'h3']:
                    break
                sibling = sibling.find_next_sibling()

    return None


def parse_electoral_history_table(table, state_abbr):
    """Parse the electoral history table with dynamic party columns."""
    rows = table.find_all('tr')
    if len(rows) < 3:
        return []

    # Parse header row 1 to get party names
    header1_cells = rows[0].find_all(['th', 'td'])
    party_columns = []

    col_idx = 0
    for cell in header1_cells:
        text = clean_text(cell)
        colspan = int(cell.get('colspan', 1))
        rowspan = int(cell.get('rowspan', 1))

        # Skip the Year column
        if col_idx == 0 or 'year' in text.lower():
            col_idx += colspan
            continue

        party_name = text.replace(' nominee', '').replace(' candidate', '').strip()
        if party_name and party_name not in ['-', '–', '—']:
            party_columns.append({
                'name': party_name,
                'num_subcols': colspan,  # typically 4 (color, name, votes, pct)
            })
        col_idx += colspan

    debug(f'Electoral history parties ({len(party_columns)}): {[p["name"] for p in party_columns]}')

    # Parse data rows (skip 2 header rows)
    elections = []
    for row in rows[2:]:
        cells = row.find_all(['td', 'th'])
        if not cells:
            continue

        year_text = clean_text(cells[0])
        year_match = re.match(r'(\d{4})', year_text)
        if not year_match:
            continue
        year = int(year_match.group(1))
        if year < 1959:
            continue

        # Parse candidates - each party group has N subcols (typically 4: color, name, votes, pct)
        candidates = []
        cell_idx = 1

        for party_info in party_columns:
            party_name = party_info['name']
            expected_subcols = party_info['num_subcols']

            if cell_idx >= len(cells):
                break

            # Check for dash placeholder (colspan spanning all subcols)
            first_cell = cells[cell_idx]
            first_cs = int(first_cell.get('colspan', 1))
            first_text = clean_text(first_cell).strip()

            if first_cs >= 3 and first_text in ['-', '–', '—', '']:
                cell_idx += 1
                continue

            # Parse subcols: typically color(1), name(1-2), votes(1), pct(1)
            # Color cell (narrow, usually bgcolor or background style)
            if cell_idx >= len(cells):
                break
            color_cell = cells[cell_idx]
            cell_idx += 1

            # Name cell
            if cell_idx >= len(cells):
                break
            name_cell = cells[cell_idx]
            name_cs = int(name_cell.get('colspan', 1))
            is_winner = name_cell.find('b') is not None
            # Use full cell text (not just link text) to avoid getting party names
            # e.g., "J.R. Myers(Constitution)" → "J.R. Myers"
            cand_name = clean_text(name_cell)
            cand_name = re.sub(r'\s*\(.*?\)\s*$', '', cand_name).strip()
            # If still looks like a party name, try using the bold/link text
            if not cand_name or len(cand_name) < 2:
                name_link = name_cell.find('a')
                if name_link:
                    cand_name = name_link.get_text().strip()
            cell_idx += 1

            # Votes cell
            votes = None
            if cell_idx < len(cells):
                votes_cell = cells[cell_idx]
                if votes_cell.find('b'):
                    is_winner = True
                votes_text = clean_text(votes_cell).replace(',', '').strip()
                v_match = re.match(r'(\d+)', votes_text)
                if v_match:
                    votes = int(v_match.group(1))
                cell_idx += 1

            # Percentage cell
            pct = None
            if cell_idx < len(cells):
                pct_cell = cells[cell_idx]
                if pct_cell.find('b'):
                    is_winner = True
                pct_text = clean_text(pct_cell).replace('%', '').strip()
                p_match = re.match(r'([\d.]+)', pct_text)
                if p_match:
                    pct = float(p_match.group(1))
                cell_idx += 1

            if cand_name and cand_name not in ['-', '–', '—', '']:
                party_code = normalize_party(party_name)
                candidates.append({
                    'name': cand_name,
                    'party': party_code,
                    'party_raw': party_name,
                    'votes': votes,
                    'pct': pct,
                    'is_winner': is_winner,
                })

        if candidates:
            total_votes = sum(c['votes'] for c in candidates if c['votes'])
            winners = [c['name'] for c in candidates if c['is_winner']]
            elections.append({
                'year': year,
                'state': state_abbr,
                'candidates': candidates,
                'total_votes': total_votes if total_votes > 0 else None,
            })
            debug(f'  Election {year}: {len(candidates)} cands, winner={winners}')

    return elections


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

def parse_state(state_abbr):
    html_path = find_html_file(state_abbr)
    if not html_path:
        print(f'WARNING: No HTML file found for {state_abbr}')
        return None

    print(f'Parsing {state_abbr} ({STATE_NAMES[state_abbr]})...')

    with open(html_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')

    state_name = STATE_NAMES[state_abbr]

    gov_table = find_governor_table(soup, state_name)
    if not gov_table:
        print(f'  WARNING: Could not find governor table for {state_abbr}')
        return None

    governors = parse_timeline_table(gov_table, state_abbr)
    print(f'  Found {len(governors)} governors since 1959')

    electoral = []
    if state_abbr in ELECTORAL_HISTORY_STATES:
        elec_table = find_electoral_history_table(soup)
        if elec_table:
            electoral = parse_electoral_history_table(elec_table, state_abbr)
            print(f'  Found {len(electoral)} elections with vote data')
        else:
            print(f'  WARNING: Expected electoral history table but not found')

    # Cross-reference electoral data with governor elections
    elec_by_year = {e['year']: e for e in electoral}
    for gov in governors:
        gov['elections_detail'] = []
        for ey in gov.get('election_years', []):
            yr = ey['year']
            if yr in elec_by_year:
                detail = dict(elec_by_year[yr])
                detail['is_recall'] = ey.get('is_recall', False)
                gov['elections_detail'].append(detail)
            else:
                gov['elections_detail'].append({
                    'year': yr,
                    'state': state_abbr,
                    'candidates': [],
                    'total_votes': None,
                    'is_recall': ey.get('is_recall', False),
                })

    return {
        'state': state_abbr,
        'state_name': state_name,
        'governors': governors,
        'electoral_history': electoral,
    }


def main():
    global DEBUG
    parser = argparse.ArgumentParser(description='Parse Wikipedia governor HTML pages')
    parser.add_argument('--state', type=str, help='Parse single state (e.g., AK)')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    args = parser.parse_args()
    DEBUG = args.debug

    if args.state:
        states = [args.state.upper()]
        if states[0] not in STATE_NAMES:
            print(f'Unknown state: {states[0]}')
            sys.exit(1)
    else:
        states = sorted(STATE_NAMES.keys())

    results = {}
    total_governors = 0
    total_elections = 0

    for state in states:
        data = parse_state(state)
        if data:
            results[state] = data
            total_governors += len(data['governors'])
            total_elections += len(data['electoral_history'])

    print(f'\n=== Summary ===')
    print(f'States parsed: {len(results)}')
    print(f'Total governors (1959+): {total_governors}')
    print(f'Total elections with vote data: {total_elections}')

    for state in sorted(results.keys()):
        data = results[state]
        gov_count = len(data['governors'])
        elec_count = len(data['electoral_history'])
        acting = sum(1 for g in data['governors'] if g.get('is_acting'))
        succeeded = sum(1 for g in data['governors'] if g.get('start_reason') == 'succeeded')
        elected = sum(1 for g in data['governors'] if g.get('start_reason') == 'elected')
        parts = [f'{elected} elected']
        if elec_count:
            parts.append(f'{elec_count} elections with votes')
        if acting:
            parts.append(f'{acting} acting')
        if succeeded:
            parts.append(f'{succeeded} succeeded')
        print(f'  {state}: {gov_count} governors ({", ".join(parts)})')

    with open(OUTPUT_PATH, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f'\nOutput saved to {OUTPUT_PATH}')


if __name__ == '__main__':
    main()
