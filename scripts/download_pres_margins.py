"""
Download 2024 presidential margins by state legislative district from StateNavigate.

StateNavigate publishes a master Google Sheet with 2024 presidential results
disaggregated to every state legislative district in all 50 states:
https://docs.google.com/spreadsheets/d/1GiIiDrVwddCH4Pc0Jsc9u9hxbm3mVRZVgivUeeT2ssQ

This script downloads each state's tab as CSV, parses the district identifiers and
presidential vote columns, computes R+X.X / D+X.X margins, and outputs a JSON file
that can be loaded by populate_pres_margins.py.

Usage:
    python3 scripts/download_pres_margins.py
    python3 scripts/download_pres_margins.py --state VA
    python3 scripts/download_pres_margins.py --output /tmp/pres_margins.json
"""
import sys
import csv
import io
import re
import json
import time
import argparse

import httpx

SPREADSHEET_ID = '1GiIiDrVwddCH4Pc0Jsc9u9hxbm3mVRZVgivUeeT2ssQ'
BASE_URL = f'https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/export?format=csv&gid='

# GID for each state tab (from master spreadsheet)
STATE_GIDS = {
    'AL': 793111162,
    'AK': 458308642,
    'AZ': 657456034,
    'AR': 2113176849,
    'CA': 1189434443,
    'CO': 1183716712,
    'CT': 270510667,
    'DE': 175643055,
    'FL': 487522789,
    'GA': 1641474359,
    'HI': 1147625798,
    'ID': 2045591051,
    'IL': 1834588321,
    'IN': 181654401,
    'IA': 1573725616,
    'KS': 2085518013,
    'KY': 187793333,
    'LA': 2097811836,
    'ME': 2074207262,
    'MD': 740926362,
    'MA': 369250193,
    'MI': 511800067,
    'MN': 2027633406,
    'MS': 919501018,  # 2024 lines
    'MO': 1162598219,
    'MT': 108322499,
    'NE': 1014574813,
    'NV': 1865646801,
    'NH': 181680625,
    'NJ': 124704063,
    'NM': 344518152,
    'NY': 723959999,
    'NC': 882955521,
    'ND': 1630846088,
    'OH': 1131200413,
    'OK': 1386480017,
    'OR': 1927332818,
    'PA': 877772072,
    'RI': 165266933,
    'SC': 675976337,
    'SD': 474196433,
    'TN': 1019324265,
    'TX': 1847454882,
    'UT': 399840990,
    'VT': 1061735069,
    'VA': 0,
    'WA': 880776207,
    'WI': 528270556,
    'WV': 384830667,
    'WY': 338262477,
}

# ═══════════════════════════════════════════════════════════════
# States with joint legislative districts (LD-): both House and Senate
# share the same numbered district. StateNavigate only has one row per LD.
# ═══════════════════════════════════════════════════════════════
JOINT_LD_STATES = {'AZ', 'ID', 'NJ', 'ND', 'WA', 'NE'}

# ═══════════════════════════════════════════════════════════════
# Chamber prefix mapping: what prefix to expect in each state
# Most states: HD- for House, SD- for Senate
# Special: AD- for Assembly (CA, NV, NY, WI), LD- for joint districts
# ═══════════════════════════════════════════════════════════════

# Map DB chamber names to StateNavigate prefixes
CHAMBER_PREFIX_MAP = {
    'House': 'HD',
    'Senate': 'SD',
    'Assembly': 'AD',
    'House of Delegates': 'HD',
    'Legislature': 'LD',  # NE unicameral
}

# ═══════════════════════════════════════════════════════════════
# MA district name → number mapping (alphabetical sort)
# ═══════════════════════════════════════════════════════════════
MA_HOUSE_DISTRICTS = [
    "1st Barnstable", "1st Berkshire", "1st Bristol", "1st Essex", "1st Franklin",
    "1st Hampden", "1st Hampshire", "1st Middlesex", "1st Norfolk", "1st Plymouth",
    "1st Suffolk", "1st Worcester",
    "2nd Barnstable", "2nd Berkshire", "2nd Bristol", "2nd Essex", "2nd Franklin",
    "2nd Hampden", "2nd Hampshire", "2nd Middlesex", "2nd Norfolk", "2nd Plymouth",
    "2nd Suffolk", "2nd Worcester",
    "3rd Barnstable", "3rd Berkshire", "3rd Bristol", "3rd Essex",
    "3rd Hampden", "3rd Hampshire", "3rd Middlesex", "3rd Norfolk", "3rd Plymouth",
    "3rd Suffolk", "3rd Worcester",
    "4th Barnstable", "4th Bristol", "4th Essex", "4th Hampden",
    "4th Middlesex", "4th Norfolk", "4th Plymouth", "4th Suffolk", "4th Worcester",
    "5th Barnstable", "5th Bristol", "5th Essex", "5th Hampden",
    "5th Middlesex", "5th Norfolk", "5th Plymouth", "5th Suffolk", "5th Worcester",
    "6th Bristol", "6th Essex", "6th Hampden", "6th Middlesex",
    "6th Norfolk", "6th Plymouth", "6th Suffolk", "6th Worcester",
    "7th Bristol", "7th Essex", "7th Hampden", "7th Middlesex",
    "7th Norfolk", "7th Plymouth", "7th Suffolk", "7th Worcester",
    "8th Bristol", "8th Essex", "8th Hampden", "8th Middlesex",
    "8th Norfolk", "8th Plymouth", "8th Suffolk", "8th Worcester",
    "9th Bristol", "9th Essex", "9th Hampden", "9th Middlesex",
    "9th Norfolk", "9th Plymouth", "9th Suffolk", "9th Worcester",
    "10th Bristol", "10th Essex", "10th Hampden", "10th Middlesex",
    "10th Norfolk", "10th Plymouth", "10th Suffolk", "10th Worcester",
    "11th Bristol", "11th Essex", "11th Hampden", "11th Middlesex",
    "11th Norfolk", "11th Plymouth", "11th Suffolk", "11th Worcester",
    "12th Bristol", "12th Essex", "12th Hampden", "12th Middlesex",
    "12th Norfolk", "12th Plymouth", "12th Suffolk", "12th Worcester",
    "13th Bristol", "13th Essex", "13th Middlesex", "13th Norfolk",
    "13th Suffolk", "13th Worcester",
    "14th Bristol", "14th Essex", "14th Middlesex", "14th Norfolk",
    "14th Suffolk", "14th Worcester",
    "15th Essex", "15th Middlesex", "15th Norfolk", "15th Suffolk", "15th Worcester",
    "16th Essex", "16th Middlesex", "16th Suffolk", "16th Worcester",
    "17th Essex", "17th Middlesex", "17th Suffolk", "17th Worcester",
    "18th Essex", "18th Middlesex", "18th Suffolk", "18th Worcester",
    "19th Middlesex", "19th Suffolk", "19th Worcester",
    "20th Middlesex", "21st Middlesex", "22nd Middlesex", "23rd Middlesex",
    "24th Middlesex", "25th Middlesex", "26th Middlesex", "27th Middlesex",
    "28th Middlesex", "29th Middlesex", "30th Middlesex", "31st Middlesex",
    "32nd Middlesex", "33rd Middlesex", "34th Middlesex", "35th Middlesex",
    "36th Middlesex", "37th Middlesex",
    "Barnstable, Dukes and Nantucket",
]

MA_SENATE_DISTRICTS = [
    "1st Bristol and Plymouth", "1st Essex", "1st Essex and Middlesex",
    "1st Middlesex", "1st Plymouth and Norfolk", "1st Suffolk", "1st Worcester",
    "2nd Bristol and Plymouth", "2nd Essex", "2nd Essex and Middlesex",
    "2nd Middlesex", "2nd Plymouth and Norfolk", "2nd Suffolk", "2nd Worcester",
    "3rd Bristol and Plymouth", "3rd Essex", "3rd Middlesex", "3rd Suffolk",
    "4th Middlesex", "5th Middlesex",
    "Berkshire, Hampden, Franklin, and Hampshire",
    "Bristol and Norfolk", "Cape and Islands",
    "Hampden", "Hampden and Hampshire", "Hampden, Hampshire, and Worcester",
    "Hampshire, Franklin, and Worcester",
    "Middlesex and Norfolk", "Middlesex and Suffolk", "Middlesex and Worcester",
    "Norfolk and Middlesex", "Norfolk and Plymouth", "Norfolk and Suffolk",
    "Norfolk, Plymouth, and Bristol", "Norfolk, Worcester, and Middlesex",
    "Plymouth and Barnstable",
    "Suffolk and Middlesex",
    "Worcester and Hampden", "Worcester and Hampshire", "Worcester and Middlesex",
]


def build_ma_maps():
    """Build MA name→number maps (alphabetical sort)."""
    house_sorted = sorted(MA_HOUSE_DISTRICTS)
    senate_sorted = sorted(MA_SENATE_DISTRICTS)
    house_map = {name: str(i + 1) for i, name in enumerate(house_sorted)}
    senate_map = {name: str(i + 1) for i, name in enumerate(senate_sorted)}
    return house_map, senate_map


def normalize_ma_sn_name(sn_name):
    """Convert StateNavigate MA name to our Ballotpedia canonical name.

    StateNavigate: "HD-1st Barnstable", "SD-Hampden & Hampshire"
    BP canonical:  "1st Barnstable", "Hampden and Hampshire"
    """
    # Strip chamber prefix
    if sn_name.startswith('HD-'):
        name = sn_name[3:]
    elif sn_name.startswith('SD-'):
        name = sn_name[3:]
    else:
        return sn_name

    # Convert ordinals: "First" → "1st", "Second" → "2nd", etc.
    ordinal_map = {
        'First ': '1st ', 'Second ': '2nd ', 'Third ': '3rd ',
        'Fourth ': '4th ', 'Fifth ': '5th ',
    }
    for word, abbr in ordinal_map.items():
        if name.startswith(word):
            name = abbr + name[len(word):]
            break

    # Convert "&" to "and"
    name = name.replace(' & ', ' and ')

    # Add Oxford comma where BP uses it (multi-county names with "and")
    # BP: "Berkshire, Hampden, Franklin, and Hampshire" (has Oxford comma)
    # SN: "Berkshire, Hampden, Franklin & Hampshire" → "Berkshire, Hampden, Franklin and Hampshire"
    # Need to insert comma before final "and" when there are 3+ items
    # Actually check: BP uses ", and" as Oxford comma
    # This is tricky — let's just handle the known cases
    # For now, this simple replacement should work for most

    # Handle "Barnstable, Dukes and Nantucket" (no Oxford comma in BP)
    # vs multi-county: "Berkshire, Hampden, Franklin, and Hampshire" (Oxford comma)
    # Since we already converted & → and, let's add Oxford comma for 3+ item lists
    parts = name.split(', ')
    if len(parts) >= 3:
        # Check if last part starts with "and" — if not, the "and" is in the last comma-part
        last = parts[-1]
        if not last.startswith('and '):
            # Check second-to-last to see if it contains "and"
            second_last = parts[-2]
            if ' and ' in second_last:
                # e.g. "Berkshire, Hampden, Franklin and Hampshire"
                # → "Berkshire, Hampden, Franklin, and Hampshire"
                sub_parts = second_last.split(' and ')
                parts[-2] = sub_parts[0]
                parts.insert(-1, '')  # placeholder
                parts[-2] = sub_parts[0]
                name = ', '.join(parts[:-2]) + ', ' + sub_parts[0] + ', and ' + sub_parts[1]
                # Actually let me just do a simpler approach
    # Simpler: just try matching and if it fails, try with/without Oxford comma
    return name


# ═══════════════════════════════════════════════════════════════
# VT Senate district name mapping
# StateNavigate may have separate Essex and Orleans; our DB has Essex-Orleans
# ═══════════════════════════════════════════════════════════════
VT_SENATE_SN_TO_DB = {
    'Chittenden Central': 'Chittenden-Central',
    'Chittenden North': 'Chittenden-North',
    'Chittenden South East': 'Chittenden-Southeast',
    'Grand Isle': 'Grand Isle',
    # Essex and Orleans are separate in SN but combined in our DB
}


def parse_int(s):
    """Parse comma-formatted number string to int.
    Some StateNavigate cells have decimal vote counts (estimated/allocated),
    so parse as float first then round to int.
    """
    if not s or s == '#DIV/0!' or s == '#VALUE!' or s == '#REF!':
        return None
    cleaned = str(s).replace(',', '').replace('"', '').strip()
    if not cleaned:
        return None
    try:
        return round(float(cleaned))
    except ValueError:
        return None


def compute_margin(harris, trump, total):
    """Compute margin string from vote counts.
    Returns "D+X.X", "R+X.X", or "EVEN"
    """
    if harris is None or trump is None or total is None or total == 0:
        return None
    margin_pct = (harris - trump) / total * 100
    if abs(margin_pct) < 0.05:
        return "EVEN"
    elif margin_pct > 0:
        return f"D+{margin_pct:.1f}"
    else:
        return f"R+{abs(margin_pct):.1f}"


def download_state_csv(state_abbr):
    """Download a state's tab as CSV text."""
    gid = STATE_GIDS[state_abbr]
    url = f'{BASE_URL}{gid}'
    resp = httpx.get(url, follow_redirects=True, timeout=30)
    if resp.status_code != 200:
        print(f"  ERROR downloading {state_abbr}: HTTP {resp.status_code}")
        return None
    return resp.text


def parse_district_id(district_str, state_abbr):
    """Parse a StateNavigate district string into (chamber, district_number).

    Returns (chamber, district_number) where chamber is one of:
    'House', 'Senate', 'Assembly', 'House of Delegates', 'Legislature'
    and district_number matches our DB format.
    """
    # Some states have year suffixes like "SD-1 (2024)" or "SD-1 (2022)"
    # Strip these, but skip old-line districts (keep only current/2024 lines)
    year_match = re.match(r'^(.+)\s+\((\d{4})\)$', district_str)
    if year_match:
        base = year_match.group(1)
        year = year_match.group(2)
        # Skip old redistricting lines
        if year not in ('2024', '2025'):
            return None, None
        district_str = base

    # Determine prefix and chamber
    # States where lower chamber has special names
    HOD_STATES = {'MD', 'VA', 'WV'}  # House of Delegates
    ASSEMBLY_STATES = {'CA'}  # CA uses HD- in StateNavigate but Assembly in our DB
    if district_str.startswith('HD-'):
        raw = district_str[3:]
        if state_abbr in HOD_STATES:
            chamber = 'House of Delegates'
        elif state_abbr in ASSEMBLY_STATES:
            chamber = 'Assembly'
        else:
            chamber = 'House'
    elif district_str.startswith('SD-'):
        raw = district_str[3:]
        chamber = 'Senate'
    elif district_str.startswith('AD-'):
        raw = district_str[3:]
        chamber = 'Assembly'
    elif district_str.startswith('LD-'):
        raw = district_str[3:]
        if state_abbr == 'NE':
            chamber = 'Legislature'
        else:
            # Joint LD states: return both chambers
            chamber = 'LD'  # special marker
    else:
        return None, None

    # Now convert raw district identifier to our DB's district_number
    district_number = convert_district_number(raw, state_abbr, chamber)

    return chamber, district_number


def convert_district_number(raw, state_abbr, chamber):
    """Convert StateNavigate raw district ID to our DB district_number format."""

    # ── NH House: "Belknap 1" → "Belknap-1", fix "Coös" → "Coos" ──
    if state_abbr == 'NH' and chamber == 'House':
        result = raw.replace(' ', '-')
        result = result.replace('Coös', 'Coos').replace('Co\u00f6s', 'Coos')
        return result

    # ── VT House: "Addison 1" → "Addison-1", "Addison-Rutland" stays ──
    if state_abbr == 'VT' and chamber == 'House':
        m = re.match(r'^(.+)\s+(\d+)$', raw)
        if m:
            return f"{m.group(1)}-{m.group(2)}"
        else:
            return raw

    # ── VT Senate: name-based ──
    if state_abbr == 'VT' and chamber == 'Senate':
        if raw in VT_SENATE_SN_TO_DB:
            return VT_SENATE_SN_TO_DB[raw]
        return raw

    # ── NH Senate: simple numeric "1" → "1" ──
    if state_abbr == 'NH' and chamber == 'Senate':
        return raw

    # ── AK Senate: letters A-T → numbers 1-20 ──
    if state_abbr == 'AK' and chamber == 'Senate':
        if len(raw) == 1 and raw.isalpha():
            return str(ord(raw.upper()) - ord('A') + 1)
        return raw

    # ── MN House: "1A" → "1", "1B" → "2", "67A" → "133", "67B" → "134" ──
    if state_abbr == 'MN' and chamber == 'House':
        m = re.match(r'^(\d+)([AB])$', raw)
        if m:
            num = int(m.group(1))
            letter = m.group(2)
            db_num = (num - 1) * 2 + (1 if letter == 'A' else 2)
            return str(db_num)

    # ── ND/SD sub-districts: "4A"/"4B" → "4" (strip letter suffix) ──
    # These are sub-districts of paired-member districts; our DB uses the parent number
    if state_abbr in ('ND', 'SD') and chamber in ('House', 'LD'):
        m = re.match(r'^(\d+)[AB]$', raw)
        if m:
            return m.group(1)  # Return as sub-district marker for combining later

    # ── MA: named districts → numbered via alphabetical sort ──
    # Handled separately in the main parse function

    # ── MD House of Delegates: "1A" → "1A" (direct match) ──
    if state_abbr == 'MD' and chamber == 'House of Delegates':
        return raw

    # ── Default: direct match ──
    return raw


def parse_state(state_abbr, csv_text):
    """Parse a state's CSV data into a list of district margin records."""
    ma_house_map, ma_senate_map = build_ma_maps()

    reader = csv.DictReader(io.StringIO(csv_text))

    # Find the right column names
    fieldnames = reader.fieldnames
    if not fieldnames:
        print(f"  WARNING: No columns found for {state_abbr}")
        return []

    # Find Harris/Trump/Total columns
    harris_col = None
    trump_col = None
    total_col = None
    margin_col = None
    for col in fieldnames:
        col_lower = col.strip().lower()
        if col_lower == 'harris':
            harris_col = col
        elif col_lower == 'trump':
            trump_col = col
        elif col_lower in ('pres total', 'pres_total', 'presidential total'):
            total_col = col
        elif col_lower == '2024 president':
            margin_col = col

    if not harris_col or not trump_col or not total_col:
        print(f"  WARNING: Missing vote columns for {state_abbr}")
        print(f"  Available columns: {fieldnames}")
        return []

    records = []
    # Only sum lower-chamber districts for state totals (avoids double-counting)
    # Lower chamber prefixes: HD-, AD- for most states; LD- for joint/unicameral states
    lower_prefixes = ('HD-', 'AD-')
    if state_abbr in JOINT_LD_STATES or state_abbr == 'NE':
        lower_prefixes = ('LD-',)
    state_harris_total = 0
    state_trump_total = 0
    state_pres_total = 0

    for row in reader:
        district_str = row.get('District', '').strip()
        if not district_str:
            continue

        harris = parse_int(row.get(harris_col, ''))
        trump = parse_int(row.get(trump_col, ''))
        total = parse_int(row.get(total_col, ''))

        if harris is None or trump is None or total is None:
            continue

        margin = compute_margin(harris, trump, total)
        if margin is None:
            continue

        # Accumulate state totals from lower chamber only (skip old-line districts)
        base_district = district_str
        yr_m = re.match(r'^(.+)\s+\(\d{4}\)$', base_district)
        if yr_m:
            base_district = yr_m.group(1)
        is_lower = any(base_district.startswith(p) for p in lower_prefixes)
        # Skip old-line districts for accumulation (they'll be skipped in parsing too)
        has_old_year = bool(re.match(r'.+\s+\((?!2024|2025)\d{4}\)$', district_str))
        if is_lower and not has_old_year:
            state_harris_total += harris
            state_trump_total += trump
            state_pres_total += total

        # Handle MA specially (named districts → numbers)
        if state_abbr == 'MA':
            if district_str.startswith('HD-'):
                name = normalize_ma_sn_name(district_str)
                db_num = ma_house_map.get(name)
                if not db_num:
                    # Try fuzzy match
                    db_num = find_ma_match(name, ma_house_map)
                if db_num:
                    records.append({
                        'state': state_abbr,
                        'chamber': 'House',
                        'district_number': db_num,
                        'margin': margin,
                        'harris': harris,
                        'trump': trump,
                        'total': total,
                    })
                else:
                    print(f"  WARNING: MA House unmatched: {district_str} → normalized: {name}")
            elif district_str.startswith('SD-'):
                name = normalize_ma_sn_name(district_str)
                db_num = ma_senate_map.get(name)
                if not db_num:
                    db_num = find_ma_match(name, ma_senate_map)
                if db_num:
                    records.append({
                        'state': state_abbr,
                        'chamber': 'Senate',
                        'district_number': db_num,
                        'margin': margin,
                        'harris': harris,
                        'trump': trump,
                        'total': total,
                    })
                else:
                    print(f"  WARNING: MA Senate unmatched: {district_str} → normalized: {name}")
            continue

        # Handle VT Senate Essex/Orleans merge
        if state_abbr == 'VT' and district_str in ('SD-Essex', 'SD-Orleans'):
            # These need to be combined into Essex-Orleans
            # We'll handle this specially below
            records.append({
                'state': state_abbr,
                'chamber': 'Senate',
                'district_number': '_VT_' + district_str[3:],  # temporary marker
                'margin': margin,
                'harris': harris,
                'trump': trump,
                'total': total,
            })
            continue

        # Handle joint LD states
        if district_str.startswith('LD-') and state_abbr in JOINT_LD_STATES:
            raw = district_str[3:]
            # Handle ND/SD sub-districts: "4A"/"4B" → "4"
            sub_m = re.match(r'^(\d+)[AB]$', raw)
            if sub_m:
                raw = sub_m.group(1)

            if state_abbr == 'NE':
                records.append({
                    'state': state_abbr,
                    'chamber': 'Legislature',
                    'district_number': raw,
                    'margin': margin,
                    'harris': harris,
                    'trump': trump,
                    'total': total,
                })
            else:
                # Determine correct lower chamber name for this state
                lower_chamber = 'Assembly' if state_abbr == 'NJ' else 'House'
                records.append({
                    'state': state_abbr,
                    'chamber': lower_chamber,
                    'district_number': raw,
                    'margin': margin,
                    'harris': harris,
                    'trump': trump,
                    'total': total,
                })
                records.append({
                    'state': state_abbr,
                    'chamber': 'Senate',
                    'district_number': raw,
                    'margin': margin,
                    'harris': harris,
                    'trump': trump,
                    'total': total,
                })
            continue

        # Standard parsing
        chamber, district_number = parse_district_id(district_str, state_abbr)
        if chamber is None or district_number is None:
            # Silently skip old-line districts (year suffix that's not current)
            if not re.match(r'.+\s+\(\d{4}\)$', district_str):
                print(f"  WARNING: Unparsed district: {district_str}")
            continue

        records.append({
            'state': state_abbr,
            'chamber': chamber,
            'district_number': district_number,
            'margin': margin,
            'harris': harris,
            'trump': trump,
            'total': total,
        })

    # Post-process VT Senate Essex/Orleans → Essex-Orleans
    vt_essex = None
    vt_orleans = None
    final_records = []
    for r in records:
        if r['district_number'] == '_VT_Essex':
            vt_essex = r
        elif r['district_number'] == '_VT_Orleans':
            vt_orleans = r
        else:
            final_records.append(r)

    if vt_essex and vt_orleans:
        combined_harris = vt_essex['harris'] + vt_orleans['harris']
        combined_trump = vt_essex['trump'] + vt_orleans['trump']
        combined_total = vt_essex['total'] + vt_orleans['total']
        combined_margin = compute_margin(combined_harris, combined_trump, combined_total)
        final_records.append({
            'state': 'VT',
            'chamber': 'Senate',
            'district_number': 'Essex-Orleans',
            'margin': combined_margin,
            'harris': combined_harris,
            'trump': combined_trump,
            'total': combined_total,
        })
    elif vt_essex:
        print("  WARNING: VT has Essex but not Orleans senate district")
    elif vt_orleans:
        print("  WARNING: VT has Orleans but not Essex senate district")

    records = final_records if (vt_essex or vt_orleans) else records

    # Post-process: combine duplicate district entries (ND/SD sub-districts)
    # When sub-districts like 4A and 4B both map to district "4", combine vote totals
    seen = {}
    deduped = []
    for r in records:
        key = (r['state'], r['chamber'], r['district_number'])
        if key in seen:
            # Combine vote totals
            prev = seen[key]
            prev['harris'] += r['harris']
            prev['trump'] += r['trump']
            prev['total'] += r['total']
            prev['margin'] = compute_margin(prev['harris'], prev['trump'], prev['total'])
        else:
            seen[key] = r
            deduped.append(r)
    records = deduped

    # Add state-level margin for statewide seats
    if state_pres_total > 0:
        state_margin = compute_margin(state_harris_total, state_trump_total, state_pres_total)
        records.append({
            'state': state_abbr,
            'chamber': 'Statewide',
            'district_number': 'Statewide',
            'margin': state_margin,
            'harris': state_harris_total,
            'trump': state_trump_total,
            'total': state_pres_total,
        })

    return records


def find_ma_match(name, name_map):
    """Try fuzzy matching for MA district names."""
    # Try with/without Oxford comma
    variants = [
        name,
        name.replace(', and ', ' and '),
        name.replace(' and ', ', and '),
        name.replace(', Dukes and Nantucket', ', Dukes, and Nantucket'),
        name.replace(', Dukes, and Nantucket', ', Dukes and Nantucket'),
    ]
    for v in variants:
        if v in name_map:
            return name_map[v]
    # Try case-insensitive
    name_lower = name.lower()
    for k, v in name_map.items():
        if k.lower() == name_lower:
            return v
    return None


def main():
    parser = argparse.ArgumentParser(description='Download 2024 presidential margins by legislative district')
    parser.add_argument('--state', type=str, help='Download a single state (abbreviation)')
    parser.add_argument('--output', type=str, default='/tmp/pres_margins.json', help='Output JSON path')
    args = parser.parse_args()

    states = [args.state.upper()] if args.state else sorted(STATE_GIDS.keys())

    all_records = []
    errors = []

    for i, state in enumerate(states):
        print(f"[{i+1}/{len(states)}] Downloading {state}...")
        csv_text = download_state_csv(state)
        if csv_text is None:
            errors.append(state)
            continue

        records = parse_state(state, csv_text)
        print(f"  → {len(records)} district records (incl statewide)")
        all_records.extend(records)

        if len(states) > 1 and i < len(states) - 1:
            time.sleep(0.5)  # rate limit Google Sheets

    # Summary
    print(f"\n{'='*60}")
    print(f"Total records: {len(all_records)}")
    print(f"States processed: {len(states) - len(errors)}/{len(states)}")
    if errors:
        print(f"Errors: {', '.join(errors)}")

    # Count by chamber
    from collections import Counter
    chamber_counts = Counter(r['chamber'] for r in all_records)
    for chamber, count in sorted(chamber_counts.items()):
        print(f"  {chamber}: {count}")

    # Write output
    with open(args.output, 'w') as f:
        json.dump(all_records, f, indent=2)
    print(f"\nWritten to {args.output}")


if __name__ == '__main__':
    main()
