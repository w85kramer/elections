"""
Audit seat gaps: compare Ballotpedia legislature members vs DB seat_terms.

Loads parsed BP member data from /tmp/legislature_members.json, queries DB for
current seat_terms, and produces a categorized gap report.

Categories:
  - match: BP name matches DB current_holder (no action needed)
  - name_mismatch: BP has different person than DB (someone new replaced old holder)
  - vacancy_confirmed: both BP and DB say vacant
  - vacancy_new: BP says vacant, DB still has holder
  - filled_vacancy: BP has holder, DB says vacant
  - midterm_start: BP name matches DB but assumed_office suggests mid-term replacement
  - date_update: BP name matches DB, but start_date could be corrected

Output: /tmp/seat_gaps_report.json

Usage:
    python3 scripts/audit_seat_gaps.py
    python3 scripts/audit_seat_gaps.py --state TX
    python3 scripts/audit_seat_gaps.py --summary
"""
import sys
import os
import re
import json
import argparse
from collections import Counter, defaultdict

import httpx

INPUT_PATH = '/tmp/legislature_members.json'
OUTPUT_PATH = '/tmp/seat_gaps_report.json'

# ══════════════════════════════════════════════════════════════════════
# DB QUERIES
# ══════════════════════════════════════════════════════════════════════

def run_query(sql):
    """Execute SQL via Supabase Management API."""
    resp = httpx.post(
        f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
        json={'query': sql},
        timeout=30,
    )
    if resp.status_code == 429:
        import time
        time.sleep(5)
        return run_query(sql)
    resp.raise_for_status()
    return resp.json()

def load_db_seats(state_filter=None):
    """
    Load current seat data from DB.

    Returns dict: (state_abbrev, chamber, district_number) -> {
        seat_id, seat_label, current_holder, current_holder_party,
        office_type, seat_term_id, candidate_id, start_date, start_reason,
        election_id, district_id
    }
    """
    where = ""
    if state_filter:
        where = f"AND st.abbreviation = '{state_filter}'"

    sql = f"""
    SELECT
        st.abbreviation AS state,
        d.chamber,
        d.district_number,
        s.id AS seat_id,
        s.seat_label,
        s.current_holder,
        s.current_holder_party,
        s.current_holder_caucus,
        s.office_type,
        s.seat_designator,
        d.id AS district_id,
        t.id AS seat_term_id,
        t.candidate_id,
        t.start_date,
        t.start_reason,
        t.party AS term_party,
        t.election_id,
        c.full_name AS candidate_name
    FROM seats s
    JOIN districts d ON s.district_id = d.id
    JOIN states st ON d.state_id = st.id
    LEFT JOIN seat_terms t ON t.seat_id = s.id AND t.end_date IS NULL
    LEFT JOIN candidates c ON t.candidate_id = c.id
    WHERE d.office_level = 'Legislative'
      AND s.selection_method = 'Elected'
      {where}
    ORDER BY st.abbreviation, d.chamber, d.district_number, s.seat_designator
    """
    return run_query(sql)

def load_existing_specials(state_filter=None):
    """Load existing special elections to cross-reference."""
    where = ""
    if state_filter:
        where = f"AND st.abbreviation = '{state_filter}'"

    sql = f"""
    SELECT
        st.abbreviation AS state,
        d.chamber,
        d.district_number,
        e.election_type,
        e.election_date,
        e.result_status,
        e.election_year
    FROM elections e
    JOIN seats s ON e.seat_id = s.id
    JOIN districts d ON s.district_id = d.id
    JOIN states st ON d.state_id = st.id
    WHERE e.election_type IN ('Special', 'Special_Primary', 'Special_Runoff')
      AND e.election_year IN (2025, 2026)
      {where}
    ORDER BY st.abbreviation, d.district_number
    """
    return run_query(sql)

# ══════════════════════════════════════════════════════════════════════
# NAME MATCHING
# ══════════════════════════════════════════════════════════════════════

def normalize_name(name):
    """Normalize a name for comparison."""
    if not name:
        return ''
    name = name.strip()
    # Remove suffixes
    name = re.sub(r'\s+(Jr\.?|Sr\.?|III|II|IV)$', '', name, flags=re.IGNORECASE)
    # Remove middle initials (single letter with period)
    name = re.sub(r'\s+[A-Z]\.\s+', ' ', name)
    # Remove leading initials like "E. Sam" -> "Sam"
    name = re.sub(r'^[A-Z]\.\s+', '', name)
    # Remove quotes/nicknames
    name = re.sub(r'"[^"]*"', '', name)
    name = re.sub(r"'[^']*'", '', name)
    # Normalize accented characters
    import unicodedata
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    # Normalize curly quotes/apostrophes to straight
    name = name.replace('\u2018', "'").replace('\u2019', "'")
    name = name.replace('\u201c', '"').replace('\u201d', '"')
    # Remove periods from initials (M.D. -> MD, T.J. -> TJ, R.J. -> RJ)
    name = re.sub(r'\.', '', name)
    # Collapse whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    return name.lower()

NICKNAMES = {
    'william': ['bill', 'will', 'billy', 'willy'],
    'robert': ['bob', 'bobby', 'rob'],
    'richard': ['dick', 'rick', 'rich'],
    'james': ['jim', 'jimmy', 'jamie'],
    'john': ['jack', 'johnny'],
    'joseph': ['joe', 'joey'],
    'thomas': ['tom', 'tommy'],
    'charles': ['charlie', 'chuck', 'chaz'],
    'edward': ['ed', 'eddie', 'ted', 'teddy'],
    'michael': ['mike', 'mikey'],
    'daniel': ['dan', 'danny'],
    'david': ['dave'],
    'stephen': ['steve', 'steven'],
    'steven': ['steve', 'stephen'],
    'christopher': ['chris'],
    'matthew': ['matt'],
    'anthony': ['tony'],
    'donald': ['don', 'donnie'],
    'timothy': ['tim', 'timmy'],
    'patrick': ['pat', 'paddy'],
    'elizabeth': ['liz', 'beth', 'betty', 'eliza'],
    'katherine': ['kate', 'kathy', 'katie', 'cathy'],
    'catherine': ['kate', 'kathy', 'katie', 'cathy'],
    'margaret': ['maggie', 'meg', 'peggy', 'marge'],
    'jennifer': ['jen', 'jenny'],
    'patricia': ['pat', 'patty', 'trish'],
    'deborah': ['deb', 'debbie', 'debby'],
    'pamela': ['pam'],
    'samantha': ['sam'],
    'samuel': ['sam', 'sammy'],
    'kenneth': ['ken', 'kenny'],
    'lawrence': ['larry'],
    'gerald': ['gerry', 'jerry'],
    'raymond': ['ray'],
    'andrew': ['andy', 'drew'],
    'benjamin': ['ben'],
    'gregory': ['greg'],
    'frederick': ['fred', 'freddy'],
    'ronald': ['ron', 'ronnie'],
    'alexander': ['alex'],
    'nicholas': ['nick', 'nicky'],
    'guadalupe': ['lupe'],
    'maria luisa': ['lulu'],
    'armando': ['mando'],
    'jesus': ['chuy'],
    'juan': ['chuy'],  # Regional nickname in TX
    'rafael': ['rafa'],
    'alejandro': ['alex'],
    'jessica': ['jess'],
    'caroline': ['caroline'],  # Harris vs Harris Davila is a last name issue
    'jacob': ['jake'],
    'antonio': ['tony'],
    'philip': ['griff', 'phil'],
    'phillip': ['phil'],
    'jennifer': ['jen', 'jenny'],
    'suzanne': ['sue', 'suzy'],
    'lucinda': ['cindy'],
    'denise': ['mitzi'],
    'cynthia': ['cindy'],
    'christine': ['tina', 'chris'],
    'melissa': ['missy'],
    'ismail': ['izzy'],
    'hurchel': ['trey'],
    'artis': ['a.j.'],
    'javan': ['j.d.'],
    'alexandra': ['ali', 'alex'],
    'anastasia': ['stacey'],
    'glenn': ['mike'],  # Glenn "Mike" Prax
    'kerry': ['bubba'],
    'patrice': ['penni'],
    'jervonte': ['tae'],
    'daryl': ['joy'],  # Uses middle name
    'michael': ['mike', 'doc', 'mikey'],
    'anissa': ['nissa'],
    'larry': ['butch'],
    'harold': ['trey'],
    'leonidas': ['lou'],
    'arthur': ['doc'],
    'jonathan': ['jack'],
    'roberto': ['bobby'],
    'susan': ['sue'],
    'dagmara': ['dee'],
    'michel': ['mike'],
    'john': ['jack', 'johnny', 'jay'],
    'david': ['dave'],
}

# Build reverse lookup
_NICKNAME_GROUPS = {}
for formal, nicks in NICKNAMES.items():
    group = frozenset([formal] + nicks)
    _NICKNAME_GROUPS[formal] = group
    for n in nicks:
        _NICKNAME_GROUPS[n] = group

def nicknames_match(name1, name2):
    """Check if two first names are nickname equivalents."""
    if name1 == name2:
        return True
    g1 = _NICKNAME_GROUPS.get(name1)
    g2 = _NICKNAME_GROUPS.get(name2)
    if g1 and name2 in g1:
        return True
    if g2 and name1 in g2:
        return True
    return False

def names_match(bp_name, db_name):
    """Check if two names refer to the same person."""
    if not bp_name or not db_name:
        return False

    n1 = normalize_name(bp_name)
    n2 = normalize_name(db_name)

    # Exact match
    if n1 == n2:
        return True

    # Split into parts
    parts1 = n1.split()
    parts2 = n2.split()

    if not parts1 or not parts2:
        return False

    # Last name: check if one contains the other (handles "Harris" vs "Harris Davila")
    last1 = parts1[-1]
    last2 = parts2[-1]
    # Try multi-word last names
    lname_match = (last1 == last2)
    if not lname_match:
        # Check if the last words match when one has extra words
        # "Harris Davila" vs "Harris" -> match on "Harris"
        full1 = ' '.join(parts1[1:]) if len(parts1) > 1 else last1
        full2 = ' '.join(parts2[1:]) if len(parts2) > 1 else last2
        if full1.startswith(full2) or full2.startswith(full1):
            lname_match = True
        elif last1 in full2 or last2 in full1:
            lname_match = True

    if not lname_match:
        return False

    # First name matching
    first1 = parts1[0]
    first2 = parts2[0]

    if first1 == first2:
        return True

    # Nickname matching
    if nicknames_match(first1, first2):
        return True

    # One is initial of the other
    if len(first1) <= 2 and first2.startswith(first1.rstrip('.')):
        return True
    if len(first2) <= 2 and first1.startswith(first2.rstrip('.')):
        return True

    # First 3 chars match (avoid false positives like LeMario→Liz)
    if len(first1) >= 3 and len(first2) >= 3 and first1[:3] == first2[:3]:
        return True

    # Multi-word first name: "Maria Luisa Flores" -> first name is "Maria Luisa"
    # Check all possible splits for multi-word first names
    for split in range(1, len(parts1)):
        bp_first_full = ' '.join(parts1[:split])
        if nicknames_match(bp_first_full, first2):
            return True
    for split in range(1, len(parts2)):
        db_first_full = ' '.join(parts2[:split])
        if nicknames_match(first1, db_first_full):
            return True

    return False

# ══════════════════════════════════════════════════════════════════════
# DISTRICT MATCHING
# ══════════════════════════════════════════════════════════════════════

# Map BP chamber names to DB chamber names
CHAMBER_MAP = {
    'Senate': 'Senate',
    'House': 'House',
    'Assembly': 'Assembly',
    'House of Delegates': 'House of Delegates',
    'Legislature': 'Legislature',
}

def normalize_district(state, chamber, district):
    """Normalize a district number for matching."""
    d = district.strip()
    # For numeric districts, strip leading zeros
    if d.isdigit():
        d = str(int(d))
    return d

def find_matching_db_seats(bp_member, db_seats_by_key):
    """
    Find DB seats matching a BP member record.

    Returns list of matching DB seat records.
    """
    state = bp_member['state']
    chamber = bp_member['chamber']
    district = bp_member['district']

    # Direct match
    key = (state, chamber, district)
    if key in db_seats_by_key:
        return db_seats_by_key[key]

    # Try normalized
    norm_district = normalize_district(state, chamber, district)
    key2 = (state, chamber, norm_district)
    if key2 in db_seats_by_key:
        return db_seats_by_key[key2]

    # ── MA: named districts → numeric via alphabetical sort ──
    if state == 'MA' and _MA_MAPS:
        house_map, senate_map = _MA_MAPS
        stripped = re.sub(r'\s+District$', '', district)
        m_map = house_map if chamber == 'House' else senate_map
        if stripped in m_map:
            db_num = m_map[stripped]
            key3 = (state, chamber, db_num)
            if key3 in db_seats_by_key:
                return db_seats_by_key[key3]

    # ── MN House: "1A" → DB district "1", "1B" → DB district "2" ──
    # Each MN Senate district N has two House sub-districts NA and NB
    # DB numbering: NA → 2*N-1, NB → 2*N
    if state == 'MN' and chamber == 'House':
        m = re.match(r'^(\d+)([AB])$', district)
        if m:
            senate_num = int(m.group(1))
            suffix = m.group(2)
            db_num = str(2 * senate_num - 1) if suffix == 'A' else str(2 * senate_num)
            key3 = (state, chamber, db_num)
            if key3 in db_seats_by_key:
                return db_seats_by_key[key3]

    # ── AK Senate: letter districts A=1, B=2, ..., T=20 ──
    if state == 'AK' and chamber == 'Senate':
        if len(district) == 1 and district.isalpha():
            db_num = str(ord(district.upper()) - ord('A') + 1)
            key3 = (state, chamber, db_num)
            if key3 in db_seats_by_key:
                return db_seats_by_key[key3]

    # ── Paired 2-member House: WA, ID, NJ, ND, SD, AZ ──
    # BP format: "1A"/"1B" or "1-Position 1"/"1-Position 2"
    # DB format: district "1" with seat_designator "A"/"B"
    # Return ALL seats for the district (position↔seat mapping isn't 1:1)
    if state in ('WA', 'ID', 'NJ', 'ND', 'SD', 'AZ') and chamber in ('House', 'Assembly'):
        base_district = None

        # Handle "N-Position P" format (WA)
        m = re.match(r'^(\d+)-Position\s+(\d+)$', district)
        if m:
            base_district = m.group(1)

        # Handle "NA"/"NB" format (ID, SD, AZ)
        if not base_district:
            m = re.match(r'^(\d+)([AB])$', district)
            if m:
                base_district = m.group(1)

        if base_district:
            key3 = (state, chamber, base_district)
            if key3 in db_seats_by_key:
                return db_seats_by_key[key3]

    # ── VT: strip trailing -1 for single multi-county districts ──
    if state == 'VT' and district.endswith('-1'):
        base = district[:-2]
        key3 = (state, chamber, base)
        if key3 in db_seats_by_key:
            return db_seats_by_key[key3]

    # ── VT: "Grand-Isle-Chittenden" -> "Grand Isle-Chittenden" ──
    # Also handles "Grand-Isle" -> "Grand Isle" (all hyphens to spaces)
    if state == 'VT' and '-' in district:
        parts = district.split('-')
        # Try all hyphens as spaces
        all_spaces = ' '.join(parts)
        key_all = (state, chamber, all_spaces)
        if key_all in db_seats_by_key:
            return db_seats_by_key[key_all]
        # Try partial: first N words joined by space, rest by hyphen
        for i in range(2, len(parts)):
            candidate = ' '.join(parts[:i]) + '-' + '-'.join(parts[i:])
            key4 = (state, chamber, candidate)
            if key4 in db_seats_by_key:
                return db_seats_by_key[key4]
            # Also try without trailing number
            if candidate.endswith('-1'):
                key5 = (state, chamber, candidate[:-2])
                if key5 in db_seats_by_key:
                    return db_seats_by_key[key5]

    # ── VT Senate: BP splits combined districts ──
    # "Essex" + "Orleans" → "Essex-Orleans" in DB
    if state == 'VT' and chamber == 'Senate':
        # Try hyphenating with each existing DB district that contains this name
        for db_key in db_seats_by_key:
            if db_key[0] == 'VT' and db_key[1] == 'Senate':
                db_dist = db_key[2]
                # Check if our district is a component of a combined DB district
                if district in db_dist.split('-') or district.replace('-', ' ') in db_dist.split('-'):
                    return db_seats_by_key[db_key]

    return []

# Pre-built MA district name→number maps
_MA_MAPS = None

def _bp_to_os_name(name, chamber):
    """Convert BP district name to OpenStates format (for MA sorting)."""
    result = name
    if chamber == 'Senate':
        ordinal_map = {
            "1st ": "First ", "2nd ": "Second ", "3rd ": "Third ",
            "4th ": "Fourth ", "5th ": "Fifth ",
        }
        for abbr, word in ordinal_map.items():
            if result.startswith(abbr):
                result = word + result[len(abbr):]
                break
    # Remove Oxford comma
    result = result.replace(", and ", " and ")
    return result

def get_ma_district_maps(bp_members):
    """Build MA district name → DB number mapping from BP data.

    The DB assigned numbers by sorting OpenStates-format names alphabetically.
    We must replicate that exact sort order.
    """
    global _MA_MAPS
    if _MA_MAPS is not None:
        return _MA_MAPS

    house_bp_names = set()
    senate_bp_names = set()
    for m in bp_members:
        if m['state'] != 'MA':
            continue
        # Strip " District" suffix
        name = re.sub(r'\s+District$', '', m['district'])
        if m['chamber'] == 'House':
            house_bp_names.add(name)
        elif m['chamber'] == 'Senate':
            senate_bp_names.add(name)

    # Convert to OS format and sort (matches DB numbering)
    house_os = sorted(_bp_to_os_name(n, 'House') for n in house_bp_names)
    senate_os = sorted(_bp_to_os_name(n, 'Senate') for n in senate_bp_names)

    # Build reverse map: BP name → DB number
    # First build OS name → number
    house_os_map = {name: str(i + 1) for i, name in enumerate(house_os)}
    senate_os_map = {name: str(i + 1) for i, name in enumerate(senate_os)}

    # Then map BP name → OS name → number
    house_map = {}
    for bp_name in house_bp_names:
        os_name = _bp_to_os_name(bp_name, 'House')
        if os_name in house_os_map:
            house_map[bp_name] = house_os_map[os_name]

    senate_map = {}
    for bp_name in senate_bp_names:
        os_name = _bp_to_os_name(bp_name, 'Senate')
        if os_name in senate_os_map:
            senate_map[bp_name] = senate_os_map[os_name]

    _MA_MAPS = (house_map, senate_map)
    return _MA_MAPS

# ══════════════════════════════════════════════════════════════════════
# DATE ANALYSIS
# ══════════════════════════════════════════════════════════════════════

def parse_assumed_date(date_str):
    """Parse BP's 'Date assumed office' field. Returns (year, month, day) or None."""
    if not date_str or date_str in ('—', '-', 'N/A', ''):
        return None

    # Full date: "January 10, 2017"
    m = re.match(r'^(\w+)\s+(\d{1,2}),?\s+(\d{4})$', date_str)
    if m:
        months = {'january': 1, 'february': 2, 'march': 3, 'april': 4,
                  'may': 5, 'june': 6, 'july': 7, 'august': 8,
                  'september': 9, 'october': 10, 'november': 11, 'december': 12}
        month = months.get(m.group(1).lower(), 0)
        return (int(m.group(3)), month, int(m.group(2)))

    # Year only: "2015"
    m = re.match(r'^(\d{4})$', date_str)
    if m:
        return (int(m.group(1)), 1, 1)

    return None

def is_midterm_date(assumed_date_tuple, chamber):
    """
    Determine if an assumed-office date suggests a mid-term replacement.

    Most legislators take office in January of odd years (after November elections).
    If someone took office at another time, they were likely appointed or won a special.
    """
    if not assumed_date_tuple:
        return False

    year, month, day = assumed_date_tuple

    # Standard inauguration: January of odd year (or early December for NH/some states)
    # If month is Jan and year is odd → normal
    if month in (1, 12) and year % 2 == 1:
        return False
    # If month is Jan and year is even for states with even-year terms
    if month in (1, 12) and year % 2 == 0:
        return False  # Could be normal for some states

    # If year-only "2015" → can't determine precisely, assume normal
    if month == 1 and day == 1 and year < 2024:
        return False

    # Otherwise, mid-term if month is not January/December
    if month not in (1, 12):
        return True

    return False

# ══════════════════════════════════════════════════════════════════════
# MAIN AUDIT
# ══════════════════════════════════════════════════════════════════════

def run_audit(state_filter=None, summary_only=False):
    # Load BP data
    if not os.path.exists(INPUT_PATH):
        print(f'ERROR: {INPUT_PATH} not found. Run download_legislature_members.py first.')
        sys.exit(1)

    with open(INPUT_PATH) as f:
        bp_members = json.load(f)

    if state_filter:
        bp_members = [m for m in bp_members if m['state'] == state_filter]

    print(f'Loaded {len(bp_members)} BP member records')

    # Build MA district name→number maps (needed before matching)
    get_ma_district_maps(bp_members)

    # Load DB data
    print('Loading DB seats...')
    db_rows = load_db_seats(state_filter)
    print(f'Loaded {len(db_rows)} DB seat records')

    # Load existing specials for cross-reference
    print('Loading existing special elections...')
    specials = load_existing_specials(state_filter)
    special_keys = set()
    for sp in specials:
        special_keys.add((sp['state'], sp['chamber'], sp['district_number']))

    # Index DB seats by (state, chamber, district)
    # For multi-member districts, there will be multiple seats per district
    db_seats_by_key = defaultdict(list)
    for row in db_rows:
        key = (row['state'], row['chamber'], row['district_number'])
        db_seats_by_key[key].append(row)

    # Track results
    results = {
        'match': [],           # Name matches, no action needed
        'name_mismatch': [],   # Different person in seat
        'vacancy_confirmed': [],  # Both say vacant
        'vacancy_new': [],     # BP vacant, DB has holder
        'filled_vacancy': [],  # BP has holder, DB vacant
        'midterm_start': [],   # Matches but mid-term assumed office
        'date_update': [],     # Matches but start_date can be corrected
        'no_db_match': [],     # BP district not found in DB
    }

    # Pre-process: normalize paired-state BP districts for proper grouping
    # WA: "1-Position 1" → "1", ID/SD/AZ: "1A" → "1", MN: "1A" → mapped number
    for m in bp_members:
        s, ch, d = m['state'], m['chamber'], m['district']
        if s in ('WA',) and ch == 'House':
            match = re.match(r'^(\d+)-Position\s+\d+$', d)
            if match:
                m['district'] = match.group(1)
        elif s in ('ID', 'SD', 'AZ') and ch == 'House':
            match = re.match(r'^(\d+)[AB]$', d)
            if match:
                m['district'] = match.group(1)
        elif s == 'MN' and ch == 'House':
            match = re.match(r'^(\d+)([AB])$', d)
            if match:
                senate_num = int(match.group(1))
                suffix = match.group(2)
                m['district'] = str(2 * senate_num - 1) if suffix == 'A' else str(2 * senate_num)
        elif s == 'AK' and ch == 'Senate':
            if len(d) == 1 and d.isalpha():
                m['district'] = str(ord(d.upper()) - ord('A') + 1)
        elif s == 'MA':
            stripped = re.sub(r'\s+District$', '', d)
            m_map = _MA_MAPS[0] if ch == 'House' else _MA_MAPS[1]
            if stripped in m_map:
                m['district'] = m_map[stripped]

    # Group BP members by (state, chamber, district) for multi-member handling
    bp_by_district = defaultdict(list)
    for m in bp_members:
        key = (m['state'], m['chamber'], m['district'])
        bp_by_district[key].append(m)

    # Process each BP district group
    processed_seats = set()  # Track which DB seat_ids we've matched

    for bp_key, bp_group in bp_by_district.items():
        state, chamber, district = bp_key

        # Find matching DB seats
        db_seats = find_matching_db_seats(bp_group[0], db_seats_by_key)

        if not db_seats:
            for m in bp_group:
                results['no_db_match'].append({
                    'state': state, 'chamber': chamber, 'district': district,
                    'bp_name': m['name'], 'bp_party': m['party'],
                    'assumed_office': m['assumed_office'],
                    'is_vacant': m['is_vacant'],
                })
            continue

        # Match BP members to DB seats
        # For single-member districts: 1 BP member -> 1 DB seat
        # For multi-member: multiple BP members -> multiple DB seats
        bp_filled = [m for m in bp_group if not m['is_vacant']]
        bp_vacant = [m for m in bp_group if m['is_vacant']]
        db_filled = [s for s in db_seats if s['current_holder']]
        db_vacant = [s for s in db_seats if not s['current_holder']]

        # Match filled BP members to filled DB seats by name
        matched_bp = set()
        matched_db = set()

        for i, bp in enumerate(bp_filled):
            for j, db in enumerate(db_filled):
                if j in matched_db:
                    continue
                if names_match(bp['name'], db['current_holder']):
                    matched_bp.add(i)
                    matched_db.add(j)
                    processed_seats.add(db['seat_id'])

                    # Check for date updates
                    assumed = parse_assumed_date(bp['assumed_office'])
                    is_mid = is_midterm_date(assumed, chamber)

                    has_special = (state, chamber, district) in special_keys

                    if is_mid and not has_special:
                        results['midterm_start'].append({
                            'state': state, 'chamber': chamber, 'district': district,
                            'bp_name': bp['name'], 'bp_party': bp['party'],
                            'assumed_office': bp['assumed_office'],
                            'db_holder': db['current_holder'],
                            'db_start_date': db['start_date'],
                            'db_start_reason': db['start_reason'],
                            'seat_id': db['seat_id'],
                            'seat_term_id': db['seat_term_id'],
                            'seat_label': db['seat_label'],
                        })
                    elif assumed and db['start_date'] == '2025-01-01' and db['start_reason'] == 'elected':
                        # Generic start_date from OpenStates bulk load
                        results['date_update'].append({
                            'state': state, 'chamber': chamber, 'district': district,
                            'bp_name': bp['name'],
                            'assumed_office': bp['assumed_office'],
                            'db_start_date': db['start_date'],
                            'seat_id': db['seat_id'],
                            'seat_term_id': db['seat_term_id'],
                            'seat_label': db['seat_label'],
                        })
                    else:
                        results['match'].append({
                            'state': state, 'chamber': chamber, 'district': district,
                            'bp_name': bp['name'],
                            'db_holder': db['current_holder'],
                            'assumed_office': bp['assumed_office'],
                            'seat_label': db['seat_label'],
                        })
                    break

        # Unmatched BP filled = name mismatches or filled vacancies
        for i, bp in enumerate(bp_filled):
            if i in matched_bp:
                continue
            # Try to pair with an unmatched DB seat
            paired_db = None
            for j, db in enumerate(db_filled):
                if j not in matched_db:
                    paired_db = db
                    matched_db.add(j)
                    break

            if paired_db:
                # Name mismatch: different person in same seat
                has_special = (state, chamber, district) in special_keys
                results['name_mismatch'].append({
                    'state': state, 'chamber': chamber, 'district': district,
                    'bp_name': bp['name'], 'bp_party': bp['party'],
                    'assumed_office': bp['assumed_office'],
                    'db_holder': paired_db['current_holder'],
                    'db_party': paired_db['current_holder_party'],
                    'db_start_date': paired_db['start_date'],
                    'seat_id': paired_db['seat_id'],
                    'seat_term_id': paired_db['seat_term_id'],
                    'seat_label': paired_db['seat_label'],
                    'has_special_election': has_special,
                })
            else:
                # Filled vacancy: BP has holder, no DB seat to pair
                # Pair with a vacant DB seat if available
                if db_vacant:
                    vac_db = db_vacant.pop(0)
                    results['filled_vacancy'].append({
                        'state': state, 'chamber': chamber, 'district': district,
                        'bp_name': bp['name'], 'bp_party': bp['party'],
                        'assumed_office': bp['assumed_office'],
                        'seat_id': vac_db['seat_id'],
                        'seat_label': vac_db['seat_label'],
                    })
                else:
                    results['name_mismatch'].append({
                        'state': state, 'chamber': chamber, 'district': district,
                        'bp_name': bp['name'], 'bp_party': bp['party'],
                        'assumed_office': bp['assumed_office'],
                        'db_holder': '(no unmatched DB seat)',
                        'db_party': '',
                        'seat_id': None,
                        'seat_label': f'{state} {chamber} {district}',
                        'has_special_election': False,
                    })

        # BP vacancies
        for v in bp_vacant:
            # Try to pair with unmatched DB filled seats
            paired_db = None
            for j, db in enumerate(db_filled):
                if j not in matched_db:
                    paired_db = db
                    matched_db.add(j)
                    break

            if paired_db:
                # New vacancy: BP says vacant, DB still has holder
                has_special = (state, chamber, district) in special_keys
                results['vacancy_new'].append({
                    'state': state, 'chamber': chamber, 'district': district,
                    'db_holder': paired_db['current_holder'],
                    'db_party': paired_db['current_holder_party'],
                    'seat_id': paired_db['seat_id'],
                    'seat_term_id': paired_db['seat_term_id'],
                    'seat_label': paired_db['seat_label'],
                    'has_special_election': has_special,
                })
            elif db_vacant:
                # Confirmed vacancy: both BP and DB say vacant
                vac_db = db_vacant.pop(0)
                results['vacancy_confirmed'].append({
                    'state': state, 'chamber': chamber, 'district': district,
                    'seat_id': vac_db['seat_id'],
                    'seat_label': vac_db['seat_label'],
                })
            else:
                results['vacancy_confirmed'].append({
                    'state': state, 'chamber': chamber, 'district': district,
                    'seat_id': None,
                    'seat_label': f'{state} {chamber} {district}',
                })

        # Remaining unmatched DB filled seats (DB has holder but no BP member)
        for j, db in enumerate(db_filled):
            if j not in matched_db:
                results['name_mismatch'].append({
                    'state': state, 'chamber': chamber, 'district': district,
                    'bp_name': '(not in BP)',
                    'db_holder': db['current_holder'],
                    'db_party': db['current_holder_party'],
                    'seat_id': db['seat_id'],
                    'seat_label': db['seat_label'],
                    'has_special_election': False,
                })

    # Report
    print(f'\n{"═"*60}')
    print(f'SEAT GAP AUDIT REPORT')
    print(f'{"═"*60}')
    print(f'Matches (no action):        {len(results["match"]):>5}')
    print(f'Date updates possible:      {len(results["date_update"]):>5}')
    print(f'Mid-term start detected:    {len(results["midterm_start"]):>5}')
    print(f'Name mismatches:            {len(results["name_mismatch"]):>5}')
    print(f'Vacancies (confirmed):      {len(results["vacancy_confirmed"]):>5}')
    print(f'Vacancies (new in BP):      {len(results["vacancy_new"]):>5}')
    print(f'Filled vacancies:           {len(results["filled_vacancy"]):>5}')
    print(f'No DB match:                {len(results["no_db_match"]):>5}')
    print(f'{"─"*60}')
    total_issues = (len(results['name_mismatch']) + len(results['vacancy_new']) +
                    len(results['filled_vacancy']) + len(results['midterm_start']))
    print(f'Total issues to investigate: {total_issues:>4}')

    if not summary_only:
        # Print details for each category
        if results['name_mismatch']:
            print(f'\n{"─"*60}')
            print('NAME MISMATCHES:')
            by_state = defaultdict(list)
            for r in results['name_mismatch']:
                by_state[r['state']].append(r)
            for state in sorted(by_state):
                print(f'\n  {state}:')
                for r in by_state[state]:
                    special_flag = ' [HAS SPECIAL]' if r.get('has_special_election') else ''
                    print(f'    {r["seat_label"]}: BP="{r["bp_name"]}" vs DB="{r["db_holder"]}"{special_flag}')
                    if r.get('assumed_office'):
                        print(f'      Assumed office: {r["assumed_office"]}')

        if results['vacancy_new']:
            print(f'\n{"─"*60}')
            print('NEW VACANCIES (BP says vacant, DB has holder):')
            for r in results['vacancy_new']:
                special_flag = ' [HAS SPECIAL]' if r.get('has_special_election') else ''
                print(f'  {r["seat_label"]}: DB holder="{r["db_holder"]}"{special_flag}')

        if results['filled_vacancy']:
            print(f'\n{"─"*60}')
            print('FILLED VACANCIES (BP has holder, DB says vacant):')
            for r in results['filled_vacancy']:
                print(f'  {r["seat_label"]}: BP="{r["bp_name"]}" ({r["bp_party"]}), assumed: {r["assumed_office"]}')

        if results['midterm_start']:
            print(f'\n{"─"*60}')
            print('MID-TERM STARTS (no special election in DB):')
            by_state = defaultdict(list)
            for r in results['midterm_start']:
                by_state[r['state']].append(r)
            for state in sorted(by_state):
                print(f'\n  {state}:')
                for r in by_state[state]:
                    print(f'    {r["seat_label"]}: "{r["bp_name"]}" assumed {r["assumed_office"]}')
                    print(f'      DB start: {r["db_start_date"]}, reason: {r["db_start_reason"]}')

        if results['no_db_match']:
            print(f'\n{"─"*60}')
            print('NO DB MATCH:')
            by_state = defaultdict(list)
            for r in results['no_db_match']:
                by_state[r['state']].append(r)
            for state in sorted(by_state):
                print(f'\n  {state}:')
                for r in by_state[state]:
                    name = r['bp_name'] if r['bp_name'] else 'VACANT'
                    print(f'    {state} {r["chamber"]} {r["district"]}: {name}')

    # Save full report
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f'\nFull report saved to {OUTPUT_PATH}')

    return results

def main():
    parser = argparse.ArgumentParser(description='Audit seat gaps: BP vs DB')
    parser.add_argument('--state', help='Filter to single state (e.g., TX)')
    parser.add_argument('--summary', action='store_true', help='Show summary only')
    args = parser.parse_args()

    run_audit(state_filter=args.state.upper() if args.state else None,
              summary_only=args.summary)

if __name__ == '__main__':
    main()
