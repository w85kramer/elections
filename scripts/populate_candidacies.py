"""
Populate candidacies table from Ballotpedia election pages.

Downloads Ballotpedia's 2026 state legislative election pages, parses candidate
lists from the primary election tables, matches candidates to DB elections, and
inserts candidacy records.

Phase 1: 9 states with closed filing deadlines (as of 2026-02-08).

Usage:
    python3 scripts/populate_candidacies.py --state TX
    python3 scripts/populate_candidacies.py --state TX --dry-run
    python3 scripts/populate_candidacies.py --all-closed
    python3 scripts/populate_candidacies.py --state NC --force   # re-run on partially-populated state
"""
import sys
import re
import time
import argparse
import html as htmlmod
from collections import Counter, defaultdict

import httpx
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

BATCH_SIZE = 400

# Filing deadlines for states with closed filing (as of 2026-02-20)
CLOSED_FILING_STATES = {
    'IL': '2025-11-03',
    'AR': '2025-11-11',
    'TX': '2025-12-08',
    'NC': '2025-12-19',
    'UT': '2026-01-08',
    'KY': '2026-01-09',
    'AL': '2026-01-23',
    'WV': '2026-01-31',
    'NM': '2026-02-03',
    'OH': '2026-02-04',
    'IN': '2026-02-06',
    'MD': '2026-02-24',
    'ID': '2026-02-27',
}

# Ballotpedia URL patterns for each state's chambers
# (state_name, chamber_type_in_url, our_office_type)
STATE_CHAMBERS = {
    'AL': [('Alabama', 'House_of_Representatives', 'State House'),
           ('Alabama', 'State_Senate', 'State Senate')],
    'AR': [('Arkansas', 'House_of_Representatives', 'State House'),
           ('Arkansas', 'State_Senate', 'State Senate')],
    'IL': [('Illinois', 'House_of_Representatives', 'State House'),
           ('Illinois', 'State_Senate', 'State Senate')],
    'IN': [('Indiana', 'House_of_Representatives', 'State House'),
           ('Indiana', 'State_Senate', 'State Senate')],
    'KY': [('Kentucky', 'House_of_Representatives', 'State House'),
           ('Kentucky', 'State_Senate', 'State Senate')],
    'NC': [('North_Carolina', 'House_of_Representatives', 'State House'),
           ('North_Carolina', 'State_Senate', 'State Senate')],
    'OH': [('Ohio', 'House_of_Representatives', 'State House'),
           ('Ohio', 'State_Senate', 'State Senate')],
    'TX': [('Texas', 'House_of_Representatives', 'State House'),
           ('Texas', 'State_Senate', 'State Senate')],
    'WV': [('West_Virginia', 'House_of_Delegates', 'State House'),
           ('West_Virginia', 'State_Senate', 'State Senate')],
    'UT': [('Utah', 'House_of_Representatives', 'State House'),
           ('Utah', 'State_Senate', 'State Senate')],
    'NM': [('New_Mexico', 'House_of_Representatives', 'State House')],
    'MD': [('Maryland', 'House_of_Delegates', 'State House'),
           ('Maryland', 'State_Senate', 'State Senate')],
    'ID': [('Idaho', 'House_of_Representatives', 'State House'),
           ('Idaho', 'State_Senate', 'State Senate')],
}

PARTY_MAP = {
    'D': 'D',
    'R': 'R',
    'Democratic': 'D',
    'Republican': 'R',
    'Independent': 'I',
    'Libertarian': 'L',
    'Green': 'G',
}

# Ballotpedia URL patterns for statewide offices
# Values are (url_suffix_template, [office_types_on_page])
# Most pages have one office; OH/IL have joint Gov+LtGov pages
STATEWIDE_PAGES = {
    'AL': [
        ('{s}_gubernatorial_election,_2026', ['Governor']),
        ('{s}_lieutenant_gubernatorial_election,_2026', ['Lt. Governor']),
        ('{s}_Attorney_General_election,_2026', ['Attorney General']),
        ('{s}_Secretary_of_State_election,_2026', ['Secretary of State']),
        ('{s}_Treasurer_election,_2026', ['Treasurer']),
        ('{s}_Auditor_election,_2026', ['Auditor']),
        ('{s}_Agriculture_Commissioner_election,_2026', ['Agriculture Commissioner']),
    ],
    'AR': [
        ('{s}_gubernatorial_election,_2026', ['Governor']),
        ('{s}_lieutenant_gubernatorial_election,_2026', ['Lt. Governor']),
        ('{s}_Attorney_General_election,_2026', ['Attorney General']),
        ('{s}_Secretary_of_State_election,_2026', ['Secretary of State']),
        ('{s}_Treasurer_election,_2026', ['Treasurer']),
        ('{s}_Auditor_election,_2026', ['Auditor']),
    ],
    'IL': [
        ('{s}_gubernatorial_and_lieutenant_gubernatorial_election,_2026', ['Governor', 'Lt. Governor']),
        ('{s}_Attorney_General_election,_2026', ['Attorney General']),
        ('{s}_Secretary_of_State_election,_2026', ['Secretary of State']),
        ('{s}_Treasurer_election,_2026', ['Treasurer']),
        ('{s}_Comptroller_election,_2026', ['Controller']),
    ],
    'OH': [
        ('{s}_gubernatorial_and_lieutenant_gubernatorial_election,_2026', ['Governor', 'Lt. Governor']),
        ('{s}_Attorney_General_election,_2026', ['Attorney General']),
        ('{s}_Secretary_of_State_election,_2026', ['Secretary of State']),
        ('{s}_Treasurer_election,_2026', ['Treasurer']),
        ('{s}_Auditor_election,_2026', ['Auditor']),
    ],
    'TX': [
        ('{s}_gubernatorial_election,_2026', ['Governor']),
        ('{s}_lieutenant_gubernatorial_election,_2026', ['Lt. Governor']),
        ('{s}_Attorney_General_election,_2026', ['Attorney General']),
        ('{s}_Comptroller_election,_2026', ['Controller']),
        ('{s}_Agriculture_Commissioner_election,_2026', ['Agriculture Commissioner']),
    ],
    'NM': [
        ('{s}_gubernatorial_election,_2026', ['Governor']),
        ('{s}_lieutenant_gubernatorial_election,_2026', ['Lt. Governor']),
        ('{s}_Attorney_General_election,_2026', ['Attorney General']),
        ('{s}_Secretary_of_State_election,_2026', ['Secretary of State']),
        ('{s}_Treasurer_election,_2026', ['Treasurer']),
        ('{s}_Auditor_election,_2026', ['Auditor']),
    ],
    'MD': [
        ('{s}_gubernatorial_and_lieutenant_gubernatorial_election,_2026', ['Governor', 'Lt. Governor']),
        ('{s}_Attorney_General_election,_2026', ['Attorney General']),
        ('{s}_Comptroller_election,_2026', ['Controller']),
    ],
    'ID': [
        ('{s}_gubernatorial_election,_2026', ['Governor']),
        ('{s}_lieutenant_gubernatorial_election,_2026', ['Lt. Governor']),
        ('{s}_Attorney_General_election,_2026', ['Attorney General']),
        ('{s}_Secretary_of_State_election,_2026', ['Secretary of State']),
        ('{s}_Treasurer_election,_2026', ['Treasurer']),
        ('{s}_Controller_election,_2026', ['Controller']),
        ('{s}_Superintendent_of_Public_Instruction_election,_2026', ['Superintendent of Public Instruction']),
    ],
}

STATE_FULL_NAMES = {
    'AL': 'Alabama', 'AR': 'Arkansas', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'KY': 'Kentucky',
    'MD': 'Maryland', 'NC': 'North_Carolina', 'NM': 'New_Mexico',
    'OH': 'Ohio', 'TX': 'Texas', 'UT': 'Utah', 'WV': 'West_Virginia',
}

def run_sql(query, exit_on_error=True):
    resp = httpx.post(
        f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
        json={'query': query},
        timeout=120
    )
    if resp.status_code != 201:
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None
    return resp.json()

def esc(s):
    if s is None:
        return ''
    return str(s).replace("'", "''")

# ══════════════════════════════════════════════════════════════════════
# STEP 1: Download Ballotpedia HTML
# ══════════════════════════════════════════════════════════════════════

def download_bp_page(state_name, chamber_type):
    """Download a Ballotpedia election page. Returns HTML string or None."""
    url = f'https://ballotpedia.org/{state_name}_{chamber_type}_elections,_2026'
    try:
        resp = httpx.get(
            url,
            headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'},
            follow_redirects=True,
            timeout=30
        )
        if resp.status_code == 200:
            return resp.text
        else:
            print(f'    WARNING: HTTP {resp.status_code} for {url}')
            return None
    except Exception as e:
        print(f'    WARNING: Download failed for {url}: {e}')
        return None

# ══════════════════════════════════════════════════════════════════════
# STEP 2: Parse HTML → Extract Candidates
# ══════════════════════════════════════════════════════════════════════

def parse_primary_candidates(html_text):
    """
    Parse Ballotpedia primary election table and extract candidates.

    Returns list of (district_number: int, candidate_name: str, party: str, is_incumbent: bool)
    """
    # Find all candidateListTablePartisan tables
    table_starts = list(re.finditer(
        r'<table class="wikitable sortable collapsible candidateListTablePartisan">',
        html_text
    ))

    if not table_starts:
        return []

    # The first table is the primary table. Extract HTML between first and second table.
    primary_start = table_starts[0].start()
    if len(table_starts) > 1:
        primary_end = table_starts[1].start()
    else:
        primary_end = len(html_text)
    primary_html = html_text[primary_start:primary_end]

    # Verify it's actually a primary table
    title_match = re.search(r'<h4>([^<]+)</h4>', primary_html)
    if title_match:
        title = title_match.group(1).lower()
        if 'primary' not in title:
            print(f'    WARNING: First table is not primary: "{title_match.group(1)}"')

    # Parse district rows.
    # Pattern: <tr> <td><a href="...District_N">District N</a></td> <td>Dem</td> <td>Rep</td> <td>Other</td> </tr>
    # Also handles ID-style "District 1A" — captures optional seat letter suffix
    row_pattern = re.compile(
        r'<tr>\s*<td>\s*<a\s+href="[^"]*?District[_\s]+(\d+)\s*"[^>]*>'
        r'District\s+\d+([A-Z])?\s*</a></td>'
        r'(.*?)</tr>',
        re.DOTALL
    )

    # Candidate span pattern
    cand_pattern = re.compile(
        r'<span\s+class="candidate">\s*<a\s+href="[^"]*">([^<]+)</a>\s*(.*?)</span>',
        re.DOTALL
    )

    # Split row content into <td> columns
    td_pattern = re.compile(r'<td[^>]*>(.*?)</td>', re.DOTALL)

    candidates = []
    rows = row_pattern.findall(primary_html)

    for dist_str, seat_suffix, rest in rows:
        dist_num = int(dist_str)
        tds = td_pattern.findall(rest)

        for col_idx, td_content in enumerate(tds):
            if col_idx == 0:
                party = 'D'
            elif col_idx == 1:
                party = 'R'
            else:
                party = 'Other'

            cands = cand_pattern.findall(td_content)
            for name_raw, suffix in cands:
                name = htmlmod.unescape(name_raw.strip())
                is_incumbent = '(i)' in suffix
                candidates.append((dist_num, name, party, is_incumbent, seat_suffix or None))

    return candidates

# ══════════════════════════════════════════════════════════════════════
# STEP 2b: Parse Statewide Votebox HTML → Extract Candidates
# ══════════════════════════════════════════════════════════════════════

def parse_statewide_primary_candidates(html_text):
    """
    Parse Ballotpedia statewide election votebox HTML and extract primary candidates.

    Statewide pages use a 'votebox' format (not candidateListTablePartisan).
    Structure: h4 headers ("Democratic primary election", "Republican primary election")
    followed by votebox divs with results_row entries.

    Returns list of (candidate_name: str, party: str, is_incumbent: bool)
    """
    # Find the candidates section — bounded by the anchor and the next <h2>
    cand_start = html_text.find('id="Candidates_and_election_results"')
    if cand_start == -1:
        cand_start = html_text.find('id="Candidates"')
    if cand_start == -1:
        return []

    # End at Past_elections or next <h2>
    end_pos = len(html_text)
    past = html_text.find('id="Past_elections"', cand_start + 100)
    if past > 0:
        end_pos = min(end_pos, past)
    # Also try the <h2> that wraps Past_elections (the span is inside the h2)
    for m in re.finditer(r'<h2>', html_text[cand_start + 100:]):
        end_pos = min(end_pos, cand_start + 100 + m.start())
        break

    section = html_text[cand_start:end_pos]

    candidates = []

    # Find h4 sections and parse each primary
    for h4_match in re.finditer(r'<h4>(.*?)</h4>', section):
        h4_text = re.sub(r'<[^>]+>', '', h4_match.group(1)).strip().lower()

        # Only parse primary sections
        if 'democratic primary' in h4_text:
            party = 'D'
        elif 'republican primary' in h4_text:
            party = 'R'
        else:
            continue

        # Find the subsection between this h4 and the next h4
        next_h4 = section.find('<h4>', h4_match.end())
        if next_h4 == -1:
            next_h4 = len(section)
        subsection = section[h4_match.end():next_h4]

        # Parse candidate rows from votebox results_table
        for row_match in re.finditer(
            r'<tr\s+class="results_row[^"]*"[^>]*>(.*?)</tr>',
            subsection, re.DOTALL
        ):
            row_html = row_match.group(1)

            # Extract name cell
            name_cell = re.search(
                r'class="votebox-results-cell--text"[^>]*>(.*?)</td>',
                row_html, re.DOTALL
            )
            if not name_cell:
                continue
            cell_html = name_cell.group(1)

            # Check for incumbent (bold+underline around the link)
            is_incumbent = bool(re.search(r'<b><u><a', cell_html))

            # Extract name from link
            name_link = re.search(r'<a\s+href="[^"]*"[^>]*>(.*?)</a>', cell_html)
            if not name_link:
                continue
            name = htmlmod.unescape(name_link.group(1).strip())

            candidates.append((name, party, is_incumbent))

    return candidates

# ══════════════════════════════════════════════════════════════════════
# STEP 3: Build DB Lookup Maps
# ══════════════════════════════════════════════════════════════════════

def build_lookup_maps(state_abbrev):
    """
    Query DB and build lookup maps for matching candidates to elections.

    Returns:
        seat_map: {(office_type, district_number_str) → seat_id} for single-seat
        multi_seat_map: {(office_type, district_number_str) → [seat_ids sorted by designator]}
        election_map: {seat_id → {'Primary_D': election_id, 'Primary_R': election_id, 'General': election_id}}
        incumbent_map: {seat_id → (candidate_id, full_name)}
    """
    # Load all seats for this state with 2026 elections
    seats = run_sql(f"""
        SELECT se.id as seat_id, se.office_type, se.seat_designator,
               d.district_number, d.num_seats
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}'
          AND se.next_regular_election_year = 2026
          AND se.selection_method = 'Elected'
          AND d.office_level = 'Legislative'
        ORDER BY se.office_type, d.district_number, se.seat_designator
    """)

    seat_map = {}       # (office_type, district_num) → seat_id (for single-seat districts)
    multi_seat_map = {} # (office_type, district_num) → [seat_ids]

    for s in seats:
        key = (s['office_type'], s['district_number'])
        if s['num_seats'] == 1:
            seat_map[key] = s['seat_id']
        else:
            if key not in multi_seat_map:
                multi_seat_map[key] = []
            multi_seat_map[key].append(s['seat_id'])

    # Load all elections for this state (2026)
    elections = run_sql(f"""
        SELECT e.id as election_id, e.seat_id, e.election_type
        FROM elections e
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}'
          AND e.election_year = 2026
          AND d.office_level = 'Legislative'
    """)

    election_map = defaultdict(dict)
    for e in elections:
        election_map[e['seat_id']][e['election_type']] = e['election_id']

    # Load current seat_terms (incumbents)
    incumbents = run_sql(f"""
        SELECT st.seat_id, st.candidate_id, c.full_name
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        JOIN candidates c ON st.candidate_id = c.id
        WHERE s.abbreviation = '{state_abbrev}'
          AND st.end_date IS NULL
          AND d.office_level = 'Legislative'
    """)

    incumbent_map = {}
    for inc in incumbents:
        incumbent_map[inc['seat_id']] = (inc['candidate_id'], inc['full_name'])

    return seat_map, multi_seat_map, election_map, incumbent_map

# ══════════════════════════════════════════════════════════════════════
# STEP 4: Match Candidates to Elections
# ══════════════════════════════════════════════════════════════════════

def strip_accents(s):
    """Remove diacritics/accents from a string (ñ→n, é→e, etc.)."""
    import unicodedata
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )

def name_similarity(name1, name2):
    """Name matching that handles suffixes, accents, and nicknames."""
    if name1 is None or name2 is None:
        return 0.0

    def normalize(n):
        # Remove suffixes like Jr., Sr., III, IV
        n = re.sub(r'\s+(Jr\.?|Sr\.?|III|IV|II)\s*$', '', n, flags=re.IGNORECASE)
        n = strip_accents(n)
        return n.strip().lower()

    n1 = normalize(name1)
    n2 = normalize(name2)

    if n1 == n2:
        return 1.0

    parts1 = n1.split()
    parts2 = n2.split()
    if not parts1 or not parts2:
        return 0.0

    # Last name match — compare last word, but also handle compound last names
    # e.g. "Caroline Harris" vs "Caroline Harris Davila"
    last1 = parts1[-1]
    last2 = parts2[-1]
    last_match = (last1 == last2)

    # Also check if one last name is contained in the other's name parts
    if not last_match:
        if last1 in parts2 or last2 in parts1:
            last_match = True

    if not last_match:
        return 0.0

    # First name or initial match
    first1 = parts1[0]
    first2 = parts2[0]
    if first1 == first2:
        return 0.9

    # One name starts with the other (Jeff/Jeffrey, Liz/Elizabeth handled below)
    if first1.startswith(first2) or first2.startswith(first1):
        return 0.8

    # Same first initial
    if first1[0] == first2[0]:
        return 0.7

    # Different first names but same last name — could be a nickname
    # (Mando/Armando, Jo/Jolanda, Cas/Cassandra, Lulu/Maria)
    return 0.3

def match_candidates(parsed_candidates, office_type, seat_map, multi_seat_map,
                     election_map, incumbent_map):
    """
    Match parsed candidates to DB elections.

    Returns:
        matched: list of {
            'election_id': int, 'candidate_id': int or None, 'candidate_name': str,
            'party': str, 'is_incumbent': bool, 'seat_id': int
        }
        unmatched: list of (district, name, party, reason)
    """
    matched = []
    unmatched = []

    # Group by district for multi-seat handling
    by_district = defaultdict(list)
    for entry in parsed_candidates:
        if len(entry) == 5:
            dist_num, name, party, is_incumbent, seat_suffix = entry
        else:
            dist_num, name, party, is_incumbent = entry
            seat_suffix = None
        by_district[dist_num].append((name, party, is_incumbent, seat_suffix))

    for dist_num in sorted(by_district.keys()):
        dist_str = str(dist_num)
        key = (office_type, dist_str)
        candidates_in_district = by_district[dist_num]

        # Find seat(s) for this district
        if key in seat_map:
            # Single seat — all candidates (D and R) compete for primaries linked to this seat
            seat_id = seat_map[key]
            elections = election_map.get(seat_id, {})
            incumbent_info = incumbent_map.get(seat_id)

            for name, party, bp_incumbent, _seat_suffix in candidates_in_district:
                if party == 'Other':
                    unmatched.append((dist_num, name, party, 'third_party_skipped'))
                    continue

                # Find election
                election_type = f'Primary_{party}'
                election_id = elections.get(election_type)
                if not election_id:
                    unmatched.append((dist_num, name, party, f'no_{election_type}_election'))
                    continue

                # Check incumbent match
                candidate_id = None
                is_inc = False
                if bp_incumbent and incumbent_info:
                    sim = name_similarity(name, incumbent_info[1])
                    if sim >= 0.3:
                        candidate_id = incumbent_info[0]
                        is_inc = True
                    else:
                        # BP says incumbent but name doesn't match our DB
                        # They might be marked (i) but running for a different seat
                        pass
                elif not bp_incumbent and incumbent_info:
                    # Check if this candidate IS the incumbent (BP might not mark them)
                    sim = name_similarity(name, incumbent_info[1])
                    if sim >= 0.7:
                        candidate_id = incumbent_info[0]
                        is_inc = True

                matched.append({
                    'election_id': election_id,
                    'candidate_id': candidate_id,
                    'candidate_name': name,
                    'party': party,
                    'is_incumbent': is_inc,
                    'seat_id': seat_id,
                })

        elif key in multi_seat_map:
            # Multi-seat district — each seat has its own elections
            seat_ids = multi_seat_map[key]

            # Build seat designator → seat_id map for direct routing
            # seat_ids are ordered by designator (A=0, B=1, etc.)
            desig_to_sid = {}
            for idx, sid in enumerate(seat_ids):
                desig_to_sid[chr(ord('A') + idx)] = sid

            # Check if parser provided seat suffixes (e.g., ID House "District 1A")
            has_seat_suffixes = any(ss for _, _, _, ss in candidates_in_district)

            # Group candidates by party
            d_cands = [(n, p, i, ss) for n, p, i, ss in candidates_in_district if p == 'D']
            r_cands = [(n, p, i, ss) for n, p, i, ss in candidates_in_district if p == 'R']

            for party_cands, party in [(d_cands, 'D'), (r_cands, 'R')]:
                election_type = f'Primary_{party}'

                if has_seat_suffixes:
                    # Direct routing: seat suffix tells us exactly which seat
                    for name, p, bp_incumbent, seat_suffix in party_cands:
                        sid = desig_to_sid.get(seat_suffix)
                        if not sid:
                            unmatched.append((dist_num, name, p, f'unknown_seat_{seat_suffix}'))
                            continue
                        election_id = election_map.get(sid, {}).get(election_type)
                        if not election_id:
                            unmatched.append((dist_num, name, p, f'no_{election_type}_election'))
                            continue

                        # Check incumbent match
                        inc_info = incumbent_map.get(sid)
                        cand_id = None
                        is_inc = False
                        if bp_incumbent and inc_info and name_similarity(name, inc_info[1]) >= 0.3:
                            cand_id = inc_info[0]
                            is_inc = True
                        elif inc_info and name_similarity(name, inc_info[1]) >= 0.7:
                            cand_id = inc_info[0]
                            is_inc = True

                        matched.append({
                            'election_id': election_id,
                            'candidate_id': cand_id,
                            'candidate_name': name,
                            'party': party,
                            'is_incumbent': is_inc,
                            'seat_id': sid,
                        })
                else:
                    # No seat suffixes — use heuristic assignment
                    # Count how many seats have an election for this party
                    seats_with_election = [(sid, election_map.get(sid, {}).get(election_type))
                                           for sid in seat_ids
                                           if election_map.get(sid, {}).get(election_type)]
                    one_seat_up = len(seats_with_election) == 1

                    # Incumbents get matched to their specific seat first
                    assigned_seats = set()
                    pending = []

                    for name, p, bp_incumbent, _ss in party_cands:
                        assigned = False
                        if bp_incumbent:
                            for sid in seat_ids:
                                if not one_seat_up and sid in assigned_seats:
                                    continue
                                inc_info = incumbent_map.get(sid)
                                if inc_info and name_similarity(name, inc_info[1]) >= 0.3:
                                    election_id = election_map.get(sid, {}).get(election_type)
                                    if election_id:
                                        matched.append({
                                            'election_id': election_id,
                                            'candidate_id': inc_info[0],
                                            'candidate_name': name,
                                            'party': party,
                                            'is_incumbent': True,
                                            'seat_id': sid,
                                        })
                                        if not one_seat_up:
                                            assigned_seats.add(sid)
                                        assigned = True
                                        break
                        if not assigned:
                            pending.append((name, p, bp_incumbent))

                    # Assign remaining candidates to available seats
                    for name, p, bp_incumbent in pending:
                        assigned = False
                        for sid in seat_ids:
                            if not one_seat_up and sid in assigned_seats:
                                continue
                            election_id = election_map.get(sid, {}).get(election_type)
                            if election_id:
                                inc_info = incumbent_map.get(sid)
                                cand_id = None
                                is_inc = False
                                if inc_info and name_similarity(name, inc_info[1]) >= 0.7:
                                    cand_id = inc_info[0]
                                    is_inc = True

                                matched.append({
                                    'election_id': election_id,
                                    'candidate_id': cand_id,
                                    'candidate_name': name,
                                    'party': p,
                                    'is_incumbent': is_inc,
                                    'seat_id': sid,
                                })
                                if not one_seat_up:
                                    assigned_seats.add(sid)
                                assigned = True
                                break

                        if not assigned:
                            unmatched.append((dist_num, name, p, 'no_available_seat'))

            # Handle other parties
            other_cands = [(n, p, i, ss) for n, p, i, ss in candidates_in_district if p == 'Other']
            for name, p, bp_inc, _ss in other_cands:
                unmatched.append((dist_num, name, p, 'third_party_skipped'))

        else:
            # No seats found for this district
            for name, party, bp_inc, _ss in candidates_in_district:
                unmatched.append((dist_num, name, party, 'no_seat_in_db'))

    return matched, unmatched

# ══════════════════════════════════════════════════════════════════════
# STEP 5-6: Insert Candidates and Candidacies
# ══════════════════════════════════════════════════════════════════════

def insert_candidacies(matched, dry_run=False, force=False):
    """
    Insert new candidates (challengers) and candidacy records.

    Returns (new_candidates_count, candidacies_count)
    """
    # When --force, filter out candidates who already have a candidacy in their election
    if force:
        election_ids = list(set(m['election_id'] for m in matched))
        ids_str = ','.join(str(eid) for eid in election_ids)
        existing = run_sql(f"""
            SELECT cy.election_id, cy.candidate_id, c.full_name
            FROM candidacies cy
            JOIN candidates c ON cy.candidate_id = c.id
            WHERE cy.election_id IN ({ids_str})
        """)
        existing_pairs = set()
        existing_names = defaultdict(set)  # election_id -> set of lowercase names
        if existing:
            existing_pairs = set((r['election_id'], r['candidate_id']) for r in existing)
            for r in existing:
                existing_names[r['election_id']].add(r['full_name'].lower().strip())
        if existing_pairs:
            before = len(matched)
            filtered = []
            for m in matched:
                if m['candidate_id'] is not None and (m['election_id'], m['candidate_id']) in existing_pairs:
                    continue  # exact candidate already in this election
                if m['candidate_id'] is None and m['candidate_name'].lower().strip() in existing_names.get(m['election_id'], set()):
                    continue  # name already in this election
                filtered.append(m)
            matched = filtered
            skipped = before - len(matched)
            if skipped:
                print(f"    --force: skipping {skipped} already-existing candidacies")
        if not matched:
            print(f"    No new candidacies to insert (all already populated)")
            return 0, 0

    # Separate incumbents (reuse candidate_id) from new candidates (need INSERT)
    reuse = [m for m in matched if m['candidate_id'] is not None]
    new = [m for m in matched if m['candidate_id'] is None]

    print(f"    Incumbent candidacies (reuse existing candidate): {len(reuse)}")
    print(f"    New candidate records needed: {len(new)}")

    if dry_run:
        return len(new), len(matched)

    # Insert new candidates
    new_candidate_ids = []
    if new:
        values = []
        for m in new:
            # Try to split name into first/last
            parts = m['candidate_name'].split()
            first = esc(parts[0]) if parts else ''
            last = esc(parts[-1]) if len(parts) > 1 else esc(parts[0]) if parts else ''
            full = esc(m['candidate_name'])
            values.append(f"('{full}', '{first}', '{last}', NULL)")

        total_inserted = 0
        for batch_start in range(0, len(values), BATCH_SIZE):
            batch = values[batch_start:batch_start + BATCH_SIZE]
            sql = (
                "INSERT INTO candidates (full_name, first_name, last_name, gender) VALUES\n"
                + ",\n".join(batch)
                + "\nRETURNING id;"
            )
            result = run_sql(sql, exit_on_error=False)
            if result is None:
                print(f"      Batch failed, retrying in 2s...")
                time.sleep(2)
                result = run_sql(sql)
            new_candidate_ids.extend(r['id'] for r in result)
            total_inserted += len(result)

        print(f"    Inserted {total_inserted} new candidates")
        if total_inserted != len(new):
            print(f"    ERROR: Expected {len(new)}, got {total_inserted}")
            sys.exit(1)

    # Assign new candidate_ids
    for i, m in enumerate(new):
        m['candidate_id'] = new_candidate_ids[i]

    # Insert all candidacies
    all_candidacies = reuse + new
    values = []
    for m in all_candidacies:
        values.append(
            f"({m['election_id']}, {m['candidate_id']}, '{esc(m['party'])}', "
            f"'Filed', {m['is_incumbent']}, false, NULL, NULL, NULL, NULL, 'Pending', NULL, NULL)"
        )

    total_inserted = 0
    for batch_start in range(0, len(values), BATCH_SIZE):
        batch = values[batch_start:batch_start + BATCH_SIZE]
        sql = (
            "INSERT INTO candidacies (election_id, candidate_id, party, "
            "candidate_status, is_incumbent, is_write_in, filing_date, "
            "withdrawal_date, votes_received, vote_percentage, result, "
            "endorsements, notes) VALUES\n"
            + ",\n".join(batch)
            + "\nRETURNING id;"
        )
        result = run_sql(sql, exit_on_error=False)
        if result is None:
            print(f"      Batch failed, retrying in 2s...")
            time.sleep(2)
            result = run_sql(sql)
        total_inserted += len(result)

    print(f"    Inserted {total_inserted} candidacies")
    if total_inserted != len(all_candidacies):
        print(f"    ERROR: Expected {len(all_candidacies)}, got {total_inserted}")
        sys.exit(1)

    return len(new), total_inserted

# ══════════════════════════════════════════════════════════════════════
# MAIN: Process a single state
# ══════════════════════════════════════════════════════════════════════

def process_state(state_abbrev, dry_run=False, force=False):
    """Process all chambers for a single state."""
    print(f"\n{'=' * 60}")
    print(f"PROCESSING: {state_abbrev}" + (" (FORCE)" if force else ""))
    print(f"{'=' * 60}")

    if state_abbrev not in STATE_CHAMBERS:
        print(f"  ERROR: No chamber config for {state_abbrev}")
        return False

    # Check for existing candidacies
    existing = run_sql(f"""
        SELECT COUNT(*) as cnt FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}' AND d.office_level = 'Legislative'
    """)
    if existing[0]['cnt'] > 0:
        if force:
            print(f"  NOTE: {state_abbrev} already has {existing[0]['cnt']} legislative candidacies.")
            print(f"  --force: will skip elections that already have candidacies.")
        else:
            print(f"  WARNING: {state_abbrev} already has {existing[0]['cnt']} legislative candidacies!")
            print(f"  Skipping to prevent duplicates. Use --force to fill gaps.")
            return False

    # Build lookup maps
    print(f"\n  Loading DB maps...")
    seat_map, multi_seat_map, election_map, incumbent_map = build_lookup_maps(state_abbrev)
    total_seats = len(seat_map) + sum(len(v) for v in multi_seat_map.values())
    print(f"  Seats with 2026 elections: {total_seats} "
          f"(single: {len(seat_map)}, multi-member: {sum(len(v) for v in multi_seat_map.values())})")
    print(f"  Seats with elections in map: {len(election_map)}")
    print(f"  Current incumbents: {len(incumbent_map)}")

    state_total_matched = 0
    state_total_unmatched = 0
    state_new_candidates = 0
    state_candidacies = 0

    for state_name, chamber_type, office_type in STATE_CHAMBERS[state_abbrev]:
        chamber_label = f"{state_abbrev} {chamber_type.replace('_', ' ')}"
        print(f"\n  --- {chamber_label} ---")

        # Download
        print(f"  Downloading Ballotpedia page...")
        html_text = download_bp_page(state_name, chamber_type)
        if not html_text:
            print(f"  SKIPPED: Could not download {chamber_label}")
            continue

        # Save for debugging
        fname = f"/tmp/bp_{state_abbrev.lower()}_{office_type.replace(' ', '_').lower()}.html"
        with open(fname, 'w', encoding='utf-8') as f:
            f.write(html_text)

        # Parse
        print(f"  Parsing candidates...")
        parsed = parse_primary_candidates(html_text)
        if not parsed:
            print(f"  WARNING: No candidates parsed from {chamber_label}")
            continue

        party_counts = Counter(c[2] for c in parsed)
        inc_count = sum(1 for c in parsed if c[3])
        districts = len(set(c[0] for c in parsed))
        print(f"  Parsed: {len(parsed)} candidates in {districts} districts")
        print(f"    D: {party_counts.get('D', 0)}, R: {party_counts.get('R', 0)}, "
              f"Other: {party_counts.get('Other', 0)}")
        print(f"    Incumbents (BP): {inc_count}")

        # Match
        print(f"  Matching to DB elections...")
        matched, unmatched = match_candidates(
            parsed, office_type, seat_map, multi_seat_map, election_map, incumbent_map
        )
        print(f"  Matched: {len(matched)}, Unmatched: {len(unmatched)}")

        if unmatched:
            reasons = Counter(u[3] for u in unmatched)
            for reason, count in reasons.most_common():
                print(f"    Unmatched ({reason}): {count}")
            # Show first few unmatched
            for dist, name, party, reason in unmatched[:5]:
                print(f"      District {dist}: {name} ({party}) — {reason}")

        state_total_matched += len(matched)
        state_total_unmatched += len(unmatched)

        # Insert
        if matched:
            print(f"  Inserting candidacies...")
            new_cands, cand_count = insert_candidacies(matched, dry_run=dry_run, force=force)
            state_new_candidates += new_cands
            state_candidacies += cand_count

    # Summary
    print(f"\n  {'=' * 40}")
    print(f"  {state_abbrev} SUMMARY:")
    print(f"    Total matched: {state_total_matched}")
    print(f"    Total unmatched: {state_total_unmatched}")
    print(f"    New candidates created: {state_new_candidates}")
    print(f"    Candidacies inserted: {state_candidacies}")

    return True

# ══════════════════════════════════════════════════════════════════════
# VERIFICATION
# ══════════════════════════════════════════════════════════════════════

def verify_state(state_abbrev):
    """Run verification queries for a processed state."""
    print(f"\n  VERIFICATION for {state_abbrev}:")

    # Candidacy counts by party
    result = run_sql(f"""
        SELECT ca.party, COUNT(*) as cnt
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}' AND d.office_level = 'Legislative'
        GROUP BY ca.party ORDER BY cnt DESC
    """)
    for r in result:
        print(f"    {r['party']}: {r['cnt']}")

    # Candidacies by election type
    result2 = run_sql(f"""
        SELECT e.election_type, COUNT(*) as cnt
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}' AND d.office_level = 'Legislative'
        GROUP BY e.election_type ORDER BY e.election_type
    """)
    for r in result2:
        print(f"    {r['election_type']}: {r['cnt']}")

    # Incumbent count
    result3 = run_sql(f"""
        SELECT COUNT(*) as cnt FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}' AND ca.is_incumbent = true
          AND d.office_level = 'Legislative'
    """)
    print(f"    Incumbents running: {result3[0]['cnt']}")

    # Duplicate check
    result4 = run_sql(f"""
        SELECT ca.election_id, ca.candidate_id, COUNT(*) as cnt
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}' AND d.office_level = 'Legislative'
        GROUP BY ca.election_id, ca.candidate_id
        HAVING COUNT(*) > 1
    """)
    if result4:
        print(f"    WARNING: {len(result4)} duplicate candidacies!")
    else:
        print(f"    No duplicate candidacies!")

    # Spot checks
    result5 = run_sql(f"""
        SELECT se.seat_label, c.full_name, ca.party, ca.is_incumbent, e.election_type
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN candidates c ON ca.candidate_id = c.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}' AND d.office_level = 'Legislative'
        ORDER BY RANDOM() LIMIT 8
    """)
    print(f"    Spot checks:")
    for r in result5:
        inc = " (i)" if r['is_incumbent'] else ""
        print(f"      {r['seat_label']}: {r['full_name']}{inc} [{r['party']}] → {r['election_type']}")

# ══════════════════════════════════════════════════════════════════════
# STATEWIDE: Process statewide races for a single state
# ══════════════════════════════════════════════════════════════════════

def process_state_statewide(state_abbrev, dry_run=False):
    """Process statewide races for a single state."""
    print(f"\n{'=' * 60}")
    print(f"STATEWIDE: {state_abbrev}")
    print(f"{'=' * 60}")

    if state_abbrev not in STATEWIDE_PAGES:
        print(f"  No statewide races in 2026 for {state_abbrev}. Skipping.")
        return False

    # Check for existing statewide candidacies
    existing = run_sql(f"""
        SELECT COUNT(*) as cnt FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}' AND d.office_level = 'Statewide'
    """)
    if existing[0]['cnt'] > 0:
        print(f"  WARNING: {state_abbrev} already has {existing[0]['cnt']} statewide candidacies!")
        print(f"  Skipping to prevent duplicates.")
        return False

    # Build statewide lookup maps
    print(f"\n  Loading statewide DB maps...")
    seats = run_sql(f"""
        SELECT se.id as seat_id, se.office_type, se.current_holder, se.current_holder_party
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}'
          AND d.office_level = 'Statewide'
          AND se.next_regular_election_year = 2026
          AND se.selection_method = 'Elected'
    """)
    # office_type → seat_id
    sw_seat_map = {}
    for s in seats:
        sw_seat_map[s['office_type']] = s['seat_id']
    print(f"  Statewide seats up in 2026: {len(sw_seat_map)}")
    for ot, sid in sorted(sw_seat_map.items()):
        print(f"    {ot}: seat_id={sid}")

    # Load elections for these seats
    seat_ids_str = ','.join(str(sid) for sid in sw_seat_map.values())
    elections = run_sql(f"""
        SELECT e.id as election_id, e.seat_id, e.election_type
        FROM elections e
        WHERE e.seat_id IN ({seat_ids_str}) AND e.election_year = 2026
    """)
    sw_election_map = defaultdict(dict)
    for e in elections:
        sw_election_map[e['seat_id']][e['election_type']] = e['election_id']

    # Load incumbents (from seat_terms)
    incumbents = run_sql(f"""
        SELECT st.seat_id, st.candidate_id, c.full_name
        FROM seat_terms st
        JOIN candidates c ON st.candidate_id = c.id
        WHERE st.seat_id IN ({seat_ids_str}) AND st.end_date IS NULL
    """)
    sw_incumbent_map = {}
    for inc in incumbents:
        sw_incumbent_map[inc['seat_id']] = (inc['candidate_id'], inc['full_name'])

    state_name = STATE_FULL_NAMES[state_abbrev]
    state_total_matched = 0
    state_total_unmatched = 0
    state_new_candidates = 0
    state_candidacies = 0

    for url_template, office_types in STATEWIDE_PAGES[state_abbrev]:
        url_path = url_template.format(s=state_name)
        page_label = ', '.join(office_types)
        print(f"\n  --- {page_label} ---")

        # Download
        url = f'https://ballotpedia.org/{url_path}'
        print(f"  Downloading {url_path}...")
        try:
            resp = httpx.get(
                url,
                headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'},
                follow_redirects=True, timeout=30
            )
            if resp.status_code != 200:
                print(f"    WARNING: HTTP {resp.status_code}, skipping")
                continue
            html_text = resp.text
        except Exception as e:
            print(f"    WARNING: Download failed: {e}")
            continue

        # Save for debugging
        fname = f"/tmp/bp_{state_abbrev.lower()}_sw_{'_'.join(o.replace(' ', '').replace('.', '') for o in office_types).lower()}.html"
        with open(fname, 'w', encoding='utf-8') as f:
            f.write(html_text)

        # Parse primary candidates
        parsed = parse_statewide_primary_candidates(html_text)
        if not parsed:
            print(f"    No primary candidates found")
            continue

        party_counts = Counter(c[1] for c in parsed)
        inc_count = sum(1 for c in parsed if c[2])
        print(f"  Parsed: {len(parsed)} primary candidates")
        print(f"    D: {party_counts.get('D', 0)}, R: {party_counts.get('R', 0)}")
        print(f"    Incumbents (BP): {inc_count}")

        # For joint pages (Gov+LtGov), we need to figure out which candidates
        # are for which office. Joint pages have separate votebox sections per office.
        # But our simple parser returns all candidates together.
        # For joint pages, parse per-office by looking at votebox headers.
        if len(office_types) > 1:
            # Re-parse more carefully: split by office
            parsed_by_office = _parse_joint_statewide(html_text, office_types)
        else:
            parsed_by_office = {office_types[0]: parsed}

        for office_type, office_parsed in parsed_by_office.items():
            if not office_parsed:
                print(f"    {office_type}: no candidates")
                continue

            seat_id = sw_seat_map.get(office_type)
            if not seat_id:
                print(f"    WARNING: No seat for {office_type} in DB, skipping {len(office_parsed)} candidates")
                state_total_unmatched += len(office_parsed)
                continue

            elections = sw_election_map.get(seat_id, {})
            incumbent_info = sw_incumbent_map.get(seat_id)

            matched = []
            unmatched_count = 0

            for name, party, bp_incumbent in office_parsed:
                election_type = f'Primary_{party}'
                election_id = elections.get(election_type)
                if not election_id:
                    unmatched_count += 1
                    print(f"    WARNING: No {election_type} election for {office_type}, skipping {name}")
                    continue

                candidate_id = None
                is_inc = False
                if incumbent_info:
                    sim = name_similarity(name, incumbent_info[1])
                    if bp_incumbent and sim >= 0.3:
                        candidate_id = incumbent_info[0]
                        is_inc = True
                    elif not bp_incumbent and sim >= 0.7:
                        candidate_id = incumbent_info[0]
                        is_inc = True

                matched.append({
                    'election_id': election_id,
                    'candidate_id': candidate_id,
                    'candidate_name': name,
                    'party': party,
                    'is_incumbent': is_inc,
                    'seat_id': seat_id,
                })

            p_counts = Counter(m['party'] for m in matched)
            inc_matched = sum(1 for m in matched if m['is_incumbent'])
            print(f"    {office_type}: {len(matched)} matched (D:{p_counts.get('D',0)}, R:{p_counts.get('R',0)}, "
                  f"inc:{inc_matched})")

            state_total_matched += len(matched)
            state_total_unmatched += unmatched_count

            if matched:
                new_cands, cand_count = insert_candidacies(matched, dry_run=dry_run)
                state_new_candidates += new_cands
                state_candidacies += cand_count

        # Small delay between page downloads
        time.sleep(0.5)

    # Summary
    print(f"\n  {'=' * 40}")
    print(f"  {state_abbrev} STATEWIDE SUMMARY:")
    print(f"    Total matched: {state_total_matched}")
    print(f"    Total unmatched: {state_total_unmatched}")
    print(f"    New candidates created: {state_new_candidates}")
    print(f"    Candidacies inserted: {state_candidacies}")

    return state_total_matched > 0

def _parse_joint_statewide(html_text, office_types):
    """
    Parse a joint statewide page (e.g., Governor + Lt. Governor).

    These pages have multiple votebox sections, each with a header like
    "Democratic primary election for Governor of Ohio".
    We split candidates by matching the election_label to the office.
    """
    cand_start = html_text.find('id="Candidates_and_election_results"')
    if cand_start == -1:
        cand_start = html_text.find('id="Candidates"')
    if cand_start == -1:
        return {ot: [] for ot in office_types}

    end_pos = len(html_text)
    past = html_text.find('id="Past_elections"', cand_start + 100)
    if past > 0:
        end_pos = min(end_pos, past)
    for m in re.finditer(r'<h2>', html_text[cand_start + 100:]):
        end_pos = min(end_pos, cand_start + 100 + m.start())
        break

    section = html_text[cand_start:end_pos]
    result = {ot: [] for ot in office_types}

    # Find each votebox with a race_header
    for race_match in re.finditer(
        r'<div\s+class="race_header\s+(\w+)"[^>]*>(.*?)</div>',
        section, re.DOTALL
    ):
        party_class = race_match.group(1).lower()

        # Map party_class to party letter
        if party_class == 'democratic':
            party = 'D'
        elif party_class == 'republican':
            party = 'R'
        else:
            continue

        header_html = race_match.group(2)
        label_match = re.search(r'<h[35][^>]*>(.*?)</h[35]>', header_html, re.DOTALL)
        label = re.sub(r'<[^>]+>', '', label_match.group(1)).strip().lower() if label_match else ''

        # Only process primary sections
        if 'primary' not in label:
            continue

        # Determine which office this votebox is for
        target_office = None
        if 'governor' in label and 'lieutenant' not in label:
            target_office = 'Governor'
        elif 'lieutenant' in label:
            target_office = 'Lt. Governor'
        else:
            # Fallback: use first unmatched office
            for ot in office_types:
                if ot.lower() in label:
                    target_office = ot
                    break

        if target_office not in result:
            continue

        # Find results_rows between this header and the next race_header
        # Look ahead for next race_header
        next_race = re.search(
            r'<div\s+class="race_header', section[race_match.end():]
        )
        if next_race:
            sub_end = race_match.end() + next_race.start()
        else:
            sub_end = len(section)
        subsection = section[race_match.end():sub_end]

        for row_match in re.finditer(
            r'<tr\s+class="results_row[^"]*"[^>]*>(.*?)</tr>',
            subsection, re.DOTALL
        ):
            row_html = row_match.group(1)
            name_cell = re.search(
                r'class="votebox-results-cell--text"[^>]*>(.*?)</td>',
                row_html, re.DOTALL
            )
            if not name_cell:
                continue
            cell_html = name_cell.group(1)
            is_incumbent = bool(re.search(r'<b><u><a', cell_html))
            name_link = re.search(r'<a\s+href="[^"]*"[^>]*>(.*?)</a>', cell_html)
            if not name_link:
                continue
            name = htmlmod.unescape(name_link.group(1).strip())
            result[target_office].append((name, party, is_incumbent))

    return result

def verify_state_statewide(state_abbrev):
    """Run verification queries for statewide candidacies."""
    print(f"\n  STATEWIDE VERIFICATION for {state_abbrev}:")

    result = run_sql(f"""
        SELECT se.office_type, ca.party, COUNT(*) as cnt,
               SUM(CASE WHEN ca.is_incumbent THEN 1 ELSE 0 END) as inc_cnt
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}' AND d.office_level = 'Statewide'
        GROUP BY se.office_type, ca.party
        ORDER BY se.office_type, ca.party
    """)
    if not result:
        print(f"    No statewide candidacies found")
        return

    current_office = None
    for r in result:
        if r['office_type'] != current_office:
            current_office = r['office_type']
            print(f"    {current_office}:")
        inc_str = f" ({r['inc_cnt']} inc)" if r['inc_cnt'] > 0 else ""
        print(f"      {r['party']}: {r['cnt']}{inc_str}")

    # Duplicate check
    dupes = run_sql(f"""
        SELECT ca.election_id, ca.candidate_id, COUNT(*) as cnt
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}' AND d.office_level = 'Statewide'
        GROUP BY ca.election_id, ca.candidate_id HAVING COUNT(*) > 1
    """)
    if dupes:
        print(f"    WARNING: {len(dupes)} duplicate candidacies!")
    else:
        print(f"    No duplicate candidacies!")

    # Spot checks
    spots = run_sql(f"""
        SELECT se.office_type, c.full_name, ca.party, ca.is_incumbent, e.election_type
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN candidates c ON ca.candidate_id = c.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}' AND d.office_level = 'Statewide'
        ORDER BY RANDOM() LIMIT 6
    """)
    print(f"    Spot checks:")
    for r in spots:
        inc = " (i)" if r['is_incumbent'] else ""
        print(f"      {r['office_type']}: {r['full_name']}{inc} [{r['party']}] → {r['election_type']}")

# ══════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Populate candidacies from Ballotpedia')
    parser.add_argument('--state', type=str, help='Process a single state (e.g., TX)')
    parser.add_argument('--all-closed', action='store_true',
                        help='Process all states with closed filing deadlines')
    parser.add_argument('--statewide', action='store_true',
                        help='Process statewide races (instead of legislative)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and match only, no database inserts')
    parser.add_argument('--force', action='store_true',
                        help='Re-run on states with existing candidacies (skips already-populated elections)')
    args = parser.parse_args()

    if not args.state and not args.all_closed:
        parser.error('Specify --state XX or --all-closed')

    if args.dry_run:
        print("DRY RUN MODE — no database changes will be made.\n")
    if args.force:
        print("FORCE MODE — will skip already-populated elections.\n")

    # Determine which states to process
    if args.all_closed:
        states = list(CLOSED_FILING_STATES.keys())
        print(f"Processing all {len(states)} closed-filing states: {', '.join(states)}")
    else:
        st = args.state.upper()
        if st not in CLOSED_FILING_STATES:
            print(f"WARNING: {st} is not in closed-filing list. Proceeding anyway...")
        states = [st]

    if args.statewide:
        # Process statewide races
        for st in states:
            success = process_state_statewide(st, dry_run=args.dry_run)
            if success and not args.dry_run:
                verify_state_statewide(st)
            if len(states) > 1:
                time.sleep(1)
    else:
        # Process legislative races
        for st in states:
            success = process_state(st, dry_run=args.dry_run, force=args.force)
            if success and not args.dry_run:
                verify_state(st)
            if len(states) > 1:
                time.sleep(1)

    # Final summary
    if not args.dry_run:
        print(f"\n{'=' * 60}")
        print(f"FINAL SUMMARY")
        print(f"{'=' * 60}")
        overall = run_sql("SELECT COUNT(*) as cnt FROM candidacies")
        print(f"Total candidacies in DB: {overall[0]['cnt']}")
        overall_cands = run_sql("SELECT COUNT(*) as cnt FROM candidates")
        print(f"Total candidates in DB: {overall_cands[0]['cnt']}")

        # Breakdown by level
        by_level = run_sql("""
            SELECT d.office_level, COUNT(*) as cnt
            FROM candidacies ca
            JOIN elections e ON ca.election_id = e.id
            JOIN seats se ON e.seat_id = se.id
            JOIN districts d ON se.district_id = d.id
            GROUP BY d.office_level
        """)
        for r in by_level:
            print(f"  {r['office_level']}: {r['cnt']}")

    print("\nDone!")

if __name__ == '__main__':
    main()
