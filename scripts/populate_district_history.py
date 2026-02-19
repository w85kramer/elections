"""
Populate historical election results from parsed Ballotpedia district data.

Reads /tmp/district_history/{state}.json (from download_district_history.py),
matches districts to DB seats, creates/matches candidates, and inserts
historical elections + candidacies.

Usage:
    python3 scripts/populate_district_history.py --state AK --dry-run
    python3 scripts/populate_district_history.py --state AK
    python3 scripts/populate_district_history.py --state AK --skip-candidates
"""
import sys
import os
import re
import json
import time
import argparse
import unicodedata

import httpx
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

INPUT_DIR = '/tmp/district_history'
BATCH_SIZE = 50

# ══════════════════════════════════════════════════════════════════════
# DB HELPERS
# ══════════════════════════════════════════════════════════════════════

def run_sql(query, max_retries=5, exit_on_error=True):
    """Execute SQL via Supabase Management API with retry."""
    for attempt in range(max_retries):
        resp = httpx.post(
            f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120,
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f'    Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR ({resp.status_code}): {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None
    print(f'SQL FAILED after {max_retries} retries')
    if exit_on_error:
        sys.exit(1)
    return None


def esc(s):
    """Escape single quotes for SQL."""
    if s is None:
        return None
    return str(s).replace("'", "''")


def sql_val(v):
    """Convert Python value to SQL literal."""
    if v is None:
        return 'NULL'
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, (int, float)):
        return str(v)
    return f"'{esc(v)}'"


# ══════════════════════════════════════════════════════════════════════
# NAME MATCHING (from audit_seat_gaps.py)
# ══════════════════════════════════════════════════════════════════════

def normalize_name(name):
    """Normalize a name for comparison."""
    if not name:
        return ''
    name = name.strip()
    name = re.sub(r'\s+(Jr\.?|Sr\.?|III|II|IV)$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+[A-Z]\.\s+', ' ', name)
    name = re.sub(r'^[A-Z]\.\s+', '', name)
    name = re.sub(r'"[^"]*"', '', name)
    name = re.sub(r"'[^']*'", '', name)
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    name = name.replace('\u2018', "'").replace('\u2019', "'")
    name = name.replace('\u201c', '"').replace('\u201d', '"')
    name = re.sub(r'\.', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name.lower()


NICKNAMES = {
    'william': ['bill', 'will', 'billy', 'willy'],
    'robert': ['bob', 'bobby', 'rob'],
    'richard': ['dick', 'rick', 'rich'],
    'james': ['jim', 'jimmy', 'jamie'],
    'john': ['jack', 'johnny', 'jay'],
    'joseph': ['joe', 'joey'],
    'thomas': ['tom', 'tommy'],
    'charles': ['charlie', 'chuck', 'chaz'],
    'edward': ['ed', 'eddie', 'ted', 'teddy'],
    'michael': ['mike', 'mikey', 'doc'],
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
    'antonio': ['tony'],
    'philip': ['phil', 'griff'],
    'phillip': ['phil'],
    'suzanne': ['sue', 'suzy'],
    'cynthia': ['cindy'],
    'christine': ['tina', 'chris'],
    'susan': ['sue'],
    'roberto': ['bobby'],
    'jonathan': ['jack'],
    'alexandra': ['ali', 'alex'],
    'jacob': ['jake'],
    'jessica': ['jess'],
    'melissa': ['missy'],
}

_NICKNAME_GROUPS = {}
for _formal, _nicks in NICKNAMES.items():
    _group = frozenset([_formal] + _nicks)
    _NICKNAME_GROUPS[_formal] = _group
    for _n in _nicks:
        _NICKNAME_GROUPS[_n] = _group


def nicknames_match(name1, name2):
    if name1 == name2:
        return True
    g1 = _NICKNAME_GROUPS.get(name1)
    if g1 and name2 in g1:
        return True
    g2 = _NICKNAME_GROUPS.get(name2)
    if g2 and name1 in g2:
        return True
    return False


def names_match(name1, name2):
    """Check if two names refer to the same person."""
    if not name1 or not name2:
        return False

    n1 = normalize_name(name1)
    n2 = normalize_name(name2)

    if n1 == n2:
        return True

    parts1 = n1.split()
    parts2 = n2.split()

    if not parts1 or not parts2:
        return False

    # Last name matching
    last1 = parts1[-1]
    last2 = parts2[-1]
    lname_match = (last1 == last2)
    if not lname_match:
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
    if nicknames_match(first1, first2):
        return True
    if len(first1) <= 2 and first2.startswith(first1.rstrip('.')):
        return True
    if len(first2) <= 2 and first1.startswith(first2.rstrip('.')):
        return True
    if len(first1) >= 3 and len(first2) >= 3 and first1[:3] == first2[:3]:
        return True

    return False


def split_name(full_name):
    """Split a full name into first_name and last_name."""
    parts = full_name.strip().split()
    if len(parts) == 0:
        return ('', '')
    if len(parts) == 1:
        return ('', parts[0])
    # Handle suffixes
    suffixes = {'jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv'}
    last_parts = [parts[-1]]
    if parts[-1].lower().rstrip('.') in suffixes and len(parts) > 2:
        last_parts = [parts[-2], parts[-1]]
        first_parts = parts[:-2]
    else:
        first_parts = parts[:-1]
    return (' '.join(first_parts), ' '.join(last_parts))


# ══════════════════════════════════════════════════════════════════════
# DISTRICT MATCHING
# ══════════════════════════════════════════════════════════════════════

def load_db_seats(state):
    """Load all legislative seats for a state from DB."""
    sql = f"""
    SELECT
        s.id AS seat_id,
        s.seat_label,
        s.current_holder,
        s.current_holder_party,
        s.seat_designator,
        d.id AS district_id,
        d.chamber,
        d.district_number,
        d.district_name,
        d.num_seats
    FROM seats s
    JOIN districts d ON s.district_id = d.id
    JOIN states st ON d.state_id = st.id
    WHERE st.abbreviation = '{state}'
      AND d.office_level = 'Legislative'
      AND s.selection_method = 'Elected'
    ORDER BY d.chamber, d.district_number, s.seat_designator
    """
    return run_sql(sql)


def load_existing_elections(state):
    """Load existing elections for a state to avoid duplicates."""
    sql = f"""
    SELECT e.id, e.seat_id, e.election_date, e.election_year, e.election_type
    FROM elections e
    JOIN seats s ON e.seat_id = s.id
    JOIN districts d ON s.district_id = d.id
    JOIN states st ON d.state_id = st.id
    WHERE st.abbreviation = '{state}'
      AND e.election_year < 2026
    """
    return run_sql(sql)


def match_districts(state, parsed_districts, db_seats):
    """
    Match BP district records to DB seats.

    Returns list of matched dicts:
      {bp_district, seat_id, seat_label, district_number, ...}
    """
    # Index DB seats by (chamber, district_number)
    # For multi-member districts, there may be multiple seats per district
    seats_by_key = {}
    for seat in db_seats:
        key = (seat['chamber'], seat['district_number'])
        if key not in seats_by_key:
            seats_by_key[key] = []
        seats_by_key[key].append(seat)

    # Also index by district_name for named-district states
    seats_by_name = {}
    for seat in db_seats:
        if seat['district_name']:
            key = (seat['chamber'], seat['district_name'])
            if key not in seats_by_name:
                seats_by_name[key] = []
            seats_by_name[key].append(seat)

    matched = []
    unmatched = []

    for dist in parsed_districts:
        chamber = dist['chamber']
        dist_id = dist['district_identifier']

        # Try direct match by district_number
        key = (chamber, dist_id)
        seats = seats_by_key.get(key)

        if not seats:
            # Try numeric normalization (strip leading zeros)
            if dist_id.isdigit():
                norm = str(int(dist_id))
                seats = seats_by_key.get((chamber, norm))

        if not seats and state == 'AK' and chamber == 'Senate':
            # AK Senate uses letters A-T → map to numbers 1-20
            if len(dist_id) == 1 and dist_id.isalpha():
                num = str(ord(dist_id.upper()) - ord('A') + 1)
                seats = seats_by_key.get((chamber, num))

        if not seats and state == 'MN' and chamber == 'House':
            # MN House: "1A" → DB "1", "1B" → DB "2"
            m = re.match(r'^(\d+)([AB])$', dist_id)
            if m:
                senate_num = int(m.group(1))
                suffix = m.group(2)
                db_num = str(2 * senate_num - 1) if suffix == 'A' else str(2 * senate_num)
                seats = seats_by_key.get((chamber, db_num))

        # Try matching by district_name
        if not seats:
            key_name = (chamber, dist_id)
            seats = seats_by_name.get(key_name)

        if not seats:
            # Try with spaces replaced, common variations
            variations = [
                dist_id.replace('-', ' '),
                dist_id.replace(' ', '-'),
                re.sub(r'\s+District$', '', dist_id),
            ]
            for var in variations:
                seats = seats_by_name.get((chamber, var))
                if seats:
                    break

        if not seats:
            # Try matching district_name contains the identifier
            for seat in db_seats:
                if seat['chamber'] != chamber:
                    continue
                if seat['district_name'] and dist_id.lower() in seat['district_name'].lower():
                    seats = [seat]
                    break

        if seats:
            # For single-member districts, use the first (only) seat
            # For multi-member districts, use the first seat (elections are per-seat but
            # historical data is per-district, so we'll link to seat with designator 'A' or first)
            seat = seats[0]
            matched.append({
                **dist,
                'seat_id': seat['seat_id'],
                'seat_label': seat['seat_label'],
                'district_number': seat['district_number'],
                'district_name': seat['district_name'],
                'num_seats': seat['num_seats'],
                'all_seats': seats,
            })
        else:
            unmatched.append(dist)

    return matched, unmatched


# ══════════════════════════════════════════════════════════════════════
# CANDIDATE MATCHING/CREATION
# ══════════════════════════════════════════════════════════════════════

# Session cache for candidate lookups: name → candidate_id
_candidate_cache = {}
# All candidates by last_name for in-memory matching: last_name → [{id, full_name}]
_candidates_by_last = {}


def find_or_create_candidate(name, dry_run=False, skip_create=False):
    """
    Find an existing candidate by name, or create a new one.

    Returns candidate_id or None.
    """
    if not name or len(name.strip()) < 2:
        return None

    name = name.strip()

    # Check session cache first
    cache_key = normalize_name(name)
    if cache_key in _candidate_cache:
        return _candidate_cache[cache_key]

    # Search in-memory by last name (no DB query needed)
    first, last = split_name(name)
    if not last:
        return None

    last_lower = last.lower()
    candidates = _candidates_by_last.get(last_lower, [])
    for r in candidates:
        if names_match(name, r['full_name']):
            _candidate_cache[cache_key] = r['id']
            return r['id']

    if skip_create:
        return None

    if dry_run:
        _candidate_cache[cache_key] = -1
        return -1

    # Create new candidate
    result = run_sql(
        f"INSERT INTO candidates (full_name, first_name, last_name) "
        f"VALUES ('{esc(name)}', '{esc(first)}', '{esc(last)}') "
        f"RETURNING id",
        exit_on_error=False
    )
    if result and len(result) > 0:
        cid = result[0]['id']
        _candidate_cache[cache_key] = cid
        # Add to in-memory index
        _candidates_by_last.setdefault(last_lower, []).append({'id': cid, 'full_name': name})
        return cid

    return None


def preload_candidate_cache(state):
    """Preload ALL candidates into memory for fast in-memory matching."""
    global _candidates_by_last

    # Load all candidates (not just state-specific) since historical
    # candidates may not have existing candidacies in this state
    sql = "SELECT id, full_name, last_name FROM candidates ORDER BY id"
    results = run_sql(sql, exit_on_error=False)
    if results:
        for r in results:
            key = normalize_name(r['full_name'])
            _candidate_cache[key] = r['id']
            last_lower = (r['last_name'] or '').lower()
            _candidates_by_last.setdefault(last_lower, []).append({
                'id': r['id'],
                'full_name': r['full_name']
            })
    print(f'  Preloaded {len(_candidate_cache)} candidates into cache ({len(_candidates_by_last)} unique last names)')


# ══════════════════════════════════════════════════════════════════════
# INSERT ELECTIONS + CANDIDACIES
# ══════════════════════════════════════════════════════════════════════

def insert_elections(matched_districts, existing_elections, dry_run=False, skip_candidates=False):
    """Insert historical elections and candidacies.

    Optimized to batch operations per district:
    1. Batch-create new candidates (1 API call)
    2. Batch-insert all elections for the district (1 API call with RETURNING)
    3. Batch-insert all candidacies (1 API call)
    ~3 API calls per district instead of ~10+.
    """
    # Build set of existing elections for dedup
    existing_keys = set()
    existing_year_type_keys = set()
    for e in existing_elections:
        existing_keys.add((e['seat_id'], e['election_date'], e['election_type']))
        existing_year_type_keys.add((e['seat_id'], e['election_year'], e['election_type']))

    stats = {
        'elections_inserted': 0,
        'elections_skipped': 0,
        'candidacies_inserted': 0,
        'candidates_created': 0,
        'candidates_matched': 0,
    }

    for dist in matched_districts:
        seat_id = dist['seat_id']

        # Phase 1: Filter to new elections only
        new_elections = []
        for election in dist.get('elections', []):
            year = election['year']
            etype = election['election_type']
            edate = election.get('election_date')

            if (seat_id, edate, etype) in existing_keys:
                stats['elections_skipped'] += 1
                continue
            if (seat_id, year, etype) in existing_year_type_keys:
                stats['elections_skipped'] += 1
                continue

            candidates = election.get('candidates', [])
            if not candidates:
                continue

            new_elections.append(election)

        if not new_elections:
            continue

        # Phase 2: Find/create all candidates needed for this district's new elections
        # Collect unique candidate names
        all_cand_names = set()
        for election in new_elections:
            for c in election.get('candidates', []):
                cname = c.get('name', '').strip()
                if cname and len(cname) >= 2:
                    all_cand_names.add(cname)

        # Resolve existing candidates from cache, collect names needing creation
        names_needing_creation = []
        for cname in all_cand_names:
            cache_key = normalize_name(cname)
            if cache_key in _candidate_cache:
                stats['candidates_matched'] += 1
                continue
            first, last = split_name(cname)
            if not last:
                continue
            last_lower = last.lower()
            candidates_list = _candidates_by_last.get(last_lower, [])
            found = False
            for r in candidates_list:
                if names_match(cname, r['full_name']):
                    _candidate_cache[cache_key] = r['id']
                    stats['candidates_matched'] += 1
                    found = True
                    break
            if not found and not skip_candidates:
                names_needing_creation.append(cname)

        # Batch-create new candidates (single SQL with multi-row INSERT RETURNING)
        if names_needing_creation and not dry_run:
            for batch_start in range(0, len(names_needing_creation), BATCH_SIZE):
                batch = names_needing_creation[batch_start:batch_start + BATCH_SIZE]
                values = []
                for cname in batch:
                    first, last = split_name(cname)
                    values.append(f"('{esc(cname)}', '{esc(first)}', '{esc(last)}')")
                create_sql = (
                    "INSERT INTO candidates (full_name, first_name, last_name) VALUES\n"
                    + ",\n".join(values)
                    + "\nRETURNING id, full_name"
                )
                result = run_sql(create_sql, exit_on_error=False)
                if result:
                    for r in result:
                        cid = r['id']
                        fname = r['full_name']
                        cache_key = normalize_name(fname)
                        _candidate_cache[cache_key] = cid
                        first, last = split_name(fname)
                        last_lower = last.lower()
                        _candidates_by_last.setdefault(last_lower, []).append({
                            'id': cid, 'full_name': fname
                        })
                        stats['candidates_created'] += 1
        elif names_needing_creation and dry_run:
            for cname in names_needing_creation:
                cache_key = normalize_name(cname)
                _candidate_cache[cache_key] = -1
                stats['candidates_created'] += 1

        if dry_run:
            stats['elections_inserted'] += len(new_elections)
            for election in new_elections:
                stats['candidacies_inserted'] += len(election.get('candidates', []))
            continue

        # Phase 3: Batch-insert all elections for this district
        election_values = []
        election_meta = []  # Track (date, type) for mapping returned IDs
        for election in new_elections:
            year = election['year']
            etype = election['election_type']
            edate = election.get('election_date')
            total_votes = election.get('total_votes')
            is_open = not any(c.get('incumbent') for c in election.get('candidates', []))

            election_values.append(
                f"({seat_id}, {sql_val(edate)}, {year}, '{esc(etype)}', "
                f"{sql_val(total_votes)}, 'Certified', {sql_val(is_open)})"
            )
            election_meta.append((edate, etype, election))

        # Insert elections in batches, getting back IDs
        election_id_map = {}  # (edate, etype) → election_id
        for batch_start in range(0, len(election_values), BATCH_SIZE):
            val_batch = election_values[batch_start:batch_start + BATCH_SIZE]
            meta_batch = election_meta[batch_start:batch_start + BATCH_SIZE]

            insert_sql = (
                "INSERT INTO elections "
                "(seat_id, election_date, election_year, election_type, "
                "total_votes_cast, result_status, is_open_seat) VALUES\n"
                + ",\n".join(val_batch)
                + "\nRETURNING id, election_date, election_type"
            )
            result = run_sql(insert_sql, exit_on_error=False)
            if result:
                for i, row in enumerate(result):
                    eid = row['id']
                    if i < len(meta_batch):
                        edate, etype, election_obj = meta_batch[i]
                        election_id_map[(edate, etype)] = eid
                        stats['elections_inserted'] += 1
            else:
                print(f'    WARNING: Failed to insert election batch for {dist["seat_label"]}')

        # Phase 4: Batch-insert all candidacies for this district
        cand_values = []
        for election in new_elections:
            edate = election.get('election_date')
            etype = election['election_type']
            election_id = election_id_map.get((edate, etype))
            if not election_id:
                continue

            for c in election.get('candidates', []):
                cname = c.get('name', '').strip()
                if not cname or len(cname) < 2:
                    continue
                cache_key = normalize_name(cname)
                cid = _candidate_cache.get(cache_key)
                if not cid:
                    continue

                party = c.get('party')
                votes = c.get('votes')
                pct = c.get('pct')
                is_winner = c.get('winner', False)
                is_incumbent = c.get('incumbent', False)
                result_val = 'Won' if is_winner else 'Lost'

                cand_values.append(
                    f"({election_id}, {cid}, {sql_val(party)}, "
                    f"'Active', {sql_val(is_incumbent)}, "
                    f"{sql_val(votes)}, {sql_val(pct)}, "
                    f"'{result_val}')"
                )

        if cand_values:
            for batch_start in range(0, len(cand_values), BATCH_SIZE):
                batch = cand_values[batch_start:batch_start + BATCH_SIZE]
                cand_sql = (
                    "INSERT INTO candidacies "
                    "(election_id, candidate_id, party, candidate_status, "
                    "is_incumbent, votes_received, vote_percentage, result) VALUES\n"
                    + ",\n".join(batch)
                )
                cand_result = run_sql(cand_sql, exit_on_error=False)
                if cand_result is not None:
                    stats['candidacies_inserted'] += len(batch)

    return stats


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Populate historical district elections')
    parser.add_argument('--state', required=True, help='State abbreviation (e.g., AK)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be inserted')
    parser.add_argument('--skip-candidates', action='store_true',
                        help='Only match existing candidates, don\'t create new ones')
    args = parser.parse_args()

    state = args.state.upper()
    if args.dry_run:
        print('DRY RUN MODE — no database changes will be made.\n')

    # Load parsed JSON
    input_path = os.path.join(INPUT_DIR, f'{state}.json')
    if not os.path.exists(input_path):
        print(f'ERROR: {input_path} not found. Run download_district_history.py --state {state} first.')
        sys.exit(1)

    with open(input_path) as f:
        data = json.load(f)

    parsed_districts = data['districts']
    print(f'Loaded {len(parsed_districts)} districts from {input_path}')
    total_elections = sum(len(d.get('elections', [])) for d in parsed_districts)
    print(f'Total elections in JSON: {total_elections}')

    # Load DB data
    print(f'\nLoading DB seats for {state}...')
    db_seats = load_db_seats(state)
    print(f'  {len(db_seats)} seats in DB')

    print(f'Loading existing elections...')
    existing = load_existing_elections(state)
    print(f'  {len(existing)} existing pre-2026 elections')

    # Preload candidate cache
    print(f'Preloading candidate cache...')
    preload_candidate_cache(state)

    # Match districts
    print(f'\nMatching BP districts to DB seats...')
    matched, unmatched = match_districts(state, parsed_districts, db_seats)
    print(f'  Matched: {len(matched)}')
    print(f'  Unmatched: {len(unmatched)}')

    if unmatched:
        print(f'\n  Unmatched districts:')
        for u in unmatched[:20]:
            print(f'    {u["chamber"]} {u["district_identifier"]}: {u["bp_district_name"]}')
        if len(unmatched) > 20:
            print(f'    ... and {len(unmatched) - 20} more')

    # Insert elections + candidacies
    print(f'\n{"═"*60}')
    print(f'INSERTING HISTORICAL DATA — {state}')
    print(f'{"═"*60}')

    stats = insert_elections(
        matched, existing,
        dry_run=args.dry_run,
        skip_candidates=args.skip_candidates
    )

    # Summary
    print(f'\n{"═"*60}')
    print(f'RESULTS — {state}')
    print(f'{"═"*60}')
    print(f'  Elections inserted: {stats["elections_inserted"]}')
    print(f'  Elections skipped (existing): {stats["elections_skipped"]}')
    print(f'  Candidacies inserted: {stats["candidacies_inserted"]}')
    print(f'  Candidates matched: {stats["candidates_matched"]}')
    print(f'  Candidates created: {stats["candidates_created"]}')

    # Verify (if not dry run)
    if not args.dry_run:
        print(f'\n{"═"*60}')
        print(f'VERIFICATION')
        print(f'{"═"*60}')

        verify = run_sql(f"""
            SELECT e.election_year, e.election_type, COUNT(*) as cnt,
                   SUM(COALESCE(e.total_votes_cast, 0)) as total_votes
            FROM elections e
            JOIN seats s ON e.seat_id = s.id
            JOIN districts d ON s.district_id = d.id
            JOIN states st ON d.state_id = st.id
            WHERE st.abbreviation = '{state}'
              AND e.election_year < 2026
            GROUP BY e.election_year, e.election_type
            ORDER BY e.election_year DESC, e.election_type
        """, exit_on_error=False)

        if verify:
            print(f'  Historical elections by year/type:')
            for r in verify:
                print(f'    {r["election_year"]} {r["election_type"]}: {r["cnt"]} elections, '
                      f'{r["total_votes"]:,} total votes')

    print(f'\nDone!')


if __name__ == '__main__':
    main()
