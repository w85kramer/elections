#!/usr/bin/env python3
"""
Populate MA election results from Secretary of State data.

Reads /tmp/ma_sos_elections.json (from download_ma_sos_elections.py),
reconciles against the existing Supabase database, and inserts/updates
elections and candidacies.

Handles:
  - State Senate and State House legislative races
  - Statewide offices (Governor, Lt. Governor, AG, SoS, Treasurer, Auditor)
  - District name normalization (spelled-out ordinals → abbreviated ordinals)
  - Fuzzy matching for multi-county senate districts (punctuation varies by era)
  - Party primary filtering (D/R only; minor parties skipped)
  - Candidate matching via CandidateLookup

Usage:
    python3 scripts/populate_ma_sos_elections.py --dry-run
    python3 scripts/populate_ma_sos_elections.py --report-only
    python3 scripts/populate_ma_sos_elections.py --year-from 2012 --year-to 2024
    python3 scripts/populate_ma_sos_elections.py --office senate
    python3 scripts/populate_ma_sos_elections.py --fix-votes
"""

import sys
import os
import json
import time
import argparse
import re
from collections import Counter
from difflib import SequenceMatcher

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL
from candidate_lookup import CandidateLookup


# ══════════════════════════════════════════════════════════════════════
# SQL helpers
# ══════════════════════════════════════════════════════════════════════

def run_sql(query, retries=8, exit_on_error=True):
    """Execute SQL via Supabase Management API with retry/backoff."""
    for attempt in range(retries):
        resp = httpx.post(
            API_URL,
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code in (429, 500, 503) and attempt < retries - 1:
            wait = 10 * (attempt + 1)
            print(f'  Rate limited/error ({resp.status_code}), waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None
    print('Max retries exceeded')
    if exit_on_error:
        sys.exit(1)
    return None


def escape_sql(s):
    """Escape a string for SQL, or return 'NULL' for None."""
    if s is None:
        return 'NULL'
    return "'" + str(s).replace("'", "''") + "'"


# ══════════════════════════════════════════════════════════════════════
# Ordinal conversion: spelled-out → abbreviated
# ══════════════════════════════════════════════════════════════════════

ORDINAL_MAP = {}

# Build mapping for 1st through 60th (covers all MA district numbers)
_ONES = ['', 'First', 'Second', 'Third', 'Fourth', 'Fifth', 'Sixth',
         'Seventh', 'Eighth', 'Ninth', 'Tenth', 'Eleventh', 'Twelfth',
         'Thirteenth', 'Fourteenth', 'Fifteenth', 'Sixteenth',
         'Seventeenth', 'Eighteenth', 'Nineteenth']
_TENS = ['', '', 'Twenty', 'Thirty', 'Forty', 'Fifty', 'Sixty']
# The "-ieth" ordinal forms for even tens: Twentieth, Thirtieth, etc.
_TENS_ORDINAL = ['', '', 'Twentieth', 'Thirtieth', 'Fortieth', 'Fiftieth', 'Sixtieth']
_SUFFIXES = {1: 'st', 2: 'nd', 3: 'rd'}

def _ordinal_suffix(n):
    """Return abbreviated ordinal like '1st', '2nd', '23rd', '11th'."""
    if 11 <= (n % 100) <= 13:
        return f'{n}th'
    return f'{n}{_SUFFIXES.get(n % 10, "th")}'

# 1-19
for i in range(1, 20):
    ORDINAL_MAP[_ONES[i].lower()] = _ordinal_suffix(i)

# 20-60
for tens_digit in range(2, 7):
    tens_word = _TENS[tens_digit]
    # "Twenty" as a standalone (shouldn't appear in district names, but just in case)
    ORDINAL_MAP[tens_word.lower()] = _ordinal_suffix(tens_digit * 10)
    # "Twentieth", "Thirtieth", etc. — the proper ordinal form
    ORDINAL_MAP[_TENS_ORDINAL[tens_digit].lower()] = _ordinal_suffix(tens_digit * 10)
    # Hyphenated compounds: Twenty-First, Thirty-Second, etc.
    for ones_digit in range(1, 10):
        compound = f'{tens_word}-{_ONES[ones_digit]}'
        n = tens_digit * 10 + ones_digit
        ORDINAL_MAP[compound.lower()] = _ordinal_suffix(n)


def normalize_district_name(sos_name):
    """Convert SoS district name to our DB format.

    Examples:
        'First Middlesex' → '1st Middlesex'
        'Twenty-Third Worcester' → '23rd Worcester'
        'Berkshire, Hampshire and Franklin' → 'Berkshire, Hampshire and Franklin'
          (multi-county names pass through for fuzzy matching)
    """
    if not sos_name:
        return None

    name = sos_name.strip()

    # Try to match the leading ordinal word(s)
    # Pattern: one or two words (possibly hyphenated) followed by a county name
    m = re.match(r'^([A-Za-z]+(?:-[A-Za-z]+)?)\s+(.+)$', name)
    if m:
        ordinal_word = m.group(1).lower()
        rest = m.group(2)
        if ordinal_word in ORDINAL_MAP:
            return f'{ORDINAL_MAP[ordinal_word]} {rest}'

    # No ordinal prefix — return as-is (multi-county names like "Berkshire, Hampshire and Franklin")
    return name


def normalize_for_matching(name):
    """Normalize a district name for fuzzy comparison.

    Strips punctuation variations so 'Berkshire, Hampden, Franklin & Hampshire'
    matches 'Berkshire, Hampden, Hampshire and Franklin'.
    """
    if not name:
        return ''
    n = name.lower()
    # Normalize ampersands and 'and'
    n = n.replace('&', 'and')
    # Remove commas and extra spaces
    n = re.sub(r'[,]', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    # Sort the county words to handle order differences
    # Split on ' and ' to get county groups, then sort
    parts = re.split(r'\s+and\s+', n)
    # Further split on spaces within each part (but keep multi-word names together)
    # Actually, just sort the whole set of words
    words = sorted(n.split())
    return ' '.join(words)


# ══════════════════════════════════════════════════════════════════════
# Office mapping
# ══════════════════════════════════════════════════════════════════════

# SoS office_id → our office_type
OFFICE_MAP = {
    9: 'State Senate',
    8: 'State House',       # SoS calls it "State Representative"
    3: 'Governor',
    4: 'Lt. Governor',
    12: 'Attorney General',
    45: 'Secretary of State',  # SoS calls it "Secretary of the Commonwealth"
    53: 'Treasurer',
    90: 'Auditor',
}

OFFICE_FILTER_MAP = {
    'senate': 9,
    'house': 8,     # SoS office_id for "State Representative"
    'governor': 3,
    'lt-governor': 4,
    'ag': 12,
    'sos': 45,
    'treasurer': 53,
    'auditor': 90,
}


# ══════════════════════════════════════════════════════════════════════
# Party mapping
# ══════════════════════════════════════════════════════════════════════

PARTY_MAP = {
    'Democratic': 'D',
    'Republican': 'R',
    'Independent': 'I',
    'Libertarian': 'L',
    'Green-Rainbow': 'G',
    'Green-rainbow': 'G',
    'Green': 'G',
    'United Independent Party': 'I',
    'United Independent': 'I',
    'Independent Voters': 'I',
    'American': 'O',
    'Working Families': 'O',
}


def map_party(party_str):
    """Map SoS party name to our single-letter code."""
    if not party_str:
        return 'O'
    return PARTY_MAP.get(party_str, 'O')


# ══════════════════════════════════════════════════════════════════════
# Election type mapping
# ══════════════════════════════════════════════════════════════════════

# Parties whose primaries we track
TRACKED_PRIMARY_PARTIES = {'Democratic', 'Republican'}

# Parties whose primaries we skip
MINOR_PARTIES = {'Libertarian', 'Green-Rainbow', 'Green-rainbow', 'Green',
                 'United Independent Party', 'United Independent',
                 'Independent Voters', 'American', 'Working Families',
                 'Independent'}


def classify_election(election_data):
    """Determine our election_type from SoS fields.

    Returns election_type string, or None if this election should be skipped.
    """
    party_primary = election_data.get('party_primary')
    is_special = election_data.get('is_special') == '1'

    if party_primary:
        # It's a primary
        if party_primary in MINOR_PARTIES:
            return None  # Skip minor party primaries

        if is_special:
            return 'Special_Primary'

        if party_primary == 'Democratic':
            return 'Primary_D'
        elif party_primary == 'Republican':
            return 'Primary_R'
        else:
            return None  # Unknown party primary

    # Not a primary
    if is_special:
        return 'Special'

    # Regular general election
    return 'General'


# ══════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Populate MA SoS election results')
    parser.add_argument('--dry-run', action='store_true',
                        help='Report what would happen without writing to DB')
    parser.add_argument('--fix-votes', action='store_true',
                        help='Update existing elections where vote counts differ')
    parser.add_argument('--year-from', type=int, default=None,
                        help='Only process elections from this year onward')
    parser.add_argument('--year-to', type=int, default=None,
                        help='Only process elections up to this year')
    parser.add_argument('--office', choices=['senate', 'house', 'governor', 'ag',
                                             'sos', 'treasurer', 'auditor', 'lt-governor'],
                        help='Filter to one office type')
    parser.add_argument('--report-only', action='store_true',
                        help='Just count what\'s missing vs what we have')
    args = parser.parse_args()

    # ── Load JSON ──
    input_path = '/tmp/ma_sos_elections.json'
    if not os.path.exists(input_path):
        print(f'ERROR: {input_path} not found. Run download_ma_sos_elections.py first.')
        sys.exit(1)

    with open(input_path) as f:
        all_records = json.load(f)

    print(f'Loaded {len(all_records)} election records from {input_path}')

    # ── Filter by year ──
    if args.year_from:
        all_records = [r for r in all_records if int(r['Election']['year']) >= args.year_from]
    if args.year_to:
        all_records = [r for r in all_records if int(r['Election']['year']) <= args.year_to]

    # ── Filter by office ──
    if args.office:
        target_office_id = OFFICE_FILTER_MAP[args.office]
        all_records = [r for r in all_records if r['_office_id'] == target_office_id]

    print(f'After filters: {len(all_records)} records')

    # ── Load DB mappings ──
    print('\nLoading DB mappings...')

    # Get MA state_id
    state_rows = run_sql("SELECT id FROM states WHERE abbreviation = 'MA'")
    ma_state_id = state_rows[0]['id']
    print(f'  MA state_id: {ma_state_id}')

    # Get all MA seats with district info
    seat_rows = run_sql(f"""
        SELECT s.id AS seat_id, s.office_type, s.seat_label,
               d.id AS district_id, d.chamber, d.district_name,
               d.district_number, d.office_level, d.redistricting_cycle
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        WHERE d.state_id = {ma_state_id}
          AND s.selection_method = 'Elected'
        ORDER BY d.office_level, d.chamber, d.district_name
    """)
    print(f'  {len(seat_rows)} MA seats loaded')

    # Build seat lookup maps
    # For legislative: district_name → seat_id (current cycle only)
    leg_seat_by_name = {}   # normalized district_name → seat_id
    leg_seat_by_name_fuzzy = {}  # normalized_for_matching → seat_id (for multi-county)

    # For statewide: office_type → seat_id
    statewide_seat_map = {}

    for row in seat_rows:
        if row['office_level'] == 'Statewide':
            statewide_seat_map[row['office_type']] = row['seat_id']
        elif row['office_level'] == 'Legislative' and row['redistricting_cycle'] == '2022':
            name = row['district_name']
            if name:
                leg_seat_by_name[name.lower()] = row['seat_id']
                leg_seat_by_name_fuzzy[normalize_for_matching(name)] = (row['seat_id'], name)

    print(f'  {len(leg_seat_by_name)} legislative seats (current cycle)')
    print(f'  {len(statewide_seat_map)} statewide seats: {list(statewide_seat_map.keys())}')

    # Get existing elections for MA
    existing_elec_rows = run_sql(f"""
        SELECT e.id AS election_id, e.seat_id, e.election_type, e.election_date,
               e.election_year, e.result_status, e.total_votes_cast
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        WHERE d.state_id = {ma_state_id}
    """)

    # Key: (seat_id, election_type, election_date) → election info
    existing_elections = {}
    for r in existing_elec_rows:
        key = (r['seat_id'], r['election_type'], str(r['election_date']) if r['election_date'] else None)
        existing_elections[key] = {
            'id': r['election_id'],
            'total_votes': r['total_votes_cast'],
            'result_status': r['result_status'],
            'election_year': r['election_year'],
        }

    print(f'  {len(existing_elections)} existing MA elections')

    # Get existing candidacies to check for vote discrepancies
    existing_candidacy_rows = run_sql(f"""
        SELECT cy.id AS candidacy_id, cy.election_id, cy.candidate_id,
               cy.votes_received, cy.vote_percentage, cy.party, cy.result
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        WHERE d.state_id = {ma_state_id}
    """)

    # Key: (election_id, candidate_id) → candidacy info
    existing_candidacies = {}
    # Also: election_id → list of candidacies
    candidacies_by_election = {}
    for r in existing_candidacy_rows:
        existing_candidacies[(r['election_id'], r['candidate_id'])] = {
            'id': r['candidacy_id'],
            'votes': r['votes_received'],
            'pct': float(r['vote_percentage']) if r['vote_percentage'] is not None else None,
            'party': r['party'],
            'result': r['result'],
        }
        candidacies_by_election.setdefault(r['election_id'], []).append(r)

    print(f'  {len(existing_candidacies)} existing candidacies')

    # Initialize candidate lookup
    candidate_lookup = CandidateLookup(run_sql)
    candidate_lookup.load_state('MA')
    print('  Candidate lookup loaded')

    # ── Process records ──
    print('\nClassifying SoS records...')

    stats = {
        'records_total': len(all_records),
        'records_skipped_minor_party': 0,
        'records_skipped_no_district_match': 0,
        'records_skipped_no_office_match': 0,
        'records_skipped_no_election_type': 0,
        'elections_matched': 0,
        'elections_verified_ok': 0,
        'elections_vote_discrepancy': 0,
        'elections_to_add': 0,
        'elections_added': 0,
        'candidacies_to_add': 0,
        'candidacies_added': 0,
        'candidacies_skipped_existing': 0,
        'candidates_created': 0,
        'candidates_matched': 0,
        'votes_fixed': 0,
    }

    # Track skipped districts for reporting
    skipped_districts = Counter()
    vote_discrepancies = []

    # Group SoS records by our election key: (seat_id, election_type, election_date)
    # Each group contains the SoS election + its candidates
    elections_to_process = {}  # (seat_id, etype, date) → {election_data, candidates, ...}

    for record in all_records:
        election = record['Election']
        office = record['Office']
        district = record['District']
        candidates = record.get('Candidate', [])
        office_id = record['_office_id']

        # Classify election type
        etype = classify_election(election)
        if etype is None:
            stats['records_skipped_minor_party'] += 1
            continue

        # Map office to our office_type
        our_office_type = OFFICE_MAP.get(office_id)
        if our_office_type is None:
            stats['records_skipped_no_office_match'] += 1
            continue

        # Find the seat_id
        seat_id = None

        if our_office_type in ('Governor', 'Lt. Governor', 'Attorney General',
                               'Secretary of State', 'Treasurer', 'Auditor'):
            # Statewide office
            seat_id = statewide_seat_map.get(our_office_type)
            if not seat_id:
                stats['records_skipped_no_office_match'] += 1
                continue
        else:
            # Legislative — need to match district name
            sos_district_name = district.get('name')
            if not sos_district_name:
                stats['records_skipped_no_district_match'] += 1
                continue

            normalized = normalize_district_name(sos_district_name)
            if normalized:
                seat_id = leg_seat_by_name.get(normalized.lower())

            # If no direct match, try fuzzy matching (for multi-county names)
            if not seat_id and normalized:
                fuzzy_key = normalize_for_matching(normalized)
                match = leg_seat_by_name_fuzzy.get(fuzzy_key)
                if match:
                    seat_id = match[0]

            # Still no match — try fuzzy comparison against all districts
            if not seat_id and normalized:
                best_score = 0
                best_seat = None
                fuzzy_key = normalize_for_matching(normalized)
                for db_fuzzy, (sid, db_name) in leg_seat_by_name_fuzzy.items():
                    score = SequenceMatcher(None, fuzzy_key, db_fuzzy).ratio()
                    if score > best_score:
                        best_score = score
                        best_seat = sid
                if best_score >= 0.85:
                    seat_id = best_seat

            if not seat_id:
                stats['records_skipped_no_district_match'] += 1
                skipped_districts[f"{sos_district_name} ({our_office_type})"] += 1
                continue

        election_date = election.get('date')
        election_year = int(election.get('year', 0))
        # SoS n_total_votes includes blanks — subtract them for our total_votes_cast
        raw_total = int(election['n_total_votes']) if election.get('n_total_votes') else None
        blank_votes = int(election['n_blank_votes']) if election.get('n_blank_votes') else 0
        total_votes = (raw_total - blank_votes) if raw_total is not None else None

        key = (seat_id, etype, election_date)

        # Store for processing (may have multiple SoS records per key in rare cases)
        if key not in elections_to_process:
            elections_to_process[key] = {
                'seat_id': seat_id,
                'election_type': etype,
                'election_date': election_date,
                'election_year': election_year,
                'total_votes': total_votes,
                'candidates': [],
                'office_type': our_office_type,
            }

        # Add candidates from this record
        for cand in candidates:
            cte = cand.get('CandidateToElection', {})
            party_str = cte.get('party') or cand.get('party', '')
            party = map_party(party_str)
            votes = int(cte['n_votes']) if cte.get('n_votes') else 0
            pct_raw = float(cte['pct_candidate_votes']) if cte.get('pct_candidate_votes') else None
            pct = round(pct_raw * 100, 1) if pct_raw is not None else None
            is_winner = cte.get('is_winner') == '1'
            is_write_in = cte.get('is_write_in') == '1'

            elections_to_process[key]['candidates'].append({
                'display_name': cand.get('display_name', '').strip(),
                'first_name': cand.get('first_name', '').strip(),
                'last_name': cand.get('last_name', '').strip(),
                'party': party,
                'votes': votes,
                'pct': pct,
                'is_winner': is_winner,
                'is_write_in': is_write_in,
            })

    print(f'  {len(elections_to_process)} unique elections identified from SoS data')
    print(f'  {stats["records_skipped_minor_party"]} records skipped (minor party primaries)')
    print(f'  {stats["records_skipped_no_district_match"]} records skipped (no district match)')

    if skipped_districts:
        print(f'\n  Top skipped districts (historical/redistricted):')
        for name, count in skipped_districts.most_common(20):
            print(f'    {name}: {count} records')

    # ── Reconcile against DB ──
    print('\nReconciling against database...')

    elections_to_insert = []    # List of election dicts to insert
    candidacies_to_insert = []  # List of (election_key, candidacy_dict) — election_key used to look up election_id after insert
    elections_to_fix_votes = []  # List of (election_id, field, old_val, new_val)

    for key, edata in sorted(elections_to_process.items(), key=lambda x: (x[0][2] or '', x[0][1])):
        seat_id, etype, edate = key
        existing = existing_elections.get(key)

        if existing:
            stats['elections_matched'] += 1

            # Check vote totals
            if edata['total_votes'] is not None and existing['total_votes'] is not None:
                if edata['total_votes'] != existing['total_votes']:
                    stats['elections_vote_discrepancy'] += 1
                    vote_discrepancies.append({
                        'election_id': existing['id'],
                        'seat_id': seat_id,
                        'type': etype,
                        'date': edate,
                        'db_votes': existing['total_votes'],
                        'sos_votes': edata['total_votes'],
                    })
                else:
                    stats['elections_verified_ok'] += 1
            else:
                stats['elections_verified_ok'] += 1

            # Check individual candidacy votes if election exists
            election_id = existing['id']
            for cand in edata['candidates']:
                if cand['is_write_in']:
                    continue

                # Try to find matching candidacy
                cand_id = candidate_lookup.find_match(cand['display_name'], 'MA')
                if cand_id:
                    existing_cy = existing_candidacies.get((election_id, cand_id))
                    if existing_cy:
                        # Compare votes
                        if (cand['votes'] and existing_cy['votes'] is not None
                                and cand['votes'] != existing_cy['votes']):
                            vote_discrepancies.append({
                                'candidacy_id': existing_cy['id'],
                                'election_id': election_id,
                                'candidate': cand['display_name'],
                                'type': 'candidacy_votes',
                                'date': edate,
                                'db_votes': existing_cy['votes'],
                                'sos_votes': cand['votes'],
                            })
                        stats['candidacies_skipped_existing'] += 1
                        continue

                # Candidacy doesn't exist — add it
                stats['candidacies_to_add'] += 1
                candidacies_to_insert.append((key, cand, election_id))
        else:
            # Election doesn't exist — insert it
            stats['elections_to_add'] += 1
            elections_to_insert.append(edata)

            # All candidates for new elections
            for cand in edata['candidates']:
                if cand['is_write_in']:
                    continue
                stats['candidacies_to_add'] += 1
                candidacies_to_insert.append((key, cand, None))  # election_id TBD

    print(f'\n  Elections matched: {stats["elections_matched"]}')
    print(f'  Elections verified OK: {stats["elections_verified_ok"]}')
    print(f'  Vote discrepancies: {stats["elections_vote_discrepancy"]}')
    print(f'  Elections to add: {stats["elections_to_add"]}')
    print(f'  Candidacies to add: {stats["candidacies_to_add"]}')
    print(f'  Candidacies already exist: {stats["candidacies_skipped_existing"]}')

    if vote_discrepancies:
        print(f'\n  Vote discrepancies ({len(vote_discrepancies)}):')
        for d in vote_discrepancies[:20]:
            if 'candidate' in d:
                print(f'    Candidacy {d.get("candidacy_id")}: {d["candidate"]} on {d["date"]} — '
                      f'DB={d["db_votes"]}, SoS={d["sos_votes"]}')
            else:
                print(f'    Election {d["election_id"]} ({d["type"]} {d["date"]}): '
                      f'DB={d["db_votes"]}, SoS={d["sos_votes"]}')
        if len(vote_discrepancies) > 20:
            print(f'    ... and {len(vote_discrepancies) - 20} more')

    # ── Breakdown of new elections by year/type ──
    if elections_to_insert:
        new_by_year_type = Counter((e['election_year'], e['election_type']) for e in elections_to_insert)
        print(f'\n  New elections by year/type:')
        for (yr, et), cnt in sorted(new_by_year_type.items()):
            print(f'    {yr} {et:20s} {cnt}')

    # ── Report-only or dry-run exits ──
    if args.report_only:
        print('\n[Report-only mode — no changes made]')
        return

    if args.dry_run and not args.fix_votes:
        print('\n[Dry-run mode — no changes made]')
        return

    # ── Fix votes (if --fix-votes) ──
    if args.fix_votes and vote_discrepancies:
        print(f'\nFixing vote discrepancies...')
        fixed = 0
        for d in vote_discrepancies:
            if args.dry_run:
                if 'candidate' in d:
                    print(f'  Would update candidacy {d["candidacy_id"]}: '
                          f'{d["db_votes"]} → {d["sos_votes"]}')
                else:
                    print(f'  Would update election {d["election_id"]} total_votes: '
                          f'{d["db_votes"]} → {d["sos_votes"]}')
                fixed += 1
                continue

            if 'candidacy_id' in d and 'candidate' in d:
                # Fix candidacy votes
                pct_val = 'NULL'
                # We'd need the percentage too, but we can recalculate
                run_sql(f"""
                    UPDATE candidacies
                    SET votes_received = {d['sos_votes']}
                    WHERE id = {d['candidacy_id']}
                """, exit_on_error=False)
                fixed += 1
            elif 'election_id' in d and 'candidate' not in d:
                # Fix election total votes
                run_sql(f"""
                    UPDATE elections
                    SET total_votes_cast = {d['sos_votes']}
                    WHERE id = {d['election_id']}
                """, exit_on_error=False)
                fixed += 1

        stats['votes_fixed'] = fixed
        print(f'  Fixed {fixed} discrepancies')

        if args.dry_run:
            print('\n[Dry-run mode — no other changes made]')
            return

    if args.dry_run:
        print('\n[Dry-run mode — no changes made]')
        return

    # ══════════════════════════════════════════════════════════════════
    # Step 1: Insert new elections
    # ══════════════════════════════════════════════════════════════════

    new_election_ids = {}  # (seat_id, etype, date) → election_id

    if elections_to_insert:
        print(f'\nInserting {len(elections_to_insert)} new elections...')
        batch_size = 200

        for i in range(0, len(elections_to_insert), batch_size):
            batch = elections_to_insert[i:i + batch_size]
            values = []
            batch_keys = []

            for edata in batch:
                sid = edata['seat_id']
                yr = edata['election_year']
                et = edata['election_type']
                dt = edata['election_date']
                tv = edata['total_votes']

                date_sql = f"'{dt}'" if dt else 'NULL'
                tv_sql = str(tv) if tv is not None else 'NULL'

                values.append(
                    f"({sid}, {yr}, '{et}', {date_sql}, {tv_sql}, 'Certified')"
                )
                batch_keys.append((sid, et, dt))

            joiner = ',\n                '
            values_sql = joiner.join(values)
            result = run_sql(f"""
                INSERT INTO elections (seat_id, election_year, election_type,
                                       election_date, total_votes_cast, result_status)
                VALUES
                {values_sql}
                ON CONFLICT DO NOTHING
                RETURNING id, seat_id, election_year, election_type, election_date
            """)

            if result:
                for r in result:
                    key = (r['seat_id'], r['election_type'],
                           str(r['election_date']) if r['election_date'] else None)
                    new_election_ids[key] = r['id']
                    existing_elections[key] = {
                        'id': r['id'],
                        'total_votes': None,
                        'result_status': 'Certified',
                        'election_year': r['election_year'],
                    }

            created = len(result) if result else 0
            stats['elections_added'] += created
            print(f'  Batch {i // batch_size + 1}: {created} created')

            if i + batch_size < len(elections_to_insert):
                time.sleep(5)

        print(f'  Total elections inserted: {stats["elections_added"]}')

    # ══════════════════════════════════════════════════════════════════
    # Step 2: Insert candidacies
    # ══════════════════════════════════════════════════════════════════

    if candidacies_to_insert:
        print(f'\nProcessing {len(candidacies_to_insert)} candidacies...')

        candidacy_values = []
        skipped_cands = 0

        print('  Resolving candidates (this may take a while)...')
        for idx, (key, cand, existing_election_id) in enumerate(candidacies_to_insert):
            # Resolve election_id
            election_id = existing_election_id
            if election_id is None:
                election_id = new_election_ids.get(key)
            if election_id is None:
                # Might not have been created (ON CONFLICT DO NOTHING)
                continue

            # Resolve candidate
            display_name = cand['display_name']
            if not display_name or display_name.lower() in ('write-in', 'write-in votes',
                                                             'all others', 'scattered',
                                                             'blanks', 'blank'):
                continue

            first_name = cand.get('first_name', '')
            last_name = cand.get('last_name', '')

            cand_id = candidate_lookup.find_or_create(
                full_name=display_name,
                state='MA',
                first_name=first_name if first_name else None,
                last_name=last_name if last_name else None,
            )

            if not cand_id:
                continue

            # Check if this exact candidacy already exists
            if (election_id, cand_id) in existing_candidacies:
                continue

            # Track that we've seen this one (avoid duplicates within batch)
            existing_candidacies[(election_id, cand_id)] = True

            party = cand['party']
            votes = cand['votes']
            pct = cand['pct']
            is_winner = cand['is_winner']
            is_write_in = cand['is_write_in']
            is_major = party in ('D', 'R')
            result = 'Won' if is_winner else 'Lost'

            votes_sql = str(votes) if votes is not None else 'NULL'
            pct_sql = str(pct) if pct is not None else 'NULL'

            candidacy_values.append(
                f"({election_id}, {cand_id}, {escape_sql(party)}, "
                f"'{result}', {votes_sql}, {pct_sql}, "
                f"{'TRUE' if is_major else 'FALSE'}, "
                f"{'TRUE' if is_write_in else 'FALSE'})"
            )

        if candidacy_values:
            print(f'  Inserting {len(candidacy_values)} candidacies...')
            batch_size = 200

            for i in range(0, len(candidacy_values), batch_size):
                batch = candidacy_values[i:i + batch_size]
                joiner = ',\n                '
                values_sql = joiner.join(batch)
                result = run_sql(f"""
                    INSERT INTO candidacies (election_id, candidate_id, party,
                                              result, votes_received, vote_percentage,
                                              is_major, is_write_in)
                    VALUES
                    {values_sql}
                    ON CONFLICT DO NOTHING
                """, exit_on_error=False)

                stats['candidacies_added'] += len(batch)
                print(f'    Batch {i // batch_size + 1}: {len(batch)} rows')

                if i + batch_size < len(candidacy_values):
                    time.sleep(5)

            print(f'  Total candidacies inserted: {stats["candidacies_added"]}')
        else:
            print('  No candidacies to insert')

    # ══════════════════════════════════════════════════════════════════
    # Summary
    # ══════════════════════════════════════════════════════════════════

    print('\n' + '=' * 60)
    print('SUMMARY')
    print('=' * 60)
    print(f'  Records processed:           {stats["records_total"]}')
    print(f'  Skipped (minor party):       {stats["records_skipped_minor_party"]}')
    print(f'  Skipped (no district match): {stats["records_skipped_no_district_match"]}')
    print(f'  Skipped (no office match):   {stats["records_skipped_no_office_match"]}')
    print(f'  Elections matched in DB:     {stats["elections_matched"]}')
    print(f'  Elections verified OK:       {stats["elections_verified_ok"]}')
    print(f'  Vote discrepancies found:    {stats["elections_vote_discrepancy"]}')
    print(f'  Elections added:             {stats["elections_added"]}')
    print(f'  Candidacies added:           {stats["candidacies_added"]}')
    print(f'  Candidacies already existed: {stats["candidacies_skipped_existing"]}')
    if args.fix_votes:
        print(f'  Votes fixed:                 {stats["votes_fixed"]}')


if __name__ == '__main__':
    main()
