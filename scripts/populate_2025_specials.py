"""
Populate 2025 special election results into the database.

Reads parsed JSON from download_2025_specials.py and creates:
- Election records (Special, Special_Primary, Special_Runoff)
- Former incumbent candidate records + closed seat_terms
- New candidate records (challengers)
- Candidacy records with vote results
- Updated winner seat_terms (link election_id, set start_date)

Usage:
    python3 scripts/populate_2025_specials.py --dry-run
    python3 scripts/populate_2025_specials.py
    python3 scripts/populate_2025_specials.py --state VA
"""
import sys
import re
import json
import time
import argparse
import unicodedata
from collections import Counter, defaultdict

import httpx

TOKEN = 'sbp_134edd259126b21a7fc11c7a13c0c8c6834d7fa7'
PROJECT_REF = 'pikcvwulzfxgwfcfssxc'
BATCH_SIZE = 400
INPUT_PATH = '/tmp/2025_special_results.json'


def run_sql(query, exit_on_error=True, max_retries=5):
    for attempt in range(max_retries):
        resp = httpx.post(
            f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f'  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None
    print(f'SQL ERROR: Max retries exceeded')
    if exit_on_error:
        sys.exit(1)
    return None


def esc(s):
    if s is None:
        return ''
    return str(s).replace("'", "''")


def strip_accents(s):
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def name_similarity(name1, name2):
    if name1 is None or name2 is None:
        return 0.0

    def normalize(n):
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

    last1 = parts1[-1]
    last2 = parts2[-1]
    last_match = (last1 == last2)
    if not last_match:
        if last1 in parts2 or last2 in parts1:
            last_match = True
    if not last_match:
        return 0.0

    first1 = parts1[0]
    first2 = parts2[0]
    if first1 == first2:
        return 0.9
    if first1.startswith(first2) or first2.startswith(first1):
        return 0.8
    if first1[0] == first2[0]:
        return 0.7
    return 0.3


# ══════════════════════════════════════════════════════════════════════
# STATE-SPECIFIC DISTRICT/SEAT MAPPING
# ══════════════════════════════════════════════════════════════════════

# MA House district name → number mapping (alphabetical sort of canonical names)
MA_HOUSE_DISTRICTS_BP = [
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
    "Barnstable, Dukes, and Nantucket",
]

# Build MA House mapping: name → district_number
MA_HOUSE_MAP = {name: str(i + 1) for i, name in enumerate(sorted(MA_HOUSE_DISTRICTS_BP))}


def resolve_db_district(race):
    """
    Map a special election race to its DB district_number and chamber.

    Returns (db_district_number: str, db_chamber: str, seat_designator: str|None, office_type: str)
    """
    state = race['state']
    chamber = race['chamber']
    district = race['district']

    # Default mapping
    seat_designator = None

    if state == 'MN' and chamber == 'House':
        # MN House: '34B' → district 68 (formula: (n-1)*2 + (A=1, B=2))
        match = re.match(r'(\d+)([AB])', district)
        if match:
            n = int(match.group(1))
            letter = match.group(2)
            db_num = (n - 1) * 2 + (1 if letter == 'A' else 2)
            return str(db_num), 'House', None, 'State House'

    if state == 'MA' and chamber == 'House':
        # MA House: '3rd Bristol' → numeric via alphabetical mapping
        db_num = MA_HOUSE_MAP.get(district)
        if db_num:
            return db_num, 'House', None, 'State House'
        print(f'    WARNING: No MA House mapping for {district!r}')
        return None, None, None, None

    if state == 'WA' and chamber == 'House':
        # WA House: '33-Pos1' → district '33', seat 'A'
        match = re.match(r'(\d+)-Pos(\d)', district)
        if match:
            db_dist = match.group(1)
            pos = int(match.group(2))
            seat_designator = 'A' if pos == 1 else 'B'
            return db_dist, 'House', seat_designator, 'State House'

    if state == 'NH' and chamber == 'House':
        # NH House: 'Coos-5' or 'Strafford-12' — district_number is the same format
        return district, 'House', None, 'State House'

    # Map chamber names to DB chamber names
    chamber_map = {
        'House': 'House',
        'Senate': 'Senate',
        'Assembly': 'Assembly',
        'House of Delegates': 'House of Delegates',
    }
    db_chamber = chamber_map.get(chamber, chamber)

    # Map to office_type
    if chamber == 'Senate':
        office_type = 'State Senate'
    elif chamber == 'Assembly':
        office_type = 'State House'
    elif chamber == 'House of Delegates':
        office_type = 'State House'
    else:
        office_type = 'State House'

    return district, db_chamber, seat_designator, office_type


# ══════════════════════════════════════════════════════════════════════
# STEP 1: Load DB Maps
# ══════════════════════════════════════════════════════════════════════

def load_db_maps(states_needed):
    """
    Load all seats, seat_terms, candidates, and districts for the needed states.

    Returns:
        seat_map: {state: {(office_type, district_number, seat_designator) → seat_info}}
        current_terms: {seat_id → {candidate_id, full_name, party, caucus, term_id}}
        all_candidates: [(id, full_name)] for name matching
        district_map: {state: {(chamber, district_number) → {district_id, pres_2024_margin}}}
    """
    states_str = ','.join(f"'{s}'" for s in states_needed)

    # Load seats
    print('  Loading seats...')
    seats = run_sql(f"""
        SELECT se.id as seat_id, se.office_type, se.seat_designator, se.seat_label,
               d.district_number, d.num_seats, d.id as district_id, d.chamber,
               d.pres_2024_margin,
               s.abbreviation as state
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation IN ({states_str})
          AND se.selection_method = 'Elected'
          AND d.office_level = 'Legislative'
        ORDER BY s.abbreviation, se.office_type, d.district_number, se.seat_designator
    """)

    seat_map = defaultdict(dict)
    district_map = defaultdict(dict)
    for s in seats:
        key = (s['office_type'], s['district_number'], s['seat_designator'])
        seat_map[s['state']][key] = {
            'seat_id': s['seat_id'],
            'seat_label': s['seat_label'],
            'district_id': s['district_id'],
            'num_seats': s['num_seats'],
            'pres_2024_margin': s['pres_2024_margin'],
        }
        d_key = (s['chamber'], s['district_number'])
        district_map[s['state']][d_key] = {
            'district_id': s['district_id'],
            'pres_2024_margin': s['pres_2024_margin'],
        }

    print(f'  Loaded {sum(len(v) for v in seat_map.values())} seats across {len(seat_map)} states')
    time.sleep(2)

    # Load current seat_terms
    print('  Loading seat_terms...')
    all_seat_ids = []
    for state_seats in seat_map.values():
        for sinfo in state_seats.values():
            all_seat_ids.append(str(sinfo['seat_id']))

    current_terms = {}
    if all_seat_ids:
        for batch_start in range(0, len(all_seat_ids), 500):
            batch = all_seat_ids[batch_start:batch_start + 500]
            seat_ids_str = ','.join(batch)
            terms = run_sql(f"""
                SELECT st.id as term_id, st.seat_id, st.candidate_id, st.party, st.caucus,
                       c.full_name
                FROM seat_terms st
                JOIN candidates c ON st.candidate_id = c.id
                WHERE st.seat_id IN ({seat_ids_str})
                  AND st.end_date IS NULL
            """)
            for t in terms:
                current_terms[t['seat_id']] = {
                    'term_id': t['term_id'],
                    'candidate_id': t['candidate_id'],
                    'full_name': t['full_name'],
                    'party': t['party'],
                    'caucus': t['caucus'],
                }

    print(f'  Loaded {len(current_terms)} current seat_terms')
    time.sleep(2)

    # Load all candidates (for name matching)
    print('  Loading candidates...')
    all_candidates = run_sql("SELECT id, full_name FROM candidates")
    print(f'  Loaded {len(all_candidates)} candidates')

    return seat_map, current_terms, all_candidates, district_map


# ══════════════════════════════════════════════════════════════════════
# STEP 2: Process Each Race
# ══════════════════════════════════════════════════════════════════════

def find_seat(race, seat_map, current_terms=None):
    """Find the seat for this special election race. Returns seat_info dict or None."""
    state = race['state']
    db_district, db_chamber, seat_designator, office_type = resolve_db_district(race)
    if db_district is None:
        return None, None

    state_seats = seat_map.get(state, {})

    # Try exact match
    key = (office_type, db_district, seat_designator)
    if key in state_seats:
        return state_seats[key], key

    # For multi-member NH districts, we need to find the vacant seat
    if seat_designator is None:
        matching_seats = []
        for k, v in state_seats.items():
            if k[0] == office_type and k[1] == db_district:
                matching_seats.append((k, v))

        if len(matching_seats) == 1:
            return matching_seats[0][1], matching_seats[0][0]
        elif len(matching_seats) > 1:
            # Multi-member district — find the seat whose current holder
            # matches the former incumbent
            former = race.get('former_incumbent', '')
            if current_terms and former:
                for k, v in matching_seats:
                    term = current_terms.get(v['seat_id'])
                    if term:
                        sim = name_similarity(former, term['full_name'])
                        if sim >= 0.7:
                            return v, k
                # Also check for vacant seats (no current term)
                for k, v in matching_seats:
                    if v['seat_id'] not in current_terms:
                        return v, k
            # Fallback: return first match
            return matching_seats[0][1], matching_seats[0][0]

    print(f'    WARNING: No seat found for {state} {office_type} {db_district} (designator={seat_designator})')
    return None, None


def process_all_races(races, seat_map, current_terms, all_candidates, district_map,
                      dry_run=False):
    """
    Process all special election races.

    Returns summary stats dict.
    """
    stats = {
        'elections_created': 0,
        'former_incumbents_created': 0,
        'seat_terms_closed': 0,
        'new_candidates': 0,
        'candidacies_created': 0,
        'winner_terms_linked': 0,
        'skipped': 0,
    }

    # Build candidate name index for matching
    cand_index = {}  # last_name_lower → [(id, full_name)]
    for c in all_candidates:
        parts = c['full_name'].lower().split()
        if parts:
            last = parts[-1]
            if last not in cand_index:
                cand_index[last] = []
            cand_index[last].append((c['id'], c['full_name']))

    # Collect all operations to batch
    elections_to_create = []     # (race_idx, election_data, seat_info, seat_key)
    former_incumbents = []       # (race_idx, name, seat_id, vacancy_reason, end_date)

    for i, race in enumerate(races):
        state = race['state']
        seat_info, seat_key = find_seat(race, seat_map, current_terms)
        if not seat_info:
            stats['skipped'] += 1
            continue

        seat_id = seat_info['seat_id']

        # Check if this race has a runoff (affects how we interpret Special "winners")
        has_runoff = any(e['type'] == 'Special_Runoff' for e in race['elections'])

        # Collect elections for this race
        for elec in race['elections']:
            # If Special has multiple winners and there's a runoff, mark them as Advanced
            if elec['type'] == 'Special' and has_runoff:
                winners = [c for c in elec['candidates'] if c.get('is_winner')]
                if len(winners) > 1:
                    for c in elec['candidates']:
                        if c.get('is_winner'):
                            c['advanced_to_runoff'] = True
            elections_to_create.append((i, elec, seat_info, seat_key))

        # Former incumbent handling
        former_name = race['former_incumbent']
        vacancy_reason = race['vacancy_reason']

        # Find the earliest election date for this race to set end_date
        dates = [e['date'] for e in race['elections'] if e['date']]
        if dates:
            earliest = min(dates)
            # End date is the day before the first special election
            from datetime import datetime, timedelta
            end_dt = datetime.strptime(earliest, '%Y-%m-%d') - timedelta(days=1)
            end_date = end_dt.strftime('%Y-%m-%d')
        else:
            end_date = None

        former_incumbents.append((i, former_name, seat_id, vacancy_reason, end_date))

    print(f'\n  Elections to create: {len(elections_to_create)}')
    print(f'  Former incumbents to process: {len(former_incumbents)}')

    if dry_run:
        # Summarize what would happen
        etype_counts = Counter(e[1]['type'] for e in elections_to_create)
        print(f'\n  Election types:')
        for etype, cnt in sorted(etype_counts.items()):
            print(f'    {etype}: {cnt}')

        total_cands = sum(len(e[1]['candidates']) for e in elections_to_create)
        print(f'  Total candidacies: {total_cands}')
        stats['elections_created'] = len(elections_to_create)
        stats['former_incumbents_created'] = len(former_incumbents)
        return stats

    # ── STEP 2a: Create former incumbent candidates + close seat_terms ──
    print('\n  Processing former incumbents...')

    # Phase 1: Identify which former incumbents need new candidate records
    new_former_cands = []  # [(former_name, seat_id, vacancy_reason, end_date)]
    existing_former = []   # [(cand_id, former_name, seat_id, vacancy_reason, end_date)]
    skipped_incumbents = []  # former incumbent is current holder (won their special)

    for i, former_name, seat_id, vacancy_reason, end_date in former_incumbents:
        term_info = current_terms.get(seat_id)

        # Check if current holder IS the former incumbent (they won their own special)
        if term_info and name_similarity(former_name, term_info['full_name']) >= 0.7:
            print(f'    SKIP: {former_name} is still current holder of seat {seat_id} (won their special)')
            skipped_incumbents.append(seat_id)
            continue

        # Check if former incumbent already exists in candidates table
        existing_cand_id = None
        parts = former_name.lower().split()
        if parts:
            last = parts[-1]
            for cid, cname in cand_index.get(last, []):
                if name_similarity(former_name, cname) >= 0.7:
                    existing_cand_id = cid
                    break

        if existing_cand_id:
            existing_former.append((existing_cand_id, former_name, seat_id, vacancy_reason, end_date))
        else:
            new_former_cands.append((former_name, seat_id, vacancy_reason, end_date))

    print(f'    Existing candidates: {len(existing_former)}')
    print(f'    New candidates needed: {len(new_former_cands)}')
    print(f'    Skipped (still current holder): {len(skipped_incumbents)}')

    # Phase 2: Batch-create new former incumbent candidates
    if new_former_cands:
        values = []
        for former_name, seat_id, vacancy_reason, end_date in new_former_cands:
            name_parts = former_name.split()
            first = esc(name_parts[0]) if name_parts else ''
            last = esc(name_parts[-1]) if len(name_parts) > 1 else esc(name_parts[0]) if name_parts else ''
            values.append(f"('{esc(former_name)}', '{first}', '{last}', NULL)")

        result = run_sql(
            "INSERT INTO candidates (full_name, first_name, last_name, gender) VALUES\n"
            + ",\n".join(values)
            + "\nRETURNING id;"
        )
        new_ids = [r['id'] for r in result]
        stats['former_incumbents_created'] = len(new_ids)
        print(f'    Inserted {len(new_ids)} new former incumbent candidates')
        time.sleep(2)

        # Add to existing_former list and index
        for idx, (former_name, seat_id, vacancy_reason, end_date) in enumerate(new_former_cands):
            cand_id = new_ids[idx]
            existing_former.append((cand_id, former_name, seat_id, vacancy_reason, end_date))
            parts = former_name.lower().split()
            if parts:
                last = parts[-1]
                if last not in cand_index:
                    cand_index[last] = []
                cand_index[last].append((cand_id, former_name))

    # Phase 3: Close seat_terms for former incumbents (batch SQL)
    end_reason_map = {
        'resigned': 'resigned', 'died': 'died',
        'removed': 'removed', 'appointed_elsewhere': 'appointed_elsewhere',
    }

    # Build batch: check which former incumbents have open seat_terms
    if existing_former:
        # Query all open seat_terms for these candidate/seat combos
        seat_ids = [str(sid) for _, _, sid, _, _ in existing_former]
        open_terms = run_sql(
            f"SELECT st.id, st.seat_id, st.candidate_id, st.party "
            f"FROM seat_terms st WHERE st.seat_id IN ({','.join(seat_ids)}) "
            f"AND st.end_date IS NULL"
        )
        time.sleep(2)

        open_terms_by_seat_cand = {}
        for t in open_terms:
            open_terms_by_seat_cand[(t['seat_id'], t['candidate_id'])] = t

        # Close existing terms or create historical ones
        close_sqls = []
        insert_terms = []
        for cand_id, former_name, seat_id, vacancy_reason, end_date in existing_former:
            end_reason = end_reason_map.get(vacancy_reason, 'resigned')
            end_date_sql = f"'{end_date}'" if end_date else 'NULL'
            ot = open_terms_by_seat_cand.get((seat_id, cand_id))
            if ot:
                # Close existing term
                close_sqls.append(
                    f"UPDATE seat_terms SET end_date = {end_date_sql}, "
                    f"end_reason = '{end_reason}' WHERE id = {ot['id']}"
                )
            else:
                # Create historical term
                insert_terms.append(
                    f"({seat_id}, {cand_id}, NULL, NULL, {end_date_sql}, "
                    f"'elected', '{end_reason}', NULL, NULL)"
                )

        if close_sqls:
            # Execute closures in batches
            for batch_start in range(0, len(close_sqls), 20):
                batch = close_sqls[batch_start:batch_start + 20]
                run_sql(";\n".join(batch))
                stats['seat_terms_closed'] += len(batch)
                time.sleep(2)
            print(f'    Closed {len(close_sqls)} existing seat_terms')

        if insert_terms:
            result = run_sql(
                "INSERT INTO seat_terms (seat_id, candidate_id, party, "
                "start_date, end_date, start_reason, end_reason, caucus, election_id) VALUES\n"
                + ",\n".join(insert_terms)
            )
            stats['seat_terms_closed'] += len(insert_terms)
            print(f'    Created {len(insert_terms)} historical seat_terms')
            time.sleep(2)

    time.sleep(2)

    # ── STEP 2b: Create election records ──
    print('\n  Creating election records...')

    # Group by seat to handle related_election_id linking
    elections_by_seat = defaultdict(list)
    for idx, (race_idx, elec, seat_info, seat_key) in enumerate(elections_to_create):
        elections_by_seat[seat_info['seat_id']].append((idx, race_idx, elec, seat_info))

    election_ids = {}  # idx → election_id

    # Phase 1: Create all Special (general) elections first in a batch
    special_inserts = []  # [(idx, seat_id, date, pres_margin, total_votes)]
    non_special_items = []  # [(idx, seat_id, elec, seat_info)]

    for seat_id, seat_elections in elections_by_seat.items():
        for idx, race_idx, elec, seat_info in seat_elections:
            etype = elec['type']
            date = elec['date']
            total_votes = elec.get('total_votes') or 0
            pres_margin = seat_info.get('pres_2024_margin')

            if etype == 'Special':
                special_inserts.append((idx, seat_id, date, pres_margin, total_votes))
            else:
                non_special_items.append((idx, seat_id, elec, seat_info))

    # Batch insert Special elections
    if special_inserts:
        values = []
        for idx, seat_id, date, pres_margin, total_votes in special_inserts:
            date_sql = f"'{date}'" if date else 'NULL'
            pm_sql = f"'{esc(pres_margin)}'" if pres_margin else 'NULL'
            tv_sql = total_votes if total_votes else 'NULL'
            values.append(
                f"({seat_id}, {date_sql}, 2025, 'Special', NULL, {pm_sql}, {tv_sql}, 'Certified')"
            )

        for batch_start in range(0, len(values), 50):
            batch = values[batch_start:batch_start + 50]
            batch_indices = [special_inserts[i][0] for i in range(batch_start, min(batch_start + 50, len(values)))]
            result = run_sql(
                "INSERT INTO elections (seat_id, election_date, election_year, election_type, "
                "related_election_id, pres_margin_this_cycle, total_votes_cast, "
                "result_status) VALUES\n"
                + ",\n".join(batch)
                + "\nRETURNING id;"
            )
            for i, r in enumerate(result):
                election_ids[batch_indices[i]] = r['id']
            stats['elections_created'] += len(result)
            time.sleep(2)

    # Build seat_id → Special election_id mapping
    seat_to_special = {}
    for idx, seat_id, date, pres_margin, total_votes in special_inserts:
        seat_to_special[seat_id] = election_ids[idx]

    # Phase 2: Create non-Special elections (primaries, runoffs) linking to Specials
    if non_special_items:
        values = []
        ns_indices = []
        for idx, seat_id, elec, seat_info in non_special_items:
            etype = elec['type']
            date = elec['date']
            total_votes = elec.get('total_votes') or 0
            pres_margin = seat_info.get('pres_2024_margin')

            db_etype = etype
            if etype in ('Special_Primary_D', 'Special_Primary_R', 'Special_Primary',
                          'Special_Primary_Runoff_D', 'Special_Primary_Runoff_R'):
                db_etype = 'Special_Primary'

            date_sql = f"'{date}'" if date else 'NULL'
            pm_sql = f"'{esc(pres_margin)}'" if pres_margin else 'NULL'
            tv_sql = total_votes if total_votes else 'NULL'
            related = seat_to_special.get(seat_id)
            related_sql = str(related) if related else 'NULL'

            values.append(
                f"({seat_id}, {date_sql}, 2025, '{db_etype}', {related_sql}, {pm_sql}, {tv_sql}, 'Certified')"
            )
            ns_indices.append(idx)

        for batch_start in range(0, len(values), 50):
            batch = values[batch_start:batch_start + 50]
            batch_indices = ns_indices[batch_start:batch_start + 50]
            result = run_sql(
                "INSERT INTO elections (seat_id, election_date, election_year, election_type, "
                "related_election_id, pres_margin_this_cycle, total_votes_cast, "
                "result_status) VALUES\n"
                + ",\n".join(batch)
                + "\nRETURNING id;"
            )
            for i, r in enumerate(result):
                election_ids[batch_indices[i]] = r['id']
            stats['elections_created'] += len(result)
            time.sleep(2)

    print(f'  Created {stats["elections_created"]} elections')
    time.sleep(2)

    # ── STEP 2c: Create candidates + candidacies ──
    print('\n  Creating candidacies...')

    all_candidacies = []

    for idx, (race_idx, elec, seat_info, seat_key) in enumerate(elections_to_create):
        election_id = election_ids.get(idx)
        if not election_id:
            continue

        seat_id = seat_info['seat_id']
        race = races[race_idx]
        etype = elec['type']

        # Get current term holder for incumbent matching
        term_info = current_terms.get(seat_id)

        for cand in elec['candidates']:
            cand_name = cand['name']
            cand_party = cand['party']
            votes = cand.get('votes')
            vote_pct = cand.get('vote_pct')
            is_winner = cand.get('is_winner', False)

            # Try to match to existing candidate
            candidate_id = None
            is_inc = False

            # Check if this is the current holder
            if term_info:
                sim = name_similarity(cand_name, term_info['full_name'])
                if sim >= 0.7:
                    candidate_id = term_info['candidate_id']
                    is_inc = False  # They won the special, not an incumbent in the traditional sense

            # Check broader candidate database
            if not candidate_id:
                parts = cand_name.lower().split()
                if parts:
                    last = parts[-1]
                    for cid, cname in cand_index.get(last, []):
                        sim = name_similarity(cand_name, cname)
                        if sim >= 0.7:
                            candidate_id = cid
                            break

            # Determine result
            if cand.get('advanced_to_runoff'):
                result = 'Advanced'
            elif is_winner:
                result = 'Won'
            else:
                result = 'Lost'

            # For D/R-specific primaries, set the party from the election type
            if etype == 'Special_Primary_D' or etype == 'Special_Primary_Runoff_D':
                cand_party = cand_party or 'D'
            elif etype == 'Special_Primary_R' or etype == 'Special_Primary_Runoff_R':
                cand_party = cand_party or 'R'

            # For promoted primaries (general canceled, primary became the Special),
            # try to infer party from the current holder or seat data
            if cand_party is None and etype == 'Special':
                # Check if we can get party from current seat term
                term_info_for_party = current_terms.get(seat_id)
                if term_info_for_party:
                    sim = name_similarity(cand_name, term_info_for_party['full_name'])
                    if sim >= 0.7:
                        cand_party = term_info_for_party.get('party')

            all_candidacies.append({
                'election_id': election_id,
                'candidate_id': candidate_id,
                'candidate_name': cand_name,
                'party': cand_party,
                'is_incumbent': is_inc,
                'votes': votes,
                'vote_pct': vote_pct,
                'result': result,
                'seat_id': seat_id,
                'is_winner': is_winner,
                'etype': etype,
            })

    # Create new candidates
    new_cands = [c for c in all_candidacies if c['candidate_id'] is None]
    reuse_cands = [c for c in all_candidacies if c['candidate_id'] is not None]
    print(f'  Total candidacies: {len(all_candidacies)}')
    print(f'    Existing candidates: {len(reuse_cands)}')
    print(f'    New candidates needed: {len(new_cands)}')

    if new_cands:
        values = []
        for m in new_cands:
            parts = m['candidate_name'].split()
            first = esc(parts[0]) if parts else ''
            last = esc(parts[-1]) if len(parts) > 1 else esc(parts[0]) if parts else ''
            full = esc(m['candidate_name'])
            values.append(f"('{full}', '{first}', '{last}', NULL)")

        new_ids = []
        for batch_start in range(0, len(values), BATCH_SIZE):
            batch = values[batch_start:batch_start + BATCH_SIZE]
            sql = (
                "INSERT INTO candidates (full_name, first_name, last_name, gender) VALUES\n"
                + ",\n".join(batch)
                + "\nRETURNING id;"
            )
            result = run_sql(sql, exit_on_error=False)
            if result is None:
                time.sleep(2)
                result = run_sql(sql)
            new_ids.extend(r['id'] for r in result)

        print(f'  Inserted {len(new_ids)} new candidates')
        stats['new_candidates'] = len(new_ids)

        if len(new_ids) != len(new_cands):
            print(f'  ERROR: Expected {len(new_cands)}, got {len(new_ids)}')
            sys.exit(1)

        for i, m in enumerate(new_cands):
            m['candidate_id'] = new_ids[i]
            # Add to index
            parts = m['candidate_name'].lower().split()
            if parts:
                last = parts[-1]
                if last not in cand_index:
                    cand_index[last] = []
                cand_index[last].append((new_ids[i], m['candidate_name']))

    # Insert candidacies
    values = []
    for m in all_candidacies:
        votes_sql = m['votes'] if m['votes'] is not None else 'NULL'
        pct_sql = m['vote_pct'] if m['vote_pct'] is not None else 'NULL'
        party_sql = f"'{esc(m['party'])}'" if m['party'] else 'NULL'
        values.append(
            f"({m['election_id']}, {m['candidate_id']}, {party_sql}, "
            f"'Active', {m['is_incumbent']}, false, NULL, NULL, "
            f"{votes_sql}, {pct_sql}, '{m['result']}', NULL, NULL)"
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
            time.sleep(2)
            result = run_sql(sql)
        total_inserted += len(result)
        time.sleep(0.8)

    stats['candidacies_created'] = total_inserted
    print(f'  Inserted {total_inserted} candidacies')

    # ── STEP 2d: Link winner seat_terms to special elections ──
    print('\n  Linking winner seat_terms...')

    # For races with runoffs, use the runoff winner; for others use Special winner
    # Build a map of seat_id → true winner (runoff winner takes priority)
    seat_winners = {}  # seat_id → candidacy dict
    for m in all_candidacies:
        if m['is_winner'] and m['result'] == 'Won':
            sid = m['seat_id']
            if m['etype'] == 'Special_Runoff':
                # Runoff winner always takes priority
                seat_winners[sid] = m
            elif m['etype'] == 'Special' and sid not in seat_winners:
                seat_winners[sid] = m

    # Batch the seat_term updates
    term_updates = []  # [(term_id, election_id)]
    unlinked = []
    for sid, m in seat_winners.items():
        candidate_id = m['candidate_id']
        election_id = m['election_id']

        term_info = current_terms.get(sid)
        if term_info and term_info['candidate_id'] == candidate_id:
            term_updates.append((term_info['term_id'], election_id))
        elif term_info:
            sim = name_similarity(m['candidate_name'], term_info['full_name'])
            if sim >= 0.7:
                term_updates.append((term_info['term_id'], election_id))
            else:
                unlinked.append((sid, m['candidate_name'], term_info['full_name']))
        else:
            unlinked.append((sid, m['candidate_name'], 'NO CURRENT HOLDER'))

    if term_updates:
        update_sqls = [
            f"UPDATE seat_terms SET election_id = {eid} WHERE id = {tid}"
            for tid, eid in term_updates
        ]
        for batch_start in range(0, len(update_sqls), 20):
            batch = update_sqls[batch_start:batch_start + 20]
            run_sql(";\n".join(batch))
            time.sleep(2)
        stats['winner_terms_linked'] = len(term_updates)

    for sid, winner, holder in unlinked:
        print(f'    WARNING: Winner {winner} != current holder {holder} for seat {sid}')

    print(f'  Linked {stats["winner_terms_linked"]} winner seat_terms')

    return stats


# ══════════════════════════════════════════════════════════════════════
# VERIFICATION
# ══════════════════════════════════════════════════════════════════════

def verify():
    print(f'\n{"=" * 60}')
    print('VERIFICATION')
    print(f'{"=" * 60}')

    # Election count by type
    r = run_sql("""
        SELECT election_type, COUNT(*) as cnt FROM elections
        WHERE election_year = 2025 AND election_type LIKE 'Special%'
        GROUP BY election_type ORDER BY election_type
    """)
    print('Special elections by type:')
    for row in r:
        print(f'  {row["election_type"]}: {row["cnt"]}')

    # Candidacy results
    r = run_sql("""
        SELECT result, COUNT(*) as cnt FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        WHERE e.election_type LIKE 'Special%' AND e.election_year = 2025
        GROUP BY result ORDER BY result
    """)
    print('\nCandidacy results:')
    for row in r:
        print(f'  {row["result"]}: {row["cnt"]}')

    # Seat terms closed
    r = run_sql("""
        SELECT COUNT(*) as cnt FROM seat_terms
        WHERE end_date IS NOT NULL AND end_date >= '2025-01-01' AND end_date < '2026-06-01'
    """)
    print(f'\nSeat terms with end_date in 2025: {r[0]["cnt"]}')

    # Seat terms linked to special elections
    r = run_sql("""
        SELECT COUNT(*) as cnt FROM seat_terms st
        JOIN elections e ON st.election_id = e.id
        WHERE e.election_type = 'Special'
    """)
    print(f'Seat terms linked to Special elections: {r[0]["cnt"]}')

    # Winners by party
    r = run_sql("""
        SELECT ca.party, COUNT(*) as cnt FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        WHERE e.election_type = 'Special' AND ca.result = 'Won' AND e.election_year = 2025
        GROUP BY ca.party ORDER BY COUNT(*) DESC
    """)
    print('\nSpecial general winners by party:')
    for row in r:
        print(f'  {row["party"]}: {row["cnt"]}')

    # Winners by state
    r = run_sql("""
        SELECT s.abbreviation, COUNT(*) as cnt FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE e.election_type = 'Special' AND ca.result = 'Won' AND e.election_year = 2025
        GROUP BY s.abbreviation ORDER BY s.abbreviation
    """)
    print('\nSpecial general winners by state:')
    for row in r:
        print(f'  {row["abbreviation"]}: {row["cnt"]}')

    # Spot checks
    r = run_sql("""
        SELECT se.seat_label, c.full_name, ca.party, ca.votes_received,
               ca.vote_percentage, ca.result, e.election_type
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN candidates c ON ca.candidate_id = c.id
        WHERE e.election_type = 'Special' AND e.election_year = 2025 AND ca.result = 'Won'
        ORDER BY RANDOM() LIMIT 10
    """)
    print('\nSpot checks (Special general winners):')
    for row in r:
        print(f'  {row["seat_label"]}: {row["full_name"]} ({row["party"]}) '
              f'{row["votes_received"]} votes ({row["vote_percentage"]}%)')

    # Duplicate check
    r = run_sql("""
        SELECT ca.election_id, ca.candidate_id, COUNT(*) as cnt
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        WHERE e.election_year = 2025 AND e.election_type LIKE 'Special%'
        GROUP BY ca.election_id, ca.candidate_id HAVING COUNT(*) > 1
    """)
    if r:
        print(f'\nWARNING: {len(r)} duplicate candidacies!')
    else:
        print('\nNo duplicate candidacies.')

    # Total DB counts
    r = run_sql("""
        SELECT
            (SELECT COUNT(*) FROM elections) as total_elections,
            (SELECT COUNT(*) FROM candidates) as total_candidates,
            (SELECT COUNT(*) FROM candidacies) as total_candidacies,
            (SELECT COUNT(*) FROM seat_terms) as total_seat_terms
    """)
    c = r[0]
    print(f'\nTotal DB counts:')
    print(f'  Elections: {c["total_elections"]}')
    print(f'  Candidates: {c["total_candidates"]}')
    print(f'  Candidacies: {c["total_candidacies"]}')
    print(f'  Seat_terms: {c["total_seat_terms"]}')


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Populate 2025 special election results')
    parser.add_argument('--state', type=str,
                        help='Process a single state (e.g., VA, MS)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and match only, no database inserts')
    args = parser.parse_args()

    if args.dry_run:
        print('DRY RUN MODE — no database changes will be made.\n')

    # Load JSON data
    with open(INPUT_PATH) as f:
        all_races = json.load(f)
    print(f'Loaded {len(all_races)} race records from {INPUT_PATH}')

    # Filter by state if specified
    if args.state:
        all_races = [r for r in all_races if r['state'] == args.state.upper()]
        print(f'Filtered to {len(all_races)} races for {args.state.upper()}')

    if not all_races:
        print('No races to process.')
        sys.exit(0)

    # Check for existing special elections in 2025
    time.sleep(2)
    existing = run_sql("""
        SELECT COUNT(*) as cnt FROM elections
        WHERE election_year = 2025 AND election_type LIKE 'Special%'
    """)
    if existing[0]['cnt'] > 0 and not args.dry_run:
        print(f'\nWARNING: {existing[0]["cnt"]} special elections already exist for 2025!')
        print('Aborting to prevent duplicates. Delete existing special election data first.')
        sys.exit(1)

    # Get list of states needed
    states_needed = sorted(set(r['state'] for r in all_races))
    print(f'\nStates: {", ".join(states_needed)}')

    # Load DB maps
    print(f'\n{"=" * 60}')
    print('LOADING DATABASE MAPS')
    print(f'{"=" * 60}')
    seat_map, current_terms, all_candidates, district_map = load_db_maps(states_needed)

    # Process races
    print(f'\n{"=" * 60}')
    print('PROCESSING RACES')
    print(f'{"=" * 60}')
    stats = process_all_races(
        all_races, seat_map, current_terms, all_candidates, district_map,
        dry_run=args.dry_run
    )

    # Summary
    print(f'\n{"=" * 60}')
    print('SUMMARY')
    print(f'{"=" * 60}')
    for key, val in stats.items():
        print(f'  {key}: {val}')

    if not args.dry_run:
        verify()

    print('\nDone!')


if __name__ == '__main__':
    main()
