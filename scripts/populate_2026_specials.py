"""
Populate 2026 special election data into the database.

Reads parsed JSON from download_2026_specials.py and creates:
- Election records (Special, Special_Primary, Special_Runoff)
- Former incumbent candidate records + closed seat_terms
- New candidate records (challengers)
- Candidacy records with vote results (for held elections only)
- Updated winner seat_terms (link election_id, set start_date)
- Placeholder election records for upcoming races (no candidacies)

Handles 3 categories:
  Cat 1: General held — full population with results
  Cat 2: Primary held, general upcoming — primary with results, general placeholder
  Cat 3: Nothing held — placeholder elections only
  Overlap: close seat_term only, no new elections

Usage:
    python3 scripts/populate_2026_specials.py --dry-run
    python3 scripts/populate_2026_specials.py
    python3 scripts/populate_2026_specials.py --state VA
"""
import sys
import re
import json
import time
import argparse
import unicodedata
from collections import Counter, defaultdict

import httpx

BATCH_SIZE = 400
INPUT_PATH = '/tmp/2026_special_results.json'

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
MA_HOUSE_MAP = {name: str(i + 1) for i, name in enumerate(sorted(MA_HOUSE_DISTRICTS_BP))}

# MA Senate district name → number mapping (alphabetical sort)
MA_SENATE_DISTRICTS_BP = [
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
MA_SENATE_MAP = {name: str(i + 1) for i, name in enumerate(sorted(MA_SENATE_DISTRICTS_BP))}

def resolve_db_district(race):
    """
    Map a special election race to its DB district_number and chamber.
    Returns (db_district_number, db_chamber, seat_designator, office_type)
    """
    state = race['state']
    chamber = race['chamber']
    district = race['district']

    seat_designator = None

    if state == 'MN' and chamber == 'House':
        match = re.match(r'(\d+)([AB])', district)
        if match:
            n = int(match.group(1))
            letter = match.group(2)
            db_num = (n - 1) * 2 + (1 if letter == 'A' else 2)
            return str(db_num), 'House', None, 'State House'

    if state == 'MA':
        if chamber == 'House':
            db_num = MA_HOUSE_MAP.get(district)
            if db_num:
                return db_num, 'House', None, 'State House'
            print(f'    WARNING: No MA House mapping for {district!r}')
            return None, None, None, None
        elif chamber == 'Senate':
            db_num = MA_SENATE_MAP.get(district)
            if db_num:
                return db_num, 'Senate', None, 'State Senate'
            print(f'    WARNING: No MA Senate mapping for {district!r}')
            return None, None, None, None

    if state == 'NH' and chamber == 'House':
        return district, 'House', None, 'State House'

    if state == 'NE' and chamber == 'Legislature':
        return district, 'Legislature', None, 'State Legislature'

    if state == 'ND' and chamber == 'House':
        # ND House: 2 seats per district (A + B). Find which seat is vacant
        # by matching former incumbent name in populate step.
        return district, 'House', None, 'State House'

    if state == 'WV' and chamber == 'Senate':
        # WV Senate: 2 seats per district (A + B). Need to identify which seat.
        # Will be resolved during find_seat() by matching incumbent name.
        return district, 'Senate', None, 'State Senate'

    # Map chamber names to DB chamber names
    chamber_map = {
        'House': 'House',
        'Senate': 'Senate',
        'Assembly': 'Assembly',
        'House of Delegates': 'House of Delegates',
    }
    db_chamber = chamber_map.get(chamber, chamber)

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
    """Load seats, seat_terms, candidates, and districts for needed states."""
    states_str = ','.join(f"'{s}'" for s in states_needed)

    print('  Loading seats...')
    seats = run_sql(f"""
        SELECT se.id as seat_id, se.office_type, se.seat_designator, se.seat_label,
               se.election_class,
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
            'election_class': s['election_class'],
        }
        d_key = (s['chamber'], s['district_number'])
        district_map[s['state']][d_key] = {
            'district_id': s['district_id'],
            'pres_2024_margin': s['pres_2024_margin'],
        }

    print(f'  Loaded {sum(len(v) for v in seat_map.values())} seats across {len(seat_map)} states')
    time.sleep(2)

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

    print('  Loading candidates...')
    all_candidates = run_sql("SELECT id, full_name FROM candidates")
    print(f'  Loaded {len(all_candidates)} candidates')

    # Load existing 2026 elections for overlap detection
    print('  Loading existing 2026 elections for overlap check...')
    existing_elections = run_sql(f"""
        SELECT e.id, e.seat_id, e.election_type, e.election_date
        FROM elections e
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation IN ({states_str})
          AND e.election_year = 2026
          AND e.election_type = 'General'
    """)
    existing_generals = {}
    for eg in existing_elections:
        existing_generals[eg['seat_id']] = eg['id']
    print(f'  Loaded {len(existing_generals)} existing 2026 General elections')
    time.sleep(2)

    return seat_map, current_terms, all_candidates, district_map, existing_generals

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

    # For multi-member districts (NH, ND, WV), find the specific seat
    if seat_designator is None:
        matching_seats = []
        for k, v in state_seats.items():
            if k[0] == office_type and k[1] == db_district:
                matching_seats.append((k, v))

        if len(matching_seats) == 1:
            return matching_seats[0][1], matching_seats[0][0]
        elif len(matching_seats) > 1:
            former = race.get('former_incumbent', '')
            if current_terms and former:
                for k, v in matching_seats:
                    term = current_terms.get(v['seat_id'])
                    if term:
                        sim = name_similarity(former, term['full_name'])
                        if sim >= 0.7:
                            return v, k
                # Check for vacant seats
                for k, v in matching_seats:
                    if v['seat_id'] not in current_terms:
                        return v, k

            # WV special handling: check election_class for seat timing
            if state == 'WV':
                # We need to figure out which seat (A or B) the former incumbent held
                # A = 2026 cycle, B = 2028 cycle. If the incumbent name doesn't match,
                # try vacancy-based matching
                for k, v in matching_seats:
                    term = current_terms.get(v['seat_id'])
                    if not term:
                        # Vacant seat — this is likely the one
                        return v, k

            # Fallback
            return matching_seats[0][1], matching_seats[0][0]

    print(f'    WARNING: No seat found for {state} {office_type} {db_district} (designator={seat_designator})')
    return None, None

def process_all_races(races, seat_map, current_terms, all_candidates, district_map,
                      existing_generals, dry_run=False):
    """Process all special election races."""
    stats = {
        'elections_created': 0,
        'elections_placeholder': 0,
        'former_incumbents_created': 0,
        'seat_terms_closed': 0,
        'new_candidates': 0,
        'candidacies_created': 0,
        'winner_terms_linked': 0,
        'winner_terms_created': 0,
        'skipped': 0,
        'overlaps': 0,
    }

    cand_index = {}
    for c in all_candidates:
        parts = c['full_name'].lower().split()
        if parts:
            last = parts[-1]
            if last not in cand_index:
                cand_index[last] = []
            cand_index[last].append((c['id'], c['full_name']))

    # Categorize races
    held_elections = []       # elections with parsed results (candidacies needed)
    placeholder_elections = [] # elections without results (Cat 2 generals, Cat 3 all)
    former_incumbents = []
    overlap_races = []

    for i, race in enumerate(races):
        state = race['state']
        category = race.get('category', 3)

        seat_info, seat_key = find_seat(race, seat_map, current_terms)
        if not seat_info:
            stats['skipped'] += 1
            continue

        seat_id = seat_info['seat_id']

        # Handle overlaps
        if category == 'overlap':
            overlap_races.append((i, race, seat_info, seat_key))
            stats['overlaps'] += 1
            continue

        # Check for actual overlap with existing general
        if seat_id in existing_generals:
            # This seat already has a 2026 General — if Nov 3 special, it overlaps
            gen_date = race.get('general_date', '')
            if gen_date == '2026-11-03':
                print(f'    {state} {race["chamber"]} {race["district"]}: '
                      f'Detected overlap with existing General (seat {seat_id})')
                overlap_races.append((i, race, seat_info, seat_key))
                stats['overlaps'] += 1
                continue

        # Check for runoff (affects how we interpret Special "winners")
        has_runoff = any(e['type'] == 'Special_Runoff' for e in race.get('elections', []))

        # Separate held vs placeholder elections
        for elec in race.get('elections', []):
            etype = elec['type']

            if elec.get('candidates') and len(elec['candidates']) > 0:
                # Has results — held election
                if etype == 'Special' and has_runoff:
                    winners = [c for c in elec['candidates'] if c.get('is_winner')]
                    if len(winners) > 1:
                        for c in elec['candidates']:
                            if c.get('is_winner'):
                                c['advanced_to_runoff'] = True
                held_elections.append((i, elec, seat_info, seat_key))
            else:
                # No results — placeholder
                placeholder_elections.append((i, elec, seat_info, seat_key))

        # For Cat 2: create placeholder for the upcoming general
        if category == 2 and not any(e['type'] == 'Special' for e in race.get('elections', [])):
            # General hasn't happened — create placeholder
            pres_margin = seat_info.get('pres_2024_margin')
            placeholder_elections.append((i, {
                'type': 'Special',
                'date': race['general_date'],
                'total_votes': None,
                'candidates': [],
            }, seat_info, seat_key))

        # For Cat 3 races with no parsed elections: create all placeholders
        if not race.get('elections'):
            pres_margin = seat_info.get('pres_2024_margin')
            # Create Special (general) placeholder
            placeholder_elections.append((i, {
                'type': 'Special',
                'date': race['general_date'],
                'total_votes': None,
                'candidates': [],
            }, seat_info, seat_key))
            # Create Special_Primary placeholder if applicable
            if race.get('primary_date'):
                if state == 'LA':
                    # LA: jungle primary = Special. The general_date = runoff date.
                    # Swap: the placeholder Special should be at primary_date (jungle primary)
                    # and if needed a Special_Runoff at general_date
                    # But for Cat 3, nothing is held, so just create both
                    placeholder_elections[-1] = (i, {
                        'type': 'Special',
                        'date': race['primary_date'],
                        'total_votes': None,
                        'candidates': [],
                    }, seat_info, seat_key)
                    placeholder_elections.append((i, {
                        'type': 'Special_Runoff',
                        'date': race['general_date'],
                        'total_votes': None,
                        'candidates': [],
                    }, seat_info, seat_key))
                else:
                    placeholder_elections.append((i, {
                        'type': 'Special_Primary',
                        'date': race['primary_date'],
                        'total_votes': None,
                        'candidates': [],
                    }, seat_info, seat_key))

        # Former incumbent handling — for ALL races (including overlaps handled above)
        former_name = race['former_incumbent']
        vacancy_reason = race['vacancy_reason']
        dates = []
        for e in race.get('elections', []):
            if e.get('date'):
                dates.append(e['date'])
        if not dates and race.get('general_date'):
            dates.append(race['general_date'])
        if not dates and race.get('primary_date'):
            dates.append(race['primary_date'])

        if dates:
            from datetime import datetime, timedelta
            earliest = min(dates)
            end_dt = datetime.strptime(earliest, '%Y-%m-%d') - timedelta(days=1)
            end_date = end_dt.strftime('%Y-%m-%d')
        else:
            end_date = None

        former_incumbents.append((i, former_name, seat_id, vacancy_reason, end_date))

    # Also close seat_terms for overlap races
    for i, race, seat_info, seat_key in overlap_races:
        former_name = race['former_incumbent']
        vacancy_reason = race['vacancy_reason']
        seat_id = seat_info['seat_id']
        gen_date = race.get('general_date', '')
        from datetime import datetime, timedelta
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL
        if gen_date:
            end_dt = datetime.strptime(gen_date, '%Y-%m-%d') - timedelta(days=1)
            end_date = end_dt.strftime('%Y-%m-%d')
        else:
            end_date = None
        former_incumbents.append((i, former_name, seat_id, vacancy_reason, end_date))

    print(f'\n  Elections with results to create: {len(held_elections)}')
    print(f'  Placeholder elections to create: {len(placeholder_elections)}')
    print(f'  Former incumbents to process: {len(former_incumbents)}')
    print(f'  Overlap races (seat_term only): {len(overlap_races)}')

    if dry_run:
        etype_counts = Counter()
        for _, elec, _, _ in held_elections:
            etype_counts[f'{elec["type"]} (held)'] += 1
        for _, elec, _, _ in placeholder_elections:
            etype_counts[f'{elec["type"]} (placeholder)'] += 1
        print(f'\n  Election types:')
        for etype, cnt in sorted(etype_counts.items()):
            print(f'    {etype}: {cnt}')

        total_cands = sum(len(e[1].get('candidates', [])) for e in held_elections)
        print(f'  Total candidacies: {total_cands}')
        stats['elections_created'] = len(held_elections)
        stats['elections_placeholder'] = len(placeholder_elections)
        stats['former_incumbents_created'] = len(former_incumbents)
        return stats

    # ── STEP 2a: Create former incumbent candidates + close seat_terms ──
    print('\n  Processing former incumbents...')

    new_former_cands = []
    existing_former = []
    skipped_incumbents = []

    for i, former_name, seat_id, vacancy_reason, end_date in former_incumbents:
        term_info = current_terms.get(seat_id)

        if term_info and name_similarity(former_name, term_info['full_name']) >= 0.7:
            print(f'    SKIP: {former_name} is still current holder of seat {seat_id} (won their special)')
            skipped_incumbents.append(seat_id)
            continue

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

        for idx, (former_name, seat_id, vacancy_reason, end_date) in enumerate(new_former_cands):
            cand_id = new_ids[idx]
            existing_former.append((cand_id, former_name, seat_id, vacancy_reason, end_date))
            parts = former_name.lower().split()
            if parts:
                last = parts[-1]
                if last not in cand_index:
                    cand_index[last] = []
                cand_index[last].append((cand_id, former_name))

    # Close seat_terms
    end_reason_map = {
        'resigned': 'resigned', 'died': 'died',
        'removed': 'removed', 'appointed_elsewhere': 'appointed_elsewhere',
    }

    if existing_former:
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

        close_sqls = []
        insert_terms = []
        for cand_id, former_name, seat_id, vacancy_reason, end_date in existing_former:
            end_reason = end_reason_map.get(vacancy_reason, 'resigned')
            end_date_sql = f"'{end_date}'" if end_date else 'NULL'
            ot = open_terms_by_seat_cand.get((seat_id, cand_id))
            if ot:
                close_sqls.append(
                    f"UPDATE seat_terms SET end_date = {end_date_sql}, "
                    f"end_reason = '{end_reason}' WHERE id = {ot['id']}"
                )
            else:
                # Check if there's ANY open term on this seat (name mismatch)
                any_open = None
                for (sid, cid), term in open_terms_by_seat_cand.items():
                    if sid == seat_id:
                        any_open = term
                        break
                if any_open:
                    close_sqls.append(
                        f"UPDATE seat_terms SET end_date = {end_date_sql}, "
                        f"end_reason = '{end_reason}' WHERE id = {any_open['id']}"
                    )
                    print(f'    WARNING: Name mismatch for seat {seat_id}: '
                          f'expected {former_name}, closing term for candidate {any_open["candidate_id"]}')
                else:
                    insert_terms.append(
                        f"({seat_id}, {cand_id}, NULL, NULL, {end_date_sql}, "
                        f"'elected', '{end_reason}', NULL, NULL)"
                    )

        if close_sqls:
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

    # Also update seats.current_holder cache for closed terms
    if close_sqls or insert_terms:
        closed_seat_ids = set()
        for _, _, seat_id, _, _ in existing_former:
            if seat_id not in skipped_incumbents:
                closed_seat_ids.add(seat_id)
        if closed_seat_ids:
            update_sqls = [
                f"UPDATE seats SET current_holder = NULL, current_holder_party = NULL, "
                f"current_holder_caucus = NULL WHERE id = {sid}"
                for sid in closed_seat_ids
            ]
            for batch_start in range(0, len(update_sqls), 20):
                batch = update_sqls[batch_start:batch_start + 20]
                run_sql(";\n".join(batch))
                time.sleep(2)
            print(f'    Cleared current_holder cache for {len(closed_seat_ids)} seats')

    time.sleep(2)

    # ── STEP 2b: Create election records ──
    print('\n  Creating election records...')

    # Combine held + placeholder elections, create Specials first for linking
    all_elections = held_elections + placeholder_elections
    elections_by_seat = defaultdict(list)
    for idx, (race_idx, elec, seat_info, seat_key) in enumerate(all_elections):
        elections_by_seat[seat_info['seat_id']].append((idx, race_idx, elec, seat_info))

    election_ids = {}  # idx → election_id
    held_count = len(held_elections)

    # Phase 1: Create all Special (general) elections first
    special_inserts = []
    non_special_items = []

    for seat_id, seat_elections in elections_by_seat.items():
        for idx, race_idx, elec, seat_info in seat_elections:
            etype = elec['type']
            date = elec.get('date')
            total_votes = elec.get('total_votes') or 0
            pres_margin = seat_info.get('pres_2024_margin')
            is_held = idx < held_count

            if etype == 'Special':
                special_inserts.append((idx, seat_id, date, pres_margin, total_votes, is_held))
            else:
                non_special_items.append((idx, seat_id, elec, seat_info, is_held))

    if special_inserts:
        values = []
        for idx, seat_id, date, pres_margin, total_votes, is_held in special_inserts:
            date_sql = f"'{date}'" if date else 'NULL'
            pm_sql = f"'{esc(pres_margin)}'" if pres_margin else 'NULL'
            tv_sql = total_votes if total_votes else 'NULL'
            rs_sql = "'Certified'" if is_held else 'NULL'
            values.append(
                f"({seat_id}, {date_sql}, 2026, 'Special', NULL, {pm_sql}, {tv_sql}, {rs_sql})"
            )

        for batch_start in range(0, len(values), 50):
            batch = values[batch_start:batch_start + 50]
            batch_indices = [special_inserts[j][0] for j in range(batch_start, min(batch_start + 50, len(values)))]
            result = run_sql(
                "INSERT INTO elections (seat_id, election_date, election_year, election_type, "
                "related_election_id, pres_margin_this_cycle, total_votes_cast, "
                "result_status) VALUES\n"
                + ",\n".join(batch)
                + "\nRETURNING id;"
            )
            for j, r in enumerate(result):
                election_ids[batch_indices[j]] = r['id']
            stats['elections_created'] += sum(1 for s in special_inserts[batch_start:batch_start+50] if s[5])
            stats['elections_placeholder'] += sum(1 for s in special_inserts[batch_start:batch_start+50] if not s[5])
            time.sleep(2)

    seat_to_special = {}
    for idx, seat_id, date, pres_margin, total_votes, is_held in special_inserts:
        seat_to_special[seat_id] = election_ids[idx]

    # Phase 2: Non-Special elections (primaries, runoffs)
    if non_special_items:
        values = []
        ns_indices = []
        for idx, seat_id, elec, seat_info, is_held in non_special_items:
            etype = elec['type']
            date = elec.get('date')
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
            rs_sql = "'Certified'" if is_held else 'NULL'

            values.append(
                f"({seat_id}, {date_sql}, 2026, '{db_etype}', {related_sql}, {pm_sql}, {tv_sql}, {rs_sql})"
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
            for j, r in enumerate(result):
                election_ids[batch_indices[j]] = r['id']
            for j in range(batch_start, min(batch_start + 50, len(non_special_items))):
                if non_special_items[j][4]:  # is_held
                    stats['elections_created'] += 1
                else:
                    stats['elections_placeholder'] += 1
            time.sleep(2)

    print(f'  Created {stats["elections_created"]} held + {stats["elections_placeholder"]} placeholder elections')
    time.sleep(2)

    # ── STEP 2c: Create candidates + candidacies (held elections only) ──
    print('\n  Creating candidacies...')

    all_candidacies = []

    for idx, (race_idx, elec, seat_info, seat_key) in enumerate(held_elections):
        election_id = election_ids.get(idx)
        if not election_id:
            continue

        seat_id = seat_info['seat_id']
        race = races[race_idx]
        etype = elec['type']
        term_info = current_terms.get(seat_id)

        for cand in elec.get('candidates', []):
            cand_name = cand['name']
            cand_party = cand['party']
            votes = cand.get('votes')
            vote_pct = cand.get('vote_pct')
            is_winner = cand.get('is_winner', False)

            candidate_id = None
            is_inc = False

            if term_info:
                sim = name_similarity(cand_name, term_info['full_name'])
                if sim >= 0.7:
                    candidate_id = term_info['candidate_id']

            if not candidate_id:
                parts = cand_name.lower().split()
                if parts:
                    last = parts[-1]
                    for cid, cname in cand_index.get(last, []):
                        sim = name_similarity(cand_name, cname)
                        if sim >= 0.7:
                            candidate_id = cid
                            break

            if cand.get('advanced_to_runoff'):
                result = 'Advanced'
            elif is_winner:
                result = 'Won'
            else:
                result = 'Lost'

            if etype == 'Special_Primary_D' or etype == 'Special_Primary_Runoff_D':
                cand_party = cand_party or 'D'
            elif etype == 'Special_Primary_R' or etype == 'Special_Primary_Runoff_R':
                cand_party = cand_party or 'R'

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

        for j, m in enumerate(new_cands):
            m['candidate_id'] = new_ids[j]
            parts = m['candidate_name'].lower().split()
            if parts:
                last = parts[-1]
                if last not in cand_index:
                    cand_index[last] = []
                cand_index[last].append((new_ids[j], m['candidate_name']))

    # Insert candidacies
    if all_candidacies:
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

    # ── STEP 2d: Link/create winner seat_terms ──
    print('\n  Linking winner seat_terms...')

    seat_winners = {}
    for m in all_candidacies:
        if m['is_winner'] and m['result'] == 'Won':
            sid = m['seat_id']
            if m['etype'] == 'Special_Runoff':
                seat_winners[sid] = m
            elif m['etype'] == 'Special' and sid not in seat_winners:
                seat_winners[sid] = m

    term_updates = []
    new_winner_terms = []
    unlinked = []

    for sid, m in seat_winners.items():
        candidate_id = m['candidate_id']
        election_id = m['election_id']
        party = m.get('party')

        term_info = current_terms.get(sid)
        if term_info and term_info['candidate_id'] == candidate_id:
            term_updates.append((term_info['term_id'], election_id))
        elif term_info:
            sim = name_similarity(m['candidate_name'], term_info['full_name'])
            if sim >= 0.7:
                term_updates.append((term_info['term_id'], election_id))
            else:
                # Current holder doesn't match winner — create new term
                new_winner_terms.append((sid, candidate_id, party, election_id, m['candidate_name']))
        else:
            # No current holder — create new term
            new_winner_terms.append((sid, candidate_id, party, election_id, m['candidate_name']))

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

    if new_winner_terms:
        values = []
        seat_holder_updates = []
        for sid, cand_id, party, election_id, cand_name in new_winner_terms:
            party_sql = f"'{esc(party)}'" if party else 'NULL'
            values.append(
                f"({sid}, {cand_id}, {party_sql}, NULL, NULL, "
                f"'elected', NULL, {party_sql}, {election_id})"
            )
            seat_holder_updates.append(
                f"UPDATE seats SET current_holder = '{esc(cand_name)}', "
                f"current_holder_party = {party_sql}, "
                f"current_holder_caucus = {party_sql} WHERE id = {sid}"
            )

        run_sql(
            "INSERT INTO seat_terms (seat_id, candidate_id, party, "
            "start_date, end_date, start_reason, end_reason, caucus, election_id) VALUES\n"
            + ",\n".join(values)
        )
        stats['winner_terms_created'] = len(new_winner_terms)
        time.sleep(2)

        # Update current_holder cache for winners
        for batch_start in range(0, len(seat_holder_updates), 20):
            batch = seat_holder_updates[batch_start:batch_start + 20]
            run_sql(";\n".join(batch))
            time.sleep(2)

    for sid, winner, holder in unlinked:
        print(f'    WARNING: Winner {winner} != current holder {holder} for seat {sid}')

    print(f'  Linked {stats["winner_terms_linked"]} existing seat_terms')
    print(f'  Created {stats["winner_terms_created"]} new winner seat_terms')

    return stats

# ══════════════════════════════════════════════════════════════════════
# VERIFICATION
# ══════════════════════════════════════════════════════════════════════

def verify():
    print(f'\n{"=" * 60}')
    print('VERIFICATION')
    print(f'{"=" * 60}')

    r = run_sql("""
        SELECT election_type, result_status, COUNT(*) as cnt FROM elections
        WHERE election_year = 2026 AND election_type LIKE 'Special%'
        GROUP BY election_type, result_status ORDER BY election_type, result_status
    """)
    print('Special elections by type and status:')
    for row in r:
        rs = row.get('result_status') or 'NULL'
        print(f'  {row["election_type"]} [{rs}]: {row["cnt"]}')
    time.sleep(1)

    r = run_sql("""
        SELECT result, COUNT(*) as cnt FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        WHERE e.election_type LIKE 'Special%' AND e.election_year = 2026
        GROUP BY result ORDER BY result
    """)
    print('\nCandidacy results:')
    for row in r:
        print(f'  {row["result"]}: {row["cnt"]}')
    time.sleep(1)

    r = run_sql("""
        SELECT ca.party, COUNT(*) as cnt FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        WHERE e.election_type = 'Special' AND ca.result = 'Won' AND e.election_year = 2026
        GROUP BY ca.party ORDER BY COUNT(*) DESC
    """)
    print('\nSpecial general winners by party:')
    for row in r:
        print(f'  {row["party"]}: {row["cnt"]}')
    time.sleep(1)

    r = run_sql("""
        SELECT st.abbreviation, se.seat_label, e.election_type, e.election_date, e.result_status
        FROM elections e JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id JOIN states st ON d.state_id = st.id
        WHERE e.election_year = 2026 AND e.election_type LIKE 'Special%' AND e.result_status IS NULL
        ORDER BY e.election_date
    """)
    print(f'\nUpcoming special elections ({len(r)} total):')
    for row in r:
        print(f'  {row["abbreviation"]} {row["seat_label"]}: {row["election_type"]} on {row["election_date"]}')
    time.sleep(1)

    r = run_sql("""
        SELECT se.seat_label, c.full_name, ca.party, ca.votes_received,
               ca.vote_percentage, ca.result, e.election_type
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN candidates c ON ca.candidate_id = c.id
        WHERE e.election_type = 'Special' AND e.election_year = 2026 AND ca.result = 'Won'
        ORDER BY se.seat_label
    """)
    print(f'\nSpecial general winners:')
    for row in r:
        print(f'  {row["seat_label"]}: {row["full_name"]} ({row["party"]}) '
              f'{row["votes_received"]} votes ({row["vote_percentage"]}%)')
    time.sleep(1)

    r = run_sql("""
        SELECT ca.election_id, ca.candidate_id, COUNT(*) as cnt
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        WHERE e.election_year = 2026 AND e.election_type LIKE 'Special%'
        GROUP BY ca.election_id, ca.candidate_id HAVING COUNT(*) > 1
    """)
    if r:
        print(f'\nWARNING: {len(r)} duplicate candidacies!')
    else:
        print('\nNo duplicate candidacies.')

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
    parser = argparse.ArgumentParser(description='Populate 2026 special election data')
    parser.add_argument('--state', type=str,
                        help='Process a single state (e.g., VA, LA)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and match only, no database inserts')
    args = parser.parse_args()

    if args.dry_run:
        print('DRY RUN MODE — no database changes will be made.\n')

    with open(INPUT_PATH) as f:
        all_races = json.load(f)
    print(f'Loaded {len(all_races)} race records from {INPUT_PATH}')

    if args.state:
        all_races = [r for r in all_races if r['state'] == args.state.upper()]
        print(f'Filtered to {len(all_races)} races for {args.state.upper()}')

    if not all_races:
        print('No races to process.')
        sys.exit(0)

    # Check for existing 2026 special elections
    time.sleep(2)
    existing = run_sql("""
        SELECT COUNT(*) as cnt FROM elections
        WHERE election_year = 2026 AND election_type LIKE 'Special%'
    """)
    if existing[0]['cnt'] > 0 and not args.dry_run:
        print(f'\nWARNING: {existing[0]["cnt"]} special elections already exist for 2026!')
        print('Aborting to prevent duplicates. Delete existing special election data first.')
        sys.exit(1)

    states_needed = sorted(set(r['state'] for r in all_races))
    print(f'\nStates: {", ".join(states_needed)}')

    print(f'\n{"=" * 60}')
    print('LOADING DATABASE MAPS')
    print(f'{"=" * 60}')
    seat_map, current_terms, all_candidates, district_map, existing_generals = load_db_maps(states_needed)

    print(f'\n{"=" * 60}')
    print('PROCESSING RACES')
    print(f'{"=" * 60}')
    stats = process_all_races(
        all_races, seat_map, current_terms, all_candidates, district_map,
        existing_generals, dry_run=args.dry_run
    )

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
