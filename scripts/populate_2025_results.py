"""
Populate 2025 general election results for Virginia and New Jersey.

Reads parsed JSON from download_2025_results.py and creates:
- Election records (2025 generals)
- New candidate records (challengers)
- Candidacy records with votes and results

Usage:
    python3 scripts/populate_2025_results.py --dry-run
    python3 scripts/populate_2025_results.py
    python3 scripts/populate_2025_results.py --state VA
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
INPUT_PATH = '/tmp/2025_election_results.json'


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
# STEP 1: Load DB Maps
# ══════════════════════════════════════════════════════════════════════

def load_seat_map(state_abbrev):
    """
    Load seat data for VA or NJ.

    Returns:
        legislative_seats: {(office_type, district_number, seat_designator_or_None) -> seat_id}
        statewide_seats: {office_type -> seat_id}
        incumbent_map: {seat_id -> (candidate_id, full_name, party)}
        district_map: {(chamber, district_number) -> district_id}  # for pres margin
    """
    # Legislative seats
    seats = run_sql(f"""
        SELECT se.id as seat_id, se.office_type, se.seat_designator,
               d.district_number, d.num_seats, d.id as district_id, d.chamber,
               d.pres_2024_margin
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}'
          AND se.selection_method = 'Elected'
          AND d.office_level = 'Legislative'
        ORDER BY se.office_type, d.district_number, se.seat_designator
    """)

    legislative_seats = {}
    district_map = {}
    for s in seats:
        key = (s['office_type'], s['district_number'], s['seat_designator'])
        legislative_seats[key] = s['seat_id']
        d_key = (s['chamber'], s['district_number'])
        district_map[d_key] = {
            'district_id': s['district_id'],
            'pres_2024_margin': s['pres_2024_margin'],
        }

    # Statewide seats
    sw_seats = run_sql(f"""
        SELECT se.id as seat_id, se.office_type,
               d.id as district_id, d.pres_2024_margin
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state_abbrev}'
          AND se.selection_method = 'Elected'
          AND d.office_level = 'Statewide'
    """)

    statewide_seats = {}
    for s in sw_seats:
        statewide_seats[s['office_type']] = s['seat_id']
        district_map[('Statewide', 'Statewide')] = {
            'district_id': s['district_id'],
            'pres_2024_margin': s['pres_2024_margin'],
        }

    # Incumbents (current seat_terms)
    all_seat_ids = list(legislative_seats.values()) + list(statewide_seats.values())
    seat_ids_str = ','.join(str(sid) for sid in all_seat_ids)
    incumbents = run_sql(f"""
        SELECT st.seat_id, st.candidate_id, c.full_name, st.party
        FROM seat_terms st
        JOIN candidates c ON st.candidate_id = c.id
        WHERE st.seat_id IN ({seat_ids_str})
          AND st.end_date IS NULL
    """)

    incumbent_map = {}
    for inc in incumbents:
        incumbent_map[inc['seat_id']] = (inc['candidate_id'], inc['full_name'], inc['party'])

    return legislative_seats, statewide_seats, incumbent_map, district_map


# ══════════════════════════════════════════════════════════════════════
# STEP 2: Create Elections
# ══════════════════════════════════════════════════════════════════════

def create_elections(races, legislative_seats, statewide_seats, district_map, dry_run=False):
    """
    Create General election records for 2025.

    Returns: {race_key -> election_id} where race_key is
        (office_type, district_number, seat_designator) for legislative
        (office_type, 'Statewide', None) for statewide
    """
    election_map = {}
    values = []

    for race in races:
        state = race['state']
        office_type = race['office_type']
        district_number = race['district_number']
        total_votes = race['total_votes']

        # Get pres margin for this district
        d_key = (race['chamber'], district_number)
        d_info = district_map.get(d_key, {})
        pres_margin = d_info.get('pres_2024_margin')
        pres_margin_sql = f"'{esc(pres_margin)}'" if pres_margin else 'NULL'
        total_sql = total_votes if total_votes else 'NULL'

        if race['chamber'] == 'Statewide':
            # Map office_type string to DB office_type
            db_office_type = race['office_type']
            seat_id = statewide_seats.get(db_office_type)
            if not seat_id:
                print(f'    WARNING: No seat for {db_office_type} in {state}')
                continue
            key = (db_office_type, 'Statewide', None)
            values.append(
                f"({seat_id}, '2025-11-04', 2025, 'General', NULL, NULL, NULL, "
                f"{pres_margin_sql}, NULL, {total_sql}, NULL)"
            )
            election_map[key] = None  # placeholder, will be filled with actual ID
        else:
            # Legislative: for NJ Assembly, need to create per-seat elections
            if state == 'NJ' and office_type == 'State House':
                # Two seats per district (A and B)
                for designator in ['A', 'B']:
                    seat_key = (office_type, district_number, designator)
                    seat_id = legislative_seats.get(seat_key)
                    if not seat_id:
                        print(f'    WARNING: No seat for NJ Assembly D{district_number} Seat {designator}')
                        continue
                    values.append(
                        f"({seat_id}, '2025-11-04', 2025, 'General', NULL, NULL, NULL, "
                        f"{pres_margin_sql}, NULL, {total_sql}, NULL)"
                    )
                    election_map[seat_key] = None
            else:
                # Single seat (VA HoD)
                seat_key = (office_type, district_number, None)
                seat_id = legislative_seats.get(seat_key)
                if not seat_id:
                    print(f'    WARNING: No seat for {office_type} D{district_number} in {state}')
                    continue
                values.append(
                    f"({seat_id}, '2025-11-04', 2025, 'General', NULL, NULL, NULL, "
                    f"{pres_margin_sql}, NULL, {total_sql}, NULL)"
                )
                election_map[seat_key] = None

    print(f'  Elections to create: {len(values)}')

    if dry_run:
        # Assign fake IDs for dry run
        for i, key in enumerate(election_map.keys()):
            election_map[key] = 90000 + i
        return election_map

    # Insert elections in batches
    all_ids = []
    for batch_start in range(0, len(values), BATCH_SIZE):
        batch = values[batch_start:batch_start + BATCH_SIZE]
        sql = (
            "INSERT INTO elections (seat_id, election_date, election_year, election_type, "
            "related_election_id, filing_deadline, forecast_rating, "
            "pres_margin_this_cycle, previous_result_margin, total_votes_cast, notes) VALUES\n"
            + ",\n".join(batch)
            + "\nRETURNING id;"
        )
        result = run_sql(sql, exit_on_error=False)
        if result is None:
            print('    Batch failed, retrying in 2s...')
            time.sleep(2)
            result = run_sql(sql)
        all_ids.extend(r['id'] for r in result)
        time.sleep(0.8)

    print(f'  Inserted {len(all_ids)} elections')

    # Map IDs back to keys
    keys_list = list(election_map.keys())
    if len(all_ids) != len(keys_list):
        print(f'  ERROR: Expected {len(keys_list)} IDs, got {len(all_ids)}')
        sys.exit(1)
    for i, key in enumerate(keys_list):
        election_map[key] = all_ids[i]

    return election_map


# ══════════════════════════════════════════════════════════════════════
# STEP 3: Match and Create Candidates + Candidacies
# ══════════════════════════════════════════════════════════════════════

def process_candidacies(races, election_map, legislative_seats, statewide_seats,
                        incumbent_map, dry_run=False):
    """
    Match candidates to DB records, create new candidates as needed,
    and insert candidacies with vote results.
    """
    all_candidacies = []  # list of dicts to insert

    for race in races:
        state = race['state']
        office_type = race['office_type']
        district_number = race['district_number']

        if race['chamber'] == 'Statewide':
            db_office_type = race['office_type']
            seat_id = statewide_seats.get(db_office_type)
            if not seat_id:
                continue
            key = (db_office_type, 'Statewide', None)
            election_id = election_map.get(key)
            if not election_id:
                continue

            inc_info = incumbent_map.get(seat_id)

            for cand in race['candidates']:
                candidate_id = None
                is_inc = False
                if inc_info:
                    sim = name_similarity(cand['name'], inc_info[1])
                    if sim >= 0.7:
                        candidate_id = inc_info[0]
                        is_inc = True

                all_candidacies.append({
                    'election_id': election_id,
                    'candidate_id': candidate_id,
                    'candidate_name': cand['name'],
                    'party': cand['party'],
                    'is_incumbent': is_inc or cand['is_incumbent'],
                    'votes': cand['votes'],
                    'vote_pct': cand['vote_pct'],
                    'result': 'Won' if cand['is_winner'] else 'Lost',
                    'seat_id': seat_id,
                })

        elif state == 'NJ' and office_type == 'State House':
            # NJ Assembly: 2 winners per district, assign to Seat A and B
            winners = [c for c in race['candidates'] if c['is_winner']]
            losers = [c for c in race['candidates'] if not c['is_winner']]

            # Try to match winners to their incumbent seats
            seat_a_id = legislative_seats.get((office_type, district_number, 'A'))
            seat_b_id = legislative_seats.get((office_type, district_number, 'B'))

            if not seat_a_id or not seat_b_id:
                print(f'    WARNING: Missing seats for NJ Assembly D{district_number}')
                continue

            inc_a = incumbent_map.get(seat_a_id)
            inc_b = incumbent_map.get(seat_b_id)

            # Assign winners to seats
            winner_assignments = {}  # candidate -> (seat_id, seat_designator)
            assigned_seats = set()

            # First pass: match incumbents to their seats
            for w in winners:
                if inc_a and inc_a[0] not in [v[0] for v in winner_assignments.values()]:
                    sim = name_similarity(w['name'], inc_a[1])
                    if sim >= 0.7 and seat_a_id not in assigned_seats:
                        winner_assignments[w['name']] = (seat_a_id, 'A', inc_a[0], True)
                        assigned_seats.add(seat_a_id)
                        continue
                if inc_b and inc_b[0] not in [v[0] for v in winner_assignments.values()]:
                    sim = name_similarity(w['name'], inc_b[1])
                    if sim >= 0.7 and seat_b_id not in assigned_seats:
                        winner_assignments[w['name']] = (seat_b_id, 'B', inc_b[0], True)
                        assigned_seats.add(seat_b_id)

            # Second pass: assign remaining winners to available seats
            for w in winners:
                if w['name'] not in winner_assignments:
                    if seat_a_id not in assigned_seats:
                        winner_assignments[w['name']] = (seat_a_id, 'A', None, False)
                        assigned_seats.add(seat_a_id)
                    elif seat_b_id not in assigned_seats:
                        winner_assignments[w['name']] = (seat_b_id, 'B', None, False)
                        assigned_seats.add(seat_b_id)

            # Create candidacies for winners (each on their assigned seat's election)
            for w in winners:
                if w['name'] not in winner_assignments:
                    print(f'    WARNING: Could not assign seat for winner {w["name"]} in NJ D{district_number}')
                    continue
                seat_id, designator, cand_id, is_inc = winner_assignments[w['name']]
                key = (office_type, district_number, designator)
                election_id = election_map.get(key)
                if not election_id:
                    continue

                all_candidacies.append({
                    'election_id': election_id,
                    'candidate_id': cand_id,
                    'candidate_name': w['name'],
                    'party': w['party'],
                    'is_incumbent': is_inc or w['is_incumbent'],
                    'votes': w['votes'],
                    'vote_pct': w['vote_pct'],
                    'result': 'Won',
                    'seat_id': seat_id,
                })

            # Create candidacies for losers on Seat A election
            key_a = (office_type, district_number, 'A')
            election_id_a = election_map.get(key_a)
            if election_id_a:
                for l in losers:
                    # Check if loser is an incumbent of either seat
                    cand_id = None
                    is_inc = False
                    if inc_a:
                        sim = name_similarity(l['name'], inc_a[1])
                        if sim >= 0.7:
                            cand_id = inc_a[0]
                            is_inc = True
                    if not cand_id and inc_b:
                        sim = name_similarity(l['name'], inc_b[1])
                        if sim >= 0.7:
                            cand_id = inc_b[0]
                            is_inc = True

                    all_candidacies.append({
                        'election_id': election_id_a,
                        'candidate_id': cand_id,
                        'candidate_name': l['name'],
                        'party': l['party'],
                        'is_incumbent': is_inc or l['is_incumbent'],
                        'votes': l['votes'],
                        'vote_pct': l['vote_pct'],
                        'result': 'Lost',
                        'seat_id': seat_a_id,
                    })

        else:
            # VA House of Delegates: single seat
            seat_key = (office_type, district_number, None)
            seat_id = legislative_seats.get(seat_key)
            if not seat_id:
                continue
            election_id = election_map.get(seat_key)
            if not election_id:
                continue

            inc_info = incumbent_map.get(seat_id)

            for cand in race['candidates']:
                candidate_id = None
                is_inc = False
                if inc_info:
                    sim = name_similarity(cand['name'], inc_info[1])
                    if sim >= 0.7:
                        candidate_id = inc_info[0]
                        is_inc = True

                all_candidacies.append({
                    'election_id': election_id,
                    'candidate_id': candidate_id,
                    'candidate_name': cand['name'],
                    'party': cand['party'],
                    'is_incumbent': is_inc or cand['is_incumbent'],
                    'votes': cand['votes'],
                    'vote_pct': cand['vote_pct'],
                    'result': 'Won' if cand['is_winner'] else 'Lost',
                    'seat_id': seat_id,
                })

    # Summary
    reuse = [c for c in all_candidacies if c['candidate_id'] is not None]
    new = [c for c in all_candidacies if c['candidate_id'] is None]
    print(f'  Total candidacies: {len(all_candidacies)}')
    print(f'    Incumbent/existing candidates: {len(reuse)}')
    print(f'    New candidates needed: {len(new)}')

    if dry_run:
        return len(new), len(all_candidacies)

    # Insert new candidates
    new_candidate_ids = []
    if new:
        values = []
        for m in new:
            parts = m['candidate_name'].split()
            first = esc(parts[0]) if parts else ''
            last = esc(parts[-1]) if len(parts) > 1 else esc(parts[0]) if parts else ''
            full = esc(m['candidate_name'])
            values.append(f"('{full}', '{first}', '{last}', NULL)")

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
            new_candidate_ids.extend(r['id'] for r in result)

        print(f'  Inserted {len(new_candidate_ids)} new candidates')
        if len(new_candidate_ids) != len(new):
            print(f'  ERROR: Expected {len(new)}, got {len(new_candidate_ids)}')
            sys.exit(1)

    # Assign new candidate_ids
    for i, m in enumerate(new):
        m['candidate_id'] = new_candidate_ids[i]

    # Insert candidacies
    values = []
    for m in all_candidacies:
        votes_sql = m['votes'] if m['votes'] is not None else 'NULL'
        pct_sql = m['vote_pct'] if m['vote_pct'] is not None else 'NULL'
        values.append(
            f"({m['election_id']}, {m['candidate_id']}, '{esc(m['party'])}', "
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

    print(f'  Inserted {total_inserted} candidacies')
    return len(new), total_inserted


# ══════════════════════════════════════════════════════════════════════
# VERIFICATION
# ══════════════════════════════════════════════════════════════════════

def verify():
    """Run verification queries."""
    print(f'\n{"=" * 60}')
    print('VERIFICATION')
    print(f'{"=" * 60}')

    # Election count
    r = run_sql("SELECT COUNT(*) as cnt FROM elections WHERE election_year = 2025")
    print(f'2025 elections: {r[0]["cnt"]}')

    # Candidacies
    r = run_sql("""
        SELECT COUNT(*) as cnt FROM candidacies c
        JOIN elections e ON c.election_id = e.id WHERE e.election_year = 2025
    """)
    print(f'2025 candidacies: {r[0]["cnt"]}')

    # By result
    r = run_sql("""
        SELECT c.result, COUNT(*) as cnt FROM candidacies c
        JOIN elections e ON c.election_id = e.id WHERE e.election_year = 2025
        GROUP BY c.result ORDER BY c.result
    """)
    for row in r:
        print(f'  {row["result"]}: {row["cnt"]}')

    # By state and office type
    r = run_sql("""
        SELECT s.abbreviation, se.office_type, COUNT(*) as elections,
               SUM(CASE WHEN ca.result = 'Won' THEN 1 ELSE 0 END) as winners,
               SUM(CASE WHEN ca.party = 'D' AND ca.result = 'Won' THEN 1 ELSE 0 END) as d_wins,
               SUM(CASE WHEN ca.party = 'R' AND ca.result = 'Won' THEN 1 ELSE 0 END) as r_wins
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE e.election_year = 2025
        GROUP BY s.abbreviation, se.office_type
        ORDER BY s.abbreviation, se.office_type
    """)
    print('\nBy state/office:')
    for row in r:
        print(f'  {row["abbreviation"]} {row["office_type"]}: '
              f'{row["elections"]} candidacies, '
              f'{row["winners"]} winners (D:{row["d_wins"]}, R:{row["r_wins"]})')

    # Spot checks
    r = run_sql("""
        SELECT se.seat_label, c.full_name, ca.party, ca.is_incumbent,
               ca.votes_received, ca.vote_percentage, ca.result
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN candidates c ON ca.candidate_id = c.id
        WHERE e.election_year = 2025
        ORDER BY RANDOM() LIMIT 10
    """)
    print('\nSpot checks:')
    for row in r:
        inc = ' (i)' if row['is_incumbent'] else ''
        print(f'  {row["seat_label"]}: {row["full_name"]}{inc} [{row["party"]}] '
              f'{row["votes_received"]} votes ({row["vote_percentage"]}%) → {row["result"]}')

    # Duplicate check
    r = run_sql("""
        SELECT ca.election_id, ca.candidate_id, COUNT(*) as cnt
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        WHERE e.election_year = 2025
        GROUP BY ca.election_id, ca.candidate_id HAVING COUNT(*) > 1
    """)
    if r:
        print(f'\nWARNING: {len(r)} duplicate candidacies!')
    else:
        print('\nNo duplicate candidacies.')


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Populate 2025 election results')
    parser.add_argument('--state', type=str, choices=['VA', 'NJ'],
                        help='Process a single state')
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
        all_races = [r for r in all_races if r['state'] == args.state]
        print(f'Filtered to {len(all_races)} races for {args.state}')

    # Check for existing 2025 elections
    existing = run_sql("SELECT COUNT(*) as cnt FROM elections WHERE election_year = 2025")
    if existing[0]['cnt'] > 0 and not args.dry_run:
        print(f'\nWARNING: {existing[0]["cnt"]} elections already exist for 2025!')
        print('Aborting to prevent duplicates. Delete existing 2025 data first.')
        sys.exit(1)

    # Process each state
    states = sorted(set(r['state'] for r in all_races))
    total_new_cands = 0
    total_candidacies = 0

    for state in states:
        print(f'\n{"=" * 60}')
        print(f'PROCESSING: {state}')
        print(f'{"=" * 60}')

        state_races = [r for r in all_races if r['state'] == state]
        print(f'  Races: {len(state_races)}')

        # Load DB maps
        print('  Loading DB maps...')
        legislative_seats, statewide_seats, incumbent_map, district_map = load_seat_map(state)
        print(f'  Legislative seats: {len(legislative_seats)}')
        print(f'  Statewide seats: {len(statewide_seats)}')
        print(f'  Incumbents: {len(incumbent_map)}')

        # Create elections
        print('\n  Creating elections...')
        election_map = create_elections(
            state_races, legislative_seats, statewide_seats, district_map,
            dry_run=args.dry_run
        )

        # Create candidacies
        print('\n  Creating candidacies...')
        new_cands, cand_count = process_candidacies(
            state_races, election_map, legislative_seats, statewide_seats,
            incumbent_map, dry_run=args.dry_run
        )
        total_new_cands += new_cands
        total_candidacies += cand_count

    # Summary
    print(f'\n{"=" * 60}')
    print('SUMMARY')
    print(f'{"=" * 60}')
    print(f'New candidates: {total_new_cands}')
    print(f'Total candidacies: {total_candidacies}')

    if not args.dry_run:
        verify()

    print('\nDone!')


if __name__ == '__main__':
    main()
