"""
Populate historical governor data (1959-present) into the database.

Reads parsed JSON from parse_governor_wiki.py and creates:
- Election records (General, Recall) — all result_status='Certified'
- Candidate records (match existing 50 incumbents; create new for historical)
- Candidacy records (winner-only for 40 states; full data for 10 with vote history)
- Seat_term records (all closed; current incumbents NOT touched)

Usage:
    python3 scripts/populate_governors_historical.py --dry-run
    python3 scripts/populate_governors_historical.py --state AK
    python3 scripts/populate_governors_historical.py
"""
import sys
import re
import json
import time
import argparse
import unicodedata
from collections import defaultdict
from datetime import date

import httpx

BATCH_SIZE = 400
INPUT_PATH = '/tmp/governor_history.json'

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
    if len(first1) >= 3 and len(first2) >= 3 and first1[:3] == first2[:3]:
        return 0.8
    if first1.startswith(first2) or first2.startswith(first1):
        return 0.75
    return 0.3

def election_date_for_year(year, state=None):
    """Compute the general election date for a given year.
    Standard: first Tuesday after the first Monday in November.
    Special cases for odd-year states and recall elections."""
    # Standard November election
    from datetime import date, timedelta
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

    # November 1 of the election year
    nov1 = date(year, 11, 1)
    # First Monday in November
    days_until_monday = (7 - nov1.weekday()) % 7
    if nov1.weekday() == 0:
        first_monday = nov1
    else:
        first_monday = nov1 + timedelta(days=days_until_monday)
    # First Tuesday after first Monday
    election_day = first_monday + timedelta(days=1)

    return election_day.isoformat()

# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Populate historical governor data')
    parser.add_argument('--dry-run', action='store_true', help='Print actions without writing')
    parser.add_argument('--state', type=str, help='Process single state (e.g., AK)')
    args = parser.parse_args()

    DRY_RUN = args.dry_run
    if DRY_RUN:
        print('=== DRY RUN MODE ===\n')

    # Load parsed data
    print(f'Loading {INPUT_PATH}...')
    with open(INPUT_PATH) as f:
        all_data = json.load(f)

    if args.state:
        state_key = args.state.upper()
        if state_key not in all_data:
            print(f'State {state_key} not found in parsed data')
            sys.exit(1)
        states_to_process = [state_key]
    else:
        states_to_process = sorted(all_data.keys())

    print(f'Processing {len(states_to_process)} states')

    # ── STEP 1: Safety check — abort if historical governor elections already exist ──
    print('\n── Step 1: Safety check ──')
    check = run_sql("""
        SELECT COUNT(*) as cnt FROM elections e
        JOIN seats s ON e.seat_id = s.id
        WHERE s.office_type = 'Governor'
          AND e.election_year < 2024
          AND e.election_type = 'General'
    """)
    existing_count = check[0]['cnt'] if check else 0
    if existing_count > 0:
        print(f'  ABORT: {existing_count} historical governor General elections already exist.')
        print('  This script is designed for initial population only.')
        sys.exit(1)
    print('  No existing historical governor elections — safe to proceed')
    time.sleep(1)

    # ── STEP 2: Load governor seat IDs from database ──
    print('\n── Step 2: Load governor seats ──')
    states_str = ','.join(f"'{s}'" for s in states_to_process)
    seats_result = run_sql(f"""
        SELECT se.id as seat_id, se.seat_label,
               st.abbreviation as state,
               d.id as district_id
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE se.office_type = 'Governor'
          AND st.abbreviation IN ({states_str})
    """)
    seat_by_state = {}
    for s in seats_result:
        seat_by_state[s['state']] = {
            'seat_id': s['seat_id'],
            'seat_label': s['seat_label'],
            'district_id': s['district_id'],
        }
    print(f'  Loaded {len(seat_by_state)} governor seats')
    time.sleep(1)

    # ── STEP 3: Load existing candidates for name matching ──
    print('\n── Step 3: Load existing candidates ──')
    cands_result = run_sql("SELECT id, full_name, first_name, last_name FROM candidates ORDER BY id")
    existing_candidates = [(c['id'], c['full_name']) for c in cands_result]
    print(f'  Loaded {len(existing_candidates)} existing candidates')
    time.sleep(1)

    # ── STEP 4: Load current governor seat_terms (to avoid modifying them) ──
    print('\n── Step 4: Load current governor seat_terms ──')
    seat_ids_str = ','.join(str(seat_by_state[s]['seat_id']) for s in seat_by_state)
    current_terms = run_sql(f"""
        SELECT st.id as term_id, st.seat_id, st.candidate_id, c.full_name
        FROM seat_terms st
        JOIN candidates c ON st.candidate_id = c.id
        WHERE st.seat_id IN ({seat_ids_str})
          AND st.end_date IS NULL
    """)
    current_term_by_seat = {}
    for t in current_terms:
        current_term_by_seat[t['seat_id']] = t
    print(f'  {len(current_term_by_seat)} current governor seat_terms (will NOT be modified)')
    time.sleep(1)

    # ── STEP 5: Process each state ──
    print('\n── Step 5: Process governors ──')

    # Collect all SQL operations
    election_inserts = []
    candidate_creates = []
    candidacy_inserts = []
    seat_term_inserts = []

    # Track candidate name → ref for deduplication within this run
    candidate_name_map = {}  # normalized_name → ref_key

    # Track which elections are created for linking candidacies
    election_refs = {}  # (state, year, type) → ref_key

    stats = defaultdict(int)

    def find_or_create_candidate(name):
        """Find an existing candidate or create a new one. Returns a ref tuple."""
        norm_key = re.sub(r'\s+(Jr\.?|Sr\.?|III|IV|II)\s*$', '', name, flags=re.IGNORECASE).strip().lower()

        # Check if already resolved in this run
        if norm_key in candidate_name_map:
            return candidate_name_map[norm_key]

        # Try to match against real existing candidates (id > 0 only)
        best_match = None
        best_score = 0
        for cid, cname in existing_candidates:
            if cid < 0:
                continue  # Skip placeholder entries
            score = name_similarity(name, cname)
            if score > best_score:
                best_score = score
                best_match = (cid, cname)

        if best_score >= 0.8:
            ref = ('existing', best_match[0])
            candidate_name_map[norm_key] = ref
            return ref

        # Create new candidate
        parts = name.split()
        first_name = parts[0] if parts else name
        last_name = parts[-1] if len(parts) > 1 else ''
        ref = ('new', len(candidate_creates))
        candidate_creates.append({
            'full_name': name,
            'first_name': first_name,
            'last_name': last_name,
        })
        candidate_name_map[norm_key] = ref
        stats['candidates_created'] += 1
        return ref

    for state in states_to_process:
        data = all_data[state]
        governors = data['governors']

        if state not in seat_by_state:
            print(f'  {state}: No governor seat found in DB — skipping')
            continue

        seat_info = seat_by_state[state]
        seat_id = seat_info['seat_id']
        current_term = current_term_by_seat.get(seat_id)

        print(f'\n  {state}: {len(governors)} governors')

        for gov in governors:
            name = gov['name']
            party = gov['party']
            caucus = gov.get('caucus')
            start_date = gov['start_date']
            end_date = gov['end_date']
            start_reason = gov['start_reason']
            end_reason = gov['end_reason']
            is_acting = gov.get('is_acting', False)
            election_years_info = gov.get('election_years', [])
            elections_detail = gov.get('elections_detail', [])

            # Skip acting governors — they don't get seat_terms or elections
            if is_acting:
                stats['acting_skipped'] += 1
                continue

            # Skip the current incumbent — they already have a seat_term
            if current_term and end_date is None:
                sim = name_similarity(name, current_term['full_name'])
                if sim >= 0.7:
                    print(f'    Skipping current incumbent: {name} (matches {current_term["full_name"]})')
                    stats['current_skipped'] += 1
                    continue

            # ── Create/find candidate ──
            cand_ref = find_or_create_candidate(name)

            # ── Create elections for each election year ──
            election_refs_for_gov = []
            for i, ey_info in enumerate(election_years_info):
                yr = ey_info['year']
                is_recall = ey_info.get('is_recall', False)

                elec_type = 'Recall' if is_recall else 'General'
                ref_key = (state, yr, elec_type)

                if ref_key not in election_refs:
                    # Get detail data if available
                    detail = elections_detail[i] if i < len(elections_detail) else None
                    total_votes = None
                    if detail and detail.get('total_votes'):
                        total_votes = detail['total_votes']

                    elec_date = election_date_for_year(yr, state)
                    notes = None
                    if not detail or not detail.get('candidates'):
                        notes = 'Source: Wikipedia timeline (no vote data)'

                    election_inserts.append({
                        'seat_id': seat_id,
                        'election_year': yr,
                        'election_type': elec_type,
                        'election_date': elec_date,
                        'total_votes': total_votes,
                        'result_status': 'Certified',
                        'notes': notes,
                        'ref_key': ref_key,
                        'detail': detail,
                        'winner_cand_ref': cand_ref,
                        'winner_party': party,
                    })
                    election_refs[ref_key] = len(election_inserts) - 1
                    stats['elections_created'] += 1

                    # ── Create candidacies ──
                    if detail and detail.get('candidates'):
                        # Full candidate data from electoral history
                        for cand_data in detail['candidates']:
                            c_name = cand_data['name']
                            c_party = cand_data['party']
                            c_votes = cand_data.get('votes')
                            c_pct = cand_data.get('pct')
                            c_winner = cand_data.get('is_winner', False)

                            # Find/create this candidate
                            c_ref = find_or_create_candidate(c_name)

                            candidacy_inserts.append({
                                'election_ref': ref_key,
                                'candidate_ref': c_ref,
                                'party': c_party,
                                'is_incumbent': False,  # Will update later
                                'votes': c_votes,
                                'pct': c_pct,
                                'result': 'Won' if c_winner else 'Lost',
                            })
                            stats['candidacies_created'] += 1
                    else:
                        # No detailed data — create winner-only candidacy
                        if not is_recall:
                            candidacy_inserts.append({
                                'election_ref': ref_key,
                                'candidate_ref': cand_ref,
                                'party': party,
                                'is_incumbent': False,
                                'votes': None,
                                'pct': None,
                                'result': 'Won',
                            })
                            stats['candidacies_created'] += 1

                election_refs_for_gov.append(ref_key)

            # ── Create seat_term ──
            # Only create if this governor has an end_date (closed term)
            # Current incumbents are skipped above
            if end_date is not None:
                # Link to the first election (the one that got them into office)
                first_election_ref = election_refs_for_gov[0] if election_refs_for_gov else None

                seat_term_inserts.append({
                    'seat_id': seat_id,
                    'candidate_ref': cand_ref,
                    'party': party,
                    'caucus': caucus,
                    'start_date': start_date,
                    'end_date': end_date,
                    'start_reason': start_reason,
                    'end_reason': end_reason,
                    'election_ref': first_election_ref,
                })
                stats['seat_terms_created'] += 1

    # ── Summary ──
    print(f'\n=== Planned Operations ===')
    print(f'  Candidates to create: {stats["candidates_created"]}')
    print(f'  Elections to create: {stats["elections_created"]}')
    print(f'  Candidacies to create: {stats["candidacies_created"]}')
    print(f'  Seat terms to create: {stats["seat_terms_created"]}')
    print(f'  Current incumbents skipped: {stats.get("current_skipped", 0)}')
    print(f'  Acting governors skipped: {stats.get("acting_skipped", 0)}')

    if DRY_RUN:
        print('\n=== DRY RUN — no database changes ===')

        # Show sample elections by state
        print('\nElections by state:')
        by_state = defaultdict(int)
        for e in election_inserts:
            by_state[e['ref_key'][0]] += 1
        for s in sorted(by_state):
            print(f'  {s}: {by_state[s]}')

        # Show sample seat terms by state
        print('\nSeat terms by state:')
        by_state = defaultdict(int)
        for t in seat_term_inserts:
            by_state[next(s for s in states_to_process if seat_by_state.get(s, {}).get('seat_id') == t['seat_id'])] += 1
        for s in sorted(by_state):
            print(f'  {s}: {by_state[s]}')

        return

    # ══════════════════════════════════════════════════════════════════
    # EXECUTE — Write to database
    # ══════════════════════════════════════════════════════════════════

    # ── Step A: Create new candidates ──
    print('\n── Step A: Create candidates ──')
    new_candidate_ids = {}  # index → db_id
    if candidate_creates:
        for batch_start in range(0, len(candidate_creates), BATCH_SIZE):
            batch = candidate_creates[batch_start:batch_start + BATCH_SIZE]
            values = []
            for c in batch:
                values.append(f"('{esc(c['full_name'])}', '{esc(c['first_name'])}', '{esc(c['last_name'])}')")
            sql = f"""
                INSERT INTO candidates (full_name, first_name, last_name)
                VALUES {', '.join(values)}
                RETURNING id, full_name
            """
            result = run_sql(sql)
            for i, row in enumerate(result):
                new_candidate_ids[batch_start + i] = row['id']
            print(f'  Created candidates {batch_start+1}-{batch_start+len(batch)} of {len(candidate_creates)}')
            time.sleep(2)

    print(f'  Created {len(new_candidate_ids)} new candidates')

    def resolve_candidate_id(ref):
        if ref[0] == 'existing':
            return ref[1]
        elif ref[0] == 'new':
            return new_candidate_ids[ref[1]]
        return None

    # ── Step B: Create elections ──
    print('\n── Step B: Create elections ──')
    election_db_ids = {}  # ref_key → db_id
    if election_inserts:
        for batch_start in range(0, len(election_inserts), BATCH_SIZE):
            batch = election_inserts[batch_start:batch_start + BATCH_SIZE]
            values = []
            for e in batch:
                total = f"{e['total_votes']}" if e['total_votes'] else 'NULL'
                notes = f"'{esc(e['notes'])}'" if e['notes'] else 'NULL'
                values.append(
                    f"({e['seat_id']}, '{e['election_date']}', {e['election_year']}, "
                    f"'{e['election_type']}', '{e['result_status']}', {total}, {notes})"
                )
            sql = f"""
                INSERT INTO elections (seat_id, election_date, election_year, election_type, result_status, total_votes_cast, notes)
                VALUES {', '.join(values)}
                RETURNING id
            """
            result = run_sql(sql)
            for i, row in enumerate(result):
                e = batch[i]
                election_db_ids[e['ref_key']] = row['id']
            print(f'  Created elections {batch_start+1}-{batch_start+len(batch)} of {len(election_inserts)}')
            time.sleep(2)

    print(f'  Created {len(election_db_ids)} elections')

    # ── Step C: Create candidacies ──
    print('\n── Step C: Create candidacies ──')
    if candidacy_inserts:
        for batch_start in range(0, len(candidacy_inserts), BATCH_SIZE):
            batch = candidacy_inserts[batch_start:batch_start + BATCH_SIZE]
            values = []
            for c in batch:
                elec_id = election_db_ids.get(c['election_ref'])
                cand_id = resolve_candidate_id(c['candidate_ref'])
                if not elec_id or not cand_id:
                    continue
                party = f"'{esc(c['party'])}'" if c['party'] else 'NULL'
                votes = str(c['votes']) if c['votes'] is not None else 'NULL'
                pct = str(c['pct']) if c['pct'] is not None else 'NULL'
                values.append(
                    f"({elec_id}, {cand_id}, {party}, "
                    f"{'TRUE' if c['is_incumbent'] else 'FALSE'}, "
                    f"'Active', {votes}, {pct}, '{c['result']}')"
                )
            if values:
                sql = f"""
                    INSERT INTO candidacies (election_id, candidate_id, party, is_incumbent, candidate_status, votes_received, vote_percentage, result)
                    VALUES {', '.join(values)}
                """
                run_sql(sql)
            print(f'  Created candidacies {batch_start+1}-{batch_start+len(batch)} of {len(candidacy_inserts)}')
            time.sleep(2)

    print(f'  Created {len(candidacy_inserts)} candidacies')

    # ── Step D: Create seat_terms ──
    print('\n── Step D: Create seat_terms ──')
    if seat_term_inserts:
        for batch_start in range(0, len(seat_term_inserts), BATCH_SIZE):
            batch = seat_term_inserts[batch_start:batch_start + BATCH_SIZE]
            values = []
            for t in batch:
                cand_id = resolve_candidate_id(t['candidate_ref'])
                if not cand_id:
                    continue
                party = f"'{esc(t['party'])}'" if t['party'] else 'NULL'
                caucus = f"'{esc(t['caucus'])}'" if t['caucus'] else 'NULL'
                start = f"'{t['start_date']}'" if t['start_date'] else 'NULL'
                end = f"'{t['end_date']}'" if t['end_date'] else 'NULL'
                start_reason = f"'{t['start_reason']}'" if t['start_reason'] else 'NULL'
                end_reason = f"'{t['end_reason']}'" if t['end_reason'] else 'NULL'
                elec_id = 'NULL'
                if t.get('election_ref') and t['election_ref'] in election_db_ids:
                    elec_id = str(election_db_ids[t['election_ref']])
                values.append(
                    f"({t['seat_id']}, {cand_id}, {party}, {start}, {end}, "
                    f"{start_reason}, {end_reason}, {caucus}, {elec_id})"
                )
            if values:
                sql = f"""
                    INSERT INTO seat_terms (seat_id, candidate_id, party, start_date, end_date,
                                           start_reason, end_reason, caucus, election_id)
                    VALUES {', '.join(values)}
                """
                run_sql(sql)
            print(f'  Created seat_terms {batch_start+1}-{batch_start+len(batch)} of {len(seat_term_inserts)}')
            time.sleep(2)

    print(f'  Created {len(seat_term_inserts)} seat_terms')

    # ── Final summary ──
    print(f'\n=== Done ===')
    print(f'  Candidates: {len(new_candidate_ids)} new')
    print(f'  Elections: {len(election_db_ids)}')
    print(f'  Candidacies: {len(candidacy_inserts)}')
    print(f'  Seat terms: {len(seat_term_inserts)}')

if __name__ == '__main__':
    main()
