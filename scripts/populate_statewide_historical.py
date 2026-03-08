"""
Populate historical statewide officeholder data (1960-present) into the database.

Reads parsed JSON from parse_statewide_wiki.py and creates:
- Election records (General) — all result_status='Certified'
- Candidate records (match existing; create new for historical)
- Candidacy records (winner-only — Wikipedia list pages don't have vote data)
- Seat_term records (all closed; current incumbents NOT touched)

Usage:
    python3 scripts/populate_statewide_historical.py --office ag --dry-run
    python3 scripts/populate_statewide_historical.py --office ag --state CA
    python3 scripts/populate_statewide_historical.py --office ag
    python3 scripts/populate_statewide_historical.py --office all
"""
import sys
import re
import json
import time
import argparse
import unicodedata
from collections import defaultdict
from datetime import date, timedelta

import httpx

sys.path.insert(0, __import__('os').path.join(__import__('os').path.dirname(__import__('os').path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

BATCH_SIZE = 400

OFFICE_TYPES = {
    'ag': 'Attorney General',
    'lt_gov': 'Lt. Governor',
    'sos': 'Secretary of State',
    'treasurer': 'Treasurer',
}

OFFICE_LABELS = {
    'ag': 'Attorney General',
    'lt_gov': 'Lieutenant Governor',
    'sos': 'Secretary of State',
    'treasurer': 'Treasurer',
}


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


def election_date_for_year(year):
    """Compute the general election date for a given year.
    First Tuesday after the first Monday in November."""
    nov1 = date(year, 11, 1)
    days_until_monday = (7 - nov1.weekday()) % 7
    if nov1.weekday() == 0:
        first_monday = nov1
    else:
        first_monday = nov1 + timedelta(days=days_until_monday)
    election_day = first_monday + timedelta(days=1)
    return election_day.isoformat()


def main():
    parser = argparse.ArgumentParser(description='Populate historical statewide officeholder data')
    parser.add_argument('--office', required=True, choices=list(OFFICE_TYPES.keys()) + ['all'],
                        help='Office type to populate (or "all")')
    parser.add_argument('--dry-run', action='store_true', help='Print actions without writing')
    parser.add_argument('--state', type=str, help='Process single state (e.g., CA)')
    args = parser.parse_args()

    DRY_RUN = args.dry_run
    if DRY_RUN:
        print('=== DRY RUN MODE ===\n')

    offices = list(OFFICE_TYPES.keys()) if args.office == 'all' else [args.office]

    for office in offices:
        process_office(office, DRY_RUN, args.state)


def process_office(office, DRY_RUN, single_state):
    office_type = OFFICE_TYPES[office]
    label = OFFICE_LABELS[office]
    input_path = f'/tmp/statewide_{office}_history.json'

    print(f'\n{"=" * 60}')
    print(f'Processing {label}')
    print(f'{"=" * 60}')

    # Load parsed data
    print(f'Loading {input_path}...')
    try:
        with open(input_path) as f:
            all_records = json.load(f)
    except FileNotFoundError:
        print(f'  ERROR: {input_path} not found. Run parse_statewide_wiki.py --office {office} first.')
        return

    # Group by state
    by_state = defaultdict(list)
    for r in all_records:
        by_state[r['state']].append(r)

    if single_state:
        state_key = single_state.upper()
        if state_key not in by_state:
            print(f'  State {state_key} not found in parsed data')
            return
        states_to_process = [state_key]
    else:
        states_to_process = sorted(by_state.keys())

    print(f'States: {len(states_to_process)}, Records: {len(all_records)}')

    # ── STEP 1: Safety check ──
    print('\n── Step 1: Safety check ──')
    check = run_sql(f"""
        SELECT COUNT(*) as cnt FROM elections e
        JOIN seats s ON e.seat_id = s.id
        WHERE s.office_type = '{esc(office_type)}'
          AND e.election_year < 2024
          AND e.election_type = 'General'
    """)
    existing_count = check[0]['cnt'] if check else 0
    if existing_count > 10:
        print(f'  WARNING: {existing_count} historical {label} General elections already exist.')
        print('  This script is designed for initial population.')
        print('  Proceeding anyway — will skip duplicate elections.')
    elif existing_count > 0:
        print(f'  Found {existing_count} existing historical elections (likely from 2025 specials)')
    else:
        print(f'  No existing historical {label} elections — safe to proceed')
    time.sleep(1)

    # ── STEP 2: Load seat IDs ──
    print('\n── Step 2: Load seats ──')
    states_str = ','.join(f"'{s}'" for s in states_to_process)
    seats_result = run_sql(f"""
        SELECT se.id as seat_id, se.seat_label, se.office_type,
               st.abbreviation as state,
               d.id as district_id
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE se.office_type = '{esc(office_type)}'
          AND st.abbreviation IN ({states_str})
    """)
    seat_by_state = {}
    for s in seats_result:
        seat_by_state[s['state']] = {
            'seat_id': s['seat_id'],
            'seat_label': s['seat_label'],
        }
    print(f'  Loaded {len(seat_by_state)} {label} seats')
    time.sleep(1)

    # ── STEP 3: Load existing candidates ──
    print('\n── Step 3: Load existing candidates ──')
    cands_result = run_sql("SELECT id, full_name FROM candidates ORDER BY id")
    existing_candidates = [(c['id'], c['full_name']) for c in cands_result]
    print(f'  Loaded {len(existing_candidates)} existing candidates')
    time.sleep(1)

    # ── STEP 4: Load current seat_terms (don't modify) ──
    print('\n── Step 4: Load current seat_terms ──')
    if seat_by_state:
        seat_ids_str = ','.join(str(seat_by_state[s]['seat_id']) for s in seat_by_state)
        current_terms = run_sql(f"""
            SELECT st.id, st.seat_id, st.candidate_id, c.full_name
            FROM seat_terms st
            JOIN candidates c ON st.candidate_id = c.id
            WHERE st.seat_id IN ({seat_ids_str})
              AND st.end_date IS NULL
        """)
    else:
        current_terms = []
    current_term_by_seat = {}
    for t in current_terms:
        current_term_by_seat[t['seat_id']] = t
    print(f'  {len(current_term_by_seat)} current {label} seat_terms (will NOT be modified)')
    time.sleep(1)

    # ── STEP 5: Load existing elections to avoid duplicates ──
    print('\n── Step 5: Check existing elections ──')
    if seat_by_state:
        existing_elecs = run_sql(f"""
            SELECT e.seat_id, e.election_year, e.election_type
            FROM elections e
            WHERE e.seat_id IN ({seat_ids_str})
        """)
    else:
        existing_elecs = []
    existing_election_keys = set()
    for e in existing_elecs:
        existing_election_keys.add((e['seat_id'], e['election_year'], e['election_type']))
    print(f'  {len(existing_election_keys)} existing elections for these seats')
    time.sleep(1)

    # ── STEP 6: Process records ──
    print('\n── Step 6: Process officeholders ──')

    election_inserts = []
    candidate_creates = []
    candidacy_inserts = []
    seat_term_inserts = []

    candidate_name_map = {}
    election_refs = {}
    stats = defaultdict(int)

    def find_or_create_candidate(name):
        norm_key = re.sub(r'\s+(Jr\.?|Sr\.?|III|IV|II)\s*$', '', name, flags=re.IGNORECASE).strip().lower()
        if norm_key in candidate_name_map:
            return candidate_name_map[norm_key]

        best_match = None
        best_score = 0
        for cid, cname in existing_candidates:
            if cid < 0:
                continue
            score = name_similarity(name, cname)
            if score > best_score:
                best_score = score
                best_match = (cid, cname)

        if best_score >= 0.8:
            ref = ('existing', best_match[0])
            candidate_name_map[norm_key] = ref
            return ref

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
        records = by_state.get(state, [])
        if state not in seat_by_state:
            print(f'  {state}: No {label} seat found in DB — skipping')
            stats['states_skipped'] += 1
            continue

        seat_info = seat_by_state[state]
        seat_id = seat_info['seat_id']
        current_term = current_term_by_seat.get(seat_id)

        # Filter: skip acting, skip entries before 1960
        regular = [r for r in records if not r.get('is_acting', False)]
        if not regular:
            print(f'  {state}: No regular officeholders — skipping')
            continue

        print(f'  {state}: {len(regular)} officeholders')

        for i, rec in enumerate(regular):
            name = rec['name']
            party = rec.get('party')
            start_year = rec.get('start_year')
            end_year = rec.get('end_year')
            start_date = rec.get('start_date')
            end_date = rec.get('end_date')
            is_incumbent = rec.get('is_incumbent', False)

            # Skip current incumbent
            if is_incumbent and current_term:
                sim = name_similarity(name, current_term['full_name'])
                if sim >= 0.7:
                    print(f'    Skipping current: {name} (matches {current_term["full_name"]})')
                    stats['current_skipped'] += 1
                    continue

            # Find/create candidate
            cand_ref = find_or_create_candidate(name)

            # Determine election year(s) for this officeholder
            # Elected officials typically won the election in the year before or same year they took office
            election_year = None
            if start_year:
                # Even year = election year; odd year = took office year after election
                if start_year % 2 == 0:
                    election_year = start_year
                else:
                    election_year = start_year - 1
                    # Some states have odd-year elections (VA, NJ, etc.)
                    # If start_year is odd and < 1962, might be pre-cycle
                    if start_year in (2017, 2013, 2009, 2005, 2001, 1997, 1993, 1989, 1985,
                                      1981, 1977, 1973, 1969, 1965, 1961):
                        # These are valid odd-year election years (VA, NJ, etc.)
                        election_year = start_year

            # Create election if we have a year and it's not already in the DB
            if election_year and election_year >= 1960 and election_year <= 2024:
                ref_key = (state, election_year, 'General')
                elec_key = (seat_id, election_year, 'General')

                if elec_key in existing_election_keys:
                    stats['elections_existing'] += 1
                elif ref_key not in election_refs:
                    elec_date = election_date_for_year(election_year)
                    election_inserts.append({
                        'seat_id': seat_id,
                        'election_year': election_year,
                        'election_type': 'General',
                        'election_date': elec_date,
                        'result_status': 'Certified',
                        'notes': 'Source: Wikipedia officeholder list',
                        'ref_key': ref_key,
                    })
                    election_refs[ref_key] = len(election_inserts) - 1
                    stats['elections_created'] += 1

                    # Create winner-only candidacy
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

            # Create seat_term (only for closed terms)
            if end_date and not is_incumbent:
                seat_term_inserts.append({
                    'seat_id': seat_id,
                    'candidate_ref': cand_ref,
                    'party': party,
                    'start_date': start_date,
                    'end_date': end_date,
                    'start_reason': 'elected',
                    'end_reason': 'term_expired',
                })
                stats['seat_terms_created'] += 1

    # ── Summary ──
    print(f'\n=== Planned Operations ({label}) ===')
    print(f'  Candidates to create: {stats["candidates_created"]}')
    print(f'  Elections to create: {stats["elections_created"]}')
    print(f'  Elections already existing: {stats.get("elections_existing", 0)}')
    print(f'  Candidacies to create: {stats["candidacies_created"]}')
    print(f'  Seat terms to create: {stats["seat_terms_created"]}')
    print(f'  Current incumbents skipped: {stats.get("current_skipped", 0)}')
    print(f'  States skipped (no seat): {stats.get("states_skipped", 0)}')

    if DRY_RUN:
        print('\n=== DRY RUN — no database changes ===')
        print('\nElections by state:')
        by_st = defaultdict(int)
        for e in election_inserts:
            by_st[e['ref_key'][0]] += 1
        for s in sorted(by_st):
            print(f'  {s}: {by_st[s]}')
        return

    # ══════════════════════════════════════════════════════════════════
    # EXECUTE
    # ══════════════════════════════════════════════════════════════════

    # ── Step A: Create new candidates ──
    print(f'\n── Step A: Create candidates ──')
    new_candidate_ids = {}
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
            return new_candidate_ids.get(ref[1])
        return None

    # ── Step B: Create elections ──
    print(f'\n── Step B: Create elections ──')
    election_db_ids = {}
    if election_inserts:
        for batch_start in range(0, len(election_inserts), BATCH_SIZE):
            batch = election_inserts[batch_start:batch_start + BATCH_SIZE]
            values = []
            for e in batch:
                notes = f"'{esc(e['notes'])}'" if e['notes'] else 'NULL'
                values.append(
                    f"({e['seat_id']}, '{e['election_date']}', {e['election_year']}, "
                    f"'{e['election_type']}', '{e['result_status']}', NULL, {notes})"
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
    print(f'\n── Step C: Create candidacies ──')
    created_candidacies = 0
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
                values.append(
                    f"({elec_id}, {cand_id}, {party}, "
                    f"{'TRUE' if c['is_incumbent'] else 'FALSE'}, "
                    f"'Active', NULL, NULL, '{c['result']}')"
                )
            if values:
                sql = f"""
                    INSERT INTO candidacies (election_id, candidate_id, party, is_incumbent, candidate_status, votes_received, vote_percentage, result)
                    VALUES {', '.join(values)}
                """
                run_sql(sql)
                created_candidacies += len(values)
            print(f'  Created candidacies {batch_start+1}-{batch_start+len(batch)} of {len(candidacy_inserts)}')
            time.sleep(2)
    print(f'  Created {created_candidacies} candidacies')

    # ── Step D: Create seat_terms ──
    print(f'\n── Step D: Create seat_terms ──')
    created_terms = 0
    if seat_term_inserts:
        for batch_start in range(0, len(seat_term_inserts), BATCH_SIZE):
            batch = seat_term_inserts[batch_start:batch_start + BATCH_SIZE]
            values = []
            for t in batch:
                cand_id = resolve_candidate_id(t['candidate_ref'])
                if not cand_id:
                    continue
                party = f"'{esc(t['party'])}'" if t['party'] else 'NULL'
                start = f"'{t['start_date']}'" if t['start_date'] else 'NULL'
                end = f"'{t['end_date']}'" if t['end_date'] else 'NULL'
                start_reason = f"'{t['start_reason']}'" if t.get('start_reason') else 'NULL'
                end_reason = f"'{t['end_reason']}'" if t.get('end_reason') else 'NULL'
                values.append(
                    f"({t['seat_id']}, {cand_id}, {party}, {start}, {end}, "
                    f"{start_reason}, {end_reason})"
                )
            if values:
                sql = f"""
                    INSERT INTO seat_terms (seat_id, candidate_id, party, start_date, end_date,
                                           start_reason, end_reason)
                    VALUES {', '.join(values)}
                """
                run_sql(sql)
                created_terms += len(values)
            print(f'  Created seat_terms {batch_start+1}-{batch_start+len(batch)} of {len(seat_term_inserts)}')
            time.sleep(2)
    print(f'  Created {created_terms} seat_terms')

    print(f'\n=== Done: {label} ===')
    print(f'  Candidates: {len(new_candidate_ids)} new')
    print(f'  Elections: {len(election_db_ids)}')
    print(f'  Candidacies: {created_candidacies}')
    print(f'  Seat terms: {created_terms}')


if __name__ == '__main__':
    main()
