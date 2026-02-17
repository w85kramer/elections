"""
Populate historical statewide officeholder data into the database.

Reads parsed JSON from parse_statewide_wiki.py and creates:
- Candidate records (match existing; create new for historical)
- Seat_term records (all closed; current incumbents NOT touched)

Infers missing end dates from successor start dates.
Skips acting/interim officeholders.

Usage:
    python3 scripts/populate_statewide_history.py --office ag --dry-run
    python3 scripts/populate_statewide_history.py --office ag --state CA
    python3 scripts/populate_statewide_history.py --office ag
"""
import sys
import re
import json
import time
import argparse
import unicodedata
from collections import defaultdict

import httpx
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

BATCH_SIZE = 400

OFFICE_TYPES = {
    'ag': 'Attorney General',
    'lt_gov': 'Lt. Governor',
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

def fix_end_dates(records):
    """Infer missing end dates from successor start dates within each state."""
    by_state = defaultdict(list)
    for r in records:
        by_state[r['state']].append(r)

    for state, recs in by_state.items():
        # Sort by start_year (None at end)
        recs.sort(key=lambda x: (x['start_year'] or 9999))

        for i in range(len(recs) - 1):
            curr = recs[i]
            next_rec = recs[i + 1]

            if curr['end_year'] is None and next_rec['start_year'] is not None:
                curr['end_year'] = next_rec['start_year']
                if curr.get('end_date') is None and next_rec.get('start_date'):
                    curr['end_date'] = next_rec['start_date']
                elif curr.get('end_date') is None and curr['end_year']:
                    curr['end_date'] = f"{curr['end_year']}-01-01"

    return records

def main():
    parser = argparse.ArgumentParser(description='Populate historical statewide officeholder data')
    parser.add_argument('--office', required=True, choices=OFFICE_TYPES.keys(),
                        help='Office type to populate')
    parser.add_argument('--dry-run', action='store_true', help='Print actions without writing')
    parser.add_argument('--state', type=str, help='Process single state (e.g., CA)')
    args = parser.parse_args()

    DRY_RUN = args.dry_run
    office = args.office
    office_type = OFFICE_TYPES[office]
    input_path = f'/tmp/statewide_{office}_history.json'

    if DRY_RUN:
        print('=== DRY RUN MODE ===\n')

    # Load parsed data
    print(f'Loading {input_path}...')
    with open(input_path) as f:
        all_records = json.load(f)
    print(f'  Loaded {len(all_records)} records')

    # Filter by state if specified
    if args.state:
        state_key = args.state.upper()
        all_records = [r for r in all_records if r['state'] == state_key]
        if not all_records:
            print(f'No records found for state {state_key}')
            sys.exit(1)
        print(f'  Filtered to {len(all_records)} records for {state_key}')

    # Remove acting/interim entries
    regular_records = [r for r in all_records if not r.get('is_acting', False)]
    acting_count = len(all_records) - len(regular_records)
    print(f'  {len(regular_records)} regular + {acting_count} acting (skipped)')

    # Infer missing end dates
    regular_records = fix_end_dates(regular_records)

    states_in_data = sorted(set(r['state'] for r in regular_records))

    # ── STEP 1: Safety check — abort if historical seat_terms already exist ──
    print('\n-- Step 1: Safety check --')
    states_str = ','.join(f"'{s}'" for s in states_in_data)
    check = run_sql(f"""
        SELECT COUNT(*) as cnt FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE se.office_type = '{esc(office_type)}'
          AND s.abbreviation IN ({states_str})
          AND st.end_date IS NOT NULL
          AND st.start_date < '2020-01-01'
    """)
    existing_count = check[0]['cnt'] if check else 0
    if existing_count > 0:
        print(f'  WARNING: {existing_count} historical {office_type} seat_terms already exist.')
        print(f'  This script is designed for initial population only.')
        print(f'  Proceeding would create duplicates. Aborting.')
        sys.exit(1)
    print(f'  No existing historical {office_type} seat_terms -- safe to proceed')
    time.sleep(1)

    # ── STEP 2: Load seat IDs from database ──
    print(f'\n-- Step 2: Load {office_type} seats --')
    seats_result = run_sql(f"""
        SELECT se.id as seat_id, se.seat_label,
               st.abbreviation as state
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE se.office_type = '{esc(office_type)}'
          AND st.abbreviation IN ({states_str})
    """)
    seat_by_state = {}
    for s in seats_result:
        seat_by_state[s['state']] = s['seat_id']
    print(f'  Loaded {len(seat_by_state)} seats')
    time.sleep(1)

    # ── STEP 3: Load existing candidates for name matching ──
    print('\n-- Step 3: Load existing candidates --')
    cands_result = run_sql("SELECT id, full_name FROM candidates ORDER BY id")
    existing_candidates = [(c['id'], c['full_name']) for c in cands_result]
    print(f'  Loaded {len(existing_candidates)} existing candidates')
    time.sleep(1)

    # ── STEP 4: Load current seat_terms (to skip incumbents) ──
    print(f'\n-- Step 4: Load current {office_type} seat_terms --')
    seat_ids_str = ','.join(str(v) for v in seat_by_state.values())
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
    print(f'  {len(current_term_by_seat)} current {office_type} seat_terms (will NOT be modified)')
    time.sleep(1)

    # ── STEP 5: Process records ──
    print(f'\n-- Step 5: Process {office_type} records --')

    candidate_creates = []
    seat_term_inserts = []
    candidate_name_map = {}  # normalized_name -> ref
    stats = defaultdict(int)

    def find_or_create_candidate(name):
        norm_key = re.sub(r'\s+(Jr\.?|Sr\.?|III|IV|II)\s*$', '', name, flags=re.IGNORECASE).strip().lower()
        norm_key = strip_accents(norm_key)

        if norm_key in candidate_name_map:
            return candidate_name_map[norm_key]

        # Try matching existing candidates
        best_match = None
        best_score = 0
        for cid, cname in existing_candidates:
            score = name_similarity(name, cname)
            if score > best_score:
                best_score = score
                best_match = (cid, cname)

        if best_score >= 0.8:
            ref = ('existing', best_match[0])
            candidate_name_map[norm_key] = ref
            return ref

        # Create new
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

    for state in states_in_data:
        state_recs = [r for r in regular_records if r['state'] == state]
        seat_id = seat_by_state.get(state)
        if not seat_id:
            print(f'  {state}: No {office_type} seat found -- skipping')
            continue

        current_term = current_term_by_seat.get(seat_id)

        for rec in state_recs:
            name = rec['name']
            party = rec['party']
            start_year = rec.get('start_year')
            end_year = rec.get('end_year')
            start_date = rec.get('start_date')
            end_date = rec.get('end_date')

            # Skip if still no end date (likely current incumbent)
            if end_year is None:
                # Check if matches current DB incumbent
                if current_term:
                    sim = name_similarity(name, current_term['full_name'])
                    if sim >= 0.7:
                        stats['incumbents_skipped'] += 1
                        continue
                # If doesn't match current incumbent, still skip (we can't create
                # an open-ended seat_term when one already exists)
                stats['no_end_date_skipped'] += 1
                continue

            # Build dates
            if not start_date and start_year:
                start_date = f'{start_year}-01-01'
            if not end_date and end_year:
                end_date = f'{end_year}-01-01'

            if not start_date:
                stats['no_start_date_skipped'] += 1
                continue

            # Determine start_reason
            start_reason = 'elected'

            # Find/create candidate
            cand_ref = find_or_create_candidate(name)

            seat_term_inserts.append({
                'seat_id': seat_id,
                'candidate_ref': cand_ref,
                'party': party,
                'start_date': start_date,
                'end_date': end_date,
                'start_reason': start_reason,
                'end_reason': 'term_expired',
                'state': state,
            })
            stats['seat_terms_created'] += 1

        state_created = sum(1 for t in seat_term_inserts if t['state'] == state)
        print(f'  {state}: {state_created} seat_terms')

    # ── Summary ──
    print(f'\n=== Planned Operations ===')
    print(f'  Candidates to create: {stats["candidates_created"]}')
    print(f'  Seat terms to create: {stats["seat_terms_created"]}')
    print(f'  Incumbents skipped: {stats.get("incumbents_skipped", 0)}')
    print(f'  No end date skipped: {stats.get("no_end_date_skipped", 0)}')
    print(f'  No start date skipped: {stats.get("no_start_date_skipped", 0)}')

    if DRY_RUN:
        print('\n=== DRY RUN -- no database changes ===')

        # Show seat terms by state
        print('\nSeat terms by state:')
        by_state = defaultdict(int)
        for t in seat_term_inserts:
            by_state[t['state']] += 1
        for s in sorted(by_state):
            print(f'  {s}: {by_state[s]}')

        # Show sample records
        print('\nSample seat_terms:')
        for t in seat_term_inserts[:10]:
            ref = t['candidate_ref']
            if ref[0] == 'existing':
                name = next((c[1] for c in existing_candidates if c[0] == ref[1]), '?')
            else:
                name = candidate_creates[ref[1]]['full_name']
            print(f'  {t["state"]} {name} ({t["party"]}) {t["start_date"]} - {t["end_date"]}')

        return

    # ══════════════════════════════════════════════════════════════════
    # EXECUTE
    # ══════════════════════════════════════════════════════════════════

    # ── Step A: Create new candidates ──
    print('\n-- Step A: Create candidates --')
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
            return new_candidate_ids[ref[1]]
        return None

    # ── Step B: Create seat_terms ──
    print('\n-- Step B: Create seat_terms --')
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
                start_reason = f"'{t['start_reason']}'" if t['start_reason'] else 'NULL'
                end_reason = f"'{t['end_reason']}'" if t['end_reason'] else 'NULL'
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
            print(f'  Created seat_terms {batch_start+1}-{batch_start+len(batch)} of {len(seat_term_inserts)}')
            time.sleep(2)

    print(f'  Created {len(seat_term_inserts)} seat_terms')

    # ── Verify ──
    print(f'\n-- Verification --')
    verify = run_sql(f"""
        SELECT s.abbreviation, COUNT(*) as cnt
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE se.office_type = '{esc(office_type)}'
          AND s.abbreviation IN ({states_str})
        GROUP BY s.abbreviation
        ORDER BY s.abbreviation
    """)
    total_terms = sum(r['cnt'] for r in verify)
    print(f'  Total {office_type} seat_terms: {total_terms}')
    for r in verify:
        print(f'    {r["abbreviation"]}: {r["cnt"]}')

    print(f'\n=== Done ===')

if __name__ == '__main__':
    main()
