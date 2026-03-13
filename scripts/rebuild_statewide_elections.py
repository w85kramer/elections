#!/usr/bin/env python3
"""
Rebuild statewide elections for AG, Lt. Governor, Secretary of State, and Treasurer.

After the "nuclear option" — all elections/candidacies for these 4 offices were deleted
because the original data was inferred from officeholder terms and ~35% wrong.

This script rebuilds elections from scratch using:
1. Known election cycles (term_length + next_regular_election_year on seats)
2. Seat terms with start_reason='elected' to identify winners
3. Creates General elections + winner candidacies

Phase 2 (separate script) will scrape Ballotpedia for opponents and vote percentages.

Usage:
    python3 scripts/rebuild_statewide_elections.py --dry-run
    python3 scripts/rebuild_statewide_elections.py
"""

import json
import os
import sys
import time
import requests
from pathlib import Path

# Load env
env_path = Path(__file__).parent.parent / '.env'
env = {}
with open(env_path) as f:
    for line in f:
        line = line.strip()
        if '=' in line and not line.startswith('#'):
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()

TOKEN = env['SUPABASE_MANAGEMENT_TOKEN']
PROJECT_REF = 'pikcvwulzfxgwfcfssxc'
API_URL = f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query'
DRY_RUN = '--dry-run' in sys.argv
MIN_YEAR = 1960  # Don't generate elections before this


def run_sql(query, label=""):
    if DRY_RUN:
        print(f"[DRY RUN] {label}: {query[:120]}...")
        return None
    for attempt in range(1, 6):
        r = requests.post(API_URL,
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query})
        if r.status_code in (200, 201):
            try:
                return r.json()
            except:
                return []
        if r.status_code == 429:
            wait = 5 * attempt
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        print(f"  ERROR ({r.status_code}): {r.text[:300]}")
        return None
    print(f"  Failed after 5 retries: {label}")
    return None


def run_sql_read(query, label=""):
    for attempt in range(1, 6):
        r = requests.post(API_URL,
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query})
        if r.status_code in (200, 201):
            try:
                return r.json()
            except:
                return []
        if r.status_code == 429:
            wait = 5 * attempt
            time.sleep(wait)
            continue
        print(f"  ERROR ({r.status_code}): {r.text[:200]}")
        return None
    return None


def esc(s):
    return s.replace("'", "''")


def main():
    print(f"{'[DRY RUN] ' if DRY_RUN else ''}Rebuild Statewide Elections")
    print("=" * 60)

    # === Step 1: Get all elected statewide seats ===
    seats = run_sql_read("""
        SELECT s.id as seat_id, s.office_type, s.term_length_years, s.next_regular_election_year,
               st.abbreviation as state, st.id as state_id, d.id as district_id
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE d.office_level = 'Statewide' AND s.selection_method = 'Elected'
        AND s.office_type IN ('Attorney General', 'Lt. Governor', 'Secretary of State', 'Treasurer')
        ORDER BY s.office_type, st.abbreviation
    """, "Get seats")

    if not seats:
        print("ERROR: No seats found")
        return

    print(f"Elected statewide seats: {len(seats)}")

    # === Step 2: Get ALL seat_terms (to find both new winners and re-elected incumbents) ===
    all_terms = run_sql_read("""
        SELECT st2.seat_id, st2.candidate_id, st2.party, st2.start_date, st2.end_date,
               st2.start_reason, c.full_name, c.first_name, c.last_name
        FROM seat_terms st2
        JOIN seats s ON st2.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN candidates c ON st2.candidate_id = c.id
        WHERE d.office_level = 'Statewide' AND s.selection_method = 'Elected'
        AND s.office_type IN ('Attorney General', 'Lt. Governor', 'Secretary of State', 'Treasurer')
        ORDER BY st2.seat_id, st2.start_date
    """, "Get all terms")

    if not all_terms:
        print("ERROR: No terms found")
        return

    elected_terms = [t for t in all_terms if t['start_reason'] == 'elected']
    print(f"Total seat_terms: {len(all_terms)}, elected: {len(elected_terms)}")

    # Build two lookups:
    # 1. term_lookup: seat_id -> list of {election_year, candidate_id, ...} for newly-elected terms
    # 2. span_lookup: seat_id -> list of {start_year, end_year, candidate_id, ...} for ALL terms
    #    Used to find incumbents who were re-elected (their term spans the election)

    term_lookup = {}
    for t in elected_terms:
        start_year = int(t['start_date'][:4])
        start_month = int(t['start_date'][5:7])
        election_year = start_year if start_month >= 11 else start_year - 1

        if t['seat_id'] not in term_lookup:
            term_lookup[t['seat_id']] = []
        term_lookup[t['seat_id']].append({
            'election_year': election_year,
            'candidate_id': t['candidate_id'],
            'party': t['party'],
            'full_name': t['full_name'],
        })

    span_lookup = {}
    for t in all_terms:
        start_year = int(t['start_date'][:4])
        end_year = int(t['end_date'][:4]) if t['end_date'] else 9999
        if t['seat_id'] not in span_lookup:
            span_lookup[t['seat_id']] = []
        span_lookup[t['seat_id']].append({
            'start_year': start_year,
            'end_year': end_year,
            'candidate_id': t['candidate_id'],
            'party': t['party'],
            'full_name': t['full_name'],
        })

    # === Step 3: Generate election years for each seat ===
    elections_to_create = []  # (seat_id, election_year, state, office_type)
    matched_winners = []  # (seat_id, election_year, candidate_id, party, full_name)
    unmatched_elections = []

    for seat in seats:
        seat_id = seat['seat_id']
        term_len = seat['term_length_years']
        next_year = seat['next_regular_election_year']
        state = seat['state']
        office = seat['office_type']

        if not term_len or not next_year:
            print(f"  WARNING: Missing term/year for {state} {office}")
            continue

        # Generate election years going backward from next_regular_election_year
        year = next_year
        while year >= MIN_YEAR:
            elections_to_create.append({
                'seat_id': seat_id,
                'year': year,
                'state': state,
                'office': office,
            })

            # Try to find a matching winner from seat_terms
            # Strategy 1: newly-elected term starting after this election
            new_terms = term_lookup.get(seat_id, [])
            winner = None
            for st in new_terms:
                if st['election_year'] == year:
                    winner = st
                    break

            # Strategy 2: re-elected incumbent — their term spans this election
            # The person in office during the election who continues after inauguration
            if not winner:
                spans = span_lookup.get(seat_id, [])
                # Inauguration year = election_year + 1 for even-year elections
                # For odd-year states, inauguration could be election_year + 1 too
                inaug_year = year + 1
                for sp in spans:
                    # Their term must have started BEFORE this election
                    # and ended AFTER the inauguration (or still ongoing)
                    if sp['start_year'] <= year and sp['end_year'] > inaug_year:
                        winner = sp
                        break

            if winner:
                matched_winners.append({
                    'seat_id': seat_id,
                    'year': year,
                    'state': state,
                    'office': office,
                    'candidate_id': winner['candidate_id'],
                    'party': winner['party'],
                    'full_name': winner['full_name'],
                })
            else:
                unmatched_elections.append({
                    'seat_id': seat_id,
                    'year': year,
                    'state': state,
                    'office': office,
                })

            year -= term_len

    # Also check for elected terms that don't match any generated election year
    # (could indicate mid-term special elections or wrong cycle)
    all_generated = {(e['seat_id'], e['year']) for e in elections_to_create}
    orphan_terms = []
    for seat_id, term_list in term_lookup.items():
        for t in term_list:
            if (seat_id, t['election_year']) not in all_generated:
                # Find seat info
                seat_info = next((s for s in seats if s['seat_id'] == seat_id), None)
                if seat_info:
                    orphan_terms.append({
                        'seat_id': seat_id,
                        'year': t['election_year'],
                        'state': seat_info['state'],
                        'office': seat_info['office_type'],
                        'full_name': t['full_name'],
                    })

    print(f"\n--- Election Generation Summary ---")
    print(f"  Elections to create: {len(elections_to_create)}")
    print(f"  With matched winner: {len(matched_winners)}")
    print(f"  Without winner match: {len(unmatched_elections)}")
    print(f"  Orphan terms (elected, no matching cycle): {len(orphan_terms)}")

    # Show breakdown by office
    from collections import Counter
    by_office = Counter()
    matched_by_office = Counter()
    for e in elections_to_create:
        by_office[e['office']] += 1
    for m in matched_winners:
        matched_by_office[m['office']] += 1
    print(f"\n  By office:")
    for office in sorted(by_office.keys()):
        print(f"    {office:30s} {by_office[office]:4d} elections, {matched_by_office[office]:4d} with winners")

    # Show sample orphan terms
    if orphan_terms:
        print(f"\n  Sample orphan terms (first 20):")
        for o in orphan_terms[:20]:
            print(f"    {o['state']:3s} {o['office']:25s} {o['year']} {o['full_name']}")

    # Show sample unmatched elections (recent ones)
    if unmatched_elections:
        recent_unmatched = [e for e in unmatched_elections if e['year'] >= 2010]
        if recent_unmatched:
            print(f"\n  Unmatched elections 2010+ (first 20):")
            for e in recent_unmatched[:20]:
                print(f"    {e['state']:3s} {e['office']:25s} {e['year']}")

    if DRY_RUN:
        print(f"\n[DRY RUN — no changes made]")
        return

    # === Step 4: Insert elections ===
    print(f"\n--- Creating elections ---")

    # Batch insert elections, collecting their IDs
    # We'll insert in batches of 50 and track seat_id+year -> election_id
    election_id_map = {}  # (seat_id, year) -> election_id

    batch_size = 50
    sorted_elections = sorted(elections_to_create, key=lambda e: (e['state'], e['office'], e['year']))

    for i in range(0, len(sorted_elections), batch_size):
        batch = sorted_elections[i:i+batch_size]

        # Build a single INSERT with RETURNING
        values = []
        for e in batch:
            values.append(f"({e['seat_id']}, {e['year']}, 'General')")

        sql = f"""INSERT INTO elections (seat_id, election_year, election_type)
                  VALUES {', '.join(values)}
                  RETURNING id, seat_id, election_year"""

        result = run_sql(sql, f"Insert elections batch {i//batch_size + 1}")
        if result:
            for row in result:
                election_id_map[(row['seat_id'], row['election_year'])] = row['id']

        batch_num = i // batch_size + 1
        total_batches = (len(sorted_elections) - 1) // batch_size + 1
        if batch_num % 10 == 0 or batch_num == total_batches:
            print(f"  Batch {batch_num}/{total_batches} done ({len(election_id_map)} elections created)")
        time.sleep(1)

    print(f"  Total elections created: {len(election_id_map)}")

    # === Step 5: Insert candidacies for winners ===
    print(f"\n--- Creating winner candidacies ---")

    candidacy_count = 0
    candidacy_batches = []
    current_batch = []

    for m in matched_winners:
        key = (m['seat_id'], m['year'])
        election_id = election_id_map.get(key)
        if not election_id:
            continue

        party = esc(m['party']) if m['party'] else 'Unknown'
        current_batch.append(
            f"({m['candidate_id']}, {election_id}, '{party}', 'Active', 'Won')"
        )
        candidacy_count += 1

        if len(current_batch) >= batch_size:
            candidacy_batches.append(current_batch)
            current_batch = []

    if current_batch:
        candidacy_batches.append(current_batch)

    for i, batch in enumerate(candidacy_batches):
        sql = f"""INSERT INTO candidacies (candidate_id, election_id, party, candidate_status, result)
                  VALUES {', '.join(batch)}"""
        run_sql(sql, f"Insert candidacies batch {i+1}")
        if (i + 1) % 10 == 0 or i + 1 == len(candidacy_batches):
            print(f"  Batch {i+1}/{len(candidacy_batches)} done")
        time.sleep(1)

    print(f"  Total winner candidacies created: {candidacy_count}")

    # === Summary ===
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"  Elections created: {len(election_id_map)}")
    print(f"  Winner candidacies created: {candidacy_count}")
    print(f"  Elections without matched winner: {len(election_id_map) - candidacy_count}")
    print(f"  Orphan terms (need manual review): {len(orphan_terms)}")


if __name__ == '__main__':
    main()
