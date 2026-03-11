#!/usr/bin/env python3
"""
Populate AK election results from Secretary of State data.

Reads /tmp/ak_sos_results.json (from download_ak_sos_results.py),
matches races to existing DB districts/seats, and inserts elections,
candidates, and candidacies.

Handles:
  - New elections (2004-2008 generals, 2004-2016 primaries)
  - Existing elections (2012-2018 generals) — updates vote totals
  - Senate letter→number mapping (A→1, B→2, ..., T→20)
  - Name format conversion (SoS "Last, First M." → DB "First M. Last")

Usage:
    python3 scripts/populate_ak_sos_results.py              # Execute
    python3 scripts/populate_ak_sos_results.py --dry-run    # Show plan only
    python3 scripts/populate_ak_sos_results.py --year 2012  # Single year
"""

import sys
import os
import json
import time
import argparse
import re
from difflib import SequenceMatcher

import httpx
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL


def run_sql(query, retries=5, exit_on_error=True):
    for attempt in range(retries):
        resp = httpx.post(
            API_URL,
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429 and attempt < retries - 1:
            wait = 10 * (attempt + 1)
            print(f'  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None


def senate_letter_to_number(letter):
    """Convert Senate district letter (A-T) to number (1-20)."""
    return str(ord(letter.upper()) - ord('A') + 1)


def normalize_name(sos_name):
    """Convert SoS 'LAST, FIRST M.' or 'Last, First M.' to 'First M. Last'.

    Handles:
      - ALL CAPS (2004-2008): 'STEDMAN, BERT K.' → 'Bert K. Stedman'
      - Mixed case (2012+): 'Coghill, John B. Jr.' → 'John B. Jr. Coghill'
      - Hyphenated: 'ALLEN-HERRON, DAWN' → 'Dawn Allen-Herron'
      - No comma (write-ins): 'Write-in Votes' → 'Write-in Votes'
    """
    if ',' not in sos_name:
        return sos_name.strip()

    parts = sos_name.split(',', 1)
    last = parts[0].strip()
    first = parts[1].strip()

    # Title-case if all caps
    if last == last.upper() and len(last) > 2:
        last = last.title()
        # Fix common patterns
        last = re.sub(r"Mc(\w)", lambda m: f"Mc{m.group(1).upper()}", last)
        last = re.sub(r"O'(\w)", lambda m: f"O'{m.group(1).upper()}", last)
    if first == first.upper() and len(first) > 2:
        first = first.title()
        # Keep suffixes like Jr., Sr., II, III, IV
        first = re.sub(r'\bIi\b', 'II', first)
        first = re.sub(r'\bIii\b', 'III', first)
        first = re.sub(r'\bIv\b', 'IV', first)

    return f'{first} {last}'.strip()


def name_similarity(name1, name2):
    """Compare two names, ignoring case, suffixes, and middle initials."""
    def simplify(n):
        n = n.lower().strip()
        # Remove common suffixes
        n = re.sub(r'\b(jr\.?|sr\.?|ii|iii|iv)\b', '', n)
        # Remove periods and extra spaces
        n = re.sub(r'\.', '', n)
        n = re.sub(r'\s+', ' ', n).strip()
        return n

    s1 = simplify(name1)
    s2 = simplify(name2)

    if s1 == s2:
        return 1.0

    return SequenceMatcher(None, s1, s2).ratio()


def escape_sql(s):
    """Escape string for SQL."""
    if s is None:
        return 'NULL'
    return "'" + str(s).replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser(description='Populate AK SoS election results')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--year', type=int, help='Single year')
    args = parser.parse_args()

    # Load downloaded data
    input_path = '/tmp/ak_sos_results.json'
    if not os.path.exists(input_path):
        print(f'ERROR: {input_path} not found. Run download_ak_sos_results.py first.')
        sys.exit(1)

    with open(input_path) as f:
        all_races = json.load(f)

    if args.year:
        all_races = [r for r in all_races if r['year'] == args.year]

    print(f'Loaded {len(all_races)} races from {input_path}')

    # --- Load DB mappings ---
    print('\nLoading DB mappings...')

    # Get AK state_id
    state_rows = run_sql("SELECT id FROM states WHERE abbreviation = 'AK'")
    ak_state_id = state_rows[0]['id']

    # Get all AK legislative seats: {(chamber, district_number) → seat_id}
    seat_rows = run_sql(f"""
        SELECT s.id as seat_id, d.chamber, d.district_number, d.district_name,
               s.seat_designator
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        WHERE d.state_id = {ak_state_id}
          AND s.office_level = 'Legislative'
          AND COALESCE(d.redistricting_cycle, '2022') = '2022'
        ORDER BY d.chamber, d.district_number
    """)

    seat_map = {}  # (chamber, district_number) → seat_id
    for r in seat_rows:
        key = (r['chamber'], r['district_number'])
        if r['seat_designator']:
            key = (r['chamber'], r['district_number'], r['seat_designator'])
        seat_map[key] = r['seat_id']

    print(f'  {len(seat_map)} seats loaded')

    # Get existing elections: {(seat_id, year, election_type) → election_id}
    elec_rows = run_sql(f"""
        SELECT e.id as election_id, e.seat_id, e.election_year, e.election_type,
               e.total_votes_cast
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        WHERE d.state_id = {ak_state_id}
          AND s.office_level = 'Legislative'
    """)

    existing_elections = {}
    for r in elec_rows:
        key = (r['seat_id'], r['election_year'], r['election_type'])
        existing_elections[key] = {
            'id': r['election_id'],
            'total_votes': r['total_votes_cast'],
        }

    print(f'  {len(existing_elections)} existing elections')

    # Get existing candidates: {name_lower → (candidate_id, full_name)}
    cand_rows = run_sql("""
        SELECT DISTINCT c.id, c.full_name, c.last_name
        FROM candidates c
        JOIN candidacies cy ON cy.candidate_id = c.id
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'AK'
    """)

    existing_candidates = {}  # full_name_lower → {id, full_name}
    for r in cand_rows:
        existing_candidates[r['full_name'].lower()] = {
            'id': r['id'],
            'full_name': r['full_name'],
        }

    print(f'  {len(existing_candidates)} existing AK candidates')

    # Get existing candidacies to avoid duplicates
    existing_candidacies = set()
    ccy_rows = run_sql(f"""
        SELECT cy.election_id, cy.candidate_id
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        WHERE d.state_id = {ak_state_id}
          AND s.office_level = 'Legislative'
    """)
    for r in ccy_rows:
        existing_candidacies.add((r['election_id'], r['candidate_id']))

    print(f'  {len(existing_candidacies)} existing candidacies')

    # --- Process races ---
    print('\nProcessing races...')

    # Map SoS party codes to standard party abbreviations
    PARTY_MAP = {
        'REP': 'Republican',
        'DEM': 'Democrat',
        'LIB': 'Libertarian',
        'GRE': 'Green',
        'AI': 'Alaskan Independence',
        'AKI': 'Alaskan Independence',
        'NP': 'Nonpartisan',
        'NA': 'Nonpartisan',
        'IND': 'Independent',
        'MOD': 'Moderate',
        'VET': 'Veterans',
        'UND': 'Undeclared',
    }

    elections_to_create = []   # (seat_id, year, type, date, total_votes, precincts_reporting, precincts_total)
    elections_to_update = []   # (election_id, total_votes, precincts_reporting, precincts_total)
    candidates_to_create = []  # (normalized_name, first, last)
    candidacies_to_create = [] # pending — need election_id and candidate_id first

    stats = {
        'races_matched': 0,
        'races_skipped_no_seat': 0,
        'elections_new': 0,
        'elections_existing': 0,
        'elections_updated': 0,
        'candidates_new': 0,
        'candidates_matched': 0,
        'candidacies_new': 0,
        'candidacies_skipped': 0,
    }

    # Group races by (seat_id, year, election_type) so we can batch
    race_groups = {}
    for race in all_races:
        chamber = race['chamber']
        district = race['district']

        # Map Senate letters to numbers
        if chamber == 'Senate':
            district_number = senate_letter_to_number(district)
        else:
            district_number = str(int(district))  # normalize '01' → '1'

        seat_id = seat_map.get((chamber, district_number))
        if not seat_id:
            stats['races_skipped_no_seat'] += 1
            continue

        stats['races_matched'] += 1
        key = (seat_id, race['year'], race['election_type'])

        race_groups[key] = race

    print(f'  {stats["races_matched"]} races matched to seats, {stats["races_skipped_no_seat"]} skipped')

    # Determine which elections need creating vs updating
    for (seat_id, year, etype), race in sorted(race_groups.items()):
        existing = existing_elections.get((seat_id, year, etype))

        if existing:
            stats['elections_existing'] += 1
            # Update total_votes if we have better data
            if race['total_votes'] and not existing['total_votes']:
                elections_to_update.append((
                    existing['id'],
                    race['total_votes'],
                    race['precincts_reporting'],
                    race['num_precincts'],
                ))
                stats['elections_updated'] += 1
        else:
            stats['elections_new'] += 1
            elections_to_create.append((
                seat_id, year, etype,
                race.get('election_date'),
                race['total_votes'],
                race['precincts_reporting'],
                race['num_precincts'],
            ))

    print(f'  {stats["elections_new"]} new elections, {stats["elections_existing"]} existing ({stats["elections_updated"]} to update)')

    if args.dry_run:
        # Show breakdown of new elections by year/type
        from collections import Counter
        new_by_type = Counter((e[1], e[2]) for e in elections_to_create)
        print('\n  New elections by year/type:')
        for (yr, et), cnt in sorted(new_by_type.items()):
            print(f'    {yr} {et:15s} {cnt}')

        print('\n  Would also create candidates and candidacies for all races.')
        return

    # --- Step 1: Create new elections ---
    if elections_to_create:
        print(f'\nCreating {len(elections_to_create)} elections...')
        batch_size = 200
        new_election_ids = {}  # (seat_id, year, type) → election_id

        for i in range(0, len(elections_to_create), batch_size):
            batch = elections_to_create[i:i+batch_size]
            values = []
            for (seat_id, year, etype, edate, total_votes, prec_rep, prec_total) in batch:
                date_sql = f"'{edate}'" if edate else 'NULL'
                tv_sql = str(total_votes) if total_votes else 'NULL'
                pr_sql = str(prec_rep) if prec_rep else 'NULL'
                pt_sql = str(prec_total) if prec_total else 'NULL'
                values.append(f"({seat_id}, {year}, '{etype}', {date_sql}, {tv_sql}, {pr_sql}, {pt_sql})")

            joiner = ',\n                '
            values_sql = joiner.join(values)
            result = run_sql(f"""
                INSERT INTO elections (seat_id, election_year, election_type, election_date,
                                       total_votes_cast, precincts_reporting, precincts_total)
                VALUES
                {values_sql}
                ON CONFLICT DO NOTHING
                RETURNING id, seat_id, election_year, election_type
            """)

            for r in (result or []):
                key = (r['seat_id'], r['election_year'], r['election_type'])
                new_election_ids[key] = r['id']
                existing_elections[key] = {'id': r['id'], 'total_votes': None}

            print(f'    Batch {i//batch_size + 1}: {len(result or [])} created')
            if i + batch_size < len(elections_to_create):
                time.sleep(1)

        print(f'  Created {len(new_election_ids)} elections')

    # --- Step 2: Update existing elections with vote totals ---
    if elections_to_update:
        print(f'\nUpdating {len(elections_to_update)} elections with vote totals...')
        for (eid, total_votes, prec_rep, prec_total) in elections_to_update:
            tv_sql = str(total_votes) if total_votes else 'NULL'
            pr_sql = str(prec_rep) if prec_rep else 'NULL'
            pt_sql = str(prec_total) if prec_total else 'NULL'
            run_sql(f"""
                UPDATE elections
                SET total_votes_cast = {tv_sql},
                    precincts_reporting = {pr_sql},
                    precincts_total = {pt_sql}
                WHERE id = {eid}
                  AND total_votes_cast IS NULL
            """, exit_on_error=False)
        print('  Done')

    # --- Step 3: Process candidates and candidacies ---
    print('\nProcessing candidates and candidacies...')

    # Collect all unique candidate names from SoS data
    all_sos_candidates = {}  # normalized_name → first occurrence info
    for (seat_id, year, etype), race in race_groups.items():
        for cand in race['candidates']:
            if cand['is_write_in']:
                continue
            norm_name = normalize_name(cand['name'])
            if norm_name.lower() not in all_sos_candidates:
                all_sos_candidates[norm_name.lower()] = norm_name

    # Match to existing candidates or prepare new ones
    candidate_id_map = {}  # normalized_name_lower → candidate_id
    candidates_needed = []  # names we need to create

    for name_lower, norm_name in all_sos_candidates.items():
        # Direct match
        if name_lower in existing_candidates:
            candidate_id_map[name_lower] = existing_candidates[name_lower]['id']
            stats['candidates_matched'] += 1
            continue

        # Fuzzy match
        best_match = None
        best_score = 0
        for existing_lower, existing_info in existing_candidates.items():
            score = name_similarity(norm_name, existing_info['full_name'])
            if score > best_score:
                best_score = score
                best_match = existing_info

        if best_match and best_score >= 0.85:
            candidate_id_map[name_lower] = best_match['id']
            stats['candidates_matched'] += 1
        else:
            candidates_needed.append(norm_name)
            stats['candidates_new'] += 1

    print(f'  {stats["candidates_matched"]} matched, {stats["candidates_new"]} new candidates')

    # Create new candidates
    if candidates_needed:
        print(f'  Creating {len(candidates_needed)} new candidates...')
        batch_size = 200
        for i in range(0, len(candidates_needed), batch_size):
            batch = candidates_needed[i:i+batch_size]
            values = []
            for name in batch:
                parts = name.rsplit(' ', 1)
                if len(parts) == 2:
                    first = parts[0]
                    last = parts[1]
                else:
                    first = None
                    last = name
                first_sql = escape_sql(first)
                last_sql = escape_sql(last)
                values.append(f"({escape_sql(name)}, {first_sql}, {last_sql})")

            joiner = ',\n                '
            values_sql = joiner.join(values)
            result = run_sql(f"""
                INSERT INTO candidates (full_name, first_name, last_name)
                VALUES
                {values_sql}
                RETURNING id, full_name
            """)

            for r in (result or []):
                candidate_id_map[r['full_name'].lower()] = r['id']
                existing_candidates[r['full_name'].lower()] = {
                    'id': r['id'],
                    'full_name': r['full_name'],
                }

            if i + batch_size < len(candidates_needed):
                time.sleep(1)

    # --- Step 4: Create candidacies ---
    print('\nCreating candidacies...')

    candidacy_values = []
    for (seat_id, year, etype), race in sorted(race_groups.items()):
        election_info = existing_elections.get((seat_id, year, etype))
        if not election_info:
            continue
        election_id = election_info['id']

        # Sort candidates by votes descending
        sorted_cands = sorted(race['candidates'], key=lambda c: c['votes'] or 0, reverse=True)

        for i, cand in enumerate(sorted_cands):
            if cand['is_write_in']:
                continue  # Skip write-in aggregate rows

            norm_name = normalize_name(cand['name'])
            cand_id = candidate_id_map.get(norm_name.lower())
            if not cand_id:
                continue

            # Skip if candidacy already exists
            if (election_id, cand_id) in existing_candidacies:
                stats['candidacies_skipped'] += 1
                continue

            party = PARTY_MAP.get(cand['party'], cand['party'])
            votes = cand['votes']
            pct = cand['pct']

            # Determine result
            if etype == 'General':
                result = 'Won' if i == 0 and votes > 0 else 'Lost'
            else:
                # Primary — winner advances
                result = 'Won' if i == 0 and votes > 0 else 'Lost'

            party_sql = escape_sql(party)
            votes_sql = str(votes) if votes is not None else 'NULL'
            pct_sql = str(pct) if pct is not None else 'NULL'
            result_sql = escape_sql(result)
            is_write_in_sql = 'TRUE' if cand['is_write_in'] else 'FALSE'

            candidacy_values.append(
                f"({election_id}, {cand_id}, {party_sql}, {votes_sql}, {pct_sql}, {result_sql}, {is_write_in_sql})"
            )
            stats['candidacies_new'] += 1

    if candidacy_values:
        print(f'  Inserting {len(candidacy_values)} candidacies...')
        batch_size = 300
        total_inserted = 0
        for i in range(0, len(candidacy_values), batch_size):
            batch = candidacy_values[i:i+batch_size]
            joiner = ',\n                '
            values_sql = joiner.join(batch)
            result = run_sql(f"""
                INSERT INTO candidacies (election_id, candidate_id, party,
                                          votes_received, vote_percentage, result, is_write_in)
                VALUES
                {values_sql}
                ON CONFLICT DO NOTHING
            """, exit_on_error=False)

            total_inserted += len(batch)
            print(f'    Batch {i//batch_size + 1}: {len(batch)} rows')
            if i + batch_size < len(candidacy_values):
                time.sleep(1)

    # --- Summary ---
    print('\n=== Summary ===')
    for k, v in stats.items():
        print(f'  {k}: {v}')

    # Verification
    print('\nVerifying...')
    verify = run_sql(f"""
        SELECT e.election_year, e.election_type, COUNT(*) as cnt
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        WHERE d.state_id = {ak_state_id}
          AND s.office_level = 'Legislative'
          AND e.election_year BETWEEN 2004 AND 2018
        GROUP BY e.election_year, e.election_type
        ORDER BY e.election_year, e.election_type
    """)
    print('  AK elections 2004-2018:')
    for r in verify:
        print(f'    {r["election_year"]} {r["election_type"]:20s} {r["cnt"]}')


if __name__ == '__main__':
    main()
