#!/usr/bin/env python3
"""
Populate AK statewide election results (Governor, Lt Governor) from SoS data.

Reads /tmp/ak_sos_results.json (run download_ak_sos_results.py --statewide first),
creates elections, candidates, and candidacies for:
  - Governor primaries (1998-2018)
  - Lt Governor primaries (1998-2018)
  - Lt Governor generals (fills in missing elections and candidacies)
  - Links Gov/Lt Gov general elections via linked_election_id

Governor generals already have candidacies from Ballotpedia; this script
fills in the Lt Gov side and all primaries.

Usage:
    python3 scripts/populate_ak_statewide.py              # Execute
    python3 scripts/populate_ak_statewide.py --dry-run    # Show plan only
    python3 scripts/populate_ak_statewide.py --year 2010  # Single year
"""

import sys
import os
import json
import time
import argparse
import re
from collections import defaultdict
from difflib import SequenceMatcher

import httpx
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

# AK statewide seat IDs
GOV_SEAT_ID = 7387
LTGOV_SEAT_ID = 7388

# Map SoS party codes to standard party abbreviations
PARTY_MAP = {
    'REP': 'R', 'DEM': 'D', 'LIB': 'L', 'GRE': 'G', 'GRN': 'G',
    'AI': 'AIP', 'AKI': 'AIP', 'NP': 'NP', 'NA': 'I',
    'IND': 'I', 'MOD': 'MOD', 'VET': 'VET', 'UND': 'NP', 'CON': 'CON',
}


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


def escape_sql(s):
    if s is None:
        return 'NULL'
    return "'" + str(s).replace("'", "''") + "'"


def normalize_name(sos_name):
    """Convert SoS 'LAST, FIRST M.' to 'First M. Last'."""
    if '/' in sos_name and ',' not in sos_name:
        return sos_name.strip()

    if ',' not in sos_name:
        return sos_name.strip()

    parts = sos_name.split(',', 1)
    last = parts[0].strip()
    first = parts[1].strip()

    if last == last.upper() and len(last) > 2:
        last = last.title()
        last = re.sub(r"Mc(\w)", lambda m: f"Mc{m.group(1).upper()}", last)
        last = re.sub(r"O'(\w)", lambda m: f"O'{m.group(1).upper()}", last)
        last = re.sub(r"\bDe([A-Z])", lambda m: f"De{m.group(1).upper()}", last)
    if first == first.upper() and len(first) > 2:
        first = first.title()
        first = re.sub(r'\bIi\b', 'II', first)
        first = re.sub(r'\bIii\b', 'III', first)
        first = re.sub(r'\bIv\b', 'IV', first)

    # Strip truncated nicknames: names like 'Gerald L. "J' or 'Carolyn F. "C'
    # where the full nickname was cut off by fixed-width formatting
    first = re.sub(r'\s*"[A-Za-z]{1,2}$', '', first).strip()
    # Also strip complete quoted nicknames: 'J. J. "Jack"'
    first = re.sub(r'\s*"([^"]+)"$', r' \1', first).strip()

    return f'{first} {last}'.strip()


def name_similarity(name1, name2):
    """Compare two names, ignoring case, suffixes, and middle initials."""
    def simplify(n):
        n = n.lower().strip()
        n = re.sub(r'\b(jr\.?|sr\.?|ii|iii|iv)\b', '', n)
        n = re.sub(r'\.', '', n)
        n = re.sub(r'\s+', ' ', n).strip()
        return n

    s1 = simplify(name1)
    s2 = simplify(name2)
    if s1 == s2:
        return 1.0
    return SequenceMatcher(None, s1, s2).ratio()


def match_candidate_by_name(name, existing_candidates, threshold=0.85):
    """Find a candidate match by full name. Returns (id, full_name) or (None, None)."""
    name_lower = name.lower().strip()

    # Direct match
    if name_lower in existing_candidates:
        info = existing_candidates[name_lower]
        return info['id'], info['full_name']

    # Fuzzy match
    best_match = None
    best_score = 0
    for existing_lower, existing_info in existing_candidates.items():
        score = name_similarity(name, existing_info['full_name'])
        if score > best_score:
            best_score = score
            best_match = existing_info

    if best_match and best_score >= threshold:
        return best_match['id'], best_match['full_name']

    return None, None


def match_candidate_by_last_name(last_name, existing_candidates, party=None, ltgov_primary_winners=None):
    """Match by last name. Returns (id, full_name) or (None, None).

    If multiple matches, narrows by party+year context using ltgov_primary_winners.
    Falls back to unique last name match.
    """
    last_lower = last_name.lower().strip()
    # Title-case if all caps
    if last_name == last_name.upper() and len(last_name) > 2:
        last_lower = last_name.title().lower()

    matches = []
    for existing_lower, info in existing_candidates.items():
        # Extract last name from "First M. Last" pattern
        parts = info['full_name'].rsplit(' ', 1)
        existing_last = parts[-1].lower() if parts else ''
        if existing_last == last_lower:
            matches.append(info)

    if len(matches) == 1:
        return matches[0]['id'], matches[0]['full_name']

    # Multiple matches — try to narrow using Lt Gov primary winners for this party
    if len(matches) > 1 and party and ltgov_primary_winners:
        primary_winner_id = ltgov_primary_winners.get(party)
        if primary_winner_id:
            for m in matches:
                if m['id'] == primary_winner_id:
                    return m['id'], m['full_name']

    return None, None


def main():
    parser = argparse.ArgumentParser(description='Populate AK statewide election results')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--year', type=int, help='Single year')
    parser.add_argument('--primaries-only', action='store_true',
                        help='Only process primaries (skip general Lt Gov matching)')
    args = parser.parse_args()

    input_path = '/tmp/ak_sos_results.json'
    if not os.path.exists(input_path):
        print(f'ERROR: {input_path} not found. Run download_ak_sos_results.py --statewide first.')
        sys.exit(1)

    with open(input_path) as f:
        all_races = json.load(f)

    all_races = [r for r in all_races if r.get('office_type')]

    if args.year:
        all_races = [r for r in all_races if r['year'] == args.year]

    print(f'Loaded {len(all_races)} statewide races')

    # --- Load DB data ---
    print('\nLoading DB state...')

    elec_rows = run_sql(f"""
        SELECT id, seat_id, election_year, election_type, election_date,
               total_votes_cast, linked_election_id
        FROM elections
        WHERE seat_id IN ({GOV_SEAT_ID}, {LTGOV_SEAT_ID})
        ORDER BY election_year, election_type
    """)

    existing_elections = {}
    for r in elec_rows:
        key = (r['seat_id'], r['election_year'], r['election_type'])
        existing_elections[key] = {
            'id': r['id'],
            'date': r['election_date'],
            'total_votes': r['total_votes_cast'],
            'linked': r['linked_election_id'],
        }

    print(f'  {len(existing_elections)} existing Gov/Lt Gov elections')

    ccy_rows = run_sql(f"""
        SELECT cy.election_id, cy.candidate_id
        FROM candidacies cy
        WHERE cy.election_id IN (
            SELECT id FROM elections WHERE seat_id IN ({GOV_SEAT_ID}, {LTGOV_SEAT_ID})
        )
    """)

    existing_candidacies = set()
    for r in ccy_rows:
        existing_candidacies.add((r['election_id'], r['candidate_id']))

    print(f'  {len(existing_candidacies)} existing candidacies')

    cand_rows = run_sql("""
        SELECT DISTINCT c.id, c.full_name
        FROM candidates c
        JOIN candidacies cy ON cy.candidate_id = c.id
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        WHERE d.state_id = (SELECT id FROM states WHERE abbreviation = 'AK')
    """)

    existing_candidates = {}
    for r in cand_rows:
        existing_candidates[r['full_name'].lower()] = {
            'id': r['id'],
            'full_name': r['full_name'],
        }

    print(f'  {len(existing_candidates)} existing AK candidates')

    # --- Categorize and merge races ---
    # Merge fragmented primaries: for 2002 and earlier, each party had its own primary.
    # Group all non-R primaries into a single "Primary" election per seat/year.

    # First, bucket races by (office_type, election_type, year)
    gov_primaries = defaultdict(list)    # (year, etype) → [race, ...]
    ltgov_primaries = defaultdict(list)
    combined_generals = []

    for race in all_races:
        ot = race['office_type']
        et = race['election_type']
        year = race['year']

        if ot == 'Governor/Lt Governor':
            combined_generals.append(race)
        elif ot == 'Governor':
            gov_primaries[(year, et)].append(race)
        elif ot == 'Lt Governor':
            ltgov_primaries[(year, et)].append(race)

    # Merge races that share the same (year, election_type)
    def merge_primary_races(races_by_key):
        """Merge multiple party-primary races into single election entries."""
        merged = []
        for (year, etype), races in sorted(races_by_key.items()):
            all_cands = []
            total_votes = 0
            date = None
            for race in races:
                for cand in race['candidates']:
                    all_cands.append(cand)
                total_votes += (race['total_votes'] or 0)
                if race.get('election_date'):
                    date = race['election_date']
            merged.append({
                'year': year,
                'election_type': etype,
                'election_date': date,
                'total_votes': total_votes,
                'candidates': all_cands,
            })
        return merged

    gov_primary_list = merge_primary_races(gov_primaries)
    ltgov_primary_list = merge_primary_races(ltgov_primaries)

    print(f'\n  {len(gov_primary_list)} Governor primary elections')
    print(f'  {len(ltgov_primary_list)} Lt Governor primary elections')
    print(f'  {len(combined_generals)} combined Gov/Lt Gov generals')

    # --- Build candidacy plan ---
    elections_plan = []  # (seat_id, year, type, date, total_votes)
    candidacies_plan = []  # (seat_id, year, type, name, party, votes, pct, result, is_last_name_only)

    # Governor primaries
    for race in gov_primary_list:
        year = race['year']
        etype = race['election_type']
        key = (GOV_SEAT_ID, year, etype)

        if key not in existing_elections:
            elections_plan.append((GOV_SEAT_ID, year, etype, race['election_date'], race['total_votes']))

        sorted_cands = sorted(
            [c for c in race['candidates'] if not c['is_write_in']],
            key=lambda c: c['votes'] or 0, reverse=True
        )
        for i, cand in enumerate(sorted_cands):
            norm_name = normalize_name(cand['name'])
            party = PARTY_MAP.get(cand['party'], cand['party'])
            result = 'Won' if i == 0 and cand['votes'] > 0 else 'Lost'
            candidacies_plan.append((GOV_SEAT_ID, year, etype, norm_name, party,
                                     cand['votes'], cand['pct'], result, False))

    # Lt Governor primaries
    for race in ltgov_primary_list:
        year = race['year']
        etype = race['election_type']
        key = (LTGOV_SEAT_ID, year, etype)

        if key not in existing_elections:
            elections_plan.append((LTGOV_SEAT_ID, year, etype, race['election_date'], race['total_votes']))

        sorted_cands = sorted(
            [c for c in race['candidates'] if not c['is_write_in']],
            key=lambda c: c['votes'] or 0, reverse=True
        )
        for i, cand in enumerate(sorted_cands):
            norm_name = normalize_name(cand['name'])
            party = PARTY_MAP.get(cand['party'], cand['party'])
            result = 'Won' if i == 0 and cand['votes'] > 0 else 'Lost'
            candidacies_plan.append((LTGOV_SEAT_ID, year, etype, norm_name, party,
                                     cand['votes'], cand['pct'], result, False))

    # Combined generals — extract Lt Gov running mates
    if args.primaries_only:
        combined_generals = []
        print('  (skipping generals — --primaries-only)')

    for race in combined_generals:
        year = race['year']

        # Skip if 0 votes (2018 SoS page is incomplete)
        if not race['total_votes'] or race['total_votes'] == 0:
            print(f'  Skipping {year} general (0 votes in SoS data)')
            continue

        # Ensure Lt Gov general election exists
        ltgov_key = (LTGOV_SEAT_ID, year, 'General')
        if ltgov_key not in existing_elections:
            elections_plan.append((LTGOV_SEAT_ID, year, 'General',
                                   race.get('election_date'), race['total_votes']))

        sorted_cands = sorted(
            [c for c in race['candidates'] if not c['is_write_in']],
            key=lambda c: c['votes'] or 0, reverse=True
        )
        for i, cand in enumerate(sorted_cands):
            if '/' not in cand['name']:
                continue  # Skip non-ticket entries

            parts = cand['name'].split('/', 1)
            ltgov_last = parts[1].strip()

            party = PARTY_MAP.get(cand['party'], cand['party'])
            result = 'Won' if i == 0 and cand['votes'] > 0 else 'Lost'

            candidacies_plan.append((LTGOV_SEAT_ID, year, 'General', ltgov_last, party,
                                     cand['votes'], cand['pct'], result, True))

    # --- Show plan ---
    print(f'\n=== Plan ===')
    print(f'  Elections to create: {len(elections_plan)}')
    for seat_id, year, etype, date, tv in sorted(elections_plan):
        office = 'Governor' if seat_id == GOV_SEAT_ID else 'Lt Gov'
        print(f'    {year} {etype:15s} {office} (date={date}, votes={tv})')

    print(f'\n  Total candidacies planned: {len(candidacies_plan)}')

    # Build Lt Gov primary winner lookup: {(year, party) → candidate_id}
    # This helps disambiguate last-name matches for general election Lt Gov candidates
    ltgov_primary_winners_by_year = {}  # year → {party → cand_id}
    for idx, (seat_id, year, etype, name, party, votes, pct, result, is_last) in enumerate(candidacies_plan):
        if seat_id == LTGOV_SEAT_ID and 'Primary' in etype and result == 'Won':
            cand_id, _ = match_candidate_by_name(name, existing_candidates)
            if cand_id:
                ltgov_primary_winners_by_year.setdefault(year, {})[party] = cand_id

    # Check candidate matching
    candidates_to_create = []
    unmatched_last_names = []
    match_results = {}  # index → (cand_id, matched_name)

    for idx, (seat_id, year, etype, name, party, votes, pct, result, is_last) in enumerate(candidacies_plan):
        if is_last:
            primary_winners = ltgov_primary_winners_by_year.get(year, {})
            cand_id, matched = match_candidate_by_last_name(
                name, existing_candidates, party=party,
                ltgov_primary_winners=primary_winners)
            if cand_id:
                match_results[idx] = (cand_id, matched)
            else:
                unmatched_last_names.append((idx, name, year, party))
        else:
            cand_id, matched = match_candidate_by_name(name, existing_candidates)
            if cand_id:
                match_results[idx] = (cand_id, matched)
            else:
                candidates_to_create.append((idx, name))

    print(f'  Candidates matched: {len(match_results)}')
    print(f'  Candidates to create: {len(candidates_to_create)}')
    print(f'  Unmatched Lt Gov last names: {len(unmatched_last_names)}')

    if candidates_to_create:
        print('\n  New candidates:')
        for idx, name in candidates_to_create:
            print(f'    {name}')

    if unmatched_last_names:
        print('\n  Unmatched last names (will try again after creating primary candidates):')
        for idx, name, year, party in unmatched_last_names:
            print(f'    {year} General: {name} ({party})')

    if args.dry_run:
        print('\n  Candidacy details:')
        for idx, (seat_id, year, etype, name, party, votes, pct, result, is_last) in enumerate(candidacies_plan):
            office = 'Gov' if seat_id == GOV_SEAT_ID else 'LtG'
            if idx in match_results:
                cid, mname = match_results[idx]
                match_info = f' → {mname} (#{cid})' if mname.lower() != name.lower() else f' (#{cid})'
            elif (idx, name) in [(i, n) for i, n in candidates_to_create]:
                match_info = ' [NEW]'
            else:
                match_info = ' [UNMATCHED]'
            print(f'    {year} {etype:12s} {office} {name:30s} {party:5s} {votes:>8d} {result:4s}{match_info}')
        return

    # --- Execute ---

    # Step 1: Create new candidates (from primaries — not last-name-only entries)
    # Deduplicate by name
    unique_names = {}
    for idx, name in candidates_to_create:
        if name.lower() not in unique_names:
            unique_names[name.lower()] = name

    if unique_names:
        print(f'\nCreating {len(unique_names)} new candidates (deduped from {len(candidates_to_create)})...')
        values = []
        for name in unique_names.values():
            parts = name.rsplit(' ', 1)
            if len(parts) == 2:
                first, last = parts
            else:
                first, last = None, name
            values.append(f"({escape_sql(name)}, {escape_sql(first)}, {escape_sql(last)})")

        result = run_sql(f"""
            INSERT INTO candidates (full_name, first_name, last_name)
            VALUES {', '.join(values)}
            RETURNING id, full_name
        """)

        for r in (result or []):
            existing_candidates[r['full_name'].lower()] = {
                'id': r['id'],
                'full_name': r['full_name'],
            }
            # Map back to plan indices
            for idx, name in candidates_to_create:
                if name.lower() == r['full_name'].lower():
                    match_results[idx] = (r['id'], r['full_name'])

        print(f'  Created {len(result or [])} candidates')

    # Re-attempt unmatched last names (now that primary candidates exist)
    still_unmatched = []
    for idx, last_name, year, party in unmatched_last_names:
        primary_winners = ltgov_primary_winners_by_year.get(year, {})
        cand_id, matched = match_candidate_by_last_name(
            last_name, existing_candidates, party=party,
            ltgov_primary_winners=primary_winners)
        if cand_id:
            match_results[idx] = (cand_id, matched)
            print(f'  Resolved {last_name} → {matched}')
        else:
            still_unmatched.append((idx, last_name, year))

    if still_unmatched:
        print(f'\n  Still unmatched last names ({len(still_unmatched)}):')
        for idx, name, year in still_unmatched:
            print(f'    {year}: {name} — creating as last-name-only candidate')
            # Title-case
            display_name = name.title() if name == name.upper() else name
            result = run_sql(f"""
                INSERT INTO candidates (full_name, last_name)
                VALUES ({escape_sql(display_name)}, {escape_sql(display_name)})
                RETURNING id, full_name
            """)
            if result:
                cid = result[0]['id']
                fname = result[0]['full_name']
                existing_candidates[fname.lower()] = {'id': cid, 'full_name': fname}
                match_results[idx] = (cid, fname)

    # Step 2: Create new elections
    if elections_plan:
        print(f'\nCreating {len(elections_plan)} elections...')
        values = []
        for (seat_id, year, etype, date, tv) in elections_plan:
            date_sql = f"'{date}'" if date else 'NULL'
            tv_sql = str(tv) if tv else 'NULL'
            values.append(f"({seat_id}, {year}, '{etype}', {date_sql}, {tv_sql})")

        result = run_sql(f"""
            INSERT INTO elections (seat_id, election_year, election_type, election_date, total_votes_cast)
            VALUES {', '.join(values)}
            ON CONFLICT DO NOTHING
            RETURNING id, seat_id, election_year, election_type
        """)

        for r in (result or []):
            key = (r['seat_id'], r['election_year'], r['election_type'])
            existing_elections[key] = {
                'id': r['id'], 'date': None, 'total_votes': None, 'linked': None,
            }

        print(f'  Created {len(result or [])} elections')

    # Step 3: Update existing Lt Gov elections with dates
    print('\nUpdating Lt Gov elections with missing dates...')
    updates_done = 0
    for race in combined_generals:
        year = race['year']
        if not race.get('election_date'):
            continue
        key = (LTGOV_SEAT_ID, year, 'General')
        if key in existing_elections and existing_elections[key].get('date') is None:
            eid = existing_elections[key]['id']
            tv = race['total_votes'] or 'NULL'
            run_sql(f"""
                UPDATE elections
                SET election_date = '{race['election_date']}',
                    total_votes_cast = COALESCE(total_votes_cast, {tv})
                WHERE id = {eid}
            """, exit_on_error=False)
            updates_done += 1

    print(f'  Updated {updates_done} elections')

    # Step 4: Link Gov/Lt Gov general elections
    print('\nLinking Gov/Lt Gov general elections...')
    links_done = 0
    for race in combined_generals:
        year = race['year']
        gov_key = (GOV_SEAT_ID, year, 'General')
        ltgov_key = (LTGOV_SEAT_ID, year, 'General')

        if gov_key in existing_elections and ltgov_key in existing_elections:
            gov_id = existing_elections[gov_key]['id']
            ltgov_id = existing_elections[ltgov_key]['id']

            if not existing_elections[gov_key].get('linked'):
                run_sql(f"UPDATE elections SET linked_election_id = {ltgov_id} WHERE id = {gov_id}",
                        exit_on_error=False)
                run_sql(f"UPDATE elections SET linked_election_id = {gov_id} WHERE id = {ltgov_id}",
                        exit_on_error=False)
                links_done += 1

    print(f'  Linked {links_done} election pairs')

    # Step 5: Create candidacies
    print('\nCreating candidacies...')
    candidacy_values = []
    skipped = 0

    for idx, (seat_id, year, etype, name, party, votes, pct, result, is_last) in enumerate(candidacies_plan):
        key = (seat_id, year, etype)
        elec_info = existing_elections.get(key)
        if not elec_info:
            print(f'  WARNING: No election for {key}')
            continue
        election_id = elec_info['id']

        if idx not in match_results:
            print(f'  WARNING: No candidate match for "{name}"')
            continue
        cand_id = match_results[idx][0]

        if (election_id, cand_id) in existing_candidacies:
            skipped += 1
            continue

        party_sql = escape_sql(party)
        votes_sql = str(votes) if votes is not None else 'NULL'
        pct_sql = str(pct) if pct is not None else 'NULL'
        result_sql = escape_sql(result)

        candidacy_values.append(
            f"({election_id}, {cand_id}, {party_sql}, {votes_sql}, {pct_sql}, {result_sql})"
        )

    if candidacy_values:
        print(f'  Inserting {len(candidacy_values)} candidacies (skipped {skipped} existing)...')
        batch_size = 200
        for i in range(0, len(candidacy_values), batch_size):
            batch = candidacy_values[i:i+batch_size]
            run_sql(f"""
                INSERT INTO candidacies (election_id, candidate_id, party,
                                          votes_received, vote_percentage, result)
                VALUES {', '.join(batch)}
                ON CONFLICT DO NOTHING
            """, exit_on_error=False)
            print(f'    Batch {i//batch_size + 1}: {len(batch)} rows')
            if i + batch_size < len(candidacy_values):
                time.sleep(1)
    else:
        print(f'  No new candidacies (skipped {skipped} existing)')

    # Step 6: Set running_mate_candidacy_id for Gov↔Lt Gov general pairs
    print('\nSetting running_mate_candidacy_id for Gov↔Lt Gov generals...')
    mate_links = 0
    for race in combined_generals:
        year = race['year']
        if not race['total_votes'] or race['total_votes'] == 0:
            continue

        gov_key = (GOV_SEAT_ID, year, 'General')
        ltgov_key = (LTGOV_SEAT_ID, year, 'General')

        if gov_key not in existing_elections or ltgov_key not in existing_elections:
            continue

        gov_eid = existing_elections[gov_key]['id']
        ltgov_eid = existing_elections[ltgov_key]['id']

        # Match by party: for each Gov candidacy, find matching Lt Gov candidacy
        result = run_sql(f"""
            UPDATE candidacies gov_cy
            SET running_mate_candidacy_id = ltgov_cy.id
            FROM candidacies ltgov_cy
            WHERE gov_cy.election_id = {gov_eid}
              AND ltgov_cy.election_id = {ltgov_eid}
              AND gov_cy.party = ltgov_cy.party
              AND gov_cy.running_mate_candidacy_id IS NULL
            RETURNING gov_cy.id
        """, exit_on_error=False)
        gov_linked = len(result or [])

        result = run_sql(f"""
            UPDATE candidacies ltgov_cy
            SET running_mate_candidacy_id = gov_cy.id
            FROM candidacies gov_cy
            WHERE ltgov_cy.election_id = {ltgov_eid}
              AND gov_cy.election_id = {gov_eid}
              AND ltgov_cy.party = gov_cy.party
              AND ltgov_cy.running_mate_candidacy_id IS NULL
            RETURNING ltgov_cy.id
        """, exit_on_error=False)
        ltgov_linked = len(result or [])

        if gov_linked or ltgov_linked:
            print(f'  {year}: {gov_linked} Gov→LtGov, {ltgov_linked} LtGov→Gov')
            mate_links += gov_linked + ltgov_linked

    print(f'  Total: {mate_links} running mate links set')

    # --- Verification ---
    print('\n=== Verification ===')
    verify = run_sql(f"""
        SELECT e.seat_id, e.election_year, e.election_type, e.election_date,
               e.linked_election_id, COUNT(cy.id) as num_cands
        FROM elections e
        LEFT JOIN candidacies cy ON cy.election_id = e.id
        WHERE e.seat_id IN ({GOV_SEAT_ID}, {LTGOV_SEAT_ID})
          AND e.election_year BETWEEN 1998 AND 2018
        GROUP BY e.seat_id, e.election_year, e.election_type, e.election_date, e.linked_election_id
        ORDER BY e.election_year, e.election_type, e.seat_id
    """)

    for r in verify:
        office = 'Governor' if r['seat_id'] == GOV_SEAT_ID else 'Lt Gov '
        linked = f' ↔ {r["linked_election_id"]}' if r['linked_election_id'] else ''
        print(f'  {r["election_year"]} {r["election_type"]:15s} {office} {r["num_cands"]:2d} cands  date={r["election_date"]}{linked}')


if __name__ == '__main__':
    main()
