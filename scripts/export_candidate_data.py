#!/usr/bin/env python3
"""
Export candidate-level data for candidate detail pages.

Generates:
  - site/data/candidates/{ST}.json — one file per state with all candidate data

Usage:
    python3 scripts/export_candidate_data.py                  # Export all 50 states
    python3 scripts/export_candidate_data.py --state PA       # Single state
    python3 scripts/export_candidate_data.py --dry-run        # Show queries only
"""

import sys
import os
import json
import time
import argparse
from datetime import datetime
from collections import defaultdict

import httpx
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

SITE_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'site', 'data')

# Minimum election year to include (matches export_district_data.py)
MIN_EXPORT_YEAR = {
    'NE': 2014,
}


def run_sql(query, retries=5):
    for attempt in range(retries):
        resp = httpx.post(
            f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
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
        sys.exit(1)


def export_candidates(dry_run=False, single_state=None):
    """Export candidate data for all states."""
    label = single_state or 'all 50 states'
    print(f'Exporting candidate data for {label}...')

    state_filter = f"AND st.abbreviation = '{single_state}'" if single_state else ""

    # Query 1: All candidacies with election + district context
    # This is the primary query — determines which candidates appear in which state file
    q_candidacies = f"""
        SELECT
            st.abbreviation as state,
            cy.candidate_id,
            cy.election_id,
            c.full_name,
            c.first_name,
            c.last_name,
            c.gender,
            c.hometown,
            cy.party,
            cy.caucus,
            cy.votes_received as votes,
            cy.vote_percentage as pct,
            cy.result,
            cy.is_incumbent,
            cy.is_write_in,
            cy.candidate_status,
            cy.filing_date,
            e.election_date,
            e.election_year,
            e.election_type,
            e.total_votes_cast,
            e.result_status,
            e.forecast_rating,
            e.seat_id,
            d.chamber,
            d.district_number,
            d.district_name,
            s.office_level,
            s.office_type
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON cy.candidate_id = c.id
        WHERE s.office_level IN ('Legislative', 'Statewide')
          {state_filter}
        ORDER BY st.abbreviation, cy.candidate_id, e.election_year DESC, e.election_type
    """

    # Query 2: Seat terms for all legislative candidates
    q_terms = f"""
        SELECT
            st.abbreviation as state,
            stm.candidate_id,
            stm.seat_id,
            stm.party,
            stm.caucus,
            stm.start_date,
            stm.end_date,
            stm.start_reason,
            stm.end_reason,
            stm.notes,
            d.chamber,
            d.district_number,
            d.district_name,
            s.office_type
        FROM seat_terms stm
        JOIN seats s ON stm.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_level IN ('Legislative', 'Statewide')
          {state_filter}
        ORDER BY st.abbreviation, stm.candidate_id, stm.start_date
    """

    # Query 3: Party switches for candidates
    q_switches = f"""
        SELECT
            st.abbreviation as state,
            ps.candidate_id,
            ps.old_party,
            ps.new_party,
            ps.old_caucus,
            ps.new_caucus,
            ps.switch_date,
            ps.switch_year
        FROM party_switches ps
        JOIN states st ON ps.state_id = st.id
        WHERE 1=1
          {"AND st.abbreviation = '" + single_state + "'" if single_state else ""}
        ORDER BY st.abbreviation, ps.candidate_id, ps.switch_year
    """

    # Query 4: All candidacies grouped by election_id for opponent lookup
    # (We need to know who else ran in each election)
    q_opponents = f"""
        SELECT
            st.abbreviation as state,
            cy.election_id,
            cy.candidate_id,
            c.full_name as name,
            cy.party,
            cy.caucus,
            cy.votes_received as votes,
            cy.vote_percentage as pct,
            cy.result,
            cy.is_incumbent
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON cy.candidate_id = c.id
        WHERE s.office_level IN ('Legislative', 'Statewide')
          {state_filter}
        ORDER BY st.abbreviation, cy.election_id,
            CASE cy.result WHEN 'Won' THEN 0 WHEN 'Advanced' THEN 1 ELSE 2 END,
            cy.votes_received DESC NULLS LAST
    """

    if dry_run:
        print('  Would run 4 queries and write candidate JSON files')
        print(f'\n  Sample query (candidacies):\n{q_candidacies[:300]}...')
        return

    print('  Running 4 bulk queries...')
    candidacies_data = run_sql(q_candidacies)
    print(f'    1/4 candidacies: {len(candidacies_data)} rows')
    terms_data = run_sql(q_terms)
    print(f'    2/4 seat_terms: {len(terms_data)} rows')
    switches_data = run_sql(q_switches)
    print(f'    3/4 party_switches: {len(switches_data)} rows')
    opponents_data = run_sql(q_opponents)
    print(f'    4/4 opponents: {len(opponents_data)} rows')

    # --- Index data ---

    # Group opponents by election_id for quick lookup
    opponents_by_election = defaultdict(list)
    for r in opponents_data:
        opponents_by_election[r['election_id']].append(r)

    # Group seat terms by (state, candidate_id)
    terms_by_candidate = defaultdict(list)
    for r in terms_data:
        terms_by_candidate[(r['state'], r['candidate_id'])].append(r)

    # Group party switches by (state, candidate_id)
    switches_by_candidate = defaultdict(list)
    for r in switches_data:
        switches_by_candidate[(r['state'], r['candidate_id'])].append(r)

    # --- Build candidate records, grouped by state ---
    # A candidate belongs to the state of their most recent candidacy
    candidates_by_state = defaultdict(dict)  # state -> {candidate_id -> candidate_obj}

    for r in candidacies_data:
        state = r['state']
        cid = r['candidate_id']
        min_year = MIN_EXPORT_YEAR.get(state, 0)
        if r['election_year'] < min_year:
            continue

        if cid not in candidates_by_state[state]:
            candidates_by_state[state][cid] = {
                'id': cid,
                'full_name': r['full_name'],
                'first_name': r['first_name'],
                'last_name': r['last_name'],
                'gender': r['gender'],
                'hometown': r['hometown'],
                'candidacies': [],
                'seat_terms': [],
                'party_switches': [],
            }

        cand = candidates_by_state[state][cid]

        # Build opponent list for this election
        opp_list = []
        for opp in opponents_by_election.get(r['election_id'], []):
            if opp['candidate_id'] == cid:
                continue
            opp_obj = {
                'name': opp['name'],
                'id': opp['candidate_id'],
                'party': opp['party'],
                'votes': opp['votes'],
                'pct': float(opp['pct']) if opp['pct'] is not None else None,
                'result': opp['result'],
            }
            if opp.get('caucus'):
                opp_obj['caucus'] = opp['caucus']
            opp_list.append(opp_obj)

        candidacy_obj = {
            'year': r['election_year'],
            'type': r['election_type'],
            'date': str(r['election_date']) if r.get('election_date') else None,
            'state': state,
            'chamber': r['chamber'],
            'district': r['district_number'],
            'district_name': r['district_name'],
            'office_type': r.get('office_type'),
            'party': r['party'],
            'votes': r['votes'],
            'pct': float(r['pct']) if r['pct'] is not None else None,
            'result': r['result'],
            'is_incumbent': r['is_incumbent'],
            'is_write_in': r['is_write_in'],
            'total_votes': r['total_votes_cast'],
            'result_status': r['result_status'],
            'forecast_rating': r['forecast_rating'],
            'opponents': opp_list,
        }
        if r.get('caucus'):
            candidacy_obj['caucus'] = r['caucus']
        if r.get('candidate_status'):
            candidacy_obj['candidate_status'] = r['candidate_status']

        cand['candidacies'].append(candidacy_obj)

    # Attach seat terms
    for (state, cid), terms in terms_by_candidate.items():
        if state in candidates_by_state and cid in candidates_by_state[state]:
            cand = candidates_by_state[state][cid]
            for t in terms:
                term_obj = {
                    'state': state,
                    'chamber': t['chamber'],
                    'district': t['district_number'],
                    'district_name': t['district_name'],
                    'party': t['party'],
                    'start_date': str(t['start_date']) if t.get('start_date') else None,
                    'end_date': str(t['end_date']) if t.get('end_date') else None,
                    'start_reason': t['start_reason'],
                    'end_reason': t['end_reason'],
                }
                if t.get('caucus'):
                    term_obj['caucus'] = t['caucus']
                if t.get('office_type'):
                    term_obj['office_type'] = t['office_type']
                cand['seat_terms'].append(term_obj)

    # Attach party switches
    for (state, cid), switches in switches_by_candidate.items():
        if state in candidates_by_state and cid in candidates_by_state[state]:
            cand = candidates_by_state[state][cid]
            for sw in switches:
                sw_obj = {
                    'old_party': sw['old_party'],
                    'new_party': sw['new_party'],
                    'year': sw['switch_year'],
                    'date': str(sw['switch_date']) if sw.get('switch_date') else None,
                }
                if sw.get('old_caucus'):
                    sw_obj['old_caucus'] = sw['old_caucus']
                if sw.get('new_caucus'):
                    sw_obj['new_caucus'] = sw['new_caucus']
                cand['party_switches'].append(sw_obj)

    # --- Determine current office for each candidate ---
    for state, cands in candidates_by_state.items():
        for cid, cand in cands.items():
            current_term = None
            for t in cand['seat_terms']:
                if t['end_date'] is None:
                    current_term = t
                    break
            if current_term:
                cand['current_office'] = {
                    'state': current_term['state'],
                    'chamber': current_term['chamber'],
                    'district': current_term['district'],
                    'district_name': current_term['district_name'],
                    'party': current_term['party'],
                    'since': current_term['start_date'],
                }
                if current_term.get('caucus'):
                    cand['current_office']['caucus'] = current_term['caucus']
                if current_term.get('office_type'):
                    cand['current_office']['office_type'] = current_term['office_type']

            # Determine most recent party for display.
            # Use actual registered party (not caucus). Caucus is stored
            # separately for annotations (AK coalition, NE nonpartisan).
            party = None
            caucus = None
            if current_term:
                party = current_term.get('party')
                caucus = current_term.get('caucus')
            if not party:
                for cy in cand['candidacies']:
                    party = cy.get('party')
                    if not caucus:
                        caucus = cy.get('caucus')
                    if party:
                        break
            if not party:
                for t in reversed(cand['seat_terms']):
                    party = t.get('party')
                    if not caucus:
                        caucus = t.get('caucus')
                    if party:
                        break
            cand['party'] = party
            # Store caucus when it adds info beyond party (AK coalition, NE)
            if caucus and caucus != party:
                cand['caucus'] = caucus

            # Strip empty lists to save space
            if not cand['party_switches']:
                del cand['party_switches']
            if not cand['seat_terms']:
                del cand['seat_terms']

    # --- Compute quality flags ---
    total_flags = 0
    for state, cands in candidates_by_state.items():
        # Build name index for duplicate detection
        # Key: (last_name_lower, first_3_chars_lower) -> list of candidates
        name_groups = defaultdict(list)
        for cid, cand in cands.items():
            ln = (cand.get('last_name') or '').lower().strip()
            fn = (cand.get('first_name') or '').lower().strip()[:3]
            if len(ln) >= 2 and len(fn) >= 2:
                name_groups[(ln, fn)].append(cand)

        for cid, cand in cands.items():
            flags = []

            # 1. Potential duplicate name
            ln = (cand.get('last_name') or '').lower().strip()
            fn = (cand.get('first_name') or '').lower().strip()[:3]
            if len(ln) >= 2 and len(fn) >= 2:
                group = name_groups.get((ln, fn), [])
                others = [c for c in group if c['id'] != cand['id']]
                if others:
                    flags.append({
                        'type': 'potential_duplicate',
                        'msg': f"{len(others)} other '{cand.get('last_name')}' in {state}",
                        'ids': [c['id'] for c in others],
                    })

            # 2. Missing party
            if not cand.get('party'):
                flags.append({'type': 'missing_party', 'msg': 'No party identified'})

            # 3. Missing votes on certified general/special elections
            missing_votes = 0
            for cy in cand['candidacies']:
                if cy.get('result_status') == 'Certified' and cy.get('result') in ('Won', 'Lost'):
                    if cy.get('votes') is None and cy.get('pct') is None:
                        missing_votes += 1
            if missing_votes:
                flags.append({
                    'type': 'missing_votes',
                    'msg': f'{missing_votes} certified race(s) without vote totals',
                })

            # 4. Current officeholder with no candidacies
            if cand.get('current_office') and not cand['candidacies']:
                flags.append({
                    'type': 'no_candidacies',
                    'msg': 'Current officeholder with no election history',
                })

            if flags:
                cand['quality_flags'] = flags
                total_flags += len(flags)

    print(f'\n  Quality flags: {total_flags} flags across all candidates')

    # --- Write per-state JSON files ---
    out_dir = os.path.join(SITE_DATA_DIR, 'candidates')
    os.makedirs(out_dir, exist_ok=True)
    generated_at = datetime.utcnow().isoformat() + 'Z'

    total_candidates = 0

    for state in sorted(candidates_by_state.keys()):
        cands = candidates_by_state[state]
        cand_list = sorted(cands.values(), key=lambda c: (c.get('last_name') or '', c.get('first_name') or ''))

        total_candidates += len(cand_list)

        result = {
            'generated_at': generated_at,
            'state': state,
            'candidates': cand_list,
        }

        out_path = os.path.join(out_dir, f'{state}.json')
        with open(out_path, 'w') as f:
            json.dump(result, f, separators=(',', ':'))
        size_kb = os.path.getsize(out_path) / 1024
        print(f'    {state}: {len(cand_list)} candidates, {size_kb:.0f} KB')

    print(f'\n  Total: {total_candidates} candidates across {len(candidates_by_state)} states')
    print(f'  Written to {out_dir}/')

    # --- Write search index ---
    # Lightweight array for the browse/search page
    new_entries = []
    exported_states = set(candidates_by_state.keys())
    for state in sorted(exported_states):
        for cand in candidates_by_state[state].values():
            current = cand.get('current_office')
            entry = {
                'id': cand['id'],
                'n': cand['full_name'],
                'st': state,
                'p': cand.get('party', ''),
            }
            if cand.get('caucus'):
                entry['c'] = cand['caucus']
            if current:
                entry['ch'] = current['chamber']
                entry['d'] = current['district']
                entry['a'] = 1  # active (current officeholder)
            else:
                # Use most recent candidacy for chamber/district
                if cand['candidacies']:
                    latest = cand['candidacies'][0]
                    entry['ch'] = latest['chamber']
                    entry['d'] = latest['district']
            # Include office_type for statewide candidates (Governor, AG, etc.)
            if cand['candidacies']:
                ot = cand['candidacies'][0].get('office_type')
                if ot and ot != 'State Representative' and ot != 'State Senator':
                    entry['ot'] = ot
            new_entries.append(entry)

    search_path = os.path.join(SITE_DATA_DIR, 'candidate_search.json')

    # When exporting a single state, merge with existing index
    if single_state:
        try:
            with open(search_path) as f:
                existing = json.load(f).get('candidates', [])
            # Keep entries from other states, replace entries from exported state(s)
            kept = [e for e in existing if e.get('st') not in exported_states]
            search_index = sorted(kept + new_entries, key=lambda e: (e.get('st', ''), e.get('n', '')))
        except (FileNotFoundError, json.JSONDecodeError):
            search_index = new_entries
    else:
        search_index = new_entries

    with open(search_path, 'w') as f:
        json.dump({'generated_at': generated_at, 'candidates': search_index}, f, separators=(',', ':'))
    size_kb = os.path.getsize(search_path) / 1024
    print(f'\n  Search index: {len(search_index)} entries, {size_kb:.0f} KB')


def main():
    parser = argparse.ArgumentParser(description='Export candidate data for site pages')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--state', type=str, help='Single state (2-letter abbreviation)')
    args = parser.parse_args()

    if args.state:
        export_candidates(dry_run=args.dry_run, single_state=args.state.upper())
    else:
        export_candidates(dry_run=args.dry_run)

    print('\nDone.')


if __name__ == '__main__':
    main()
