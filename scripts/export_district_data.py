#!/usr/bin/env python3
"""
Export district-level data (election history, candidates, forecasts) for district detail pages.

Generates:
  - site/data/districts/{ST}.json — one file per state with all district data

Usage:
    python3 scripts/export_district_data.py                  # Export all 50 states
    python3 scripts/export_district_data.py --state PA       # Single state
    python3 scripts/export_district_data.py --dry-run        # Show queries only
"""

import sys
import os
import json
import time
import argparse
import math
from datetime import datetime

import httpx
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

SITE_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'site', 'data')

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

def export_all_districts(dry_run=False, single_state=None):
    """Export district data for all states using bulk queries."""
    label = single_state or 'all 50 states'
    print(f'Exporting district data for {label}...')

    state_filter = f"AND st.abbreviation = '{single_state}'" if single_state else ""

    # Query 1: Districts + seats
    q_districts = f"""
        SELECT
            st.abbreviation as state,
            d.id as district_id,
            d.chamber,
            d.district_number,
            d.district_name,
            d.num_seats,
            d.pres_2024_margin,
            d.pres_2024_winner,
            d.is_floterial,
            s.id as seat_id,
            s.seat_label,
            s.seat_designator,
            s.current_holder,
            s.current_holder_party,
            COALESCE(s.current_holder_caucus, s.current_holder_party) as current_holder_caucus,
            s.term_length_years,
            s.next_regular_election_year,
            s.election_class
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_level = 'Legislative'
          {state_filter}
        ORDER BY st.abbreviation, d.chamber,
            CASE WHEN d.district_number SIMILAR TO '[0-9]+' THEN d.district_number::int ELSE 99999 END,
            d.district_number, s.seat_designator
    """

    # Query 2: All elections for legislative seats (historical + 2026)
    q_elections = f"""
        SELECT
            st.abbreviation as state,
            e.id as election_id,
            e.seat_id,
            e.election_date,
            e.election_year,
            e.election_type,
            e.total_votes_cast,
            e.is_open_seat,
            e.result_status,
            e.filing_deadline,
            e.forecast_rating
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_level = 'Legislative'
          {state_filter}
        ORDER BY st.abbreviation, e.seat_id, e.election_year DESC, e.election_type
    """

    # Query 3: All candidacies for legislative elections
    q_candidacies = f"""
        SELECT
            st.abbreviation as state,
            cy.election_id,
            c.full_name as name,
            cy.party,
            cy.votes_received as votes,
            cy.vote_percentage as pct,
            cy.result,
            cy.is_incumbent,
            cy.is_write_in,
            cy.candidate_status
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON cy.candidate_id = c.id
        WHERE s.office_level = 'Legislative'
          {state_filter}
        ORDER BY st.abbreviation, cy.election_id,
            CASE cy.result WHEN 'Won' THEN 0 WHEN 'Advanced' THEN 1 ELSE 2 END,
            cy.votes_received DESC NULLS LAST
    """

    # Query 4: Seat terms (officeholder history) — for "Since YYYY" info
    q_terms = f"""
        SELECT
            st.abbreviation as state,
            stm.seat_id,
            c.full_name as holder_name,
            stm.party as holder_party,
            stm.start_date,
            stm.end_date,
            stm.start_reason
        FROM seat_terms stm
        JOIN seats s ON stm.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON stm.candidate_id = c.id
        WHERE s.office_level = 'Legislative'
          AND stm.end_date IS NULL
          {state_filter}
        ORDER BY st.abbreviation, stm.seat_id
    """

    # Query 5: State info (for primary type, runoffs)
    q_states = f"""
        SELECT abbreviation, state_name, uses_jungle_primary, has_runoffs,
               senate_term_years, house_term_years
        FROM states
        {"WHERE abbreviation = '" + single_state + "'" if single_state else ""}
        ORDER BY abbreviation
    """

    # Query 6: Forecasts for 2026 legislative races
    q_forecasts = f"""
        SELECT
            st.abbreviation as state,
            f.election_id,
            e.seat_id,
            f.source,
            f.rating
        FROM forecasts f
        JOIN elections e ON f.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_level = 'Legislative'
          AND e.election_year = 2026
          AND e.election_type = 'General'
          {state_filter}
        ORDER BY st.abbreviation, e.seat_id
    """

    if dry_run:
        print('  Would run 6 queries and write district JSON files')
        print(f'\n  Sample query (districts):\n{q_districts[:300]}...')
        return

    print('  Running 6 bulk queries...')
    districts_data = run_sql(q_districts)
    print(f'    1/6 districts+seats: {len(districts_data)} rows')
    elections_data = run_sql(q_elections)
    print(f'    2/6 elections: {len(elections_data)} rows')
    candidacies_data = run_sql(q_candidacies)
    print(f'    3/6 candidacies: {len(candidacies_data)} rows')
    terms_data = run_sql(q_terms)
    print(f'    4/6 seat_terms: {len(terms_data)} rows')
    states_data = run_sql(q_states)
    print(f'    5/6 states: {len(states_data)} rows')
    forecasts_data = run_sql(q_forecasts)
    print(f'    6/6 forecasts: {len(forecasts_data)} rows')

    # --- Index data ---

    # State info lookup
    states_info = {r['abbreviation']: r for r in states_data}

    # Elections indexed by seat_id
    elections_by_seat = {}
    for r in elections_data:
        elections_by_seat.setdefault(r['seat_id'], []).append(r)

    # Candidacies indexed by election_id
    candidacies_by_election = {}
    for r in candidacies_data:
        candidacies_by_election.setdefault(r['election_id'], []).append(r)

    # Current terms indexed by seat_id
    terms_by_seat = {}
    for r in terms_data:
        terms_by_seat[r['seat_id']] = r

    # Forecasts indexed by seat_id
    forecasts_by_seat = {}
    for r in forecasts_data:
        forecasts_by_seat.setdefault(r['seat_id'], []).append({
            'source': r['source'],
            'rating': r['rating'],
        })

    # --- Group districts by state, then by district_id ---
    # Multiple seats can share the same district (multi-member)
    districts_by_state = {}
    for r in districts_data:
        state = r['state']
        did = r['district_id']
        if state not in districts_by_state:
            districts_by_state[state] = {}
        if did not in districts_by_state[state]:
            districts_by_state[state][did] = {
                'district_number': r['district_number'],
                'district_name': r['district_name'],
                'chamber': r['chamber'],
                'num_seats': r['num_seats'],
                'pres_2024_margin': r['pres_2024_margin'],
                'pres_2024_winner': r['pres_2024_winner'],
                'is_floterial': r['is_floterial'],
                'seats': [],
            }
        # Build seat object
        seat_id = r['seat_id']
        term = terms_by_seat.get(seat_id)
        since_year = None
        if term and term.get('start_date'):
            since_year = int(str(term['start_date'])[:4])

        # Build election history for this seat
        seat_elections = []
        for e in elections_by_seat.get(seat_id, []):
            cands = candidacies_by_election.get(e['election_id'], [])
            # Build candidate list
            candidate_list = []
            for c in cands:
                candidate_list.append({
                    'name': c['name'],
                    'party': c['party'],
                    'votes': c['votes'],
                    'pct': float(c['pct']) if c['pct'] is not None else None,
                    'result': c['result'],
                    'is_incumbent': c['is_incumbent'],
                    'is_write_in': c['is_write_in'],
                })

            seat_elections.append({
                'year': e['election_year'],
                'type': e['election_type'],
                'date': str(e['election_date']) if e.get('election_date') else None,
                'total_votes': e['total_votes_cast'],
                'is_open_seat': e['is_open_seat'],
                'result_status': e['result_status'],
                'filing_deadline': str(e['filing_deadline']) if e.get('filing_deadline') else None,
                'forecast_rating': e['forecast_rating'],
                'candidates': candidate_list,
            })

        # Forecast info for this seat
        forecast = forecasts_by_seat.get(seat_id)

        districts_by_state[state][did]['seats'].append({
            'seat_id': seat_id,
            'seat_label': r['seat_label'],
            'seat_designator': r['seat_designator'],
            'current_holder': r['current_holder'],
            'current_holder_party': r['current_holder_party'],
            'current_holder_caucus': r['current_holder_caucus'],
            'term_length': r['term_length_years'],
            'next_election': r['next_regular_election_year'],
            'election_class': r['election_class'],
            'since_year': since_year,
            'elections': seat_elections,
            'forecast': forecast,
        })

    # --- Compute similar districts across all states ---
    # Collect all districts with their pres margins for cross-state similarity
    all_district_margins = []
    for state, dists in districts_by_state.items():
        for did, dinfo in dists.items():
            margin = dinfo.get('pres_2024_margin')
            if margin:
                try:
                    margin_val = float(margin)
                except (ValueError, TypeError):
                    continue
                all_district_margins.append({
                    'state': state,
                    'chamber': dinfo['chamber'],
                    'district_number': dinfo['district_number'],
                    'district_name': dinfo['district_name'],
                    'margin': margin_val,
                })

    # Sort by margin for efficient lookup
    all_district_margins.sort(key=lambda x: x['margin'])

    def find_similar(state, chamber, margin_val, count=5):
        """Find similar districts from other states within same chamber type."""
        # Map chamber types: Senate-like vs House-like
        is_upper = chamber == 'Senate'
        similar = []
        for d in all_district_margins:
            if d['state'] == state:
                continue
            d_is_upper = d['chamber'] == 'Senate'
            if d_is_upper != is_upper:
                continue
            diff = abs(d['margin'] - margin_val)
            if diff <= 8:  # within 8 points
                similar.append({
                    'state': d['state'],
                    'chamber': d['chamber'],
                    'district_number': d['district_number'],
                    'district_name': d['district_name'],
                    'pres_margin': f"{d['margin']:+.1f}",
                    'diff': diff,
                })
        similar.sort(key=lambda x: x['diff'])
        # Return top N, removing diff field
        return [{'state': s['state'], 'chamber': s['chamber'],
                 'district_number': s['district_number'],
                 'district_name': s['district_name'],
                 'pres_margin': s['pres_margin']}
                for s in similar[:count]]

    # --- Write per-state JSON files ---
    out_dir = os.path.join(SITE_DATA_DIR, 'districts')
    os.makedirs(out_dir, exist_ok=True)
    generated_at = datetime.utcnow().isoformat() + 'Z'

    total_districts = 0
    total_elections = 0

    for state in sorted(districts_by_state.keys()):
        si = states_info.get(state, {})
        dists = districts_by_state[state]

        district_list = []
        for did in sorted(dists.keys()):
            d = dists[did]

            # Compute partisan shift: compare earliest and most recent general election margin
            general_margins = []
            for seat in d['seats']:
                for e in seat['elections']:
                    if e['type'] == 'General' and e['candidates']:
                        winner = next((c for c in e['candidates'] if c['result'] == 'Won'), None)
                        if winner and winner['pct'] is not None:
                            # Use two-party margin, capped for uncontested races
                            raw_margin = (winner['pct'] - 50.0) * 2
                            # Cap at ±40 to avoid absurd values from uncontested races
                            margin = max(-40.0, min(40.0, raw_margin))
                            party_sign = 1 if winner['party'] == 'D' else -1 if winner['party'] == 'R' else 0
                            if party_sign != 0:
                                general_margins.append({
                                    'year': e['year'],
                                    'margin': margin * party_sign,
                                })

            general_margins.sort(key=lambda x: x['year'])
            partisan_shift = None
            if len(general_margins) >= 2:
                earliest = general_margins[0]['margin']
                latest = general_margins[-1]['margin']
                partisan_shift = round(latest - earliest, 1)

            # Similar districts
            margin_val = None
            if d.get('pres_2024_margin'):
                try:
                    margin_val = float(d['pres_2024_margin'])
                except (ValueError, TypeError):
                    pass

            similar = find_similar(state, d['chamber'], margin_val) if margin_val is not None else []

            district_obj = {
                'district_number': d['district_number'],
                'district_name': d['district_name'],
                'chamber': d['chamber'],
                'num_seats': d['num_seats'],
                'is_floterial': d['is_floterial'],
                'pres_2024_margin': d['pres_2024_margin'],
                'pres_2024_winner': d['pres_2024_winner'],
                'seats': d['seats'],
                'partisan_shift': partisan_shift,
                'similar_districts': similar,
            }

            # Count elections for stats
            for seat in d['seats']:
                total_elections += len(seat['elections'])

            district_list.append(district_obj)

        total_districts += len(district_list)

        result = {
            'generated_at': generated_at,
            'state': state,
            'state_name': si.get('state_name', state),
            'uses_jungle_primary': si.get('uses_jungle_primary', False),
            'has_runoffs': si.get('has_runoffs', False),
            'senate_term_years': si.get('senate_term_years'),
            'house_term_years': si.get('house_term_years'),
            'districts': district_list,
        }

        out_path = os.path.join(out_dir, f'{state}.json')
        with open(out_path, 'w') as f:
            json.dump(result, f, separators=(',', ':'))  # compact — these files can be large
        size_kb = os.path.getsize(out_path) / 1024
        print(f'    {state}: {len(district_list)} districts, {size_kb:.0f} KB')

    print(f'\n  Total: {total_districts} districts, {total_elections} election records')
    print(f'  Written to {out_dir}/')

def main():
    parser = argparse.ArgumentParser(description='Export district data for site pages')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--state', type=str, help='Single state (2-letter abbreviation)')
    args = parser.parse_args()

    if args.state:
        export_all_districts(dry_run=args.dry_run, single_state=args.state.upper())
    else:
        export_all_districts(dry_run=args.dry_run)

    print('\nDone.')

if __name__ == '__main__':
    main()
