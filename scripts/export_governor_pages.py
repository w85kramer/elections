#!/usr/bin/env python3
"""
Export per-state governor page data for the site.

Generates:
  - site/data/governors/{ST}.json — one file per state with governor timeline,
    election history, and 2026 race data

Usage:
    python3 scripts/export_governor_pages.py                  # Export all 50 states
    python3 scripts/export_governor_pages.py --state VA       # Single state
    python3 scripts/export_governor_pages.py --dry-run        # Show queries only
"""

import sys
import os
import json
import time
import argparse
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


def export_governor_pages(dry_run=False, single_state=None):
    """Export governor page data for all states using bulk queries."""
    label = single_state or 'all 50 states'
    print(f'Exporting governor page data for {label}...')

    state_filter = f"AND st.abbreviation = '{single_state}'" if single_state else ""
    state_filter_direct = f"WHERE abbreviation = '{single_state}'" if single_state else ""

    # Query 1: Governor seats — current holder, term info
    q_seats = f"""
        SELECT
            st.abbreviation as state,
            st.state_name,
            st.gov_term_years,
            st.gov_term_limit,
            d.pres_2024_margin,
            se.id as seat_id,
            se.current_holder,
            se.current_holder_party,
            se.next_regular_election_year
        FROM states st
        JOIN districts d ON d.state_id = st.id AND d.office_level = 'Statewide'
        JOIN seats se ON se.district_id = d.id AND se.office_type = 'Governor'
        WHERE 1=1
          {state_filter}
        ORDER BY st.abbreviation
    """

    # Query 2: All seat_terms (full governor history per state)
    q_terms = f"""
        SELECT
            st.abbreviation as state,
            c.full_name as name,
            stm.party,
            stm.start_date,
            stm.end_date,
            stm.start_reason,
            stm.end_reason
        FROM seat_terms stm
        JOIN seats se ON stm.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON stm.candidate_id = c.id
        WHERE se.office_type = 'Governor'
          {state_filter}
        ORDER BY st.abbreviation, stm.start_date
    """

    # Query 3: All governor general elections
    q_elections = f"""
        SELECT
            st.abbreviation as state,
            e.id as election_id,
            e.seat_id,
            e.election_year,
            e.election_type,
            e.election_date,
            e.total_votes_cast,
            e.result_status,
            e.is_open_seat,
            e.filing_deadline,
            e.forecast_rating
        FROM elections e
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE se.office_type = 'Governor'
          {state_filter}
        ORDER BY st.abbreviation, e.election_year DESC, e.election_type
    """

    # Query 4: Candidacies for governor elections
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
            cy.candidate_status,
            cy.is_major,
            e.election_type
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON cy.candidate_id = c.id
        WHERE se.office_type = 'Governor'
          {state_filter}
        ORDER BY st.abbreviation, cy.election_id,
            CASE cy.result WHEN 'Won' THEN 0 WHEN 'Advanced' THEN 1 ELSE 2 END,
            cy.votes_received DESC NULLS LAST
    """

    # Query 5: Forecasts for 2026 governor races
    q_forecasts = f"""
        SELECT
            st.abbreviation as state,
            f.source,
            f.rating,
            f.election_id
        FROM forecasts f
        JOIN elections e ON f.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE se.office_type = 'Governor'
          AND e.election_year = 2026
          AND e.election_type = 'General'
          {state_filter}
        ORDER BY st.abbreviation, f.source
    """

    # Query 6: Primary dates for 2026 governor races
    q_primaries = f"""
        SELECT DISTINCT
            st.abbreviation as state,
            e.election_date as primary_date
        FROM elections e
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE se.office_type = 'Governor'
          AND e.election_type IN ('Primary_D', 'Primary_R', 'Primary')
          AND e.election_year = 2026
          {state_filter}
        ORDER BY st.abbreviation
    """

    if dry_run:
        print('  Would run 6 queries and write governor JSON files')
        print(f'\n  Sample query (seats):\n{q_seats[:300]}...')
        return

    print('  Running 6 bulk queries...')
    seats_data = run_sql(q_seats)
    print(f'    1/6 governor seats: {len(seats_data)} rows')
    terms_data = run_sql(q_terms)
    print(f'    2/6 seat_terms: {len(terms_data)} rows')
    elections_data = run_sql(q_elections)
    print(f'    3/6 elections: {len(elections_data)} rows')
    candidacies_data = run_sql(q_candidacies)
    print(f'    4/6 candidacies: {len(candidacies_data)} rows')
    forecasts_data = run_sql(q_forecasts)
    print(f'    5/6 forecasts: {len(forecasts_data)} rows')
    primaries_data = run_sql(q_primaries)
    print(f'    6/6 primaries: {len(primaries_data)} rows')

    # --- Index data ---

    # Seats by state
    seats_by_state = {}
    for r in seats_data:
        seats_by_state[r['state']] = r

    # Terms by state (ordered by start_date)
    terms_by_state = {}
    for r in terms_data:
        terms_by_state.setdefault(r['state'], []).append(r)

    # Elections by state
    elections_by_state = {}
    for r in elections_data:
        elections_by_state.setdefault(r['state'], []).append(r)

    # Candidacies by election_id
    candidacies_by_election = {}
    for r in candidacies_data:
        candidacies_by_election.setdefault(r['election_id'], []).append(r)

    # Forecasts by state
    forecasts_by_state = {}
    for r in forecasts_data:
        forecasts_by_state.setdefault(r['state'], {})[r['source']] = r['rating']

    # Primary dates by state
    primary_by_state = {}
    for r in primaries_data:
        if r['state'] not in primary_by_state:
            primary_by_state[r['state']] = r['primary_date']

    # Known open seats for 2026
    OPEN_SEATS = {
        'AK', 'AL', 'CA', 'CO', 'FL', 'GA', 'HI', 'IA', 'KS', 'ME', 'MI', 'MN',
        'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'SC', 'SD', 'TN', 'WI', 'WY'
    }

    # --- Write per-state JSON ---
    out_dir = os.path.join(SITE_DATA_DIR, 'governors')
    os.makedirs(out_dir, exist_ok=True)
    generated_at = datetime.utcnow().isoformat() + 'Z'

    total_terms = 0
    total_elections = 0

    for state, seat in sorted(seats_by_state.items()):
        terms = terms_by_state.get(state, [])
        elections = elections_by_state.get(state, [])

        # Build timeline (seat_terms with election margin links)
        # Index general elections by year for margin lookup
        generals_by_year = {}
        for e in elections:
            if e['election_type'] == 'General':
                cands = candidacies_by_election.get(e['election_id'], [])
                winner = next((c for c in cands if c['result'] == 'Won'), None)
                margin = None
                if winner and winner['pct'] is not None:
                    pct = float(winner['pct'])
                    margin = round((pct - 50) * 2, 1)
                    # Positive for winner's party
                generals_by_year[e['election_year']] = {
                    'margin': margin,
                    'winner_party': winner['party'] if winner else None,
                }

        timeline = []
        for t in terms:
            # Match to election: governor usually takes office in Jan of year after election
            # e.g., elected Nov 2021, takes office Jan 2022. So election_year = start_year - 1
            # But some states have same-year inaugurations (e.g., some specials)
            election_year = None
            margin = None
            if t['start_date']:
                start_year = int(t['start_date'][:4])
                # Check year before (normal case) and same year (special/off-cycle)
                for try_year in [start_year - 1, start_year]:
                    if try_year in generals_by_year:
                        ge = generals_by_year[try_year]
                        election_year = try_year
                        margin = ge['margin']
                        break

            timeline.append({
                'name': t['name'],
                'party': t['party'],
                'start': t['start_date'],
                'end': t['end_date'],
                'start_reason': t['start_reason'],
                'end_reason': t['end_reason'],
                'election_year': election_year,
                'margin': margin,
            })
        # Reverse so most recent first
        timeline.reverse()

        # Build elections list with candidacies
        elections_list = []
        for e in elections:
            cands = candidacies_by_election.get(e['election_id'], [])
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
                    'election_type': c['election_type'],
                    'candidate_status': c['candidate_status'],
                    'is_major': c.get('is_major') or False,
                })

            elections_list.append({
                'year': e['election_year'],
                'type': e['election_type'],
                'date': str(e['election_date']) if e.get('election_date') else None,
                'total_votes': e['total_votes_cast'],
                'result_status': e['result_status'],
                'is_open_seat': e['is_open_seat'],
                'filing_deadline': str(e['filing_deadline']) if e.get('filing_deadline') else None,
                'forecast_rating': e['forecast_rating'],
                'candidates': candidate_list,
            })

        # Current governor (first term with no end_date)
        current_gov = None
        for t in terms:
            if t['end_date'] is None:
                current_gov = {
                    'name': t['name'],
                    'party': t['party'],
                    'since': t['start_date'],
                    'start_reason': t['start_reason'],
                }
                break

        # 2026 race data
        race_2026 = None
        if seat['next_regular_election_year'] == 2026:
            # Find 2026 general election
            gen_2026 = next((e for e in elections
                           if e['election_year'] == 2026 and e['election_type'] == 'General'), None)
            fcasts = forecasts_by_state.get(state, {})

            # Gather 2026 candidates
            cands_2026 = []
            for e in elections:
                if e['election_year'] == 2026:
                    for c in candidacies_by_election.get(e['election_id'], []):
                        if not any(x['name'] == c['name'] and x['party'] == c['party']
                                   for x in cands_2026):
                            cands_2026.append({
                                'name': c['name'],
                                'party': c['party'],
                                'status': c['candidate_status'],
                                'result': c['result'],
                                'is_major': c.get('is_major') or False,
                                'election_type': c['election_type'],
                            })

            race_2026 = {
                'is_open_seat': state in OPEN_SEATS,
                'forecast': gen_2026['forecast_rating'] if gen_2026 else None,
                'forecast_cook': fcasts.get('Cook Political Report'),
                'forecast_sabato': fcasts.get("Sabato's Crystal Ball"),
                'election_date': str(gen_2026['election_date']) if gen_2026 and gen_2026.get('election_date') else None,
                'filing_deadline': str(gen_2026['filing_deadline']) if gen_2026 and gen_2026.get('filing_deadline') else None,
                'primary_date': primary_by_state.get(state),
                'candidates': cands_2026,
            }

        # Format presidential margin
        pres_margin = seat.get('pres_2024_margin')
        pres_margin_str = None
        if pres_margin is not None:
            try:
                v = float(pres_margin)
                pres_margin_str = f"D+{v:.1f}" if v > 0 else f"R+{abs(v):.1f}" if v < 0 else "Even"
            except (ValueError, TypeError):
                pres_margin_str = str(pres_margin)

        result = {
            'generated_at': generated_at,
            'state': state,
            'state_name': seat['state_name'],
            'gov_term_years': seat['gov_term_years'],
            'gov_term_limit': seat['gov_term_limit'],
            'pres_2024_margin': pres_margin_str,
            'next_regular_election': seat['next_regular_election_year'],
            'current_governor': current_gov,
            'timeline': timeline,
            'elections': elections_list,
            'race_2026': race_2026,
        }

        out_path = os.path.join(out_dir, f'{state}.json')
        with open(out_path, 'w') as f:
            json.dump(result, f, separators=(',', ':'))
        size_kb = os.path.getsize(out_path) / 1024
        print(f'    {state}: {len(timeline)} governors, {len(elections_list)} elections, {size_kb:.1f} KB')

        total_terms += len(timeline)
        total_elections += len(elections_list)

    print(f'\n  Total: {len(seats_by_state)} states, {total_terms} governor terms, {total_elections} elections')
    print(f'  Written to {out_dir}/')


def main():
    parser = argparse.ArgumentParser(description='Export governor page data')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--state', type=str, help='Single state (2-letter abbreviation)')
    args = parser.parse_args()

    if args.state:
        export_governor_pages(dry_run=args.dry_run, single_state=args.state.upper())
    else:
        export_governor_pages(dry_run=args.dry_run)

    print('\nDone.')


if __name__ == '__main__':
    main()
