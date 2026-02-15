#!/usr/bin/env python3
"""Export governor-specific data files for the site:
  1. data/governors_2026.json — All 36 races with forecasts, candidates, deadlines
  2. data/governor_history.json — Historical seat_terms for trend analysis
"""

import json
import os
import time
from datetime import datetime

import requests

SITE_DIR = '/home/billkramer/elections/site'
API_URL = 'https://api.supabase.com/v1/projects/pikcvwulzfxgwfcfssxc/database/query'

# Load token from .env
env_path = os.path.join(os.path.dirname(SITE_DIR), '.env')
TOKEN = None
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            if line.startswith('SUPABASE_MANAGEMENT_TOKEN='):
                TOKEN = line.strip().split('=', 1)[1].strip('"\'')
if not TOKEN:
    TOKEN = 'sbp_134edd259126b21a7fc11c7a13c0c8c6834d7fa7'

HEADERS = {
    'Authorization': f'Bearer {TOKEN}',
    'Content-Type': 'application/json'
}


def query_db(sql, retries=5):
    """Execute SQL via Supabase Management API with retry."""
    for attempt in range(1, retries + 1):
        resp = requests.post(API_URL, headers=HEADERS, json={'query': sql})
        if resp.status_code in (200, 201):
            return resp.json()
        if resp.status_code == 429:
            wait = 5 * attempt
            print(f'  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        resp.raise_for_status()
    raise Exception(f'Failed after {retries} retries')


def export_governors_2026():
    """Export 2026 governor race data."""
    print('Exporting governors_2026.json...')

    # Query 1: All governor seats with current holders and forecast
    q_races = """
    SELECT
      s.abbreviation as state,
      s.state_name,
      s.gov_term_years,
      s.gov_term_limit,
      se.id as seat_id,
      se.current_holder,
      se.current_holder_party,
      se.next_regular_election_year,
      st.candidate_id,
      st.party as holder_party,
      st.start_date as holder_start,
      st.start_reason,
      c.full_name as holder_name
    FROM states s
    JOIN districts d ON d.state_id = s.id AND d.office_level = 'Statewide'
    JOIN seats se ON se.district_id = d.id AND se.office_type = 'Governor'
    LEFT JOIN seat_terms st ON st.seat_id = se.id AND st.end_date IS NULL
    LEFT JOIN candidates c ON c.id = st.candidate_id
    ORDER BY s.abbreviation
    """

    # Query 2: 2026 governor general elections with forecasts
    q_elections = """
    SELECT
      s.abbreviation as state,
      e.id as election_id,
      e.election_date,
      e.election_type,
      e.forecast_rating,
      e.filing_deadline,
      e.pres_margin_this_cycle
    FROM elections e
    JOIN seats se ON se.id = e.seat_id
    JOIN districts d ON d.id = se.district_id
    JOIN states s ON s.id = d.state_id
    WHERE se.office_type = 'Governor'
      AND e.election_type = 'General'
      AND e.election_date >= '2026-01-01'
      AND e.election_date <= '2026-12-31'
    ORDER BY s.abbreviation
    """

    # Query 3: Forecast details (Cook + Sabato)
    q_forecasts = """
    SELECT
      s.abbreviation as state,
      f.source,
      f.rating
    FROM forecasts f
    JOIN elections e ON e.id = f.election_id
    JOIN seats se ON se.id = e.seat_id
    JOIN districts d ON d.id = se.district_id
    JOIN states s ON s.id = d.state_id
    WHERE se.office_type = 'Governor'
      AND e.election_type = 'General'
      AND e.election_date >= '2026-01-01'
    ORDER BY s.abbreviation, f.source
    """

    # Query 4: 2026 governor candidates (from candidacies)
    q_candidates = """
    SELECT
      s.abbreviation as state,
      c.full_name,
      ca.party,
      ca.candidate_status,
      ca.result,
      ca.is_major,
      e.election_type
    FROM candidacies ca
    JOIN elections e ON e.id = ca.election_id
    JOIN candidates c ON c.id = ca.candidate_id
    JOIN seats se ON se.id = e.seat_id
    JOIN districts d ON d.id = se.district_id
    JOIN states s ON s.id = d.state_id
    WHERE se.office_type = 'Governor'
      AND e.election_date >= '2026-01-01'
      AND e.election_date <= '2026-12-31'
    ORDER BY s.abbreviation, e.election_type, ca.party, c.full_name
    """

    # Query 5: Primary dates for governor races
    q_primaries = """
    SELECT DISTINCT
      s.abbreviation as state,
      e.election_date as primary_date,
      e.election_type
    FROM elections e
    JOIN seats se ON se.id = e.seat_id
    JOIN districts d ON d.id = se.district_id
    JOIN states s ON s.id = d.state_id
    WHERE se.office_type = 'Governor'
      AND e.election_type IN ('Primary_D', 'Primary_R', 'Primary')
      AND e.election_date >= '2026-01-01'
      AND e.election_date <= '2026-12-31'
    ORDER BY s.abbreviation
    """

    races_raw = query_db(q_races)
    elections_raw = query_db(q_elections)
    forecasts_raw = query_db(q_forecasts)
    candidates_raw = query_db(q_candidates)
    primaries_raw = query_db(q_primaries)

    # Build lookup maps
    elections_by_state = {}
    for e in elections_raw:
        elections_by_state[e['state']] = e

    forecasts_by_state = {}
    for f in forecasts_raw:
        forecasts_by_state.setdefault(f['state'], {})[f['source']] = f['rating']

    candidates_by_state = {}
    for c in candidates_raw:
        candidates_by_state.setdefault(c['state'], []).append({
            'name': c['full_name'],
            'party': c['party'],
            'status': c['candidate_status'],
            'result': c['result'],
            'is_major': c['is_major'] or False,
            'election_type': c['election_type'],
        })

    primary_by_state = {}
    for p in primaries_raw:
        if p['state'] not in primary_by_state:
            primary_by_state[p['state']] = p['primary_date']

    # Known open seats (term-limited or retiring)
    OPEN_SEATS = {
        'AK', 'AL', 'CA', 'CO', 'FL', 'GA', 'HI', 'IA', 'KS', 'ME', 'MI', 'MN',
        'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'SC', 'SD', 'TN', 'WI', 'WY'
    }

    races = []
    no_race = []

    for row in races_raw:
        st = row['state']
        if row['next_regular_election_year'] != 2026:
            no_race.append(st)
            continue

        elec = elections_by_state.get(st, {})
        fcasts = forecasts_by_state.get(st, {})
        cands = candidates_by_state.get(st, [])

        # Compute term number from seat_term start_date
        term_years = row['gov_term_years'] or 4
        running_for_term = None
        if row['holder_start'] and st not in OPEN_SEATS:
            start_year = int(row['holder_start'][:4])
            current_term = (2026 - start_year) // term_years + 1
            running_for_term = current_term + 1

        race = {
            'state': st,
            'state_name': row['state_name'],
            'incumbent': row['holder_name'] or row['current_holder'],
            'incumbent_party': row['holder_party'] or row['current_holder_party'],
            'is_open_seat': st in OPEN_SEATS,
            'term_limit': row['gov_term_limit'],
            'term_years': row['gov_term_years'],
            'running_for_term': running_for_term,
            'forecast': elec.get('forecast_rating'),
            'forecast_cook': fcasts.get('Cook Political Report'),
            'forecast_sabato': fcasts.get("Sabato's Crystal Ball"),
            'election_date': elec.get('election_date'),
            'filing_deadline': elec.get('filing_deadline'),
            'primary_date': primary_by_state.get(st),
            'pres_margin_2024': elec.get('pres_margin_this_cycle'),
            'candidates': cands,
        }
        races.append(race)

    # Sort by competitiveness (toss-ups first)
    rating_order = {
        'Toss-up': 0, 'Tilt D': 1, 'Tilt R': 1,
        'Lean D': 2, 'Lean R': 2, 'Likely D': 3, 'Likely R': 3,
        'Very Likely D': 4, 'Very Likely R': 4, 'Solid D': 5, 'Solid R': 5,
        None: 6
    }
    races.sort(key=lambda r: (rating_order.get(r['forecast'], 6), r['state']))

    data = {
        'generated_at': datetime.now().isoformat(),
        'total_races': len(races),
        'open_seats': sum(1 for r in races if r['is_open_seat']),
        'races': races,
        'no_race_states': sorted(no_race),
    }

    outpath = os.path.join(SITE_DIR, 'data', 'governors_2026.json')
    with open(outpath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f'  Written: {outpath} ({len(races)} races)')


def export_governor_history():
    """Export historical governor seat_terms for analytics."""
    print('Exporting governor_history.json...')

    q_terms = """
    SELECT
      s.abbreviation as state,
      s.state_name,
      c.full_name as name,
      st.party,
      st.start_date,
      st.end_date,
      st.start_reason,
      st.end_reason
    FROM seat_terms st
    JOIN seats se ON se.id = st.seat_id
    JOIN districts d ON d.id = se.district_id
    JOIN states s ON s.id = d.state_id
    JOIN candidates c ON c.id = st.candidate_id
    WHERE se.office_type = 'Governor'
    ORDER BY s.abbreviation, st.start_date
    """

    rows = query_db(q_terms)

    terms = []
    for r in rows:
        terms.append({
            'state': r['state'],
            'state_name': r['state_name'],
            'name': r['name'],
            'party': r['party'],
            'start_date': r['start_date'],
            'end_date': r['end_date'],
            'start_reason': r['start_reason'],
            'end_reason': r['end_reason'],
        })

    # Pre-compute yearly partisan balance (Jan 1 each year)
    yearly = []
    for year in range(1960, 2027):
        ref_date = f'{year}-01-15'  # mid-January to catch inaugurations
        d_count = 0
        r_count = 0
        other = 0
        for t in terms:
            if t['start_date'] and t['start_date'] <= ref_date:
                if t['end_date'] is None or t['end_date'] >= ref_date:
                    if t['party'] == 'D':
                        d_count += 1
                    elif t['party'] == 'R':
                        r_count += 1
                    else:
                        other += 1
        yearly.append({'year': year, 'd': d_count, 'r': r_count, 'other': other})

    # Compute party flips (when governor party changes in a state)
    flips = []
    by_state = {}
    for t in terms:
        by_state.setdefault(t['state'], []).append(t)

    for st, state_terms in sorted(by_state.items()):
        prev_party = None
        for t in state_terms:
            if prev_party and t['party'] and t['party'] != prev_party:
                flip_year = int(t['start_date'][:4]) if t['start_date'] else None
                if flip_year and flip_year >= 1960:
                    flips.append({
                        'state': st,
                        'year': flip_year,
                        'from_party': prev_party,
                        'to_party': t['party'],
                        'new_governor': t['name'],
                    })
            if t['party']:
                prev_party = t['party']

    data = {
        'generated_at': datetime.now().isoformat(),
        'total_terms': len(terms),
        'terms': terms,
        'yearly_balance': yearly,
        'flips': flips,
    }

    outpath = os.path.join(SITE_DIR, 'data', 'governor_history.json')
    with open(outpath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f'  Written: {outpath} ({len(terms)} terms, {len(flips)} flips)')


if __name__ == '__main__':
    export_governors_2026()
    export_governor_history()
    print('Done!')
