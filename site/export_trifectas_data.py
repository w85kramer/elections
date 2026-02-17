#!/usr/bin/env python3
"""Export trifecta data for site pages:
  - data/trifectas_data.json — timeline, current status, at-risk analysis
"""

import json
import os
import time
from datetime import datetime

import requests
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

SITE_DIR = '/home/billkramer/elections/site'

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

def export_trifectas():
    """Export all trifecta data to a single JSON."""
    print('Exporting trifectas_data.json...')

    # Query 1: Full trifecta history
    q_trifectas = """
    SELECT
      s.abbreviation as state,
      s.state_name,
      t.year,
      t.governor_party,
      t.legislature_status,
      t.trifecta_status,
      t.notes
    FROM trifectas t
    JOIN states s ON s.id = t.state_id
    ORDER BY t.year, s.abbreviation
    """

    # Query 2: Governor races in 2026
    q_gov_2026 = """
    SELECT
      s.abbreviation as state,
      se.current_holder,
      se.current_holder_party,
      e.forecast_rating
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

    # Query 3: Current chamber control (2025)
    q_chambers = """
    SELECT
      s.abbreviation as state,
      cc.chamber,
      cc.control_status,
      cc.forecast_rating,
      cc.d_seats,
      cc.r_seats,
      cc.total_seats
    FROM chamber_control cc
    JOIN states s ON s.id = cc.state_id
    WHERE cc.effective_date = '2025-01-01'
    ORDER BY s.abbreviation, cc.chamber
    """

    # Query 4: Current governors
    q_governors = """
    SELECT
      s.abbreviation as state,
      c.full_name as governor_name,
      st.party as governor_party
    FROM seat_terms st
    JOIN seats se ON se.id = st.seat_id
    JOIN districts d ON d.id = se.district_id
    JOIN states s ON s.id = d.state_id
    JOIN candidates c ON c.id = st.candidate_id
    WHERE se.office_type = 'Governor'
      AND st.end_date IS NULL
    ORDER BY s.abbreviation
    """

    print('  Querying trifecta history...')
    trifectas_raw = query_db(q_trifectas)
    print(f'  Got {len(trifectas_raw)} trifecta rows')

    print('  Querying 2026 governor races...')
    gov_2026_raw = query_db(q_gov_2026)
    print(f'  Got {len(gov_2026_raw)} governor races')

    print('  Querying current chambers...')
    chambers_raw = query_db(q_chambers)
    print(f'  Got {len(chambers_raw)} chamber rows')

    print('  Querying current governors...')
    governors_raw = query_db(q_governors)
    print(f'  Got {len(governors_raw)} governors')

    # Build lookups
    gov_2026 = {}
    for r in gov_2026_raw:
        gov_2026[r['state']] = {
            'incumbent': r['current_holder'],
            'incumbent_party': r['current_holder_party'],
            'forecast': r['forecast_rating'],
        }

    # Chambers by state
    chambers_by_state = {}
    for r in chambers_raw:
        chambers_by_state.setdefault(r['state'], []).append(r)

    # Current governors
    gov_current = {}
    for r in governors_raw:
        gov_current[r['state']] = {
            'name': r['governor_name'],
            'party': r['governor_party'],
        }

    # ── Current year trifecta status ──
    # Get 2026 trifecta data
    current_trifectas = {}
    for r in trifectas_raw:
        if r['year'] == 2026:
            current_trifectas[r['state']] = r

    # ── State-level detail ──
    states = []
    d_trifectas = 0
    r_trifectas = 0
    split_count = 0
    competitive_count = 0

    # Get all states from current trifectas
    state_list = sorted(set(r['state'] for r in trifectas_raw if r['year'] == 2026))

    for state in state_list:
        t = current_trifectas.get(state, {})
        trifecta = t.get('trifecta_status', 'Split')
        gov_party = t.get('governor_party', '')
        leg_status = t.get('legislature_status', '')

        if trifecta == 'Democrat':
            d_trifectas += 1
        elif trifecta == 'Republican':
            r_trifectas += 1
        else:
            split_count += 1

        # Governor info — use seat_terms data for party (trifectas table may have nulls)
        gov = gov_current.get(state, {})
        if not gov_party and gov.get('party'):
            gov_party = gov['party']
        gov_up_2026 = state in gov_2026
        gov_forecast = gov_2026.get(state, {}).get('forecast') if gov_up_2026 else None

        # Chamber info
        chs = chambers_by_state.get(state, [])
        senate_ctrl = None
        house_ctrl = None
        senate_up = False
        house_up = False
        senate_forecast = None
        house_forecast = None

        for ch in chs:
            if ch['chamber'] == 'Senate':
                senate_ctrl = ch['control_status']
                senate_forecast = ch['forecast_rating']
            elif ch['chamber'] == 'Legislature':
                # NE unicameral
                senate_ctrl = ch['control_status']
                house_ctrl = ch['control_status']
                senate_forecast = ch['forecast_rating']
                house_forecast = ch['forecast_rating']
            else:
                house_ctrl = ch['control_status']
                house_forecast = ch['forecast_rating']

        # Check if any chambers have elections in 2026
        # (Simplify: most chambers have elections in 2026 unless they're staggered)
        # Use election data from legislatures export
        try:
            with open(os.path.join(SITE_DIR, 'data', 'legislatures_data.json')) as f:
                leg_data = json.load(f)
            for c in leg_data['chambers']:
                if c['state'] == state:
                    if c['chamber'] == 'Senate' or c['chamber'] == 'Legislature':
                        senate_up = c['seats_up_2026'] > 0
                    else:
                        house_up = c['seats_up_2026'] > 0
        except (FileNotFoundError, KeyError):
            pass

        # ── Competitive trifecta analysis ──
        # Determine if this state's trifecta status could change in 2026.
        # For each party, check if there's a realistic path to gaining (or losing) a trifecta.
        # A race "could flip to D" if forecast favors D or is a toss-up.
        # A race "could flip to R" if forecast favors R or is a toss-up.
        def could_win(forecast, party):
            """Could `party` win this race given the forecast?
            Uses Sabato's Crystal Ball scale: Solid > Likely > Lean > Toss-Up.
            Solid and Likely for the opposing party are considered safe (not flippable).
            Lean and Toss-Up are competitive."""
            if not forecast or 'Coalition' in forecast:
                return False
            safe_d = {'Very Likely D', 'Likely D', 'Solid D'}
            safe_r = {'Very Likely R', 'Likely R', 'Solid R'}
            if party == 'D':
                return forecast not in safe_r
            else:
                return forecast not in safe_d

        def party_letter(party):
            return 'D' if party == 'D' else 'R'

        # Determine current governor's party from trifecta context
        # If trifecta is D/R, gov matches. If split, infer from what's NOT matching.
        gov_is_d = (trifecta == 'Democrat') or (trifecta == 'Split' and senate_ctrl == 'R' and house_ctrl == 'R') or \
                   (trifecta == 'Split' and senate_ctrl in ('R', 'Coalition') and house_ctrl in ('R', 'Coalition', 'Power_Sharing'))
        gov_is_r = (trifecta == 'Republican') or (trifecta == 'Split' and senate_ctrl == 'D' and house_ctrl == 'D') or \
                   (trifecta == 'Split' and senate_ctrl in ('D', 'Coalition') and house_ctrl in ('D', 'Coalition', 'Power_Sharing'))
        # Fallback: use gov name lookup from our governor data
        if not gov_is_d and not gov_is_r:
            gp = gov.get('party', '')
            gov_is_d = gp == 'Democrat'
            gov_is_r = gp == 'Republican'

        competitive = False
        competitive_type = None  # 'at_risk_d', 'at_risk_r', 'pickup_d', 'pickup_r', 'pickup_both'
        competitive_scenario = None

        if trifecta == 'Democrat':
            # Check if D trifecta is at risk: could R flip any one piece?
            risk_pieces = []
            if gov_up_2026 and could_win(gov_forecast, 'R'):
                risk_pieces.append('governor')
            if senate_up and could_win(senate_forecast, 'R'):
                risk_pieces.append('senate')
            if house_up and could_win(house_forecast, 'R'):
                risk_pieces.append('house')
            if risk_pieces:
                competitive = True
                competitive_type = 'at_risk_d'
                competitive_scenario = 'D trifecta at risk: competitive ' + ' & '.join(risk_pieces)

        elif trifecta == 'Republican':
            # Check if R trifecta is at risk: could D flip any one piece?
            risk_pieces = []
            if gov_up_2026 and could_win(gov_forecast, 'D'):
                risk_pieces.append('governor')
            if senate_up and could_win(senate_forecast, 'D'):
                risk_pieces.append('senate')
            if house_up and could_win(house_forecast, 'D'):
                risk_pieces.append('house')
            if risk_pieces:
                competitive = True
                competitive_type = 'at_risk_r'
                competitive_scenario = 'R trifecta at risk: competitive ' + ' & '.join(risk_pieces)

        else:
            # Split/divided: check pickup paths for each party
            # D trifecta path: D must control gov + senate + house
            d_path = True
            d_needs = []
            # Governor
            if not gov_is_d:
                if gov_up_2026 and could_win(gov_forecast, 'D'):
                    d_needs.append('governor')
                else:
                    d_path = False
            else:
                # D holds gov but could lose it
                if gov_up_2026 and not could_win(gov_forecast, 'D'):
                    d_path = False  # D can't even hold gov
            # Senate
            if senate_ctrl not in ('D',):
                if senate_up and could_win(senate_forecast, 'D'):
                    d_needs.append('senate')
                elif not senate_up:
                    d_path = False  # not up, can't flip
                else:
                    d_path = False
            # House
            if house_ctrl not in ('D',):
                if house_up and could_win(house_forecast, 'D'):
                    d_needs.append('house')
                elif not house_up:
                    d_path = False
                else:
                    d_path = False
            if not d_needs:
                d_path = False  # D already has everything (shouldn't be split then)

            # R trifecta path: R must control gov + senate + house
            r_path = True
            r_needs = []
            if not gov_is_r:
                if gov_up_2026 and could_win(gov_forecast, 'R'):
                    r_needs.append('governor')
                else:
                    r_path = False
            else:
                if gov_up_2026 and not could_win(gov_forecast, 'R'):
                    r_path = False
            if senate_ctrl not in ('R',):
                if senate_up and could_win(senate_forecast, 'R'):
                    r_needs.append('senate')
                elif not senate_up:
                    r_path = False
                else:
                    r_path = False
            if house_ctrl not in ('R',):
                if house_up and could_win(house_forecast, 'R'):
                    r_needs.append('house')
                elif not house_up:
                    r_path = False
                else:
                    r_path = False
            if not r_needs:
                r_path = False

            if d_path and r_path:
                competitive = True
                competitive_type = 'pickup_both'
                competitive_scenario = 'D needs ' + ' + '.join(d_needs) + '; R needs ' + ' + '.join(r_needs)
            elif d_path:
                competitive = True
                competitive_type = 'pickup_d'
                competitive_scenario = 'D pickup: needs ' + ' + '.join(d_needs)
            elif r_path:
                competitive = True
                competitive_type = 'pickup_r'
                competitive_scenario = 'R pickup: needs ' + ' + '.join(r_needs)

        if competitive:
            competitive_count += 1

        # Compute streak: how long current trifecta type has persisted
        streak_since = None
        state_history = [r for r in trifectas_raw if r['state'] == state]
        state_history.sort(key=lambda x: x['year'], reverse=True)
        for i, h in enumerate(state_history):
            if h['trifecta_status'] != trifecta:
                if i > 0:
                    streak_since = state_history[i - 1]['year']
                break
        else:
            if state_history:
                streak_since = state_history[-1]['year']

        state_name = t.get('state_name', state)

        states.append({
            'state': state,
            'state_name': state_name,
            'trifecta_status': trifecta,
            'governor_party': gov_party,
            'governor_name': gov.get('name'),
            'legislature_status': leg_status,
            'governor_up_2026': gov_up_2026,
            'gov_forecast': gov_forecast,
            'senate_control': senate_ctrl,
            'house_control': house_ctrl,
            'senate_up_2026': senate_up,
            'house_up_2026': house_up,
            'senate_forecast': senate_forecast,
            'house_forecast': house_forecast,
            'competitive': competitive,
            'competitive_type': competitive_type,
            'competitive_scenario': competitive_scenario,
            'streak_since': streak_since,
        })

    # ── Timeline (aggregates per year) ──
    timeline = []
    years = sorted(set(r['year'] for r in trifectas_raw))
    for year in years:
        year_rows = [r for r in trifectas_raw if r['year'] == year]
        d = sum(1 for r in year_rows if r['trifecta_status'] == 'Democrat')
        r_count = sum(1 for r in year_rows if r['trifecta_status'] == 'Republican')
        s = sum(1 for r in year_rows if r['trifecta_status'] == 'Split')
        timeline.append({
            'year': year,
            'd': d,
            'r': r_count,
            'split': s,
        })

    # ── Flips (year-over-year changes) ──
    flips = []
    # Build state-year lookup
    state_year = {}
    for r in trifectas_raw:
        state_year[(r['state'], r['year'])] = r['trifecta_status']

    all_states_in_data = sorted(set(r['state'] for r in trifectas_raw))
    for year_idx in range(1, len(years)):
        prev_year = years[year_idx - 1]
        curr_year = years[year_idx]
        for st in all_states_in_data:
            prev = state_year.get((st, prev_year))
            curr = state_year.get((st, curr_year))
            if prev and curr and prev != curr:
                # Find state name
                st_name = st
                for r in trifectas_raw:
                    if r['state'] == st:
                        st_name = r['state_name']
                        break
                flips.append({
                    'state': st,
                    'state_name': st_name,
                    'year': curr_year,
                    'from': prev,
                    'to': curr,
                })

    data = {
        'generated_at': datetime.now().isoformat(),
        'summary': {
            'current_year': 2026,
            'd_trifectas': d_trifectas,
            'r_trifectas': r_trifectas,
            'split': split_count,
            'competitive': competitive_count,
        },
        'states': states,
        'timeline': timeline,
        'flips': flips,
    }

    outpath = os.path.join(SITE_DIR, 'data', 'trifectas_data.json')
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with open(outpath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f'  Written: {outpath} ({len(states)} states, {len(timeline)} timeline years, {len(flips)} flips)')

if __name__ == '__main__':
    export_trifectas()
    print('Done!')
