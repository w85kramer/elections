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

# Minimum election year to include in site export (per-state overrides).
# Elections before this year are excluded from the JSON but kept in the DB.
# Used to exclude unverified historical data from the live site.
MIN_EXPORT_YEAR = {
    'NE': 2014,  # 2010/2012 loser caucus data unverified (no Wikipedia source)
}

# ── Recount threshold rules by state ──────────────────────────────────
# States with official margin-based recount thresholds (automatic or
# mandatory-to-grant when requested). Researched from NCSL, Ballotpedia,
# and state statutes. Format: { state: { 'statewide': pct, 'legislative': pct } }
#
# States NOT listed here use the CLOSE_RACE_PCT fallback (1%).
# These include: states with no recount provision (IL, MS, TN), states
# where recounts are purely candidate-requested with no margin limit
# (AR, CA, IN, LA, NV, NH, NJ, OK, WV), and states with tie-only
# automatic recounts (AK, ME, MT, SD, TX, UT, VT).
RECOUNT_THRESHOLDS = {
    # Automatic recount states
    'AL': {'statewide': 0.5, 'legislative': 0.5},
    'AZ': {'statewide': 0.5, 'legislative': 0.5},   # Changed from 0.1% to 0.5% in 2022
    'CO': {'statewide': 0.5, 'legislative': 0.5},
    'CT': {'statewide': 0.5, 'legislative': 0.5},
    'DE': {'statewide': 0.5, 'legislative': 0.5},
    'FL': {'statewide': 0.5, 'legislative': 0.5},    # 0.5% machine, 0.25% manual
    'HI': {'statewide': 0.25, 'legislative': 0.25},
    'KY': {'statewide': 0.5, 'legislative': 0.5},
    'MI': {'statewide': 0.1, 'legislative': 0.1},    # Fixed: 0.1% statewide; legislative uses fixed vote counts but ~0.1% equivalent
    'NE': {'statewide': 1.0, 'legislative': 1.0},    # 1% for >500 votes; 2% for <=500
    'NM': {'statewide': 0.25, 'legislative': 1.0},
    'NY': {'statewide': 0.5, 'legislative': 0.5},
    'ND': {'statewide': 0.5, 'legislative': 0.5},
    'OH': {'statewide': 0.25, 'legislative': 0.5},
    'OR': {'statewide': 0.2, 'legislative': 0.2},
    'SC': {'statewide': 1.0, 'legislative': 1.0},
    'WA': {'statewide': 0.5, 'legislative': 0.5},
    'WY': {'statewide': 1.0, 'legislative': 1.0},
    # Candidate-requested but mandatory to grant at threshold
    'GA': {'statewide': 0.5, 'legislative': 0.5},    # Fixed: 0.5% all races (requested, not automatic)
    'ID': {'statewide': 0.5, 'legislative': 0.5},
    'IA': {'statewide': 1.0, 'legislative': 1.0},
    'KS': {'statewide': 0.5, 'legislative': 0.5},    # State pays if <=0.5%
    'MA': {'statewide': 0.5, 'legislative': 0.5},
    'MD': {'statewide': 0.1, 'legislative': 0.1},    # Any candidate can request; state pays if <=0.1%
    'MN': {'statewide': 0.25, 'legislative': 0.5},
    'MO': {'statewide': 0.5, 'legislative': 1.0},
    'NC': {'statewide': 0.5, 'legislative': 1.0},
    'PA': {'statewide': 0.5, 'legislative': 0.5},
    'VA': {'statewide': 1.0, 'legislative': 1.0},
    'WI': {'statewide': 0.25, 'legislative': 0.25},  # Fixed: state pays if <=0.25%
}

# Close-race threshold for states without a specific margin-based rule
# (e.g. AR where any candidate can request a recount regardless of margin).
# Flags races under 1% as "close" — not a legal threshold, just notable.
CLOSE_RACE_PCT = 1.0


def _check_recount_eligible(state_abbr, candidates, total_votes, election_type, result_status):
    """
    Check if an election result is within recount threshold or triggered a runoff.

    Three badge types:
    - 'runoff_triggered': top candidates both advancing to runoff (margin irrelevant)
    - 'recount': margin within official state threshold (advancing vs eliminated)
    - 'close_race': margin within 1% fallback for states without thresholds

    Only flags races with 'Called' or 'Unofficial' result_status (not Certified).
    Returns a dict with badge info if eligible, None otherwise.
    """
    if not total_votes or total_votes == 0:
        return None
    if result_status not in ('Called', 'Unofficial'):
        return None

    # Sort by votes descending
    sorted_cands = sorted(
        [c for c in candidates if c.get('votes') and c['votes'] > 0],
        key=lambda c: c['votes'], reverse=True)
    if len(sorted_cands) < 2:
        return None

    # Check for runoff scenario: if top candidates both have result='Runoff',
    # the margin between them doesn't affect who advances.
    runoff_cands = [c for c in sorted_cands if c.get('result') == 'Runoff']
    if len(runoff_cands) >= 2:
        # Runoff triggered — show badge with margin between the runoff candidates
        r_margin = runoff_cands[0]['votes'] - runoff_cands[-1]['votes']
        r_margin_pct = (r_margin / total_votes) * 100
        # Also check if there's a close margin between last advancing and first
        # eliminated candidate (that's where a recount would actually matter)
        eliminated = [c for c in sorted_cands if c.get('result') not in ('Runoff', 'Won', 'Advanced')]
        if eliminated:
            cutoff_margin = runoff_cands[-1]['votes'] - eliminated[0]['votes']
            cutoff_pct = (cutoff_margin / total_votes) * 100
        else:
            cutoff_margin = None
            cutoff_pct = None
        return {
            'type': 'runoff_triggered',
            'margin': r_margin,
            'margin_pct': round(r_margin_pct, 2),
            'cutoff_margin': cutoff_margin,
            'cutoff_margin_pct': round(cutoff_pct, 2) if cutoff_pct is not None else None,
        }

    # Standard recount/close-race check using top-2 margin
    margin = sorted_cands[0]['votes'] - sorted_cands[1]['votes']
    margin_pct = (margin / total_votes) * 100

    thresholds = RECOUNT_THRESHOLDS.get(state_abbr)

    if thresholds:
        # Official threshold state
        is_statewide = election_type in ('General',) and 'Statewide' in str(candidates)
        key = 'statewide' if is_statewide else 'legislative'
        threshold = thresholds.get(key)
        if threshold is not None and margin_pct <= threshold:
            return {
                'type': 'recount',
                'margin': margin,
                'margin_pct': round(margin_pct, 2),
                'threshold_pct': threshold,
            }
    else:
        # No official threshold — use close-race fallback
        if margin_pct <= CLOSE_RACE_PCT:
            return {
                'type': 'close_race',
                'margin': margin,
                'margin_pct': round(margin_pct, 2),
                'threshold_pct': CLOSE_RACE_PCT,
            }

    return None


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
            d.redistricting_cycle,
            d.is_floterial,
            s.id as seat_id,
            s.seat_label,
            s.seat_designator,
            s.current_holder,
            s.current_holder_party,
            s.current_holder_caucus as raw_caucus,
            CASE WHEN s.current_holder_caucus = 'C' THEN s.current_holder_party
                 ELSE COALESCE(s.current_holder_caucus, s.current_holder_party)
            END as current_holder_caucus,
            s.term_length_years,
            s.next_regular_election_year,
            s.election_class
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_level = 'Legislative'
          AND COALESCE(d.redistricting_cycle, '2022') = '2022'
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
            e.forecast_rating,
            e.precincts_reporting,
            e.precincts_total
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_level = 'Legislative'
          AND COALESCE(d.redistricting_cycle, '2022') = '2022'
          {state_filter}
        ORDER BY st.abbreviation, e.seat_id, e.election_year DESC, e.election_type
    """

    # Query 3: All candidacies for legislative elections
    q_candidacies = f"""
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
          AND COALESCE(d.redistricting_cycle, '2022') = '2022'
          {state_filter}
        ORDER BY st.abbreviation, cy.election_id,
            CASE cy.result WHEN 'Won' THEN 0 WHEN 'Advanced' THEN 1 ELSE 2 END,
            cy.votes_received DESC NULLS LAST
    """

    # Query 4: Seat terms (officeholder history) — ALL terms for timeline events + "Since YYYY"
    q_terms = f"""
        SELECT
            st.abbreviation as state,
            stm.seat_id,
            c.full_name as holder_name,
            stm.party as holder_party,
            stm.caucus as holder_caucus,
            stm.start_date,
            stm.end_date,
            stm.start_reason,
            stm.end_reason,
            stm.notes
        FROM seat_terms stm
        JOIN seats s ON stm.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON stm.candidate_id = c.id
        WHERE s.office_level = 'Legislative'
          AND COALESCE(d.redistricting_cycle, '2022') = '2022'
          {state_filter}
        ORDER BY st.abbreviation, stm.seat_id, stm.start_date
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

    # Query 7: Party switches for legislative seats
    q_switches = f"""
        SELECT
            st.abbreviation as state,
            ps.seat_id,
            c.full_name as name,
            ps.old_party,
            ps.new_party,
            ps.old_caucus,
            ps.new_caucus,
            ps.switch_year,
            ps.switch_date,
            ps.bp_profile_url
        FROM party_switches ps
        JOIN seats s ON ps.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON ps.candidate_id = c.id
        WHERE s.office_level = 'Legislative'
          AND COALESCE(d.redistricting_cycle, '2022') = '2022'
          {state_filter}
        ORDER BY st.abbreviation, ps.seat_id, ps.switch_year
    """

    # --- Old-era queries (Q8-Q11): elections from non-current redistricting cycles ---

    # Query 8: Old-era districts + seats (for matching and eliminated district generation)
    q_old_districts = f"""
        SELECT
            st.abbreviation as state,
            d.id as district_id,
            d.chamber,
            d.district_number,
            d.district_name,
            d.num_seats,
            d.redistricting_cycle,
            s.id as seat_id,
            s.seat_designator
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_level = 'Legislative'
          AND d.redistricting_cycle IS NOT NULL
          AND d.redistricting_cycle != '2022'
          AND d.redistricting_cycle != 'permanent'
          {state_filter}
        ORDER BY st.abbreviation, d.chamber, d.district_number, s.seat_designator
    """

    # Query 9: Old-era elections
    q_old_elections = f"""
        SELECT
            st.abbreviation as state,
            d.chamber as old_chamber,
            d.district_number as old_district_number,
            d.num_seats as old_num_seats,
            d.redistricting_cycle as old_cycle,
            e.id as election_id,
            e.seat_id,
            e.election_date,
            e.election_year,
            e.election_type,
            e.total_votes_cast,
            e.is_open_seat,
            e.result_status,
            e.filing_deadline,
            e.forecast_rating,
            e.precincts_reporting,
            e.precincts_total
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_level = 'Legislative'
          AND d.redistricting_cycle IS NOT NULL
          AND d.redistricting_cycle != '2022'
          AND d.redistricting_cycle != 'permanent'
          {state_filter}
        ORDER BY st.abbreviation, e.seat_id, e.election_year DESC, e.election_type
    """

    # Query 10: Old-era candidacies
    q_old_candidacies = f"""
        SELECT
            st.abbreviation as state,
            d.chamber as old_chamber,
            d.district_number as old_district_number,
            cy.election_id,
            cy.candidate_id,
            c.full_name as name,
            cy.party,
            cy.caucus,
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
          AND d.redistricting_cycle IS NOT NULL
          AND d.redistricting_cycle != '2022'
          AND d.redistricting_cycle != 'permanent'
          {state_filter}
        ORDER BY st.abbreviation, cy.election_id,
            CASE cy.result WHEN 'Won' THEN 0 WHEN 'Advanced' THEN 1 ELSE 2 END,
            cy.votes_received DESC NULLS LAST
    """

    # Query 11: Old-era seat terms
    q_old_terms = f"""
        SELECT
            st.abbreviation as state,
            d.chamber as old_chamber,
            d.district_number as old_district_number,
            stm.seat_id,
            c.full_name as holder_name,
            stm.party as holder_party,
            stm.caucus as holder_caucus,
            stm.start_date,
            stm.end_date,
            stm.start_reason,
            stm.end_reason,
            stm.notes
        FROM seat_terms stm
        JOIN seats s ON stm.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON stm.candidate_id = c.id
        WHERE s.office_level = 'Legislative'
          AND d.redistricting_cycle IS NOT NULL
          AND d.redistricting_cycle != '2022'
          AND d.redistricting_cycle != 'permanent'
          {state_filter}
        ORDER BY st.abbreviation, stm.seat_id, stm.start_date
    """

    # Query 12: Old-era party switches
    q_old_switches = f"""
        SELECT
            st.abbreviation as state,
            d.chamber as old_chamber,
            d.district_number as old_district_number,
            ps.seat_id,
            c.full_name as name,
            ps.old_party,
            ps.new_party,
            ps.old_caucus,
            ps.new_caucus,
            ps.switch_year,
            ps.switch_date,
            ps.bp_profile_url
        FROM party_switches ps
        JOIN seats s ON ps.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON ps.candidate_id = c.id
        WHERE s.office_level = 'Legislative'
          AND d.redistricting_cycle IS NOT NULL
          AND d.redistricting_cycle != '2022'
          AND d.redistricting_cycle != 'permanent'
          {state_filter}
        ORDER BY st.abbreviation, ps.seat_id, ps.switch_year
    """

    # Query 13: State redistricting cycles
    q_redistricting = f"""
        SELECT
            st.abbreviation as state,
            sr.chamber,
            sr.effective_year
        FROM state_redistricting sr
        JOIN states st ON sr.state_id = st.id
        WHERE 1=1
          {state_filter}
        ORDER BY st.abbreviation, sr.chamber, sr.effective_year
    """

    if dry_run:
        print('  Would run 13 queries and write district JSON files')
        print(f'\n  Sample query (districts):\n{q_districts[:300]}...')
        return

    print('  Running 13 bulk queries...')
    districts_data = run_sql(q_districts)
    print(f'    1/13 districts+seats: {len(districts_data)} rows')
    elections_data = run_sql(q_elections)
    print(f'    2/13 elections: {len(elections_data)} rows')
    candidacies_data = run_sql(q_candidacies)
    print(f'    3/13 candidacies: {len(candidacies_data)} rows')
    terms_data = run_sql(q_terms)
    print(f'    4/13 seat_terms: {len(terms_data)} rows')
    states_data = run_sql(q_states)
    print(f'    5/13 states: {len(states_data)} rows')
    forecasts_data = run_sql(q_forecasts)
    print(f'    6/13 forecasts: {len(forecasts_data)} rows')
    switches_data = run_sql(q_switches)
    print(f'    7/13 party_switches: {len(switches_data)} rows')
    old_districts_data = run_sql(q_old_districts)
    print(f'    8/13 old-era districts: {len(old_districts_data)} rows')
    old_elections_data = run_sql(q_old_elections)
    print(f'    9/13 old-era elections: {len(old_elections_data)} rows')
    old_candidacies_data = run_sql(q_old_candidacies)
    print(f'    10/13 old-era candidacies: {len(old_candidacies_data)} rows')
    old_terms_data = run_sql(q_old_terms)
    print(f'    11/13 old-era seat_terms: {len(old_terms_data)} rows')
    old_switches_data = run_sql(q_old_switches)
    print(f'    12/13 old-era party_switches: {len(old_switches_data)} rows')
    redistricting_data = run_sql(q_redistricting)
    print(f'    13/13 redistricting cycles: {len(redistricting_data)} rows')

    # --- Index data ---

    # Redistricting cycles indexed by state → {chamber: [years]}
    redistricting_by_state = {}
    for r in redistricting_data:
        state = r['state']
        redistricting_by_state.setdefault(state, {})
        redistricting_by_state[state].setdefault(r['chamber'], []).append(r['effective_year'])

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

    # All terms indexed by seat_id (list), plus current holder (end_date IS NULL)
    all_terms_by_seat = {}
    current_term_by_seat = {}
    for r in terms_data:
        all_terms_by_seat.setdefault(r['seat_id'], []).append(r)
        if r['end_date'] is None:
            current_term_by_seat[r['seat_id']] = r

    # Forecasts indexed by seat_id
    forecasts_by_seat = {}
    for r in forecasts_data:
        forecasts_by_seat.setdefault(r['seat_id'], []).append({
            'source': r['source'],
            'rating': r['rating'],
        })

    # Party switches indexed by seat_id
    switches_by_seat = {}
    for r in switches_data:
        switches_by_seat.setdefault(r['seat_id'], []).append(r)

    # --- Index old-era data ---

    # Old-era elections indexed by seat_id
    old_elections_by_seat = {}
    for r in old_elections_data:
        old_elections_by_seat.setdefault(r['seat_id'], []).append(r)

    # Old-era candidacies indexed by election_id
    old_candidacies_by_election = {}
    for r in old_candidacies_data:
        old_candidacies_by_election.setdefault(r['election_id'], []).append(r)

    # Old-era terms indexed by seat_id
    old_terms_by_seat = {}
    for r in old_terms_data:
        old_terms_by_seat.setdefault(r['seat_id'], []).append(r)

    # Old-era party switches indexed by seat_id
    old_switches_by_seat = {}
    for r in old_switches_data:
        old_switches_by_seat.setdefault(r['seat_id'], []).append(r)

    # Build old-era district info: (state, chamber, district_number) -> {num_seats, cycle, seat_ids}
    old_district_info = {}  # (state, chamber, district_number) -> dict
    for r in old_districts_data:
        key = (r['state'], r['chamber'], r['district_number'])
        if key not in old_district_info:
            old_district_info[key] = {
                'district_id': r['district_id'],
                'district_name': r['district_name'],
                'num_seats': r['num_seats'],
                'redistricting_cycle': r['redistricting_cycle'],
                'seat_ids': [],
            }
        old_district_info[key]['seat_ids'].append(r['seat_id'])

    # Track which old districts are matched to current districts (by name)
    # Will be populated below when building current districts
    matched_old_districts = set()

    # --- Group districts by state, then by district_id ---
    # Multiple seats can share the same district (multi-member)
    districts_by_state = {}
    for r in districts_data:
        state = r['state']
        did = r['district_id']
        if state not in districts_by_state:
            districts_by_state[state] = {}
        if did not in districts_by_state[state]:
            # Compute redistricting_year from redistricting_cycle
            rc = r.get('redistricting_cycle')
            redistricting_year = int(rc) if rc and rc != 'permanent' else None

            districts_by_state[state][did] = {
                'district_number': r['district_number'],
                'district_name': r['district_name'],
                'chamber': r['chamber'],
                'num_seats': r['num_seats'],
                'pres_2024_margin': r['pres_2024_margin'],
                'pres_2024_winner': r['pres_2024_winner'],
                'redistricting_year': redistricting_year,
                'is_floterial': r['is_floterial'],
                'seats': [],
            }
        # Build seat object
        seat_id = r['seat_id']
        term = current_term_by_seat.get(seat_id)
        since_year = None
        if term and term.get('start_date'):
            since_year = int(str(term['start_date'])[:4])

        # Build term_events: resignations, deaths, removals, appointments for timeline display
        interesting_end_reasons = {'resigned', 'died', 'removed', 'appointed_elsewhere'}
        interesting_start_reasons = {'appointed', 'succeeded'}
        term_events = []
        for t in all_terms_by_seat.get(seat_id, []):
            if t.get('end_reason') in interesting_end_reasons and t.get('end_date'):
                end_date_str = str(t['end_date'])
                term_events.append({
                    'name': t['holder_name'],
                    'party': t['holder_party'],
                    'year': int(end_date_str[:4]),
                    'date': end_date_str,
                    'reason': t['end_reason'],
                    'notes': t.get('notes'),
                })
            if t.get('start_reason') in interesting_start_reasons and t.get('start_date'):
                start_date_str = str(t['start_date'])
                term_events.append({
                    'name': t['holder_name'],
                    'party': t['holder_party'],
                    'year': int(start_date_str[:4]),
                    'date': start_date_str,
                    'reason': t['start_reason'],
                    'notes': t.get('notes'),
                })

        # Build election history for this seat
        min_year = MIN_EXPORT_YEAR.get(r['state'], 0)
        seat_elections = []
        for e in elections_by_seat.get(seat_id, []):
            if e['election_year'] < min_year:
                continue
            cands = candidacies_by_election.get(e['election_id'], [])
            # Build candidate list
            candidate_list = []
            for c in cands:
                cand_obj = {
                    'id': c['candidate_id'],
                    'name': c['name'],
                    'party': c['party'],
                    'votes': c['votes'],
                    'pct': float(c['pct']) if c['pct'] is not None else None,
                    'result': c['result'],
                    'is_incumbent': c['is_incumbent'],
                    'is_write_in': c['is_write_in'],
                }
                if c.get('caucus'):
                    cand_obj['caucus'] = c['caucus']
                candidate_list.append(cand_obj)

            elec_obj = {
                'year': e['election_year'],
                'type': e['election_type'],
                'date': str(e['election_date']) if e.get('election_date') else None,
                'total_votes': e['total_votes_cast'],
                'is_open_seat': e['is_open_seat'],
                'result_status': e['result_status'],
                'filing_deadline': str(e['filing_deadline']) if e.get('filing_deadline') else None,
                'forecast_rating': e['forecast_rating'],
                'candidates': candidate_list,
            }
            if e.get('precincts_reporting') is not None:
                elec_obj['precincts_reporting'] = e['precincts_reporting']
                elec_obj['precincts_total'] = e['precincts_total']

            # Check for potential recount eligibility
            recount_flag = _check_recount_eligible(
                state, candidate_list, e['total_votes_cast'],
                e['election_type'], e.get('result_status'))
            if recount_flag:
                elec_obj['recount_eligible'] = recount_flag

            # Check for incumbent defeated in primary
            if 'Primary' in e['election_type']:
                inc_lost = [c for c in candidate_list
                            if c.get('is_incumbent') and c['result'] == 'Lost']
                if inc_lost:
                    elec_obj['incumbent_defeated'] = True

            # Check for party flip — only election types that determine who
            # holds office (not primaries, which are intra-party)
            FLIP_ELIGIBLE_TYPES = {
                'General', 'General_Runoff',
                'Special', 'Special_General',
                'Special_Runoff',  # nonpartisan/general special runoff
                'Recall',
                # NOT Special_Runoff_D/R — those are partisan primary runoffs
            }
            is_flip_eligible = e['election_type'] in FLIP_ELIGIBLE_TYPES
            winner = next((c for c in candidate_list if c['result'] == 'Won'), None)
            if winner and is_flip_eligible:
                winner_caucus = winner.get('caucus') or winner['party']
                # Case 1: incumbent lost in this election (different party)
                inc_loser = next((c for c in candidate_list
                                  if c.get('is_incumbent') and c['result'] == 'Lost'
                                  and c['party'] != winner['party']), None)
                if inc_loser:
                    elec_obj['flipped_seat'] = {
                        'from': inc_loser.get('caucus') or inc_loser['party'],
                        'to': winner_caucus,
                    }
                # Case 2: open seat (no incumbent running) — compare to
                # the most recent ended term for this seat before election date
                elif not any(c.get('is_incumbent') for c in candidate_list):
                    elec_date = str(e.get('election_date', ''))
                    prev_terms = [
                        t for t in all_terms_by_seat.get(seat_id, [])
                        if t.get('end_date') and str(t['end_date']) <= elec_date
                    ]
                    if prev_terms:
                        prev = max(prev_terms, key=lambda t: str(t['end_date']))
                        prev_caucus = prev.get('holder_caucus') or prev.get('holder_party')
                        if prev_caucus and prev_caucus != winner_caucus:
                            elec_obj['flipped_seat'] = {
                                'from': prev_caucus,
                                'to': winner_caucus,
                            }

            seat_elections.append(elec_obj)

        # Forecast info for this seat
        forecast = forecasts_by_seat.get(seat_id)

        # Party switches for this seat
        seat_switches = [
            {
                'name': sw['name'],
                'year': sw['switch_year'],
                'date': str(sw['switch_date']) if sw.get('switch_date') else None,
                'old_party': sw['old_party'],
                'new_party': sw['new_party'],
                'old_caucus': sw.get('old_caucus'),
                'new_caucus': sw.get('new_caucus'),
                'bp_url': sw.get('bp_profile_url'),
            }
            for sw in switches_by_seat.get(seat_id, [])
        ]

        seat_obj = {
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
            'party_switches': seat_switches,
            'term_events': term_events,
            'forecast': forecast,
        }
        # Include raw caucus for coalition annotation (AK)
        if r.get('raw_caucus') == 'C':
            seat_obj['raw_caucus'] = 'C'
        districts_by_state[state][did]['seats'].append(seat_obj)

    # --- Helper: build election/term/switch objects from old-era seat data ---
    def build_old_era_elections(seat_ids, state):
        """Build election list, term_events, and party_switches from old-era seat IDs.

        For bloc-voting multi-member districts, the same election (year/type/date)
        appears on every seat. Deduplicate by merging candidates from all seats
        into a single election object per (year, type, date).
        """
        min_year = MIN_EXPORT_YEAR.get(state, 0)
        elections_map = {}  # (year, type, date) -> elec_obj
        term_events = []
        party_switches = []
        interesting_end_reasons = {'resigned', 'died', 'removed', 'appointed_elsewhere'}
        interesting_start_reasons = {'appointed', 'succeeded'}

        for sid in seat_ids:
            for e in old_elections_by_seat.get(sid, []):
                if e['election_year'] < min_year:
                    continue
                edate = str(e['election_date']) if e.get('election_date') else None
                dedup_key = (e['election_year'], e['election_type'], edate)

                cands = old_candidacies_by_election.get(e['election_id'], [])
                candidate_list = []
                for c in cands:
                    cand_obj = {
                        'id': c['candidate_id'],
                        'name': c['name'],
                        'party': c['party'],
                        'votes': c['votes'],
                        'pct': float(c['pct']) if c['pct'] is not None else None,
                        'result': c['result'],
                        'is_incumbent': c['is_incumbent'],
                        'is_write_in': c['is_write_in'],
                    }
                    if c.get('caucus'):
                        cand_obj['caucus'] = c['caucus']
                    candidate_list.append(cand_obj)

                if dedup_key in elections_map:
                    # Merge: add any candidates not already present (by candidate id)
                    existing_ids = {c['id'] for c in elections_map[dedup_key]['candidates']}
                    for c in candidate_list:
                        if c['id'] not in existing_ids:
                            elections_map[dedup_key]['candidates'].append(c)
                            existing_ids.add(c['id'])
                    # Use highest total_votes across seats
                    if (e['total_votes_cast'] or 0) > (elections_map[dedup_key]['total_votes'] or 0):
                        elections_map[dedup_key]['total_votes'] = e['total_votes_cast']
                else:
                    elections_map[dedup_key] = {
                        'year': e['election_year'],
                        'type': e['election_type'],
                        'date': edate,
                        'total_votes': e['total_votes_cast'],
                        'is_open_seat': e['is_open_seat'],
                        'result_status': e['result_status'],
                        'filing_deadline': str(e['filing_deadline']) if e.get('filing_deadline') else None,
                        'forecast_rating': e['forecast_rating'],
                        'candidates': candidate_list,
                        'old_era': True,
                    }
                    if e.get('precincts_reporting') is not None:
                        elections_map[dedup_key]['precincts_reporting'] = e['precincts_reporting']
                        elections_map[dedup_key]['precincts_total'] = e['precincts_total']

        elections = list(elections_map.values())

        for sid in seat_ids:
            for t in old_terms_by_seat.get(sid, []):
                if t.get('end_reason') in interesting_end_reasons and t.get('end_date'):
                    end_date_str = str(t['end_date'])
                    term_events.append({
                        'name': t['holder_name'],
                        'party': t['holder_party'],
                        'year': int(end_date_str[:4]),
                        'date': end_date_str,
                        'reason': t['end_reason'],
                        'notes': t.get('notes'),
                    })
                if t.get('start_reason') in interesting_start_reasons and t.get('start_date'):
                    start_date_str = str(t['start_date'])
                    term_events.append({
                        'name': t['holder_name'],
                        'party': t['holder_party'],
                        'year': int(start_date_str[:4]),
                        'date': start_date_str,
                        'reason': t['start_reason'],
                        'notes': t.get('notes'),
                    })

            for sw in old_switches_by_seat.get(sid, []):
                party_switches.append({
                    'name': sw['name'],
                    'year': sw['switch_year'],
                    'date': str(sw['switch_date']) if sw.get('switch_date') else None,
                    'old_party': sw['old_party'],
                    'new_party': sw['new_party'],
                    'old_caucus': sw.get('old_caucus'),
                    'new_caucus': sw.get('new_caucus'),
                    'bp_url': sw.get('bp_profile_url'),
                })

        return elections, term_events, party_switches

    # --- Merge old-era elections into matching current districts ---
    for state, dists in districts_by_state.items():
        for did, dinfo in dists.items():
            key = (state, dinfo['chamber'], dinfo['district_number'])
            old_info = old_district_info.get(key)
            if not old_info:
                continue
            matched_old_districts.add(key)

            old_elecs, old_term_evts, old_switches = build_old_era_elections(
                old_info['seat_ids'], state
            )
            if not old_elecs and not old_term_evts and not old_switches:
                continue

            # Record old-era seat count if different from current
            if old_info['num_seats'] != dinfo['num_seats']:
                dinfo['old_era_seats'] = old_info['num_seats']

            # Append old-era elections to the first seat (seat A)
            # For bloc-voting old-era districts, all candidates appeared in one pool
            primary_seat = dinfo['seats'][0]
            primary_seat['elections'].extend(old_elecs)
            primary_seat['term_events'].extend(old_term_evts)
            primary_seat['party_switches'].extend(old_switches)

    # --- Generate eliminated district entries (old-era only, not in current cycle) ---
    for key, old_info in old_district_info.items():
        if key in matched_old_districts:
            continue
        state, chamber, dist_num = key
        old_elecs, old_term_evts, old_switches = build_old_era_elections(
            old_info['seat_ids'], state
        )
        if not old_elecs:
            continue  # No elections to show — skip

        rc = old_info['redistricting_cycle']
        # redistricting_year = when this district was eliminated (the next cycle start),
        # not when it was created. Any old-era district not in 2022 was eliminated by
        # the 2022 redistricting. If future cycles are added, this should find the
        # next cycle after rc instead of hardcoding.
        redistricting_year = 2022

        if state not in districts_by_state:
            districts_by_state[state] = {}

        # Use a synthetic district_id key (negative to avoid collision)
        synthetic_id = f'old_{old_info["district_id"]}'

        # Build a minimal seat object for the eliminated district
        eliminated_seat = {
            'seat_id': None,
            'seat_label': f'{chamber} {dist_num}',
            'seat_designator': 'A',
            'current_holder': None,
            'current_holder_party': None,
            'current_holder_caucus': None,
            'term_length': None,
            'next_election': None,
            'election_class': None,
            'since_year': None,
            'elections': old_elecs,
            'party_switches': old_switches,
            'term_events': old_term_evts,
            'forecast': None,
        }

        districts_by_state[state][synthetic_id] = {
            'district_number': dist_num,
            'district_name': old_info['district_name'],
            'chamber': chamber,
            'num_seats': old_info['num_seats'],
            'pres_2024_margin': None,
            'pres_2024_winner': None,
            'redistricting_year': redistricting_year,
            'is_floterial': False,
            'eliminated': True,
            'redistricting_cycle': rc,
            'seats': [eliminated_seat],
        }

    # --- Detect new districts (exist in 2022 cycle but not in any older cycle) ---
    for state, dists in districts_by_state.items():
        for did, dinfo in dists.items():
            if dinfo.get('eliminated'):
                continue
            key = (state, dinfo['chamber'], dinfo['district_number'])
            if key not in old_district_info and dinfo.get('redistricting_year'):
                dinfo['is_new_district'] = True

    # --- Detect bloc voting for multi-member districts ---
    # Bloc voting: seats share identical candidate sets for the same election.
    # If no elections have candidates to compare, assume bloc voting for multi-member.
    for state, dists in districts_by_state.items():
        for did, dinfo in dists.items():
            uses_bloc = False
            seats = dinfo['seats']
            if len(seats) > 1:
                # Build seat 0's non-special election candidate sets
                seat0_elecs = {}
                for e in seats[0]['elections']:
                    if 'Special' in e['type']:
                        continue  # Skip specials — those are per-seat even in bloc voting
                    cnames = set(c['name'] for c in e['candidates'])
                    if cnames:
                        seat0_elecs[(e['year'], e['type'])] = cnames
                if not seat0_elecs:
                    # No non-special elections with candidates — assume bloc for multi-member
                    uses_bloc = True
                else:
                    # Check if any other seat has different candidates (= position-based)
                    found_different = False
                    found_match = False
                    for other_seat in seats[1:]:
                        for e in other_seat['elections']:
                            if 'Special' in e['type']:
                                continue
                            key = (e['year'], e['type'])
                            other_cnames = set(c['name'] for c in e['candidates'])
                            if key in seat0_elecs and other_cnames:
                                if seat0_elecs[key] == other_cnames:
                                    found_match = True
                                else:
                                    found_different = True
                                    break
                        if found_different:
                            break
                    # Bloc unless we found evidence of different candidates per seat
                    uses_bloc = not found_different
            dinfo['uses_bloc_voting'] = uses_bloc

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
        # Sort districts by chamber, then by district_number (numeric then alpha)
        def dist_sort_key(did):
            d = dists[did]
            dn = d['district_number']
            try:
                num = int(dn)
            except (ValueError, TypeError):
                num = 99999
            return (d['chamber'], num, dn)
        for did in sorted(dists.keys(), key=dist_sort_key):
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
                            effective_party = winner.get('caucus') or winner['party']
                            party_sign = 1 if effective_party == 'D' else -1 if effective_party == 'R' else 0
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
                'redistricting_year': d.get('redistricting_year'),
                'seats': d['seats'],
                'uses_bloc_voting': d.get('uses_bloc_voting', False),
                'partisan_shift': partisan_shift,
                'similar_districts': similar,
            }
            # Add optional flags for old-era data
            if d.get('eliminated'):
                district_obj['eliminated'] = True
                district_obj['redistricting_cycle'] = d.get('redistricting_cycle')
            if d.get('old_era_seats'):
                district_obj['old_era_seats'] = d['old_era_seats']
            if d.get('is_new_district'):
                district_obj['is_new_district'] = True

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
            'redistricting_cycles': redistricting_by_state.get(state, {}),
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
