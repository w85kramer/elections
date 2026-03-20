#!/usr/bin/env python3
"""
Export per-state statewide office page data and dashboard summary files.

Generates:
  - site/data/ag/{ST}.json — per-state Attorney General data
  - site/data/sos/{ST}.json — per-state Secretary of State data
  - site/data/ltgov/{ST}.json — per-state Lt. Governor data
  - site/data/ag_2026.json — AG dashboard summary
  - site/data/sos_2026.json — SoS dashboard summary
  - site/data/ltgov_2026.json — Lt. Gov dashboard summary

Usage:
    python3 scripts/export_statewide_pages.py                          # All offices, all states
    python3 scripts/export_statewide_pages.py --office ag              # AG only
    python3 scripts/export_statewide_pages.py --office ltgov --state PA  # Single office + state
    python3 scripts/export_statewide_pages.py --dry-run                # Show queries only
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

# Import recount thresholds and close-race constant from district export
from export_district_data import RECOUNT_THRESHOLDS, CLOSE_RACE_PCT


def _check_recount_eligible(state_abbr, candidates, total_votes, election_type, result_status):
    """Check if an election is within recount threshold or triggered a runoff."""
    if not total_votes or total_votes == 0:
        return None
    if result_status not in ('Called', 'Unofficial'):
        return None
    sorted_cands = sorted(
        [c for c in candidates if c.get('votes') and c['votes'] > 0],
        key=lambda c: c['votes'], reverse=True)
    if len(sorted_cands) < 2:
        return None
    runoff_cands = [c for c in sorted_cands if c.get('result') == 'Runoff']
    if len(runoff_cands) >= 2:
        r_margin = runoff_cands[0]['votes'] - runoff_cands[-1]['votes']
        r_margin_pct = (r_margin / total_votes) * 100
        eliminated = [c for c in sorted_cands if c.get('result') not in ('Runoff', 'Won', 'Advanced')]
        cutoff_margin = cutoff_pct = None
        if eliminated:
            cutoff_margin = runoff_cands[-1]['votes'] - eliminated[0]['votes']
            cutoff_pct = (cutoff_margin / total_votes) * 100
        return {
            'type': 'runoff_triggered', 'margin': r_margin,
            'margin_pct': round(r_margin_pct, 2), 'cutoff_margin': cutoff_margin,
            'cutoff_margin_pct': round(cutoff_pct, 2) if cutoff_pct is not None else None,
        }
    margin = sorted_cands[0]['votes'] - sorted_cands[1]['votes']
    margin_pct = (margin / total_votes) * 100
    thresholds = RECOUNT_THRESHOLDS.get(state_abbr)
    if thresholds:
        threshold = thresholds.get('statewide')
        if threshold is not None and margin_pct <= threshold:
            return {'type': 'recount', 'margin': margin, 'margin_pct': round(margin_pct, 2), 'threshold_pct': threshold}
    else:
        if margin_pct <= CLOSE_RACE_PCT:
            return {'type': 'close_race', 'margin': margin, 'margin_pct': round(margin_pct, 2), 'threshold_pct': CLOSE_RACE_PCT}
    return None


# Office type mapping: CLI key -> (DB office_type, output directory, display name)
OFFICE_TYPES = {
    'ag': ('Attorney General', 'ag', 'Attorney General'),
    'sos': ('Secretary of State', 'sos', 'Secretary of State'),
    'ltgov': ('Lt. Governor', 'ltgov', 'Lt. Governor'),
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


def export_statewide_pages(office_key, dry_run=False, single_state=None):
    """Export per-state page data for a statewide office using bulk queries."""
    office_type, out_subdir, display_name = OFFICE_TYPES[office_key]
    label = single_state or 'all states'
    print(f'\nExporting {display_name} page data for {label}...')

    state_filter = f"AND st.abbreviation = '{single_state}'" if single_state else ""

    # Query 1: Seats — current holder, term info, selection method
    q_seats = f"""
        SELECT
            st.abbreviation as state,
            st.state_name,
            d.pres_2024_margin,
            se.id as seat_id,
            se.current_holder,
            se.current_holder_party,
            se.current_holder_caucus,
            se.next_regular_election_year,
            se.term_length_years,
            se.selection_method,
            se.notes as seat_notes
        FROM states st
        JOIN districts d ON d.state_id = st.id AND d.office_level = 'Statewide'
        JOIN seats se ON se.district_id = d.id AND se.office_type = '{office_type}'
        WHERE 1=1
          {state_filter}
        ORDER BY st.abbreviation
    """

    # Query 2: All seat_terms (full officeholder history per state)
    q_terms = f"""
        SELECT
            st.abbreviation as state,
            c.full_name as name,
            stm.party,
            stm.start_date,
            stm.end_date,
            stm.start_reason,
            stm.end_reason,
            stm.notes
        FROM seat_terms stm
        JOIN seats se ON stm.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON stm.candidate_id = c.id
        WHERE se.office_type = '{office_type}'
          {state_filter}
        ORDER BY st.abbreviation, stm.start_date
    """

    # Query 3: All general elections for this office
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
            e.forecast_rating,
            e.precincts_reporting,
            e.precincts_total,
            e.linked_election_id,
            e.notes
        FROM elections e
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE se.office_type = '{office_type}'
          {state_filter}
        ORDER BY st.abbreviation, e.election_year DESC, e.election_type
    """

    # Query 4: Candidacies
    q_candidacies = f"""
        SELECT
            st.abbreviation as state,
            cy.id as candidacy_id,
            cy.election_id,
            c.id as candidate_id,
            c.full_name as name,
            cy.party,
            cy.votes_received as votes,
            cy.vote_percentage as pct,
            cy.result,
            cy.is_incumbent,
            cy.is_write_in,
            cy.candidate_status,
            cy.is_major,
            cy.running_mate_candidacy_id,
            e.election_type
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON cy.candidate_id = c.id
        WHERE se.office_type = '{office_type}'
          {state_filter}
        ORDER BY st.abbreviation, cy.election_id,
            CASE cy.result WHEN 'Won' THEN 0 WHEN 'Advanced' THEN 1 ELSE 2 END,
            cy.votes_received DESC NULLS LAST
    """

    # Query 5: Forecasts for 2026 races
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
        WHERE se.office_type = '{office_type}'
          AND e.election_year = 2026
          AND e.election_type = 'General'
          {state_filter}
        ORDER BY st.abbreviation, f.source
    """

    # Query 6: Primary dates for 2026 races
    q_primaries = f"""
        SELECT DISTINCT
            st.abbreviation as state,
            e.election_date as primary_date
        FROM elections e
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE se.office_type = '{office_type}'
          AND e.election_type IN ('Primary_D', 'Primary_R', 'Primary')
          AND e.election_year = 2026
          {state_filter}
        ORDER BY st.abbreviation
    """

    if dry_run:
        print(f'  Would run 7 queries and write {display_name} JSON files')
        print(f'\n  Sample query (seats):\n{q_seats[:300]}...')
        return

    print('  Running 7 bulk queries...')
    seats_data = run_sql(q_seats)
    print(f'    1/7 {display_name} seats: {len(seats_data)} rows')
    terms_data = run_sql(q_terms)
    print(f'    2/7 seat_terms: {len(terms_data)} rows')
    elections_data = run_sql(q_elections)
    print(f'    3/7 elections: {len(elections_data)} rows')
    candidacies_data = run_sql(q_candidacies)
    print(f'    4/7 candidacies: {len(candidacies_data)} rows')
    forecasts_data = run_sql(q_forecasts)
    print(f'    5/7 forecasts: {len(forecasts_data)} rows')
    primaries_data = run_sql(q_primaries)
    print(f'    6/7 primaries: {len(primaries_data)} rows')

    # Query 7: Candidacies for linked elections (Gov/Lt Gov running mates)
    linked_ids = [e['linked_election_id'] for e in elections_data if e.get('linked_election_id')]
    linked_candidacies_data = []
    if linked_ids:
        ids_str = ','.join(str(i) for i in linked_ids)
        q_linked = f"""
            SELECT
                cy.id as candidacy_id,
                cy.election_id,
                c.id as candidate_id,
                c.full_name as name,
                cy.party
            FROM candidacies cy
            JOIN candidates c ON cy.candidate_id = c.id
            WHERE cy.election_id IN ({ids_str})
        """
        linked_candidacies_data = run_sql(q_linked)
    print(f'    7/7 linked candidacies: {len(linked_candidacies_data)} rows')

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

    # Candidacy by id (for running_mate_candidacy_id lookups)
    candidacy_by_id = {}
    for r in candidacies_data:
        candidacy_by_id[r['candidacy_id']] = {
            'name': r['name'],
            'candidate_id': r['candidate_id'],
        }
    for r in linked_candidacies_data:
        candidacy_by_id[r['candidacy_id']] = {
            'name': r['name'],
            'candidate_id': r['candidate_id'],
        }

    # Candidates by (election_id, party) for party-based j/t fallback
    # Include linked candidacies so Gov↔Lt Gov matching works
    candidates_by_election_party = {}
    for r in linked_candidacies_data:
        candidates_by_election_party.setdefault(r['election_id'], {})[r['party']] = {
            'name': r['name'],
            'candidate_id': r['candidate_id'],
        }

    # Forecasts by state
    forecasts_by_state = {}
    for r in forecasts_data:
        forecasts_by_state.setdefault(r['state'], {})[r['source']] = r['rating']

    # Primary dates by state
    primary_by_state = {}
    for r in primaries_data:
        if r['state'] not in primary_by_state:
            primary_by_state[r['state']] = r['primary_date']

    # --- Write per-state JSON ---
    out_dir = os.path.join(SITE_DATA_DIR, out_subdir)
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
                generals_by_year[e['election_year']] = {
                    'margin': margin,
                    'winner_party': winner['party'] if winner else None,
                }

        timeline = []
        for t in terms:
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

            entry = {
                'name': t['name'],
                'party': t['party'],
                'start': t['start_date'],
                'end': t['end_date'],
                'start_reason': t['start_reason'],
                'end_reason': t['end_reason'],
                'election_year': election_year,
                'margin': margin,
            }
            if t.get('notes'):
                entry['notes'] = t['notes']
            timeline.append(entry)
        # Reverse so most recent first
        timeline.reverse()

        # Build elections list with candidacies
        elections_list = []
        for e in elections:
            cands = candidacies_by_election.get(e['election_id'], [])
            candidate_list = []
            for c in cands:
                entry = {
                    'candidate_id': c['candidate_id'],
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
                }
                # Joint ticket running mate resolution
                mate = None
                rm_id = c.get('running_mate_candidacy_id')
                if rm_id and rm_id in candidacy_by_id:
                    mate = candidacy_by_id[rm_id]
                else:
                    linked_id = e.get('linked_election_id')
                    if linked_id and linked_id in candidates_by_election_party:
                        mate = candidates_by_election_party[linked_id].get(c['party'])
                if mate:
                    entry['jt_name'] = mate['name']
                    entry['jt_candidate_id'] = mate['candidate_id']
                candidate_list.append(entry)

            elec_obj = {
                'year': e['election_year'],
                'type': e['election_type'],
                'date': str(e['election_date']) if e.get('election_date') else None,
                'total_votes': e['total_votes_cast'],
                'result_status': e['result_status'],
                'is_open_seat': e['is_open_seat'],
                'filing_deadline': str(e['filing_deadline']) if e.get('filing_deadline') else None,
                'forecast_rating': e['forecast_rating'],
                'candidates': candidate_list,
            }
            if e.get('precincts_reporting') is not None:
                elec_obj['precincts_reporting'] = e['precincts_reporting']
                elec_obj['precincts_total'] = e['precincts_total']
            if e.get('notes'):
                elec_obj['notes'] = e['notes']

            # --- Badge computations ---
            recount_flag = _check_recount_eligible(
                state, candidate_list, e['total_votes_cast'],
                e['election_type'], e.get('result_status'))
            if recount_flag:
                elec_obj['recount_eligible'] = recount_flag

            if 'Primary' in e['election_type']:
                inc_lost = [c for c in candidate_list
                            if c.get('is_incumbent') and c['result'] == 'Lost']
                if inc_lost:
                    elec_obj['incumbent_defeated'] = True

            FLIP_ELIGIBLE_TYPES = {
                'General', 'General_Runoff',
                'Special', 'Special_General', 'Special_Runoff', 'Recall',
            }
            winner = next((c for c in candidate_list if c['result'] == 'Won'), None)
            if winner and e['election_type'] in FLIP_ELIGIBLE_TYPES:
                winner_party = winner['party']
                inc_loser = next((c for c in candidate_list
                                  if c.get('is_incumbent') and c['result'] == 'Lost'
                                  and c['party'] != winner_party), None)
                if inc_loser:
                    elec_obj['flipped_seat'] = {'from': inc_loser['party'], 'to': winner_party}
                elif not any(c.get('is_incumbent') for c in candidate_list):
                    elec_year = e.get('election_year')
                    prev_terms = [t for t in terms if t.get('start_date') and int(str(t['start_date'])[:4]) < elec_year]
                    if prev_terms:
                        prev = max(prev_terms, key=lambda t: str(t['start_date']))
                        prev_party = prev.get('party')
                        if prev_party and prev_party != winner_party:
                            elec_obj['flipped_seat'] = {'from': prev_party, 'to': winner_party}

            elections_list.append(elec_obj)

        # Current holder (first term with no end_date)
        current_holder = None
        for t in terms:
            if t['end_date'] is None:
                current_holder = {
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

            # Gather 2026 candidates (deduplicated across primary/general)
            cands_2026 = []
            for e in elections:
                if e['election_year'] == 2026:
                    for c in candidacies_by_election.get(e['election_id'], []):
                        if not any(x['name'] == c['name'] and x['party'] == c['party']
                                   for x in cands_2026):
                            entry_2026 = {
                                'candidate_id': c['candidate_id'],
                                'name': c['name'],
                                'party': c['party'],
                                'status': c['candidate_status'],
                                'result': c['result'],
                                'is_major': c.get('is_major') or False,
                                'election_type': c['election_type'],
                            }
                            # J/t for 2026 candidates
                            mate = None
                            rm_id = c.get('running_mate_candidacy_id')
                            if rm_id and rm_id in candidacy_by_id:
                                mate = candidacy_by_id[rm_id]
                            else:
                                linked_id = e.get('linked_election_id')
                                if linked_id and linked_id in candidates_by_election_party:
                                    mate = candidates_by_election_party[linked_id].get(c['party'])
                            if mate:
                                entry_2026['jt_name'] = mate['name']
                                entry_2026['jt_candidate_id'] = mate['candidate_id']
                            cands_2026.append(entry_2026)

            # Determine open seat: if current holder started via election and term is expiring
            is_open = gen_2026['is_open_seat'] if gen_2026 and gen_2026.get('is_open_seat') is not None else False

            race_2026 = {
                'is_open_seat': is_open,
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

        # Determine method from selection_method column
        method = seat.get('selection_method') or 'Elected'

        result = {
            'generated_at': generated_at,
            'state': state,
            'state_name': seat['state_name'],
            'office_type': display_name,
            'office_key': office_key,
            'term_years': seat.get('term_length_years'),
            'term_limit': seat.get('seat_notes'),  # term limit info often in notes
            'method': method,
            'pres_2024_margin': pres_margin_str,
            'next_regular_election': seat['next_regular_election_year'],
            'current_holder': current_holder,
            'timeline': timeline,
            'elections': elections_list,
            'race_2026': race_2026,
        }

        out_path = os.path.join(out_dir, f'{state}.json')
        with open(out_path, 'w') as f:
            json.dump(result, f, separators=(',', ':'))
        size_kb = os.path.getsize(out_path) / 1024
        print(f'    {state}: {len(timeline)} terms, {len(elections_list)} elections, {size_kb:.1f} KB')

        total_terms += len(timeline)
        total_elections += len(elections_list)

    print(f'\n  Total: {len(seats_by_state)} states, {total_terms} terms, {total_elections} elections')
    print(f'  Written to {out_dir}/')


def export_statewide_dashboard(office_key, dry_run=False):
    """Export 2026 dashboard summary for a statewide office."""
    office_type, out_subdir, display_name = OFFICE_TYPES[office_key]
    print(f'\nExporting {display_name} 2026 dashboard summary...')

    # Query 1: All seats of this office type with current holders
    q_races = f"""
    SELECT
      s.abbreviation as state,
      s.state_name,
      se.id as seat_id,
      se.current_holder,
      se.current_holder_party,
      se.current_holder_caucus,
      se.next_regular_election_year,
      se.term_length_years,
      se.selection_method,
      se.notes as seat_notes,
      st.candidate_id,
      st.party as holder_party,
      st.start_date as holder_start,
      st.start_reason,
      c.full_name as holder_name
    FROM states s
    JOIN districts d ON d.state_id = s.id AND d.office_level = 'Statewide'
    JOIN seats se ON se.district_id = d.id AND se.office_type = '{office_type}'
    LEFT JOIN seat_terms st ON st.seat_id = se.id AND st.end_date IS NULL
    LEFT JOIN candidates c ON c.id = st.candidate_id
    ORDER BY s.abbreviation
    """

    # Query 2: 2026 general elections
    q_elections = f"""
    SELECT
      s.abbreviation as state,
      e.id as election_id,
      e.election_date,
      e.election_type,
      e.forecast_rating,
      e.filing_deadline,
      e.is_open_seat,
      e.pres_margin_this_cycle
    FROM elections e
    JOIN seats se ON se.id = e.seat_id
    JOIN districts d ON d.id = se.district_id
    JOIN states s ON s.id = d.state_id
    WHERE se.office_type = '{office_type}'
      AND e.election_type = 'General'
      AND e.election_date >= '2026-01-01'
      AND e.election_date <= '2026-12-31'
    ORDER BY s.abbreviation
    """

    # Query 3: Forecast details (Cook + Sabato)
    q_forecasts = f"""
    SELECT
      s.abbreviation as state,
      f.source,
      f.rating
    FROM forecasts f
    JOIN elections e ON e.id = f.election_id
    JOIN seats se ON se.id = e.seat_id
    JOIN districts d ON d.id = se.district_id
    JOIN states s ON s.id = d.state_id
    WHERE se.office_type = '{office_type}'
      AND e.election_type = 'General'
      AND e.election_date >= '2026-01-01'
    ORDER BY s.abbreviation, f.source
    """

    # Query 4: 2026 candidates
    q_candidates = f"""
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
    WHERE se.office_type = '{office_type}'
      AND e.election_date >= '2026-01-01'
      AND e.election_date <= '2026-12-31'
    ORDER BY s.abbreviation, e.election_type, ca.party, c.full_name
    """

    # Query 5: Primary dates
    q_primaries = f"""
    SELECT DISTINCT
      s.abbreviation as state,
      e.election_date as primary_date,
      e.election_type
    FROM elections e
    JOIN seats se ON se.id = e.seat_id
    JOIN districts d ON d.id = se.district_id
    JOIN states s ON s.id = d.state_id
    WHERE se.office_type = '{office_type}'
      AND e.election_type IN ('Primary_D', 'Primary_R', 'Primary')
      AND e.election_date >= '2026-01-01'
      AND e.election_date <= '2026-12-31'
    ORDER BY s.abbreviation
    """

    if dry_run:
        print(f'  Would run 5 queries and write {out_subdir}_2026.json')
        print(f'\n  Sample query (races):\n{q_races[:300]}...')
        return

    print('  Running 5 bulk queries...')
    races_raw = run_sql(q_races)
    print(f'    1/5 seats: {len(races_raw)} rows')
    elections_raw = run_sql(q_elections)
    print(f'    2/5 elections: {len(elections_raw)} rows')
    forecasts_raw = run_sql(q_forecasts)
    print(f'    3/5 forecasts: {len(forecasts_raw)} rows')
    candidates_raw = run_sql(q_candidates)
    print(f'    4/5 candidates: {len(candidates_raw)} rows')
    primaries_raw = run_sql(q_primaries)
    print(f'    5/5 primaries: {len(primaries_raw)} rows')

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

    races = []
    no_race = []

    for row in races_raw:
        st = row['state']
        method = row.get('selection_method') or 'Elected'

        # States without a 2026 election or non-elected offices go to no_race
        if row['next_regular_election_year'] != 2026 or method != 'Elected':
            reason = 'not_elected' if method != 'Elected' else 'no_race_2026'
            no_race.append({
                'state': st,
                'state_name': row['state_name'],
                'reason': reason,
                'method': method,
                'incumbent': row.get('holder_name') or row['current_holder'],
                'incumbent_party': row.get('holder_party') or row['current_holder_party'],
            })
            continue

        elec = elections_by_state.get(st, {})
        fcasts = forecasts_by_state.get(st, {})
        cands = candidates_by_state.get(st, [])

        # Determine open seat from election data
        is_open = elec.get('is_open_seat') or False

        # Compute term number from seat_term start_date
        term_years = row.get('term_length_years') or 4
        running_for_term = None
        if row.get('holder_start') and not is_open:
            start_year = int(row['holder_start'][:4])
            current_term = (2026 - start_year) // term_years + 1
            running_for_term = current_term + 1

        race = {
            'state': st,
            'state_name': row['state_name'],
            'incumbent': row.get('holder_name') or row['current_holder'],
            'incumbent_party': row.get('holder_party') or row['current_holder_party'],
            'is_open_seat': is_open,
            'term_years': row.get('term_length_years'),
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
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'office_type': display_name,
        'office_key': office_key,
        'total_races': len(races),
        'open_seats': sum(1 for r in races if r['is_open_seat']),
        'races': races,
        'no_race_states': sorted(no_race, key=lambda x: x['state']),
    }

    os.makedirs(SITE_DATA_DIR, exist_ok=True)
    outpath = os.path.join(SITE_DATA_DIR, f'{out_subdir}_2026.json')
    with open(outpath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f'  Written: {outpath} ({len(races)} races, {len(no_race)} no-race states)')


def main():
    parser = argparse.ArgumentParser(description='Export statewide office page data')
    parser.add_argument('--office', type=str, default='all',
                        choices=['ag', 'sos', 'ltgov', 'all'],
                        help='Which office to export (default: all)')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--state', type=str, help='Single state (2-letter abbreviation)')
    args = parser.parse_args()

    offices = list(OFFICE_TYPES.keys()) if args.office == 'all' else [args.office]
    single_state = args.state.upper() if args.state else None

    for office_key in offices:
        # Export per-state page data
        export_statewide_pages(office_key, dry_run=args.dry_run, single_state=single_state)
        # Export dashboard summary (only if not filtering to a single state)
        if not single_state:
            export_statewide_dashboard(office_key, dry_run=args.dry_run)

    print('\nDone.')


if __name__ == '__main__':
    main()
