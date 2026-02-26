"""
Export database data to JSON files for the elections website.

Generates:
  - site/data/states_summary.json   (50-state overview)
  - site/data/pres_margins.json     (all district margins for swing calculator)
  - site/data/states/{ST}.json      (per-state detail, all 50 states)

Usage:
    python3 scripts/export_site_data.py                   # Export all (summary + margins + all 50 states)
    python3 scripts/export_site_data.py --state MI        # Single state detail only
    python3 scripts/export_site_data.py --summary-only    # Just states_summary.json
    python3 scripts/export_site_data.py --margins-only    # Just pres_margins.json
    python3 scripts/export_site_data.py --states-only     # Just all 50 state detail JSONs
    python3 scripts/export_site_data.py --dry-run         # Print queries, don't write
"""
import sys
import os
import json
import time
import argparse
import math
from datetime import datetime

import httpx
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

SITE_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'site', 'data')

# Effective partisan alignment for counting/coloring: uses caucus for most states,
# but falls back to party for coalition members (AK) where caucus='C'
# denotes coalition membership rather than partisan alignment.
# Pre-formatted for table alias 's' (used in all seat queries).
EP = ("CASE WHEN s.current_holder_caucus = 'C' THEN s.current_holder_party "
      "ELSE COALESCE(s.current_holder_caucus, s.current_holder_party) END")

def run_sql(query, exit_on_error=True, retries=5):
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
        if exit_on_error:
            sys.exit(1)
        return None

LOWER_CHAMBER_NAMES = {
    'CA': 'Assembly', 'NV': 'Assembly', 'NY': 'Assembly', 'WI': 'Assembly', 'NJ': 'Assembly',
    'MD': 'House of Delegates', 'VA': 'House of Delegates', 'WV': 'House of Delegates',
    'NE': 'Legislature',
}

# States where governor is up in 2026
GOV_2026_STATES = set()  # populated from DB

# Who appoints each appointed/ex-officio statewide officer
APPOINTED_BY = {
    # Lt. Governor (Ex Officio — Senate Presidents)
    ('TN', 'Lt. Governor'): 'State Senate',
    ('WV', 'Lt. Governor'): 'State Senate',
    # Attorney General
    ('AK', 'Attorney General'): 'Governor',
    ('HI', 'Attorney General'): 'Governor',
    ('ME', 'Attorney General'): 'Legislature',
    ('NH', 'Attorney General'): 'Governor',
    ('NJ', 'Attorney General'): 'Governor',
    ('TN', 'Attorney General'): 'State Supreme Court',
    ('WY', 'Attorney General'): 'Governor',
    # Secretary of State
    ('DE', 'Secretary of State'): 'Governor',
    ('FL', 'Secretary of State'): 'Governor',
    ('MD', 'Secretary of State'): 'Governor',
    ('ME', 'Secretary of State'): 'Legislature',
    ('NH', 'Secretary of State'): 'Legislature',
    ('NJ', 'Secretary of State'): 'Governor',
    ('NY', 'Secretary of State'): 'Governor',
    ('OK', 'Secretary of State'): 'Governor',
    ('PA', 'Secretary of State'): 'Governor',
    ('TN', 'Secretary of State'): 'Legislature',
    ('TX', 'Secretary of State'): 'Governor',
    ('VA', 'Secretary of State'): 'Governor',
    # Treasurer
    ('AK', 'Treasurer'): 'Governor',
    ('GA', 'Treasurer'): 'Governor',
    ('HI', 'Treasurer'): 'Governor',
    ('MD', 'Treasurer'): 'Legislature',
    ('ME', 'Treasurer'): 'Legislature',
    ('MI', 'Treasurer'): 'Governor',
    ('MN', 'Treasurer'): 'Governor',
    ('MT', 'Treasurer'): 'Governor',
    ('NH', 'Treasurer'): 'Legislature',
    ('NJ', 'Treasurer'): 'Governor',
    ('TN', 'Treasurer'): 'Legislature',
    ('VA', 'Treasurer'): 'Governor',
    # Auditor (mostly Legislature)
    ('AK', 'Auditor'): 'Legislature', ('AZ', 'Auditor'): 'Legislature',
    ('CA', 'Auditor'): 'Governor', ('CO', 'Auditor'): 'Legislature',
    ('CT', 'Auditor'): 'Legislature', ('FL', 'Auditor'): 'Legislature',
    ('GA', 'Auditor'): 'Legislature', ('HI', 'Auditor'): 'Legislature',
    ('ID', 'Auditor'): 'Legislature', ('IL', 'Auditor'): 'Legislature',
    ('KS', 'Auditor'): 'Legislature', ('LA', 'Auditor'): 'Legislature',
    ('MD', 'Auditor'): 'Legislature', ('ME', 'Auditor'): 'Legislature',
    ('MI', 'Auditor'): 'Legislature', ('NH', 'Auditor'): 'Legislature',
    ('NJ', 'Auditor'): 'Legislature', ('NV', 'Auditor'): 'Legislature',
    ('NY', 'Auditor'): 'Legislature', ('OR', 'Auditor'): 'Secretary of State',
    ('RI', 'Auditor'): 'Legislature', ('SC', 'Auditor'): 'Legislature',
    ('TN', 'Auditor'): 'Legislature', ('TX', 'Auditor'): 'Legislature',
    ('VA', 'Auditor'): 'Legislature', ('WI', 'Auditor'): 'Legislature',
    # Controller (mostly Governor)
    ('AK', 'Controller'): 'Governor', ('AL', 'Controller'): 'Governor',
    ('CO', 'Controller'): 'Governor', ('MA', 'Controller'): 'Governor',
    ('ME', 'Controller'): 'Governor', ('NC', 'Controller'): 'Governor',
    ('NH', 'Controller'): 'Governor', ('NJ', 'Controller'): 'Governor',
    ('NM', 'Controller'): 'Governor', ('TN', 'Controller'): 'Legislature',
    ('VA', 'Controller'): 'Governor',
}

# Supermajority thresholds: default ceil(total * 2/3), overrides below
SUPERMAJORITY_OVERRIDES = {
    # state_chamber: threshold  (if different from ceil(2/3 * total))
}

def get_lower_chamber(state_abbr):
    return LOWER_CHAMBER_NAMES.get(state_abbr, 'House')

def compute_trifecta(gov_party, chambers):
    """Determine trifecta status from governor party and chamber compositions."""
    if not gov_party:
        return 'N/A'
    chamber_parties = []
    for ch_name, ch_data in chambers.items():
        d = ch_data.get('d', 0)
        r = ch_data.get('r', 0)
        total = ch_data.get('total', 0)
        majority = total // 2 + 1
        if d >= majority:
            chamber_parties.append('D')
        elif r >= majority:
            chamber_parties.append('R')
        else:
            chamber_parties.append('split')

    if all(p == gov_party for p in chamber_parties):
        if gov_party == 'D':
            return 'Democratic Trifecta'
        elif gov_party == 'R':
            return 'Republican Trifecta'
    return 'Divided'

def export_states_summary(dry_run=False):
    """Export states_summary.json with 50-state overview."""
    print('Exporting states_summary.json...')

    # Query 1: Chamber composition per state (from seats table, active seat_terms)
    q_chambers = f"""
        SELECT
            st.abbreviation,
            st.state_name,
            st.senate_seats,
            st.house_seats,
            st.next_gov_election_year,
            d.chamber,
            COUNT(*) as total_seats,
            COUNT(*) FILTER (WHERE ({EP}) = 'D') as d_seats,
            COUNT(*) FILTER (WHERE ({EP}) = 'R') as r_seats,
            COUNT(*) FILTER (WHERE ({EP}) NOT IN ('D','R')
                             AND s.current_holder IS NOT NULL) as other_seats,
            COUNT(*) FILTER (WHERE s.current_holder IS NULL) as vacant_seats,
            COUNT(*) FILTER (WHERE s.next_regular_election_year = 2026) as seats_up_2026
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_level = 'Legislative'
        GROUP BY st.abbreviation, st.state_name, st.senate_seats, st.house_seats,
                 st.next_gov_election_year, d.chamber
        ORDER BY st.abbreviation, d.chamber
    """

    # Query 2: Statewide officers per state
    q_officers = f"""
        SELECT
            st.abbreviation,
            s.office_type,
            s.current_holder as name,
            {EP} as party,
            s.selection_method,
            s.next_regular_election_year
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_level = 'Statewide'
          AND s.selection_method = 'Elected'
        ORDER BY st.abbreviation,
            CASE s.office_type
                WHEN 'Governor' THEN 1
                WHEN 'Lt. Governor' THEN 2
                WHEN 'Attorney General' THEN 3
                WHEN 'Secretary of State' THEN 4
                WHEN 'Treasurer' THEN 5
                WHEN 'Auditor' THEN 6
                WHEN 'Controller' THEN 7
                WHEN 'Superintendent of Public Instruction' THEN 8
                WHEN 'Insurance Commissioner' THEN 9
                WHEN 'Agriculture Commissioner' THEN 10
                WHEN 'Labor Commissioner' THEN 11
            END
    """

    # Query 3: Election counts and dates per state (with split filing deadlines)
    q_elections = """
        SELECT
            st.abbreviation,
            COUNT(*) FILTER (WHERE e.election_type = 'General' AND e.election_year = 2026) as generals_2026,
            MIN(e.filing_deadline) FILTER (WHERE e.election_type = 'General' AND e.election_year = 2026
                                           AND s.office_level = 'Legislative') as filing_deadline_leg,
            MIN(e.filing_deadline) FILTER (WHERE e.election_type = 'General' AND e.election_year = 2026
                                           AND s.office_level = 'Statewide') as filing_deadline_sw,
            MIN(e.election_date) FILTER (WHERE e.election_type IN ('Primary_D','Primary_R','Primary','Primary_Nonpartisan')
                                         AND e.election_year = 2026) as primary_date
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE e.election_year = 2026
          AND e.election_type IN ('General','Primary_D','Primary_R','Primary','Primary_Nonpartisan')
        GROUP BY st.abbreviation
        ORDER BY st.abbreviation
    """

    # Query 4: Governor forecasts
    q_forecasts = """
        SELECT
            st.abbreviation,
            e.forecast_rating
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_type = 'Governor'
          AND e.election_type = 'General'
          AND e.election_year = 2026
    """

    # Query 5: Ballot measures count by state for 2026
    q_measures = """
        SELECT st.abbreviation, COUNT(*) as cnt
        FROM ballot_measures bm
        JOIN states st ON bm.state_id = st.id
        WHERE bm.election_year = 2026
        GROUP BY st.abbreviation
    """

    # Query 6: Special elections (upcoming and recent)
    q_specials = """
        SELECT
            st.abbreviation as state,
            st.state_name,
            e.election_date,
            e.election_type,
            e.result_status,
            s.seat_label,
            d.chamber,
            d.district_number
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE e.election_type LIKE 'Special%'
          AND e.election_date IS NOT NULL
          AND e.election_date >= '2025-11-01'
        ORDER BY e.election_date, st.abbreviation, s.seat_label
    """

    # Query 7: Uncontested primary counts per state
    q_uncontested = """
        WITH primary_counts AS (
            SELECT e.id, st.abbreviation as state, e.election_type, d.chamber,
                COUNT(cy.id) FILTER (
                    WHERE cy.candidate_status NOT IN ('Withdrawn_Pre_Ballot','Withdrawn_Post_Ballot','Disqualified')
                      AND cy.is_write_in = FALSE
                ) as active_count
            FROM elections e
            JOIN seats s ON e.seat_id = s.id
            JOIN districts d ON s.district_id = d.id
            JOIN states st ON d.state_id = st.id
            LEFT JOIN candidacies cy ON cy.election_id = e.id
            WHERE e.election_year = 2026
              AND e.election_type IN ('Primary_D','Primary_R')
              AND s.office_level = 'Legislative'
            GROUP BY e.id, st.abbreviation, e.election_type, d.chamber
            HAVING COUNT(cy.id) > 0
        )
        SELECT state, election_type, chamber,
            COUNT(*) as total, COUNT(*) FILTER (WHERE active_count <= 1) as uncontested
        FROM primary_counts
        GROUP BY state, election_type, chamber
        ORDER BY state, election_type, chamber
    """

    if dry_run:
        print('  Would run 7 queries and write states_summary.json')
        return

    chambers_data = run_sql(q_chambers)
    officers_data = run_sql(q_officers)
    elections_data = run_sql(q_elections)
    forecasts_data = run_sql(q_forecasts)
    measures_data = run_sql(q_measures)
    specials_data = run_sql(q_specials)
    uncontested_data = run_sql(q_uncontested)

    # Index forecasts and measures
    forecasts_by_state = {r['abbreviation']: r['forecast_rating'] for r in forecasts_data}
    measures_by_state = {r['abbreviation']: r['cnt'] for r in measures_data}
    elections_by_state = {r['abbreviation']: r for r in elections_data}

    # Index uncontested data: {state: {Primary_D: {chamber: {total, uncontested}}, ...}}
    uncontested_by_state = {}
    for r in (uncontested_data or []):
        st = r['state']
        if st not in uncontested_by_state:
            uncontested_by_state[st] = {}
        etype = r['election_type']
        if etype not in uncontested_by_state[st]:
            uncontested_by_state[st][etype] = {}
        uncontested_by_state[st][etype][r['chamber']] = {
            'total': r['total'],
            'uncontested': r['uncontested'],
        }

    # Index officers by state
    officers_by_state = {}
    for r in officers_data:
        abbr = r['abbreviation']
        if abbr not in officers_by_state:
            officers_by_state[abbr] = []
        officers_by_state[abbr].append({
            'office': r['office_type'],
            'name': r['name'],
            'party': r['party'],
            'next_election': r['next_regular_election_year'],
        })

    # Build state entries from chamber data
    states = {}
    for r in chambers_data:
        abbr = r['abbreviation']
        if abbr not in states:
            states[abbr] = {
                'name': r['state_name'],
                'abbr': abbr,
                'governor': None,
                'chambers': {},
                'trifecta': None,
                'statewide_officers': officers_by_state.get(abbr, []),
                'elections_2026': {},
                'ballot_measures_2026': measures_by_state.get(abbr, 0),
            }

        chamber_name = r['chamber']
        states[abbr]['chambers'][chamber_name] = {
            'total': r['total_seats'],
            'd': r['d_seats'],
            'r': r['r_seats'],
            'other': r['other_seats'],
            'vacant': r['vacant_seats'],
            'seats_up_2026': r['seats_up_2026'],
        }

    # Fill in governor info and trifecta
    for abbr, st in states.items():
        officers = st['statewide_officers']
        gov = next((o for o in officers if o['office'] == 'Governor'), None)
        if gov:
            forecast = forecasts_by_state.get(abbr)
            st['governor'] = {
                'name': gov['name'],
                'party': gov['party'],
                'next_election': gov['next_election'],
                'forecast': forecast,
            }
            st['trifecta'] = compute_trifecta(gov['party'], st['chambers'])

        elec = elections_by_state.get(abbr, {})
        filing_leg = str(elec['filing_deadline_leg']) if elec.get('filing_deadline_leg') else None
        filing_sw = str(elec['filing_deadline_sw']) if elec.get('filing_deadline_sw') else None
        # Use earliest as the primary filing_deadline for backwards compat
        filing_min = min(filter(None, [filing_leg, filing_sw])) if (filing_leg or filing_sw) else None
        st['elections_2026'] = {
            'total_general': elec.get('generals_2026', 0),
            'filing_deadline': filing_min,
            'filing_deadline_legislative': filing_leg,
            'filing_deadline_statewide': filing_sw,
            'primary_date': str(elec['primary_date']) if elec.get('primary_date') else None,
        }

        # Uncontested primary data
        unc_state = uncontested_by_state.get(abbr)
        if unc_state:
            st['elections_2026']['uncontested'] = {
                'data_available': True,
                'primaries_d': unc_state.get('Primary_D', {}),
                'primaries_r': unc_state.get('Primary_R', {}),
                'general': None,
            }
        else:
            st['elections_2026']['uncontested'] = {
                'data_available': False,
                'primaries_d': {},
                'primaries_r': {},
                'general': None,
            }

    # Build special elections list
    special_elections = []
    for r in specials_data:
        special_elections.append({
            'state': r['state'],
            'state_name': r['state_name'],
            'date': str(r['election_date']) if r.get('election_date') else None,
            'type': r['election_type'],
            'seat_label': r['seat_label'],
            'chamber': r['chamber'],
            'district': r['district_number'],
            'result_status': r['result_status'],
        })

    result = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'states': states,
        'special_elections': special_elections,
    }

    out_path = os.path.join(SITE_DATA_DIR, 'states_summary.json')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(result, f, indent=2)
    print(f'  Written {out_path} ({len(states)} states)')

def export_pres_margins(dry_run=False):
    """Export pres_margins.json with all legislative seat margins for swing calculator."""
    print('Exporting pres_margins.json...')

    q = f"""
        SELECT
            st.abbreviation as state,
            d.chamber,
            d.district_number as district,
            s.seat_designator,
            d.pres_2024_margin as pres_margin,
            {EP} as current_party,
            s.next_regular_election_year,
            s.seat_label
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_level = 'Legislative'
        ORDER BY st.abbreviation, d.chamber,
            CASE WHEN d.district_number SIMILAR TO '[0-9]+' THEN d.district_number::int ELSE 99999 END,
            d.district_number, s.seat_designator
    """

    if dry_run:
        print('  Would run 1 query and write pres_margins.json')
        return

    rows = run_sql(q)

    districts = []
    for r in rows:
        districts.append({
            'state': r['state'],
            'chamber': r['chamber'],
            'district': r['district'],
            'seat_designator': r['seat_designator'],
            'pres_margin': r['pres_margin'],
            'current_party': r['current_party'],
            'up_2026': r['next_regular_election_year'] == 2026,
            'seat_label': r['seat_label'],
        })

    result = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'districts': districts,
    }

    out_path = os.path.join(SITE_DATA_DIR, 'pres_margins.json')
    with open(out_path, 'w') as f:
        json.dump(result, f)  # No indent — this file is ~500KB
    print(f'  Written {out_path} ({len(districts)} seats)')

def export_state_detail(state_abbr, dry_run=False):
    """Export detailed JSON for a single state."""
    print(f'Exporting states/{state_abbr}.json...')

    # Query 1: State info
    q_state = f"""
        SELECT id, state_name, abbreviation, senate_seats, house_seats,
               senate_term_years, house_term_years, uses_jungle_primary, has_runoffs,
               has_multimember_districts, gov_term_years, gov_term_limit,
               next_gov_election_year
        FROM states WHERE abbreviation = '{state_abbr}'
    """

    # Query 2: Statewide officers (elected + appointed + ex officio, excluding N/A)
    q_officers = f"""
        SELECT
            st.abbreviation as state_abbr,
            s.office_type, s.current_holder as name,
            {EP} as party,
            s.current_holder_caucus as caucus,
            s.selection_method, s.next_regular_election_year,
            stm.start_date
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        LEFT JOIN seat_terms stm ON s.id = stm.seat_id AND stm.end_date IS NULL
        WHERE st.abbreviation = '{state_abbr}'
          AND s.office_level = 'Statewide'
          AND (s.selection_method = 'Elected'
               OR (s.selection_method IN ('Appointed','Ex_Officio')
                   AND s.office_type IN ('Lt. Governor','Attorney General','Secretary of State',
                                         'Treasurer','Auditor','Controller')))
        ORDER BY CASE s.office_type
            WHEN 'Governor' THEN 1 WHEN 'Lt. Governor' THEN 2
            WHEN 'Attorney General' THEN 3 WHEN 'Secretary of State' THEN 4
            WHEN 'Treasurer' THEN 5 WHEN 'Auditor' THEN 6
            WHEN 'Controller' THEN 7 WHEN 'Superintendent of Public Instruction' THEN 8
            WHEN 'Insurance Commissioner' THEN 9 WHEN 'Agriculture Commissioner' THEN 10
            WHEN 'Labor Commissioner' THEN 11
        END
    """

    # Query 3: Legislative members by chamber
    q_members = f"""
        SELECT
            d.chamber,
            d.district_number as district,
            d.district_name,
            s.seat_designator,
            s.current_holder as name,
            {EP} as party,
            s.current_holder_caucus as caucus,
            d.pres_2024_margin as pres_margin,
            s.next_regular_election_year,
            s.seat_label
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = '{state_abbr}'
          AND s.office_level = 'Legislative'
        ORDER BY d.chamber,
            CASE WHEN d.district_number SIMILAR TO '[0-9]+' THEN d.district_number::int ELSE 99999 END,
            d.district_number, s.seat_designator
    """

    # Query 4: 2026 candidacies for this state
    q_candidacies = f"""
        SELECT
            s.office_type, s.seat_label, d.chamber, d.district_number,
            e.election_type, e.election_date, e.forecast_rating,
            c.full_name as candidate_name, cy.party, cy.is_incumbent,
            cy.candidate_status, cy.result
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON cy.candidate_id = c.id
        WHERE st.abbreviation = '{state_abbr}'
          AND e.election_year = 2026
          AND e.election_type IN ('General', 'Primary_D', 'Primary_R', 'Primary', 'Primary_Nonpartisan')
        ORDER BY s.office_type,
            CASE WHEN d.district_number SIMILAR TO '[0-9]+' THEN d.district_number::int ELSE 99999 END,
            d.district_number, e.election_type, cy.party, c.full_name
    """

    # Query 5: Ballot measures
    q_measures = f"""
        SELECT measure_number, short_title, description, measure_type,
               status, result, election_year, election_date
        FROM ballot_measures bm
        JOIN states st ON bm.state_id = st.id
        WHERE st.abbreviation = '{state_abbr}'
          AND bm.election_year = 2026
        ORDER BY measure_number
    """

    # Query 6: Governor forecast
    q_forecast = f"""
        SELECT e.forecast_rating, f.source, f.rating
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        LEFT JOIN forecasts f ON f.election_id = e.id
        WHERE st.abbreviation = '{state_abbr}'
          AND s.office_type = 'Governor'
          AND e.election_type = 'General'
          AND e.election_year = 2026
    """

    # Query 7: Uncontested primary detail
    q_uncontested = f"""
        WITH primary_counts AS (
            SELECT
                e.id,
                e.election_type,
                d.chamber,
                d.district_number,
                s.seat_label,
                d.pres_2024_margin,
                {EP} as holder_party,
                e.is_open_seat,
                COUNT(cy.id) FILTER (
                    WHERE cy.candidate_status NOT IN ('Withdrawn_Pre_Ballot','Withdrawn_Post_Ballot','Disqualified')
                      AND cy.is_write_in = FALSE
                ) as active_count,
                STRING_AGG(
                    CASE WHEN cy.candidate_status NOT IN ('Withdrawn_Pre_Ballot','Withdrawn_Post_Ballot','Disqualified')
                              AND cy.is_write_in = FALSE THEN c.full_name END, ', '
                ) as candidate_names
            FROM elections e
            JOIN seats s ON e.seat_id = s.id
            JOIN districts d ON s.district_id = d.id
            JOIN states st ON d.state_id = st.id
            LEFT JOIN candidacies cy ON cy.election_id = e.id
            LEFT JOIN candidates c ON cy.candidate_id = c.id
            WHERE e.election_year = 2026
              AND e.election_type IN ('Primary_D','Primary_R')
              AND s.office_level = 'Legislative'
              AND st.abbreviation = '{state_abbr}'
            GROUP BY e.id, e.election_type, d.chamber, d.district_number,
                     s.seat_label, d.pres_2024_margin, s.current_holder_caucus,
                     s.current_holder_party, e.is_open_seat
            HAVING COUNT(cy.id) > 0
        )
        SELECT * FROM primary_counts WHERE active_count <= 1
        ORDER BY election_type, chamber,
            CASE WHEN district_number SIMILAR TO '[0-9]+' THEN district_number::int ELSE 99999 END,
            district_number
    """

    if dry_run:
        print(f'  Would run 7 queries and write states/{state_abbr}.json')
        return

    state_info = run_sql(q_state)
    if not state_info:
        print(f'  ERROR: State {state_abbr} not found')
        return
    si = state_info[0]

    officers_data = run_sql(q_officers)
    members_data = run_sql(q_members)
    candidacies_data = run_sql(q_candidacies)
    measures_data = run_sql(q_measures)
    forecast_data = run_sql(q_forecast)
    uncontested_data = run_sql(q_uncontested) or []

    # Build officers list
    statewide_officers = []
    for r in officers_data:
        start_year = int(str(r['start_date'])[:4]) if r.get('start_date') else None
        officer = {
            'office': r['office_type'],
            'name': r['name'],
            'party': r['party'],
            'method': r['selection_method'],
            'next_election': r['next_regular_election_year'],
            'start_year': start_year,
        }
        if r['selection_method'] in ('Appointed', 'Ex_Officio'):
            ab = APPOINTED_BY.get((r['state_abbr'], r['office_type']))
            if ab:
                officer['appointed_by'] = ab
        # Add forecast for governor
        if r['office_type'] == 'Governor' and forecast_data:
            officer['forecast'] = forecast_data[0].get('forecast_rating')
            officer['forecast_details'] = [
                {'source': fr['source'], 'rating': fr['rating']}
                for fr in forecast_data if fr.get('source')
            ]
        statewide_officers.append(officer)

    # Build chambers
    chambers = {}
    for r in members_data:
        ch = r['chamber']
        if ch not in chambers:
            chambers[ch] = {
                'total': 0,
                'composition': {'D': 0, 'R': 0, 'Other': 0, 'Vacant': 0},
                'supermajority': 0,
                'seats_up_2026': 0,
                'members': [],
            }
        chambers[ch]['total'] += 1
        party = r['party']
        if party == 'D':
            chambers[ch]['composition']['D'] += 1
        elif party == 'R':
            chambers[ch]['composition']['R'] += 1
        elif r['name'] is None:
            chambers[ch]['composition']['Vacant'] += 1
        else:
            chambers[ch]['composition']['Other'] += 1

        if r['next_regular_election_year'] == 2026:
            chambers[ch]['seats_up_2026'] += 1

        member = {
            'district': r['district'],
            'district_name': r['district_name'],
            'seat_designator': r['seat_designator'],
            'name': r['name'],
            'party': party,
            'pres_margin': r['pres_margin'],
            'next_election': r['next_regular_election_year'],
        }
        if r.get('caucus') and r['caucus'] != party:
            member['caucus'] = r['caucus']
        chambers[ch]['members'].append(member)

    # Set supermajority thresholds
    for ch_name, ch_data in chambers.items():
        total = ch_data['total']
        key = f"{state_abbr}_{ch_name}"
        ch_data['supermajority'] = SUPERMAJORITY_OVERRIDES.get(key, math.ceil(total * 2 / 3))

    # Build 2026 elections section from candidacies
    elections_2026 = {
        'governor': None,
        'statewide': [],
        'legislative': {},
    }

    # Group candidacies
    gov_candidates = []
    statewide_candidates = {}
    leg_candidates = {}

    for r in candidacies_data:
        if r['office_type'] == 'Governor':
            gov_candidates.append({
                'name': r['candidate_name'],
                'party': r['party'],
                'is_incumbent': r['is_incumbent'],
                'election_type': r['election_type'],
                'status': r['candidate_status'],
            })
        elif r['office_type'] not in ('State Senate', 'State House', 'State Legislature'):
            key = r['office_type']
            if key not in statewide_candidates:
                statewide_candidates[key] = []
            statewide_candidates[key].append({
                'name': r['candidate_name'],
                'party': r['party'],
                'is_incumbent': r['is_incumbent'],
                'election_type': r['election_type'],
            })
        else:
            key = f"{r['chamber']}_{r['district_number']}"
            if key not in leg_candidates:
                leg_candidates[key] = {
                    'seat_label': r['seat_label'],
                    'chamber': r['chamber'],
                    'district': r['district_number'],
                    'candidates': [],
                }
            leg_candidates[key]['candidates'].append({
                'name': r['candidate_name'],
                'party': r['party'],
                'is_incumbent': r['is_incumbent'],
                'election_type': r['election_type'],
            })

    if gov_candidates or (si['next_gov_election_year'] == 2026):
        forecast_rating = None
        if forecast_data:
            forecast_rating = forecast_data[0].get('forecast_rating')
        elections_2026['governor'] = {
            'forecast': forecast_rating,
            'candidates': gov_candidates,
        }

    elections_2026['statewide'] = [
        {'office': k, 'candidates': v} for k, v in statewide_candidates.items()
    ]
    elections_2026['legislative'] = list(leg_candidates.values())

    # Ballot measures
    ballot_measures = []
    for r in measures_data:
        ballot_measures.append({
            'number': r['measure_number'],
            'title': r['short_title'],
            'description': r['description'],
            'type': r['measure_type'],
            'status': r['status'],
            'result': r['result'],
            'date': str(r['election_date']) if r.get('election_date') else None,
        })

    # Filing deadline and primary date from elections
    elec_dates = run_sql(f"""
        SELECT
            MIN(e.filing_deadline) as filing_deadline,
            MIN(e.election_date) FILTER (WHERE e.election_type IN ('Primary_D','Primary_R','Primary','Primary_Nonpartisan'))
                as primary_date
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = '{state_abbr}'
          AND e.election_year = 2026
          AND e.election_type IN ('General','Primary_D','Primary_R','Primary','Primary_Nonpartisan')
    """)
    dates = elec_dates[0] if elec_dates else {}

    # Uncontested primaries
    uncontested_primaries = []
    for r in uncontested_data:
        uncontested_primaries.append({
            'election_type': r['election_type'],
            'chamber': r['chamber'],
            'district': r['district_number'],
            'seat_label': r['seat_label'],
            'pres_margin': r['pres_2024_margin'],
            'holder_party': r['holder_party'],
            'candidate': r['candidate_names'] if r['active_count'] == 1 else None,
            'is_open_seat': r['is_open_seat'],
        })

    result = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'state': {
            'name': si['state_name'],
            'abbr': si['abbreviation'],
            'senate_seats': si['senate_seats'],
            'house_seats': si['house_seats'],
            'senate_term_years': si['senate_term_years'],
            'house_term_years': si['house_term_years'],
            'gov_term_years': si['gov_term_years'],
            'gov_term_limit': si['gov_term_limit'],
            'next_gov_election_year': si['next_gov_election_year'],
            'uses_jungle_primary': si['uses_jungle_primary'],
            'has_runoffs': si['has_runoffs'],
        },
        'statewide_officers': statewide_officers,
        'chambers': chambers,
        'elections_2026': elections_2026,
        'uncontested_primaries': uncontested_primaries,
        'ballot_measures': ballot_measures,
        'filing_deadline': str(dates.get('filing_deadline')) if dates.get('filing_deadline') else None,
        'primary_date': str(dates.get('primary_date')) if dates.get('primary_date') else None,
    }

    out_path = os.path.join(SITE_DATA_DIR, 'states', f'{state_abbr}.json')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(result, f, indent=2)
    print(f'  Written {out_path}')

def export_all_state_details(dry_run=False):
    """Export detail JSON for all 50 states using bulk queries (7 total, not 350)."""
    print('Exporting all 50 state detail JSONs (bulk mode)...')

    # --- Bulk Query 1: All state info ---
    q_states = """
        SELECT id, state_name, abbreviation, senate_seats, house_seats,
               senate_term_years, house_term_years, uses_jungle_primary, has_runoffs,
               has_multimember_districts, gov_term_years, gov_term_limit,
               next_gov_election_year
        FROM states ORDER BY abbreviation
    """

    # --- Bulk Query 2: All statewide officers (elected + appointed + ex officio, excluding N/A) ---
    q_officers = f"""
        SELECT
            st.abbreviation,
            s.office_type, s.current_holder as name,
            {EP} as party,
            s.current_holder_caucus as caucus,
            s.selection_method, s.next_regular_election_year,
            stm.start_date
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        LEFT JOIN seat_terms stm ON s.id = stm.seat_id AND stm.end_date IS NULL
        WHERE s.office_level = 'Statewide'
          AND (s.selection_method = 'Elected'
               OR (s.selection_method IN ('Appointed','Ex_Officio')
                   AND s.office_type IN ('Lt. Governor','Attorney General','Secretary of State',
                                         'Treasurer','Auditor','Controller')))
        ORDER BY st.abbreviation,
            CASE s.office_type
                WHEN 'Governor' THEN 1 WHEN 'Lt. Governor' THEN 2
                WHEN 'Attorney General' THEN 3 WHEN 'Secretary of State' THEN 4
                WHEN 'Treasurer' THEN 5 WHEN 'Auditor' THEN 6
                WHEN 'Controller' THEN 7 WHEN 'Superintendent of Public Instruction' THEN 8
                WHEN 'Insurance Commissioner' THEN 9 WHEN 'Agriculture Commissioner' THEN 10
                WHEN 'Labor Commissioner' THEN 11
            END
    """

    # --- Bulk Query 3: All legislative members ---
    q_members = f"""
        SELECT
            st.abbreviation,
            d.chamber,
            d.district_number as district,
            d.district_name,
            s.seat_designator,
            s.current_holder as name,
            {EP} as party,
            s.current_holder_caucus as caucus,
            d.pres_2024_margin as pres_margin,
            s.next_regular_election_year,
            s.seat_label
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_level = 'Legislative'
        ORDER BY st.abbreviation, d.chamber,
            CASE WHEN d.district_number SIMILAR TO '[0-9]+' THEN d.district_number::int ELSE 99999 END,
            d.district_number, s.seat_designator
    """

    # --- Bulk Query 4: All 2026 candidacies ---
    q_candidacies = """
        SELECT
            st.abbreviation,
            s.office_type, s.seat_label, d.chamber, d.district_number,
            e.election_type, e.election_date, e.forecast_rating,
            c.full_name as candidate_name, cy.party, cy.is_incumbent,
            cy.candidate_status, cy.result
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON cy.candidate_id = c.id
        WHERE e.election_year = 2026
          AND e.election_type IN ('General', 'Primary_D', 'Primary_R', 'Primary', 'Primary_Nonpartisan')
        ORDER BY st.abbreviation, s.office_type,
            CASE WHEN d.district_number SIMILAR TO '[0-9]+' THEN d.district_number::int ELSE 99999 END,
            d.district_number, e.election_type, cy.party, c.full_name
    """

    # --- Bulk Query 5: All 2026 ballot measures ---
    q_measures = """
        SELECT st.abbreviation, measure_number, short_title, description, measure_type,
               status, result, election_year, election_date
        FROM ballot_measures bm
        JOIN states st ON bm.state_id = st.id
        WHERE bm.election_year = 2026
        ORDER BY st.abbreviation, measure_number
    """

    # --- Bulk Query 6: All governor forecasts ---
    q_forecasts = """
        SELECT
            st.abbreviation,
            e.forecast_rating, f.source, f.rating
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        LEFT JOIN forecasts f ON f.election_id = e.id
        WHERE s.office_type = 'Governor'
          AND e.election_type = 'General'
          AND e.election_year = 2026
        ORDER BY st.abbreviation
    """

    # --- Bulk Query 7: Filing deadlines and primary dates ---
    q_dates = """
        SELECT
            st.abbreviation,
            MIN(e.filing_deadline) as filing_deadline,
            MIN(e.election_date) FILTER (WHERE e.election_type IN ('Primary_D','Primary_R','Primary','Primary_Nonpartisan'))
                as primary_date
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE e.election_year = 2026
          AND e.election_type IN ('General','Primary_D','Primary_R','Primary','Primary_Nonpartisan')
        GROUP BY st.abbreviation
        ORDER BY st.abbreviation
    """

    # --- Bulk Query 8: Uncontested primary detail ---
    q_uncontested = f"""
        WITH primary_counts AS (
            SELECT
                e.id,
                st.abbreviation as state,
                e.election_type,
                d.chamber,
                d.district_number,
                s.seat_label,
                d.pres_2024_margin,
                {EP} as holder_party,
                e.is_open_seat,
                COUNT(cy.id) FILTER (
                    WHERE cy.candidate_status NOT IN ('Withdrawn_Pre_Ballot','Withdrawn_Post_Ballot','Disqualified')
                      AND cy.is_write_in = FALSE
                ) as active_count,
                STRING_AGG(
                    CASE WHEN cy.candidate_status NOT IN ('Withdrawn_Pre_Ballot','Withdrawn_Post_Ballot','Disqualified')
                              AND cy.is_write_in = FALSE THEN c.full_name END, ', '
                ) as candidate_names
            FROM elections e
            JOIN seats s ON e.seat_id = s.id
            JOIN districts d ON s.district_id = d.id
            JOIN states st ON d.state_id = st.id
            LEFT JOIN candidacies cy ON cy.election_id = e.id
            LEFT JOIN candidates c ON cy.candidate_id = c.id
            WHERE e.election_year = 2026
              AND e.election_type IN ('Primary_D','Primary_R')
              AND s.office_level = 'Legislative'
            GROUP BY e.id, st.abbreviation, e.election_type, d.chamber, d.district_number,
                     s.seat_label, d.pres_2024_margin, s.current_holder_caucus,
                     s.current_holder_party, e.is_open_seat
            HAVING COUNT(cy.id) > 0
        )
        SELECT * FROM primary_counts WHERE active_count <= 1
        ORDER BY state, election_type, chamber,
            CASE WHEN district_number SIMILAR TO '[0-9]+' THEN district_number::int ELSE 99999 END,
            district_number
    """

    if dry_run:
        print('  Would run 8 bulk queries and write 50 state JSON files')
        return

    print('  Running 8 bulk queries...')
    all_states = run_sql(q_states)
    print('    1/8 states')
    all_officers = run_sql(q_officers)
    print('    2/8 officers')
    all_members = run_sql(q_members)
    print('    3/8 members')
    all_candidacies = run_sql(q_candidacies)
    print('    4/8 candidacies')
    all_measures = run_sql(q_measures)
    print('    5/8 measures')
    all_forecasts = run_sql(q_forecasts)
    print('    6/8 forecasts')
    all_dates = run_sql(q_dates)
    print('    7/8 dates')
    all_uncontested = run_sql(q_uncontested)
    print('    8/8 uncontested')

    # --- Index everything by state abbreviation ---
    states_info = {r['abbreviation']: r for r in all_states}

    officers_by_state = {}
    for r in all_officers:
        officers_by_state.setdefault(r['abbreviation'], []).append(r)

    members_by_state = {}
    for r in all_members:
        members_by_state.setdefault(r['abbreviation'], []).append(r)

    candidacies_by_state = {}
    for r in all_candidacies:
        candidacies_by_state.setdefault(r['abbreviation'], []).append(r)

    measures_by_state = {}
    for r in all_measures:
        measures_by_state.setdefault(r['abbreviation'], []).append(r)

    forecasts_by_state = {}
    for r in all_forecasts:
        forecasts_by_state.setdefault(r['abbreviation'], []).append(r)

    dates_by_state = {r['abbreviation']: r for r in all_dates}

    uncontested_by_state = {}
    for r in (all_uncontested or []):
        uncontested_by_state.setdefault(r['state'], []).append(r)

    # --- Build and write each state ---
    generated_at = datetime.utcnow().isoformat() + 'Z'
    out_dir = os.path.join(SITE_DATA_DIR, 'states')
    os.makedirs(out_dir, exist_ok=True)

    print(f'  Writing 50 state files...')
    for abbr in sorted(states_info.keys()):
        si = states_info[abbr]
        forecast_data = forecasts_by_state.get(abbr, [])

        # Officers
        statewide_officers = []
        for r in officers_by_state.get(abbr, []):
            start_year = int(str(r['start_date'])[:4]) if r.get('start_date') else None
            officer = {
                'office': r['office_type'],
                'name': r['name'],
                'party': r['party'],
                'method': r['selection_method'],
                'next_election': r['next_regular_election_year'],
                'start_year': start_year,
            }
            if r['selection_method'] in ('Appointed', 'Ex_Officio'):
                ab = APPOINTED_BY.get((abbr, r['office_type']))
                if ab:
                    officer['appointed_by'] = ab
            if r['office_type'] == 'Governor' and forecast_data:
                officer['forecast'] = forecast_data[0].get('forecast_rating')
                officer['forecast_details'] = [
                    {'source': fr['source'], 'rating': fr['rating']}
                    for fr in forecast_data if fr.get('source')
                ]
            statewide_officers.append(officer)

        # Chambers
        chambers = {}
        for r in members_by_state.get(abbr, []):
            ch = r['chamber']
            if ch not in chambers:
                chambers[ch] = {
                    'total': 0,
                    'composition': {'D': 0, 'R': 0, 'Other': 0, 'Vacant': 0},
                    'supermajority': 0,
                    'seats_up_2026': 0,
                    'members': [],
                }
            chambers[ch]['total'] += 1
            party = r['party']
            if party == 'D':
                chambers[ch]['composition']['D'] += 1
            elif party == 'R':
                chambers[ch]['composition']['R'] += 1
            elif r['name'] is None:
                chambers[ch]['composition']['Vacant'] += 1
            else:
                chambers[ch]['composition']['Other'] += 1
            if r['next_regular_election_year'] == 2026:
                chambers[ch]['seats_up_2026'] += 1
            member = {
                'district': r['district'],
                'district_name': r['district_name'],
                'seat_designator': r['seat_designator'],
                'name': r['name'],
                'party': party,
                'pres_margin': r['pres_margin'],
                'next_election': r['next_regular_election_year'],
            }
            # Include raw caucus when it provides additional info (coalition, cross-party)
            if r.get('caucus') and r['caucus'] != party:
                member['caucus'] = r['caucus']
            chambers[ch]['members'].append(member)

        for ch_name, ch_data in chambers.items():
            total = ch_data['total']
            key = f"{abbr}_{ch_name}"
            ch_data['supermajority'] = SUPERMAJORITY_OVERRIDES.get(key, math.ceil(total * 2 / 3))

        # Candidacies
        gov_candidates = []
        statewide_candidates = {}
        leg_candidates = {}
        for r in candidacies_by_state.get(abbr, []):
            if r['office_type'] == 'Governor':
                gov_candidates.append({
                    'name': r['candidate_name'], 'party': r['party'],
                    'is_incumbent': r['is_incumbent'], 'election_type': r['election_type'],
                    'status': r['candidate_status'],
                })
            elif r['office_type'] not in ('State Senate', 'State House', 'State Legislature'):
                key = r['office_type']
                statewide_candidates.setdefault(key, []).append({
                    'name': r['candidate_name'], 'party': r['party'],
                    'is_incumbent': r['is_incumbent'], 'election_type': r['election_type'],
                })
            else:
                key = f"{r['chamber']}_{r['district_number']}"
                if key not in leg_candidates:
                    leg_candidates[key] = {
                        'seat_label': r['seat_label'], 'chamber': r['chamber'],
                        'district': r['district_number'], 'candidates': [],
                    }
                leg_candidates[key]['candidates'].append({
                    'name': r['candidate_name'], 'party': r['party'],
                    'is_incumbent': r['is_incumbent'], 'election_type': r['election_type'],
                })

        elections_2026 = {'governor': None, 'statewide': [], 'legislative': []}
        if gov_candidates or (si['next_gov_election_year'] == 2026):
            forecast_rating = forecast_data[0].get('forecast_rating') if forecast_data else None
            elections_2026['governor'] = {'forecast': forecast_rating, 'candidates': gov_candidates}
        elections_2026['statewide'] = [{'office': k, 'candidates': v} for k, v in statewide_candidates.items()]
        elections_2026['legislative'] = list(leg_candidates.values())

        # Ballot measures
        ballot_measures = []
        for r in measures_by_state.get(abbr, []):
            ballot_measures.append({
                'number': r['measure_number'], 'title': r['short_title'],
                'description': r['description'], 'type': r['measure_type'],
                'status': r['status'], 'result': r['result'],
                'date': str(r['election_date']) if r.get('election_date') else None,
            })

        # Dates
        dates = dates_by_state.get(abbr, {})

        # Uncontested primaries
        unc_list = uncontested_by_state.get(abbr, [])
        uncontested_primaries = []
        for r in unc_list:
            entry = {
                'election_type': r['election_type'],
                'chamber': r['chamber'],
                'district': r['district_number'],
                'seat_label': r['seat_label'],
                'pres_margin': r['pres_2024_margin'],
                'holder_party': r['holder_party'],
                'candidate': r['candidate_names'] if r['active_count'] == 1 else None,
                'is_open_seat': r['is_open_seat'],
            }
            uncontested_primaries.append(entry)

        result = {
            'generated_at': generated_at,
            'state': {
                'name': si['state_name'], 'abbr': si['abbreviation'],
                'senate_seats': si['senate_seats'], 'house_seats': si['house_seats'],
                'senate_term_years': si['senate_term_years'], 'house_term_years': si['house_term_years'],
                'gov_term_years': si['gov_term_years'], 'gov_term_limit': si['gov_term_limit'],
                'next_gov_election_year': si['next_gov_election_year'],
                'uses_jungle_primary': si['uses_jungle_primary'], 'has_runoffs': si['has_runoffs'],
            },
            'statewide_officers': statewide_officers,
            'chambers': chambers,
            'elections_2026': elections_2026,
            'uncontested_primaries': uncontested_primaries,
            'ballot_measures': ballot_measures,
            'filing_deadline': str(dates.get('filing_deadline')) if dates.get('filing_deadline') else None,
            'primary_date': str(dates.get('primary_date')) if dates.get('primary_date') else None,
        }

        out_path = os.path.join(out_dir, f'{abbr}.json')
        with open(out_path, 'w') as f:
            json.dump(result, f, indent=2)

    print(f'  Written 50 state files to {out_dir}/')

def export_ballot_measures(dry_run=False):
    """Export ballot_measures.json with all measures across 2024-2026."""
    print('Exporting ballot_measures.json...')

    q = """
        SELECT
            st.abbreviation as state,
            st.state_name,
            bm.election_year,
            bm.election_date,
            bm.measure_type,
            bm.measure_number,
            bm.short_title,
            bm.description,
            bm.subject_category,
            bm.sponsor_type,
            bm.status,
            bm.result,
            bm.votes_yes,
            bm.votes_no,
            bm.yes_percentage,
            bm.passage_threshold
        FROM ballot_measures bm
        JOIN states st ON bm.state_id = st.id
        ORDER BY bm.election_year DESC, st.abbreviation, bm.measure_number
    """

    if dry_run:
        print('  Would run 1 query and write ballot_measures.json')
        return

    rows = run_sql(q)

    measures = []
    for r in rows:
        measures.append({
            'state': r['state'],
            'state_name': r['state_name'],
            'year': r['election_year'],
            'date': str(r['election_date']) if r.get('election_date') else None,
            'type': r['measure_type'],
            'number': r['measure_number'],
            'title': r['short_title'],
            'description': r['description'],
            'category': r['subject_category'],
            'sponsor': r['sponsor_type'],
            'status': r['status'],
            'result': r['result'],
            'votes_yes': r['votes_yes'],
            'votes_no': r['votes_no'],
            'yes_pct': float(r['yes_percentage']) if r.get('yes_percentage') else None,
            'threshold': r['passage_threshold'],
        })

    # Summary counts
    by_year = {}
    for m in measures:
        by_year.setdefault(m['year'], {'total': 0, 'passed': 0, 'failed': 0, 'pending': 0})
        by_year[m['year']]['total'] += 1
        if m['result'] == 'Passed':
            by_year[m['year']]['passed'] += 1
        elif m['result'] == 'Failed':
            by_year[m['year']]['failed'] += 1
        elif m['result'] == 'Pending':
            by_year[m['year']]['pending'] += 1

    result = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'summary': by_year,
        'measures': measures,
    }

    out_path = os.path.join(SITE_DATA_DIR, 'ballot_measures.json')
    with open(out_path, 'w') as f:
        json.dump(result, f, indent=2)
    print(f'  Written {out_path} ({len(measures)} measures)')

def main():
    parser = argparse.ArgumentParser(description='Export site data from Supabase to JSON')
    parser.add_argument('--dry-run', action='store_true', help='Print what would be done')
    parser.add_argument('--state', type=str, help='Export single state detail (2-letter abbreviation)')
    parser.add_argument('--summary-only', action='store_true', help='Only export states_summary.json')
    parser.add_argument('--margins-only', action='store_true', help='Only export pres_margins.json')
    parser.add_argument('--states-only', action='store_true', help='Only export all 50 state detail JSONs')
    parser.add_argument('--measures-only', action='store_true', help='Only export ballot_measures.json')
    args = parser.parse_args()

    os.makedirs(SITE_DATA_DIR, exist_ok=True)
    os.makedirs(os.path.join(SITE_DATA_DIR, 'states'), exist_ok=True)

    if args.dry_run:
        print('DRY RUN MODE')

    if args.state:
        export_state_detail(args.state.upper(), dry_run=args.dry_run)
    elif args.summary_only:
        export_states_summary(dry_run=args.dry_run)
    elif args.margins_only:
        export_pres_margins(dry_run=args.dry_run)
    elif args.states_only:
        export_all_state_details(dry_run=args.dry_run)
    elif args.measures_only:
        export_ballot_measures(dry_run=args.dry_run)
    else:
        export_states_summary(dry_run=args.dry_run)
        export_pres_margins(dry_run=args.dry_run)
        export_all_state_details(dry_run=args.dry_run)
        export_ballot_measures(dry_run=args.dry_run)

    print('\nDone.')

if __name__ == '__main__':
    main()
