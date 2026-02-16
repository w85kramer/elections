#!/usr/bin/env python3
"""Export legislature data for site pages:
  - data/legislatures_data.json — chamber composition, history, elections, supermajorities
"""

import json
import os
import re
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


def parse_veto_threshold(veto_str, total_seats):
    """Parse veto override string to compute the number of seats needed."""
    if not veto_str or not total_seats:
        return None
    v = veto_str.lower()
    if 'majority elected' in v or '50%' in v:
        return total_seats // 2 + 1
    if '3/5' in v or '60%' in v:
        import math
        return math.ceil(total_seats * 3 / 5)
    if '2/3' in v or '66' in v:
        import math
        return math.ceil(total_seats * 2 / 3)
    return None


def export_legislatures():
    """Export all legislature data to a single JSON."""
    print('Exporting legislatures_data.json...')

    # Query 1: Current chamber composition (2025-01-01)
    q_current = """
    SELECT
      s.abbreviation as state,
      s.state_name,
      cc.chamber,
      cc.total_seats,
      cc.d_seats,
      cc.r_seats,
      cc.other_seats,
      cc.vacant_seats,
      cc.control_status,
      cc.majority_threshold,
      cc.forecast_rating,
      cc.presiding_officer,
      cc.presiding_officer_title,
      cc.presiding_officer_party,
      cc.coalition_desc,
      cc.notes
    FROM chamber_control cc
    JOIN states s ON s.id = cc.state_id
    WHERE cc.effective_date = '2025-01-01'
    ORDER BY s.abbreviation, cc.chamber
    """

    # Query 2: Historical snapshots (all chamber_control rows)
    q_history = """
    SELECT
      s.abbreviation as state,
      cc.chamber,
      cc.effective_date,
      cc.total_seats,
      cc.d_seats,
      cc.r_seats,
      cc.other_seats,
      cc.vacant_seats,
      cc.control_status
    FROM chamber_control cc
    JOIN states s ON s.id = cc.state_id
    ORDER BY cc.effective_date, s.abbreviation, cc.chamber
    """

    # Query 3: Supermajority thresholds
    q_super = """
    SELECT
      s.abbreviation as state,
      sm.chamber,
      sm.veto_override,
      sm.budget_passage,
      sm.taxes,
      sm.const_amend,
      sm.quorum,
      sm.other_circumstances,
      sm.const_authority,
      sm.notes
    FROM supermajority_thresholds sm
    JOIN states s ON s.id = sm.state_id
    ORDER BY s.abbreviation, sm.chamber
    """

    # Query 4: 2026 election summary per state-chamber
    q_elections = """
    SELECT
      s.abbreviation as state,
      d.chamber,
      COUNT(DISTINCT e.id) as election_count,
      COUNT(DISTINCT CASE WHEN e.election_type = 'General' THEN e.seat_id END) as seats_up,
      MIN(CASE WHEN e.election_type = 'General' THEN e.filing_deadline END) as filing_deadline,
      MIN(CASE WHEN e.election_type IN ('Primary_D','Primary_R','Primary') THEN e.election_date END) as primary_date,
      MIN(CASE WHEN e.election_type = 'General' THEN e.election_date END) as election_date
    FROM elections e
    JOIN seats se ON se.id = e.seat_id
    JOIN districts d ON d.id = se.district_id
    JOIN states s ON s.id = d.state_id
    WHERE d.office_level = 'Legislative'
      AND e.election_date >= '2026-01-01'
      AND e.election_date <= '2026-12-31'
    GROUP BY s.abbreviation, d.chamber
    ORDER BY s.abbreviation, d.chamber
    """

    print('  Querying current chamber composition...')
    current_raw = query_db(q_current)
    print(f'  Got {len(current_raw)} current chamber rows')

    print('  Querying historical snapshots...')
    history_raw = query_db(q_history)
    print(f'  Got {len(history_raw)} historical rows')

    print('  Querying supermajority thresholds...')
    super_raw = query_db(q_super)
    print(f'  Got {len(super_raw)} supermajority rows')

    print('  Querying 2026 elections...')
    elections_raw = query_db(q_elections)
    print(f'  Got {len(elections_raw)} election rows')

    # Build supermajority lookup: state-chamber -> data
    super_map = {}
    for row in super_raw:
        key = f"{row['state']}-{row['chamber']}"
        super_map[key] = {
            'veto_override': row['veto_override'],
            'budget_passage': row['budget_passage'],
            'taxes': row['taxes'],
            'const_amend': row['const_amend'],
            'quorum': row['quorum'],
            'other_circumstances': row['other_circumstances'],
            'const_authority': row['const_authority'],
            'notes': row['notes'],
        }

    # Build election lookup: state-chamber -> data
    elec_map = {}
    for row in elections_raw:
        key = f"{row['state']}-{row['chamber']}"
        elec_map[key] = {
            'seats_up': row['seats_up'] or 0,
            'filing_deadline': row['filing_deadline'],
            'primary_date': row['primary_date'],
            'election_date': row['election_date'],
        }

    # Build chambers list
    chambers = []
    total_seats_all = 0
    d_chambers = 0
    r_chambers = 0
    other_chambers = 0
    seats_up_2026 = 0

    for row in current_raw:
        state = row['state']
        chamber = row['chamber']
        key = f"{state}-{chamber}"
        total = row['total_seats'] or 0
        d = row['d_seats'] or 0
        r = row['r_seats'] or 0
        other = row['other_seats'] or 0
        vacant = row['vacant_seats'] or 0
        majority = row['majority_threshold'] or (total // 2 + 1)

        control = row['control_status']
        if control == 'D':
            d_chambers += 1
        elif control == 'R':
            r_chambers += 1
        else:
            other_chambers += 1

        # Compute margin (seats above majority for controlling party)
        if control == 'D':
            margin = d - majority
        elif control == 'R':
            margin = r - majority
        else:
            margin = 0

        # Supermajority info
        sm = super_map.get(key, {})
        veto_threshold = parse_veto_threshold(sm.get('veto_override'), total)

        # Check if controlling party holds veto-override supermajority
        has_supermajority = False
        if veto_threshold:
            if control == 'R' and r >= veto_threshold:
                has_supermajority = True
            elif control == 'D' and d >= veto_threshold:
                has_supermajority = True

        # Election data
        el = elec_map.get(key, {})
        s_up = el.get('seats_up', 0)
        seats_up_2026 += s_up
        total_seats_all += total

        chambers.append({
            'state': state,
            'state_name': row['state_name'],
            'chamber': chamber,
            'total_seats': total,
            'd_seats': d,
            'r_seats': r,
            'other_seats': other,
            'vacant_seats': vacant,
            'control_status': control,
            'majority_threshold': majority,
            'margin': margin,
            'seats_up_2026': s_up,
            'has_elections_2026': s_up > 0,
            'filing_deadline': el.get('filing_deadline'),
            'primary_date': el.get('primary_date'),
            'election_date': el.get('election_date'),
            'forecast_rating': row['forecast_rating'],
            'presiding_officer': row['presiding_officer'],
            'presiding_officer_title': row['presiding_officer_title'],
            'presiding_officer_party': row['presiding_officer_party'],
            'coalition_desc': row['coalition_desc'],
            'supermajority': sm if sm else None,
            'has_supermajority': has_supermajority,
            'veto_threshold': veto_threshold,
            'notes': row['notes'],
        })

    # ── Historical snapshots ──
    # Group by effective_date to compute aggregate stats
    snapshots_by_date = {}
    for row in history_raw:
        date = row['effective_date']
        if date not in snapshots_by_date:
            snapshots_by_date[date] = {
                'date': date,
                'd_chambers': 0, 'r_chambers': 0, 'other_chambers': 0,
                'd_total_seats': 0, 'r_total_seats': 0,
                'chambers': []
            }
        snap = snapshots_by_date[date]
        ctrl = row['control_status']
        if ctrl == 'D':
            snap['d_chambers'] += 1
        elif ctrl == 'R':
            snap['r_chambers'] += 1
        else:
            snap['other_chambers'] += 1
        snap['d_total_seats'] += (row['d_seats'] or 0)
        snap['r_total_seats'] += (row['r_seats'] or 0)
        snap['chambers'].append({
            'state': row['state'],
            'chamber': row['chamber'],
            'control_status': ctrl,
            'd_seats': row['d_seats'],
            'r_seats': row['r_seats'],
        })

    snapshots = []
    for date in sorted(snapshots_by_date.keys()):
        s = snapshots_by_date[date]
        snapshots.append({
            'date': s['date'],
            'd_chambers': s['d_chambers'],
            'r_chambers': s['r_chambers'],
            'other_chambers': s['other_chambers'],
            'd_total_seats': s['d_total_seats'],
            'r_total_seats': s['r_total_seats'],
        })

    # ── Chamber flips ──
    # Compare consecutive snapshots to find chambers that changed control
    flips = []
    dates = sorted(snapshots_by_date.keys())
    for i in range(1, len(dates)):
        prev_date = dates[i - 1]
        curr_date = dates[i]
        prev_chambers = {f"{c['state']}-{c['chamber']}": c['control_status']
                         for c in snapshots_by_date[prev_date]['chambers']}
        curr_chambers = {f"{c['state']}-{c['chamber']}": c
                         for c in snapshots_by_date[curr_date]['chambers']}

        for key, curr_c in curr_chambers.items():
            prev_ctrl = prev_chambers.get(key)
            curr_ctrl = curr_c['control_status']
            if prev_ctrl and curr_ctrl and prev_ctrl != curr_ctrl:
                # Only count D→R or R→D flips
                if prev_ctrl in ('D', 'R') and curr_ctrl in ('D', 'R'):
                    parts = key.split('-', 1)
                    # Derive cycle year from effective date
                    cycle_year = curr_date[:4]
                    flips.append({
                        'state': parts[0],
                        'chamber': parts[1],
                        'from': prev_ctrl,
                        'to': curr_ctrl,
                        'cycle': cycle_year,
                    })

    # ── Split legislatures ──
    # A state has a split legislature if Senate and House are controlled by different parties
    state_chambers = {}
    for c in chambers:
        state_chambers.setdefault(c['state'], {})[c['chamber']] = c['control_status']

    split_legislatures = 0
    for st, ch in state_chambers.items():
        if st == 'NE':
            continue  # unicameral
        controls = set(ch.values())
        if 'D' in controls and 'R' in controls:
            split_legislatures += 1

    chambers_with_elections = sum(1 for c in chambers if c['has_elections_2026'])

    data = {
        'generated_at': datetime.now().isoformat(),
        'summary': {
            'total_chambers': len(chambers),
            'total_seats': total_seats_all,
            'd_chambers': d_chambers,
            'r_chambers': r_chambers,
            'other_chambers': other_chambers,
            'split_legislatures': split_legislatures,
            'chambers_with_elections_2026': chambers_with_elections,
            'seats_up_2026': seats_up_2026,
        },
        'chambers': chambers,
        'history': {
            'snapshots': snapshots,
            'chamber_flips': flips,
        },
    }

    outpath = os.path.join(SITE_DIR, 'data', 'legislatures_data.json')
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with open(outpath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f'  Written: {outpath} ({len(chambers)} chambers, {len(snapshots)} snapshots, {len(flips)} flips)')


if __name__ == '__main__':
    export_legislatures()
    print('Done!')
