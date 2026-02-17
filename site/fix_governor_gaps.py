#!/usr/bin/env python3
"""Fix governor seat_term gaps by backdating start_dates to actual first
inauguration/succession dates. Also adds Jane Swift (MA acting gov 2001-2003).

The issue: populate_seat_terms_statewide.py recorded only the current term's
start date, missing earlier terms for governors serving 2+ terms or who
succeeded a predecessor mid-term.
"""

import json
import time
import requests

HEADERS = {
    'Authorization': f'Bearer {TOKEN}',
    'Content-Type': 'application/json'
}

DRY_RUN = False  # Set True to preview without changes

def query_db(sql, retries=5):
    for attempt in range(1, retries + 1):
        resp = requests.post(API_URL, headers=HEADERS, json={'query': sql})
        if resp.status_code in (200, 201):
            return resp.json()
        if resp.status_code == 429:
            wait = 5 * attempt
            print(f'  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'  Error {resp.status_code}: {resp.text[:200]}')
        resp.raise_for_status()
    raise Exception(f'Failed after {retries} retries')

# Correct start dates and reasons for current governors
# Format: state -> (correct_start_date, start_reason)
# start_reason: 'elected' for regular inauguration, 'succeeded' for mid-term succession
CORRECTIONS = {
    'AK': ('2018-12-03', 'elected'),       # Dunleavy won 2018 election
    'AL': ('2017-04-10', 'succeeded'),      # Ivey succeeded Bentley (resigned)
    'CA': ('2019-01-07', 'elected'),        # Newsom 1st term
    'CO': ('2019-01-08', 'elected'),        # Polis 1st term
    'CT': ('2019-01-09', 'elected'),        # Lamont 1st term
    'FL': ('2019-01-08', 'elected'),        # DeSantis 1st term
    'GA': ('2019-01-14', 'elected'),        # Kemp 1st term
    'IA': ('2017-05-24', 'succeeded'),      # Reynolds succeeded Branstad (ambassador)
    'ID': ('2019-01-07', 'elected'),        # Little 1st term
    'IL': ('2019-01-14', 'elected'),        # Pritzker 1st term
    'KS': ('2019-01-14', 'elected'),        # Kelly 1st term
    'KY': ('2019-12-10', 'elected'),        # Beshear 1st term
    'ME': ('2019-01-02', 'elected'),        # Mills 1st term
    'MI': ('2019-01-01', 'elected'),        # Whitmer 1st term
    'MN': ('2019-01-07', 'elected'),        # Walz 1st term
    'MS': ('2020-01-14', 'elected'),        # Reeves 1st term
    'MT': ('2021-01-04', 'elected'),        # Gianforte 1st term
    'NM': ('2019-01-01', 'elected'),        # Lujan Grisham 1st term
    'NY': ('2021-08-24', 'succeeded'),      # Hochul succeeded Cuomo (resigned)
    'OH': ('2019-01-14', 'elected'),        # DeWine 1st term
    'OK': ('2019-01-14', 'elected'),        # Stitt 1st term
    'RI': ('2021-03-02', 'succeeded'),      # McKee succeeded Raimondo (Commerce Sec)
    'SC': ('2017-01-24', 'succeeded'),      # McMaster succeeded Haley (UN Ambassador)
    'TN': ('2019-01-19', 'elected'),        # Lee 1st term
    'TX': ('2015-01-20', 'elected'),        # Abbott 1st term (now in 3rd)
    'UT': ('2021-01-04', 'elected'),        # Cox 1st term
    'VT': ('2017-01-05', 'elected'),        # Scott 1st term (now in 5th, 2yr terms)
    'WI': ('2019-01-07', 'elected'),        # Evers 1st term
    'WY': ('2019-01-07', 'elected'),        # Gordon 1st term
}

def fix_current_governor_starts():
    """Update start_date on current governor seat_terms."""
    print(f'Fixing {len(CORRECTIONS)} governor start dates...')

    for state, (correct_start, reason) in sorted(CORRECTIONS.items()):
        # Get current governor seat_term
        q = f"""
        SELECT st.id, st.start_date, st.start_reason, c.full_name
        FROM seat_terms st
        JOIN seats se ON se.id = st.seat_id
        JOIN districts d ON d.id = se.district_id
        JOIN states s ON s.id = d.state_id
        JOIN candidates c ON c.id = st.candidate_id
        WHERE se.office_type = 'Governor'
          AND s.abbreviation = '{state}'
          AND st.end_date IS NULL
        """
        rows = query_db(q)
        if not rows:
            print(f'  {state}: No current governor found, skipping')
            continue

        row = rows[0]
        old_start = row['start_date']
        term_id = row['id']
        name = row['full_name']

        if old_start == correct_start:
            print(f'  {state}: {name} already correct ({correct_start})')
            continue

        print(f'  {state}: {name} — {old_start} → {correct_start} (reason: {reason})')

        if not DRY_RUN:
            update_q = f"""
            UPDATE seat_terms
            SET start_date = '{correct_start}', start_reason = '{reason}'
            WHERE id = {term_id}
            """
            query_db(update_q)

    print()

def fix_ma_historical_gap():
    """Add Jane Swift as MA acting governor 2001-2003."""
    print('Checking MA historical gap (Jane Swift)...')

    # Check if she already exists
    check = query_db("SELECT id FROM candidates WHERE full_name = 'Jane Swift'")
    if check:
        print('  Jane Swift already in candidates table, checking seat_term...')
        cid = check[0]['id']
        st_check = query_db(f"SELECT id FROM seat_terms WHERE candidate_id = {cid}")
        if st_check:
            print('  Seat term already exists, skipping')
            return
    else:
        print('  Creating candidate: Jane Swift')
        if not DRY_RUN:
            query_db("""
            INSERT INTO candidates (full_name, first_name, last_name, gender)
            VALUES ('Jane Swift', 'Jane', 'Swift', 'F')
            """)
            check = query_db("SELECT id FROM candidates WHERE full_name = 'Jane Swift'")
            cid = check[0]['id']
        else:
            cid = '(new)'

    # Get MA governor seat_id
    seat = query_db("""
    SELECT se.id FROM seats se
    JOIN districts d ON d.id = se.district_id
    JOIN states s ON s.id = d.state_id
    WHERE se.office_type = 'Governor' AND s.abbreviation = 'MA'
    """)
    seat_id = seat[0]['id']

    print(f'  Creating seat_term: Jane Swift (R), 2001-04-10 to 2003-01-02, seat_id={seat_id}')
    if not DRY_RUN:
        query_db(f"""
        INSERT INTO seat_terms (seat_id, candidate_id, party, start_date, end_date, start_reason, end_reason)
        VALUES ({seat_id}, {cid}, 'R', '2001-04-10', '2003-01-02', 'succeeded', 'term_expired')
        """)
    print()

def verify():
    """Verify the fixes by checking yearly balance for 2018-2022."""
    print('Verifying yearly balance after fixes...')
    # Re-export and check
    q = """
    SELECT s.abbreviation, st.party, st.start_date, st.end_date
    FROM seat_terms st
    JOIN seats se ON se.id = st.seat_id
    JOIN districts d ON d.id = se.district_id
    JOIN states s ON s.id = d.state_id
    WHERE se.office_type = 'Governor'
    ORDER BY s.abbreviation, st.start_date
    """
    rows = query_db(q)

    for year in range(2017, 2027):
        ref = f'{year}-01-15'
        d, r, other = 0, 0, 0
        for t in rows:
            if t['start_date'] and t['start_date'] <= ref:
                if t['end_date'] is None or t['end_date'] >= ref:
                    if t['party'] == 'D': d += 1
                    elif t['party'] == 'R': r += 1
                    else: other += 1
        total = d + r + other
        flag = ' ✓' if total >= 49 else f' ✗ MISSING {50 - total}'
        print(f'  {year}: D={d}, R={r}, Other={other} (total={total}){flag}')

if __name__ == '__main__':
    import sys
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL
    if '--dry-run' in sys.argv:
        DRY_RUN = True
        print('=== DRY RUN (no changes) ===\n')

    fix_current_governor_starts()
    fix_ma_historical_gap()

    if not DRY_RUN:
        verify()

    print('Done!')
