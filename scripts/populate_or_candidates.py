#!/usr/bin/env python3
"""
Populate Oregon 2026 candidates from official ORESTAR Excel export.

Source: https://secure.sos.state.or.us/orestar/GotoSearchByName.do
File: ~/Downloads/XcelCFSearch.xls (old .xls format, requires xlrd)

Operations:
1. Mark DB candidates NOT in SoS list as Withdrawn_Pre_Ballot
2. Add new SoS candidates NOT already in DB

Usage:
    python3 scripts/populate_or_candidates.py --dry-run
    python3 scripts/populate_or_candidates.py
"""

import os
import re
import sys
import time
import xlrd
import requests

# Load env
from pathlib import Path
env_path = Path(__file__).parent.parent / '.env'
env = {}
with open(env_path) as f:
    for line in f:
        line = line.strip()
        if '=' in line and not line.startswith('#'):
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()

SUPABASE_TOKEN = env['SUPABASE_MANAGEMENT_TOKEN']
PROJECT_REF = 'pikcvwulzfxgwfcfssxc'
API_URL = f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query'

DRY_RUN = '--dry-run' in sys.argv


def run_sql(query, label=""):
    if DRY_RUN:
        print(f"[DRY RUN] {label}: {query[:200]}...")
        return None
    for attempt in range(1, 6):
        r = requests.post(API_URL,
            headers={'Authorization': f'Bearer {SUPABASE_TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query})
        if r.status_code in (200, 201):
            try:
                return r.json()
            except:
                return []
        if r.status_code == 429:
            wait = 5 * attempt
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        print(f"  ERROR ({r.status_code}): {r.text[:200]}")
        return None
    print(f"  Failed after 5 retries")
    return None


def run_sql_read(query, label=""):
    """For read queries (always execute, even in dry-run)"""
    for attempt in range(1, 6):
        r = requests.post(API_URL,
            headers={'Authorization': f'Bearer {SUPABASE_TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query})
        if r.status_code in (200, 201):
            try:
                return r.json()
            except:
                return []
        if r.status_code == 429:
            wait = 5 * attempt
            time.sleep(wait)
            continue
        print(f"  ERROR ({r.status_code}): {r.text[:200]}")
        return None
    return None


def esc(s):
    return s.replace("'", "''")


def last_name_key(name):
    """Normalize last name for matching: lowercase, strip hyphens, commas, suffixes."""
    n = name.lower().strip()
    n = n.replace(',', '').replace('-', ' ').replace('.', '')
    # Remove common suffixes
    for suf in [' jr', ' sr', ' ii', ' iii', ' iv']:
        if n.endswith(suf):
            n = n[:-len(suf)].strip()
    return n


def parse_xls():
    """Parse the ORESTAR XLS file."""
    xls_path = os.path.expanduser('~/Downloads/XcelCFSearch.xls')
    wb = xlrd.open_workbook(xls_path)
    ws = wb.sheet_by_index(0)

    candidates = []
    for row_idx in range(1, ws.nrows):  # Skip header row
        row = ws.row(row_idx)

        office_group = str(row[2].value).strip()  # Column 2: Office Group
        if office_group not in ('State Representative', 'State Senator'):
            continue

        # Column 4: Office (district number like "13th District")
        office = str(row[4].value).strip()
        dist_match = re.search(r'(\d+)', office)
        if not dist_match:
            continue
        district = dist_match.group(1)

        # Column 18: Qualified indicator
        qualified = str(row[18].value).strip()
        if qualified != 'Y':
            continue

        # Column 21: Withdrawal date (empty = active)
        withdraw_date = str(row[21].value).strip()
        if withdraw_date and withdraw_date != '':
            continue

        # Column 9: Party
        party_raw = str(row[9].value).strip()
        if party_raw == 'Republican':
            party = 'R'
        elif party_raw == 'Democrat':
            party = 'D'
        else:
            continue  # Skip other parties

        # Column 11: Ballot name
        ballot_name = str(row[11].value).strip()

        # Column 29: First Name, 31: Last Name, 32: Suffix
        first_name = str(row[29].value).strip()
        last_name = str(row[31].value).strip()
        suffix = str(row[32].value).strip() if row[32].value else ''

        # Build full name from first + last (+ suffix if present)
        if suffix:
            full_name = f"{first_name} {last_name} {suffix}"
        else:
            full_name = f"{first_name} {last_name}"

        # Map chamber
        chamber = 'House' if office_group == 'State Representative' else 'Senate'

        candidates.append({
            'full_name': full_name,
            'first_name': first_name,
            'last_name': last_name,
            'ballot_name': ballot_name,
            'party': party,
            'district': district,
            'chamber': chamber,
        })

    return candidates


def main():
    print(f"{'[DRY RUN] ' if DRY_RUN else ''}Oregon 2026 Candidate Import")
    print("=" * 60)

    # Parse SoS data
    sos = parse_xls()
    print(f"SoS candidates (D/R, qualified, active): {len(sos)}")
    senate = [s for s in sos if s['chamber'] == 'Senate']
    house = [s for s in sos if s['chamber'] == 'House']
    print(f"  Senate: {len(senate)}, House: {len(house)}")
    print(f"  By party: {sum(1 for c in sos if c['party']=='R')}R, {sum(1 for c in sos if c['party']=='D')}D")

    # Get election IDs for OR 2026
    elections = run_sql_read("""
        SELECT e.id, e.election_type, d.district_number, d.chamber
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'OR' AND e.election_year = 2026
        AND d.office_level = 'Legislative'
        AND e.election_type IN ('Primary_D', 'Primary_R')
    """, "Get OR elections")

    if not elections:
        print("ERROR: No OR 2026 elections found")
        return

    # Build election lookup: (chamber, district, type) -> election_id
    elec_lookup = {}
    for e in elections:
        key = (e['chamber'], e['district_number'], e['election_type'])
        elec_lookup[key] = e['id']

    print(f"Elections in DB: {len(elections)}")
    print(f"  Primary_R: {sum(1 for e in elections if e['election_type'] == 'Primary_R')}")
    print(f"  Primary_D: {sum(1 for e in elections if e['election_type'] == 'Primary_D')}")

    # Get existing DB candidacies
    db_cands = run_sql_read("""
        SELECT c.first_name, c.last_name, c.full_name, cy.party, cy.candidate_status,
               d.district_number, d.chamber, cy.id as cy_id, cy.candidate_id
        FROM candidacies cy
        JOIN candidates c ON cy.candidate_id = c.id
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'OR' AND e.election_year = 2026
        AND d.office_level = 'Legislative'
    """, "Get current DB candidacies")

    print(f"Existing DB candidacies: {len(db_cands)}")

    # Build SoS lookup for withdrawal detection
    sos_lookup = set()
    for c in sos:
        lk = last_name_key(c['last_name'])
        sos_lookup.add((c['chamber'], c['district'], c['party'], lk))

    # === Step 1: Mark withdrawals ===
    print(f"\n--- Step 1: Mark withdrawals (DB candidates NOT in SoS) ---")
    withdrawal_ids = []
    for d in db_cands:
        # Skip already-withdrawn candidates
        if d.get('candidate_status') == 'Withdrawn_Pre_Ballot':
            continue

        db_lk = last_name_key(d['last_name'])
        key = (d['chamber'], d['district_number'], d['party'], db_lk)

        # Check direct match
        if key in sos_lookup:
            continue

        # Check partial match (last name contained in SoS last name or vice versa)
        found = False
        for sk in sos_lookup:
            if sk[0] == d['chamber'] and sk[1] == d['district_number'] and sk[2] == d['party']:
                if db_lk in sk[3] or sk[3] in db_lk:
                    found = True
                    break
        if found:
            continue

        withdrawal_ids.append(d['cy_id'])
        print(f"  WITHDRAWN: {d['chamber']} {d['district_number']} {d['party']}: {d['first_name']} {d['last_name']} (cy_id={d['cy_id']})")

    if withdrawal_ids:
        ids_str = ','.join(str(i) for i in withdrawal_ids)
        run_sql(f"UPDATE candidacies SET candidate_status = 'Withdrawn_Pre_Ballot' WHERE id IN ({ids_str})", "Mark withdrawals")
        print(f"  Marked {len(withdrawal_ids)} as Withdrawn_Pre_Ballot")
    else:
        print("  None found")

    # === Step 2: Add new candidates ===
    print(f"\n--- Step 2: Add new candidates (SoS NOT in DB) ---")

    # Build existing set for dedup
    existing = set()
    for d in db_cands:
        lk = last_name_key(d['last_name'])
        existing.add((d['chamber'], d['district_number'], d['party'], lk))

    new_count = 0
    batch_sql = []

    for s in sos:
        lk = last_name_key(s['last_name'])
        key = (s['chamber'], s['district'], s['party'], lk)
        if key in existing:
            continue

        # Check partial match
        found = False
        for ex_key in existing:
            if ex_key[0] == s['chamber'] and ex_key[1] == s['district'] and ex_key[2] == s['party']:
                if ex_key[3] in lk or lk in ex_key[3]:
                    found = True
                    break
        if found:
            continue

        # Find election
        elec_type = f"Primary_{s['party']}"
        elec_key = (s['chamber'], s['district'], elec_type)
        elec_id = elec_lookup.get(elec_key)

        if not elec_id:
            print(f"  WARNING: No election for {s['chamber']} D-{s['district']} {elec_type} — skipping {s['full_name']}")
            continue

        first = esc(s['first_name'])
        lastname = esc(s['last_name'])
        fullname = esc(s['full_name'])

        sql = f"""
        WITH new_cand AS (
            INSERT INTO candidates (full_name, first_name, last_name)
            VALUES ('{fullname}', '{first}', '{lastname}')
            RETURNING id
        )
        INSERT INTO candidacies (candidate_id, election_id, party, candidate_status)
        SELECT id, {elec_id}, '{s['party']}', 'Filed'
        FROM new_cand;
        """
        batch_sql.append(sql)
        existing.add(key)
        new_count += 1

    print(f"  New candidates to add: {new_count}")

    if batch_sql and not DRY_RUN:
        # Execute in batches of 20
        batch_size = 20
        for i in range(0, len(batch_sql), batch_size):
            batch = batch_sql[i:i+batch_size]
            combined = '\n'.join(batch)
            result = run_sql(combined, f"Add batch {i//batch_size + 1}")
            if result is None:
                # Try one at a time
                for j, sql in enumerate(batch):
                    run_sql(sql, f"Add candidate {i+j+1}")
                    time.sleep(0.5)
            print(f"  Batch {i//batch_size + 1}/{(len(batch_sql)-1)//batch_size + 1} done ({len(batch)} candidates)")
            time.sleep(2)

    # === Summary ===
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"  Withdrawals marked: {len(withdrawal_ids)}")
    print(f"  New candidates added: {new_count}")
    if DRY_RUN:
        print(f"\n  [DRY RUN — no changes made]")


if __name__ == '__main__':
    main()
