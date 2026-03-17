#!/usr/bin/env python3
"""
Populate Maine 2026 primary candidates from official SoS Excel export.

Source: Maine Secretary of State 2026 Primary Candidate List
File: ~/Downloads/2026 Primary Candidate List-posting 3.16.xlsx
Single sheet "June 9, 2026", 896 rows including header.

Columns: Office, Dist, County, Party, Date Filed, Last Name, First Name,
         Middle Name, Suffix, Residence Municipality

Office codes: GV=Governor, SS=State Senator, SR=State Representative
Party: R or D

Operations:
1. Dry-run report: matched, new, and potentially withdrawn candidates
2. With --import: add new candidates and candidacies
3. With --withdraw: mark DB candidates not in SoS list as Withdrawn_Pre_Ballot

Usage:
    python3 scripts/populate_me_candidates.py                    # dry-run report
    python3 scripts/populate_me_candidates.py --import           # add new candidates
    python3 scripts/populate_me_candidates.py --withdraw         # mark withdrawals
    python3 scripts/populate_me_candidates.py --import --withdraw  # both
"""

import os
import re
import sys
import time
import openpyxl
import requests

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

DO_IMPORT = '--import' in sys.argv
DO_WITHDRAW = '--withdraw' in sys.argv


def run_sql(query, label=""):
    """Execute a write query (skipped unless --import or --withdraw)."""
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
            wait = 10 * attempt
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        print(f"  ERROR ({r.status_code}): {r.text[:200]}")
        return None
    print(f"  Failed after 5 retries")
    return None


def run_sql_read(query, label=""):
    """Execute a read query (always runs)."""
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
            wait = 10 * attempt
            print(f"  Rate limited, waiting {wait}s...")
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
    for suf in [' jr', ' sr', ' ii', ' iii', ' iv']:
        if n.endswith(suf):
            n = n[:-len(suf)].strip()
    return n


def build_full_name(first, middle, last, suffix):
    """Build full name from parts: First [Middle] Last [Suffix]."""
    parts = [first.strip()]
    if middle and middle.strip():
        mid = middle.strip()
        # Add period to single-letter middle initials
        if len(mid) == 1:
            mid = mid + '.'
        parts.append(mid)
    parts.append(last.strip())
    if suffix and suffix.strip():
        parts.append(suffix.strip())
    return ' '.join(parts)


def parse_xlsx():
    """Parse the Maine SoS Excel file."""
    xlsx_path = os.path.expanduser('~/Downloads/2026 Primary Candidate List-posting 3.16.xlsx')
    wb = openpyxl.load_workbook(xlsx_path)
    ws = wb.active

    candidates = []
    office_map = {'GV': 'Governor', 'GOV': 'Governor', 'SS': 'State Senate', 'SR': 'State House'}
    chamber_map = {'SS': 'Senate', 'SR': 'House', 'GV': 'Statewide', 'GOV': 'Statewide'}
    party_map = {'R': 'Republican', 'D': 'Democratic'}

    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or not row[0]:
            continue

        office_code = str(row[0]).strip()
        if office_code not in ('GV', 'GOV', 'SS', 'SR'):
            continue

        dist_raw = row[1]
        party_raw = str(row[3]).strip() if row[3] else ''
        filing_date = row[4]  # Date Filed
        last_name = str(row[5]).strip() if row[5] else ''
        first_name = str(row[6]).strip() if row[6] else ''
        middle_name = str(row[7]).strip() if row[7] else ''
        suffix = str(row[8]).strip() if row[8] else ''

        if not last_name or not first_name:
            continue

        # Map party
        if party_raw == 'R':
            party = 'R'
            party_full = 'Republican'
        elif party_raw == 'D':
            party = 'D'
            party_full = 'Democratic'
        else:
            continue  # Skip other parties

        # District number
        if office_code in ('SS', 'SR'):
            if dist_raw is not None:
                district = str(int(dist_raw)) if isinstance(dist_raw, (int, float)) else str(dist_raw).strip()
            else:
                print(f"  WARNING: No district for {office_code} candidate {first_name} {last_name}")
                continue
        else:
            district = None  # Governor is statewide

        # Build full name
        full_name = build_full_name(first_name, middle_name, last_name, suffix)

        # Format filing date
        filing_date_str = None
        if filing_date:
            if hasattr(filing_date, 'strftime'):
                filing_date_str = filing_date.strftime('%Y-%m-%d')

        candidates.append({
            'full_name': full_name,
            'first_name': first_name,
            'last_name': last_name,
            'middle_name': middle_name,
            'suffix': suffix,
            'party': party,
            'party_full': party_full,
            'district': district,
            'office_code': office_code,
            'office_type': office_map[office_code],
            'chamber': chamber_map[office_code],
            'filing_date': filing_date_str,
        })

    return candidates


def main():
    mode = []
    if DO_IMPORT:
        mode.append('IMPORT')
    if DO_WITHDRAW:
        mode.append('WITHDRAW')
    mode_str = ' + '.join(mode) if mode else 'DRY RUN'

    print(f"[{mode_str}] Maine 2026 Primary Candidate Import")
    print("=" * 60)

    # Parse SoS data
    sos = parse_xlsx()
    gov = [c for c in sos if c['office_code'] in ('GV', 'GOV')]
    senate = [c for c in sos if c['office_code'] == 'SS']
    house = [c for c in sos if c['office_code'] == 'SR']
    print(f"SoS candidates: {len(gov)} Governor + {len(senate)} Senate + {len(house)} House = {len(sos)} total")
    print(f"  By party: {sum(1 for c in sos if c['party']=='R')}R, {sum(1 for c in sos if c['party']=='D')}D")

    # === Get election IDs for ME 2026 (legislative) ===
    elections = run_sql_read("""
        SELECT e.id, e.election_type, d.district_number, d.chamber
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'ME' AND e.election_year = 2026
        AND s.office_level = 'Legislative'
        AND e.election_type IN ('Primary_D', 'Primary_R')
    """, "Get ME legislative elections")

    if not elections:
        print("ERROR: No ME 2026 legislative elections found")
        return

    elec_lookup = {}
    for e in elections:
        key = (e['chamber'], e['district_number'], e['election_type'])
        elec_lookup[key] = e['id']

    print(f"Legislative elections in DB: {len(elections)}")
    print(f"  Primary_R: {sum(1 for e in elections if e['election_type'] == 'Primary_R')}")
    print(f"  Primary_D: {sum(1 for e in elections if e['election_type'] == 'Primary_D')}")

    # === Get statewide (Governor) elections ===
    gov_elections = run_sql_read("""
        SELECT e.id, e.election_type, s.office_type
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'ME' AND e.election_year = 2026
        AND s.office_level = 'Statewide'
        AND e.election_type IN ('Primary_D', 'Primary_R')
    """, "Get ME statewide elections")

    gov_elec_lookup = {}
    if gov_elections:
        for e in gov_elections:
            key = (e['office_type'], e['election_type'])
            gov_elec_lookup[key] = e['id']
        print(f"Statewide elections in DB: {len(gov_elections)}")
    else:
        print("WARNING: No ME 2026 statewide elections found (Governor candidates will be skipped)")

    # === Get existing DB candidacies (legislative) ===
    db_leg_cands = run_sql_read("""
        SELECT c.first_name, c.last_name, c.full_name, cy.party, cy.candidate_status,
               d.district_number, d.chamber, cy.id as cy_id, cy.candidate_id,
               e.election_type
        FROM candidacies cy
        JOIN candidates c ON cy.candidate_id = c.id
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'ME' AND e.election_year = 2026
        AND s.office_level = 'Legislative'
        AND e.election_type IN ('Primary_D', 'Primary_R')
    """, "Get existing legislative candidacies")

    if not db_leg_cands:
        db_leg_cands = []
    print(f"Existing DB legislative candidacies: {len(db_leg_cands)}")

    # === Get existing DB candidacies (statewide) ===
    db_gov_cands = run_sql_read("""
        SELECT c.first_name, c.last_name, c.full_name, cy.party, cy.candidate_status,
               s.office_type, cy.id as cy_id, cy.candidate_id,
               e.election_type
        FROM candidacies cy
        JOIN candidates c ON cy.candidate_id = c.id
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'ME' AND e.election_year = 2026
        AND s.office_level = 'Statewide'
        AND e.election_type IN ('Primary_D', 'Primary_R')
    """, "Get existing statewide candidacies")

    if not db_gov_cands:
        db_gov_cands = []
    print(f"Existing DB statewide candidacies: {len(db_gov_cands)}")

    # ================================================================
    # LEGISLATIVE CANDIDATES
    # ================================================================
    leg_sos = [c for c in sos if c['office_code'] in ('SS', 'SR')]

    # Build SoS lookup for withdrawal detection
    sos_leg_lookup = set()
    for c in leg_sos:
        lk = last_name_key(c['last_name'])
        sos_leg_lookup.add((c['chamber'], c['district'], c['party'], lk))

    # --- Matching & Withdrawal Detection ---
    print(f"\n--- Legislative: Matching & Withdrawal Detection ---")
    matched_count = 0
    withdrawal_ids = []

    for d in db_leg_cands:
        # Skip already-withdrawn candidates
        if d.get('candidate_status') == 'Withdrawn_Pre_Ballot':
            continue

        db_lk = last_name_key(d['last_name'])
        # Map election_type to party letter
        db_party = 'R' if d['election_type'] == 'Primary_R' else 'D'
        key = (d['chamber'], d['district_number'], db_party, db_lk)

        # Check direct match
        if key in sos_leg_lookup:
            matched_count += 1
            continue

        # Check partial match (last name contained in SoS last name or vice versa)
        found = False
        for sk in sos_leg_lookup:
            if sk[0] == d['chamber'] and sk[1] == d['district_number'] and sk[2] == db_party:
                if len(db_lk) >= 3 and len(sk[3]) >= 3:
                    if db_lk in sk[3] or sk[3] in db_lk:
                        found = True
                        matched_count += 1
                        break
        if found:
            continue

        withdrawal_ids.append(d['cy_id'])
        print(f"  WITHDRAWN: {d['chamber']} {d['district_number']} {db_party}: "
              f"{d['full_name']} (cy_id={d['cy_id']})")

    print(f"  Matched: {matched_count}")
    print(f"  Potential withdrawals: {len(withdrawal_ids)}")

    # --- New Candidates ---
    print(f"\n--- Legislative: New Candidates ---")

    # Build existing set for dedup
    existing_leg = set()
    for d in db_leg_cands:
        lk = last_name_key(d['last_name'])
        db_party = 'R' if d['election_type'] == 'Primary_R' else 'D'
        existing_leg.add((d['chamber'], d['district_number'], db_party, lk))

    new_leg = []
    for s in leg_sos:
        lk = last_name_key(s['last_name'])
        key = (s['chamber'], s['district'], s['party'], lk)
        if key in existing_leg:
            continue

        # Check partial match
        found = False
        for ex_key in existing_leg:
            if ex_key[0] == s['chamber'] and ex_key[1] == s['district'] and ex_key[2] == s['party']:
                if len(ex_key[3]) >= 3 and len(lk) >= 3:
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
            print(f"  WARNING: No election for {s['chamber']} D-{s['district']} {elec_type} "
                  f"-- skipping {s['full_name']}")
            continue

        s['election_id'] = elec_id
        new_leg.append(s)
        existing_leg.add(key)
        print(f"  NEW: {s['chamber']} {s['district']} {s['party']}: {s['full_name']}")

    print(f"  New legislative candidates: {len(new_leg)}")

    # ================================================================
    # GOVERNOR CANDIDATES
    # ================================================================
    gov_sos = [c for c in sos if c['office_code'] in ('GV', 'GOV')]

    # Build SoS lookup for governor
    sos_gov_lookup = set()
    for c in gov_sos:
        lk = last_name_key(c['last_name'])
        sos_gov_lookup.add((c['party'], lk))

    print(f"\n--- Governor: Matching & Withdrawal Detection ---")
    gov_matched = 0
    gov_withdrawal_ids = []

    for d in db_gov_cands:
        if d.get('candidate_status') == 'Withdrawn_Pre_Ballot':
            continue

        db_lk = last_name_key(d['last_name'])
        db_party = 'R' if d['election_type'] == 'Primary_R' else 'D'
        key = (db_party, db_lk)

        if key in sos_gov_lookup:
            gov_matched += 1
            continue

        # Partial match
        found = False
        for sk in sos_gov_lookup:
            if sk[0] == db_party:
                if len(db_lk) >= 3 and len(sk[1]) >= 3:
                    if db_lk in sk[1] or sk[1] in db_lk:
                        found = True
                        gov_matched += 1
                        break
        if found:
            continue

        gov_withdrawal_ids.append(d['cy_id'])
        print(f"  WITHDRAWN: Governor {db_party}: {d['full_name']} (cy_id={d['cy_id']})")

    print(f"  Matched: {gov_matched}")
    print(f"  Potential withdrawals: {len(gov_withdrawal_ids)}")

    # New governor candidates
    print(f"\n--- Governor: New Candidates ---")
    existing_gov = set()
    for d in db_gov_cands:
        lk = last_name_key(d['last_name'])
        db_party = 'R' if d['election_type'] == 'Primary_R' else 'D'
        existing_gov.add((db_party, lk))

    new_gov = []
    for s in gov_sos:
        lk = last_name_key(s['last_name'])
        key = (s['party'], lk)
        if key in existing_gov:
            continue

        # Partial match
        found = False
        for ex_key in existing_gov:
            if ex_key[0] == s['party']:
                if len(ex_key[1]) >= 3 and len(lk) >= 3:
                    if ex_key[1] in lk or lk in ex_key[1]:
                        found = True
                        break
        if found:
            continue

        elec_type = f"Primary_{s['party']}"
        elec_key = ('Governor', elec_type)
        elec_id = gov_elec_lookup.get(elec_key)

        if not elec_id:
            print(f"  WARNING: No election for Governor {elec_type} -- skipping {s['full_name']}")
            continue

        s['election_id'] = elec_id
        new_gov.append(s)
        existing_gov.add(key)
        print(f"  NEW: Governor {s['party']}: {s['full_name']}")

    print(f"  New governor candidates: {len(new_gov)}")

    # ================================================================
    # EXECUTE CHANGES
    # ================================================================
    all_new = new_leg + new_gov
    all_withdrawal_ids = withdrawal_ids + gov_withdrawal_ids

    # --- Mark withdrawals ---
    if DO_WITHDRAW and all_withdrawal_ids:
        print(f"\n--- Marking {len(all_withdrawal_ids)} withdrawals ---")
        ids_str = ','.join(str(i) for i in all_withdrawal_ids)
        run_sql(f"UPDATE candidacies SET candidate_status = 'Withdrawn_Pre_Ballot' "
                f"WHERE id IN ({ids_str})", "Mark withdrawals")
        print(f"  Done.")
    elif all_withdrawal_ids:
        print(f"\n  [{len(all_withdrawal_ids)} withdrawals found — use --withdraw to apply]")

    # --- Add new candidates ---
    if DO_IMPORT and all_new:
        print(f"\n--- Adding {len(all_new)} new candidates ---")
        batch_sql = []
        for s in all_new:
            first = esc(s['first_name'])
            lastname = esc(s['last_name'])
            fullname = esc(s['full_name'])
            elec_id = s['election_id']

            filing_clause = ""
            if s.get('filing_date'):
                filing_clause = f", filing_date = '{s['filing_date']}'"

            sql = f"""
            WITH new_cand AS (
                INSERT INTO candidates (full_name, first_name, last_name)
                VALUES ('{fullname}', '{first}', '{lastname}')
                RETURNING id
            )
            INSERT INTO candidacies (candidate_id, election_id, party, candidate_status, filing_date)
            SELECT id, {elec_id}, '{s['party_full']}', 'Filed',
                   {("'" + s['filing_date'] + "'") if s.get('filing_date') else 'NULL'}
            FROM new_cand;
            """
            batch_sql.append(sql)

        # Execute in batches of 20
        batch_size = 20
        for i in range(0, len(batch_sql), batch_size):
            batch = batch_sql[i:i+batch_size]
            combined = '\n'.join(batch)
            result = run_sql(combined, f"Add batch {i//batch_size + 1}")
            if result is None:
                # Try one at a time on failure
                for j, sql in enumerate(batch):
                    run_sql(sql, f"Add candidate {i+j+1}")
                    time.sleep(0.5)
            print(f"  Batch {i//batch_size + 1}/{(len(batch_sql)-1)//batch_size + 1} "
                  f"done ({len(batch)} candidates)")
            time.sleep(2)
    elif all_new:
        print(f"\n  [{len(all_new)} new candidates found — use --import to add]")

    # ================================================================
    # SUMMARY
    # ================================================================
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"  SoS candidates parsed: {len(sos)} ({len(gov)} GV, {len(senate)} SS, {len(house)} SR)")
    print(f"  DB candidacies found: {len(db_leg_cands)} legislative + {len(db_gov_cands)} statewide")
    print(f"  Matched: {matched_count} legislative + {gov_matched} statewide")
    print(f"  New candidates: {len(new_leg)} legislative + {len(new_gov)} statewide = {len(all_new)}")
    print(f"  Potential withdrawals: {len(withdrawal_ids)} legislative + {len(gov_withdrawal_ids)} statewide = {len(all_withdrawal_ids)}")
    if not DO_IMPORT and not DO_WITHDRAW:
        print(f"\n  [DRY RUN — no changes made]")
        print(f"  Use --import to add new candidates")
        print(f"  Use --withdraw to mark withdrawals")
    elif DO_IMPORT and not DO_WITHDRAW and all_withdrawal_ids:
        print(f"\n  [Candidates imported but withdrawals NOT marked — use --withdraw to apply]")
    elif DO_WITHDRAW and not DO_IMPORT and all_new:
        print(f"\n  [Withdrawals marked but new candidates NOT added — use --import to add]")


if __name__ == '__main__':
    main()
