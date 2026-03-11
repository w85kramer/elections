#!/usr/bin/env python3
"""
Populate Pennsylvania 2026 candidates from official PA Department of State
Excel exports.

Source files:
  ~/Downloads/Election Info (1).xlsx — House candidates (~378 rows)
  ~/Downloads/Election Info (2).xlsx — Senate candidates (~59 rows)

Operations:
1. Find DB candidates NOT in SoS filing list → mark as Withdrawn_Pre_Ballot
2. Find SoS candidates NOT in DB → INSERT with CTE (new candidate + candidacy)

Usage:
    python3 scripts/populate_pa_candidates.py --dry-run
    python3 scripts/populate_pa_candidates.py
"""

import os
import re
import sys
import time
import requests

try:
    import openpyxl
except ImportError:
    print("ERROR: openpyxl required. Install with: pip install openpyxl")
    sys.exit(1)

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


def parse_excel_files():
    """Parse both PA SoS Excel files (House + Senate)."""
    files = [
        os.path.expanduser('~/Downloads/Election Info (1).xlsx'),
        os.path.expanduser('~/Downloads/Election Info (2).xlsx'),
    ]

    candidates = []
    for filepath in files:
        wb = openpyxl.load_workbook(filepath, read_only=True)
        ws = wb.active

        # Row 1 = "Election Info" title, Row 2 = column headers, Row 3+ = data
        for row in ws.iter_rows(min_row=3, values_only=True):
            name_raw = row[0]
            office = row[1]
            district_name = row[2]
            party_raw = row[3]

            # Skip empty rows
            if not name_raw or not office:
                continue

            # Map party
            if party_raw == 'Democratic':
                party = 'D'
            elif party_raw == 'Republican':
                party = 'R'
            else:
                continue  # Skip other parties

            # Map chamber from office
            if 'REPRESENTATIVE' in office.upper():
                chamber = 'House'
            elif 'SENATOR' in office.upper():
                chamber = 'Senate'
            else:
                continue

            # Extract district number from "19th Legislative District" or "20th Senatorial District"
            match = re.match(r'(\d+)', district_name)
            if not match:
                print(f"  WARNING: Could not parse district from '{district_name}'")
                continue
            district = match.group(1)

            # Parse name: "LAST, FIRST MIDDLE" (ALL CAPS)
            if ',' in name_raw:
                parts = name_raw.split(',', 1)
                last_name = parts[0].strip().title()
                first_name = parts[1].strip().title()
            else:
                # Fallback if no comma
                name_parts = name_raw.strip().title().split()
                first_name = name_parts[0] if name_parts else ''
                last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''

            # Full name in "First Last" format
            full_name = f"{first_name} {last_name}".strip()

            candidates.append({
                'full_name': full_name,
                'first_name': first_name,
                'last_name': last_name,
                'party': party,
                'district': district,
                'chamber': chamber,
            })

        wb.close()

    return candidates


def main():
    print(f"{'[DRY RUN] ' if DRY_RUN else ''}Pennsylvania 2026 Candidate Import")
    print("=" * 60)

    sos = parse_excel_files()
    print(f"SoS candidates (D/R): {len(sos)}")
    senate = [s for s in sos if s['chamber'] == 'Senate']
    house = [s for s in sos if s['chamber'] == 'House']
    print(f"  Senate: {len(senate)}, House: {len(house)}")

    # Get election IDs for PA 2026
    elections = run_sql_read("""
        SELECT e.id, e.election_type, d.district_number, d.chamber
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'PA' AND e.election_year = 2026
        AND s.office_level = 'Legislative'
        AND e.election_type IN ('Primary_D', 'Primary_R')
    """, "Get PA elections")

    if not elections:
        print("ERROR: No PA 2026 elections found")
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
        SELECT c.first_name, c.last_name, cy.party, d.district_number, d.chamber,
               cy.id as cy_id, cy.candidate_status
        FROM candidacies cy
        JOIN candidates c ON cy.candidate_id = c.id
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'PA' AND e.election_year = 2026
        AND s.office_level = 'Legislative'
    """, "Get current DB candidacies")

    if db_cands is None:
        print("ERROR: Could not fetch DB candidacies")
        return

    print(f"Existing DB candidacies: {len(db_cands)}")

    # Build lookup structures
    # DB candidates keyed by (chamber, district, party, last_name_key)
    db_lookup = {}
    for r in db_cands:
        key = (r['chamber'], r['district_number'], r['party'], last_name_key(r['last_name']))
        db_lookup[key] = r

    # SoS candidates keyed the same way
    sos_lookup = set()
    for s in sos:
        key = (s['chamber'], s['district'], s['party'], last_name_key(s['last_name']))
        sos_lookup.add(key)

    # === Step 1: Find DB candidates NOT in SoS → mark as Withdrawn_Pre_Ballot ===
    print(f"\n--- Step 1: Check for withdrawals ---")
    withdrawal_ids = []
    for r in db_cands:
        # Skip candidates already marked as withdrawn
        if r.get('candidate_status') in ('Withdrawn_Pre_Ballot', 'Withdrawn_Post_Ballot'):
            continue
        key = (r['chamber'], r['district_number'], r['party'], last_name_key(r['last_name']))
        if key not in sos_lookup:
            # Check partial matches (multi-word last names)
            found = False
            db_lnk = last_name_key(r['last_name'])
            for s_key in sos_lookup:
                if s_key[0] == r['chamber'] and s_key[1] == r['district_number'] and s_key[2] == r['party']:
                    if s_key[3] in db_lnk or db_lnk in s_key[3]:
                        found = True
                        break
            if not found:
                withdrawal_ids.append(r['cy_id'])
                print(f"  Withdrawal: {r['first_name']} {r['last_name']} ({r['party']}) "
                      f"{r['chamber']} D-{r['district_number']} (cy_id={r['cy_id']})")

    print(f"  Total withdrawals to mark: {len(withdrawal_ids)}")
    if withdrawal_ids:
        ids_str = ','.join(str(i) for i in withdrawal_ids)
        run_sql(f"UPDATE candidacies SET candidate_status = 'Withdrawn_Pre_Ballot' WHERE id IN ({ids_str})",
                "Mark withdrawals")

    # === Step 2: Find SoS candidates NOT in DB → INSERT ===
    print(f"\n--- Step 2: Add new candidates ---")

    # Build set of existing candidates for dedup
    existing = set()
    for r in db_cands:
        key = (r['chamber'], r['district_number'], r['party'], last_name_key(r['last_name']))
        existing.add(key)

    new_count = 0
    batch_sql = []

    for s in sos:
        lnk = last_name_key(s['last_name'])
        key = (s['chamber'], s['district'], s['party'], lnk)
        if key in existing:
            continue

        # Check partial matches (multi-word last names, etc.)
        found = False
        for ex_key in existing:
            if ex_key[0] == s['chamber'] and ex_key[1] == s['district'] and ex_key[2] == s['party']:
                if ex_key[3] in lnk or lnk in ex_key[3]:
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
                  f"— skipping {s['full_name']}")
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
        print(f"  NEW: {s['full_name']} ({s['party']}) {s['chamber']} D-{s['district']}")

    print(f"\n  New candidates to add: {new_count}")

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
    print(f"  SoS candidates parsed: {len(sos)}")
    print(f"  Withdrawals marked: {len(withdrawal_ids)}")
    print(f"  New candidates added: {new_count}")
    if DRY_RUN:
        print(f"\n  [DRY RUN — no changes made]")


if __name__ == '__main__':
    main()
