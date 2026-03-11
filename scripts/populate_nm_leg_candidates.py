#!/usr/bin/env python3
"""
Populate New Mexico 2026 legislative candidates from official SERVIS portal CSV export.

Source: https://servis.sos.nm.gov/ → GridViewExport.csv
File: ~/Downloads/GridViewExport.csv

Filters to State Representative and State Senator contests only.
- 125 State Representative candidates (70 House districts, all up in 2026)
- 2 State Senator candidates (SD-33 only, likely a special election)

Note: Some rows have Party=" II" or " JR" — these are data errors where a name
suffix (e.g., "HALL, II") leaked into the Party column due to CSV comma splitting.
These rows are skipped.

Usage:
    python3 scripts/populate_nm_leg_candidates.py --dry-run
    python3 scripts/populate_nm_leg_candidates.py
"""

import csv
import json
import os
import sys
import time
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


def parse_csv():
    """Parse the NM SERVIS portal CSV file"""
    csv_path = os.path.expanduser('~/Downloads/GridViewExport.csv')
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    candidates = []
    skipped_party = 0
    skipped_status = 0

    for r in rows:
        contest = r['Contest']
        if contest not in ('State Representative', 'State Senator'):
            continue

        # Map party: DEM → D, REP → R; skip anything else (catches " II", " JR" errors)
        party_raw = r['Party'].strip()
        if party_raw == 'DEM':
            party = 'D'
        elif party_raw == 'REP':
            party = 'R'
        else:
            skipped_party += 1
            print(f"  Skipping bad party: {r['First Name']} {r['Last Name']} Party=\"{r['Party']}\" ({contest} {r['District']})")
            continue

        # Skip disqualified candidates
        status = r['Status'].strip()
        if status == 'Disqualified':
            skipped_status += 1
            continue

        # Extract district number from "DISTRICT N"
        dist_str = r['District'].strip()
        dist = dist_str.replace('DISTRICT ', '').strip()

        # Map chamber
        if contest == 'State Representative':
            chamber = 'House'
        else:
            chamber = 'Senate'

        # Convert names from ALL CAPS to title case
        first_name = r['First Name'].strip().title()
        middle_name = r['Middle Name'].strip().title() if r['Middle Name'].strip() else ''
        last_name = r['Last Name'].strip().title()

        # Build full name
        if middle_name:
            full_name = f"{first_name} {middle_name} {last_name}"
        else:
            full_name = f"{first_name} {last_name}"

        candidates.append({
            'full_name': full_name,
            'first_name': first_name,
            'last_name': last_name,
            'party': party,
            'district': dist,
            'chamber': chamber,
            'status': status,
        })

    if skipped_party:
        print(f"  Skipped {skipped_party} rows with bad party values")
    if skipped_status:
        print(f"  Skipped {skipped_status} disqualified candidates")

    return candidates


def main():
    print(f"{'[DRY RUN] ' if DRY_RUN else ''}New Mexico 2026 Legislative Candidate Import")
    print("=" * 60)

    sos = parse_csv()
    print(f"\nSoS candidates (D/R, valid status): {len(sos)}")
    senate = [s for s in sos if s['chamber'] == 'Senate']
    house = [s for s in sos if s['chamber'] == 'House']
    print(f"  Senate: {len(senate)}, House: {len(house)}")

    # Get election IDs for NM 2026 legislative races
    elections = run_sql_read("""
        SELECT e.id, e.election_type, d.district_number, d.chamber
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NM' AND e.election_year = 2026
        AND s.office_level = 'Legislative'
        AND e.election_type IN ('Primary_D', 'Primary_R')
    """, "Get NM elections")

    if not elections:
        print("ERROR: No NM 2026 elections found")
        return

    # Build election lookup: (chamber, district, type) -> election_id
    elec_lookup = {}
    for e in elections:
        key = (e['chamber'], e['district_number'], e['election_type'])
        elec_lookup[key] = e['id']

    print(f"\nElections in DB: {len(elections)}")
    print(f"  Primary_R: {sum(1 for e in elections if e['election_type'] == 'Primary_R')}")
    print(f"  Primary_D: {sum(1 for e in elections if e['election_type'] == 'Primary_D')}")

    # Check if SD-33 elections exist (special case — may be a special election)
    sd33_keys = [k for k in elec_lookup if k[0] == 'Senate' and k[1] == '33']
    if sd33_keys:
        print(f"\n  SD-33 elections found in DB: {sd33_keys}")
    else:
        print(f"\n  NOTE: No SD-33 Senate elections found in DB. Skipping 2 Senate candidates.")
        print(f"         (SD-33 may need a special election created separately)")

    # Get existing DB candidacies for dedup (NM already has ~11 House candidacies from Ballotpedia)
    db_cands = run_sql_read("""
        SELECT c.first_name, c.last_name, cy.party, d.district_number, d.chamber,
               cy.id as cy_id, cy.candidate_status
        FROM candidacies cy
        JOIN candidates c ON cy.candidate_id = c.id
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NM' AND e.election_year = 2026
        AND s.office_level = 'Legislative'
    """, "Get current DB candidacies")

    if db_cands is None:
        db_cands = []

    print(f"\nExisting DB candidacies: {len(db_cands)}")

    # Build set of existing candidates for dedup
    existing = set()
    for r in db_cands:
        key = (r['chamber'], r['district_number'], r['party'], last_name_key(r['last_name']))
        existing.add(key)

    # === Step 1: Mark withdrawn candidates ===
    # DB candidates not in SoS list should be marked withdrawn
    # Build SoS lookup for comparison
    sos_lookup = set()
    for s in sos:
        key = (s['chamber'], s['district'], s['party'], last_name_key(s['last_name']))
        sos_lookup.add(key)

    withdrawn_ids = []
    for r in db_cands:
        if r['candidate_status'] in ('Withdrawn_Pre_Ballot', 'Withdrawn_Post_Ballot'):
            continue  # Already withdrawn
        db_key = (r['chamber'], r['district_number'], r['party'], last_name_key(r['last_name']))
        if db_key not in sos_lookup:
            # Check partial match before marking withdrawn
            found = False
            for sk in sos_lookup:
                if sk[0] == db_key[0] and sk[1] == db_key[1] and sk[2] == db_key[2]:
                    if sk[3] in db_key[3] or db_key[3] in sk[3]:
                        found = True
                        break
            if not found:
                withdrawn_ids.append((r['cy_id'], r['first_name'], r['last_name'],
                                      r['chamber'], r['district_number'], r['party']))

    print(f"\n--- Step 1: Mark withdrawn candidates ---")
    if withdrawn_ids:
        print(f"  Candidates in DB but NOT in SoS filing list:")
        for cy_id, first, last, ch, dist, party in withdrawn_ids:
            print(f"    {first} {last} ({party}) — {ch} D-{dist} [cy_id={cy_id}]")
        ids_str = ','.join(str(w[0]) for w in withdrawn_ids)
        run_sql(f"UPDATE candidacies SET candidate_status = 'Withdrawn_Pre_Ballot' WHERE id IN ({ids_str})",
                "Mark withdrawals")
        print(f"  Marked {len(withdrawn_ids)} as Withdrawn_Pre_Ballot")
    else:
        print(f"  No withdrawals to mark")

    # === Step 2: Add new candidates ===
    print(f"\n--- Step 2: Add new candidates ---")
    new_count = 0
    skipped_no_election = 0
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
                if len(ex_key[3]) >= 3 and len(lnk) >= 3:
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
            skipped_no_election += 1
            if s['chamber'] == 'Senate' and s['district'] == '33':
                pass  # Already noted above
            else:
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
    if skipped_no_election:
        print(f"  Skipped (no election in DB): {skipped_no_election}")

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
    print(f"  Withdrawals marked: {len(withdrawn_ids)}")
    print(f"  New candidates added: {new_count}")
    print(f"  Skipped (no election): {skipped_no_election}")
    if DRY_RUN:
        print(f"\n  [DRY RUN — no changes made]")


if __name__ == '__main__':
    main()
