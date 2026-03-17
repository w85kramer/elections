#!/usr/bin/env python3
"""
Populate Nevada 2026 candidates from official SoS CSV export.

Source: https://www.nvsos.gov/sos/elections/election-information/2026-elections
File: ~/Downloads/CandidateList (1).csv

Operations:
1. Report matched, new, and withdrawn candidates
2. With --import: add new candidates and candidacies
3. With --withdraw: mark DB candidacies not in SoS list as Withdrawn_Pre_Ballot

Usage:
    python3 scripts/populate_nv_candidates.py                  # dry-run report
    python3 scripts/populate_nv_candidates.py --import         # add new candidates
    python3 scripts/populate_nv_candidates.py --withdraw       # mark withdrawals
    python3 scripts/populate_nv_candidates.py --import --withdraw  # both
"""

import csv
import os
import re
import sys
import time
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
    """Execute a write query. Skipped unless --import or --withdraw is set."""
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
    """For read queries (always execute)."""
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


# Office Sought -> (office_type, chamber, office_level)
OFFICE_MAP = {
    'Governor': ('Governor', 'Statewide', 'Statewide'),
    'Lieutenant Governor': ('Lt. Governor', 'Statewide', 'Statewide'),
    'Attorney General': ('Attorney General', 'Statewide', 'Statewide'),
    'State Controller': ('Controller', 'Statewide', 'Statewide'),
    'State Treasurer': ('Treasurer', 'Statewide', 'Statewide'),
    'Secretary of State': ('Secretary of State', 'Statewide', 'Statewide'),
}

# Party mapping
PARTY_MAP = {
    'Republican Party': 'R',
    'Democratic Party': 'D',
    'Libertarian Party of Nevada': 'L',
    'Independent American Party': 'IAP',
    'No Political Party': 'NP',
}


def parse_csv():
    """Parse the NV SoS CSV file. Returns list of candidate dicts."""
    csv_path = os.path.expanduser('~/Downloads/CandidateList (1).csv')
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))

    candidates = []
    for r in rows:
        office = r['Office Sought'].strip()
        party_raw = r['Party or Nonpartisan Office'].strip()

        # Map party
        party = PARTY_MAP.get(party_raw)
        if not party:
            continue  # Skip unknown parties / nonpartisan offices

        # Check for statewide offices
        if office in OFFICE_MAP:
            office_type, chamber, office_level = OFFICE_MAP[office]
            district = None
        else:
            # Check for legislative districts
            m = re.match(r'State Assembly, District (\d+)', office)
            if m:
                office_type = 'State House'
                chamber = 'Assembly'
                office_level = 'Legislative'
                district = m.group(1)
            else:
                m = re.match(r'State Senate, District (\d+)', office)
                if m:
                    office_type = 'State Senate'
                    chamber = 'Senate'
                    office_level = 'Legislative'
                    district = m.group(1)
                else:
                    continue  # Not a state-level office we care about

        # Parse name from "Candidate Name" column (full name, normal order)
        full_name = r['Candidate Name'].strip()
        # Parse ballot name "Last, First" or "Last, First Middle" for last name extraction
        ballot_name = r['Name to Appear on Ballot'].strip()

        # Extract first/last from full_name
        parts = full_name.split()
        if len(parts) == 1:
            first_name = parts[0]
            last_name_val = ''
        else:
            first_name = parts[0]
            # Use ballot name to get last name (before the comma)
            comma_idx = ballot_name.find(',')
            if comma_idx > 0:
                last_name_val = ballot_name[:comma_idx].strip()
                # Clean up quotes from ballot name
                last_name_val = last_name_val.strip('"')
            else:
                last_name_val = parts[-1]

        # Parse filing date (format: "3/5/2026")
        filed_date_raw = r['Filed Date'].strip()
        if filed_date_raw:
            date_parts = filed_date_raw.split('/')
            if len(date_parts) == 3:
                filing_date = f"{date_parts[2]}-{int(date_parts[0]):02d}-{int(date_parts[1]):02d}"
            else:
                filing_date = None
        else:
            filing_date = None

        candidates.append({
            'full_name': full_name,
            'first_name': first_name,
            'last_name': last_name_val,
            'party': party,
            'office_type': office_type,
            'chamber': chamber,
            'office_level': office_level,
            'district': district,
            'filing_date': filing_date,
            'filed_status': r['Filed Status'].strip(),
        })

    return candidates


def main():
    mode = []
    if DO_IMPORT:
        mode.append('IMPORT')
    if DO_WITHDRAW:
        mode.append('WITHDRAW')
    mode_str = ' + '.join(mode) if mode else 'REPORT ONLY'

    print(f"[{mode_str}] Nevada 2026 Candidate Import")
    print("=" * 60)

    # Parse SoS data
    all_sos = parse_csv()
    print(f"SoS candidates (state-level): {len(all_sos)}")

    # Separate legislative and statewide
    leg_sos = [c for c in all_sos if c['office_level'] == 'Legislative']
    sw_sos = [c for c in all_sos if c['office_level'] == 'Statewide']
    print(f"  Legislative: {len(leg_sos)} (Assembly: {sum(1 for c in leg_sos if c['chamber']=='Assembly')}, Senate: {sum(1 for c in leg_sos if c['chamber']=='Senate')})")
    print(f"  Statewide: {len(sw_sos)}")

    # Party breakdown
    for label, group in [('Legislative', leg_sos), ('Statewide', sw_sos)]:
        by_party = {}
        for c in group:
            by_party[c['party']] = by_party.get(c['party'], 0) + 1
        party_str = ', '.join(f"{v}{k}" for k, v in sorted(by_party.items()))
        print(f"  {label} by party: {party_str}")

    # For primaries, filter to D and R only
    leg_primary = [c for c in leg_sos if c['party'] in ('D', 'R')]
    sw_primary = [c for c in sw_sos if c['party'] in ('D', 'R')]
    # Third parties go to general election only
    leg_general = [c for c in leg_sos if c['party'] not in ('D', 'R')]
    sw_general = [c for c in sw_sos if c['party'] not in ('D', 'R')]

    print(f"\n  Primary candidates (D/R): {len(leg_primary)} legislative + {len(sw_primary)} statewide")
    print(f"  General-only candidates (3rd party): {len(leg_general)} legislative + {len(sw_general)} statewide")

    # ========================================
    # Get DB elections for NV 2026
    # ========================================

    # Legislative elections
    leg_elections = run_sql_read("""
        SELECT e.id, e.election_type, d.district_number, d.chamber, s.office_type
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NV' AND e.election_year = 2026
        AND d.office_level = 'Legislative'
        AND e.election_type IN ('Primary_D', 'Primary_R', 'General')
    """, "Get NV legislative elections")

    if not leg_elections:
        print("ERROR: No NV 2026 legislative elections found")
        return

    # Build election lookup: (chamber, district, election_type) -> election_id
    leg_elec_lookup = {}
    for e in leg_elections:
        key = (e['chamber'], e['district_number'], e['election_type'])
        leg_elec_lookup[key] = e['id']

    primary_leg = [e for e in leg_elections if e['election_type'].startswith('Primary')]
    general_leg = [e for e in leg_elections if e['election_type'] == 'General']
    print(f"\nDB legislative elections: {len(leg_elections)} ({len(primary_leg)} primary, {len(general_leg)} general)")

    # Statewide elections
    sw_elections = run_sql_read("""
        SELECT e.id, e.election_type, s.office_type
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NV' AND e.election_year = 2026
        AND d.office_level = 'Statewide'
        AND e.election_type IN ('Primary_D', 'Primary_R', 'General')
    """, "Get NV statewide elections")

    sw_elec_lookup = {}
    if sw_elections:
        for e in sw_elections:
            key = (e['office_type'], e['election_type'])
            sw_elec_lookup[key] = e['id']
    print(f"DB statewide elections: {len(sw_elections or [])}")

    # ========================================
    # Get existing DB candidacies
    # ========================================

    db_leg_cands = run_sql_read("""
        SELECT cy.id as cy_id, cy.candidate_id, c.full_name, c.first_name, c.last_name,
               cy.party, cy.candidate_status, cy.filing_date,
               d.district_number, d.chamber, e.election_type, s.office_type
        FROM candidacies cy
        JOIN candidates c ON cy.candidate_id = c.id
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NV' AND e.election_year = 2026
        AND d.office_level = 'Legislative'
    """, "Get existing legislative candidacies")

    db_sw_cands = run_sql_read("""
        SELECT cy.id as cy_id, cy.candidate_id, c.full_name, c.first_name, c.last_name,
               cy.party, cy.candidate_status, cy.filing_date,
               d.district_number, d.chamber, e.election_type, s.office_type
        FROM candidacies cy
        JOIN candidates c ON cy.candidate_id = c.id
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NV' AND e.election_year = 2026
        AND d.office_level = 'Statewide'
    """, "Get existing statewide candidacies")

    if not db_leg_cands:
        db_leg_cands = []
    if not db_sw_cands:
        db_sw_cands = []

    print(f"Existing DB candidacies: {len(db_leg_cands)} legislative + {len(db_sw_cands)} statewide")

    # ========================================
    # Process legislative primary candidates (D/R)
    # ========================================
    print(f"\n{'='*60}")
    print("LEGISLATIVE PRIMARY CANDIDATES (D/R)")
    print("=" * 60)

    # Build SoS lookup for matching
    sos_leg_lookup = set()
    for c in leg_primary:
        lk = last_name_key(c['last_name'])
        sos_leg_lookup.add((c['chamber'], c['district'], c['party'], lk))

    # Build existing DB lookup
    db_leg_lookup = set()
    for d in db_leg_cands:
        lk = last_name_key(d['last_name'])
        db_leg_lookup.add((d['chamber'], d['district_number'], d['party'], lk))

    # --- Matched ---
    matched_count = 0
    for c in leg_primary:
        lk = last_name_key(c['last_name'])
        key = (c['chamber'], c['district'], c['party'], lk)
        if key in db_leg_lookup:
            matched_count += 1
        else:
            # Partial match
            for dk in db_leg_lookup:
                if dk[0] == c['chamber'] and dk[1] == c['district'] and dk[2] == c['party']:
                    if dk[3] in lk or lk in dk[3]:
                        matched_count += 1
                        break

    print(f"\n  Matched: {matched_count}")

    # --- New candidates ---
    new_leg = []
    for c in leg_primary:
        lk = last_name_key(c['last_name'])
        key = (c['chamber'], c['district'], c['party'], lk)
        if key in db_leg_lookup:
            continue
        # Partial match check
        found = False
        for dk in db_leg_lookup:
            if dk[0] == c['chamber'] and dk[1] == c['district'] and dk[2] == c['party']:
                if dk[3] in lk or lk in dk[3]:
                    found = True
                    break
        if found:
            continue
        new_leg.append(c)

    print(f"  New: {len(new_leg)}")
    for c in new_leg:
        print(f"    NEW: {c['chamber']} D-{c['district']} {c['party']}: {c['full_name']} (filed {c['filing_date']})")

    # --- Withdrawn ---
    withdrawn_leg = []
    for d in db_leg_cands:
        if d.get('candidate_status') == 'Withdrawn_Pre_Ballot':
            continue
        # Only check primary candidates
        if d['election_type'] not in ('Primary_D', 'Primary_R'):
            continue

        db_lk = last_name_key(d['last_name'])
        key = (d['chamber'], d['district_number'], d['party'], db_lk)
        if key in sos_leg_lookup:
            continue
        # Partial match
        found = False
        for sk in sos_leg_lookup:
            if sk[0] == d['chamber'] and sk[1] == d['district_number'] and sk[2] == d['party']:
                if db_lk in sk[3] or sk[3] in db_lk:
                    found = True
                    break
        if found:
            continue
        withdrawn_leg.append(d)

    print(f"  Possibly withdrawn: {len(withdrawn_leg)}")
    for d in withdrawn_leg:
        print(f"    WITHDRAWN: {d['chamber']} D-{d['district_number']} {d['party']}: {d['full_name']} (cy_id={d['cy_id']})")

    # ========================================
    # Process statewide primary candidates (D/R)
    # ========================================
    print(f"\n{'='*60}")
    print("STATEWIDE PRIMARY CANDIDATES (D/R)")
    print("=" * 60)

    sos_sw_lookup = set()
    for c in sw_primary:
        lk = last_name_key(c['last_name'])
        sos_sw_lookup.add((c['office_type'], c['party'], lk))

    db_sw_lookup = set()
    for d in db_sw_cands:
        lk = last_name_key(d['last_name'])
        db_sw_lookup.add((d['office_type'], d['party'], lk))

    # Matched
    sw_matched = 0
    for c in sw_primary:
        lk = last_name_key(c['last_name'])
        key = (c['office_type'], c['party'], lk)
        if key in db_sw_lookup:
            sw_matched += 1
        else:
            for dk in db_sw_lookup:
                if dk[0] == c['office_type'] and dk[1] == c['party']:
                    if dk[2] in lk or lk in dk[2]:
                        sw_matched += 1
                        break

    print(f"\n  Matched: {sw_matched}")

    # New statewide
    new_sw = []
    for c in sw_primary:
        lk = last_name_key(c['last_name'])
        key = (c['office_type'], c['party'], lk)
        if key in db_sw_lookup:
            continue
        found = False
        for dk in db_sw_lookup:
            if dk[0] == c['office_type'] and dk[1] == c['party']:
                if dk[2] in lk or lk in dk[2]:
                    found = True
                    break
        if found:
            continue
        new_sw.append(c)

    print(f"  New: {len(new_sw)}")
    for c in new_sw:
        print(f"    NEW: {c['office_type']} {c['party']}: {c['full_name']} (filed {c['filing_date']})")

    # Withdrawn statewide
    withdrawn_sw = []
    for d in db_sw_cands:
        if d.get('candidate_status') == 'Withdrawn_Pre_Ballot':
            continue
        if d['election_type'] not in ('Primary_D', 'Primary_R'):
            continue
        db_lk = last_name_key(d['last_name'])
        key = (d['office_type'], d['party'], db_lk)
        if key in sos_sw_lookup:
            continue
        found = False
        for sk in sos_sw_lookup:
            if sk[0] == d['office_type'] and sk[1] == d['party']:
                if db_lk in sk[2] or sk[2] in db_lk:
                    found = True
                    break
        if found:
            continue
        withdrawn_sw.append(d)

    print(f"  Possibly withdrawn: {len(withdrawn_sw)}")
    for d in withdrawn_sw:
        print(f"    WITHDRAWN: {d['office_type']} {d['party']}: {d['full_name']} (cy_id={d['cy_id']})")

    # ========================================
    # Process third-party / general election candidates
    # ========================================
    print(f"\n{'='*60}")
    print("GENERAL ELECTION CANDIDATES (Third Party)")
    print("=" * 60)

    new_gen_leg = []
    for c in leg_general:
        lk = last_name_key(c['last_name'])
        # Check if already in DB (general election candidacies)
        found = False
        for d in db_leg_cands:
            if d['election_type'] != 'General':
                continue
            if d['chamber'] == c['chamber'] and d['district_number'] == c['district']:
                db_lk = last_name_key(d['last_name'])
                if db_lk == lk or db_lk in lk or lk in db_lk:
                    found = True
                    break
        if not found:
            new_gen_leg.append(c)

    new_gen_sw = []
    for c in sw_general:
        lk = last_name_key(c['last_name'])
        found = False
        for d in db_sw_cands:
            if d['election_type'] != 'General':
                continue
            if d['office_type'] == c['office_type']:
                db_lk = last_name_key(d['last_name'])
                if db_lk == lk or db_lk in lk or lk in db_lk:
                    found = True
                    break
        if not found:
            new_gen_sw.append(c)

    print(f"  New legislative (general): {len(new_gen_leg)}")
    for c in new_gen_leg:
        print(f"    NEW: {c['chamber']} D-{c['district']} {c['party']}: {c['full_name']} (filed {c['filing_date']})")
    print(f"  New statewide (general): {len(new_gen_sw)}")
    for c in new_gen_sw:
        print(f"    NEW: {c['office_type']} {c['party']}: {c['full_name']} (filed {c['filing_date']})")

    # ========================================
    # Execute imports if --import
    # ========================================
    if DO_IMPORT:
        print(f"\n{'='*60}")
        print("IMPORTING NEW CANDIDATES")
        print("=" * 60)

        batch_sql = []

        # Legislative primary candidates
        for c in new_leg:
            elec_type = f"Primary_{c['party']}"
            elec_key = (c['chamber'], c['district'], elec_type)
            elec_id = leg_elec_lookup.get(elec_key)
            if not elec_id:
                print(f"  WARNING: No election for {c['chamber']} D-{c['district']} {elec_type} — skipping {c['full_name']}")
                continue

            fullname = esc(c['full_name'])
            first = esc(c['first_name'])
            last = esc(c['last_name'])
            filing = f"'{c['filing_date']}'" if c['filing_date'] else 'NULL'

            sql = f"""
            WITH new_cand AS (
                INSERT INTO candidates (full_name, first_name, last_name)
                VALUES ('{fullname}', '{first}', '{last}')
                RETURNING id
            )
            INSERT INTO candidacies (candidate_id, election_id, party, candidate_status, filing_date)
            SELECT id, {elec_id}, '{c['party']}', 'Filed', {filing}
            FROM new_cand;
            """
            batch_sql.append(sql)

        # Statewide primary candidates
        for c in new_sw:
            elec_type = f"Primary_{c['party']}"
            elec_key = (c['office_type'], elec_type)
            elec_id = sw_elec_lookup.get(elec_key)
            if not elec_id:
                print(f"  WARNING: No election for {c['office_type']} {elec_type} — skipping {c['full_name']}")
                continue

            fullname = esc(c['full_name'])
            first = esc(c['first_name'])
            last = esc(c['last_name'])
            filing = f"'{c['filing_date']}'" if c['filing_date'] else 'NULL'

            sql = f"""
            WITH new_cand AS (
                INSERT INTO candidates (full_name, first_name, last_name)
                VALUES ('{fullname}', '{first}', '{last}')
                RETURNING id
            )
            INSERT INTO candidacies (candidate_id, election_id, party, candidate_status, filing_date)
            SELECT id, {elec_id}, '{c['party']}', 'Filed', {filing}
            FROM new_cand;
            """
            batch_sql.append(sql)

        # General election third-party candidates (legislative)
        for c in new_gen_leg:
            elec_key = (c['chamber'], c['district'], 'General')
            elec_id = leg_elec_lookup.get(elec_key)
            if not elec_id:
                print(f"  WARNING: No General election for {c['chamber']} D-{c['district']} — skipping {c['full_name']}")
                continue

            fullname = esc(c['full_name'])
            first = esc(c['first_name'])
            last = esc(c['last_name'])
            filing = f"'{c['filing_date']}'" if c['filing_date'] else 'NULL'

            sql = f"""
            WITH new_cand AS (
                INSERT INTO candidates (full_name, first_name, last_name)
                VALUES ('{fullname}', '{first}', '{last}')
                RETURNING id
            )
            INSERT INTO candidacies (candidate_id, election_id, party, candidate_status, filing_date)
            SELECT id, {elec_id}, '{c['party']}', 'Filed', {filing}
            FROM new_cand;
            """
            batch_sql.append(sql)

        # General election third-party candidates (statewide)
        for c in new_gen_sw:
            elec_key = (c['office_type'], 'General')
            elec_id = sw_elec_lookup.get(elec_key)
            if not elec_id:
                print(f"  WARNING: No General election for {c['office_type']} — skipping {c['full_name']}")
                continue

            fullname = esc(c['full_name'])
            first = esc(c['first_name'])
            last = esc(c['last_name'])
            filing = f"'{c['filing_date']}'" if c['filing_date'] else 'NULL'

            sql = f"""
            WITH new_cand AS (
                INSERT INTO candidates (full_name, first_name, last_name)
                VALUES ('{fullname}', '{first}', '{last}')
                RETURNING id
            )
            INSERT INTO candidacies (candidate_id, election_id, party, candidate_status, filing_date)
            SELECT id, {elec_id}, '{c['party']}', 'Filed', {filing}
            FROM new_cand;
            """
            batch_sql.append(sql)

        print(f"  Total candidates to import: {len(batch_sql)}")

        if batch_sql:
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
                print(f"  Batch {i//batch_size + 1}/{(len(batch_sql)-1)//batch_size + 1} done ({len(batch)} candidates)")
                time.sleep(2)

    # ========================================
    # Execute withdrawals if --withdraw
    # ========================================
    if DO_WITHDRAW:
        print(f"\n{'='*60}")
        print("MARKING WITHDRAWALS")
        print("=" * 60)

        all_withdrawn = withdrawn_leg + withdrawn_sw
        if all_withdrawn:
            ids_str = ','.join(str(d['cy_id']) for d in all_withdrawn)
            run_sql(f"UPDATE candidacies SET candidate_status = 'Withdrawn_Pre_Ballot' WHERE id IN ({ids_str})", "Mark withdrawals")
            print(f"  Marked {len(all_withdrawn)} as Withdrawn_Pre_Ballot")
        else:
            print("  No withdrawals to mark")

    # ========================================
    # Summary
    # ========================================
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"  Legislative primary: {matched_count} matched, {len(new_leg)} new, {len(withdrawn_leg)} withdrawn")
    print(f"  Statewide primary: {sw_matched} matched, {len(new_sw)} new, {len(withdrawn_sw)} withdrawn")
    print(f"  General (3rd party): {len(new_gen_leg)} legislative + {len(new_gen_sw)} statewide new")
    if not DO_IMPORT and not DO_WITHDRAW:
        print(f"\n  [REPORT ONLY — use --import to add, --withdraw to mark withdrawals]")
    elif DO_IMPORT and not DO_WITHDRAW:
        print(f"\n  [Imported new candidates. Use --withdraw to mark withdrawals.]")
    elif DO_WITHDRAW and not DO_IMPORT:
        print(f"\n  [Marked withdrawals. Use --import to add new candidates.]")


if __name__ == '__main__':
    main()
