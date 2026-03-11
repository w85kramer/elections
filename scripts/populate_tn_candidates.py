#!/usr/bin/env python3
"""
Populate Tennessee 2026 candidates from official SoS Excel exports.

Source: https://sos.tn.gov/elections/2026-candidate-lists
Files: /tmp/TN_Senate_Filed.xlsx, /tmp/TN_House_Filed.xlsx

Operations:
1. Mark withdrawn candidates (in DB but not in SoS list)
2. Add ~133 new candidates
3. Skip Independents (no primary elections for them)

Usage:
    python3 scripts/populate_tn_candidates.py --dry-run
    python3 scripts/populate_tn_candidates.py
"""

import json
import os
import re
import sys
import time
import openpyxl
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


def normalize_name(name):
    """Clean up SoS name: strip suffixes, extra spaces, commas."""
    name = name.strip()
    name = re.sub(r'\s+', ' ', name)
    return name


def parse_name(full_name):
    """Parse a full name into first_name, last_name."""
    parts = full_name.split()
    if len(parts) == 1:
        return parts[0], ''

    # Handle suffixes
    suffixes = {'Jr', 'Jr.', 'Sr', 'Sr.', 'II', 'III', 'IV'}
    last_parts = []
    suffix_parts = []
    # Walk backwards to find suffixes
    for i in range(len(parts) - 1, 0, -1):
        if parts[i].rstrip(',') in suffixes:
            suffix_parts.insert(0, parts[i].rstrip(','))
        else:
            break

    non_suffix = parts[:len(parts) - len(suffix_parts)]
    first_name = non_suffix[0]
    # Handle middle initials — skip single-letter/initial parts
    last_name_parts = []
    for p in non_suffix[1:]:
        if len(p) <= 2 and p.endswith('.'):
            continue  # Skip middle initials like "M." "H."
        last_name_parts.append(p)

    last_name = ' '.join(last_name_parts)
    if not last_name and len(non_suffix) > 1:
        last_name = non_suffix[-1]

    return first_name, last_name


def parse_xlsx(filepath, chamber):
    """Parse a TN SoS Excel file."""
    wb = openpyxl.load_workbook(filepath)
    ws = wb.active
    candidates = []

    for row in ws.iter_rows(min_row=2, values_only=True):
        office, name, party_raw, city, filed, status = row
        if not name:
            continue

        dist_match = re.search(r'District (\d+)', office)
        if not dist_match:
            continue
        dist = dist_match.group(1)

        if party_raw == 'Republican':
            party = 'R'
        elif party_raw == 'Democratic':
            party = 'D'
        elif party_raw == 'Independent':
            party = 'I'
        else:
            party = party_raw[0] if party_raw else '?'

        full_name = normalize_name(name)
        first_name, last_name = parse_name(full_name)

        candidates.append({
            'full_name': full_name,
            'first_name': first_name,
            'last_name': last_name,
            'party': party,
            'district': dist,
            'chamber': chamber,
        })

    return candidates


def last_name_key(name):
    """Normalize last name for matching: lowercase, strip hyphens, commas, suffixes."""
    n = name.lower().strip()
    n = n.replace(',', '').replace('-', ' ').replace('.', '')
    # Remove common suffixes
    for suf in [' jr', ' sr', ' ii', ' iii', ' iv']:
        if n.endswith(suf):
            n = n[:-len(suf)].strip()
    return n


def main():
    print(f"{'[DRY RUN] ' if DRY_RUN else ''}Tennessee 2026 Candidate Verification")
    print("=" * 60)

    # Parse SoS data
    senate = parse_xlsx('/tmp/TN_Senate_Filed.xlsx', 'Senate')
    house = parse_xlsx('/tmp/TN_House_Filed.xlsx', 'House')
    sos = senate + house
    print(f"SoS candidates: {len(senate)} Senate + {len(house)} House = {len(sos)} total")
    print(f"  By party: {sum(1 for c in sos if c['party']=='R')}R, {sum(1 for c in sos if c['party']=='D')}D, {sum(1 for c in sos if c['party']=='I')}I")

    # Get election IDs for TN 2026
    elections = run_sql_read("""
        SELECT e.id, e.election_type, d.district_number, d.chamber
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'TN' AND e.election_year = 2026
        AND d.office_level = 'Legislative'
        AND e.election_type IN ('Primary_D', 'Primary_R')
    """, "Get TN elections")

    if not elections:
        print("ERROR: No TN 2026 elections found")
        return

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
        WHERE st.abbreviation = 'TN' AND e.election_year = 2026
        AND d.office_level = 'Legislative'
    """, "Get current DB candidacies")

    print(f"Existing DB candidacies: {len(db_cands)}")

    # === Name alias map for known mismatches ===
    # (chamber, district, party, db_last_lower) -> SoS last name
    NAME_ALIASES = {
        ('House', '9', 'R', 'hicks'): 'hicks',      # Gary Hicks = Gary W Hicks Jr
        ('House', '30', 'R', 'helton'): 'helton',    # Esther Helton = Esther Helton Haynes
        ('House', '58', 'D', 'love'): 'love',        # Harold Love = Harold M. Love, Jr.
        ('House', '80', 'D', 'bond-johnson'): 'bond johnson',  # hyphen vs space
        ('House', '84', 'D', 'towns,'): 'towns',     # Joe Towns, Jr. = Joe Towns Jr.
        ('House', '93', 'D', 'hardaway,'): 'hardaway',  # G.A. Hardaway, Sr. = Goffrey Hardaway
    }

    # Build SoS lookup for withdrawal detection
    sos_lookup = set()
    for c in sos:
        lk = last_name_key(c['last_name'])
        sos_lookup.add((c['chamber'], c['district'], c['party'], lk))

    # === Step 1: Mark withdrawals ===
    print(f"\n--- Step 1: Mark withdrawals ---")
    withdrawal_ids = []
    for d in db_cands:
        db_lk = last_name_key(d['last_name'])
        key = (d['chamber'], d['district_number'], d['party'], db_lk)

        # Check direct match
        if key in sos_lookup:
            continue

        # Check aliases
        alias_key = (d['chamber'], d['district_number'], d['party'], db_lk)
        if alias_key in NAME_ALIASES:
            alias_lk = NAME_ALIASES[alias_key]
            if (d['chamber'], d['district_number'], d['party'], alias_lk) in sos_lookup:
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
    print(f"\n--- Step 2: Add new candidates ---")

    # Build existing set for dedup
    existing = set()
    for d in db_cands:
        lk = last_name_key(d['last_name'])
        existing.add((d['chamber'], d['district_number'], d['party'], lk))

    new_count = 0
    skipped_independent = 0
    batch_sql = []

    for s in sos:
        if s['party'] == 'I':
            skipped_independent += 1
            continue

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
    print(f"  Independents skipped: {skipped_independent}")

    if batch_sql and not DRY_RUN:
        batch_size = 20
        for i in range(0, len(batch_sql), batch_size):
            batch = batch_sql[i:i+batch_size]
            combined = '\n'.join(batch)
            result = run_sql(combined, f"Add batch {i//batch_size + 1}")
            if result is None:
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
    print(f"  Independents skipped: {skipped_independent}")
    if DRY_RUN:
        print(f"\n  [DRY RUN — no changes made]")


if __name__ == '__main__':
    main()
