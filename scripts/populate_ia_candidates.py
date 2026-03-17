#!/usr/bin/env python3
"""
Populate Iowa 2026 primary candidates from official SoS PDF.

Source: https://sos.iowa.gov/elections/
File: ~/Downloads/2026 Primary - Candidate List Database - All Elections_10.pdf

Operations:
1. Parse PDF table data using pdfplumber
2. Match to existing DB candidacies by district + party + last name
3. Report matched, new, and potentially withdrawn candidates
4. --import flag to create new candidates/candidacies
5. --withdraw flag to mark withdrawals

Usage:
    python3 scripts/populate_ia_candidates.py --dry-run
    python3 scripts/populate_ia_candidates.py --import
    python3 scripts/populate_ia_candidates.py --withdraw
    python3 scripts/populate_ia_candidates.py --import --withdraw
"""

import os
import re
import sys
import time
import requests

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber required. Install with: pip install pdfplumber")
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
DO_IMPORT = '--import' in sys.argv
DO_WITHDRAW = '--withdraw' in sys.argv

PDF_PATH = os.path.expanduser('~/Downloads/2026 Primary - Candidate List Database - All Elections_10.pdf')

# Office mapping: SoS office name -> (office_type, chamber, office_level)
OFFICE_MAP = {
    'Governor': ('Governor', 'Statewide', 'Statewide'),
    'Secretary of State': ('Secretary of State', 'Statewide', 'Statewide'),
    'Auditor of State': ('Auditor', 'Statewide', 'Statewide'),
    'Treasurer of State': ('Treasurer', 'Statewide', 'Statewide'),
    'Secretary of Agriculture': ('Agriculture Commissioner', 'Statewide', 'Statewide'),
    'Attorney General': ('Attorney General', 'Statewide', 'Statewide'),
}

# Federal offices to skip
FEDERAL_OFFICES = {
    'United States Senator',
    'United States Representative',
}


def run_sql(query, label=""):
    if DRY_RUN and not label.startswith("Get "):
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
            wait = 10 * attempt
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
            wait = 10 * attempt
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        print(f"  ERROR ({r.status_code}): {r.text[:200]}")
        return None
    return None


def esc(s):
    return s.replace("'", "''")


def parse_name(full_name):
    """Parse a full name into first_name, last_name."""
    parts = full_name.strip().split()
    if len(parts) == 1:
        return parts[0], parts[0]

    # Handle suffixes
    suffixes = {'Jr', 'Jr.', 'Sr', 'Sr.', 'II', 'III', 'IV'}
    suffix_parts = []
    for i in range(len(parts) - 1, 0, -1):
        if parts[i].rstrip(',') in suffixes:
            suffix_parts.insert(0, parts[i].rstrip(','))
        else:
            break

    non_suffix = parts[:len(parts) - len(suffix_parts)]
    first_name = non_suffix[0]

    # Skip middle initials (single letter or letter with period)
    last_name_parts = []
    for p in non_suffix[1:]:
        if len(p) <= 2 and (p.endswith('.') or len(p) == 1):
            continue  # Skip middle initials like "M." "H" "C."
        last_name_parts.append(p)

    last_name = ' '.join(last_name_parts)
    if not last_name and len(non_suffix) > 1:
        last_name = non_suffix[-1]

    # Append suffixes to last name
    if suffix_parts:
        last_name = last_name + ' ' + ' '.join(suffix_parts)

    return first_name, last_name


def last_name_key(name):
    """Normalize last name for matching: lowercase, strip hyphens, commas, suffixes."""
    n = name.lower().strip()
    n = n.replace(',', '').replace('.', '')
    # Remove common suffixes
    for suf in [' jr', ' sr', ' ii', ' iii', ' iv']:
        if n.endswith(suf):
            n = n[:-len(suf)].strip()
    # Normalize hyphens and spaces
    n = n.replace('-', ' ')
    return n


def parse_pdf(filepath):
    """Parse Iowa SoS candidate PDF using pdfplumber text extraction.

    The PDF has no structured tables — each line is a text row with:
    [Office] Party Name Address Phone Email FilingDate

    Office appears only on first candidate row for that office; subsequent
    rows for the same office have just: Party Name Address ...
    """
    candidates = []
    current_office = None
    current_party = None

    # Known office prefixes that start a line
    office_prefixes = list(FEDERAL_OFFICES) + list(OFFICE_MAP.keys()) + [
        'State Senator District', 'State Representative District'
    ]

    with pdfplumber.open(filepath) as pdf:
        for page_num, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text:
                continue

            for line in text.split('\n'):
                line = line.strip()
                if not line:
                    continue

                # Skip headers and footers
                if line.startswith('For the Office Of') or line.startswith('Filing period') or \
                   line.startswith('For non-party') or line.startswith('For change') or \
                   line.startswith('Last Updated') or line.startswith('Candidate List') or \
                   line.startswith('June '):
                    continue

                # Check if line starts with an office name
                new_office = None
                rest_of_line = line
                for prefix in sorted(office_prefixes, key=len, reverse=True):
                    if line.startswith(prefix):
                        # For district offices, grab the full "State Senator District N"
                        if 'District' in prefix:
                            m = re.match(r'(State (?:Senator|Representative) District \d+)\s+(.*)', line)
                            if m:
                                new_office = m.group(1)
                                rest_of_line = m.group(2)
                                break
                        else:
                            # Statewide/federal office — office name then party
                            rest_of_line = line[len(prefix):].strip()
                            new_office = prefix
                            break

                if new_office:
                    current_office = new_office

                if not current_office:
                    continue

                # Now parse: Party Name Address Phone Email Date
                # Party is first word: "Republican" or "Democratic"
                # If no party prefix, carry forward from previous line (same-party multi-candidate)
                party_match = re.match(r'(Republican|Democratic)\s+(.*)', rest_of_line)
                if party_match:
                    current_party = party_match.group(1)
                    remainder = party_match.group(2)
                elif current_party and rest_of_line and not rest_of_line.startswith('--'):
                    # No party prefix — carry forward current_party
                    remainder = rest_of_line
                else:
                    continue

                # Skip "--" placeholder entries (no candidate filed)
                if remainder.strip().startswith('--'):
                    continue

                # Extract filing date from end (M/D/YYYY)
                filing_date = None
                date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})\s*$', remainder)
                if date_match:
                    fd = date_match.group(1)
                    dm = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', fd)
                    if dm:
                        filing_date = f"{dm.group(3)}-{int(dm.group(1)):02d}-{int(dm.group(2)):02d}"
                    remainder = remainder[:date_match.start()].strip()

                # Extract ballot name — everything before the address
                # Address typically starts with a number, "PO Box", or "CONFIDENTIAL"
                name_match = re.match(r'(.+?)\s+(?:\d+\s|PO Box|CONFIDENTIAL|--)', remainder)
                if name_match:
                    full_name = name_match.group(1).strip()
                else:
                    # If no address found, take everything up to phone/email
                    full_name = remainder.strip()

                if not full_name or full_name == '--':
                    continue

                # Skip federal offices
                is_federal = False
                for fed in FEDERAL_OFFICES:
                    if current_office.startswith(fed):
                        is_federal = True
                        break
                if is_federal:
                    continue

                # Determine office type, chamber, district
                office_type = None
                chamber = None
                district = None
                office_level = None

                if current_office in OFFICE_MAP:
                    office_type, chamber, office_level = OFFICE_MAP[current_office]
                elif current_office.startswith('State Senator District'):
                    m = re.search(r'District (\d+)', current_office)
                    if m:
                        office_type = 'State Senate'
                        chamber = 'Senate'
                        district = m.group(1)
                        office_level = 'Legislative'
                elif current_office.startswith('State Representative District'):
                    m = re.search(r'District (\d+)', current_office)
                    if m:
                        office_type = 'State House'
                        chamber = 'House'
                        district = m.group(1)
                        office_level = 'Legislative'
                else:
                    continue

                party = 'R' if current_party == 'Republican' else 'D'

                first_name, last_name = parse_name(full_name)

                candidates.append({
                    'full_name': full_name,
                    'first_name': first_name,
                    'last_name': last_name,
                    'party': party,
                    'office_type': office_type,
                    'office_level': office_level,
                    'chamber': chamber,
                    'district': district,
                    'filing_date': filing_date,
                })

    return candidates


def main():
    mode = []
    if DO_IMPORT:
        mode.append('IMPORT')
    if DO_WITHDRAW:
        mode.append('WITHDRAW')
    if DRY_RUN:
        mode.append('DRY RUN')
    if not mode:
        mode.append('REPORT ONLY')

    print(f"[{', '.join(mode)}] Iowa 2026 Candidate Import")
    print("=" * 60)

    # === Step 0: Parse PDF ===
    print(f"\nParsing PDF: {PDF_PATH}")
    sos_candidates = parse_pdf(PDF_PATH)
    leg = [c for c in sos_candidates if c['office_level'] == 'Legislative']
    sw = [c for c in sos_candidates if c['office_level'] == 'Statewide']
    print(f"SoS candidates parsed: {len(leg)} legislative + {len(sw)} statewide = {len(sos_candidates)} total")
    print(f"  By party: {sum(1 for c in sos_candidates if c['party']=='R')}R, {sum(1 for c in sos_candidates if c['party']=='D')}D")
    print(f"  Senate: {sum(1 for c in leg if c['chamber']=='Senate')}, House: {sum(1 for c in leg if c['chamber']=='House')}")

    # === Step 1: Get DB elections for IA 2026 ===
    print(f"\nQuerying database...")

    # Legislative elections
    elections = run_sql_read("""
        SELECT e.id, e.election_type, d.district_number, d.chamber
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'IA' AND e.election_year = 2026
        AND s.office_level = 'Legislative'
        AND e.election_type IN ('Primary_D', 'Primary_R')
    """, "Get IA legislative elections")

    if not elections:
        print("ERROR: No IA 2026 legislative elections found")
        return

    leg_elec_lookup = {}
    for e in elections:
        key = (e['chamber'], e['district_number'], e['election_type'])
        leg_elec_lookup[key] = e['id']

    print(f"Legislative elections in DB: {len(elections)}")
    print(f"  Primary_R: {sum(1 for e in elections if e['election_type'] == 'Primary_R')}")
    print(f"  Primary_D: {sum(1 for e in elections if e['election_type'] == 'Primary_D')}")

    # Statewide elections
    sw_elections = run_sql_read("""
        SELECT e.id, e.election_type, s.office_type
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'IA' AND e.election_year = 2026
        AND s.office_level = 'Statewide'
        AND e.election_type IN ('Primary_D', 'Primary_R')
    """, "Get IA statewide elections")

    sw_elec_lookup = {}
    if sw_elections:
        for e in sw_elections:
            key = (e['office_type'], e['election_type'])
            sw_elec_lookup[key] = e['id']
    print(f"Statewide elections in DB: {len(sw_elections) if sw_elections else 0}")

    # === Step 2: Get existing DB candidacies ===
    db_leg_cands = run_sql_read("""
        SELECT cy.id as cy_id, cy.candidate_id, c.full_name, c.first_name, c.last_name,
               cy.party, cy.candidate_status, cy.filing_date,
               d.district_number, d.chamber, e.election_type
        FROM candidacies cy
        JOIN candidates c ON cy.candidate_id = c.id
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'IA' AND e.election_year = 2026
        AND s.office_level = 'Legislative'
        AND e.election_type IN ('Primary_D', 'Primary_R')
    """, "Get existing legislative candidacies")

    db_sw_cands = run_sql_read("""
        SELECT cy.id as cy_id, cy.candidate_id, c.full_name, c.first_name, c.last_name,
               cy.party, cy.candidate_status, cy.filing_date,
               s.office_type, e.election_type
        FROM candidacies cy
        JOIN candidates c ON cy.candidate_id = c.id
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'IA' AND e.election_year = 2026
        AND s.office_level = 'Statewide'
        AND e.election_type IN ('Primary_D', 'Primary_R')
    """, "Get existing statewide candidacies")

    if not db_leg_cands:
        db_leg_cands = []
    if not db_sw_cands:
        db_sw_cands = []

    print(f"Existing DB candidacies: {len(db_leg_cands)} legislative + {len(db_sw_cands)} statewide")

    # === Step 3: Match candidates ===
    print(f"\n--- Matching candidates ---")

    # Build SoS lookup sets for legislative
    sos_leg_lookup = set()
    for c in leg:
        lk = last_name_key(c['last_name'])
        sos_leg_lookup.add((c['chamber'], c['district'], c['party'], lk))

    # Build SoS lookup for statewide
    sos_sw_lookup = set()
    for c in sw:
        lk = last_name_key(c['last_name'])
        sos_sw_lookup.add((c['office_type'], c['party'], lk))

    # --- Legislative matching ---
    matched_leg = 0
    new_leg = []
    withdrawal_leg_ids = []

    # Build existing DB lookup
    db_leg_lookup = set()
    for d in db_leg_cands:
        lk = last_name_key(d['last_name'])
        db_leg_lookup.add((d['chamber'], d['district_number'], d['party'], lk))

    # Find new candidates (in SoS but not in DB)
    for c in leg:
        lk = last_name_key(c['last_name'])
        key = (c['chamber'], c['district'], c['party'], lk)
        if key in db_leg_lookup:
            matched_leg += 1
            continue

        # Check partial match (last name contained in DB last name or vice versa)
        found = False
        for ex_key in db_leg_lookup:
            if ex_key[0] == c['chamber'] and ex_key[1] == c['district'] and ex_key[2] == c['party']:
                if ex_key[3] in lk or lk in ex_key[3]:
                    found = True
                    matched_leg += 1
                    break
        if found:
            continue

        new_leg.append(c)

    # Find withdrawals (in DB but not in SoS)
    for d in db_leg_cands:
        if d['candidate_status'] in ('Withdrawn_Pre_Ballot', 'Withdrawn_Post_Ballot'):
            continue  # Already marked

        db_lk = last_name_key(d['last_name'])
        key = (d['chamber'], d['district_number'], d['party'], db_lk)

        if key in sos_leg_lookup:
            continue

        # Check partial match
        found = False
        for sk in sos_leg_lookup:
            if sk[0] == d['chamber'] and sk[1] == d['district_number'] and sk[2] == d['party']:
                if db_lk in sk[3] or sk[3] in db_lk:
                    found = True
                    break
        if found:
            continue

        withdrawal_leg_ids.append(d)

    # --- Statewide matching ---
    matched_sw = 0
    new_sw = []
    withdrawal_sw_ids = []

    db_sw_lookup = set()
    for d in db_sw_cands:
        lk = last_name_key(d['last_name'])
        db_sw_lookup.add((d['office_type'], d['party'], lk))

    for c in sw:
        lk = last_name_key(c['last_name'])
        key = (c['office_type'], c['party'], lk)
        if key in db_sw_lookup:
            matched_sw += 1
            continue

        # Partial match
        found = False
        for ex_key in db_sw_lookup:
            if ex_key[0] == c['office_type'] and ex_key[1] == c['party']:
                if ex_key[2] in lk or lk in ex_key[2]:
                    found = True
                    matched_sw += 1
                    break
        if found:
            continue

        new_sw.append(c)

    for d in db_sw_cands:
        if d['candidate_status'] in ('Withdrawn_Pre_Ballot', 'Withdrawn_Post_Ballot'):
            continue

        db_lk = last_name_key(d['last_name'])
        key = (d['office_type'], d['party'], db_lk)

        if key in sos_sw_lookup:
            continue

        # Partial match
        found = False
        for sk in sos_sw_lookup:
            if sk[0] == d['office_type'] and sk[1] == d['party']:
                if db_lk in sk[2] or sk[2] in db_lk:
                    found = True
                    break
        if found:
            continue

        withdrawal_sw_ids.append(d)

    # === Report ===
    print(f"\n--- Results ---")
    print(f"Legislative: {matched_leg} matched, {len(new_leg)} new, {len(withdrawal_leg_ids)} potential withdrawals")
    print(f"Statewide: {matched_sw} matched, {len(new_sw)} new, {len(withdrawal_sw_ids)} potential withdrawals")

    if new_leg:
        print(f"\n  NEW LEGISLATIVE CANDIDATES ({len(new_leg)}):")
        for c in sorted(new_leg, key=lambda x: (x['chamber'], int(x['district']), x['party'])):
            print(f"    {c['chamber']} D-{c['district']} {c['party']}: {c['full_name']} (filed {c['filing_date']})")

    if new_sw:
        print(f"\n  NEW STATEWIDE CANDIDATES ({len(new_sw)}):")
        for c in sorted(new_sw, key=lambda x: (x['office_type'], x['party'])):
            print(f"    {c['office_type']} {c['party']}: {c['full_name']} (filed {c['filing_date']})")

    if withdrawal_leg_ids:
        print(f"\n  POTENTIAL LEGISLATIVE WITHDRAWALS ({len(withdrawal_leg_ids)}):")
        for d in sorted(withdrawal_leg_ids, key=lambda x: (x['chamber'], int(x['district_number']), x['party'])):
            print(f"    {d['chamber']} D-{d['district_number']} {d['party']}: {d['full_name']} (cy_id={d['cy_id']})")

    if withdrawal_sw_ids:
        print(f"\n  POTENTIAL STATEWIDE WITHDRAWALS ({len(withdrawal_sw_ids)}):")
        for d in withdrawal_sw_ids:
            print(f"    {d['office_type']} {d['party']}: {d['full_name']} (cy_id={d['cy_id']})")

    # === Step 4: Import new candidates (if --import) ===
    if DO_IMPORT and (new_leg or new_sw):
        print(f"\n--- Importing new candidates ---")
        batch_sql = []

        # Legislative
        for c in new_leg:
            elec_type = f"Primary_{c['party']}"
            elec_key = (c['chamber'], c['district'], elec_type)
            elec_id = leg_elec_lookup.get(elec_key)
            if not elec_id:
                print(f"  WARNING: No election for {c['chamber']} D-{c['district']} {elec_type} — skipping {c['full_name']}")
                continue

            first = esc(c['first_name'])
            last = esc(c['last_name'])
            full = esc(c['full_name'])
            filing = f"'{c['filing_date']}'" if c['filing_date'] else 'NULL'

            sql = f"""
            WITH new_cand AS (
                INSERT INTO candidates (full_name, first_name, last_name)
                VALUES ('{full}', '{first}', '{last}')
                RETURNING id
            )
            INSERT INTO candidacies (candidate_id, election_id, party, candidate_status, filing_date)
            SELECT id, {elec_id}, '{c['party']}', 'Filed', {filing}
            FROM new_cand;
            """
            batch_sql.append(sql)

        # Statewide
        for c in new_sw:
            elec_type = f"Primary_{c['party']}"
            elec_key = (c['office_type'], elec_type)
            elec_id = sw_elec_lookup.get(elec_key)
            if not elec_id:
                print(f"  WARNING: No election for {c['office_type']} {elec_type} — skipping {c['full_name']}")
                continue

            first = esc(c['first_name'])
            last = esc(c['last_name'])
            full = esc(c['full_name'])
            filing = f"'{c['filing_date']}'" if c['filing_date'] else 'NULL'

            sql = f"""
            WITH new_cand AS (
                INSERT INTO candidates (full_name, first_name, last_name)
                VALUES ('{full}', '{first}', '{last}')
                RETURNING id
            )
            INSERT INTO candidacies (candidate_id, election_id, party, candidate_status, filing_date)
            SELECT id, {elec_id}, '{c['party']}', 'Filed', {filing}
            FROM new_cand;
            """
            batch_sql.append(sql)

        print(f"  Candidates to add: {len(batch_sql)}")

        if batch_sql and not DRY_RUN:
            batch_size = 20
            for i in range(0, len(batch_sql), batch_size):
                batch = batch_sql[i:i+batch_size]
                combined = '\n'.join(batch)
                result = run_sql(combined, f"Add batch {i//batch_size + 1}")
                if result is None:
                    # Try one at a time if batch fails
                    for j, sql in enumerate(batch):
                        run_sql(sql, f"Add candidate {i+j+1}")
                        time.sleep(0.5)
                print(f"  Batch {i//batch_size + 1}/{(len(batch_sql)-1)//batch_size + 1} done ({len(batch)} candidates)")
                time.sleep(2)
    elif DO_IMPORT:
        print(f"\n  No new candidates to import.")

    # === Step 5: Mark withdrawals (if --withdraw) ===
    if DO_WITHDRAW and (withdrawal_leg_ids or withdrawal_sw_ids):
        print(f"\n--- Marking withdrawals ---")
        all_withdrawal_ids = [d['cy_id'] for d in withdrawal_leg_ids] + [d['cy_id'] for d in withdrawal_sw_ids]
        if all_withdrawal_ids:
            ids_str = ','.join(str(i) for i in all_withdrawal_ids)
            run_sql(
                f"UPDATE candidacies SET candidate_status = 'Withdrawn_Pre_Ballot' WHERE id IN ({ids_str})",
                "Mark withdrawals"
            )
            print(f"  Marked {len(all_withdrawal_ids)} as Withdrawn_Pre_Ballot")
    elif DO_WITHDRAW:
        print(f"\n  No withdrawals to mark.")

    # === Summary ===
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"  SoS candidates: {len(sos_candidates)} ({len(leg)} leg + {len(sw)} statewide)")
    print(f"  Matched: {matched_leg + matched_sw} ({matched_leg} leg + {matched_sw} statewide)")
    print(f"  New: {len(new_leg) + len(new_sw)} ({len(new_leg)} leg + {len(new_sw)} statewide)")
    print(f"  Potential withdrawals: {len(withdrawal_leg_ids) + len(withdrawal_sw_ids)} ({len(withdrawal_leg_ids)} leg + {len(withdrawal_sw_ids)} statewide)")
    if DRY_RUN:
        print(f"\n  [DRY RUN — no changes made]")
    elif not DO_IMPORT and not DO_WITHDRAW:
        print(f"\n  [REPORT ONLY — use --import and/or --withdraw to make changes]")


if __name__ == '__main__':
    main()
