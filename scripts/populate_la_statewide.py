#!/usr/bin/env python3
"""
Populate LA statewide election results from Secretary of State Excel files.

Reads the same .xlsx files as populate_la_historical.py but extracts
statewide races (Governor, Lt Gov, AG, SoS, Treasurer, Insurance Commissioner,
Ag Commissioner) instead of legislative races.

Usage:
    python3 scripts/populate_la_statewide.py --dry-run
    python3 scripts/populate_la_statewide.py --year 1995
    python3 scripts/populate_la_statewide.py --year-from 1983 --year-to 2019
"""

import sys
import os
import re
import time
import argparse
import glob

import httpx
import openpyxl

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL
from scripts.candidate_lookup import CandidateLookup


# ══════════════════════════════════════════════════════════════════════
# SQL helpers
# ══════════════════════════════════════════════════════════════════════

def run_sql(query, retries=8, exit_on_error=True):
    for attempt in range(retries):
        resp = httpx.post(
            API_URL,
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query}, timeout=120
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code in (429, 500, 503) and attempt < retries - 1:
            wait = 10 * (attempt + 1)
            print(f'  Rate limited/error ({resp.status_code}), waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None
    if exit_on_error:
        sys.exit(1)
    return None


def esc(s):
    if s is None:
        return 'NULL'
    return "'" + str(s).replace("'", "''") + "'"


# ══════════════════════════════════════════════════════════════════════
# Constants
# ══════════════════════════════════════════════════════════════════════

PARTY_MAP = {
    'REP': 'R', 'DEM': 'D', 'IND': 'I', 'LIB': 'L', 'LBT': 'L',
    'GRN': 'G', 'NOPTY': 'I', 'NP': 'I', 'OTH': 'I', 'OTHER': 'I',
    'NO PARTY': 'I', 'AI': 'I', 'REF': 'REF', 'RFM': 'REF',
}

CANDIDATE_RE = re.compile(r'^(.+?)\s*\(([A-Z ]+)\)\s*$')

# Map race header text to our office/seat names
OFFICE_MAP = {
    'governor': 'Governor',
    'lieutenant governor': 'Lt. Governor',
    'attorney general': 'Attorney General',
    'secretary of state': 'Secretary of State',
    'treasurer': 'Treasurer',
    'commissioner -- insurance': 'Insurance Commissioner',
    'commissioner -- agriculture and forestry': 'Ag Commissioner',
    'commissioner of -- insurance': 'Insurance Commissioner',
    'commissioner of -- agriculture and forestry': 'Ag Commissioner',
    'commissioner of insurance': 'Insurance Commissioner',
    'commissioner of agriculture': 'Ag Commissioner',
    'commissioner -- elections': None,  # Skip this one
    'commissioner of elections': None,  # Skip this one
}


# ══════════════════════════════════════════════════════════════════════
# Parsing
# ══════════════════════════════════════════════════════════════════════

def parse_candidate_cell(cell_value):
    if not cell_value or not isinstance(cell_value, str):
        return None, None
    cell_value = cell_value.strip()
    m = CANDIDATE_RE.match(cell_value)
    if not m:
        return None, None
    raw_name = m.group(1).strip()
    party_abbrev = m.group(2).strip()
    name = raw_name.replace('"', '').strip()
    if name == name.upper():
        name = name.title()
        name = re.sub(r"\bMc(\w)", lambda m: "Mc" + m.group(1).upper(), name)
        name = re.sub(r"\bO'(\w)", lambda m: "O'" + m.group(1).upper(), name)
    party = PARTY_MAP.get(party_abbrev, 'I')
    return name, party


def parse_statewide_races(filepath):
    """Parse statewide races from a LA SoS election results Excel file."""
    wb = openpyxl.load_workbook(filepath, read_only=True)
    ws = wb['Multi-Parish(Parish)']
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    # Extract election date from early rows
    election_date = None
    for row in rows[:5]:
        for cell in row:
            if cell and isinstance(cell, str) and 'Election Date:' in cell:
                m = re.search(r'(\d{4}-\d{2}-\d{2})', cell)
                if m:
                    election_date = m.group(1)

    races = []
    i = 0
    while i < len(rows):
        row = rows[i]
        if (row and row[0] and isinstance(row[0], str)
                and row[0].strip()
                and all(c is None for c in row[1:6] if len(row) > 1)):

            race_name = row[0].strip()
            office = identify_office(race_name)

            if office is not None and i + 2 < len(rows):
                cand_row = rows[i + 1]
                votes_row = rows[i + 2]

                if (votes_row and votes_row[0]
                        and isinstance(votes_row[0], str)
                        and votes_row[0].strip() == 'Total Votes'):

                    candidates = []
                    for col_idx in range(1, len(cand_row)):
                        name, party = parse_candidate_cell(
                            cand_row[col_idx] if col_idx < len(cand_row) else None)
                        if name:
                            votes = (votes_row[col_idx]
                                     if col_idx < len(votes_row)
                                     else 0) or 0
                            candidates.append({
                                'name': name,
                                'party': party,
                                'votes': int(votes),
                            })

                    if candidates:
                        total_votes = sum(c['votes'] for c in candidates)
                        # Determine winner
                        candidates.sort(key=lambda c: c['votes'], reverse=True)
                        majority = total_votes / 2.0
                        if candidates[0]['votes'] > majority:
                            for j, c in enumerate(candidates):
                                c['result'] = 'Won' if j == 0 else 'Lost'
                        else:
                            for j, c in enumerate(candidates):
                                c['result'] = 'Advanced' if j < 2 else 'Lost'

                        races.append({
                            'office': office,
                            'race_name': race_name,
                            'candidates': candidates,
                            'total_votes': total_votes,
                        })
        i += 1

    return election_date, races


def identify_office(race_name):
    """Map a race header to an office name, or None to skip."""
    lower = race_name.lower().strip()
    # Strip suffixes like "-- For Regular and Unexpired Term"
    lower = re.sub(r'\s*--\s*for\s+.*$', '', lower)
    for pattern, office in OFFICE_MAP.items():
        if pattern in lower:
            return office
    return None


# ══════════════════════════════════════════════════════════════════════
# File discovery
# ══════════════════════════════════════════════════════════════════════

def find_files(download_dir, year_from, year_to):
    """Find LA election result Excel files for statewide races."""
    files = []
    for path in sorted(glob.glob(os.path.join(download_dir, 'Election+Results+*.xlsx'))):
        basename = os.path.basename(path)
        m = re.search(r'\((\d{1,2})-(\d{1,2})-(\d{4})\)', basename)
        if not m:
            continue
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < year_from or year > year_to:
            continue
        # Only use primary election files (October), not runoff (November)
        if month > 10:
            continue
        date_str = f'{year}-{month:02d}-{day:02d}'
        # Prefer (1) suffix files (newer downloads) over originals
        files.append({'date': date_str, 'year': year, 'path': path})

    # Deduplicate: prefer (1) suffix files (newer downloads) over originals
    by_year = {}
    for f in files:
        existing = by_year.get(f['year'])
        if not existing or '(1)' in f['path']:
            by_year[f['year']] = f
    return sorted(by_year.values(), key=lambda f: f['year'])


# ══════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Populate LA statewide elections')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--year', type=int)
    parser.add_argument('--year-from', type=int, default=1983)
    parser.add_argument('--year-to', type=int, default=2023)
    parser.add_argument('--download-dir', default=os.path.expanduser('~/Downloads'))
    args = parser.parse_args()

    if args.year:
        args.year_from = args.year
        args.year_to = args.year

    # Find files
    files = find_files(args.download_dir, args.year_from, args.year_to)
    print(f'Found {len(files)} election files:')
    for f in files:
        print(f'  {f["year"]}: {os.path.basename(f["path"])}')

    if not files:
        print('No files found!')
        return

    # Load seat map for statewide offices
    print('\nLoading LA statewide seats...')
    rows = run_sql("""
        SELECT s.id AS seat_id, s.current_holder
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'LA' AND d.chamber = 'Statewide'
    """)

    # Map office names to seat IDs based on current holders
    HOLDER_TO_OFFICE = {
        'Jeff Landry': 'Governor',
        'Billy Nungesser': 'Lt. Governor',
        'Liz Murrill': 'Attorney General',
        'Nancy Landry': 'Secretary of State',
        'John Fleming': 'Treasurer',
        'Tim Temple': 'Insurance Commissioner',
        'Mike Strain': 'Ag Commissioner',
    }
    seat_map = {}
    for r in rows:
        office = HOLDER_TO_OFFICE.get(r['current_holder'])
        if office:
            seat_map[office] = r['seat_id']
    print(f'  Mapped {len(seat_map)} offices: {list(seat_map.keys())}')

    # Load existing elections
    print('Loading existing statewide elections...')
    existing = run_sql("""
        SELECT e.id, e.seat_id, e.election_year, e.election_type
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'LA' AND d.chamber = 'Statewide'
    """)
    existing_elections = {}
    for r in existing:
        key = (r['seat_id'], r['election_year'], r['election_type'])
        existing_elections[key] = r['id']
    print(f'  {len(existing_elections)} existing statewide elections')

    # Load existing candidacies
    existing_candys = set()
    if existing_elections:
        eids = ','.join(str(eid) for eid in existing_elections.values())
        rows = run_sql(f"SELECT election_id, candidate_id FROM candidacies WHERE election_id IN ({eids})")
        for r in rows:
            existing_candys.add((r['election_id'], r['candidate_id']))
    print(f'  {len(existing_candys)} existing candidacies')

    # Initialize candidate lookup
    print('Loading candidate data...')
    candidate_lookup = CandidateLookup(run_sql)
    candidate_lookup.load_state('LA')

    # Process files
    stats = {'elections_created': 0, 'elections_skipped': 0,
             'candidacies_created': 0, 'candidacies_skipped': 0,
             'candidates_created': 0, 'candidates_reused': 0}

    for file_info in files:
        filepath = file_info['path']
        year = file_info['year']

        print(f'\n{"="*60}')
        print(f'Processing {year}: {os.path.basename(filepath)}')
        print(f'{"="*60}')

        election_date, races = parse_statewide_races(filepath)
        print(f'  Election date: {election_date}')
        print(f'  {len(races)} statewide races found')

        for race in races:
            office = race['office']
            seat_id = seat_map.get(office)
            if not seat_id:
                print(f'  SKIP: {office} (no seat mapping)')
                continue

            # Check if election exists
            existing_key = (seat_id, year, 'General')
            election_id = existing_elections.get(existing_key)

            if election_id:
                # Check if it already has candidates
                has_candidates = any(eid == election_id for eid, _ in existing_candys)
                if has_candidates:
                    stats['elections_skipped'] += 1
                    continue
                print(f'  {office}: adding candidates to existing election {election_id}')
            else:
                if args.dry_run:
                    print(f'  {office}: would create election + {len(race["candidates"])} candidates')
                    stats['elections_created'] += 1
                    continue

                result = run_sql(f"""
                    INSERT INTO elections (seat_id, election_year, election_type, election_date, total_votes_cast, result_status)
                    VALUES ({seat_id}, {year}, 'General', {esc(election_date)}, {race['total_votes']}, 'Certified')
                    ON CONFLICT DO NOTHING RETURNING id
                """)
                if result:
                    election_id = result[0]['id']
                    existing_elections[existing_key] = election_id
                    stats['elections_created'] += 1
                    print(f'  {office}: created election {election_id}')
                else:
                    print(f'  {office}: failed to create election')
                    continue

            if args.dry_run:
                for c in race['candidates']:
                    print(f'    {c["name"]} ({c["party"]}) - {c["votes"]:,} votes - {c["result"]}')
                continue

            # Insert candidacies
            for c in race['candidates']:
                parts = c['name'].split()
                first_name = parts[0] if parts else ''
                last_name = parts[-1] if len(parts) > 1 else parts[0] if parts else ''
                pct = round(100 * c['votes'] / race['total_votes'], 2) if race['total_votes'] > 0 else 0

                cid = candidate_lookup.find_or_create(
                    full_name=c['name'], state='LA',
                    first_name=first_name, last_name=last_name)

                if (election_id, cid) in existing_candys:
                    stats['candidacies_skipped'] += 1
                    continue

                is_major = c['party'] in ('D', 'R')
                result = run_sql(f"""
                    INSERT INTO candidacies (election_id, candidate_id, party, result, votes_received, vote_percentage, is_major, is_write_in)
                    VALUES ({election_id}, {cid}, {esc(c['party'])}, {esc(c['result'])}, {c['votes']}, {pct}, {'TRUE' if is_major else 'FALSE'}, FALSE)
                    ON CONFLICT DO NOTHING
                """, exit_on_error=False)
                if result is not None:
                    stats['candidacies_created'] += 1
                    existing_candys.add((election_id, cid))

            time.sleep(2)

    # Summary
    print(f'\n{"="*60}')
    print('SUMMARY')
    print(f'{"="*60}')
    print(f'Elections created:    {stats["elections_created"]}')
    print(f'Elections skipped:    {stats["elections_skipped"]} (already had candidates)')
    print(f'Candidacies created: {stats["candidacies_created"]}')
    print(f'Candidacies skipped: {stats["candidacies_skipped"]}')


if __name__ == '__main__':
    main()
