#!/usr/bin/env python3
"""
Populate LA historical election results from Secretary of State Excel files.

Reads .xlsx files downloaded from:
  https://voterportal.sos.la.gov/graphical

LA uses a jungle primary system:
  - All candidates run together regardless of party
  - If someone gets 50%+1, they win outright (election_type = 'General')
  - Otherwise top two go to a runoff (election_type = 'General_Runoff')
  - Only contested races appear in the files (uncontested = won outright w/no election)

File format: Multi-Parish(Parish) sheet, wide format with:
  - Race header row (single cell)
  - Candidate names row with party in parens
  - Total Votes row
  - Parish-by-parish rows

Usage:
    python3 scripts/populate_la_historical.py --dry-run
    python3 scripts/populate_la_historical.py --year 2023
    python3 scripts/populate_la_historical.py --year-from 1983 --year-to 2007
"""

import sys
import os
import re
import time
import argparse
import glob
from collections import defaultdict

import httpx
import openpyxl

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL
from scripts.candidate_lookup import CandidateLookup


# ══════════════════════════════════════════════════════════════════════
# SQL helpers
# ══════════════════════════════════════════════════════════════════════

def run_sql(query, retries=8, exit_on_error=True):
    """Execute SQL via Supabase Management API with retry/backoff."""
    for attempt in range(retries):
        resp = httpx.post(
            API_URL,
            headers={'Authorization': f'Bearer {TOKEN}',
                     'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120
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
    print('Max retries exceeded')
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

# Party mapping from LA file format to our DB codes
PARTY_MAP = {
    'REP': 'R', 'DEM': 'D', 'IND': 'I', 'LIB': 'L', 'LBT': 'L',
    'GRN': 'G', 'NOPTY': 'I', 'NP': 'I', 'OTH': 'I', 'OTHER': 'I',
    'NO PARTY': 'I', 'AI': 'I', 'REF': 'REF', 'RFM': 'REF',
}

# Regex to parse candidate name and party from header cell
# e.g. '"Jeff" Landry (REP)' or 'Shawn D. Wilson (DEM)' or 'Jeffery Istre (IND)'
CANDIDATE_RE = re.compile(r'^(.+?)\s*\(([A-Z ]+)\)\s*$')

# Regex to parse race name
SENATE_RE = re.compile(r'State Senator\s*--\s*(\d+)\w*\s+Senatorial District', re.IGNORECASE)
HOUSE_RE = re.compile(r'State Representative\s*--\s*(\d+)\w*\s+Representative District', re.IGNORECASE)


# ══════════════════════════════════════════════════════════════════════
# File parsing
# ══════════════════════════════════════════════════════════════════════

def parse_race_name(name):
    """
    Parse race name to extract chamber and district number.
    Returns (chamber, district_number) or (None, None).
    """
    m = SENATE_RE.search(name)
    if m:
        return 'Senate', int(m.group(1))
    m = HOUSE_RE.search(name)
    if m:
        return 'House', int(m.group(1))
    return None, None


def parse_candidate_cell(cell_value):
    """
    Parse a candidate cell like '"Jeff" Landry (REP)'.
    Returns (name, party_code) or (None, None).
    """
    if not cell_value or not isinstance(cell_value, str):
        return None, None
    cell_value = cell_value.strip()
    m = CANDIDATE_RE.match(cell_value)
    if not m:
        return None, None
    raw_name = m.group(1).strip()
    party_abbrev = m.group(2).strip()

    # Clean up name: remove extra quotes around nicknames
    # e.g. '"Jeff" Landry' → Jeff Landry for DB, keep original for display
    # Actually keep the name as-is but clean double-quotes for DB storage
    name = raw_name.replace('"', '').strip()
    # Normalize to title case if ALL CAPS
    if name == name.upper():
        name = name.title()
        name = re.sub(r"\bMc(\w)", lambda m: "Mc" + m.group(1).upper(), name)
        name = re.sub(r"\bO'(\w)", lambda m: "O'" + m.group(1).upper(), name)

    party = PARTY_MAP.get(party_abbrev, 'I')
    if party_abbrev not in PARTY_MAP:
        print(f'  WARNING: Unknown party {party_abbrev!r} for {name}, mapping to I')

    return name, party


def parse_excel_file(filepath):
    """
    Parse a LA election results Excel file.
    Returns list of race dicts with candidates and vote totals.
    """
    wb = openpyxl.load_workbook(filepath, read_only=True)
    ws = wb['Multi-Parish(Parish)']
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    # Extract election date from row 3
    election_date = None
    for row in rows[:5]:
        for cell in row:
            if cell and isinstance(cell, str) and 'Election Date:' in cell:
                # "Election Date: 2023-10-14"
                m = re.search(r'(\d{4}-\d{2}-\d{2})', cell)
                if m:
                    election_date = m.group(1)

    races = []
    i = 0
    while i < len(rows):
        row = rows[i]
        # Look for race header (single cell in first column, rest None)
        if (row and row[0] and isinstance(row[0], str)
                and row[0].strip()
                and all(c is None for c in row[1:6] if len(row) > 1)):

            race_name = row[0].strip()
            chamber, dist_num = parse_race_name(race_name)

            if chamber is not None and i + 2 < len(rows):
                # Next row: candidate names
                cand_row = rows[i + 1]
                # Row after: Total Votes
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
                        races.append({
                            'race_name': race_name,
                            'chamber': chamber,
                            'district_number': dist_num,
                            'candidates': candidates,
                            'total_votes': total_votes,
                        })
        i += 1

    return election_date, races


def determine_election_type_and_results(candidates, is_runoff):
    """
    Determine election type and candidate results for LA jungle primary.

    Primary (jungle): if top candidate gets >50%, they win. Otherwise top 2 advance.
    Runoff: top vote-getter wins.
    """
    if not candidates:
        return 'General', candidates

    total = sum(c['votes'] for c in candidates)
    # Sort by votes descending
    candidates.sort(key=lambda c: c['votes'], reverse=True)

    if is_runoff:
        # Runoff: winner is top vote-getter
        for i, c in enumerate(candidates):
            c['result'] = 'Won' if i == 0 and c['votes'] > 0 else 'Lost'
        return 'General_Runoff', candidates
    else:
        # Jungle primary
        top_votes = candidates[0]['votes']
        majority = total / 2.0

        if top_votes > majority:
            # Won outright
            for i, c in enumerate(candidates):
                c['result'] = 'Won' if i == 0 else 'Lost'
        else:
            # Top 2 advance to runoff
            for i, c in enumerate(candidates):
                c['result'] = 'Advanced' if i < 2 else 'Lost'
        return 'General', candidates


# ══════════════════════════════════════════════════════════════════════
# File discovery
# ══════════════════════════════════════════════════════════════════════

def find_files(download_dir):
    """
    Find LA election result Excel files.
    Expected naming: Election+Results+(MM-DD-YYYY).xlsx
    """
    files = {}
    for path in glob.glob(os.path.join(download_dir, 'Election+Results+*.xlsx')):
        basename = os.path.basename(path)
        m = re.search(r'\((\d{1,2})-(\d{1,2})-(\d{4})\)', basename)
        if m:
            month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            date_str = f'{year}-{month:02d}-{day:02d}'
            files[date_str] = path
    return files


def classify_election_date(date_str):
    """
    Classify a LA election date as primary or runoff.
    LA jungle primaries are typically in October of odd years.
    Runoffs follow ~5 weeks later in November.
    """
    year = int(date_str[:4])
    month = int(date_str[5:7])

    # October = primary (jungle), November/December = runoff
    if month <= 10:
        return year, False  # primary
    else:
        return year, True   # runoff


# ══════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Populate LA historical elections')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--year', type=int, help='Process single year')
    parser.add_argument('--year-from', type=int, default=1983)
    parser.add_argument('--year-to', type=int, default=2023)
    parser.add_argument('--download-dir', default=os.path.expanduser('~/Downloads'))
    args = parser.parse_args()

    if args.year:
        args.year_from = args.year
        args.year_to = args.year

    # ── Find data files ──
    all_files = find_files(args.download_dir)
    if not all_files:
        print(f'No LA election result files found in {args.download_dir}')
        sys.exit(1)

    # Classify and filter files
    files_to_process = []
    for date_str, path in sorted(all_files.items()):
        year, is_runoff = classify_election_date(date_str)
        if year < args.year_from or year > args.year_to:
            continue
        files_to_process.append({
            'date': date_str,
            'year': year,
            'is_runoff': is_runoff,
            'path': path,
        })

    print(f'Found {len(files_to_process)} files to process:')
    for f in files_to_process:
        label = 'RUNOFF' if f['is_runoff'] else 'PRIMARY'
        print(f'  {f["date"]} ({label}): {os.path.basename(f["path"])}')

    # ── Load seat map ──
    print('\nLoading LA seat data...')
    rows = run_sql("""
        SELECT s.id AS seat_id, d.chamber, d.district_number
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'LA' AND d.chamber IN ('House', 'Senate')
    """)
    seat_map = {}
    for r in rows:
        seat_map[(r['chamber'], int(r['district_number']))] = r['seat_id']
    print(f'  {len(seat_map)} seats loaded')

    # ── Load existing elections ──
    print('Loading existing LA elections...')
    existing = run_sql("""
        SELECT e.id, e.seat_id, e.election_year, e.election_type, e.election_date
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'LA' AND d.chamber IN ('House', 'Senate')
    """)
    existing_elections = {}
    for r in existing:
        key = (r['seat_id'], r['election_year'], r['election_type'])
        existing_elections[key] = r['id']
    print(f'  {len(existing_elections)} existing legislative elections')

    # ── Load existing candidacies ──
    print('Loading existing candidacies...')
    existing_candys = set()
    if existing_elections:
        eids = ','.join(str(eid) for eid in existing_elections.values())
        rows = run_sql(f"""
            SELECT election_id, candidate_id FROM candidacies
            WHERE election_id IN ({eids})
        """)
        for r in rows:
            existing_candys.add((r['election_id'], r['candidate_id']))
    print(f'  {len(existing_candys)} existing candidacies')

    # ── Initialize candidate lookup ──
    print('Loading candidate data for LA...')
    candidate_lookup = CandidateLookup(run_sql)
    candidate_lookup.load_state('LA')

    # ── Process files ──
    stats = {
        'elections_created': 0, 'elections_skipped': 0,
        'candidacies_created': 0, 'candidacies_skipped': 0,
        'candidates_created': 0, 'candidates_reused': 0,
        'races_parsed': 0,
        'unknown_districts': [],
        'party_counts': defaultdict(int),
    }

    for file_info in files_to_process:
        filepath = file_info['path']
        year = file_info['year']
        is_runoff = file_info['is_runoff']
        label = 'RUNOFF' if is_runoff else 'PRIMARY'

        print(f'\n{"="*60}')
        print(f'Processing {year} {label}: {os.path.basename(filepath)}')
        print(f'{"="*60}')

        election_date, races = parse_excel_file(filepath)
        print(f'  Election date: {election_date}')
        print(f'  {len(races)} legislative races found')
        stats['races_parsed'] += len(races)

        elections_to_insert = []
        candidacies_to_insert = []

        for race in races:
            chamber = race['chamber']
            dist_num = race['district_number']

            seat_key = (chamber, dist_num)
            seat_id = seat_map.get(seat_key)
            if not seat_id:
                if seat_key not in stats['unknown_districts']:
                    stats['unknown_districts'].append(seat_key)
                    print(f'  WARNING: No seat for {chamber} district {dist_num}')
                continue

            # Determine election type and results
            election_type, candidates = determine_election_type_and_results(
                race['candidates'], is_runoff)

            # Check existing
            existing_key = (seat_id, year, election_type)
            election_id = existing_elections.get(existing_key)

            if election_id:
                stats['elections_skipped'] += 1
            else:
                elections_to_insert.append({
                    'seat_id': seat_id,
                    'year': year,
                    'type': election_type,
                    'date': election_date,
                    'total_votes': race['total_votes'],
                })

            for cand in candidates:
                stats['party_counts'][cand['party']] += 1

                # Split name into first/last
                parts = cand['name'].split()
                first_name = parts[0] if parts else ''
                last_name = parts[-1] if len(parts) > 1 else parts[0] if parts else ''

                pct = round(100 * cand['votes'] / race['total_votes'], 2) if race['total_votes'] > 0 else 0

                candidacies_to_insert.append({
                    'seat_id': seat_id,
                    'year': year,
                    'election_type': election_type,
                    'full_name': cand['name'],
                    'first_name': first_name,
                    'last_name': last_name,
                    'party': cand['party'],
                    'votes': cand['votes'],
                    'pct': pct,
                    'result': cand['result'],
                    'is_major': cand['party'] in ('D', 'R'),
                    'existing_election_id': election_id,
                })

        # ── Insert elections ──
        if elections_to_insert:
            print(f'  Creating {len(elections_to_insert)} elections...')
            if not args.dry_run:
                batch_size = 200
                for i in range(0, len(elections_to_insert), batch_size):
                    batch = elections_to_insert[i:i + batch_size]
                    values = []
                    for e in batch:
                        values.append(
                            f"({e['seat_id']}, {e['year']}, {esc(e['type'])}, "
                            f"{esc(e['date'])}, {e['total_votes']}, 'Certified')"
                        )
                    sql = f"""
                        INSERT INTO elections
                            (seat_id, election_year, election_type,
                             election_date, total_votes_cast, result_status)
                        VALUES {', '.join(values)}
                        ON CONFLICT DO NOTHING
                        RETURNING id, seat_id, election_year, election_type
                    """
                    result = run_sql(sql)
                    if result:
                        for r in result:
                            key = (r['seat_id'], r['election_year'], r['election_type'])
                            existing_elections[key] = r['id']
                        stats['elections_created'] += len(result)
                    if i + batch_size < len(elections_to_insert):
                        time.sleep(3)
        else:
            print('  No new elections to create')

        # ── Insert candidacies ──
        if candidacies_to_insert:
            print(f'  Processing {len(candidacies_to_insert)} candidacies...')
            candy_values = []
            for c in candidacies_to_insert:
                election_id = c['existing_election_id']
                if not election_id:
                    key = (c['seat_id'], c['year'], c['election_type'])
                    election_id = existing_elections.get(key)
                if not election_id:
                    stats['candidacies_skipped'] += 1
                    continue

                if args.dry_run:
                    existing_cid = candidate_lookup.find_match(c['full_name'], 'LA')
                    if existing_cid:
                        stats['candidates_reused'] += 1
                    else:
                        stats['candidates_created'] += 1
                    continue

                existing_cid = candidate_lookup.find_match(c['full_name'], 'LA')
                if existing_cid:
                    cand_id = existing_cid
                    stats['candidates_reused'] += 1
                else:
                    cand_id = candidate_lookup.find_or_create(
                        full_name=c['full_name'],
                        state='LA',
                        first_name=c['first_name'],
                        last_name=c['last_name'],
                    )
                    stats['candidates_created'] += 1

                if (election_id, cand_id) in existing_candys:
                    stats['candidacies_skipped'] += 1
                    continue

                candy_values.append(
                    f"({election_id}, {cand_id}, {esc(c['party'])}, "
                    f"{esc(c['result'])}, {c['votes']}, {c['pct']}, "
                    f"{'TRUE' if c['is_major'] else 'FALSE'}, FALSE)"
                )
                existing_candys.add((election_id, cand_id))

            if candy_values and not args.dry_run:
                print(f'  Inserting {len(candy_values)} candidacies...')
                batch_size = 200
                for i in range(0, len(candy_values), batch_size):
                    batch = candy_values[i:i + batch_size]
                    sql = f"""
                        INSERT INTO candidacies
                            (election_id, candidate_id, party, result,
                             votes_received, vote_percentage, is_major, is_write_in)
                        VALUES {', '.join(batch)}
                        ON CONFLICT DO NOTHING
                    """
                    result = run_sql(sql, exit_on_error=False)
                    if result is not None:
                        stats['candidacies_created'] += len(batch)
                    if i + batch_size < len(candy_values):
                        time.sleep(3)

    # ── Summary ──
    print(f'\n{"="*60}')
    print('SUMMARY')
    print(f'{"="*60}')
    print(f'Races parsed:        {stats["races_parsed"]}')
    print(f'Elections created:    {stats["elections_created"]}')
    print(f'Elections skipped:    {stats["elections_skipped"]} (already existed)')
    print(f'Candidates reused:   {stats["candidates_reused"]}')
    print(f'Candidates created:  {stats["candidates_created"]}')
    print(f'Candidacies created: {stats["candidacies_created"]}')
    print(f'Candidacies skipped: {stats["candidacies_skipped"]}')
    if stats['unknown_districts']:
        print(f'Unknown districts:   {stats["unknown_districts"]}')
    print(f'\nParty distribution:')
    for party, count in sorted(stats['party_counts'].items(),
                               key=lambda x: -x[1]):
        print(f'  {party}: {count}')


if __name__ == '__main__':
    main()
