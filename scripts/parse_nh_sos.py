"""
Parse NH Secretary of State Excel files for House general election results.

The SoS publishes one Excel file per county with all House district results.
Each district section has candidate names + party, town-by-town votes, and totals.
Multi-member districts use top-N bloc voting (no separate seat races).

District numbering maps directly: SoS County-N → DB County-N (verified).
Seat counts may differ between SoS and DB for some districts due to redistricting.

Usage:
    python3 scripts/parse_nh_sos.py --year 2024 --dry-run
    python3 scripts/parse_nh_sos.py --year 2022
    python3 scripts/parse_nh_sos.py --year 2024 --county belknap --dry-run --debug
"""
import os
import re
import sys
import time
import argparse
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

import httpx

try:
    import xlrd
except ImportError:
    xlrd = None

try:
    import openpyxl
except ImportError:
    openpyxl = None

# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════

TMP_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'tmp')
BATCH_SIZE = 250
MAX_RETRIES = 5
DEBUG = False

NH_COUNTIES = [
    'belknap', 'carroll', 'cheshire', 'coos', 'grafton',
    'hillsborough', 'merrimack', 'rockingham', 'strafford', 'sullivan',
]


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def debug(msg):
    if DEBUG:
        print(f'  DEBUG: {msg}')


def esc(s):
    if s is None:
        return ''
    return str(s).replace("'", "''")


def run_sql(query, exit_on_error=False):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = httpx.post(
                API_URL,
                headers={'Authorization': f'Bearer {TOKEN}',
                         'Content-Type': 'application/json'},
                json={'query': query},
                timeout=120
            )
            if resp.status_code == 201:
                return resp.json()
            elif resp.status_code == 429:
                wait = 5 * attempt
                print(f'  Rate limited (429), waiting {wait}s...')
                time.sleep(wait)
                continue
            else:
                print(f'  SQL ERROR ({resp.status_code}): {resp.text[:500]}')
                if exit_on_error:
                    sys.exit(1)
                if attempt < MAX_RETRIES:
                    time.sleep(5 * attempt)
                    continue
                return None
        except Exception as e:
            print(f'  HTTP error: {e}')
            if attempt < MAX_RETRIES:
                time.sleep(5 * attempt)
                continue
            if exit_on_error:
                sys.exit(1)
            return None
    return None


# ═══════════════════════════════════════════════════════════════
# EXCEL PARSING
# ═══════════════════════════════════════════════════════════════

def find_county_file(county, year):
    """Find the SoS Excel file for a county and year."""
    import glob
    pattern = os.path.join(TMP_DIR, f'{year}-ge-house-{county}*')
    matches = glob.glob(pattern)
    if matches:
        return matches[0]
    return None


def read_excel(filepath):
    """Read an Excel file (.xls or .xlsx) and return rows as list of lists."""
    if filepath.endswith('.xls'):
        if xlrd is None:
            print('ERROR: xlrd required for .xls files. Install: pip install xlrd')
            sys.exit(1)
        wb = xlrd.open_workbook(filepath)
        ws = wb.sheet_by_index(0)
        rows = []
        for r in range(ws.nrows):
            row = []
            for c in range(ws.ncols):
                val = ws.cell_value(r, c)
                row.append(val)
            rows.append(row)
        return rows
    else:
        if openpyxl is None:
            print('ERROR: openpyxl required for .xlsx files. Install: pip install openpyxl')
            sys.exit(1)
        wb = openpyxl.load_workbook(filepath, data_only=True)
        ws = wb.active
        rows = []
        for row in ws.iter_rows(values_only=True):
            rows.append(list(row))
        return rows


def parse_county_file(filepath, county_name):
    """
    Parse an NH SoS county Excel file.

    Returns list of district dicts:
    [
        {
            'sos_district_num': '5',
            'num_seats': 4,
            'is_floterial': False,
            'candidates': [
                {'name': 'Charlie St. Clair', 'party': 'D', 'votes': 3641},
                {'name': 'Mike Bordes', 'party': 'R', 'votes': 4040},
                ...
            ]
        }
    ]
    """
    rows = read_excel(filepath)
    districts = []
    current_district = None
    in_second_block = False
    last_data_row = None  # Track last town data row for single-town districts

    def _finalize_block(district, data_row, second_block):
        """For single-town districts with no Totals row, use the last data row."""
        if data_row is None or district is None:
            return
        # Check if any candidates in the current block still lack votes
        cands = district['candidates']
        if second_block:
            needs_votes = any(c['votes'] is None for c in cands
                              if c.get('_col_offset') is not None and c['votes'] is None)
        else:
            needs_votes = any(c['votes'] is None for c in cands)
        if needs_votes:
            debug(f'  No Totals row — using town data as totals')
            _extract_totals(data_row, district, second_block)

    for row in rows:
        col0 = str(row[0]).strip() if row[0] is not None else ''

        # Check for district header: "District No. N (seats)" or "District N (seats)" or with FL suffix
        # Skip RECOUNT entries (e.g., "District No. 4 (3) RECOUNT FIG")
        if 'RECOUNT' in col0.upper():
            continue
        dist_match = re.match(
            r'District(?:\s+No\.?)?\s*(\d+)\s*\((\d+)\)\s*(FL|F)?',
            col0, re.IGNORECASE
        )

        if dist_match:
            # Finalize previous district (handle single-town missing Totals)
            _finalize_block(current_district, last_data_row, in_second_block)
            if current_district and current_district['candidates']:
                districts.append(current_district)

            sos_num = dist_match.group(1)
            num_seats = int(dist_match.group(2))
            is_fl = bool(dist_match.group(3))

            current_district = {
                'sos_district_num': sos_num,
                'num_seats': num_seats,
                'is_floterial': is_fl,
                'candidates': [],
            }
            in_second_block = False
            last_data_row = None

            # Parse candidate names from this header row
            _extract_candidates_from_header(row, current_district)
            debug(f'District {sos_num} ({num_seats} seats){"FL" if is_fl else ""}: '
                  f'{len(current_district["candidates"])} candidates from header')
            continue

        # Check for second candidate block (empty col0, candidate names in cols 1+)
        if current_district and not col0 and _row_has_candidate_names(row):
            # Finalize first block if it has no Totals (single-town before 2nd block)
            _finalize_block(current_district, last_data_row, False)
            in_second_block = True
            last_data_row = None
            _extract_candidates_from_header(row, current_district)
            debug(f'  Second block: +candidates, total now {len(current_district["candidates"])}')
            continue

        # Check for Totals row — extract vote totals
        if col0 == 'Totals' and current_district:
            _extract_totals(row, current_district, in_second_block)
            last_data_row = None  # Clear — Totals already handled
            if in_second_block:
                in_second_block = False
            continue

        # Track town data rows (for single-town districts without Totals)
        if current_district and col0:
            has_numeric = any(isinstance(v, (int, float)) and v > 0 for v in row[1:])
            if has_numeric:
                last_data_row = row

    # Don't forget the last district
    _finalize_block(current_district, last_data_row, in_second_block)
    if current_district and current_district['candidates']:
        districts.append(current_district)

    return districts


def _row_has_candidate_names(row):
    """Check if a row contains candidate name strings (e.g., 'Name, d')."""
    for val in row[1:]:
        if val and isinstance(val, str):
            val = val.strip()
            if re.match(r'.+,\s*[dr]$', val, re.IGNORECASE):
                return True
            if val == 'RECOUNT':
                return True
    return False


def _extract_candidates_from_header(row, district):
    """Extract candidate names and parties from a header row."""
    start_idx = len(district['candidates'])

    for val in row[1:]:
        if val is None:
            continue
        val = str(val).strip()
        if not val or val in ('Undervotes', 'Overvotes', 'Write-Ins', 'RECOUNT'):
            continue

        # Pattern: "Name, d" or "Name, r"
        m = re.match(r'(.+),\s*([dr])$', val, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            party = 'D' if m.group(2).lower() == 'd' else 'R'
            district['candidates'].append({
                'name': name,
                'party': party,
                'votes': None,
                '_col_offset': start_idx,  # Track which block this candidate belongs to
            })
            start_idx += 1


def _extract_totals(row, district, is_second_block):
    """Extract vote totals from a Totals row and assign to candidates."""
    # The totals row has vote counts in columns matching the candidate positions
    # First block candidates start at col 1, second block also at col 1
    candidates = district['candidates']

    if is_second_block:
        # Find candidates from the second block (those without votes yet from this block)
        second_block_cands = [c for c in candidates if c.get('_col_offset') is not None
                              and c['votes'] is None]
    else:
        # First block — candidates that don't have votes yet
        second_block_cands = None

    col_idx = 1
    for val in row[1:]:
        if val is None or (isinstance(val, str) and not val.strip()):
            col_idx += 1
            continue

        try:
            votes = int(float(val))
        except (ValueError, TypeError):
            col_idx += 1
            continue

        # Assign to the appropriate candidate
        if is_second_block and second_block_cands:
            cand_idx = col_idx - 1
            if cand_idx < len(second_block_cands):
                second_block_cands[cand_idx]['votes'] = votes
        else:
            # First block: candidate index = col_idx - 1
            cand_idx = col_idx - 1
            if cand_idx < len(candidates):
                candidates[cand_idx]['votes'] = votes

        col_idx += 1


# ═══════════════════════════════════════════════════════════════
# DB MATCHING — Match SoS districts to DB districts by holders
# ═══════════════════════════════════════════════════════════════

def load_nh_db_context(year):
    """Load NH House districts, seats, elections, and current holders from DB."""
    # Select redistricting cycle based on election year
    cycle = '2022' if year >= 2022 else '2012'
    print(f'  Using redistricting_cycle={cycle} for year {year}')

    # Districts + seats (filtered by redistricting cycle)
    seats_data = run_sql(f"""
        SELECT se.id as seat_id, se.seat_designator, se.seat_label,
               d.id as district_id, d.district_number, d.num_seats, d.is_floterial
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NH'
          AND se.office_type = 'State House'
          AND d.chamber = 'House'
          AND d.redistricting_cycle = '{cycle}'
        ORDER BY d.district_number, se.seat_designator
    """)
    if not seats_data:
        print('  ERROR: No NH House seats found')
        return None

    # Existing elections for this year
    elections_data = run_sql(f"""
        SELECT e.id as election_id, e.seat_id
        FROM elections e
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NH' AND d.chamber = 'House'
          AND e.election_year = {year} AND e.election_type = 'General'
    """)

    existing_election_seats = set()
    if elections_data:
        for e in elections_data:
            existing_election_seats.add(e['seat_id'])

    # Current holders (for district matching)
    holders_data = run_sql("""
        SELECT d.district_number, c.full_name, c.last_name
        FROM seat_terms st2
        JOIN seats s ON st2.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON st2.candidate_id = c.id
        WHERE st.abbreviation = 'NH' AND d.chamber = 'House'
          AND st2.end_date IS NULL
    """)

    holders_by_district = defaultdict(list)
    if holders_data:
        for h in holders_data:
            holders_by_district[h['district_number']].append(h['full_name'])

    # All candidates for name matching
    cands_data = run_sql("SELECT id, full_name, last_name FROM candidates")
    candidates_by_name = defaultdict(list)
    if cands_data:
        for c in cands_data:
            if c['last_name']:
                candidates_by_name[c['last_name'].lower().strip()].append(
                    (c['id'], c['full_name'])
                )

    return {
        'seats': seats_data,
        'existing_election_seats': existing_election_seats,
        'holders_by_district': holders_by_district,
        'candidates_by_name': candidates_by_name,
    }


def _extract_last_name(full_name):
    """Extract last name from a full name, handling suffixes."""
    parts = full_name.split()
    if not parts:
        return ''
    last = parts[-1].lower().strip().rstrip('.')
    if last in ('jr', 'sr', 'ii', 'iii', 'iv', 'jr.', 'sr.'):
        last = parts[-2].lower().strip() if len(parts) >= 2 else last
    return last


def match_district(sos_district, county, db_context):
    """
    Match an SoS district to a DB district.

    Primary strategy: direct number mapping (SoS County-N → DB County-N).
    Verified that SoS numbering matches DB (Ballotpedia) numbering across all counties.
    Seat counts may differ (DB may have stale redistricting data) but numbers align.

    Fallback: holder name overlap (for edge cases where direct mapping fails).

    Returns the DB district_number or None.
    """
    county_title = county.title()
    sos_num = sos_district['sos_district_num']

    # Primary: direct number mapping
    direct_name = f'{county_title}-{sos_num}'
    db_districts = set(s['district_number'] for s in db_context['seats'])
    if direct_name in db_districts:
        debug(f'  Matched SoS {county}-{sos_num} → DB {direct_name} (direct)')
        return direct_name

    # Fallback: name-based matching (ignoring seat count)
    num_seats = sos_district['num_seats']
    candidates = sorted(
        [c for c in sos_district['candidates'] if c['votes'] is not None],
        key=lambda c: c['votes'],
        reverse=True
    )
    all_cand_last_names = set(_extract_last_name(c['name']) for c in candidates)
    # Include uncontested candidates with None votes
    for c in sos_district['candidates']:
        if c['votes'] is None:
            ln = _extract_last_name(c['name'])
            if ln:
                all_cand_last_names.add(ln)

    best_match = None
    best_score = 0
    for dist_num in db_districts:
        if not dist_num.startswith(county_title + '-'):
            continue
        holders = db_context['holders_by_district'].get(dist_num, [])
        if not holders:
            continue
        holder_last_names = set(_extract_last_name(h) for h in holders)
        overlap = len(all_cand_last_names & holder_last_names)
        if overlap > best_score:
            best_score = overlap
            best_match = dist_num

    if best_match and best_score > 0:
        debug(f'  Matched SoS {county}-{sos_num} → DB {best_match} '
              f'(name fallback, score: {best_score})')
        return best_match

    return None


# ═══════════════════════════════════════════════════════════════
# POPULATION
# ═══════════════════════════════════════════════════════════════

def election_date_for_year(year):
    """Return the NH general election date for a given year (first Tues after first Mon in Nov)."""
    import datetime
    nov1 = datetime.date(year, 11, 1)
    # First Monday: if Nov 1 is Mon, that's it; else advance to next Mon
    days_to_mon = (7 - nov1.weekday()) % 7  # 0=Mon
    first_mon = nov1 + datetime.timedelta(days=days_to_mon)
    # First Tuesday after first Monday
    return first_mon + datetime.timedelta(days=1)


def populate_nh(all_districts, db_context, year, dry_run=False):
    """Insert elections + candidacies for matched NH districts."""
    seats_by_district = defaultdict(list)
    for s in db_context['seats']:
        seats_by_district[s['district_number']].append(s)

    elections_to_insert = []
    total_candidates = 0
    skipped_existing = 0
    skipped_no_match = 0

    for county, dist in all_districts:
        db_dist_num = dist.get('_db_district')
        if not db_dist_num:
            skipped_no_match += 1
            continue

        # Get seats for this district (sorted by designator)
        district_seats = sorted(
            seats_by_district.get(db_dist_num, []),
            key=lambda s: s['seat_designator'] or ''
        )

        if not district_seats:
            skipped_no_match += 1
            continue

        # Sort candidates by votes (top N = winners)
        candidates = sorted(
            [c for c in dist['candidates'] if c['votes'] is not None],
            key=lambda c: c['votes'],
            reverse=True
        )
        sos_num_seats = dist['num_seats']
        db_num_seats = len(district_seats)
        # Use actual SoS seat count for winner determination, but only fill DB seats
        effective_seats = min(sos_num_seats, db_num_seats)
        total_votes = sum(c['votes'] for c in candidates)

        if sos_num_seats != db_num_seats:
            debug(f'  Seat count mismatch {db_dist_num}: SoS={sos_num_seats}, DB={db_num_seats}, '
                  f'using {effective_seats}')

        # Assign winners to seats by vote order (only up to effective_seats)
        for seat_idx, seat in enumerate(district_seats):
            if seat_idx >= effective_seats:
                break  # Don't create elections beyond effective seat count

            seat_id = seat['seat_id']

            # Check if election already exists
            if seat_id in db_context['existing_election_seats']:
                skipped_existing += 1
                continue

            # Build candidate list for this seat
            # Winner = candidates[seat_idx] (if they exist)
            seat_candidates = []
            if seat_idx < len(candidates):
                winner = candidates[seat_idx]
                winner_entry = {
                    'name': winner['name'],
                    'party': winner['party'],
                    'votes': winner['votes'],
                    'pct': round(100 * winner['votes'] / total_votes, 2) if total_votes else None,
                    'winner': True,
                    'incumbent': False,
                }
                seat_candidates.append(winner_entry)

            # All non-winners as losers
            for i, c in enumerate(candidates):
                if i < sos_num_seats:
                    if i != seat_idx:
                        continue  # Other winners go to their own seats
                else:
                    seat_candidates.append({
                        'name': c['name'],
                        'party': c['party'],
                        'votes': c['votes'],
                        'pct': round(100 * c['votes'] / total_votes, 2) if total_votes else None,
                        'winner': False,
                        'incumbent': False,
                    })

            edate = election_date_for_year(year)
            elections_to_insert.append({
                'seat_id': seat_id,
                'election_date': edate.isoformat(),
                'year': year,
                'election_type': 'General',
                'candidates': seat_candidates,
                'dist_num': db_dist_num,
                'seat_desig': seat['seat_designator'],
            })
            total_candidates += len(seat_candidates)

    print(f'  Elections to insert: {len(elections_to_insert)}')
    print(f'  Candidates to process: {total_candidates}')
    print(f'  Skipped (already exist): {skipped_existing}')
    print(f'  Skipped (no DB match): {skipped_no_match}')

    if dry_run:
        for e in elections_to_insert[:5]:
            print(f'    {e["dist_num"]} Seat {e["seat_desig"]}: '
                  f'{len(e["candidates"])} candidates')
            for c in e['candidates']:
                w = ' *WINNER*' if c['winner'] else ''
                print(f'      {c["name"]} ({c["party"]}) {c["votes"]} votes{w}')
        if len(elections_to_insert) > 5:
            print(f'    ... and {len(elections_to_insert) - 5} more')
        return len(elections_to_insert), total_candidates, 0

    if not elections_to_insert:
        print('  Nothing to insert.')
        return 0, 0, 0

    # Insert elections
    print(f'\n  Inserting {len(elections_to_insert)} elections...')
    election_values = []
    for e in elections_to_insert:
        election_values.append(
            f"({e['seat_id']}, '{e['election_date']}', {e['year']}, 'General', NULL)"
        )

    seat_to_election_id = {}
    total_inserted = 0
    for batch_start in range(0, len(election_values), BATCH_SIZE):
        batch = election_values[batch_start:batch_start + BATCH_SIZE]
        sql = (
            "INSERT INTO elections "
            "(seat_id, election_date, election_year, election_type, related_election_id) "
            "VALUES " + ",\n".join(batch) + "\nRETURNING id, seat_id;"
        )
        result = run_sql(sql)
        if result:
            for row in result:
                seat_to_election_id[row['seat_id']] = row['id']
            total_inserted += len(result)
            print(f'    Batch: +{len(result)} elections (total: {total_inserted})')
        else:
            print(f'    ERROR: Election insert failed!')
            return total_inserted, 0, 0
        time.sleep(1)

    # Create candidates + candidacies
    print(f'\n  Processing candidates...')
    candidates_by_name = db_context['candidates_by_name']
    all_candidacies = []
    new_candidates_count = 0
    candidacies_count = 0

    for e in elections_to_insert:
        election_id = seat_to_election_id.get(e['seat_id'])
        if not election_id:
            continue
        for cand in e['candidates']:
            cand_id = _find_candidate_id(cand['name'], candidates_by_name)
            all_candidacies.append({
                'election_id': election_id,
                'candidate_id': cand_id,
                'name': cand['name'],
                'party': cand['party'],
                'votes': cand['votes'],
                'pct': cand['pct'],
                'winner': cand['winner'],
                'incumbent': cand['incumbent'],
            })

    # Insert new candidates
    new_cands = [c for c in all_candidacies if c['candidate_id'] is None]
    if new_cands:
        print(f'  Creating {len(new_cands)} new candidates...')
        cand_values = []
        for c in new_cands:
            parts = c['name'].split()
            first = esc(parts[0]) if parts else ''
            last = esc(parts[-1]) if len(parts) > 1 else first
            full = esc(c['name'])
            cand_values.append(f"('{full}', '{first}', '{last}', NULL)")

        new_ids = []
        for batch_start in range(0, len(cand_values), BATCH_SIZE):
            batch = cand_values[batch_start:batch_start + BATCH_SIZE]
            sql = (
                "INSERT INTO candidates (full_name, first_name, last_name, gender) VALUES\n"
                + ",\n".join(batch) + "\nRETURNING id;"
            )
            result = run_sql(sql)
            if result:
                new_ids.extend(r['id'] for r in result)
            else:
                print(f'    ERROR: Candidate insert failed!')
                return total_inserted, 0, 0
            time.sleep(1)

        for i, c in enumerate(new_cands):
            if i < len(new_ids):
                c['candidate_id'] = new_ids[i]
                parts = c['name'].split()
                last = parts[-1].lower().strip() if parts else ''
                candidates_by_name[last].append((new_ids[i], c['name']))
        new_candidates_count = len(new_ids)
        print(f'  Created {new_candidates_count} new candidates')

    # Insert candidacies
    ready = [c for c in all_candidacies if c['candidate_id'] is not None]
    if ready:
        print(f'  Inserting {len(ready)} candidacies...')
        cand_batch = []
        for c in ready:
            votes_sql = str(c['votes']) if c['votes'] is not None else 'NULL'
            pct_sql = str(c['pct']) if c['pct'] is not None else 'NULL'
            result_val = "'Won'" if c['winner'] else "'Lost'"
            party_sql = f"'{esc(c['party'])}'" if c['party'] else 'NULL'
            cand_batch.append(
                f"({c['election_id']}, {c['candidate_id']}, {party_sql}, "
                f"'Active', {str(c['incumbent']).lower()}, false, "
                f"NULL, NULL, {votes_sql}, {pct_sql}, "
                f"{result_val}, NULL, NULL)"
            )

        for batch_start in range(0, len(cand_batch), BATCH_SIZE):
            batch = cand_batch[batch_start:batch_start + BATCH_SIZE]
            sql = (
                "INSERT INTO candidacies "
                "(election_id, candidate_id, party, candidate_status, "
                "is_incumbent, is_write_in, filing_date, withdrawal_date, "
                "votes_received, vote_percentage, result, endorsements, notes) "
                "VALUES\n" + ",\n".join(batch) + "\nRETURNING id;"
            )
            result = run_sql(sql)
            if result:
                candidacies_count += len(result)
                print(f'    Batch: +{len(result)} candidacies (total: {candidacies_count})')
            else:
                print(f'    ERROR: Candidacy insert failed!')
            time.sleep(1)

    print(f'  Total: {total_inserted} elections, {candidacies_count} candidacies, '
          f'{new_candidates_count} new candidates')
    return total_inserted, candidacies_count, new_candidates_count


def _find_candidate_id(name, candidates_by_name):
    """Find a candidate ID by name matching."""
    parts = name.split()
    if not parts:
        return None
    last_name = parts[-1].lower().strip()
    if last_name in ('jr.', 'jr', 'sr.', 'sr', 'ii', 'iii', 'iv'):
        if len(parts) >= 3:
            last_name = parts[-2].lower().strip()

    matches = candidates_by_name.get(last_name, [])
    if not matches:
        return None

    best_id = None
    best_score = 0
    for cand_id, full_name in matches:
        score = _name_similarity(name, full_name)
        if score > best_score:
            best_score = score
            best_id = cand_id
    if best_score >= 0.7:
        return best_id
    return None


def _name_similarity(name1, name2):
    def normalize(n):
        n = re.sub(r'\s+(Jr\.?|Sr\.?|III|IV|II)\s*$', '', n, flags=re.IGNORECASE)
        return n.strip().lower()
    n1 = normalize(name1)
    n2 = normalize(name2)
    if n1 == n2:
        return 1.0
    parts1 = n1.split()
    parts2 = n2.split()
    if not parts1 or not parts2:
        return 0.0
    last1, last2 = parts1[-1], parts2[-1]
    if last1 != last2:
        if last1 not in parts2 and last2 not in parts1:
            return 0.0
    first1, first2 = parts1[0], parts2[0]
    if first1 == first2:
        return 0.9
    if first1.startswith(first2) or first2.startswith(first1):
        return 0.8
    if first1[0] == first2[0] and len(first1) >= 3 and len(first2) >= 3:
        return 0.7
    return 0.3


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    global DEBUG
    parser = argparse.ArgumentParser(
        description='Parse NH SoS Excel files for House general election results'
    )
    parser.add_argument('--year', type=int, required=True,
                        help='Election year (e.g., 2022, 2024)')
    parser.add_argument('--county', type=str,
                        help='Process a single county (e.g., belknap)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and match only, no DB inserts')
    parser.add_argument('--debug', action='store_true',
                        help='Show debug output')
    args = parser.parse_args()
    DEBUG = args.debug

    if args.dry_run:
        print('DRY RUN MODE — no database changes will be made.\n')

    year = args.year
    counties = [args.county.lower()] if args.county else NH_COUNTIES

    # Load DB context once
    print(f'Loading NH database context for {year}...')
    db_context = load_nh_db_context(year)
    if not db_context:
        sys.exit(1)
    print(f'  {len(db_context["seats"])} seats, '
          f'{len(db_context["existing_election_seats"])} existing elections, '
          f'{sum(len(v) for v in db_context["holders_by_district"].values())} holders\n')

    all_matched = []

    for county in counties:
        filepath = find_county_file(county, year)
        if not filepath:
            print(f'  WARNING: No file found for {county}')
            continue

        print(f'Parsing {county.title()} County: {os.path.basename(filepath)}')
        districts = parse_county_file(filepath, county)
        print(f'  Found {len(districts)} districts')

        # Match to DB
        matched = 0
        unmatched = 0
        for dist in districts:
            db_dist = match_district(dist, county, db_context)
            if db_dist:
                dist['_db_district'] = db_dist
                matched += 1
            else:
                debug(f'  UNMATCHED: SoS {county}-{dist["sos_district_num"]} '
                      f'({dist["num_seats"]} seats, FL={dist["is_floterial"]})')
                unmatched += 1
            all_matched.append((county, dist))

        print(f'  Matched: {matched}, Unmatched: {unmatched}')

    if not all_matched:
        print('No districts to process.')
        sys.exit(0)

    print(f'\nTotal: {len(all_matched)} districts across {len(counties)} counties')
    populate_nh(all_matched, db_context, year=year, dry_run=args.dry_run)
    print('\nDone!')


if __name__ == '__main__':
    main()
