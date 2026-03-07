"""
Populate NJ 2013/2015/2017 Assembly general election results.

Reads parsed JSON from /tmp/nj_elections_all.json and creates:
- Election records (Assembly generals for 2013, 2015, 2017)
- Election record for 2015 Senate D5 special
- New candidate records for candidates not yet in DB
- Candidacy records with votes and results

Senate 2013/2017 already have full candidacy data in the DB.

Usage:
    python3 scripts/populate_nj_old_results.py --dry-run
    python3 scripts/populate_nj_old_results.py
    python3 scripts/populate_nj_old_results.py --year 2015
"""
import sys
import re
import json
import time
import argparse
import unicodedata

import httpx
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

BATCH_SIZE = 400
INPUT_PATH = '/tmp/nj_elections_all.json'

ELECTION_DATES = {
    (2013, 'assembly'): '2013-11-05',
    (2013, 'senate'): '2013-11-05',
    (2015, 'assembly'): '2015-11-03',
    (2015, 'senate'): '2015-11-03',
    (2017, 'assembly'): '2017-11-07',
    (2017, 'senate'): '2017-11-07',
}


def run_sql(query, exit_on_error=True):
    for attempt in range(5):
        resp = httpx.post(
            f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f'  Rate limited, waiting {wait}s...')
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
        return ''
    return str(s).replace("'", "''")


def strip_accents(s):
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def name_similarity(name1, name2):
    if name1 is None or name2 is None:
        return 0.0

    def normalize(n):
        n = re.sub(r'\s+(Jr\.?|Sr\.?|III|IV|II)\s*$', '', n, flags=re.IGNORECASE)
        n = strip_accents(n)
        # Remove quotes/nicknames
        n = re.sub(r'"[^"]*"', '', n)
        return n.strip().lower()

    n1 = normalize(name1)
    n2 = normalize(name2)
    if n1 == n2:
        return 1.0

    parts1 = n1.split()
    parts2 = n2.split()
    if not parts1 or not parts2:
        return 0.0

    last1 = parts1[-1]
    last2 = parts2[-1]
    last_match = (last1 == last2)
    if not last_match:
        if last1 in parts2 or last2 in parts1:
            last_match = True
    if not last_match:
        return 0.0

    first1 = parts1[0]
    first2 = parts2[0]
    if first1 == first2:
        return 0.9
    if first1.startswith(first2) or first2.startswith(first1):
        return 0.8
    if first1[0] == first2[0]:
        return 0.7
    return 0.3


# ══════════════════════════════════════════════════════════════════════
# STEP 1: Load DB Maps
# ══════════════════════════════════════════════════════════════════════

def load_seat_map():
    """Load NJ Assembly and Senate seat data."""
    seats = run_sql("""
        SELECT se.id as seat_id, se.office_type, se.seat_designator,
               d.district_number, d.id as district_id, d.chamber
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'NJ'
          AND se.selection_method = 'Elected'
          AND d.office_level = 'Legislative'
        ORDER BY se.office_type, d.district_number, se.seat_designator
    """)

    # {(office_type, district_number, seat_designator) -> seat_id}
    seat_map = {}
    for s in seats:
        key = (s['office_type'], s['district_number'], s['seat_designator'])
        seat_map[key] = s['seat_id']

    return seat_map


def load_existing_candidates():
    """Load all NJ candidates already in the DB for matching."""
    cands = run_sql("""
        SELECT DISTINCT c.id, c.full_name
        FROM candidates c
        JOIN candidacies ca ON ca.candidate_id = c.id
        JOIN elections e ON ca.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NJ'
    """)
    # Group by name to handle duplicates - prefer lower ID (original)
    by_name = {}
    for c in cands:
        if c['full_name'] not in by_name or c['id'] < by_name[c['full_name']]:
            by_name[c['full_name']] = c['id']
    return by_name  # {full_name -> candidate_id}


def find_candidate_match(name, existing_candidates):
    """Find best matching candidate in DB."""
    best_score = 0
    best_id = None
    for db_name, cand_id in existing_candidates.items():
        score = name_similarity(name, db_name)
        if score > best_score:
            best_score = score
            best_id = cand_id
    if best_score >= 0.8:
        return best_id
    return None


def map_party(party_str, party_code):
    """Map parsed party to DB party code."""
    if party_code == 'D':
        return 'D'
    if party_code == 'R':
        return 'R'
    if party_code == 'L':
        return 'L'
    if party_code == 'G':
        return 'G'
    # Third-party slogans → 'I' (Independent) in our DB
    return 'I'


# ══════════════════════════════════════════════════════════════════════
# STEP 2: Create Elections
# ══════════════════════════════════════════════════════════════════════

def create_elections(year, chamber, districts, seat_map, dry_run=False):
    """Create election records. Returns {(district, designator) -> election_id}."""
    election_date = ELECTION_DATES[(year, chamber)]
    election_map = {}
    values = []

    if chamber == 'assembly':
        for d in districts:
            dn = str(d['district'])
            total = d['total_votes']
            for designator in ['A', 'B']:
                seat_key = ('State House', dn, designator)
                seat_id = seat_map.get(seat_key)
                if not seat_id:
                    print(f'  WARNING: No seat for Assembly D{dn} Seat {designator}')
                    continue
                key = (d['district'], designator)
                values.append(
                    f"({seat_id}, '{election_date}', {year}, 'General', NULL, NULL, NULL, NULL, "
                    f"NULL, NULL, {total}, NULL, 'Certified', false, NULL, NULL)"
                )
                election_map[key] = None
    elif chamber == 'senate':
        for d in districts:
            dn = str(d['district'])
            total = d['total_votes']
            seat_key = ('State Senate', dn, None)
            seat_id = seat_map.get(seat_key)
            if not seat_id:
                print(f'  WARNING: No seat for Senate D{dn}')
                continue
            # 2015 Senate D5 was a special election (vacancy)
            election_type = 'Special' if year == 2015 else 'General'
            key = (d['district'], None)
            values.append(
                f"({seat_id}, '{election_date}', {year}, '{election_type}', NULL, NULL, NULL, NULL, "
                f"NULL, NULL, {total}, NULL, 'Certified', false, NULL, NULL)"
            )
            election_map[key] = None

    print(f'  Elections to create: {len(values)}')

    if dry_run:
        for i, key in enumerate(election_map.keys()):
            election_map[key] = 90000 + i
        return election_map

    # Insert
    all_ids = []
    for batch_start in range(0, len(values), BATCH_SIZE):
        batch = values[batch_start:batch_start + BATCH_SIZE]
        sql = (
            "INSERT INTO elections (seat_id, election_date, election_year, election_type, "
            "related_election_id, filing_deadline, forecast_rating, forecast_source, "
            "pres_margin_this_cycle, previous_result_margin, total_votes_cast, notes, "
            "result_status, is_open_seat, precincts_reporting, precincts_total) VALUES\n"
            + ",\n".join(batch)
            + "\nRETURNING id;"
        )
        result = run_sql(sql, exit_on_error=False)
        if result is None:
            time.sleep(3)
            result = run_sql(sql)
        all_ids.extend(r['id'] for r in result)
        time.sleep(0.8)

    print(f'  Inserted {len(all_ids)} elections')
    keys_list = list(election_map.keys())
    if len(all_ids) != len(keys_list):
        print(f'  ERROR: Expected {len(keys_list)} IDs, got {len(all_ids)}')
        sys.exit(1)
    for i, key in enumerate(keys_list):
        election_map[key] = all_ids[i]

    return election_map


# ══════════════════════════════════════════════════════════════════════
# STEP 3: Create Candidacies
# ══════════════════════════════════════════════════════════════════════

def process_candidacies(year, chamber, districts, election_map, seat_map,
                        existing_candidates, dry_run=False):
    """Create candidate records and candidacies."""
    all_candidacies = []

    for d in districts:
        dn = str(d['district'])

        if chamber == 'assembly':
            winners = [c for c in d['candidates'] if c['winner']]
            losers = [c for c in d['candidates'] if not c['winner']]

            seat_a_id = seat_map.get(('State House', dn, 'A'))
            seat_b_id = seat_map.get(('State House', dn, 'B'))
            if not seat_a_id or not seat_b_id:
                continue

            # Assign winners to Seat A and B (first winner → A, second → B)
            for idx, w in enumerate(winners):
                designator = 'A' if idx == 0 else 'B'
                seat_id = seat_a_id if designator == 'A' else seat_b_id
                key = (d['district'], designator)
                election_id = election_map.get(key)
                if not election_id:
                    continue

                cand_id = find_candidate_match(w['name'], existing_candidates)
                total = d['total_votes']
                pct = round(w['votes'] / total * 100, 1) if total > 0 else None

                all_candidacies.append({
                    'election_id': election_id,
                    'candidate_id': cand_id,
                    'candidate_name': w['name'],
                    'party': map_party(w['party'], w['party_code']),
                    'is_incumbent': w['incumbent'],
                    'votes': w['votes'],
                    'vote_pct': pct,
                    'result': 'Won',
                })

            # Losers go on Seat A election
            key_a = (d['district'], 'A')
            election_id_a = election_map.get(key_a)
            if election_id_a:
                total = d['total_votes']
                for l in losers:
                    cand_id = find_candidate_match(l['name'], existing_candidates)
                    pct = round(l['votes'] / total * 100, 1) if total > 0 else None
                    all_candidacies.append({
                        'election_id': election_id_a,
                        'candidate_id': cand_id,
                        'candidate_name': l['name'],
                        'party': map_party(l['party'], l['party_code']),
                        'is_incumbent': l['incumbent'],
                        'votes': l['votes'],
                        'vote_pct': pct,
                        'result': 'Lost',
                    })

        elif chamber == 'senate':
            key = (d['district'], None)
            election_id = election_map.get(key)
            if not election_id:
                continue

            total = d['total_votes']
            for c in d['candidates']:
                cand_id = find_candidate_match(c['name'], existing_candidates)
                pct = round(c['votes'] / total * 100, 1) if total > 0 else None
                all_candidacies.append({
                    'election_id': election_id,
                    'candidate_id': cand_id,
                    'candidate_name': c['name'],
                    'party': map_party(c['party'], c['party_code']),
                    'is_incumbent': c['incumbent'],
                    'votes': c['votes'],
                    'vote_pct': pct,
                    'result': 'Won' if c['winner'] else 'Lost',
                })

    # Summary
    reuse = [c for c in all_candidacies if c['candidate_id'] is not None]
    new = [c for c in all_candidacies if c['candidate_id'] is None]
    print(f'  Total candidacies: {len(all_candidacies)}')
    print(f'    Matched to existing candidates: {len(reuse)}')
    print(f'    New candidates needed: {len(new)}')

    if dry_run:
        # Show matches for verification
        for c in reuse[:10]:
            db_name = next((n for n, cid in existing_candidates.items() if cid == c['candidate_id']), '?')
            print(f'      MATCH: "{c["candidate_name"]}" → DB: "{db_name}" (id={c["candidate_id"]})')
        if len(reuse) > 10:
            print(f'      ... and {len(reuse) - 10} more')
        return len(new), len(all_candidacies)

    # Insert new candidates
    new_candidate_ids = []
    if new:
        values = []
        for m in new:
            parts = m['candidate_name'].split()
            first = esc(parts[0]) if parts else ''
            last = esc(parts[-1]) if len(parts) > 1 else esc(parts[0]) if parts else ''
            full = esc(m['candidate_name'])
            values.append(f"('{full}', '{first}', '{last}', NULL)")

        for batch_start in range(0, len(values), BATCH_SIZE):
            batch = values[batch_start:batch_start + BATCH_SIZE]
            sql = (
                "INSERT INTO candidates (full_name, first_name, last_name, gender) VALUES\n"
                + ",\n".join(batch)
                + "\nRETURNING id;"
            )
            result = run_sql(sql, exit_on_error=False)
            if result is None:
                time.sleep(3)
                result = run_sql(sql)
            new_candidate_ids.extend(r['id'] for r in result)
            time.sleep(0.8)

        print(f'  Inserted {len(new_candidate_ids)} new candidates')
        if len(new_candidate_ids) != len(new):
            print(f'  ERROR: Expected {len(new)}, got {len(new_candidate_ids)}')
            sys.exit(1)

    # Assign new candidate_ids
    for i, m in enumerate(new):
        m['candidate_id'] = new_candidate_ids[i]

    # Insert candidacies
    values = []
    for m in all_candidacies:
        votes_sql = m['votes'] if m['votes'] is not None else 'NULL'
        pct_sql = m['vote_pct'] if m['vote_pct'] is not None else 'NULL'
        values.append(
            f"({m['election_id']}, {m['candidate_id']}, '{esc(m['party'])}', "
            f"'Active', {str(m['is_incumbent']).lower()}, false, NULL, NULL, "
            f"{votes_sql}, {pct_sql}, '{m['result']}', NULL, NULL)"
        )

    total_inserted = 0
    for batch_start in range(0, len(values), BATCH_SIZE):
        batch = values[batch_start:batch_start + BATCH_SIZE]
        sql = (
            "INSERT INTO candidacies (election_id, candidate_id, party, "
            "candidate_status, is_incumbent, is_write_in, filing_date, "
            "withdrawal_date, votes_received, vote_percentage, result, "
            "endorsements, notes) VALUES\n"
            + ",\n".join(batch)
            + "\nRETURNING id;"
        )
        result = run_sql(sql, exit_on_error=False)
        if result is None:
            time.sleep(3)
            result = run_sql(sql)
        total_inserted += len(result)
        time.sleep(0.8)

    print(f'  Inserted {total_inserted} candidacies')
    return len(new_candidate_ids) if new else 0, total_inserted


# ══════════════════════════════════════════════════════════════════════
# VERIFICATION
# ══════════════════════════════════════════════════════════════════════

def verify(years):
    print(f'\n{"=" * 60}')
    print('VERIFICATION')
    print(f'{"=" * 60}')

    years_str = ','.join(str(y) for y in years)

    r = run_sql(f"""
        SELECT e.election_year, d.chamber, COUNT(DISTINCT e.id) as elections,
               COUNT(ca.id) as candidacies,
               SUM(CASE WHEN ca.result = 'Won' THEN 1 ELSE 0 END) as winners,
               SUM(CASE WHEN ca.party = 'D' AND ca.result = 'Won' THEN 1 ELSE 0 END) as d_wins,
               SUM(CASE WHEN ca.party = 'R' AND ca.result = 'Won' THEN 1 ELSE 0 END) as r_wins
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        LEFT JOIN candidacies ca ON ca.election_id = e.id
        WHERE st.abbreviation = 'NJ' AND e.election_year IN ({years_str})
          AND d.chamber IN ('Assembly', 'Senate')
        GROUP BY e.election_year, d.chamber
        ORDER BY d.chamber, e.election_year
    """)
    for row in r:
        print(f'  {row["election_year"]} {row["chamber"]}: '
              f'{row["elections"]} elections, {row["candidacies"]} candidacies, '
              f'{row["winners"]} winners (D:{row["d_wins"]}, R:{row["r_wins"]})')

    # Spot check
    r = run_sql(f"""
        SELECT d.district_number, c.full_name, ca.party, ca.votes_received,
               ca.vote_percentage, ca.result, ca.is_incumbent, e.election_year
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON ca.candidate_id = c.id
        WHERE st.abbreviation = 'NJ' AND d.chamber = 'Assembly'
          AND e.election_year IN ({years_str})
        ORDER BY RANDOM() LIMIT 10
    """)
    print('\nSpot checks:')
    for row in r:
        inc = ' (i)' if row['is_incumbent'] else ''
        print(f'  {row["election_year"]} D{row["district_number"]}: '
              f'{row["full_name"]}{inc} [{row["party"]}] '
              f'{row["votes_received"]:,} votes ({row["vote_percentage"]}%) → {row["result"]}')

    # Duplicate check
    r = run_sql(f"""
        SELECT ca.election_id, ca.candidate_id, COUNT(*) as cnt
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        WHERE e.election_year IN ({years_str})
        GROUP BY ca.election_id, ca.candidate_id HAVING COUNT(*) > 1
    """)
    if r:
        print(f'\nWARNING: {len(r)} duplicate candidacies!')
    else:
        print('\nNo duplicate candidacies.')


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Populate NJ 2013-2017 Assembly results')
    parser.add_argument('--year', type=int, choices=[2013, 2015, 2017],
                        help='Process a single year')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and match only, no database inserts')
    args = parser.parse_args()

    if args.dry_run:
        print('DRY RUN MODE — no database changes will be made.\n')

    with open(INPUT_PATH) as f:
        all_data = json.load(f)
    print(f'Loaded data from {INPUT_PATH}')

    # What we need to create:
    # - 2013 Assembly: 80 elections + candidacies
    # - 2015 Assembly: 80 elections + candidacies
    # - 2015 Senate D5: 1 election + candidacy (special)
    # - 2017 Assembly: 80 elections + candidacies
    # (2013/2017 Senate already have full data in DB)

    tasks = [
        (2013, 'assembly'),
        (2015, 'assembly'),
        (2015, 'senate'),   # Only D5 special election
        (2017, 'assembly'),
    ]

    if args.year:
        tasks = [(y, c) for y, c in tasks if y == args.year]

    # Load DB maps
    print('Loading NJ seat map...')
    seat_map = load_seat_map()
    print(f'  {len(seat_map)} seats loaded')

    print('Loading existing NJ candidates...')
    existing_candidates = load_existing_candidates()
    print(f'  {len(existing_candidates)} existing candidates')

    # Check for existing elections to avoid duplicates
    years_to_process = sorted(set(y for y, _ in tasks))
    years_str = ','.join(str(y) for y in years_to_process)
    existing = run_sql(f"""
        SELECT e.election_year, d.chamber, COUNT(*) as cnt
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NJ' AND d.chamber = 'Assembly'
          AND e.election_year IN ({years_str})
        GROUP BY e.election_year, d.chamber
    """)
    if existing and not args.dry_run:
        for row in existing:
            print(f'  WARNING: {row["cnt"]} Assembly elections already exist for {row["election_year"]}!')
        print('  Aborting to prevent duplicates.')
        sys.exit(1)

    total_new_cands = 0
    total_candidacies = 0

    for year, chamber in tasks:
        key = f'{year}_{chamber}'
        districts = all_data.get(key, [])
        if not districts:
            print(f'\n  Skipping {year} {chamber} — no data')
            continue

        print(f'\n{"=" * 60}')
        print(f'PROCESSING: NJ {year} {chamber.title()}')
        print(f'{"=" * 60}')
        print(f'  {len(districts)} districts, '
              f'{sum(len(d["candidates"]) for d in districts)} candidates')

        # Create elections
        print('\n  Creating elections...')
        election_map = create_elections(year, chamber, districts, seat_map,
                                       dry_run=args.dry_run)

        # Create candidacies
        print('\n  Creating candidacies...')
        new_cands, cand_count = process_candidacies(
            year, chamber, districts, election_map, seat_map,
            existing_candidates, dry_run=args.dry_run
        )
        total_new_cands += new_cands
        total_candidacies += cand_count

    # Summary
    print(f'\n{"=" * 60}')
    print('SUMMARY')
    print(f'{"=" * 60}')
    print(f'New candidates: {total_new_cands}')
    print(f'Total candidacies: {total_candidacies}')

    if not args.dry_run:
        verify(years_to_process)

    print('\nDone!')


if __name__ == '__main__':
    main()
