#!/usr/bin/env python3
"""Backfill missing candidacy records for 2016 general elections.

Covers 54 elections across 25 states (55 originally, minus 1 duplicate deletion).
Also fixes incorrect election dates (filing deadlines/primary dates stored
instead of actual general election date 2016-11-08) and deletes 1 duplicate.

Data sourced from Ballotpedia (February 2026).

Usage:
    python3 scripts/backfill_2016_candidacies.py --dry-run
    python3 scripts/backfill_2016_candidacies.py
"""
import sys, os, time, argparse
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL
import httpx

def run_sql(query):
    for attempt in range(5):
        try:
            resp = httpx.post(
                API_URL,
                headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
                json={'query': query},
                timeout=120
            )
        except Exception as e:
            print(f'  Request error: {e}')
            time.sleep(5 * (attempt + 1))
            continue
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f'  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'  SQL ERROR {resp.status_code}: {resp.text[:300]}')
        return None
    print('  Max retries exceeded')
    return None

def esc(s):
    return str(s).replace("'", "''") if s else ''

# ══════════════════════════════════════════════════════════════
# DUPLICATE TO DELETE
# ══════════════════════════════════════════════════════════════
# ID House 5 Seat A has two election records for 2016 General:
#   32891 (date 2016-03-11 = filing deadline) — DUPLICATE, delete
#   32892 (date 2016-11-08 = correct) — keep, fill candidacies
DUPLICATE_DELETE = 32891

# ══════════════════════════════════════════════════════════════
# DATE FIXES: election_id -> correct_date
# These had filing deadlines or primary dates instead of the
# actual general election date (2016-11-08 for all)
# ══════════════════════════════════════════════════════════════
DATE_FIXES = {
    22531: '2016-11-08',  # AR House 5 (was 2015-11-02, filing period)
    22858: '2016-11-08',  # AR House 45 (was 2015-11-02, filing period)
    24784: '2016-11-08',  # CO House 5 (was 2016-04-04, filing deadline)
    25967: '2016-11-08',  # CT House 4 (was 2016-08-09, primary date)
    26817: '2016-11-08',  # CT House 125 (was 2016-08-09, primary date)
    31937: '2016-11-08',  # HI House 15 (was 2016-08-13, primary date)
    36422: '2016-11-08',  # IN House 71 (was 2016-02-05, filing deadline)
    41192: '2016-11-08',  # KY House 83 (was 2016-05-17, primary date)
    44475: '2016-11-08',  # MA House 59/1st Bristol (was 2016-06-07, filing deadline)
    45393: '2016-11-08',  # MA House 159/9th Worcester (was 2016-06-07, filing deadline)
    47754: '2016-11-08',  # MI House 78 (was 2016-08-02, primary date)
    49128: '2016-11-08',  # MN House 54 (was 2016-05-31, filing deadline)
    50219: '2016-11-08',  # MO House 6 (was 2016-08-02, primary date)
    50735: '2016-11-08',  # MO House 46 (was 2016-03-29, filing deadline)
    51247: '2016-11-08',  # MO House 87 (was 2016-08-02, primary date)
    55484: '2016-11-08',  # NV Assembly 19 (was 2016-06-14, primary date)
    57837: '2016-11-08',  # NH Merrimack-20 (was 2016-09-13, primary date)
    59596: '2016-11-08',  # NM Senate 41 (was 2016-06-07, primary date)
    61599: '2016-11-08',  # NY Assembly 58 (was 2016-07-14, filing deadline)
    62322: '2016-11-08',  # NY Assembly 139 (was 2016-07-14, filing deadline)
    66217: '2016-11-08',  # OH House 89 (was 2016-03-15, primary date)
    72212: '2016-11-08',  # RI House 43 (was 2016-06-29, unknown)
    73125: '2016-11-08',  # SC House 37 (was 2016-06-28, primary runoff date)
}

# ══════════════════════════════════════════════════════════════
# DATA: election_id -> [(name, party, votes, pct, result, inc)]
# votes=None for unopposed with no vote totals recorded
# ══════════════════════════════════════════════════════════════

DATA = {
    # ── AR ──
    22531: [('David Fielding', 'D', None, 100.0, 'Won', True)],
    22858: [('Jeremy Gillam', 'R', None, 100.0, 'Won', True)],

    # ── CO ──
    24784: [('Crisanta Duran', 'D', 26130, 77.31, 'Won', True),
            ('Ronnie Nelson', 'R', 7668, 22.69, 'Lost', False)],

    # ── CT ──
    25967: [('Angel Arce', 'D', 4063, 87.11, 'Won', True),
            ('Lloyd Carter', 'R', 601, 12.89, 'Lost', False)],
    26817: [("Tom O'Dea", 'R', 9261, 87.79, 'Won', True),
            ('Hector Lopez', 'G', 1288, 12.21, 'Lost', False)],

    # ── HI (general was unopposed after primary) ──
    31937: [('James Tokioka', 'D', None, 100.0, 'Won', True)],

    # ── ID (only 32892, the correctly-dated record) ──
    32892: [('Paulette Jordan', 'D', 11179, 50.66, 'Won', True),
            ('Carl Berglund', 'R', 10889, 49.34, 'Lost', False)],

    # ── IL ──
    35009: [('Chad Hays', 'R', None, 100.0, 'Won', True)],
    34507: [('Barbara Wheeler', 'R', None, 100.0, 'Won', True)],

    # ── IN ──
    36422: [('Steven Stemler', 'D', 18728, 80.36, 'Won', True),
            ('Thomas Keister', 'L', 4578, 19.64, 'Lost', False)],
    35459: [('Mark Stoops', 'D', None, 100.0, 'Won', True)],

    # ── KS ──
    40219: [('Russ Jennings', 'R', None, 100.0, 'Won', True)],

    # ── KY ──
    40827: [('Darryl Owens', 'D', 13173, 76.18, 'Won', True),
            ('John Owen', 'R', 4120, 23.82, 'Lost', False)],
    41192: [('Jeffrey Hoover', 'R', None, 100.0, 'Won', True)],

    # ── MA ──
    44943: [('Natalie Higgins', 'D', 10382, 55.20, 'Won', False),
            ('Thomas Ardinger', 'R', 8426, 44.80, 'Lost', False)],
    45393: [('David Muradian Jr.', 'R', None, 100.0, 'Won', True)],
    44475: [('F. Jay Barrows', 'R', 12561, 60.04, 'Won', True),
            ('Michael Toole', 'D', 8361, 39.96, 'Lost', False)],

    # ── ME ──
    42087: [("Beth O'Connor", 'R', 2496, 52.03, 'Won', True),
            ('Joshua Plante', 'D', 2301, 47.97, 'Lost', False)],

    # ── MI ──
    47754: [('Dave Pagel', 'R', 26037, 67.51, 'Won', True),
            ('Dean Hill', 'D', 12529, 32.49, 'Lost', False)],

    # ── MN ──
    # MN House 54 in DB — using 54A results (MN uses lettered subdivisions)
    49128: [('Keith Franke', 'R', 10483, 51.49, 'Won', False),
            ('Jen Peterson', 'D', 9877, 48.51, 'Lost', False)],
    48444: [('Chris Eaton', 'D', 21152, 68.11, 'Won', True),
            ('Robert Marvin', 'R', 9905, 31.89, 'Lost', False)],

    # ── MO ──
    50735: [('Martha Stevens', 'D', 12140, 62.65, 'Won', False),
            ('Don Waterman', 'R', 7238, 37.35, 'Lost', False)],
    50219: [('Tim Remole', 'R', None, 100.0, 'Won', True)],
    51247: [('Stacey Newman', 'D', None, 100.0, 'Won', True)],

    # ── MT ──
    53453: [('Terry Gauthier', 'R', 6135, 52.69, 'Won', False),
            ('Hal Jacobson', 'D', 5509, 47.31, 'Lost', False)],

    # ── NC ──
    63582: [('Evelyn Terry', 'D', None, 100.0, 'Won', True)],

    # ── ND ──
    64330: [('Karen Krebsbach', 'R', 3255, 72.67, 'Won', True),
            ('Phil Franklin', 'D', 1224, 27.33, 'Lost', False)],

    # ── NH (multi-member bloc voting — same candidates for all seats in district) ──
    # Belknap-2: 4-seat district, Seats B/C/D empty (Seat A already has data)
    86960: [('Glen Aldrich', 'R', 4340, 14.75, 'Won', True),
            ('Marc Abear', 'R', 4185, 14.23, 'Won', False),
            ('Herb Vadney', 'R', 4057, 13.79, 'Won', True),
            ('Norman Silber', 'R', 3933, 13.37, 'Won', False),
            ('Lisa DiMartino', 'D', 3803, 12.93, 'Lost', False),
            ('Dorothy Piquado', 'D', 3143, 10.68, 'Lost', False),
            ('Nancy Frost', 'D', 3140, 10.67, 'Lost', False),
            ('Johan Andersen', 'D', 2817, 9.58, 'Lost', False)],
    86961: [('Glen Aldrich', 'R', 4340, 14.75, 'Won', True),
            ('Marc Abear', 'R', 4185, 14.23, 'Won', False),
            ('Herb Vadney', 'R', 4057, 13.79, 'Won', True),
            ('Norman Silber', 'R', 3933, 13.37, 'Won', False),
            ('Lisa DiMartino', 'D', 3803, 12.93, 'Lost', False),
            ('Dorothy Piquado', 'D', 3143, 10.68, 'Lost', False),
            ('Nancy Frost', 'D', 3140, 10.67, 'Lost', False),
            ('Johan Andersen', 'D', 2817, 9.58, 'Lost', False)],
    86962: [('Glen Aldrich', 'R', 4340, 14.75, 'Won', True),
            ('Marc Abear', 'R', 4185, 14.23, 'Won', False),
            ('Herb Vadney', 'R', 4057, 13.79, 'Won', True),
            ('Norman Silber', 'R', 3933, 13.37, 'Won', False),
            ('Lisa DiMartino', 'D', 3803, 12.93, 'Lost', False),
            ('Dorothy Piquado', 'D', 3143, 10.68, 'Lost', False),
            ('Nancy Frost', 'D', 3140, 10.67, 'Lost', False),
            ('Johan Andersen', 'D', 2817, 9.58, 'Lost', False)],
    # Hillsborough-29: 3-seat district, Seat A empty
    57312: [('Suzanne Harvey', 'D', 2256, 19.31, 'Won', True),
            ('Sue Newman', 'D', 2127, 18.21, 'Won', False),
            ('Michael McCarthy', 'R', 2098, 17.96, 'Won', False),
            ('Gloria Timmons', 'D', 1955, 16.74, 'Lost', False),
            ('Michael Balboni', 'R', 1680, 14.38, 'Lost', False),
            ('George Coupe', 'R', 1566, 13.41, 'Lost', False)],
    # Merrimack-20: 3-seat district, Seat A empty
    57837: [('Dianne Schuett', 'D', 2419, 17.48, 'Won', True),
            ('Brian Seaworth', 'R', 2390, 17.27, 'Won', True),
            ('David Doherty', 'D', 2297, 16.59, 'Won', True),
            ('Jon Richardson', 'R', 2259, 16.32, 'Lost', False),
            ('Darren Tapp', 'R', 2248, 16.24, 'Lost', False),
            ('Doug Hall', 'D', 2229, 16.10, 'Lost', False)],
    # Merrimack-27: 2-seat district, Seat B empty (both unopposed)
    87093: [('Mary Gile', 'D', None, None, 'Won', True),
            ('Harold Rice', 'D', None, None, 'Won', True)],
    # Strafford-6: 5-seat district, Seat E empty (all 5 unopposed)
    87155: [('Wayne Burton', 'D', None, None, 'Won', True),
            ('Timothy Horrigan', 'D', None, None, 'Won', True),
            ('Marjorie Smith', 'D', None, None, 'Won', True),
            ('Judith Spang', 'D', None, None, 'Won', True),
            ('Janet Wall', 'D', None, None, 'Won', True)],
    # Sullivan-4: 1-seat district, Seat A empty
    58880: [("John O'Connor", 'R', 1065, 52.64, 'Won', False),
            ('Larry Converse', 'D', 958, 47.36, 'Lost', True)],

    # ── NM ──
    59596: [('Carroll Leavell', 'R', 9006, 100.0, 'Won', True)],

    # ── NV ──
    55484: [('Chris Edwards', 'R', None, 100.0, 'Won', True)],

    # ── NY ──
    62322: [('Stephen Hawley', 'R', None, 100.0, 'Won', True)],
    61599: [('N. Nick Perry', 'D', None, 100.0, 'Won', True)],

    # ── OH ──
    66217: [('Steven Arndt', 'R', 34721, 60.72, 'Won', True),
            ('Lawrence Hartlaub', 'D', 22464, 39.28, 'Lost', False)],

    # ── PA ──
    70648: [('Madeleine Dean', 'D', 24496, 66.25, 'Won', True),
            ('Anthony Scalfaro', 'R', 12478, 33.75, 'Lost', False)],
    69101: [('Perry Warren', 'D', 19071, 50.10, 'Won', False),
            ('Ryan Gallagher', 'R', 18996, 49.90, 'Lost', False)],

    # ── RI ──
    72212: [('Deborah Fellela', 'D', 3585, 58.14, 'Won', True),
            ('Karin Gorman', 'I', 2581, 41.86, 'Lost', False)],

    # ── SC ──
    72948: [('Mike Burns', 'R', None, 100.0, 'Won', True)],
    73125: [('Steven Long', 'R', 10386, 69.73, 'Won', False),
            ('Michael Pratt', 'D', 4509, 30.27, 'Lost', False)],
    73476: [('Ivory Thigpen', 'D', 13366, 73.03, 'Won', False),
            ('Donald Miles', 'R', 4581, 25.03, 'Lost', False),
            ('Victor Kocher', 'L', 354, 1.93, 'Lost', False)],

    # ── SD (multi-member 2-seat district, only Seat A empty) ──
    74266: [('Hugh Bartels', 'R', 5770, 35.72, 'Won', False),
            ('Nancy York', 'R', 5346, 33.09, 'Won', False),
            ('Michele Alvine', 'D', 3157, 19.54, 'Lost', False),
            ('Chuck Haan', 'I', 1882, 11.65, 'Lost', False)],

    # ── TN ──
    75886: [('Larry Miller', 'D', 14918, 84.04, 'Won', True),
            ('Orrden Williams Jr.', 'I', 2834, 15.96, 'Lost', False)],

    # ── TX ──
    76414: [('Chris Paddie', 'R', 53172, 100.0, 'Won', True)],
    77522: [('Craig Goldman', 'R', 39537, 57.23, 'Won', True),
            ('Elizabeth Tarrant', 'D', 27019, 39.11, 'Lost', False),
            ('Patrick Wentworth', 'L', 2531, 3.66, 'Lost', False)],

    # ── UT ──
    78555: [('Kelly Miles', 'R', 9521, 64.85, 'Won', False),
            ('Amy Morgan', 'D', 5161, 35.15, 'Lost', False)],

    # ── WI ──
    83639: [('Daniel Riemer', 'D', 13514, 56.24, 'Won', True),
            ('Zachary Marshall', 'R', 9212, 38.34, 'Lost', False),
            ('Matthew Bughman', 'L', 1303, 5.42, 'Lost', False)],

    # ── WV (HoD 22 is multi-member 2-seat) ──
    82500: [('Zack Maynard', 'R', 5120, 26.18, 'Won', False),
            ('Jeff Eldridge', 'D', 5070, 25.93, 'Won', True),
            ('Michel Moffatt', 'R', 4843, 24.77, 'Lost', True),
            ('Gary McCallister', 'D', 4522, 23.12, 'Lost', False)],
    83024: [('S. Marshall Wilson', 'R', 4874, 62.19, 'Won', False),
            ('Gary Collis', 'D', 2963, 37.81, 'Lost', False)],
}


def main():
    parser = argparse.ArgumentParser(description='Backfill 2016 candidacies')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    # ── Step 1: Delete duplicate election ──
    print(f'=== Step 1: Delete duplicate election {DUPLICATE_DELETE} ===')
    if args.dry_run:
        print(f'  [DRY RUN] Would delete election {DUPLICATE_DELETE} (ID House 5A duplicate)')
    else:
        result = run_sql(f'DELETE FROM elections WHERE id = {DUPLICATE_DELETE}')
        if result is not None:
            print(f'  Deleted election {DUPLICATE_DELETE}')
        else:
            print(f'  WARNING: Failed to delete election {DUPLICATE_DELETE}')
        time.sleep(1)

    # ── Step 2: Fix incorrect dates ──
    print(f'\n=== Step 2: Fix {len(DATE_FIXES)} incorrect election dates ===')
    if args.dry_run:
        for eid, date in sorted(DATE_FIXES.items()):
            print(f'  [DRY RUN] Would fix election {eid} date -> {date}')
    else:
        # Batch date fixes into one UPDATE with CASE
        cases = ' '.join(f"WHEN {eid} THEN '{date}'::date" for eid, date in DATE_FIXES.items())
        ids = ','.join(str(eid) for eid in DATE_FIXES.keys())
        sql = f"UPDATE elections SET election_date = CASE id {cases} END WHERE id IN ({ids})"
        result = run_sql(sql)
        if result is not None:
            print(f'  Fixed {len(DATE_FIXES)} election dates')
        else:
            print(f'  WARNING: Failed to fix dates')
        time.sleep(2)

    # ── Step 3: Gather candidate names ──
    all_names = set()
    for eid, candidates in DATA.items():
        for c in candidates:
            all_names.add(c[0])

    print(f'\n=== Step 3: Candidate lookup ===')
    print(f'Elections to backfill: {len(DATA)}')
    total_cand = sum(len(v) for v in DATA.values())
    print(f'Total candidacies to insert: {total_cand}')
    print(f'Unique candidate names: {len(all_names)}')

    # Check existing candidates
    existing_map = {}
    name_list = sorted(all_names)
    for i in range(0, len(name_list), 50):
        batch = name_list[i:i+50]
        names_sql = ','.join(f"'{esc(n)}'" for n in batch)
        result = run_sql(f"SELECT id, full_name FROM candidates WHERE full_name IN ({names_sql})")
        if result:
            for r in result:
                existing_map[r['full_name']] = r['id']
        time.sleep(1)

    missing = sorted(all_names - set(existing_map.keys()))
    print(f'Existing candidates: {len(existing_map)}')
    print(f'New candidates to create: {len(missing)}')

    if missing:
        if args.dry_run:
            for name in missing:
                print(f'  [NEW] {name}')
        else:
            for i in range(0, len(missing), 30):
                batch = missing[i:i+30]
                values = ','.join(f"('{esc(n)}')" for n in batch)
                result = run_sql(
                    f"INSERT INTO candidates (full_name) VALUES {values} RETURNING id, full_name"
                )
                if result:
                    for r in result:
                        existing_map[r['full_name']] = r['id']
                    print(f'  Inserted {len(result)} candidates (batch {i//30+1})')
                else:
                    print(f'  FAILED batch {i//30+1}')
                    return
                time.sleep(2)

    # Verify all IDs
    missing_ids = []
    for eid, candidates in DATA.items():
        for c in candidates:
            if c[0] not in existing_map:
                missing_ids.append((eid, c[0]))
    if missing_ids:
        print(f'\nERROR: Missing candidate IDs:')
        for eid, name in missing_ids:
            print(f'  election {eid}: {name}')
        return

    # ── Step 4: Build and insert candidacies ──
    print(f'\n=== Step 4: Insert candidacies ===')
    all_values = []
    for eid, candidates in sorted(DATA.items()):
        for name, party, votes, pct, result, is_inc in candidates:
            cid = existing_map[name]
            v = str(votes) if votes is not None else 'NULL'
            p = str(pct) if pct is not None else 'NULL'
            inc = 'true' if is_inc else 'false'
            all_values.append(
                f"({eid}, {cid}, '{esc(party)}', {v}, {p}, '{esc(result)}', {inc}, false)"
            )

    if args.dry_run:
        print(f'[DRY RUN] Would insert {len(all_values)} candidacies')
        return

    # Insert in batches
    total_inserted = 0
    for i in range(0, len(all_values), 30):
        chunk = all_values[i:i+30]
        sql = (
            "INSERT INTO candidacies "
            "(election_id, candidate_id, party, votes_received, vote_percentage, "
            "result, is_incumbent, is_write_in) VALUES " + ','.join(chunk)
        )
        result = run_sql(sql)
        if result is not None:
            total_inserted += len(chunk)
            print(f'  Batch {i//30+1}: inserted {len(chunk)} (total: {total_inserted})')
        else:
            print(f'  FAILED batch {i//30+1}')
            break
        time.sleep(2)

    print(f'\nDone! Inserted {total_inserted} candidacies across {len(DATA)} elections.')


if __name__ == '__main__':
    main()
