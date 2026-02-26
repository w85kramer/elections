#!/usr/bin/env python3
"""Backfill missing candidacy records for 2014 general elections.

Covers 35 elections across 24 states.
Also fixes 9 incorrect election dates (filing deadlines stored as election dates).

Data sourced from Ballotpedia (February 2026).

Usage:
    python3 scripts/backfill_2014_candidacies.py --dry-run
    python3 scripts/backfill_2014_candidacies.py
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
# DATE FIXES: election_id -> correct_date
# ══════════════════════════════════════════════════════════════
DATE_FIXES = {
    26818: '2014-11-04',  # CT House 125 (was 2014-06-10, filing)
    36423: '2014-11-04',  # IN House 71 (was 2014-07-02, filing)
    38714: '2014-11-04',  # KS House 1 (was 2014-06-02, filing)
    44944: '2014-11-04',  # MA House 118 (was 2014-06-03, filing)
    50220: '2014-11-04',  # MO House 6 (was 2014-03-25, filing)
    58881: '2014-11-04',  # NH Sullivan-4 (was 2014-11-12, recount date)
    66218: '2014-11-04',  # OH House 89 (was 2014-02-05, filing)
    75887: '2014-11-04',  # TN House 88 (was 2014-04-03, filing)
    78556: '2014-11-04',  # UT House 11 (was 2014-03-20, filing)
}

# ══════════════════════════════════════════════════════════════
# DATA: election_id -> [(name, party, votes, pct, result, inc)]
# ══════════════════════════════════════════════════════════════

DATA = {
    # ── AL ──
    21157: [('Jack Williams', 'R', 7073, 72.7, 'Won', True),
            ('Salvatore Bambinelli', 'D', 2642, 27.2, 'Lost', False)],
    21390: [('Alan Boothe', 'R', 5520, 50.3, 'Won', True),
            ('Joel Lee Williams', 'D', 5436, 49.5, 'Lost', False)],

    # ── AZ (2-member district, top-2 win) ──
    22013: [('Bruce Wheeler', 'D', 32731, 27.5, 'Won', True),
            ('Stefanie Mach', 'D', 31163, 26.2, 'Won', True),
            ('Todd Clodfelter', 'R', 29940, 25.1, 'Lost', False),
            ('William Wildish', 'R', 25240, 21.2, 'Lost', False)],

    # ── CA (top-two primary, both Dems in general) ──
    23550: [('Ben Hueso', 'D', 58880, 54.9, 'Won', True),
            ('Rafael Estrada', 'D', 48397, 45.1, 'Lost', False)],

    # ── CO ──
    24785: [('Crisanta Duran', 'D', 15203, 76.1, 'Won', True),
            ('Ronnie Nelson', 'R', 4769, 23.9, 'Lost', False)],

    # ── CT ──
    26818: [("Tom O'Dea", 'R', 6073, 88.0, 'Won', True),
            ('David Bedell', 'G', 825, 12.0, 'Lost', False)],

    # ── HI ──
    31938: [('James Tokioka', 'D', 5367, 73.9, 'Won', True),
            ('Steve Yoder', 'R', 1892, 26.1, 'Lost', False)],

    # ── ID ──
    32893: [('Paulette Jordan', 'D', 7371, 51.8, 'Won', False),
            ('Lucinda Agidius', 'R', 6847, 48.2, 'Lost', True)],

    # ── IL ──
    34508: [('Barbara Wheeler', 'R', 23189, 69.3, 'Won', True),
            ('Joel Mains', 'D', 10275, 30.7, 'Lost', False)],

    # ── IN ──
    36423: [('Steven Stemler', 'D', 11370, 83.9, 'Won', True),
            ('Russell Brooksbank', 'L', 2182, 16.1, 'Lost', False)],

    # ── KS ──
    38714: [('Michael Houser', 'R', 3729, 59.5, 'Won', True),
            ('Brian Caswell', 'D', 2543, 40.5, 'Lost', False)],

    # ── MA ──
    44944: [('Dennis Rosa', 'D', 6649, 55.0, 'Won', True),
            ('Jacques Perrault', 'R', 5431, 45.0, 'Lost', False)],
    45394: [('David Muradian Jr.', 'R', 8980, 62.4, 'Won', False),
            ('Martin Green', 'D', 5414, 37.6, 'Lost', False)],

    # ── MI ──
    47755: [('Dave Pagel', 'R', 15360, 67.2, 'Won', True),
            ('Cartier Shields', 'D', 7488, 32.8, 'Lost', False)],

    # ── MN (House 54 → using 54A results) ──
    49129: [('Dan Schoen', 'D', 7047, 55.5, 'Won', True),
            ('Matthew Kowalski', 'R', 5629, 44.3, 'Lost', False)],

    # ── MO ──
    50220: [('Tim Remole', 'R', 7731, 70.2, 'Won', True),
            ('Robert Harrington', 'D', 3286, 29.8, 'Lost', False)],

    # ── MT ──
    53319: [('Doug Kary', 'R', 4106, 64.3, 'Won', False),
            ('Steven Fugate', 'D', 2280, 35.7, 'Lost', False)],

    # ── NC ──
    63583: [('Evelyn Terry', 'D', 12536, 76.6, 'Won', True),
            ('Kris McCann', 'R', 3824, 23.4, 'Lost', False)],

    # ── NE (unicameral, nonpartisan) ──
    55111: [('Tyson Larson', 'NP', 8511, 67.1, 'Won', True),
            ('Keith Kube', 'NP', 4179, 32.9, 'Lost', False)],

    # ── NH (multi-member bloc voting) ──
    # Hillsborough-29 Seat A (3-seat district)
    57313: [('Suzanne Harvey', 'D', 1503, 18.7, 'Won', False),
            ('Peggy McCarthy', 'R', 1493, 18.6, 'Won', False),
            ('Donald McClarren', 'R', 1343, 16.7, 'Won', False),
            ('Kenneth Ziehm II', 'R', 1317, 16.4, 'Lost', False),
            ('Suzanne Mercier Vail', 'D', 1259, 15.6, 'Lost', True),
            ('Ward Shaff', 'D', 1122, 13.9, 'Lost', False)],
    # Merrimack-20 Seat A (3-seat district)
    57838: [('Brian Seaworth', 'R', 1815, 17.9, 'Won', True),
            ('Dianne Schuett', 'D', 1766, 17.4, 'Won', True),
            ('David Doherty', 'D', 1701, 16.8, 'Won', True),
            ('Kim Bolt', 'R', 1654, 16.3, 'Lost', False),
            ('Richard DeBold', 'D', 1647, 16.3, 'Lost', False),
            ('John Goldthwaite', 'R', 1545, 15.2, 'Lost', False)],
    # Sullivan-4 Seat A (1-seat district)
    58881: [('Larry Converse', 'D', 703, 50.4, 'Won', True),
            ('George Caccavaro Jr.', 'R', 693, 49.6, 'Lost', False)],

    # ── NV ──
    55485: [('Chris Edwards', 'R', 8503, 65.1, 'Won', True),
            ('James Zygadlo', 'D', 3900, 29.9, 'Lost', False),
            ('Donald Hendon', 'L', 659, 5.0, 'Lost', False)],

    # ── NY ──
    62323: [('Stephen Hawley', 'R', 29170, 95.5, 'Won', True),
            ('Mark Glogowski', 'L', 1363, 4.5, 'Lost', False)],

    # ── OH ──
    66218: [('Steven Kraus', 'R', 19386, 51.2, 'Won', False),
            ('Chris Redfern', 'D', 18446, 48.8, 'Lost', True)],

    # ── OR ──
    67371: [('Kim Thatcher', 'R', 27638, 58.5, 'Won', False),
            ('Ryan Howard', 'D', 19434, 41.2, 'Lost', False)],

    # ── PA ──
    69102: [('Steve Santarsiero', 'D', 13323, 58.0, 'Won', True),
            ('David Gibbon', 'R', 9639, 42.0, 'Lost', False)],
    68644: [('Mario Scavello', 'R', 38417, 59.9, 'Won', False),
            ('Mark Aurand', 'D', 25739, 40.1, 'Lost', False)],

    # ── RI ──
    72213: [('Deborah Fellela', 'D', 2483, 52.8, 'Won', True),
            ('Karin Gorman', 'I', 2217, 47.2, 'Lost', False)],

    # ── TN ──
    75887: [('Larry Miller', 'D', 7297, 70.0, 'Won', True),
            ('Harry Barber', 'R', 3134, 30.0, 'Lost', False)],

    # ── TX ──
    77523: [('Craig Goldman', 'R', 27977, 81.6, 'Won', True),
            ('Rod Wingo', 'L', 6295, 18.4, 'Lost', False)],

    # ── UT ──
    78556: [('Brad Dee', 'R', 4364, 62.6, 'Won', True),
            ('Amy Morgan', 'D', 2607, 37.4, 'Lost', False)],

    # ── WI ──
    83640: [('Daniel Riemer', 'D', 11065, 55.7, 'Won', True),
            ('Scott Espeseth', 'R', 8800, 44.3, 'Lost', False)],

    # ── WV (HoD 22 is multi-member 2-seat) ──
    82501: [('Michael Moffatt', 'R', 3756, 28.6, 'Won', False),
            ('Jeff Eldridge', 'D', 3367, 25.6, 'Won', True),
            ('Justin Mullins', 'R', 3093, 23.5, 'Lost', False),
            ('Gary McCallister', 'D', 2927, 22.3, 'Lost', False)],
    83025: [('Larry Faircloth', 'R', 2650, 66.9, 'Won', True),
            ('Gary Collis', 'D', 1314, 33.1, 'Lost', False)],
}


def main():
    parser = argparse.ArgumentParser(description='Backfill 2014 candidacies')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    # ── Step 1: Fix dates ──
    print(f'=== Step 1: Fix {len(DATE_FIXES)} incorrect election dates ===')
    if args.dry_run:
        for eid, date in sorted(DATE_FIXES.items()):
            print(f'  [DRY RUN] Would fix election {eid} date -> {date}')
    else:
        cases = ' '.join(f"WHEN {eid} THEN '{date}'::date" for eid, date in DATE_FIXES.items())
        ids = ','.join(str(eid) for eid in DATE_FIXES.keys())
        sql = f"UPDATE elections SET election_date = CASE id {cases} END WHERE id IN ({ids})"
        result = run_sql(sql)
        if result is not None:
            print(f'  Fixed {len(DATE_FIXES)} election dates')
        else:
            print(f'  WARNING: Failed to fix dates')
        time.sleep(2)

    # ── Step 2: Candidate lookup ──
    all_names = set()
    for eid, candidates in DATA.items():
        for c in candidates:
            all_names.add(c[0])

    print(f'\n=== Step 2: Candidate lookup ===')
    print(f'Elections to backfill: {len(DATA)}')
    total_cand = sum(len(v) for v in DATA.values())
    print(f'Total candidacies to insert: {total_cand}')
    print(f'Unique candidate names: {len(all_names)}')

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

    # ── Step 3: Insert candidacies ──
    print(f'\n=== Step 3: Insert candidacies ===')
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
