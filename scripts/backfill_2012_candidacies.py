#!/usr/bin/env python3
"""Backfill missing candidacy records for 2012 elections.

Covers 43 elections across 25+ states including:
- Regular generals with wrong dates (filing deadlines stored instead)
- Multi-member districts (AZ, NH, SD, WV)
- WI Governor Recall (date fix: Nov→Jun)
- MN House 54 (mapped to 54A under old redistricting)

Data sourced from Ballotpedia (February 2026).

Usage:
    python3 scripts/backfill_2012_candidacies.py --dry-run
    python3 scripts/backfill_2012_candidacies.py
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
    # AK — had wrong date 2012-06-01
    20577: '2012-11-06', 20162: '2012-11-06',
    # CT House 4 — filing deadline 2012-06-12
    25968: '2012-11-06',
    # IL House 104 — filing deadline 2011-12-05
    35010: '2012-11-06',
    # MN House 54 — filing deadline 2012-06-05
    49130: '2012-11-06',
    # MO — filing deadline 2012-03-27
    50736: '2012-11-06', 50221: '2012-11-06',
    # NC Senate 40 — filing deadline 2012-02-29
    62800: '2012-11-06',
    # NM Senate 41 — filing deadline 2012-03-20
    59597: '2012-11-06',
    # OH House 89 — filing deadline 2011-12-07
    66219: '2012-11-06',
    # PA House 153 — filing deadline 2012-02-16
    70649: '2012-11-06',
    # RI House 43 — filing deadline 2012-06-27
    72214: '2012-11-06',
    # SC House 37 — filing deadline 2012-03-30
    73126: '2012-11-06',
    # SD House 5A — filing deadline 2011-03-27 (wrong year too)
    74267: '2012-11-06',
    # TN House 88 — filing deadline 2011-04-05 (wrong year too)
    75888: '2012-11-06',
    # WI Recall — was 2012-11-06, actually 2012-06-05
    20042: '2012-06-05',
}

# ══════════════════════════════════════════════════════════════
# DATA: election_id -> [(name, party, votes, pct, result, inc)]
# ══════════════════════════════════════════════════════════════

DATA = {
    # ── AK House 32 (Nov 6) ──
    20577: [('Beth Kerttula', 'D', 7112, 95.7, 'Won', True)],

    # ── AK Senate 12 / SD-L (Nov 6) ──
    20162: [('Kevin Meyer', 'R', 10304, 72.6, 'Won', True),
            ('Jacob Hale', 'D', 3894, 27.4, 'Lost', False)],

    # ── AZ House 10 Seat A (Nov 6) — 2-member district ──
    22014: [('Bruce Wheeler', 'D', 43058, 27.4, 'Won', True),
            ('Stefanie Mach', 'D', 40843, 26.0, 'Won', False),
            ('Ted Vogt', 'R', 37758, 24.0, 'Lost', True),
            ('Todd A. Clodfelter', 'R', 35609, 22.6, 'Lost', False)],

    # ── CO House 5 (Nov 6) ──
    24786: [('Crisanta Duran', 'D', 20483, 74.3, 'Won', True),
            ('Ronnie Nelson', 'R', 6027, 21.9, 'Lost', False),
            ('Victor Forsythe Villacres', 'G', 1058, 3.8, 'Lost', False)],

    # ── CT House 125 (Nov 6) ──
    26819: [('Tom O\'Dea', 'R', 7780, 63.8, 'Won', False),
            ('Mark Robbins', 'D', 4182, 34.3, 'Lost', False),
            ('David A. Bedell', 'G', 223, 1.8, 'Lost', False)],

    # ── CT House 4 (Nov 6) ──
    25968: [('Angel Arce', 'D', 3614, 91.4, 'Won', False),
            ('Rico Dence', 'R', 341, 8.6, 'Lost', False)],

    # ── ID House 5 Seat A (Nov 6) ──
    32894: [('Cindy Agidius', 'R', 10083, 50.3, 'Won', False),
            ('Paulette Jordan', 'D', 9960, 49.7, 'Lost', False)],

    # ── IL House 104 (Nov 6) ──
    35010: [('Chad Hays', 'R', 26479, 63.5, 'Won', True),
            ('Michael Langendorf', 'D', 15240, 36.5, 'Lost', False)],

    # ── IL House 64 (Nov 6) — unopposed ──
    34509: [('Barbara Wheeler', 'R', 37465, 100.0, 'Won', False)],

    # ── IN House 71 (Nov 6) — unopposed ──
    36424: [('Steven Stemler', 'D', 19390, 100.0, 'Won', True)],

    # ── IN Senate 40 (Nov 6) — open seat (Vi Simpson withdrew) ──
    35460: [('Mark Stoops', 'D', 30656, 60.2, 'Won', False),
            ('Reid Dallas', 'R', 20275, 39.8, 'Lost', False)],

    # ── KS House 1 (Nov 6) ──
    38715: [('Michael Houser', 'R', 4823, 54.0, 'Won', False),
            ('Grant Randall', 'D', 4115, 46.0, 'Lost', False)],

    # ── MA House 118 / 4th Worcester (Nov 6) ──
    44945: [('Dennis Rosa', 'D', 10346, 57.2, 'Won', True),
            ('Justin Brooks', 'R', 7743, 42.8, 'Lost', False)],

    # ── MI House 2 (Nov 6) ──
    46547: [('Alberta Tinsley Talabi', 'D', 28990, 71.8, 'Won', True),
            ('Daniel Grano', 'R', 10459, 25.9, 'Lost', False),
            ('Hans Barbe', 'G', 938, 2.3, 'Lost', False)],

    # ── MI House 78 (Nov 6) ──
    47756: [('Dave Pagel', 'R', 23227, 61.1, 'Won', False),
            ('Jack Arbanas', 'D', 14802, 38.9, 'Lost', False)],

    # ── MN House 54 (Nov 6) — mapped to old 54A ──
    49130: [('Dan Schoen', 'D', 11069, 54.9, 'Won', True),
            ('Derrick Lehrke', 'R', 7664, 38.0, 'Lost', False),
            ('Ron Lischeid', 'IP', 1428, 7.1, 'Lost', False)],

    # ── MO House 46 (Nov 6) ──
    50736: [('Stephen Webber', 'D', 12202, 65.0, 'Won', True),
            ('Fred Berry', 'R', 6564, 35.0, 'Lost', False)],

    # ── MO House 6 (Nov 6) ──
    50221: [('Tim Remole', 'R', 10171, 64.7, 'Won', False),
            ('Diana Scott', 'D', 5559, 35.3, 'Lost', False)],

    # ── MT Senate 22 (Nov 6) ──
    53320: [('Taylor Brown', 'R', 6187, 70.8, 'Won', True),
            ('Jean Lemire Dahlman', 'D', 2548, 29.2, 'Lost', False)],

    # ── NC House 71 (Nov 6) ──
    63584: [('Evelyn Terry', 'D', 23545, 77.9, 'Won', False),
            ('Kris McCann', 'R', 6664, 22.1, 'Lost', False)],

    # ── NC Senate 40 (Nov 6) ──
    62800: [('Malcolm Graham', 'D', 63925, 84.1, 'Won', True),
            ('Earl Lyndon Philip', 'R', 12075, 15.9, 'Lost', False)],

    # ── ND Senate 40 (Nov 6) — effectively unopposed ──
    64331: [('Karen Krebsbach', 'R', 3564, 97.6, 'Won', True)],

    # ── NH House Hillsborough-29 Seat A (Nov 6) — 3-seat district ──
    57314: [('Paul Hackel', 'D', 2189, 18.9, 'Won', False),
            ('Suzanne Vail', 'D', 2168, 18.8, 'Won', False),
            ('Michael McCarthy', 'R', 2035, 17.6, 'Won', True),
            ('Ward Shaff', 'D', 1768, 15.3, 'Lost', False),
            ('Michael Balboni', 'R', 1720, 14.9, 'Lost', True),
            ('Donald McClarren', 'R', 1676, 14.5, 'Lost', True)],

    # ── NH House Hillsborough-36 Seat A (Nov 6) — 3-seat district ──
    57420: [('Michael O\'Brien', 'D', 2352, 19.8, 'Won', False),
            ('Linda Harriott-Gathright', 'D', 2075, 17.4, 'Won', False),
            ('Martin Jack', 'D', 2063, 17.3, 'Won', False),
            ('Bill Ohm', 'R', 2002, 16.8, 'Lost', True),
            ('David Robbins', 'R', 1744, 14.6, 'Lost', True),
            ('Willard Brown', 'R', 1671, 14.0, 'Lost', False)],

    # ── NH House Merrimack-20 Seat A (Nov 6) — 3-seat district ──
    57839: [('Sally Kelly', 'D', 2670, 19.6, 'Won', False),
            ('Frank Davis', 'D', 2592, 19.0, 'Won', False),
            ('Dianne Schuett', 'D', 2326, 17.0, 'Won', False),
            ('Brandon Giuda', 'R', 2206, 16.2, 'Lost', True),
            ('Brian Seaworth', 'R', 1979, 14.5, 'Lost', True),
            ('Brandon Ross', 'R', 1880, 13.8, 'Lost', False)],

    # ── NH House Sullivan-4 Seat A (Nov 6) ──
    58883: [('Thomas Donovan', 'D', 1138, 56.9, 'Won', False),
            ('Charlene Lovett', 'R', 862, 43.1, 'Lost', True)],

    # ── NM Senate 41 (Nov 6) — unopposed ──
    59597: [('Carroll Leavell', 'R', 8413, 100.0, 'Won', True)],

    # ── NV Assembly 19 (Nov 6) ──
    55486: [('Cresent Hardy', 'R', 13152, 56.6, 'Won', True),
            ('Felipe Rodriguez', 'D', 10090, 43.4, 'Lost', False)],

    # ── NY Assembly 139 (Nov 6) ──
    62324: [('Stephen Hawley', 'R', 39886, 93.2, 'Won', True),
            ('Mark E. Glogowski', 'L', 2919, 6.8, 'Lost', False)],

    # ── NY Assembly 58 (Nov 6) — unopposed ──
    61600: [('N. Nick Perry', 'D', 38495, 100.0, 'Won', True)],

    # ── NY Senate 40 (Nov 6) ──
    60859: [('Greg Ball', 'R', 64991, 51.0, 'Won', True),
            ('Justin R. Wagner', 'D', 62325, 49.0, 'Lost', False)],

    # ── OH House 89 (Nov 6) ──
    66219: [('Chris Redfern', 'D', 36025, 61.4, 'Won', False),
            ('Donald J. Janik', 'R', 22600, 38.6, 'Lost', False)],

    # ── PA House 153 (Nov 6) — general after special ──
    70649: [('Madeleine Dean', 'D', 18439, 64.6, 'Won', True),
            ('Nicholas Mattiacci', 'R', 9792, 34.3, 'Lost', False),
            ('Kenneth V. Krawchuk', 'L', 289, 1.0, 'Lost', False)],

    # ── PA House 31 (Nov 6) ──
    69103: [('Steven Santarsiero', 'D', 20640, 57.7, 'Won', True),
            ('Anne Chapman', 'R', 15105, 42.3, 'Lost', False)],

    # ── RI House 43 (Nov 6) ──
    72214: [('Deborah Fellela', 'D', 3887, 64.8, 'Won', True),
            ('Karin Gorman', 'I', 2097, 35.0, 'Lost', False)],

    # ── SC House 37 (Nov 6) — unopposed in general ──
    73126: [('Donna Wood', 'R', 9828, 98.2, 'Won', False)],

    # ── SD House 5 Seat A (Nov 6) — 2-seat district ──
    74267: [('Melissa Magstadt', 'R', 5950, 40.1, 'Won', True),
            ('Roger Solum', 'R', 5844, 39.4, 'Won', True),
            ('Dorothy Kellogg', 'D', 3042, 20.5, 'Lost', False)],

    # ── TN House 88 (Nov 6) ──
    75888: [('Larry Miller', 'D', 15816, 75.3, 'Won', True),
            ('Harry Barber', 'R', 5178, 24.7, 'Lost', False)],

    # ── TX House 97 (Nov 6) ──
    77524: [('Craig Goldman', 'R', 38139, 59.4, 'Won', False),
            ('Gary Grassia', 'D', 24159, 37.6, 'Lost', False),
            ('Rod Wingo', 'L', 1873, 2.9, 'Lost', False)],

    # ── UT House 11 (Nov 6) ──
    78557: [('Brad Dee', 'R', 9266, 68.1, 'Won', True),
            ('Pamela Udy', 'D', 4332, 31.9, 'Lost', False)],

    # ── WA Senate 41 (Nov 6) ──
    81451: [('Steve Litzow', 'R', 37314, 54.1, 'Won', True),
            ('Maureen Judge', 'D', 31734, 45.9, 'Lost', False)],

    # ── WI Governor Recall (Jun 5) ──
    20042: [('Scott Walker', 'R', 1335585, 53.1, 'Won', True),
            ('Tom Barrett', 'D', 1164480, 46.3, 'Lost', False),
            ('Hari Trivedi', 'I', 14463, 0.6, 'Lost', False)],

    # ── WV House of Delegates 22 (Nov 6) — 2-seat district ──
    82502: [('Josh Stowers', 'D', 6232, 31.2, 'Won', True),
            ('Jeff Eldridge', 'D', 5262, 26.3, 'Won', False),
            ('Michel G. Moffatt', 'R', 4851, 24.3, 'Lost', False),
            ('Gary L. Johngrass', 'R', 3637, 18.2, 'Lost', False)],
}


def main():
    parser = argparse.ArgumentParser(description='Backfill 2012 candidacies')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    # ── Step 1: Fix dates ──
    print(f'=== Step 1: Fix {len(DATE_FIXES)} election dates ===')
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
