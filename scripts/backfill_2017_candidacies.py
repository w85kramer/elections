#!/usr/bin/env python3
"""Backfill missing candidacy records for 2017 general elections.

Covers:
- NJ Senate 2017: all 40 districts (0 candidacies existed)
- VA House of Delegates 2017: 5 districts with 0 candidacies (20, 31, 69, 88, 94)

Data sourced from Ballotpedia (February 2026).

Usage:
    python3 scripts/backfill_2017_candidacies.py --dry-run
    python3 scripts/backfill_2017_candidacies.py
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
# DATA: election_id -> [(name, party, votes, pct, result, inc)]
# ══════════════════════════════════════════════════════════════

DATA = {
    # ── NJ Senate 2017 (all 40 districts) ──
    58952: [('Jeff Van Drew', 'D', 35464, 64.83, 'Won', True),
            ('Mary Gruccio', 'R', 18589, 33.98, 'Lost', False)],
    58961: [('Chris Brown', 'R', 26950, 53.52, 'Won', False),
            ('Colin Bell', 'D', 23406, 46.48, 'Lost', False)],
    58970: [('Stephen Sweeney', 'D', 31822, 58.76, 'Won', True),
            ('Fran Grenier', 'R', 22336, 41.24, 'Lost', False)],
    58979: [('Fred Madden', 'D', 38790, 100.0, 'Won', True)],
    58988: [('Nilsa Cruz-Perez', 'D', 29031, 66.06, 'Won', True),
            ('Keith Walker', 'R', 14463, 32.91, 'Lost', False)],
    58997: [('James Beach', 'D', 41376, 69.39, 'Won', False),
            ('Robert Shapiro', 'R', 18249, 30.61, 'Lost', False)],
    59006: [('Troy Singleton', 'D', 40685, 65.71, 'Won', True),
            ('John Browne', 'R', 21229, 34.29, 'Lost', False)],
    59015: [('Dawn Addiego', 'R', 30795, 52.24, 'Won', True),
            ('George Youngkin', 'D', 28158, 47.76, 'Lost', False)],
    59024: [('Christopher Connors', 'R', 41438, 64.59, 'Won', True),
            ('Brian Corley White', 'D', 22717, 35.41, 'Lost', False)],
    59033: [('James Holzapfel', 'R', 39555, 62.53, 'Won', True),
            ('Emma Mammano', 'D', 23707, 37.47, 'Lost', False)],
    59042: [('Vin Gopal', 'D', 31308, 53.56, 'Won', False),
            ('Jennifer Beck', 'R', 27150, 46.44, 'Lost', True)],
    59051: [('Samuel Thompson', 'R', 30013, 56.75, 'Won', True),
            ('David Lande', 'D', 21888, 41.38, 'Lost', False)],
    59060: [('Declan O\'Scanlon', 'R', 34976, 55.11, 'Won', True),
            ('Sean Byrnes', 'D', 28493, 44.89, 'Lost', False)],
    59069: [('Linda Greenstein', 'D', 34474, 56.49, 'Won', True),
            ('Ileana Schirmer', 'R', 26548, 43.51, 'Lost', False)],
    59078: [('Shirley Turner', 'D', 36624, 74.04, 'Won', True),
            ('Lee Newton', 'R', 12839, 25.96, 'Lost', False)],
    59087: [('Christopher Bateman', 'R', 32229, 50.45, 'Won', True),
            ('Laurie Poppe', 'D', 31655, 49.55, 'Lost', False)],
    59096: [('Bob Smith', 'D', 29816, 71.44, 'Won', True),
            ('Daryl Kipnis', 'R', 11921, 28.56, 'Lost', False)],
    59105: [('Patrick Diegnan Jr.', 'D', 32175, 65.62, 'Won', True),
            ('Lewis Glogower', 'R', 16860, 34.38, 'Lost', False)],
    59114: [('Joseph Vitale', 'D', 27681, 100.0, 'Won', True)],
    59122: [('Joseph Cryan', 'D', 25772, 83.69, 'Won', False),
            ('Ashraf Hanna', 'R', 5023, 16.31, 'Lost', False)],
    59131: [('Thomas Kean', 'R', 37579, 54.70, 'Won', True),
            ('Jill Lazare', 'D', 31123, 45.30, 'Lost', False)],
    59140: [('Nicholas Scutari', 'D', 29563, 67.30, 'Won', True),
            ('Joseph Bonilla', 'R', 14362, 32.70, 'Lost', False)],
    59149: [('Michael Doherty', 'R', 35676, 59.06, 'Won', True),
            ('Christine Lui Chen', 'D', 24730, 40.94, 'Lost', False)],
    59158: [('Steven Oroho', 'R', 35641, 61.03, 'Won', True),
            ('Jennifer Hamilton', 'D', 22760, 38.97, 'Lost', False)],
    59169: [('Anthony Bucco', 'R', 30659, 52.15, 'Won', True),
            ('Lisa Bhimani', 'D', 28131, 47.85, 'Lost', False)],
    59178: [('Joseph Pennacchio', 'R', 32269, 56.48, 'Won', True),
            ('Elliot Isibor', 'D', 24867, 43.52, 'Lost', False)],
    59187: [('Richard Codey', 'D', 43066, 69.70, 'Won', True),
            ('Pasquale Capozzoli', 'R', 18720, 30.30, 'Lost', False)],
    59196: [('Ronald Rice', 'D', 31774, 96.05, 'Won', True),
            ('Troy Knight-Napper', 'G', 1306, 3.95, 'Lost', False)],
    59204: [('Teresa Ruiz', 'D', 20506, 87.25, 'Won', True),
            ('Maria Lopez', 'R', 2547, 10.84, 'Lost', False)],
    59213: [('Robert Singer', 'R', 30735, 60.17, 'Won', True),
            ('Amy Cores', 'D', 20343, 39.83, 'Lost', False)],
    59222: [('Sandra Cunningham', 'D', 25437, 83.92, 'Won', True),
            ('Herminio Mendoza', 'R', 4874, 16.08, 'Lost', False)],
    59231: [('Nicholas Sacco', 'D', 23736, 80.25, 'Won', True),
            ('Paul Castelli', 'R', 5842, 19.75, 'Lost', False)],
    59239: [('Brian Stack', 'D', 36594, 88.22, 'Won', True),
            ('Beth Hamburger', 'R', 4887, 11.78, 'Lost', False)],
    59248: [('Nia Gill', 'D', 34565, 84.92, 'Won', True),
            ('Mahir Saleh', 'R', 6136, 15.08, 'Lost', False)],
    59257: [('Nellie Pou', 'D', 21425, 78.99, 'Won', True),
            ('Marwan Sholakh', 'R', 5698, 21.01, 'Lost', False)],
    59266: [('Paul Sarlo', 'D', 24044, 65.83, 'Won', True),
            ('Jeanine Ferrara', 'R', 12482, 34.17, 'Lost', False)],
    59275: [('Loretta Weinberg', 'D', 33017, 75.37, 'Won', True),
            ('Modesto Romero', 'R', 10788, 24.63, 'Lost', False)],
    59285: [('Robert Gordon', 'D', 30881, 57.06, 'Won', True),
            ('Kelly Langschultz', 'R', 23238, 42.94, 'Lost', False)],
    59294: [('Gerald Cardinale', 'R', 33752, 52.77, 'Won', True),
            ('Linda Schwager', 'D', 29631, 46.33, 'Lost', False)],
    59303: [('Kristin Corrado', 'R', 33495, 56.24, 'Won', False),
            ('Thomas Duch', 'D', 26060, 43.76, 'Lost', False)],

    # ── VA House of Delegates 2017 (5 empty districts) ──
    # D20: Richard Bell (R) vs Michele Edwards (D)
    80704: [('Richard Bell', 'R', 14344, 54.57, 'Won', True),
            ('Michele Edwards', 'D', 11197, 42.60, 'Lost', False)],
    # D31: Elizabeth Guzman (D) vs Scott Lingamfelter (R) — D flip
    80773: [('Elizabeth Guzman', 'D', 15466, 54.07, 'Won', False),
            ('Scott Lingamfelter', 'R', 12658, 44.25, 'Lost', True)],
    # D69: Betsy Carr (D) — unopposed
    81002: [('Betsy Carr', 'D', 19775, 100.0, 'Won', True)],
    # D88: Mark Cole (R) vs Steve Aycock (D)
    81122: [('Mark Cole', 'R', 14022, 52.78, 'Won', True),
            ('Steve Aycock', 'D', 9918, 37.33, 'Lost', False)],
    # D94: David Yancey (R) vs Shelly Simonds (D) — exact tie, won by drawing
    81160: [('David Yancey', 'R', 11608, 48.59, 'Won', False),
            ('Shelly Simonds', 'D', 11608, 48.59, 'Lost', False)],
}


def main():
    parser = argparse.ArgumentParser(description='Backfill 2017 candidacies')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    all_names = set()
    for eid, candidates in DATA.items():
        for c in candidates:
            all_names.add(c[0])

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
    print(f'\nExisting candidates: {len(existing_map)}')
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

    # Build candidacies
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
        print(f'\n[DRY RUN] Would insert {len(all_values)} candidacies')
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
