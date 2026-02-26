#!/usr/bin/env python3
"""Backfill missing candidacy records for 2015 elections.

Covers 30 elections: 24 VA HoD, 3 VA Senate, 2 MS House, 1 LA House Primary.
Also fixes null/incorrect election dates.

Data sourced from Ballotpedia (February 2026).

Usage:
    python3 scripts/backfill_2015_candidacies.py --dry-run
    python3 scripts/backfill_2015_candidacies.py
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
# VA elections were all null; MS HD-33 had filing deadline
# ══════════════════════════════════════════════════════════════
DATE_FIXES = {
    # VA HoD — all null → 2015-11-03
    80603: '2015-11-03', 80649: '2015-11-03', 80660: '2015-11-03',
    80666: '2015-11-03', 80712: '2015-11-03', 80774: '2015-11-03',
    80780: '2015-11-03', 80786: '2015-11-03', 80793: '2015-11-03',
    80813: '2015-11-03', 80821: '2015-11-03', 80840: '2015-11-03',
    80846: '2015-11-03', 80894: '2015-11-03', 80924: '2015-11-03',
    80996: '2015-11-03', 81015: '2015-11-03', 81080: '2015-11-03',
    81108: '2015-11-03', 81115: '2015-11-03', 81154: '2015-11-03',
    81161: '2015-11-03', 81168: '2015-11-03', 81197: '2015-11-03',
    # VA Senate — all null → 2015-11-03
    80418: '2015-11-03', 80455: '2015-11-03', 80566: '2015-11-03',
    # MS House 33 — was 2015-02-27 (filing deadline) → 2015-11-03
    52701: '2015-11-03',
    # MS House 121 — null → 2015-11-03
    53156: '2015-11-03',
    # LA House 83 Primary — null → 2015-10-24
    41666: '2015-10-24',
}

# ══════════════════════════════════════════════════════════════
# DATA: election_id -> [(name, party, votes, pct, result, inc)]
# ══════════════════════════════════════════════════════════════

DATA = {
    # ── VA House of Delegates 2015 (Nov 3) ──
    80603: [('Mark Dudenhefer', 'R', 5839, 50.5, 'Won', True),
            ('Joshua King', 'D', 5714, 49.5, 'Lost', False)],
    80649: [('Randy Minchew', 'R', 10415, 62.1, 'Won', True),
            ('Peter Rush', 'D', 6355, 37.9, 'Lost', False)],
    80660: [('Joseph Yost', 'R', 9245, 58.4, 'Won', True),
            ('Laurie Buchwald', 'D', 6587, 41.6, 'Lost', False)],
    80666: [('Bob Marshall', 'R', 7147, 56.1, 'Won', True),
            ('Donald Shaw', 'D', 5592, 43.9, 'Lost', False)],
    80712: [('Ronald Villanueva', 'R', 6345, 56.9, 'Won', True),
            ('Susan Bates Hippen', 'D', 4812, 43.1, 'Lost', False)],
    80774: [('Scott Lingamfelter', 'R', 9506, 53.4, 'Won', True),
            ('Sara Townsend', 'D', 8287, 46.6, 'Lost', False)],
    80780: [('Thomas Greason', 'R', 9734, 53.1, 'Won', True),
            ('Elizabeth Miller', 'D', 8596, 46.9, 'Lost', False)],
    80786: [('Dave LaRock', 'R', 12004, 59.9, 'Won', True),
            ('Chuck Hedges', 'D', 7300, 36.5, 'Lost', False),
            ('Mark Anderson', 'L', 723, 3.6, 'Lost', False)],
    80793: [('Kathleen Murphy', 'D', 10820, 50.4, 'Won', True),
            ('Craig Parisot', 'R', 10632, 49.6, 'Lost', False)],
    80813: [('David Bulova', 'D', 7065, 57.4, 'Won', True),
            ('Sang Yi', 'R', 5249, 42.6, 'Lost', False)],
    80821: [('Kaye Kory', 'D', 7819, 74.7, 'Won', True),
            ('James Leslie', 'IG', 2655, 25.3, 'Lost', False)],
    80840: [('Dave Albo', 'R', 10837, 63.4, 'Won', True),
            ('Joana Garcia', 'D', 6245, 36.6, 'Lost', False)],
    80846: [('Mark Sickles', 'D', 7696, 63.3, 'Won', True),
            ('Anna Urman', 'R', 4058, 33.4, 'Lost', False),
            ('Paul McIlvaine', 'I', 398, 3.3, 'Lost', False)],
    80894: [('Jackson Miller', 'R', 7820, 58.8, 'Won', True),
            ('Kyle McCullough', 'D', 5484, 41.2, 'Lost', False)],
    80924: [('Buddy Fowler', 'R', 10870, 60.4, 'Won', True),
            ('Toni Radler', 'D', 7118, 39.6, 'Lost', False)],
    80996: [('Manoli Loupassi', 'R', 15715, 61.3, 'Won', True),
            ('Bill Grogan', 'D', 9417, 36.8, 'Lost', False),
            ('Mike Dickinson', 'I', 484, 1.9, 'Lost', False)],
    81015: [('Jennifer McClellan', 'D', 9809, 88.8, 'Won', True),
            ('Steve Imholt', 'I', 1231, 11.2, 'Lost', False)],
    81080: [('Jason Miyares', 'R', 10046, 65.3, 'Won', False),
            ('Bill Fleming', 'D', 5335, 34.7, 'Lost', False)],
    81108: [('Jennifer Boysko', 'D', 8283, 54.5, 'Won', False),
            ('Danny Vargas', 'R', 6390, 42.0, 'Lost', False),
            ('Paul Brubaker', 'I', 526, 3.5, 'Lost', False)],
    81115: [('John Bell', 'D', 8203, 49.9, 'Won', False),
            ('Chuong Nguyen', 'R', 7883, 48.0, 'Lost', False),
            ('Brian Suojanen', 'L', 343, 2.1, 'Lost', False)],
    81154: [('Monty Mason', 'D', 8910, 54.8, 'Won', True),
            ('Lara Overy', 'R', 7354, 45.2, 'Lost', False)],
    81161: [('David Yancey', 'R', 8140, 57.6, 'Won', True),
            ('Shelly Simonds', 'D', 6002, 42.4, 'Lost', False)],
    81168: [('Marcia Price', 'D', 6106, 76.8, 'Won', False),
            ('Pricillia Burnett', 'IG', 1845, 23.2, 'Lost', False)],
    81197: [('Robert Bloxom Jr.', 'R', 8657, 58.0, 'Won', True),
            ('Willie Randall', 'D', 6278, 42.0, 'Lost', False)],

    # ── VA Senate 2015 (Nov 3) ──
    80418: [('John Miller', 'D', 17989, 59.4, 'Won', True),
            ('Mark Matney', 'R', 12278, 40.6, 'Lost', False)],
    80455: [('Glen Sturtevant', 'R', 27651, 49.8, 'Won', False),
            ('Daniel Gecker', 'D', 26173, 47.2, 'Lost', False),
            ('Marleen Durfee', 'I', 1136, 2.0, 'Lost', False),
            ('Carl Loser', 'L', 527, 0.9, 'Lost', False)],
    80566: [('Dick Saslaw', 'D', 18754, 75.6, 'Won', True),
            ('Terry Modglin', 'IG', 6055, 24.4, 'Lost', False)],

    # ── MS House 2015 (Nov 3) ──
    52701: [('Thomas Reynolds II', 'D', 5537, 76.7, 'Won', True),
            ('Jerrerico Chambers', 'R', 1684, 23.3, 'Lost', False)],
    53156: [('Carolyn Crawford', 'R', 3279, 66.9, 'Won', True),
            ('Brian Pearse', 'D', 1625, 33.1, 'Lost', False)],

    # ── LA House Primary 2015 (Oct 24, jungle primary) ──
    41666: [('Robert Billiot', 'D', 4198, 51.8, 'Won', True),
            ('Kyle Green Jr.', 'D', 3904, 48.2, 'Lost', False)],
}


def main():
    parser = argparse.ArgumentParser(description='Backfill 2015 candidacies')
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
