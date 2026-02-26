#!/usr/bin/env python3
"""Backfill missing candidacy records for 2003, 2010, and 2011 elections.

Covers 8 elections:
- 2003: CA Governor Recall (Schwarzenegger vs Davis recall)
- 2010: AL House 47, MD HoD 26 Seat A (3-member), NE Legislature 40,
        OR Senate 13, PA Senate 40
- 2011: MS House 33, VA Senate 40

Data sourced from Ballotpedia (February 2026).

Usage:
    python3 scripts/backfill_2003_2010_2011_candidacies.py --dry-run
    python3 scripts/backfill_2003_2010_2011_candidacies.py
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
# DATE FIXES
# ══════════════════════════════════════════════════════════════
DATE_FIXES = {
    # CA Recall — was 2003-11-04, actually Oct 7
    19237: '2003-10-07',
    # OR Senate 13 — filing deadline 2010-03-09
    67372: '2010-11-02',
    # MS House 33 — filing deadline 2011-06-01
    52702: '2011-11-08',
    # VA Senate 40 — was NULL
    80590: '2011-11-08',
}

# ══════════════════════════════════════════════════════════════
# DATA: election_id -> [(name, party, votes, pct, result, inc)]
# ══════════════════════════════════════════════════════════════

DATA = {
    # ── CA Governor Recall (Oct 7, 2003) — top 5 replacement candidates ──
    19237: [('Arnold Schwarzenegger', 'R', 4206284, 48.6, 'Won', False),
            ('Cruz Bustamante', 'D', 2724874, 31.5, 'Lost', False),
            ('Tom McClintock', 'R', 1161287, 13.4, 'Lost', False),
            ('Peter Camejo', 'G', 242247, 2.8, 'Lost', False),
            ('Arianna Huffington', 'I', 47505, 0.5, 'Lost', False)],

    # ── AL House 47 (Nov 2, 2010) ──
    21158: [('Jack Williams', 'R', 8735, 61.2, 'Won', True),
            ('Chip McCallum III', 'I', 5527, 38.8, 'Lost', False)],

    # ── MD House of Delegates 26 Seat A (Nov 2, 2010) — 3-member district ──
    43863: [('Veronica Turner', 'D', 27770, 35.1, 'Won', True),
            ('Jay Walker', 'D', 24328, 30.7, 'Won', True),
            ('Kris Valderrama', 'D', 24141, 30.5, 'Won', True),
            ('Holly Henderson', 'R', 2916, 3.7, 'Lost', False)],

    # ── NE Legislature 40 (Nov 2, 2010) — nonpartisan ──
    55112: [('Tyson Larson', 'NP', 5583, 52.3, 'Won', False),
            ('Merton Dierks', 'NP', 5085, 47.7, 'Lost', True)],

    # ── OR Senate 13 (Nov 2, 2010) ──
    67372: [('Larry George', 'R', 30457, 63.2, 'Won', True),
            ('Timi Parker', 'D', 17742, 36.8, 'Lost', False)],

    # ── PA Senate 40 (Nov 2, 2010) ──
    68645: [('Jane Orie', 'R', 58825, 58.0, 'Won', True),
            ('Dan DeMarco', 'D', 42643, 42.0, 'Lost', False)],

    # ── MS House 33 (Nov 8, 2011) ──
    52702: [('Thomas Reynolds II', 'D', 5928, 73.8, 'Won', True),
            ('Jerrerico Chambers', 'R', 1798, 22.4, 'Lost', False),
            ('Sean Holmes', 'L', 302, 3.8, 'Lost', False)],

    # ── VA Senate 40 (Nov 8, 2011) — open seat ──
    80590: [('Bill Carrico', 'R', 31333, 66.9, 'Won', False),
            ('John Lamie', 'D', 15480, 33.1, 'Lost', False)],
}


def main():
    parser = argparse.ArgumentParser(description='Backfill 2003/2010/2011 candidacies')
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
            values = ','.join(f"('{esc(n)}')" for n in missing)
            result = run_sql(
                f"INSERT INTO candidates (full_name) VALUES {values} RETURNING id, full_name"
            )
            if result:
                for r in result:
                    existing_map[r['full_name']] = r['id']
                print(f'  Inserted {len(result)} candidates')
            else:
                print(f'  FAILED to insert candidates')
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

    sql = (
        "INSERT INTO candidacies "
        "(election_id, candidate_id, party, votes_received, vote_percentage, "
        "result, is_incumbent, is_write_in) VALUES " + ','.join(all_values)
    )
    result = run_sql(sql)
    if result is not None:
        print(f'  Inserted {len(all_values)} candidacies')
    else:
        print(f'  FAILED to insert candidacies')
        return

    print(f'\nDone! Inserted {len(all_values)} candidacies across {len(DATA)} elections.')


if __name__ == '__main__':
    main()
