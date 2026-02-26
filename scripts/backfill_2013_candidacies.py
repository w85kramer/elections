#!/usr/bin/env python3
"""Backfill missing candidacy records for 2013 elections.

Covers 42 elections: 40 NJ Senate generals + NH Sullivan-4 special + SC House 17 special.
Also fixes null election dates.

Data sourced from Ballotpedia (February 2026).

Usage:
    python3 scripts/backfill_2013_candidacies.py --dry-run
    python3 scripts/backfill_2013_candidacies.py
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
    # NJ Senate — all null → 2013-11-05
    58953: '2013-11-05', 58962: '2013-11-05', 58971: '2013-11-05',
    58980: '2013-11-05', 58989: '2013-11-05', 58998: '2013-11-05',
    59007: '2013-11-05', 59016: '2013-11-05', 59025: '2013-11-05',
    59034: '2013-11-05', 59043: '2013-11-05', 59052: '2013-11-05',
    59061: '2013-11-05', 59070: '2013-11-05', 59079: '2013-11-05',
    59088: '2013-11-05', 59097: '2013-11-05', 59106: '2013-11-05',
    59115: '2013-11-05', 59123: '2013-11-05', 59132: '2013-11-05',
    59141: '2013-11-05', 59150: '2013-11-05', 59159: '2013-11-05',
    59170: '2013-11-05', 59179: '2013-11-05', 59188: '2013-11-05',
    59197: '2013-11-05', 59205: '2013-11-05', 59214: '2013-11-05',
    59223: '2013-11-05', 59232: '2013-11-05', 59240: '2013-11-05',
    59249: '2013-11-05', 59258: '2013-11-05', 59267: '2013-11-05',
    59276: '2013-11-05', 59286: '2013-11-05', 59295: '2013-11-05',
    59304: '2013-11-05',
    # Specials
    58882: '2013-06-04',  # NH Sullivan-4
    72949: '2013-03-12',  # SC House 17
}

# ══════════════════════════════════════════════════════════════
# DATA: election_id -> [(name, party, votes, pct, result, inc)]
# ══════════════════════════════════════════════════════════════

DATA = {
    # ── NJ Senate 2013 (all 40 districts, Nov 5) ──
    58953: [('Jeff Van Drew', 'D', 34624, 59.4, 'Won', True),
            ('Susan Adelizzi Schmidt', 'R', 22835, 39.2, 'Lost', False),
            ('Thomas Greto', 'I', 825, 1.4, 'Lost', False)],
    58962: [('Jim Whelan', 'D', 29333, 55.0, 'Won', True),
            ('Frank Balles', 'R', 24006, 45.0, 'Lost', False)],
    58971: [('Stephen Sweeney', 'D', 31045, 54.8, 'Won', True),
            ('Niki Trunk', 'R', 25599, 45.2, 'Lost', False)],
    58980: [('Fred Madden', 'D', 29439, 57.9, 'Won', True),
            ("Giancarlo D'orazio", 'R', 21376, 42.1, 'Lost', False)],
    58989: [('Donald Norcross', 'D', 25383, 57.9, 'Won', True),
            ('Keith Walker', 'R', 18448, 42.1, 'Lost', False)],
    58998: [('James Beach', 'D', 34847, 63.4, 'Won', True),
            ('Sudhir Deshmukh', 'R', 20080, 36.6, 'Lost', False)],
    59007: [('Diane Allen', 'R', 38350, 60.4, 'Won', True),
            ('Gary Catrambone', 'D', 25106, 39.6, 'Lost', False)],
    59016: [('Dawn Addiego', 'R', 35894, 63.5, 'Won', True),
            ('Javier Vasquez', 'D', 20633, 36.5, 'Lost', False)],
    59025: [('Christopher Connors', 'R', 46949, 70.8, 'Won', True),
            ('Anthony Mazella', 'D', 19365, 29.2, 'Lost', False)],
    59034: [('James Holzapfel', 'R', 45565, 69.7, 'Won', True),
            ('John Bendel', 'D', 19807, 30.3, 'Lost', False)],
    59043: [('Jennifer Beck', 'R', 30531, 60.0, 'Won', True),
            ('Michael Brantley', 'D', 19735, 38.8, 'Lost', False),
            ('Marie Amato-Juckiewicz', 'I', 599, 1.2, 'Lost', False)],
    59052: [('Samuel Thompson', 'R', 32911, 65.4, 'Won', True),
            ('Raymond Dothard', 'D', 17440, 34.6, 'Lost', False)],
    59061: [('Joseph Kyrillos', 'R', 40762, 68.1, 'Won', True),
            ('Joseph Marques', 'D', 18289, 30.6, 'Lost', False),
            ('Mac Dara Lyden', 'I', 774, 1.3, 'Lost', False)],
    59070: [('Linda Greenstein', 'D', 31387, 50.4, 'Won', True),
            ('Peter Inverso', 'R', 29903, 48.0, 'Lost', False),
            ('Don Dezarn', 'L', 1014, 1.6, 'Lost', False)],
    59079: [('Shirley Turner', 'D', 30250, 63.3, 'Won', True),
            ('Don Cox', 'R', 17507, 36.7, 'Lost', False)],
    59088: [('Christopher Bateman', 'R', 34865, 60.3, 'Won', True),
            ('Christian Mastondrea', 'D', 22990, 39.7, 'Lost', False)],
    59097: [('Bob Smith', 'D', 22920, 59.8, 'Won', True),
            ('Brian Levine', 'R', 15403, 40.2, 'Lost', False)],
    59106: [('Peter Barnes III', 'D', 25063, 51.9, 'Won', False),
            ('David Stahl', 'R', 23184, 48.1, 'Lost', False)],
    59115: [('Joseph Vitale', 'D', 24126, 62.6, 'Won', True),
            ('Robert Luban', 'R', 14439, 37.4, 'Lost', False)],
    59123: [('Raymond Lesniak', 'D', 21251, 100.0, 'Won', True)],
    59132: [('Thomas Kean', 'R', 42423, 69.6, 'Won', True),
            ('Michael Komondy', 'D', 18517, 30.4, 'Lost', False)],
    59141: [('Nicholas Scutari', 'D', 24899, 59.5, 'Won', True),
            ('Robert Sherr', 'R', 16933, 40.5, 'Lost', False)],
    59150: [('Michael Doherty', 'R', 37477, 67.6, 'Won', True),
            ('Gerard Bowers', 'D', 17311, 31.2, 'Lost', False),
            ('Daniel Seyler', 'I', 672, 1.2, 'Lost', False)],
    59159: [('Steven Oroho', 'R', 38819, 70.4, 'Won', True),
            ('Richard Tomko', 'D', 16292, 29.6, 'Lost', False)],
    59170: [('Anthony Bucco', 'R', 36517, 86.8, 'Won', True),
            ('Maureen Castriotta', 'I', 5577, 13.2, 'Lost', False)],
    59179: [('Joseph Pennacchio', 'R', 35772, 65.0, 'Won', True),
            ('Avery Ann Hart', 'D', 19250, 35.0, 'Lost', False)],
    59188: [('Richard Codey', 'D', 34291, 59.3, 'Won', True),
            ('Lee Holtzman', 'R', 23581, 40.7, 'Lost', False)],
    59197: [('Ronald Rice', 'D', 27265, 75.7, 'Won', True),
            ('Frank Contella', 'R', 8744, 24.3, 'Lost', False)],
    59205: [('Teresa Ruiz', 'D', 16078, 78.3, 'Won', True),
            ('Raafat Barsoom', 'R', 3636, 17.7, 'Lost', False),
            ('Pablo Olivera', 'I', 808, 3.9, 'Lost', False)],
    59214: [('Robert Singer', 'R', 36563, 70.2, 'Won', True),
            ('William Field', 'D', 15535, 29.8, 'Lost', False)],
    59223: [('Sandra Cunningham', 'D', 18822, 73.1, 'Won', True),
            ('Maria Karczewski', 'R', 6932, 26.9, 'Lost', False)],
    59232: [('Nicholas Sacco', 'D', 20098, 70.2, 'Won', True),
            ('Francisco Torres', 'R', 8542, 29.8, 'Lost', False)],
    59240: [('Brian Stack', 'D', 26980, 80.7, 'Won', True),
            ('James Sanford', 'R', 6460, 19.3, 'Lost', False)],
    59249: [('Nia Gill', 'D', 27132, 73.1, 'Won', True),
            ('Joseph Cupoli', 'R', 9972, 26.9, 'Lost', False)],
    59258: [('Nellie Pou', 'D', 22154, 74.1, 'Won', True),
            ('Lynda Gallashaw', 'R', 7737, 25.9, 'Lost', False)],
    59267: [('Paul Sarlo', 'D', 22677, 59.7, 'Won', True),
            ('Brian Fitzhenry', 'R', 15293, 40.3, 'Lost', False)],
    59276: [('Loretta Weinberg', 'D', 28321, 68.5, 'Won', True),
            ('Paul Duggen', 'R', 13038, 31.5, 'Lost', False)],
    59286: [('Bob Gordon', 'D', 27779, 51.9, 'Won', True),
            ('Fernando Alonso', 'R', 25767, 48.1, 'Lost', False)],
    59295: [('Gerald Cardinale', 'R', 37836, 63.6, 'Won', True),
            ('Jane Bidwell', 'D', 21616, 36.4, 'Lost', False)],
    59304: [("Kevin O'Toole", 'R', 37565, 65.9, 'Won', True),
            ('William Meredith Ashley', 'D', 19401, 34.1, 'Lost', False)],

    # ── NH House Sullivan-4 Special (Jun 4) ──
    58882: [('Joe Osgood', 'R', 322, 56.7, 'Won', False),
            ('Larry Converse', 'D', 246, 43.3, 'Lost', False)],

    # ── SC House 17 Special (Mar 12) — Burns unopposed ──
    72949: [('Mike Burns', 'R', 805, 98.2, 'Won', False)],
}


def main():
    parser = argparse.ArgumentParser(description='Backfill 2013 candidacies')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    # ── Step 1: Fix dates ──
    print(f'=== Step 1: Fix {len(DATE_FIXES)} election dates ===')
    if args.dry_run:
        print(f'  [DRY RUN] Would fix {len(DATE_FIXES)} null dates')
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
