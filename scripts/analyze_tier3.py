#!/usr/bin/env python3
"""
Analyze Tier 3 duplicate candidates in detail and sub-categorize them.

Tier 3a (auto-merge): Clear name variations (middle initial, nickname, suffix)
                      with no timeline conflicts
Tier 3b (likely same): Plausible same person — compatible timelines, related seats
                       (redistricting, house→senate) — light review needed
Tier 3c (uncertain):  Could be different people — overlapping candidacies, big gaps,
                      or names that aren't obviously the same

Usage:
  python3 scripts/analyze_tier3.py                    # Analyze and output CSVs
  python3 scripts/analyze_tier3.py --merge-3a         # Auto-merge Tier 3a pairs
"""

import argparse
import csv
import re
import sys
import os
import time

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from db_config import TOKEN, API_URL

MAX_RETRIES = 5


def run_sql(sql):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = httpx.post(
                API_URL,
                headers={'Authorization': f'Bearer {TOKEN}',
                         'Content-Type': 'application/json'},
                json={'query': sql},
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
                raise RuntimeError(f'SQL error {resp.status_code}: {resp.text[:200]}')
        except httpx.TimeoutException:
            if attempt < MAX_RETRIES:
                print(f'  Timeout, retrying ({attempt}/{MAX_RETRIES})...')
                time.sleep(5 * attempt)
            else:
                raise
    raise RuntimeError(f'Failed after {MAX_RETRIES} retries')


def merge_sql(keep_id, merge_id):
    return f"""
    UPDATE candidacies SET candidate_id = {keep_id}
    WHERE candidate_id = {merge_id}
      AND election_id NOT IN (
        SELECT election_id FROM candidacies WHERE candidate_id = {keep_id}
      );
    UPDATE seat_terms SET candidate_id = {keep_id}
    WHERE candidate_id = {merge_id}
      AND id NOT IN (
        SELECT st2.id FROM seat_terms st1
        JOIN seat_terms st2 ON st1.seat_id = st2.seat_id
          AND st1.start_date = st2.start_date
        WHERE st1.candidate_id = {keep_id} AND st2.candidate_id = {merge_id}
      );
    DELETE FROM candidacies WHERE candidate_id = {merge_id};
    DELETE FROM seat_terms WHERE candidate_id = {merge_id};
    DELETE FROM candidates WHERE id = {merge_id};
    """


def execute_batch(pairs):
    sql = "\n".join(merge_sql(keep, merge) for keep, merge in pairs)
    run_sql(sql)


# ── Name analysis helpers ──

def normalize_name(name):
    """Strip middle initials, suffixes, nicknames, extra spaces."""
    if not name:
        return ''
    n = name.lower().strip()
    # Remove quoted nicknames like "Bob" or "Bob
    n = re.sub(r'"[^"]*"?', '', n)
    # Remove single-letter middle initials (with or without period)
    n = re.sub(r'\b[a-z]\.\s*', '', n)
    n = re.sub(r'\s+[a-z]\s+', ' ', n)
    # Remove suffixes
    n = re.sub(r'\b(jr\.?|sr\.?|ii|iii|iv)\b', '', n, flags=re.IGNORECASE)
    # Collapse whitespace
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def is_nickname_pair(name1, name2):
    """Check if first names are common nickname pairs."""
    NICKNAMES = {
        'william': {'bill', 'billy', 'will', 'willy'},
        'robert': {'bob', 'bobby', 'rob', 'robby'},
        'richard': {'rick', 'dick', 'rich'},
        'james': {'jim', 'jimmy', 'jamie'},
        'john': {'jack', 'johnny', 'jon'},
        'joseph': {'joe', 'joey'},
        'michael': {'mike', 'mikey'},
        'thomas': {'tom', 'tommy'},
        'charles': {'charlie', 'chuck', 'chas'},
        'edward': {'ed', 'eddie', 'ted', 'teddy'},
        'elizabeth': {'liz', 'lizzy', 'beth', 'betty', 'eliza'},
        'margaret': {'maggie', 'meg', 'peggy', 'marge', 'margie'},
        'catherine': {'cathy', 'kate', 'kathy', 'cat', 'katie'},
        'katherine': {'kathy', 'kate', 'katie', 'kat'},
        'patricia': {'pat', 'patty', 'tricia'},
        'jennifer': {'jen', 'jenny'},
        'jessica': {'jess', 'jessie'},
        'stephanie': {'steph'},
        'christopher': {'chris'},
        'nicholas': {'nick', 'nicky'},
        'timothy': {'tim', 'timmy'},
        'stephen': {'steve', 'steven'},
        'steven': {'steve', 'stephen'},
        'daniel': {'dan', 'danny'},
        'matthew': {'matt'},
        'anthony': {'tony'},
        'donald': {'don', 'donny'},
        'kenneth': {'ken', 'kenny'},
        'ronald': {'ron', 'ronny'},
        'lawrence': {'larry'},
        'raymond': {'ray'},
        'gerald': {'jerry', 'gerry'},
        'benjamin': {'ben'},
        'samuel': {'sam'},
        'deborah': {'deb', 'debbie', 'debby'},
        'debra': {'deb', 'debbie', 'debby'},
        'virginia': {'ginny', 'ginger'},
        'dorothy': {'dot', 'dottie'},
        'barbara': {'barb', 'barbie'},
        'alexander': {'alex'},
        'alexandra': {'alex', 'lexi'},
        'jonathan': {'jon', 'john'},
        'nathaniel': {'nate', 'nathan'},
        'nathan': {'nate'},
        'phillip': {'phil'},
        'zachary': {'zach', 'zack'},
        'frederick': {'fred', 'freddy'},
        'douglas': {'doug'},
        'harold': {'hal', 'harry'},
        'leonard': {'len', 'lenny'},
        'arthur': {'art'},
        'clifford': {'cliff'},
        'russell': {'russ'},
        'terrence': {'terry'},
        'theresa': {'terry', 'tess'},
        'andrew': {'andy', 'drew'},
        'gregory': {'greg'},
        'jeffrey': {'jeff'},
        'peter': {'pete'},
        'walter': {'walt'},
    }

    f1 = name1.split()[0].lower() if name1.split() else ''
    f2 = name2.split()[0].lower() if name2.split() else ''

    if f1 == f2:
        return True

    # Check both directions
    for canonical, nicks in NICKNAMES.items():
        all_forms = {canonical} | nicks
        if f1 in all_forms and f2 in all_forms:
            return True

    return False


def names_are_clear_variation(name1, name2):
    """Return True if names are clearly the same person (middle initial, nickname, suffix diff)."""
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)

    # After normalization, identical
    if n1 == n2:
        return True

    # Check if it's just a nickname difference with same last name
    parts1 = name1.lower().split()
    parts2 = name2.lower().split()
    if parts1 and parts2:
        last1 = parts1[-1].rstrip('.')
        last2 = parts2[-1].rstrip('.')
        if last1 == last2 and is_nickname_pair(name1, name2):
            return True

    return False


def pick_canonical(id1, id2, info):
    """Choose canonical record. Returns (keep_id, merge_id)."""
    i1 = info.get(id1, {})
    i2 = info.get(id2, {})
    st1 = i1.get('st_count', 0)
    st2 = i2.get('st_count', 0)
    ca1 = i1.get('ca_count', 0)
    ca2 = i2.get('ca_count', 0)

    if st1 > 0 and st2 == 0:
        return id1, id2
    if st2 > 0 and st1 == 0:
        return id2, id1
    if ca1 > ca2:
        return id1, id2
    if ca2 > ca1:
        return id2, id1
    return (id1, id2) if id1 < id2 else (id2, id1)


def main():
    parser = argparse.ArgumentParser(description='Analyze Tier 3 duplicates in detail')
    parser.add_argument('--review-file', default='/tmp/dedup_review.csv')
    parser.add_argument('--merge-3a', action='store_true', help='Auto-merge Tier 3a pairs')
    parser.add_argument('--merge-3b', action='store_true', help='Auto-merge Tier 3b pairs')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    # Read Tier 3 pairs
    print(f'Reading Tier 3 pairs from {args.review_file}...')
    with open(args.review_file) as f:
        reader = csv.DictReader(f)
        tier3_rows = [row for row in reader if row['tier'] == '3']

    print(f'  {len(tier3_rows)} Tier 3 pairs')

    # Collect all candidate IDs
    all_ids = set()
    for row in tier3_rows:
        all_ids.add(int(row['id1']))
        all_ids.add(int(row['id2']))

    print(f'  {len(all_ids)} unique candidate IDs')

    # Fetch candidacy details for all candidates in one query
    id_list = ','.join(str(i) for i in sorted(all_ids))
    print('  Fetching candidacy details...')
    candidacies = run_sql(f"""
        SELECT ca.candidate_id, e.election_year, e.election_type, e.seat_id,
               s.seat_label as seat_name, d.chamber,
               d.district_name as district, st.abbreviation as state,
               ca.party, ca.result
        FROM candidacies ca
        JOIN elections e ON ca.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE ca.candidate_id IN ({id_list})
        ORDER BY ca.candidate_id, e.election_year, e.election_type
    """)

    print('  Fetching seat_term details...')
    seat_terms = run_sql(f"""
        SELECT st.candidate_id, st.seat_id, st.start_date, st.end_date,
               s.seat_label as seat_name, d.chamber,
               d.district_name as district, sta.abbreviation as state
        FROM seat_terms st
        JOIN seats s ON st.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states sta ON d.state_id = sta.id
        WHERE st.candidate_id IN ({id_list})
        ORDER BY st.candidate_id, st.start_date
    """)

    # Build per-candidate info
    info = {}
    for cid in all_ids:
        info[cid] = {'candidacies': [], 'seat_terms': [], 'years': set(),
                     'seats': set(), 'chambers': set(), 'districts': set(),
                     'st_count': 0, 'ca_count': 0}

    for c in candidacies:
        cid = c['candidate_id']
        info[cid]['candidacies'].append(c)
        info[cid]['years'].add(c['election_year'])
        info[cid]['seats'].add(c['seat_id'])
        if c.get('chamber'):
            info[cid]['chambers'].add(c['chamber'])
        if c.get('district'):
            info[cid]['districts'].add(c['district'])
        info[cid]['ca_count'] += 1

    for st in seat_terms:
        cid = st['candidate_id']
        info[cid]['seat_terms'].append(st)
        info[cid]['seats'].add(st['seat_id'])
        if st.get('chamber'):
            info[cid]['chambers'].add(st['chamber'])
        info[cid]['st_count'] += 1

    # ── Categorize each pair ──
    tier3a = []  # Clear name variation, no conflicts
    tier3b = []  # Likely same, compatible timelines
    tier3c = []  # Uncertain

    for row in tier3_rows:
        id1, id2 = int(row['id1']), int(row['id2'])
        name1, name2 = row['name1'], row['name2']
        state = row['state']
        i1, i2 = info[id1], info[id2]

        # Check for timeline conflicts (same year, same election type, different seats)
        years1 = {(c['election_year'], c['election_type']) for c in i1['candidacies']}
        years2 = {(c['election_year'], c['election_type']) for c in i2['candidacies']}
        overlapping_elections = years1 & years2

        # Check if overlapping elections are for different seats (= probably different people)
        conflict = False
        for yr, etype in overlapping_elections:
            seats1 = {c['seat_id'] for c in i1['candidacies']
                      if c['election_year'] == yr and c['election_type'] == etype}
            seats2 = {c['seat_id'] for c in i2['candidacies']
                      if c['election_year'] == yr and c['election_type'] == etype}
            if seats1 and seats2 and not (seats1 & seats2):
                conflict = True
                break

        clear_variation = names_are_clear_variation(name1, name2)

        # Build detail string for CSV
        def summarize(cid):
            i = info[cid]
            parts = []
            for c in i['candidacies']:
                chamber_short = (c.get('chamber') or '?')[0]  # H/S/G
                parts.append(f"{c['election_year']}{c['election_type'][0].upper()}-{chamber_short}:{c.get('district','?')}")
            for st in i['seat_terms']:
                chamber_short = (st.get('chamber') or '?')[0]
                start_yr = st['start_date'][:4] if st.get('start_date') else '?'
                end_yr = st['end_date'][:4] if st.get('end_date') else 'now'
                parts.append(f"ST:{start_yr}-{end_yr}-{chamber_short}:{st.get('district','?')}")
            return '; '.join(parts) if parts else '(no records)'

        detail1 = summarize(id1)
        detail2 = summarize(id2)

        # Check for plausible progression (same district across redistricting, or house→senate)
        shared_districts = i1['districts'] & i2['districts']
        chamber_progression = (i1['chambers'] != i2['chambers'])  # different chambers = house→senate

        entry = {
            'state': state,
            'id1': id1, 'name1': name1, 'detail1': detail1,
            'id2': id2, 'name2': name2, 'detail2': detail2,
            'conflict': conflict,
            'clear_variation': clear_variation,
            'shared_districts': bool(shared_districts),
            'chamber_progression': chamber_progression,
        }

        if conflict:
            # Ran in different seats in the same election — likely different people
            entry['reason'] = 'CONFLICT: same election, different seats'
            tier3c.append(entry)
        elif clear_variation:
            # Name is obviously the same person (middle initial, nickname, etc.)
            entry['reason'] = 'Clear name variation, no conflicts'
            tier3a.append(entry)
        elif shared_districts or chamber_progression:
            entry['reason'] = f'{"shared district" if shared_districts else ""} {"chamber move" if chamber_progression else ""}'.strip()
            tier3b.append(entry)
        else:
            entry['reason'] = 'Same first-name prefix only'
            tier3c.append(entry)

    print(f'\n  Tier 3a (auto-merge): {len(tier3a)} pairs — clear name variations')
    print(f'  Tier 3b (likely same): {len(tier3b)} pairs — compatible timelines')
    print(f'  Tier 3c (uncertain):  {len(tier3c)} pairs — needs review\n')

    # Write detailed CSVs
    fields = ['state', 'id1', 'name1', 'detail1', 'id2', 'name2', 'detail2', 'reason']

    for tier, name, rows in [('3a', 'auto_merge', tier3a),
                              ('3b', 'likely_same', tier3b),
                              ('3c', 'uncertain', tier3c)]:
        path = f'/tmp/dedup_tier{tier}_{name}.csv'
        with open(path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        print(f'  {path} ({len(rows)} pairs)')

    # ── Merge 3a if requested ──
    if args.merge_3a:
        print(f'\n{"DRY RUN — " if args.dry_run else ""}Merging Tier 3a pairs...')

        # Deduplicate: pick one pair per candidate cluster
        # (e.g., if A-B, A-C, B-C all appear, merge into one)
        merged_away = set()
        merge_pairs = []

        # Group by clusters: find connected components
        from collections import defaultdict
        adj = defaultdict(set)
        for entry in tier3a:
            adj[entry['id1']].add(entry['id2'])
            adj[entry['id2']].add(entry['id1'])

        visited = set()
        clusters = []
        for node in adj:
            if node in visited:
                continue
            cluster = []
            stack = [node]
            while stack:
                n = stack.pop()
                if n in visited:
                    continue
                visited.add(n)
                cluster.append(n)
                stack.extend(adj[n] - visited)
            clusters.append(cluster)

        print(f'  {len(clusters)} clusters from {len(tier3a)} pairs')

        batch = []
        merged = 0
        BATCH_SIZE = 20

        for cluster in clusters:
            # Pick canonical: most seat_terms, then most candidacies, then lowest id
            cluster.sort(key=lambda cid: (-info[cid]['st_count'], -info[cid]['ca_count'], cid))
            keep_id = cluster[0]
            for merge_id in cluster[1:]:
                if args.dry_run:
                    print(f'  MERGE: #{keep_id} ← #{merge_id}')
                    merged += 1
                else:
                    batch.append((keep_id, merge_id))
                    if len(batch) >= BATCH_SIZE:
                        execute_batch(batch)
                        merged += len(batch)
                        print(f'  Merged batch of {len(batch)} (total: {merged})')
                        batch = []
                        time.sleep(2)

        if batch:
            execute_batch(batch)
            merged += len(batch)
            print(f'  Merged final batch of {len(batch)} (total: {merged})')

        print(f'  {"Would merge" if args.dry_run else "Merged"}: {merged} candidates')

    # ── Merge 3b if requested ──
    if args.merge_3b:
        print(f'\n{"DRY RUN — " if args.dry_run else ""}Merging Tier 3b pairs...')

        from collections import defaultdict
        adj = defaultdict(set)
        for entry in tier3b:
            adj[entry['id1']].add(entry['id2'])
            adj[entry['id2']].add(entry['id1'])

        visited = set()
        clusters = []
        for node in adj:
            if node in visited:
                continue
            cluster = []
            stack = [node]
            while stack:
                n = stack.pop()
                if n in visited:
                    continue
                visited.add(n)
                cluster.append(n)
                stack.extend(adj[n] - visited)
            clusters.append(cluster)

        print(f'  {len(clusters)} clusters from {len(tier3b)} pairs')

        batch = []
        merged = 0
        BATCH_SIZE = 20

        for cluster in clusters:
            cluster.sort(key=lambda cid: (-info[cid]['st_count'], -info[cid]['ca_count'], cid))
            keep_id = cluster[0]
            for merge_id in cluster[1:]:
                if args.dry_run:
                    print(f'  MERGE: #{keep_id} ← #{merge_id}')
                    merged += 1
                else:
                    batch.append((keep_id, merge_id))
                    if len(batch) >= BATCH_SIZE:
                        execute_batch(batch)
                        merged += len(batch)
                        print(f'  Merged batch of {len(batch)} (total: {merged})')
                        batch = []
                        time.sleep(2)

        if batch:
            execute_batch(batch)
            merged += len(batch)
            print(f'  Merged final batch of {len(batch)} (total: {merged})')

        print(f'  {"Would merge" if args.dry_run else "Merged"}: {merged} candidates')

    print('\nDone.')


if __name__ == '__main__':
    main()
