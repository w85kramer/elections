"""
Populate NJ 2001-2011 primary election results from old-format PDFs.

Creates Primary_D and Primary_R elections and candidacies for:
- Assembly: 2001, 2003, 2005, 2007, 2009, 2011
- Senate: 2001, 2003, 2007, 2011

Reads parsed data from /tmp/nj_old_format_primaries_parsed.json.

Usage:
    python3 scripts/populate_nj_old_primaries.py --dry-run
    python3 scripts/populate_nj_old_primaries.py
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
INPUT_PATH = '/tmp/nj_old_format_primaries_parsed.json'

PRIMARY_DATES = {
    2001: '2001-06-05',
    2003: '2003-06-03',
    2005: '2005-06-07',
    2007: '2007-06-05',
    2009: '2009-06-02',
    2011: '2011-06-07',
}

TO_POPULATE = [
    ('assembly', 2001), ('senate', 2001),
    ('assembly', 2003), ('senate', 2003),
    ('assembly', 2005),
    ('assembly', 2007), ('senate', 2007),
    ('assembly', 2009),
    ('assembly', 2011), ('senate', 2011),
]


def run_sql(query, exit_on_error=True):
    for attempt in range(5):
        resp = httpx.post(
            f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query}, timeout=120
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
        n = re.sub(r'"[^"]*"', '', n)
        n = n.replace(',', '')
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
    if len(first1) >= 1 and len(first2) >= 1 and first1[0] == first2[0]:
        return 0.7
    if len(first1) <= 2 and len(parts1) >= 3:
        if parts1[1] == first2 or first2.startswith(parts1[1]) or parts1[1].startswith(first2):
            return 0.85
    if len(first2) <= 2 and len(parts2) >= 3:
        if parts2[1] == first1 or first1.startswith(parts2[1]) or parts2[1].startswith(first1):
            return 0.85
    return 0.3


def map_party(party_code):
    return {'D': 'D', 'R': 'R', 'L': 'L', 'G': 'G'}.get(party_code, 'I')


def load_seat_map():
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
    seat_map = {}
    for s in seats:
        key = (s['office_type'], s['district_number'], s['seat_designator'])
        seat_map[key] = s['seat_id']
    return seat_map


def load_existing_candidates():
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
    by_name = {}
    for c in cands:
        if c['full_name'] not in by_name or c['id'] < by_name[c['full_name']]:
            by_name[c['full_name']] = c['id']
    return by_name


def find_candidate_match(name, existing_candidates):
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


def dedup_candidates(candidates):
    """Remove duplicate candidates (same name+party, keep one with votes)."""
    seen = {}  # (name_lower, party) -> candidate
    for c in candidates:
        key = (c['name'].lower(), c['party_code'])
        if key in seen:
            # Keep the one with more votes
            if c['votes'] > seen[key]['votes']:
                seen[key] = c
        else:
            seen[key] = c
    return list(seen.values())


def populate_primaries(chamber, year, districts, seat_map, existing_candidates, dry_run=False):
    """Populate primary elections and candidacies for one chamber/year."""
    election_date = PRIMARY_DATES[year]
    office_type = 'State House' if chamber == 'assembly' else 'State Senate'
    seats_per = 2 if chamber == 'assembly' else 1

    print(f'\n  {year} {chamber.title()} Primary: {len(districts)} districts')

    # For each district, split candidates by party and create per-party elections
    election_values = []
    election_keys = []  # (district, party_code) for mapping back

    for d in districts:
        dn = str(d['district'])
        cands = dedup_candidates(d['candidates'])

        # Split by party
        by_party = {}
        for c in cands:
            pc = c['party_code']
            if pc not in ('D', 'R'):
                continue  # Skip third-party primaries (rare/nonexistent in NJ)
            if pc not in by_party:
                by_party[pc] = []
            by_party[pc].append(c)

        # For assembly, primaries go on Seat A
        if chamber == 'assembly':
            seat_key = (office_type, dn, 'A')
        else:
            seat_key = (office_type, dn, None)

        seat_id = seat_map.get(seat_key)
        if not seat_id:
            print(f'    WARNING: No seat for {office_type} D{dn}')
            continue

        for party_code, party_cands in by_party.items():
            if not party_cands:
                continue
            total = sum(c['votes'] for c in party_cands)
            etype = f'Primary_{party_code}'
            election_values.append(
                f"({seat_id}, '{election_date}', {year}, '{etype}', NULL, NULL, NULL, NULL, "
                f"NULL, NULL, {total}, NULL, 'Certified', false, NULL, NULL)"
            )
            election_keys.append((d['district'], party_code, party_cands, total))

    print(f'    Elections: {len(election_values)}')

    if dry_run:
        election_map = {(ek[0], ek[1]): 90000 + i for i, ek in enumerate(election_keys)}
    else:
        all_ids = []
        for batch_start in range(0, len(election_values), BATCH_SIZE):
            batch = election_values[batch_start:batch_start + BATCH_SIZE]
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
                time.sleep(5)
                result = run_sql(sql)
            all_ids.extend(r['id'] for r in result)
            time.sleep(1)

        if len(all_ids) != len(election_keys):
            print(f'    ERROR: Expected {len(election_keys)} IDs, got {len(all_ids)}')
            return 0, 0, 0
        election_map = {(ek[0], ek[1]): all_ids[i] for i, ek in enumerate(election_keys)}
        print(f'    Inserted {len(all_ids)} elections')

    # Build candidacies
    all_candidacies = []
    for dist_num, party_code, party_cands, total in election_keys:
        election_id = election_map.get((dist_num, party_code))
        if not election_id:
            continue

        # Determine winners: top N by votes
        sorted_cands = sorted(party_cands, key=lambda c: -c['votes'])
        n_winners = min(seats_per, len(sorted_cands))

        for i, c in enumerate(sorted_cands):
            cand_id = find_candidate_match(c['name'], existing_candidates)
            pct = round(c['votes'] / total * 100, 1) if total > 0 else None
            result = 'Won' if i < n_winners else 'Lost'
            all_candidacies.append({
                'election_id': election_id,
                'candidate_id': cand_id,
                'candidate_name': c['name'],
                'party': map_party(c['party_code']),
                'is_incumbent': c.get('incumbent', False),
                'votes': c['votes'],
                'vote_pct': pct,
                'result': result,
            })

    new_cands = [c for c in all_candidacies if c['candidate_id'] is None]
    reuse_cands = [c for c in all_candidacies if c['candidate_id'] is not None]
    print(f'    Candidacies: {len(all_candidacies)} ({len(reuse_cands)} matched, {len(new_cands)} new)')

    if dry_run:
        for c in reuse_cands[:5]:
            db_name = next((n for n, cid in existing_candidates.items() if cid == c['candidate_id']), '?')
            print(f'      MATCH: "{c["candidate_name"]}" -> "{db_name}"')
        for c in new_cands[:5]:
            print(f'      NEW: "{c["candidate_name"]}"')
        return len(election_values), len(new_cands), len(all_candidacies)

    # Insert new candidates
    if new_cands:
        values = []
        for m in new_cands:
            parts = m['candidate_name'].split()
            first = esc(parts[0]) if parts else ''
            last = esc(parts[-1]) if len(parts) > 1 else esc(parts[0]) if parts else ''
            full = esc(m['candidate_name'])
            values.append(f"('{full}', '{first}', '{last}', NULL)")

        new_ids = []
        for batch_start in range(0, len(values), BATCH_SIZE):
            batch = values[batch_start:batch_start + BATCH_SIZE]
            sql = (
                "INSERT INTO candidates (full_name, first_name, last_name, gender) VALUES\n"
                + ",\n".join(batch)
                + "\nRETURNING id;"
            )
            result = run_sql(sql, exit_on_error=False)
            if result is None:
                time.sleep(5)
                result = run_sql(sql)
            new_ids.extend(r['id'] for r in result)
            time.sleep(1)

        for i, m in enumerate(new_cands):
            m['candidate_id'] = new_ids[i]
            existing_candidates[m['candidate_name']] = new_ids[i]
        print(f'    Inserted {len(new_ids)} new candidates')

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

    inserted = 0
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
            time.sleep(5)
            result = run_sql(sql)
        inserted += len(result)
        time.sleep(1)

    print(f'    Inserted {inserted} candidacies')
    return len(election_values), len(new_cands), inserted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    print('Loading parsed results...')
    with open(INPUT_PATH) as f:
        pdf_data = json.load(f)

    print('Loading DB data...')
    seat_map = load_seat_map()
    existing_candidates = load_existing_candidates()
    print(f'  {len(seat_map)} seats, {len(existing_candidates)} existing candidates')

    total_elections = 0
    total_new_cands = 0
    total_candidacies = 0

    for chamber, year in TO_POPULATE:
        key = f'{year}_{chamber}_primary'
        if key not in pdf_data:
            print(f'\n  SKIP: {key} not in parsed data')
            continue

        districts = pdf_data[key]
        ne, nc, nca = populate_primaries(
            chamber, year, districts, seat_map, existing_candidates, dry_run=args.dry_run
        )
        total_elections += ne
        total_new_cands += nc
        total_candidacies += nca

    print(f'\n{"=" * 60}')
    print(f'TOTAL: {total_elections} elections, {total_new_cands} new candidates, {total_candidacies} candidacies')


if __name__ == '__main__':
    main()
