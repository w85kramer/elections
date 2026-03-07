"""
Populate NJ special elections and fix data gaps found by verification.

Three tasks:
1. Add 14 missing special elections (2012/2016/2018/2020 Assembly + 2016 Senate)
2. Add missing candidacies to existing elections (64 candidates not in DB)
3. Update 2019 vote counts to match official certified results

Reads parsed data from /tmp/nj_all_new_parsed.json.

Usage:
    python3 scripts/populate_nj_specials_and_fixes.py --dry-run
    python3 scripts/populate_nj_specials_and_fixes.py
    python3 scripts/populate_nj_specials_and_fixes.py --task specials
    python3 scripts/populate_nj_specials_and_fixes.py --task missing-candidates
    python3 scripts/populate_nj_specials_and_fixes.py --task vote-fixes
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
INPUT_PATH = '/tmp/nj_all_new_parsed.json'

# Special election dates (from PDF headers)
SPECIAL_DATES = {
    (2012, 'assembly'): '2012-11-06',
    (2016, 'assembly'): '2016-11-08',
    (2016, 'senate'): '2016-11-08',
    (2018, 'assembly'): '2018-11-06',
    (2020, 'assembly'): '2020-11-03',
}

# Regular general election dates
GENERAL_DATES = {
    2013: '2013-11-05',
    2015: '2015-11-03',
    2017: '2017-11-07',
    2019: '2019-11-05',
    2021: '2021-11-02',
    2023: '2023-11-07',
    2025: '2025-11-04',
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
    # Handle initial + middle name as primary: "M. Teresa Ruiz" vs "Teresa Ruiz"
    # If one name has an initial (1-2 chars with dot), try matching the second part
    if len(first1) <= 2 and len(parts1) >= 3:
        if parts1[1] == first2 or first2.startswith(parts1[1]) or parts1[1].startswith(first2):
            return 0.85
    if len(first2) <= 2 and len(parts2) >= 3:
        if parts2[1] == first1 or first1.startswith(parts2[1]) or parts2[1].startswith(first1):
            return 0.85
    return 0.3


def map_party(party_str, party_code):
    if party_code == 'D':
        return 'D'
    if party_code == 'R':
        return 'R'
    if party_code == 'L':
        return 'L'
    if party_code == 'G':
        return 'G'
    return 'I'


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
    best_name = None
    for db_name, cand_id in existing_candidates.items():
        score = name_similarity(name, db_name)
        if score > best_score:
            best_score = score
            best_id = cand_id
            best_name = db_name
    if best_score >= 0.8:
        return best_id, best_name
    return None, None


def load_db_elections():
    """Load all NJ elections with their candidacies."""
    rows = run_sql("""
        SELECT e.id as election_id, e.election_year, e.election_type,
               d.chamber, d.district_number,
               c.id as candidate_id, c.full_name,
               ca.id as candidacy_id, ca.party, ca.votes_received, ca.result, ca.is_incumbent,
               e.total_votes_cast, se.seat_designator
        FROM elections e
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        LEFT JOIN candidacies ca ON ca.election_id = e.id
        LEFT JOIN candidates c ON ca.candidate_id = c.id
        WHERE st.abbreviation = 'NJ'
          AND d.chamber IN ('Assembly', 'Senate')
        ORDER BY e.election_year, d.chamber, d.district_number::int, ca.votes_received DESC
    """)

    # Group by (year, type, chamber, district)
    db = {}
    elections_by_key = {}  # Track election IDs
    for r in rows:
        key = (r['election_year'], r['election_type'], r['chamber'], r['district_number'])
        if key not in db:
            db[key] = []
        if r['candidacy_id']:  # Only add if there's a candidacy
            db[key].append(r)
        # Track election IDs per (year, type, chamber, district, designator)
        ekey = (r['election_year'], r['election_type'], r['chamber'],
                r['district_number'], r['seat_designator'])
        elections_by_key[ekey] = r['election_id']

    return db, elections_by_key


# ══════════════════════════════════════════════════════════════════════
# TASK 1: Populate missing special elections
# ══════════════════════════════════════════════════════════════════════

def populate_specials(pdf_data, seat_map, existing_candidates, dry_run=False):
    print(f'\n{"=" * 60}')
    print('TASK 1: POPULATE MISSING SPECIAL ELECTIONS')
    print(f'{"=" * 60}')

    # These are the special general elections stored under "general" keys
    # because the PDFs say "GENERAL ELECTION" even though they're specials
    special_keys = {
        '2012_general_assembly': ('assembly', 2012),
        '2016_general_assembly': ('assembly', 2016),
        '2016_general_senate': ('senate', 2016),
        '2018_general_assembly': ('assembly', 2018),
        '2020_general_assembly': ('assembly', 2020),
    }

    total_elections = 0
    total_candidacies = 0
    total_new_candidates = 0

    for key, (chamber, year) in special_keys.items():
        if key not in pdf_data:
            print(f'  SKIP: {key} not in parsed data')
            continue

        districts = pdf_data[key]
        date_key = (year, chamber)
        election_date = SPECIAL_DATES.get(date_key)
        if not election_date:
            print(f'  SKIP: No date for {date_key}')
            continue

        print(f'\n  {year} {chamber.title()} Special: {len(districts)} districts')

        # Create elections
        election_values = []
        election_keys = []  # Track (district, designator) for each value

        for d in districts:
            dn = str(d['district'])
            total = d['total_votes']

            if chamber == 'assembly':
                for designator in ['A', 'B']:
                    seat_key = ('State House', dn, designator)
                    seat_id = seat_map.get(seat_key)
                    if not seat_id:
                        print(f'    WARNING: No seat for Assembly D{dn} Seat {designator}')
                        continue
                    election_values.append(
                        f"({seat_id}, '{election_date}', {year}, 'Special', NULL, NULL, NULL, NULL, "
                        f"NULL, NULL, {total}, NULL, 'Certified', false, NULL, NULL)"
                    )
                    election_keys.append((d['district'], designator))
            else:  # senate
                seat_key = ('State Senate', dn, None)
                seat_id = seat_map.get(seat_key)
                if not seat_id:
                    print(f'    WARNING: No seat for Senate D{dn}')
                    continue
                election_values.append(
                    f"({seat_id}, '{election_date}', {year}, 'Special', NULL, NULL, NULL, NULL, "
                    f"NULL, NULL, {total}, NULL, 'Certified', false, NULL, NULL)"
                )
                election_keys.append((d['district'], None))

        print(f'    Elections to create: {len(election_values)}')

        if dry_run:
            election_map = {k: 90000 + i for i, k in enumerate(election_keys)}
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
                    time.sleep(3)
                    result = run_sql(sql)
                all_ids.extend(r['id'] for r in result)
                time.sleep(0.8)

            if len(all_ids) != len(election_keys):
                print(f'    ERROR: Expected {len(election_keys)} IDs, got {len(all_ids)}')
                continue
            election_map = {k: all_ids[i] for i, k in enumerate(election_keys)}
            print(f'    Inserted {len(all_ids)} elections')

        total_elections += len(election_values)

        # Create candidacies
        all_candidacies = []
        for d in districts:
            dn = str(d['district'])

            if chamber == 'assembly':
                winners = sorted([c for c in d['candidates'] if c['winner']],
                                key=lambda c: -c['votes'])
                losers = [c for c in d['candidates'] if not c['winner']]

                for idx, w in enumerate(winners):
                    designator = 'A' if idx == 0 else 'B'
                    ekey = (d['district'], designator)
                    election_id = election_map.get(ekey)
                    if not election_id:
                        continue
                    cand_id, match_name = find_candidate_match(w['name'], existing_candidates)
                    total = d['total_votes']
                    pct = round(w['votes'] / total * 100, 1) if total > 0 else None
                    all_candidacies.append({
                        'election_id': election_id,
                        'candidate_id': cand_id,
                        'candidate_name': w['name'],
                        'party': map_party(w['party'], w['party_code']),
                        'is_incumbent': w.get('incumbent', False),
                        'votes': w['votes'],
                        'vote_pct': pct,
                        'result': 'Won',
                    })

                ekey_a = (d['district'], 'A')
                election_id_a = election_map.get(ekey_a)
                if election_id_a:
                    total = d['total_votes']
                    for l in losers:
                        cand_id, match_name = find_candidate_match(l['name'], existing_candidates)
                        pct = round(l['votes'] / total * 100, 1) if total > 0 else None
                        all_candidacies.append({
                            'election_id': election_id_a,
                            'candidate_id': cand_id,
                            'candidate_name': l['name'],
                            'party': map_party(l['party'], l['party_code']),
                            'is_incumbent': l.get('incumbent', False),
                            'votes': l['votes'],
                            'vote_pct': pct,
                            'result': 'Lost',
                        })
            else:  # senate
                ekey = (d['district'], None)
                election_id = election_map.get(ekey)
                if not election_id:
                    continue
                total = d['total_votes']
                for c in d['candidates']:
                    cand_id, match_name = find_candidate_match(c['name'], existing_candidates)
                    pct = round(c['votes'] / total * 100, 1) if total > 0 else None
                    all_candidacies.append({
                        'election_id': election_id,
                        'candidate_id': cand_id,
                        'candidate_name': c['name'],
                        'party': map_party(c['party'], c['party_code']),
                        'is_incumbent': c.get('incumbent', False),
                        'votes': c['votes'],
                        'vote_pct': pct,
                        'result': 'Won' if c['winner'] else 'Lost',
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
            total_candidacies += len(all_candidacies)
            total_new_candidates += len(new_cands)
            continue

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
                    time.sleep(3)
                    result = run_sql(sql)
                new_ids.extend(r['id'] for r in result)
                time.sleep(0.8)

            for i, m in enumerate(new_cands):
                m['candidate_id'] = new_ids[i]
                # Add to existing_candidates for future matching
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
                time.sleep(3)
                result = run_sql(sql)
            inserted += len(result)
            time.sleep(0.8)

        print(f'    Inserted {inserted} candidacies')
        total_candidacies += inserted
        total_new_candidates += len(new_cands)

    print(f'\n  TOTAL: {total_elections} elections, {total_new_candidates} new candidates, {total_candidacies} candidacies')


# ══════════════════════════════════════════════════════════════════════
# TASK 2: Add missing candidates to existing elections
# ══════════════════════════════════════════════════════════════════════

def fix_missing_candidates(pdf_data, db_data, db_elections, existing_candidates, dry_run=False):
    print(f'\n{"=" * 60}')
    print('TASK 2: ADD MISSING CANDIDATES TO EXISTING ELECTIONS')
    print(f'{"=" * 60}')

    def normalize_name(n):
        n = strip_accents(n)
        n = re.sub(r'\s+(Jr\.?|Sr\.?|III|IV|II|V)\s*$', '', n, flags=re.IGNORECASE)
        n = re.sub(r'"[^"]*"', '', n)
        n = n.replace(',', '')
        n = re.sub(r'\s+', ' ', n).strip().lower()
        return n

    missing_candidacies = []

    for key in sorted(pdf_data.keys()):
        if 'primary' in key:
            continue
        parts = key.split('_', 2)
        year = int(parts[0])
        etype = parts[1]
        chamber_key = parts[2]

        if etype != 'general':
            continue

        db_chamber = 'Assembly' if chamber_key == 'assembly' else 'Senate'
        districts = pdf_data[key]

        for d in districts:
            dn = str(d['district'])
            # Look for this election in DB
            db_key = (year, 'General', db_chamber, dn)
            db_cands = db_data.get(db_key, [])
            if not db_cands:
                db_key = (year, 'Special', db_chamber, dn)
                db_cands = db_data.get(db_key, [])
            if not db_cands:
                continue  # Missing election handled by Task 1

            db_names = {normalize_name(r['full_name']): r for r in db_cands}

            for pc in d['candidates']:
                pname = normalize_name(pc['name'])
                # Try exact normalized match
                match = db_names.get(pname)
                if not match:
                    # Try name_similarity against all DB candidates in this election
                    # Use 0.7 threshold (same district/year gives strong context)
                    best_sim = 0
                    best_dr = None
                    for dname, dr in db_names.items():
                        sim = name_similarity(pc['name'], dr['full_name'])
                        if sim > best_sim:
                            best_sim = sim
                            best_dr = dr
                    if best_sim >= 0.7:
                        match = best_dr

                # Extra safety checks for common name variations
                if not match:
                    pname_parts = pname.split()
                    plast = pname_parts[-1] if pname_parts else ''
                    pfirst = pname_parts[0] if pname_parts else ''
                    for dname, dr in db_names.items():
                        dname_parts = dname.split()
                        dlast = dname_parts[-1] if dname_parts else ''
                        dfirst = dname_parts[0] if dname_parts else ''
                        # Hyphenated last name: "davis-speight" contains "speight"
                        if plast and dlast and (dlast in plast.split('-') or plast in dlast.split('-')):
                            if pfirst == dfirst or (pfirst and dfirst and pfirst[0] == dfirst[0]):
                                match = dr
                                break
                        # Exact vote match as last resort
                        if pc['votes'] is not None and dr['votes_received'] == pc['votes'] and pc['votes'] > 0:
                            match = dr
                            break

                if not match:
                    # This candidate is in PDF but not in DB — need to add
                    # Find which election to add them to
                    etype_db = db_key[1]
                    if db_chamber == 'Assembly':
                        # Add to Seat A election (losers go on Seat A)
                        ekey = (year, etype_db, db_chamber, dn, 'A')
                        election_id = db_elections.get(ekey)
                    else:
                        ekey = (year, etype_db, db_chamber, dn, None)
                        election_id = db_elections.get(ekey)

                    if not election_id:
                        print(f'  WARNING: No election ID for {ekey}')
                        continue

                    total = d['total_votes']
                    pct = round(pc['votes'] / total * 100, 1) if total > 0 else None

                    missing_candidacies.append({
                        'year': year,
                        'chamber': db_chamber,
                        'district': dn,
                        'election_id': election_id,
                        'candidate_name': pc['name'],
                        'party': map_party(pc.get('party', ''), pc.get('party_code', 'O')),
                        'is_incumbent': pc.get('incumbent', False),
                        'votes': pc['votes'],
                        'vote_pct': pct,
                        'result': 'Won' if pc['winner'] else 'Lost',
                    })

    print(f'  Missing candidacies found: {len(missing_candidacies)}')

    if not missing_candidacies:
        return

    # Match to existing candidates
    for m in missing_candidacies:
        cand_id, match_name = find_candidate_match(m['candidate_name'], existing_candidates)
        m['candidate_id'] = cand_id

    new_cands = [m for m in missing_candidacies if m['candidate_id'] is None]
    reuse_cands = [m for m in missing_candidacies if m['candidate_id'] is not None]
    print(f'  Matched to existing: {len(reuse_cands)}, New candidates: {len(new_cands)}')

    for m in missing_candidacies:
        tag = 'MATCH' if m['candidate_id'] else 'NEW'
        print(f'    {tag}: {m["year"]} {m["chamber"]} D{m["district"]}: '
              f'"{m["candidate_name"]}" ({m["party"]}) {m["votes"]:,} votes [{m["result"]}]')

    if dry_run:
        return

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
                time.sleep(3)
                result = run_sql(sql)
            new_ids.extend(r['id'] for r in result)
            time.sleep(0.8)

        for i, m in enumerate(new_cands):
            m['candidate_id'] = new_ids[i]
            existing_candidates[m['candidate_name']] = new_ids[i]
        print(f'  Inserted {len(new_ids)} new candidates')

    # Insert candidacies
    values = []
    for m in missing_candidacies:
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
            time.sleep(3)
            result = run_sql(sql)
        inserted += len(result)
        time.sleep(0.8)

    print(f'  Inserted {inserted} candidacies')


# ══════════════════════════════════════════════════════════════════════
# TASK 3: Fix vote discrepancies (update to certified totals)
# ══════════════════════════════════════════════════════════════════════

def fix_vote_discrepancies(pdf_data, db_data, dry_run=False):
    print(f'\n{"=" * 60}')
    print('TASK 3: FIX VOTE DISCREPANCIES')
    print(f'{"=" * 60}')

    def normalize_name(n):
        n = strip_accents(n)
        n = re.sub(r'\s+(Jr\.?|Sr\.?|III|IV|II|V)\s*$', '', n, flags=re.IGNORECASE)
        n = re.sub(r'"[^"]*"', '', n)
        n = n.replace(',', '')
        n = re.sub(r'\s+', ' ', n).strip().lower()
        return n

    updates = []

    for key in sorted(pdf_data.keys()):
        if 'primary' in key:
            continue
        parts = key.split('_', 2)
        year = int(parts[0])
        etype = parts[1]
        chamber_key = parts[2]

        if etype != 'general':
            continue

        db_chamber = 'Assembly' if chamber_key == 'assembly' else 'Senate'
        districts = pdf_data[key]

        for d in districts:
            dn = str(d['district'])
            db_key = (year, 'General', db_chamber, dn)
            db_cands = db_data.get(db_key, [])
            if not db_cands:
                db_key = (year, 'Special', db_chamber, dn)
                db_cands = db_data.get(db_key, [])
            if not db_cands:
                continue

            db_names = {normalize_name(r['full_name']): r for r in db_cands}

            for pc in d['candidates']:
                pname = normalize_name(pc['name'])
                match = db_names.get(pname)
                if not match:
                    best_sim = 0
                    best_dr = None
                    for dname, dr in db_names.items():
                        sim = name_similarity(pc['name'], dr['full_name'])
                        if sim > best_sim:
                            best_sim = sim
                            best_dr = dr
                    if best_sim >= 0.7:
                        match = best_dr

                if match and match['votes_received'] is not None and pc['votes'] is not None:
                    diff = abs(match['votes_received'] - pc['votes'])
                    if diff > 10:
                        total = d['total_votes']
                        new_pct = round(pc['votes'] / total * 100, 1) if total > 0 else None
                        updates.append({
                            'candidacy_id': match['candidacy_id'],
                            'election_id': match['election_id'],
                            'year': year,
                            'chamber': db_chamber,
                            'district': dn,
                            'name': pc['name'],
                            'old_votes': match['votes_received'],
                            'new_votes': pc['votes'],
                            'new_pct': new_pct,
                            'diff': diff,
                        })

                # Also check winner status
                if match:
                    pdf_won = pc['winner']
                    db_won = match['result'] == 'Won'
                    if pdf_won != db_won:
                        new_result = 'Won' if pdf_won else 'Lost'
                        updates.append({
                            'candidacy_id': match['candidacy_id'],
                            'election_id': match['election_id'],
                            'year': year,
                            'chamber': db_chamber,
                            'district': dn,
                            'name': pc['name'],
                            'old_votes': match['votes_received'],
                            'new_votes': pc['votes'] if pc['votes'] != match['votes_received'] else None,
                            'new_pct': None,
                            'diff': 0,
                            'result_change': (match['result'], new_result),
                        })

    # Deduplicate by candidacy_id (keep the one with vote change if both exist)
    by_cid = {}
    for u in updates:
        cid = u['candidacy_id']
        if cid not in by_cid:
            by_cid[cid] = u
        else:
            # Merge: prefer the one with vote diff
            existing = by_cid[cid]
            if u.get('diff', 0) > existing.get('diff', 0):
                if 'result_change' in existing:
                    u['result_change'] = existing['result_change']
                by_cid[cid] = u
            elif 'result_change' in u:
                existing['result_change'] = u['result_change']

    updates = list(by_cid.values())

    vote_updates = [u for u in updates if u.get('diff', 0) > 0]
    result_updates = [u for u in updates if 'result_change' in u]

    print(f'  Vote updates needed: {len(vote_updates)}')
    print(f'  Result (winner) fixes needed: {len(result_updates)}')

    # Show by year
    by_year = {}
    for u in vote_updates:
        by_year.setdefault(u['year'], []).append(u)
    for y in sorted(by_year.keys()):
        print(f'    {y}: {len(by_year[y])} vote updates')

    for u in result_updates:
        print(f'    RESULT: {u["year"]} {u["chamber"]} D{u["district"]}: '
              f'{u["name"]} {u["result_change"][0]} -> {u["result_change"][1]}')

    if dry_run:
        # Show sample
        for u in vote_updates[:10]:
            print(f'    {u["year"]} {u["chamber"]} D{u["district"]}: '
                  f'{u["name"]} {u["old_votes"]:,} -> {u["new_votes"]:,} (diff:{u["diff"]:,})')
        if len(vote_updates) > 10:
            print(f'    ... and {len(vote_updates) - 10} more')
        return

    # Execute updates
    updated = 0
    for u in updates:
        parts = []
        if u.get('new_votes') is not None and u.get('diff', 0) > 0:
            parts.append(f"votes_received = {u['new_votes']}")
            if u.get('new_pct') is not None:
                parts.append(f"vote_percentage = {u['new_pct']}")
        if 'result_change' in u:
            parts.append(f"result = '{u['result_change'][1]}'")

        if not parts:
            continue

        sql = f"UPDATE candidacies SET {', '.join(parts)} WHERE id = {u['candidacy_id']};"
        run_sql(sql, exit_on_error=False)
        updated += 1

        if updated % 50 == 0:
            time.sleep(1)

    # Also update total_votes_cast on elections where votes changed
    election_totals = {}
    for key in sorted(pdf_data.keys()):
        if 'primary' in key:
            continue
        parts = key.split('_', 2)
        year = int(parts[0])
        if year != 2019:  # Most discrepancies are 2019
            continue
        chamber_key = parts[2]
        db_chamber = 'Assembly' if chamber_key == 'assembly' else 'Senate'
        for d in pdf_data[key]:
            dn = str(d['district'])
            db_key = (year, 'General', db_chamber, dn)
            db_cands = db_data.get(db_key, [])
            if db_cands:
                for r in db_cands:
                    eid = r['election_id']
                    if eid not in election_totals:
                        election_totals[eid] = d['total_votes']

    for eid, total in election_totals.items():
        sql = f"UPDATE elections SET total_votes_cast = {total} WHERE id = {eid};"
        run_sql(sql, exit_on_error=False)

    print(f'  Updated {updated} candidacies')
    print(f'  Updated {len(election_totals)} election total_votes_cast')


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--task', choices=['specials', 'missing-candidates', 'vote-fixes', 'all'],
                       default='all')
    args = parser.parse_args()

    print('Loading parsed PDF results...')
    with open(INPUT_PATH) as f:
        pdf_data = json.load(f)

    print('Loading DB data...')
    seat_map = load_seat_map()
    existing_candidates = load_existing_candidates()
    print(f'  {len(seat_map)} seats, {len(existing_candidates)} existing candidates')

    if args.task in ('all', 'missing-candidates', 'vote-fixes'):
        print('Loading DB elections for comparison...')
        db_data, db_elections = load_db_elections()
        print(f'  {len(db_data)} district-elections in DB')

    if args.task in ('all', 'specials'):
        populate_specials(pdf_data, seat_map, existing_candidates, dry_run=args.dry_run)

    if args.task in ('all', 'missing-candidates'):
        fix_missing_candidates(pdf_data, db_data, db_elections, existing_candidates, dry_run=args.dry_run)

    if args.task in ('all', 'vote-fixes'):
        fix_vote_discrepancies(pdf_data, db_data, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
