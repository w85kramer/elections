#!/usr/bin/env python3
"""
Import AR Secretary of State official general election results (2014–2024).

Reads JSON files from elections/tmp/AR_{year}_General.json, compares with
existing DB data, and updates vote counts to match certified totals.

Usage:
    python3 scripts/import_ar_sos_generals.py --dry-run
    python3 scripts/import_ar_sos_generals.py --dry-run --year 2024
    python3 scripts/import_ar_sos_generals.py
"""

import sys
import os
import re
import json
import time
import argparse
import unicodedata

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF

YEARS = [2014, 2016, 2018, 2020, 2022, 2024]
TMP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tmp')


# ══════════════════════════════════════════════════════════════════════
# DB HELPERS
# ══════════════════════════════════════════════════════════════════════

def run_sql(query, max_retries=5):
    for attempt in range(max_retries):
        resp = httpx.post(
            f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120,
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f'  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR ({resp.status_code}): {resp.text[:500]}')
        sys.exit(1)
    print(f'SQL FAILED after {max_retries} retries')
    sys.exit(1)


def run_sql_batch(statements, dry_run=False):
    """Execute a list of SQL statements in batches."""
    if not statements:
        return
    if dry_run:
        return
    # Combine into single transaction
    batch_size = 30
    for i in range(0, len(statements), batch_size):
        batch = statements[i:i + batch_size]
        combined = 'BEGIN;\n' + '\n'.join(batch) + '\nCOMMIT;'
        run_sql(combined)
        if i + batch_size < len(statements):
            time.sleep(1)


# ══════════════════════════════════════════════════════════════════════
# NAME MATCHING
# ══════════════════════════════════════════════════════════════════════

def normalize_name(name):
    """Normalize a name for comparison."""
    if not name:
        return ''
    name = name.strip()
    # Strip title prefixes from SoS data
    prefixes = [
        r'State Senator', r'State Representative', r'State Rep\.?',
        r'Senator', r'Sen\.?', r'Representative', r'Rep\.?',
        r'Justice of the Peace', r'Justice', r'Judge',
        r'Mayor', r'City Council Member', r'City Director',
        r'Councilman', r'Councilwoman', r'Alderman', r'Commissioner',
        r'of the Peace',
    ]
    prefix_pat = r'^(' + '|'.join(prefixes) + r')\s+'
    for _ in range(3):
        new = re.sub(prefix_pat, '', name, flags=re.IGNORECASE)
        if new == name:
            break
        name = new
    # Standard normalization
    name = re.sub(r'\s+(Jr\.?|Sr\.?|III|II|IV)$', '', name, flags=re.IGNORECASE)
    name = re.sub(r',\s*(Jr\.?|Sr\.?|III|II|IV)$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+[A-Z]\.\s+', ' ', name)
    name = re.sub(r'^[A-Z]\.\s+', '', name)
    name = re.sub(r'"[^"]*"', '', name)
    name = re.sub(r"'[^']*'", '', name)
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    name = name.replace('\u2018', "'").replace('\u2019', "'")
    name = name.replace('\u201c', '"').replace('\u201d', '"')
    name = re.sub(r'\.', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name.lower()


NICKNAMES = {
    'william': ['bill', 'will', 'billy', 'willy'],
    'robert': ['bob', 'bobby', 'rob'],
    'richard': ['dick', 'rick', 'rich'],
    'james': ['jim', 'jimmy', 'jamie'],
    'john': ['jack', 'johnny', 'jay'],
    'joseph': ['joe', 'joey'],
    'thomas': ['tom', 'tommy'],
    'charles': ['charlie', 'chuck', 'chaz'],
    'edward': ['ed', 'eddie', 'ted', 'teddy'],
    'michael': ['mike', 'mikey', 'doc'],
    'daniel': ['dan', 'danny'],
    'david': ['dave'],
    'stephen': ['steve', 'steven'],
    'steven': ['steve', 'stephen'],
    'christopher': ['chris'],
    'matthew': ['matt'],
    'anthony': ['tony'],
    'donald': ['don', 'donnie'],
    'timothy': ['tim', 'timmy'],
    'patrick': ['pat', 'paddy'],
    'elizabeth': ['liz', 'beth', 'betty', 'eliza'],
    'katherine': ['kate', 'kathy', 'katie', 'cathy'],
    'catherine': ['kate', 'kathy', 'katie', 'cathy'],
    'margaret': ['maggie', 'meg', 'peggy', 'marge'],
    'jennifer': ['jen', 'jenny'],
    'patricia': ['pat', 'patty', 'trish'],
    'deborah': ['deb', 'debbie', 'debby'],
    'pamela': ['pam'],
    'samantha': ['sam'],
    'samuel': ['sam', 'sammy'],
    'kenneth': ['ken', 'kenny'],
    'lawrence': ['larry'],
    'gerald': ['gerry', 'jerry'],
    'raymond': ['ray'],
    'andrew': ['andy', 'drew'],
    'benjamin': ['ben'],
    'gregory': ['greg'],
    'frederick': ['fred', 'freddy'],
    'ronald': ['ron', 'ronnie'],
    'alexander': ['alex'],
}

_NICKNAME_GROUPS = {}
for _formal, _nicks in NICKNAMES.items():
    _group = frozenset([_formal] + _nicks)
    _NICKNAME_GROUPS[_formal] = _group
    for _n in _nicks:
        _NICKNAME_GROUPS[_n] = _group


def nicknames_match(name1, name2):
    if name1 == name2:
        return True
    g1 = _NICKNAME_GROUPS.get(name1)
    if g1 and name2 in g1:
        return True
    g2 = _NICKNAME_GROUPS.get(name2)
    if g2 and name1 in g2:
        return True
    return False


def names_match(name1, name2):
    """Check if two names refer to the same person."""
    if not name1 or not name2:
        return False
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    if n1 == n2:
        return True

    parts1 = n1.split()
    parts2 = n2.split()
    if not parts1 or not parts2:
        return False

    # Last name matching
    last1 = parts1[-1]
    last2 = parts2[-1]
    lname_match = (last1 == last2)
    if not lname_match:
        full1 = ' '.join(parts1[1:]) if len(parts1) > 1 else last1
        full2 = ' '.join(parts2[1:]) if len(parts2) > 1 else last2
        if full1.startswith(full2) or full2.startswith(full1):
            lname_match = True
        elif last1 in full2 or last2 in full1:
            lname_match = True
    if not lname_match:
        return False

    # First name matching
    first1 = parts1[0]
    first2 = parts2[0]
    if first1 == first2:
        return True
    if nicknames_match(first1, first2):
        return True
    if len(first1) <= 2 and first2.startswith(first1.rstrip('.')):
        return True
    if len(first2) <= 2 and first1.startswith(first2.rstrip('.')):
        return True
    return False


# ══════════════════════════════════════════════════════════════════════
# SOS JSON PARSING
# ══════════════════════════════════════════════════════════════════════

def parse_contest_name(contest_name):
    """Parse a SoS contest name into (chamber, district_number, election_type).

    Returns None for non-legislative or unparseable contests.
    """
    cn = contest_name.strip()

    # Skip aggregated unopposed entries (2016 "Unopposed State Rep", 2020 "UNOPPOSED ...")
    if cn.upper().startswith('UNOPPOSED'):
        return None

    # Special elections: "Special Election for State Representative District 9"
    election_type = 'General'
    if cn.startswith('Special Election for '):
        election_type = 'Special'
        cn = cn.replace('Special Election for ', '')

    # Standard patterns:
    #   "State Senate District NN"
    #   "State Representative District NN"
    #   "State Rep District NN"  (2020 variant)
    m = re.match(r'State Senate District\s+(\d+)', cn)
    if m:
        return ('State Senate', int(m.group(1)), election_type)

    m = re.match(r'(?:State Representative|State Rep) District\s+(\d+)', cn)
    if m:
        return ('State House', int(m.group(1)), election_type)

    return None


def parse_sos_party(party_name):
    """Map SoS party name to DB code."""
    if party_name == 'Republican Party':
        return 'R'
    if party_name == 'Democratic Party':
        return 'D'
    if party_name == 'Libertarian Party':
        return 'L'
    if party_name == 'Green Party':
        return 'G'
    # Empty string = independent/unknown — return None to signal "don't overwrite"
    return None


def clean_candidate_name(name):
    """Strip title prefixes and fix mangled quotes from SoS candidate names."""
    name = name.strip()
    # SoS data sometimes has mangled quotes like: Charles Sonny" Carter"
    # Remove all double quotes — nicknames aren't useful for display names
    name = name.replace('"', '')
    # Strip various title prefixes the SoS data uses (applied iteratively for
    # compound titles like "Justice of the Peace")
    prefixes = [
        r'State Senator', r'State Representative', r'State Rep\.?',
        r'Senator', r'Sen\.?', r'Representative', r'Rep\.?',
        r'Justice of the Peace', r'Justice', r'Judge',
        r'Mayor', r'City Council Member', r'City Director',
        r'Councilman', r'Councilwoman', r'Alderman', r'Commissioner',
        r'of the Peace',  # catches leftover after "Justice" strip
    ]
    pattern = r'^(' + '|'.join(prefixes) + r')\s+'
    # Apply up to 3 times for compound titles
    for _ in range(3):
        new = re.sub(pattern, '', name, flags=re.IGNORECASE)
        if new == name:
            break
        name = new
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def load_sos_data(year):
    """Load and parse SoS JSON for a given year.

    Returns list of dicts:
      { chamber, district_number, election_type, total_votes,
        candidates: [{name, party, votes}] }
    """
    filepath = os.path.join(TMP_DIR, f'AR_{year}_General.json')
    if not os.path.exists(filepath):
        print(f'  WARNING: File not found: {filepath}')
        return []

    with open(filepath) as f:
        data = json.load(f)

    contests = data.get('ContestData', [])
    # Deduplicate: some years have duplicate contest entries for the same race
    # (e.g., "State Rep District 61" and "State Representative District 61").
    # Keep the entry with the higher total votes (the more complete report).
    raw = {}

    for contest in contests:
        parsed = parse_contest_name(contest['ContestName'])
        if not parsed:
            continue

        chamber, dist_num, election_type = parsed

        # Skip unopposed entries where candidates are just "FOR" / "FOR ALL"
        cands = contest.get('Candidates', [])
        if all(c['Name'] in ('FOR', 'FOR ALL') for c in cands):
            continue

        candidates = []
        for c in cands:
            name = clean_candidate_name(c['Name'])
            if name in ('Write-In', 'Write-in', 'WRITE-IN', 'Over Votes', 'Under Votes'):
                continue
            candidates.append({
                'name': name,
                'party': parse_sos_party(c.get('PartyName', '')),
                'votes': c['TotalVotes'],
            })

        if not candidates:
            continue

        entry = {
            'chamber': chamber,
            'district_number': dist_num,
            'election_type': election_type,
            'total_votes': contest['TotalVotes'],
            'candidates': candidates,
        }

        key = (chamber, dist_num, election_type)
        if key in raw:
            # Keep the entry with more total votes
            if contest['TotalVotes'] > raw[key]['total_votes']:
                raw[key] = entry
        else:
            raw[key] = entry

    return list(raw.values())


# ══════════════════════════════════════════════════════════════════════
# DB STATE LOADING
# ══════════════════════════════════════════════════════════════════════

def load_db_state():
    """Load all AR seats, elections, candidacies, and candidates from DB."""
    print('Loading DB state...')

    # Load AR state
    rows = run_sql("SELECT id FROM states WHERE abbreviation = 'AR'")
    state_id = rows[0]['id']

    # Load seats indexed by (office_type, district_number)
    seats = run_sql(f"""
        SELECT s.id, s.office_type, d.district_number
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        WHERE d.state_id = {state_id}
          AND s.office_type IN ('State Senate', 'State House')
    """)
    seat_map = {}
    for s in seats:
        key = (s['office_type'], str(s['district_number']))
        seat_map[key] = s['id']
    print(f'  {len(seat_map)} seats loaded')

    # Load all AR general elections (2014-2024)
    elections = run_sql(f"""
        SELECT e.id, e.seat_id, e.election_year, e.election_type, e.total_votes_cast,
               e.result_status
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        WHERE d.state_id = {state_id}
          AND e.election_year IN (2014, 2016, 2018, 2020, 2022, 2024)
          AND e.election_type IN ('General', 'Special')
    """)
    # Index by (seat_id, year, election_type)
    election_map = {}
    for e in elections:
        key = (e['seat_id'], e['election_year'], e['election_type'])
        election_map[key] = e
    print(f'  {len(election_map)} elections loaded')

    # Load all candidacies for those elections
    election_ids = [str(e['id']) for e in elections]
    candidacies = []
    # Batch to avoid query size limits
    batch_size = 200
    for i in range(0, len(election_ids), batch_size):
        batch = election_ids[i:i + batch_size]
        ids_str = ','.join(batch)
        rows = run_sql(f"""
            SELECT c.id, c.election_id, c.candidate_id, c.party, c.votes_received,
                   c.vote_percentage, c.result,
                   cand.first_name, cand.last_name, cand.full_name
            FROM candidacies c
            JOIN candidates cand ON c.candidate_id = cand.id
            WHERE c.election_id IN ({ids_str})
        """)
        candidacies.extend(rows)
        if i + batch_size < len(election_ids):
            time.sleep(0.5)

    # Index candidacies by election_id
    candidacy_map = {}
    for c in candidacies:
        eid = c['election_id']
        if eid not in candidacy_map:
            candidacy_map[eid] = []
        candidacy_map[eid].append(c)
    print(f'  {len(candidacies)} candidacies loaded')

    return state_id, seat_map, election_map, candidacy_map


# ══════════════════════════════════════════════════════════════════════
# MATCHING AND DIFFING
# ══════════════════════════════════════════════════════════════════════

def sql_escape(val):
    """Escape a string for SQL single-quote context."""
    if val is None:
        return 'NULL'
    return "'" + str(val).replace("'", "''") + "'"


def process_year(year, sos_contests, seat_map, election_map, candidacy_map, dry_run):
    """Process one year of SoS data. Returns stats dict."""
    stats = {
        'elections_updated': 0,
        'candidacies_updated': 0,
        'new_candidacies': 0,
        'unmatched_contests': 0,
        'unmatched_candidates': [],
    }

    update_stmts = []
    insert_stmts = []

    for contest in sos_contests:
        chamber = contest['chamber']
        dist_num = contest['district_number']
        election_type = contest['election_type']

        # Find the seat (SoS chamber maps to office_type)
        seat_key = (chamber, str(dist_num))
        seat_id = seat_map.get(seat_key)
        if not seat_id:
            print(f'  WARNING: No seat for {chamber} District {dist_num}')
            stats['unmatched_contests'] += 1
            continue

        # Find the election
        elec_key = (seat_id, year, election_type)
        election = election_map.get(elec_key)
        if not election:
            print(f'  WARNING: No {election_type} election for {chamber} District {dist_num} in {year}')
            stats['unmatched_contests'] += 1
            continue

        election_id = election['id']
        db_candidacies = candidacy_map.get(election_id, [])

        # --- Update election total_votes_cast and result_status ---
        sos_total = contest['total_votes']
        changes = []
        if election['total_votes_cast'] != sos_total:
            changes.append(f'total_votes_cast = {sos_total}')
        if election['result_status'] != 'Certified':
            changes.append("result_status = 'Certified'")
        if changes:
            stmt = f"UPDATE elections SET {', '.join(changes)} WHERE id = {election_id};"
            update_stmts.append(stmt)
            stats['elections_updated'] += 1
            if dry_run:
                old_votes = election['total_votes_cast']
                old_status = election['result_status']
                print(f'  {chamber} D{dist_num}: votes {old_votes} → {sos_total}, status → Certified')

        # --- Match and update candidacies ---
        for sos_cand in contest['candidates']:
            sos_name = sos_cand['name']
            sos_votes = sos_cand['votes']
            sos_party = sos_cand['party']

            # Try to match against DB candidacies
            matched = None
            for db_c in db_candidacies:
                db_full = db_c['full_name'] or f"{db_c['first_name']} {db_c['last_name']}"
                if names_match(sos_name, db_full):
                    matched = db_c
                    break

            if matched:
                # Update votes and percentage
                cand_changes = []
                if matched['votes_received'] != sos_votes:
                    cand_changes.append(f'votes_received = {sos_votes}')
                # Recompute percentage from official total
                if sos_total > 0:
                    pct = round(sos_votes / sos_total * 100, 1)
                else:
                    pct = 0.0
                # DB returns vote_percentage as string (DECIMAL); convert for comparison
                db_pct = float(matched['vote_percentage']) if matched['vote_percentage'] is not None else None
                if db_pct != pct:
                    cand_changes.append(f'vote_percentage = {pct}')
                # Fill party if DB is NULL and SoS has value
                if sos_party and not matched['party']:
                    cand_changes.append(f"party = {sql_escape(sos_party)}")

                if cand_changes:
                    stmt = f"UPDATE candidacies SET {', '.join(cand_changes)} WHERE id = {matched['id']};"
                    update_stmts.append(stmt)
                    stats['candidacies_updated'] += 1
                    if dry_run:
                        db_name = matched['full_name'] or f"{matched['first_name']} {matched['last_name']}"
                        print(f'    {db_name}: {", ".join(cand_changes)}')
            else:
                # New candidate not in DB — create candidate + candidacy
                stats['new_candidacies'] += 1
                name_parts = sos_name.split()
                first_name = name_parts[0] if name_parts else sos_name
                last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''
                full_name = sos_name

                if sos_total > 0:
                    pct = round(sos_votes / sos_total * 100, 1)
                else:
                    pct = 0.0

                party_val = sql_escape(sos_party) if sos_party else "'I'"

                # Use CTE to insert candidate and candidacy atomically
                insert_sql = f"""
                    WITH new_cand AS (
                        INSERT INTO candidates (first_name, last_name, full_name)
                        VALUES ({sql_escape(first_name)}, {sql_escape(last_name)}, {sql_escape(full_name)})
                        RETURNING id
                    )
                    INSERT INTO candidacies (election_id, candidate_id, party, votes_received, vote_percentage, result)
                    SELECT {election_id}, new_cand.id, {party_val}, {sos_votes}, {pct}, 'Lost'
                    FROM new_cand;
                """
                insert_stmts.append(insert_sql)

                stats['unmatched_candidates'].append(
                    f'{year} {chamber} D{dist_num}: {sos_name} ({sos_votes} votes) — NEW'
                )
                if dry_run:
                    print(f'    NEW: {sos_name} ({sos_party or "?"}) {sos_votes} votes')

    # Execute batched updates
    if not dry_run:
        if update_stmts:
            print(f'  Executing {len(update_stmts)} updates...')
            run_sql_batch(update_stmts)
        if insert_stmts:
            print(f'  Executing {len(insert_stmts)} inserts...')
            # Inserts with CTEs need individual execution
            for stmt in insert_stmts:
                run_sql(stmt)
                time.sleep(0.3)

    return stats


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Import AR SoS general election results')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without writing')
    parser.add_argument('--year', type=int, help='Process only this year')
    args = parser.parse_args()

    years = [args.year] if args.year else YEARS

    state_id, seat_map, election_map, candidacy_map = load_db_state()

    total_stats = {
        'elections_updated': 0,
        'candidacies_updated': 0,
        'new_candidacies': 0,
        'unmatched_contests': 0,
        'unmatched_candidates': [],
    }

    for year in years:
        print(f'\n{"="*60}')
        print(f'Processing {year}...')
        print(f'{"="*60}')

        sos_contests = load_sos_data(year)
        if not sos_contests:
            print(f'  No SoS data for {year}')
            continue

        print(f'  {len(sos_contests)} SoS contests loaded')

        stats = process_year(year, sos_contests, seat_map, election_map, candidacy_map, args.dry_run)

        for key in total_stats:
            if isinstance(total_stats[key], list):
                total_stats[key].extend(stats[key])
            else:
                total_stats[key] += stats[key]

        print(f'\n  {year} summary:')
        print(f'    Elections updated:    {stats["elections_updated"]}')
        print(f'    Candidacies updated:  {stats["candidacies_updated"]}')
        print(f'    New candidacies:      {stats["new_candidacies"]}')
        print(f'    Unmatched contests:   {stats["unmatched_contests"]}')

    print(f'\n{"="*60}')
    print(f'TOTAL SUMMARY')
    print(f'{"="*60}')
    print(f'  Elections updated:    {total_stats["elections_updated"]}')
    print(f'  Candidacies updated:  {total_stats["candidacies_updated"]}')
    print(f'  New candidacies:      {total_stats["new_candidacies"]}')
    print(f'  Unmatched contests:   {total_stats["unmatched_contests"]}')

    if total_stats['unmatched_candidates']:
        print(f'\n  New candidates added:')
        for entry in total_stats['unmatched_candidates']:
            print(f'    {entry}')

    if args.dry_run:
        print(f'\n  DRY RUN — no changes written')


if __name__ == '__main__':
    main()
