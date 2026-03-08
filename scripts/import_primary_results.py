#!/usr/bin/env python3
"""
Import official primary election results from state SoS websites.

Downloads results directly from official state election results APIs/pages,
matches candidates and elections in the database, and updates vote counts
and result statuses.

This script is designed as the ELECTION NIGHT PROCESS — use primaries as
dry runs to dial in the matching logic before November general elections.

=== SUPPORTED STATES ===

NC (North Carolina):
  Source: NC State Board of Elections (er.ncsbe.gov)
  API: JSON data at er.ncsbe.gov/enr/{YYYYMMDD}/data/results_0.txt
  Format: Clean JSON with candidate names, votes, percentages
  Covers: State House, State Senate (and statewide via ogl=COS)

TX (Texas):
  Source: TX Secretary of State (electionresults.sos.state.tx.us)
  Status: TODO — site requires browser access, may need manual CSV download
  Notes: Separate pages for D and R primaries

AR (Arkansas):
  Source: AR Secretary of State via Clarity Elections
  Status: TODO — Clarity Elections blocks server requests (403)
  Notes: May need browser export or Clarity API key

=== USAGE ===

  # Download + preview what would change (no DB writes)
  python3 scripts/import_primary_results.py --state NC --dry-run

  # Import NC results into database
  python3 scripts/import_primary_results.py --state NC

  # Specify election date (default: auto-detect latest)
  python3 scripts/import_primary_results.py --state NC --date 2026-03-03

  # Import only senate or house
  python3 scripts/import_primary_results.py --state NC --chamber senate

=== ELECTION NIGHT CHECKLIST ===

1. Verify the SoS results URL is live and returning data
2. Run with --dry-run first to check candidate matching
3. Review any UNMATCHED candidates — may need manual name fixes
4. Run without --dry-run to update the database
5. Re-export affected states:
     python3 scripts/export_site_data.py --state XX
     python3 scripts/export_district_data.py --state XX
6. Commit and push to deploy to GitHub Pages
"""

import sys
import os
import re
import json
import time
import base64
import argparse
import unicodedata

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF

TMP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tmp')


# ══════════════════════════════════════════════════════════════════════
# DB HELPERS
# ══════════════════════════════════════════════════════════════════════

def run_sql(query, max_retries=5):
    """Execute SQL via Supabase Management API with retry logic."""
    for attempt in range(max_retries):
        resp = httpx.post(
            f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
            headers={
                'Authorization': f'Bearer {TOKEN}',
                'Content-Type': 'application/json',
            },
            json={'query': query},
            timeout=30.0,
        )
        if resp.status_code in (200, 201):
            return resp.json()
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f'    Rate limited, waiting {wait}s...', flush=True)
            time.sleep(wait)
            continue
        print(f'    SQL error ({resp.status_code}): {resp.text[:200]}', flush=True)
        if attempt < max_retries - 1:
            time.sleep(2)
    return None


def normalize_name(name):
    """Normalize a candidate name for fuzzy matching."""
    if not name:
        return ''
    # Remove accents
    name = unicodedata.normalize('NFKD', name)
    name = ''.join(c for c in name if not unicodedata.combining(c))
    # Lowercase
    name = name.lower().strip()
    # Remove trailing annotations like "- DECEASED", "- WITHDRAWN"
    name = re.sub(r'\s*-\s*(deceased|withdrawn|disqualified).*$', '', name, flags=re.I)
    # Remove suffixes
    name = re.sub(r',?\s+(jr\.?|sr\.?|ii|iii|iv)$', '', name, flags=re.I)
    # Remove middle initials (single letter followed by period)
    name = re.sub(r'\s+[a-z]\.', '', name)
    # Remove parenthetical nicknames but capture them
    name = re.sub(r'\s*\([^)]+\)\s*', ' ', name)
    # Remove quotes around nicknames
    name = re.sub(r'["\']', '', name)
    # Collapse whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    # Remove non-alpha except spaces and hyphens
    name = re.sub(r'[^a-z \-]', '', name)
    # Normalize hyphens: "leo - wilson" → "leo-wilson", then "leo-wilson" → "leo wilson"
    name = re.sub(r'\s*-\s*', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def extract_nicknames(name):
    """Extract nickname(s) from parentheses in a name."""
    nicknames = re.findall(r'\(([^)]+)\)', name)
    return [n.lower().strip() for n in nicknames]


# Common nickname mappings
NICKNAMES = {
    'edward': ['ed', 'eddie'], 'william': ['bill', 'will', 'billy', 'willy'],
    'james': ['jim', 'jimmy', 'jamie'], 'robert': ['bob', 'bobby', 'rob'],
    'richard': ['rick', 'dick', 'rich'], 'michael': ['mike', 'mikey'],
    'joseph': ['joe', 'joey'], 'thomas': ['tom', 'tommy'],
    'charles': ['charlie', 'chuck'], 'christopher': ['chris'],
    'daniel': ['dan', 'danny'], 'matthew': ['matt'],
    'timothy': ['tim', 'timmy'], 'stephen': ['steve'],
    'steven': ['steve'], 'kenneth': ['ken', 'kenny'],
    'ronald': ['ron', 'ronnie'], 'donald': ['don', 'donnie'],
    'benjamin': ['ben'], 'frederick': ['fred', 'freddy'],
    'gerald': ['jerry'], 'harold': ['hal', 'harry'],
    'lawrence': ['larry'], 'raymond': ['ray'],
    'patrick': ['pat'], 'samuel': ['sam'],
    'katherine': ['kathy', 'kate', 'katie'], 'elizabeth': ['liz', 'beth'],
    'margaret': ['maggie', 'peggy'], 'patricia': ['pat', 'patty'],
    'jennifer': ['jenny', 'jen'], 'deborah': ['deb', 'debbie'],
    'catherine': ['cathy', 'cat'], 'nancy': ['nan'],
    'andrew': ['andy', 'drew'], 'anthony': ['tony'],
    'eugene': ['gene'], 'phillip': ['phil'],
    'alexander': ['alex'], 'nicholas': ['nick'],
    'jonathan': ['jon'], 'nathaniel': ['nate', 'nathan'],
    'theodore': ['ted', 'teddy'], 'walter': ['walt'],
    'wesley': ['wes'], 'douglas': ['doug'],
    'franklin': ['frank'], 'francis': ['frank'],
    'albert': ['al'], 'clifford': ['cliff'],
    'leonard': ['len', 'lenny'], 'terrence': ['terry'],
    'randall': ['randy'], 'mitchell': ['mitch'],
    'reginald': ['reggie'], 'sylvester': ['sly'],
}


def first_names_match(name1, name2):
    """Check if two first names match, including nickname lookup."""
    if name1 == name2:
        return True
    if name1[:3] == name2[:3] and len(name1) >= 3 and len(name2) >= 3:
        return True
    # Check nickname table both directions
    for formal, nicks in NICKNAMES.items():
        all_names = [formal] + nicks
        if name1 in all_names and name2 in all_names:
            return True
    return False


def names_match(sos_name, db_name):
    """Check if two candidate names match (fuzzy)."""
    # Extract nicknames from parentheses before normalizing
    sos_nicks = extract_nicknames(sos_name)
    db_nicks = extract_nicknames(db_name)

    n1 = normalize_name(sos_name)
    n2 = normalize_name(db_name)
    if not n1 or not n2:
        return False
    # Exact match after normalization
    if n1 == n2:
        return True

    parts1 = n1.split()
    parts2 = n2.split()
    if len(parts1) < 2 or len(parts2) < 2:
        return False

    last1, last2 = parts1[-1], parts2[-1]
    first1, first2 = parts1[0], parts2[0]

    # Must have same last name
    if last1 != last2:
        return False

    # Check first names match (with nickname support)
    if first_names_match(first1, first2):
        return True

    # Check if a nickname from parentheses matches the other's first name
    for nick in sos_nicks:
        if first_names_match(nick, first2):
            return True
    for nick in db_nicks:
        if first_names_match(nick, first1):
            return True

    # Check if DB first name matches any SoS nickname
    if first2 in sos_nicks or first1 in db_nicks:
        return True

    # Check if one name's first name matches the other's middle name
    # e.g., "Reece Pyrtle" vs "A. Reece Pyrtle" (middle name used as go-by)
    if len(parts1) >= 2 and len(parts2) >= 3:
        if first_names_match(first1, parts2[1]):
            return True
    if len(parts2) >= 2 and len(parts1) >= 3:
        if first_names_match(first2, parts1[1]):
            return True

    return False


# ══════════════════════════════════════════════════════════════════════
# NC (NORTH CAROLINA) — er.ncsbe.gov
# ══════════════════════════════════════════════════════════════════════

NC_API_BASE = 'https://er.ncsbe.gov/enr'

# Map SoS office group labels to our DB chamber names
NC_OFFICE_MAP = {
    'NCH': 'State House',
    'NCS': 'State Senate',
}


def nc_download(election_date):
    """Download NC SBE results JSON. Returns list of result dicts."""
    date_str = election_date.replace('-', '')  # YYYYMMDD
    url = f'{NC_API_BASE}/{date_str}/data/results_0.txt'
    print(f'  Fetching {url}...', flush=True)

    resp = httpx.get(url, timeout=30.0)
    if resp.status_code != 200:
        print(f'  ERROR: HTTP {resp.status_code}', flush=True)
        return None

    data = resp.json()
    outpath = os.path.join(TMP_DIR, f'NC_{date_str}_primary_results.json')
    os.makedirs(TMP_DIR, exist_ok=True)
    with open(outpath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f'  Saved {len(data)} entries to {outpath}', flush=True)
    return data


def nc_parse_results(data, chamber_filter=None):
    """
    Parse NC SBE results into standardized contest dicts.

    Returns list of:
    {
        'state': 'NC',
        'chamber': 'State House' or 'State Senate',
        'district': 7,
        'party': 'D' or 'R',
        'candidates': [
            {'name': '...', 'party': 'D', 'votes': 12345, 'pct': 0.567},
            ...
        ]
    }
    """
    contests = {}

    for r in data:
        ogl = r.get('ogl', '')
        if ogl not in NC_OFFICE_MAP:
            continue

        chamber = NC_OFFICE_MAP[ogl]
        if chamber_filter and chamber_filter.lower() not in chamber.lower():
            continue

        cnm = r['cnm']
        # Parse: "NC HOUSE OF REPRESENTATIVES DISTRICT 006 - DEM (VOTE FOR 1)"
        # or:    "NC STATE SENATE DISTRICT 01 - REP (VOTE FOR 1)"
        m = re.match(r'NC (?:HOUSE OF REPRESENTATIVES|STATE SENATE) DISTRICT (\d+) - (DEM|REP)', cnm)
        if not m:
            continue

        district = int(m.group(1))
        party_label = m.group(2)
        party = 'D' if party_label == 'DEM' else 'R'

        key = (chamber, district, party)
        if key not in contests:
            contests[key] = {
                'state': 'NC',
                'chamber': chamber,
                'district': district,
                'party': party,
                'candidates': [],
            }

        # Skip deceased candidates with 0 votes
        name = r['bnm'].strip()
        votes = int(r['vct'])

        contests[key]['candidates'].append({
            'name': name,
            'party': party,
            'votes': votes,
            'pct': float(r['pct']),
        })

    # Sort candidates within each contest by votes (descending)
    for contest in contests.values():
        contest['candidates'].sort(key=lambda c: c['votes'], reverse=True)

    return list(contests.values())


# ══════════════════════════════════════════════════════════════════════
# AR (ARKANSAS) — TotalResults ENR by KnowInk
# ══════════════════════════════════════════════════════════════════════

AR_API_BASE = 'https://enr-results-api.totalresults.com'
AR_CLIENT_ID = 'arkansas'

# 2026 election IDs (discovered March 4, 2026)
AR_ELECTION_IDS = {
    '2026-03-03': '7f77a178-af02-40ec-92db-c5cc50882c68',  # 2026 Preferential Primary
}


def ar_download(election_date):
    """Download AR TotalResults data. Returns (search_list, results) tuple."""
    election_id = AR_ELECTION_IDS.get(election_date)
    if not election_id:
        # Try to find election ID from the list
        print(f'  No cached election ID for {election_date}, fetching list...', flush=True)
        resp = httpx.get(
            f'{AR_API_BASE}/Election/GetElectionList?cId={AR_CLIENT_ID}',
            timeout=30.0,
        )
        if resp.status_code != 200:
            print(f'  ERROR: HTTP {resp.status_code}', flush=True)
            return None
        elections = resp.json()
        for e in elections:
            if election_date.replace('-', '') in str(e.get('electionDate', '')):
                election_id = e['electionID']
                print(f'  Found election ID: {election_id}', flush=True)
                break
        if not election_id:
            print(f'  ERROR: No election found for date {election_date}', flush=True)
            return None

    os.makedirs(TMP_DIR, exist_ok=True)

    # Get contest search list (has candidate names)
    print(f'  Fetching contest search list...', flush=True)
    resp = httpx.get(
        f'{AR_API_BASE}/Contest/GetContestSearchList?cId={AR_CLIENT_ID}&electionID={election_id}',
        timeout=30.0,
    )
    if resp.status_code != 200:
        print(f'  ERROR: Search list HTTP {resp.status_code}', flush=True)
        return None
    search_data = resp.json()

    # Get results for state legislative races
    all_results = {}
    for contest_type in ['State Senate', 'State Representative']:
        print(f'  Fetching {contest_type} results...', flush=True)
        resp = httpx.get(
            f'{AR_API_BASE}/Contest/GetContestResults?cId={AR_CLIENT_ID}'
            f'&electionID={election_id}&contestType={contest_type}',
            timeout=30.0,
        )
        if resp.status_code == 200:
            all_results[contest_type] = resp.json()

    # Save raw data
    outpath = os.path.join(TMP_DIR, f'AR_{election_date.replace("-","")}_primary_results.json')
    with open(outpath, 'w') as f:
        json.dump({'search': search_data, 'results': all_results}, f, indent=2)
    print(f'  Saved to {outpath}', flush=True)

    return {'search': search_data, 'results': all_results}


def ar_parse_results(data, chamber_filter=None):
    """Parse AR TotalResults data into standardized contest dicts."""
    contests_out = []
    search = data['search']
    results = data['results']

    # Build contest index from search list
    contest_info = {}
    if 'response' in search and 'contests' in search['response']:
        contest_info = search['response']['contests']

    for contest_type, rdata in results.items():
        chamber = 'State House' if 'Representative' in contest_type else 'State Senate'
        if chamber_filter and chamber_filter.lower() not in chamber.lower():
            continue

        if 'response' not in rdata or 'contests' not in rdata['response']:
            continue

        for cid, race in rdata['response']['contests'].items():
            # Get contest info from search list
            ci = contest_info.get(cid, {})
            contest_name = ci.get('contestName', '')

            # Parse district number from name like "REP State Representative Dist. 05"
            # or "DEM State Senate Dist. 12"
            m = re.match(r'(REP|DEM)\s+State (?:Representative|Senate)\s+Dist\.\s*(\d+)', contest_name)
            if not m:
                continue

            party_label = m.group(1)
            district = int(m.group(2))
            party = 'D' if party_label == 'DEM' else 'R'

            # Cross-reference choices: results are in same order as search list
            search_choices = list(ci.get('choices', {}).values())
            result_choices = race.get('choices', [])

            candidates = []
            for i, rc in enumerate(result_choices):
                if i < len(search_choices):
                    name = search_choices[i].get('name', f'Unknown-{i}')
                else:
                    name = f'Unknown-{i}'

                votes = rc.get('totalVotes', 0)
                pct = rc.get('votePercent', 0) / 100  # AR returns as percentage, normalize to decimal

                candidates.append({
                    'name': name,
                    'party': party,
                    'votes': votes,
                    'pct': pct,
                })

            # Sort by votes descending
            candidates.sort(key=lambda c: c['votes'], reverse=True)

            if len(candidates) > 1:  # Only contested races
                contests_out.append({
                    'state': 'AR',
                    'chamber': chamber,
                    'district': district,
                    'party': party,
                    'candidates': candidates,
                })

    return contests_out


# ══════════════════════════════════════════════════════════════════════
# TX (TEXAS) — Civix GoElect
# ══════════════════════════════════════════════════════════════════════

TX_API_BASE = 'https://goelect.txelections.civixapps.com/api-ivis-system/api/s3'

# 2026 election IDs (discovered March 4, 2026)
TX_ELECTION_IDS = {
    '2026-03-03-R': '53813',  # 2026 Republican Primary
    '2026-03-03-D': '53814',  # 2026 Democratic Primary
}


def tx_download(election_date):
    """Download TX Civix GoElect data. Returns dict with R and D results."""
    os.makedirs(TMP_DIR, exist_ok=True)
    all_data = {}

    for party_label, party_code in [('R', 'R'), ('D', 'D')]:
        eid_key = f'{election_date}-{party_code}'
        election_id = TX_ELECTION_IDS.get(eid_key)
        if not election_id:
            print(f'  No election ID for {eid_key}, skipping...', flush=True)
            continue

        print(f'  Fetching TX {party_code} Primary (election {election_id})...', flush=True)
        resp = httpx.get(
            f'{TX_API_BASE}/enr/election/{election_id}',
            headers={
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                              '(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            },
            timeout=60.0,
        )
        if resp.status_code != 200:
            print(f'  ERROR: HTTP {resp.status_code}', flush=True)
            continue

        raw = resp.json()

        # Decode base64-encoded sections
        decoded = {}
        for section in ['Home', 'Districted', 'Federal', 'StateWide']:
            if section in raw and raw[section]:
                try:
                    decoded[section] = json.loads(base64.b64decode(raw[section]))
                except Exception as e:
                    print(f'  Warning: Could not decode {section}: {e}', flush=True)

        all_data[party_code] = decoded

    # Save raw data
    outpath = os.path.join(TMP_DIR, f'TX_{election_date.replace("-","")}_primary_results.json')
    with open(outpath, 'w') as f:
        json.dump(all_data, f, indent=2)
    print(f'  Saved to {outpath}', flush=True)

    return all_data


def tx_parse_results(data, chamber_filter=None):
    """Parse TX Civix GoElect data into standardized contest dicts."""
    contests = []

    for party_code, sections in data.items():
        party = party_code  # 'R' or 'D'
        districted = sections.get('Districted', {})
        races = districted.get('Races', [])

        for race in races:
            name = race.get('N', '')

            # Parse: "STATE REPRESENTATIVE DISTRICT 1" or "STATE SENATOR, DISTRICT 9"
            m = re.match(r'STATE (REPRESENTATIVE|SENATOR),?\s*DISTRICT\s*(\d+)', name)
            if not m:
                continue

            office = m.group(1)
            district = int(m.group(2))
            chamber = 'State House' if office == 'REPRESENTATIVE' else 'State Senate'

            if chamber_filter and chamber_filter.lower() not in chamber.lower():
                continue

            candidates = []
            total = race.get('T', 0)

            for cand in race.get('Candidates', []):
                cand_name = cand.get('N', '').strip()
                # Remove incumbent marker
                cand_name = re.sub(r'\s*\(I\)\s*$', '', cand_name)
                votes = cand.get('V', 0)
                pct = cand.get('PE', 0) / 100  # TX returns as percentage, normalize to decimal

                candidates.append({
                    'name': cand_name,
                    'party': party,
                    'votes': votes,
                    'pct': pct,
                })

            candidates.sort(key=lambda c: c['votes'], reverse=True)

            if len(candidates) > 1:  # Only contested races
                contests.append({
                    'state': 'TX',
                    'chamber': chamber,
                    'district': district,
                    'party': party,
                    'candidates': candidates,
                })

    return contests


# ══════════════════════════════════════════════════════════════════════
# MATCHING + DB UPDATE (state-agnostic)
# ══════════════════════════════════════════════════════════════════════

def load_db_elections(state, year):
    """Load all primary elections + candidacies for a state/year from DB."""
    query = f"""
    SELECT
        e.id AS election_id,
        e.election_type,
        e.result_status,
        e.total_votes_cast,
        d.district_name,
        d.district_number,
        d.chamber,
        s.office_type,
        s.id AS seat_id,
        c2.id AS candidacy_id,
        c2.candidate_id,
        c2.party AS cand_party,
        c2.votes_received,
        c2.vote_percentage,
        c2.result,
        c2.is_incumbent,
        ca.full_name
    FROM elections e
    JOIN seats s ON e.seat_id = s.id
    JOIN districts d ON s.district_id = d.id
    JOIN states st ON d.state_id = st.id
    LEFT JOIN candidacies c2 ON c2.election_id = e.id
    LEFT JOIN candidates ca ON c2.candidate_id = ca.id
    WHERE st.abbreviation = '{state}'
      AND e.election_year = {year}
      AND e.election_type IN ('Primary_D', 'Primary_R')
      AND d.office_level = 'Legislative'
    ORDER BY d.district_name, e.election_type, c2.votes_received DESC NULLS LAST
    """
    return run_sql(query)


def match_and_update(contests, db_elections, dry_run=True):
    """
    Match SoS results to DB elections and update vote counts.

    Returns summary stats.
    """
    stats = {
        'matched': 0,
        'updated_votes': 0,
        'updated_results': 0,
        'updated_status': 0,
        'unmatched_contests': [],
        'unmatched_candidates': [],
        'new_info': [],
    }

    # Index DB elections by (chamber, district_number, election_type)
    db_by_contest = {}
    for row in db_elections:
        chamber = row['chamber']
        dist_num = row['district_number']
        etype = row['election_type']
        key = (chamber, str(dist_num), etype)
        if key not in db_by_contest:
            db_by_contest[key] = {
                'election_id': row['election_id'],
                'result_status': row['result_status'],
                'total_votes_cast': row['total_votes_cast'],
                'seat_id': row['seat_id'],
                'district_name': row['district_name'],
                'candidates': [],
            }
        if row['candidacy_id']:
            db_by_contest[key]['candidates'].append(row)

    for contest in contests:
        dist_num = contest['district']
        chamber = contest['chamber']
        # Map SoS chamber names to DB chamber names
        db_chamber = 'House' if 'House' in chamber else 'Senate'
        dist_name = f'{db_chamber} District {dist_num}'

        party = contest['party']
        etype = f'Primary_{party}'

        key = (db_chamber, str(dist_num), etype)
        db_contest = db_by_contest.get(key)

        if not db_contest:
            stats['unmatched_contests'].append(f'{db_chamber} {dist_num} {etype}')
            continue

        stats['matched'] += 1
        election_id = db_contest['election_id']
        dist_name = db_contest['district_name']

        # Calculate total votes for this contest
        total_votes = sum(c['votes'] for c in contest['candidates'])

        # Check if we need to update election-level data
        needs_status_update = db_contest['result_status'] != 'Certified'
        needs_votes_update = db_contest['total_votes_cast'] != total_votes

        if needs_votes_update:
            if not dry_run:
                run_sql(f"""
                    UPDATE elections
                    SET total_votes_cast = {total_votes},
                        result_status = 'Called'
                    WHERE id = {election_id}
                """)
            stats['updated_status'] += 1
            print(f'  {dist_name} {etype}: total_votes → {total_votes:,}', flush=True)

        # Match each SoS candidate to a DB candidate
        for sos_cand in contest['candidates']:
            matched_db = None
            for db_cand in db_contest['candidates']:
                if names_match(sos_cand['name'], db_cand['full_name']):
                    matched_db = db_cand
                    break

            if not matched_db:
                stats['unmatched_candidates'].append(
                    f'{dist_name} {etype}: "{sos_cand["name"]}" ({sos_cand["votes"]:,} votes)'
                )
                continue

            # Check if votes need updating
            db_votes = matched_db['votes_received']
            db_pct = matched_db['vote_percentage']
            new_votes = sos_cand['votes']
            new_pct = round(sos_cand['pct'] * 100, 1)

            # Determine result
            is_winner = (sos_cand == contest['candidates'][0])  # Highest votes
            new_result = 'Won' if is_winner else 'Lost'
            old_result = matched_db['result']

            needs_update = (
                db_votes != new_votes or
                db_pct != new_pct or
                old_result != new_result
            )

            if needs_update:
                stats['updated_votes'] += 1
                if db_votes != new_votes:
                    change = ''
                    if db_votes is not None:
                        diff = new_votes - db_votes
                        change = f' (was {db_votes:,}, diff {diff:+,})'
                    stats['new_info'].append(
                        f'  {dist_name} {etype}: {matched_db["full_name"]} → '
                        f'{new_votes:,} ({new_pct}%) [{new_result}]{change}'
                    )

                if not dry_run:
                    run_sql(f"""
                        UPDATE candidacies
                        SET votes_received = {new_votes},
                            vote_percentage = {new_pct},
                            result = '{new_result}'
                        WHERE id = {matched_db['candidacy_id']}
                    """)

    return stats


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

STATE_HANDLERS = {
    'AR': {
        'download': ar_download,
        'parse': ar_parse_results,
        'default_date': '2026-03-03',
    },
    'NC': {
        'download': nc_download,
        'parse': nc_parse_results,
        'default_date': '2026-03-03',
    },
    'TX': {
        'download': tx_download,
        'parse': tx_parse_results,
        'default_date': '2026-03-03',
    },
}


def main():
    parser = argparse.ArgumentParser(
        description='Import primary election results from state SoS websites',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('--state', required=True, choices=sorted(STATE_HANDLERS.keys()),
                        help='State to import')
    parser.add_argument('--date', type=str, help='Election date (YYYY-MM-DD)')
    parser.add_argument('--chamber', type=str, help='Filter by chamber (house/senate)')
    parser.add_argument('--year', type=int, default=2026, help='Election year (default: 2026)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without writing to DB')
    args = parser.parse_args()

    state = args.state
    handler = STATE_HANDLERS[state]
    election_date = args.date or handler['default_date']
    year = args.year

    print(f'\n{"=" * 60}')
    print(f'Importing {state} Primary Results')
    print(f'Election date: {election_date}')
    print(f'Mode: {"DRY RUN" if args.dry_run else "LIVE — writing to DB"}')
    print(f'{"=" * 60}\n')

    # Step 1: Download results from SoS
    print('Step 1: Downloading official results...', flush=True)
    raw_data = handler['download'](election_date)
    if not raw_data:
        print('ERROR: No data downloaded. Exiting.', flush=True)
        sys.exit(1)

    # Step 2: Parse into standardized format
    print('\nStep 2: Parsing results...', flush=True)
    contests = handler['parse'](raw_data, chamber_filter=args.chamber)
    print(f'  Found {len(contests)} contested primaries', flush=True)

    # Step 3: Load DB elections for matching
    print('\nStep 3: Loading DB elections...', flush=True)
    db_elections = load_db_elections(state, year)
    if db_elections is None:
        print('ERROR: Failed to load DB elections. Exiting.', flush=True)
        sys.exit(1)
    print(f'  Loaded {len(db_elections)} candidacy rows from DB', flush=True)

    # Step 4: Match and update
    print(f'\nStep 4: {"Previewing" if args.dry_run else "Applying"} updates...', flush=True)
    stats = match_and_update(contests, db_elections, dry_run=args.dry_run)

    # Step 5: Summary
    print(f'\n{"=" * 60}')
    print(f'SUMMARY')
    print(f'{"=" * 60}')
    print(f'  Contests matched: {stats["matched"]}')
    print(f'  Candidacy votes updated: {stats["updated_votes"]}')
    print(f'  Election statuses updated: {stats["updated_status"]}')

    if stats['new_info']:
        print(f'\n  Vote changes ({len(stats["new_info"])}):')
        for info in stats['new_info'][:50]:
            print(info)
        if len(stats['new_info']) > 50:
            print(f'  ... and {len(stats["new_info"]) - 50} more')

    if stats['unmatched_contests']:
        print(f'\n  UNMATCHED CONTESTS ({len(stats["unmatched_contests"])}):')
        for c in stats['unmatched_contests']:
            print(f'    {c}')

    if stats['unmatched_candidates']:
        print(f'\n  UNMATCHED CANDIDATES ({len(stats["unmatched_candidates"])}):')
        for c in stats['unmatched_candidates'][:30]:
            print(f'    {c}')
        if len(stats['unmatched_candidates']) > 30:
            print(f'    ... and {len(stats["unmatched_candidates"]) - 30} more')

    if args.dry_run:
        print(f'\n  *** DRY RUN — no changes written to DB ***')
        print(f'  Run without --dry-run to apply changes.')
    else:
        print(f'\n  Changes applied to database.')
        print(f'  Next steps:')
        print(f'    python3 scripts/export_site_data.py --state {state}')
        print(f'    python3 scripts/export_district_data.py --state {state}')


if __name__ == '__main__':
    main()
