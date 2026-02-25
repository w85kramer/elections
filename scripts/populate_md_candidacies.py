"""
Populate MD 2026 candidacies from State Board of Elections CSV.

Maryland's filing deadline closed Feb 24, 2026. The MD SBE published a CSV
with all filed candidates. This script parses legislative + statewide candidates
and imports them as candidacies.

Usage:
    python3 scripts/populate_md_candidacies.py --dry-run
    python3 scripts/populate_md_candidacies.py
    python3 scripts/populate_md_candidacies.py --statewide-only
    python3 scripts/populate_md_candidacies.py --legislative-only
"""
import csv
import sys
import re
import time
import argparse
from collections import Counter, defaultdict

import os
import httpx
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF

CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tmp',
                        'MD_2026_GP_statewide_candidatelist.csv')
BATCH_SIZE = 400

# ══════════════════════════════════════════════════════════════════════
# DB helpers
# ══════════════════════════════════════════════════════════════════════

def run_sql(query, exit_on_error=True, max_retries=5):
    for attempt in range(1, max_retries + 1):
        resp = httpx.post(
            f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429 and attempt < max_retries:
            wait = 5 * attempt
            print(f'  Rate limited (429), retrying in {wait}s (attempt {attempt}/{max_retries})...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None
    return None


def esc(s):
    if s is None:
        return ''
    return str(s).replace("'", "''")


# ══════════════════════════════════════════════════════════════════════
# CSV parsing
# ══════════════════════════════════════════════════════════════════════

PARTY_MAP = {
    'Democratic': 'D',
    'Republican': 'R',
}

def parse_csv():
    """
    Parse the MD SBE CSV file.

    Returns:
        senate: list of dicts with candidate info for State Senator rows
        house: list of dicts with candidate info for House of Delegates rows
        statewide: list of dicts for Governor, Lt. Gov, Comptroller, AG
    """
    senate = []
    house = []
    statewide = []

    with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            office = row['Office Name'].strip()
            status = row['Candidate Status'].strip()
            party_full = row['Office Political Party'].strip()
            party = PARTY_MAP.get(party_full)

            # Skip non-Active candidates
            if status != 'Active':
                continue

            # Skip non-D/R (Green, Unaffiliated, etc.)
            if party is None:
                continue

            last_name = row['Candidate Ballot Last Name and Suffix'].strip().strip('"')
            first_name = row['Candidate First Name and Middle Name'].strip()
            district_raw = row['Contest Run By District Name and Number'].strip()
            gender = row['Candidate Gender'].strip() or None

            # Parse filing date from "Regular - MM/DD/YYYY" or "Federal - MM/DD/YYYY"
            filing_str = row['Filing Type and Date'].strip()
            filing_date = None
            date_match = re.search(r'(\d{2}/\d{2}/\d{4})', filing_str)
            if date_match:
                m, d, y = date_match.group(1).split('/')
                filing_date = f'{y}-{m}-{d}'

            candidate_info = {
                'first_name': first_name,
                'last_name': last_name,
                'full_name': f'{first_name} {last_name}'.strip(),
                'party': party,
                'filing_date': filing_date,
                'gender': gender,
            }

            if office == 'State Senator':
                # "Legislative District X" → X
                dist_num = district_raw.replace('Legislative District ', '')
                candidate_info['district_number'] = dist_num
                senate.append(candidate_info)

            elif office == 'House of Delegates':
                dist_num = district_raw.replace('Legislative District ', '')
                candidate_info['district_number'] = dist_num
                house.append(candidate_info)

            elif office == 'Governor / Lt. Governor':
                candidate_info['office'] = 'Governor'
                candidate_info['district_raw'] = district_raw
                # Parse running mate info
                has_related = row.get('Has Related Candidate', '').strip()
                if has_related == 'Yes':
                    related_last = row.get('Related Candidate Last Name and Suffix', '').strip()
                    related_first = row.get('Related Candidate First Name and Middle Name', '').strip()
                    related_party = PARTY_MAP.get(row.get('Related Office Political Party', '').strip())
                    related_status = row.get('Related Candidate Status', '').strip()
                    related_gender = row.get('Related Candidate Gender', '').strip() or None
                    # Parse related filing date
                    related_filing_str = row.get('Related Candidate Filing Type and Date', '').strip()
                    related_filing_date = None
                    rd_match = re.search(r'(\d{2}/\d{2}/\d{4})', related_filing_str)
                    if rd_match:
                        rm, rd, ry = rd_match.group(1).split('/')
                        related_filing_date = f'{ry}-{rm}-{rd}'

                    if related_status == 'Active' and related_party == party:
                        candidate_info['running_mate'] = {
                            'first_name': related_first,
                            'last_name': related_last,
                            'full_name': f'{related_first} {related_last}'.strip(),
                            'party': related_party,
                            'filing_date': related_filing_date,
                            'gender': related_gender,
                            'office': 'Lt. Governor',
                        }
                statewide.append(candidate_info)

            elif office == 'Comptroller':
                candidate_info['office'] = 'Controller'  # DB uses "Controller"
                statewide.append(candidate_info)

            elif office == 'Attorney General':
                candidate_info['office'] = 'Attorney General'
                statewide.append(candidate_info)

    return senate, house, statewide


# ══════════════════════════════════════════════════════════════════════
# Name matching (reused from populate_candidacies.py)
# ══════════════════════════════════════════════════════════════════════

def strip_accents(s):
    import unicodedata
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def name_similarity(name1, name2):
    if name1 is None or name2 is None:
        return 0.0

    def normalize(n):
        n = re.sub(r'\s+(Jr\.?|Sr\.?|III|IV|II|,\s*Jr\.?|,\s*Sr\.?)\s*$', '', n, flags=re.IGNORECASE)
        n = re.sub(r',\s*(Jr\.?|Sr\.?|III|IV|II)\s*$', '', n, flags=re.IGNORECASE)
        n = strip_accents(n)
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
    if first1[0] == first2[0]:
        return 0.7
    return 0.3


# ══════════════════════════════════════════════════════════════════════
# DB lookups
# ══════════════════════════════════════════════════════════════════════

def load_legislative_context():
    """Load MD legislative seats, elections, and incumbents."""
    # All MD legislative seats with 2026 elections
    seats = run_sql("""
        SELECT se.id as seat_id, se.office_type, se.seat_designator,
               d.district_number, d.num_seats
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'MD'
          AND se.next_regular_election_year = 2026
          AND se.selection_method = 'Elected'
          AND d.office_level = 'Legislative'
        ORDER BY se.office_type, d.district_number, se.seat_designator
    """)

    # Build maps: single-seat and multi-seat
    # Key: (office_type, district_number)
    single_seat_map = {}   # → seat_id
    multi_seat_map = {}    # → [seat_ids sorted by designator]

    for s in seats:
        key = (s['office_type'], s['district_number'])
        if s['num_seats'] == 1:
            single_seat_map[key] = s['seat_id']
        else:
            if key not in multi_seat_map:
                multi_seat_map[key] = []
            multi_seat_map[key].append(s['seat_id'])

    # Elections for these seats
    elections = run_sql("""
        SELECT e.id as election_id, e.seat_id, e.election_type
        FROM elections e
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'MD'
          AND e.election_year = 2026
          AND d.office_level = 'Legislative'
    """)

    election_map = defaultdict(dict)  # seat_id → {election_type → election_id}
    for e in elections:
        election_map[e['seat_id']][e['election_type']] = e['election_id']

    # Incumbents
    incumbents = run_sql("""
        SELECT st.seat_id, st.candidate_id, c.full_name
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        JOIN candidates c ON st.candidate_id = c.id
        WHERE s.abbreviation = 'MD'
          AND st.end_date IS NULL
          AND d.office_level = 'Legislative'
    """)

    incumbent_map = {}  # seat_id → (candidate_id, full_name)
    for inc in incumbents:
        incumbent_map[inc['seat_id']] = (inc['candidate_id'], inc['full_name'])

    return single_seat_map, multi_seat_map, election_map, incumbent_map


def load_statewide_context():
    """Load MD statewide seats, elections, and incumbents for 2026."""
    seats = run_sql("""
        SELECT se.id as seat_id, se.office_type
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = 'MD'
          AND d.office_level = 'Statewide'
          AND se.next_regular_election_year = 2026
    """)

    seat_map = {}  # office_type → seat_id
    for s in seats:
        seat_map[s['office_type']] = s['seat_id']

    if not seat_map:
        return seat_map, {}, {}

    seat_ids_str = ','.join(str(sid) for sid in seat_map.values())

    elections = run_sql(f"""
        SELECT e.id as election_id, e.seat_id, e.election_type
        FROM elections e
        WHERE e.seat_id IN ({seat_ids_str}) AND e.election_year = 2026
    """)

    election_map = defaultdict(dict)
    for e in elections:
        election_map[e['seat_id']][e['election_type']] = e['election_id']

    incumbents = run_sql(f"""
        SELECT st.seat_id, st.candidate_id, c.full_name
        FROM seat_terms st
        JOIN candidates c ON st.candidate_id = c.id
        WHERE st.seat_id IN ({seat_ids_str}) AND st.end_date IS NULL
    """)

    incumbent_map = {}
    for inc in incumbents:
        incumbent_map[inc['seat_id']] = (inc['candidate_id'], inc['full_name'])

    return seat_map, election_map, incumbent_map


def load_existing_candidates():
    """Load all existing candidates for name matching."""
    candidates = run_sql("""
        SELECT id, full_name, first_name, last_name FROM candidates
    """)
    return candidates


# ══════════════════════════════════════════════════════════════════════
# Candidate matching
# ══════════════════════════════════════════════════════════════════════

def find_candidate_by_name(full_name, last_name, first_name, all_candidates):
    """
    Search existing candidates by name. Returns (candidate_id, matched_name) or (None, None).
    Requires last_name match (3+ chars) plus first name similarity.
    """
    best_id = None
    best_sim = 0.0
    best_name = None

    for c in all_candidates:
        sim = name_similarity(full_name, c['full_name'])
        if sim > best_sim:
            best_sim = sim
            best_id = c['id']
            best_name = c['full_name']

    if best_sim >= 0.7:
        return best_id, best_name
    return None, None


def find_incumbent_match(seat_id, full_name, incumbent_map):
    """Check if this candidate is the incumbent for a specific seat."""
    inc_info = incumbent_map.get(seat_id)
    if not inc_info:
        return None, False
    sim = name_similarity(full_name, inc_info[1])
    if sim >= 0.7:
        return inc_info[0], True
    return None, False


# ══════════════════════════════════════════════════════════════════════
# Match & insert logic
# ══════════════════════════════════════════════════════════════════════

def match_legislative_candidates(candidates, office_type, single_seat_map, multi_seat_map,
                                  election_map, incumbent_map, all_candidates):
    """
    Match parsed legislative candidates to DB elections.

    For multi-seat districts, ALL candidates go to Seat A's primary (bloc voting).
    """
    matched = []
    unmatched = []

    for cand in candidates:
        dist = cand['district_number']
        party = cand['party']
        key = (office_type, dist)
        election_type = f'Primary_{party}'

        # Find seat
        if key in single_seat_map:
            seat_id = single_seat_map[key]
        elif key in multi_seat_map:
            # Multi-member: use Seat A (first in sorted list)
            seat_id = multi_seat_map[key][0]
        else:
            unmatched.append((dist, cand['full_name'], party, 'no_district_in_db'))
            continue

        # Find election
        elections = election_map.get(seat_id, {})
        election_id = elections.get(election_type)
        if not election_id:
            unmatched.append((dist, cand['full_name'], party, f'no_{election_type}_election'))
            continue

        # Check incumbent match (check all seats in district for multi-member)
        candidate_id = None
        is_incumbent = False

        if key in multi_seat_map:
            # Check all seats in the district
            for sid in multi_seat_map[key]:
                cid, is_inc = find_incumbent_match(sid, cand['full_name'], incumbent_map)
                if is_inc:
                    candidate_id = cid
                    is_incumbent = True
                    break
        else:
            candidate_id, is_incumbent = find_incumbent_match(seat_id, cand['full_name'], incumbent_map)

        # If not incumbent, search all existing candidates by name
        if candidate_id is None:
            candidate_id, _ = find_candidate_by_name(
                cand['full_name'], cand['last_name'], cand['first_name'], all_candidates
            )

        matched.append({
            'election_id': election_id,
            'candidate_id': candidate_id,
            'candidate_name': cand['full_name'],
            'first_name': cand['first_name'],
            'last_name': cand['last_name'],
            'party': party,
            'is_incumbent': is_incumbent,
            'filing_date': cand['filing_date'],
            'gender': cand['gender'],
            'seat_id': seat_id,
            'district': dist,
        })

    return matched, unmatched


def match_statewide_candidates(statewide_cands, sw_seat_map, sw_election_map,
                                sw_incumbent_map, all_candidates):
    """Match statewide candidates (Gov, LtGov, AG, Comptroller) to DB elections."""
    matched = []
    unmatched = []

    for cand in statewide_cands:
        office = cand['office']
        party = cand['party']

        seat_id = sw_seat_map.get(office)
        if not seat_id:
            unmatched.append((office, cand['full_name'], party, 'no_seat_in_db'))
            continue

        election_type = f'Primary_{party}'
        elections = sw_election_map.get(seat_id, {})
        election_id = elections.get(election_type)
        if not election_id:
            unmatched.append((office, cand['full_name'], party, f'no_{election_type}_election'))
            continue

        # Check incumbent
        candidate_id, is_incumbent = find_incumbent_match(seat_id, cand['full_name'], sw_incumbent_map)

        # Search existing candidates if not incumbent
        if candidate_id is None:
            candidate_id, _ = find_candidate_by_name(
                cand['full_name'], cand['last_name'], cand['first_name'], all_candidates
            )

        matched.append({
            'election_id': election_id,
            'candidate_id': candidate_id,
            'candidate_name': cand['full_name'],
            'first_name': cand['first_name'],
            'last_name': cand['last_name'],
            'party': party,
            'is_incumbent': is_incumbent,
            'filing_date': cand['filing_date'],
            'gender': cand['gender'],
            'office': office,
        })

        # Handle running mate (Lt. Governor)
        if 'running_mate' in cand:
            mate = cand['running_mate']
            mate_office = mate['office']
            mate_seat_id = sw_seat_map.get(mate_office)
            if not mate_seat_id:
                unmatched.append((mate_office, mate['full_name'], mate['party'], 'no_seat_in_db'))
                continue

            mate_election_id = sw_election_map.get(mate_seat_id, {}).get(election_type)
            if not mate_election_id:
                unmatched.append((mate_office, mate['full_name'], mate['party'], f'no_{election_type}_election'))
                continue

            mate_cand_id, mate_is_inc = find_incumbent_match(
                mate_seat_id, mate['full_name'], sw_incumbent_map
            )
            if mate_cand_id is None:
                mate_cand_id, _ = find_candidate_by_name(
                    mate['full_name'], mate['last_name'], mate['first_name'], all_candidates
                )

            matched.append({
                'election_id': mate_election_id,
                'candidate_id': mate_cand_id,
                'candidate_name': mate['full_name'],
                'first_name': mate['first_name'],
                'last_name': mate['last_name'],
                'party': mate['party'],
                'is_incumbent': mate_is_inc,
                'filing_date': mate['filing_date'],
                'gender': mate['gender'],
                'office': mate_office,
            })

    return matched, unmatched


# ══════════════════════════════════════════════════════════════════════
# Insert into DB
# ══════════════════════════════════════════════════════════════════════

def insert_candidacies(matched, all_candidates, dry_run=False):
    """
    Create new candidate records as needed, then insert candidacies.

    Returns (new_candidates_count, candidacies_count)
    """
    reuse = [m for m in matched if m['candidate_id'] is not None]
    new = [m for m in matched if m['candidate_id'] is None]

    print(f"    Existing candidates matched: {len(reuse)}")
    print(f"    New candidate records needed: {len(new)}")

    if dry_run:
        return len(new), len(matched)

    # Insert new candidates
    if new:
        values = []
        for m in new:
            first = esc(m['first_name'])
            last = esc(m['last_name'])
            full = esc(m['candidate_name'])
            gender = f"'{esc(m['gender'])}'" if m['gender'] else 'NULL'
            values.append(f"('{full}', '{first}', '{last}', {gender})")

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
                print(f"      Batch failed, retrying in 3s...")
                time.sleep(3)
                result = run_sql(sql)
            new_ids.extend(r['id'] for r in result)

        print(f"    Inserted {len(new_ids)} new candidates")
        if len(new_ids) != len(new):
            print(f"    ERROR: Expected {len(new)}, got {len(new_ids)}")
            sys.exit(1)

        # Assign IDs back and add to all_candidates for future matching
        for i, m in enumerate(new):
            m['candidate_id'] = new_ids[i]
            all_candidates.append({
                'id': new_ids[i],
                'full_name': m['candidate_name'],
                'first_name': m['first_name'],
                'last_name': m['last_name'],
            })

    # Insert candidacies
    all_cands = reuse + new
    values = []
    for m in all_cands:
        filing = f"'{m['filing_date']}'" if m['filing_date'] else 'NULL'
        values.append(
            f"({m['election_id']}, {m['candidate_id']}, '{esc(m['party'])}', "
            f"'Filed', {m['is_incumbent']}, false, {filing}, "
            f"NULL, NULL, NULL, 'Pending', NULL, NULL, true)"
        )

    total_inserted = 0
    for batch_start in range(0, len(values), BATCH_SIZE):
        batch = values[batch_start:batch_start + BATCH_SIZE]
        sql = (
            "INSERT INTO candidacies (election_id, candidate_id, party, "
            "candidate_status, is_incumbent, is_write_in, filing_date, "
            "withdrawal_date, votes_received, vote_percentage, result, "
            "endorsements, notes, is_major) VALUES\n"
            + ",\n".join(batch)
            + "\nRETURNING id;"
        )
        result = run_sql(sql, exit_on_error=False)
        if result is None:
            print(f"      Batch failed, retrying in 3s...")
            time.sleep(3)
            result = run_sql(sql)
        total_inserted += len(result)

    print(f"    Inserted {total_inserted} candidacies")
    if total_inserted != len(all_cands):
        print(f"    ERROR: Expected {len(all_cands)}, got {total_inserted}")

    return len(new), total_inserted


# ══════════════════════════════════════════════════════════════════════
# Verification
# ══════════════════════════════════════════════════════════════════════

def verify():
    """Run verification queries."""
    print(f"\n{'=' * 60}")
    print("VERIFICATION")
    print(f"{'=' * 60}")

    # Total MD candidacies
    result = run_sql("""
        SELECT COUNT(*) as cnt FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'MD' AND e.election_year = 2026
    """)
    print(f"\n  Total MD 2026 candidacies: {result[0]['cnt']}")

    # By office level
    result = run_sql("""
        SELECT d.office_level, COUNT(*) as cnt
        FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'MD' AND e.election_year = 2026
        GROUP BY d.office_level ORDER BY d.office_level
    """)
    for r in result:
        print(f"    {r['office_level']}: {r['cnt']}")

    # Legislative by chamber and party
    result = run_sql("""
        SELECT s.office_type, c.party, COUNT(*) as cnt
        FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'MD' AND e.election_year = 2026
          AND d.office_level = 'Legislative'
        GROUP BY s.office_type, c.party ORDER BY s.office_type, c.party
    """)
    print(f"\n  Legislative breakdown:")
    for r in result:
        print(f"    {r['office_type']} {r['party']}: {r['cnt']}")

    # Statewide by office
    result = run_sql("""
        SELECT s.office_type, c.party, COUNT(*) as cnt,
               SUM(CASE WHEN c.is_incumbent THEN 1 ELSE 0 END) as inc
        FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'MD' AND e.election_year = 2026
          AND d.office_level = 'Statewide'
        GROUP BY s.office_type, c.party ORDER BY s.office_type, c.party
    """)
    if result:
        print(f"\n  Statewide breakdown:")
        for r in result:
            inc_str = f" ({r['inc']} inc)" if r['inc'] > 0 else ""
            print(f"    {r['office_type']} {r['party']}: {r['cnt']}{inc_str}")

    # Duplicate check
    dupes = run_sql("""
        SELECT c.election_id, c.candidate_id, COUNT(*) as cnt
        FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'MD' AND e.election_year = 2026
        GROUP BY c.election_id, c.candidate_id HAVING COUNT(*) > 1
    """)
    if dupes:
        print(f"\n  WARNING: {len(dupes)} duplicate candidacies!")
    else:
        print(f"\n  No duplicate candidacies found.")

    # Spot checks
    spots = run_sql("""
        SELECT s.seat_label, ca.full_name, c.party, c.is_incumbent, e.election_type,
               d.office_level
        FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN candidates ca ON c.candidate_id = ca.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'MD' AND e.election_year = 2026
        ORDER BY RANDOM() LIMIT 10
    """)
    print(f"\n  Spot checks:")
    for r in spots:
        inc = " (i)" if r['is_incumbent'] else ""
        print(f"    {r['seat_label']}: {r['full_name']}{inc} [{r['party']}] → {r['election_type']}")


# ══════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Populate MD candidacies from SBE CSV')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and match only, no database inserts')
    parser.add_argument('--statewide-only', action='store_true',
                        help='Only process statewide races')
    parser.add_argument('--legislative-only', action='store_true',
                        help='Only process legislative races')
    args = parser.parse_args()

    if args.statewide_only and args.legislative_only:
        parser.error('Cannot use both --statewide-only and --legislative-only')

    do_legislative = not args.statewide_only
    do_statewide = not args.legislative_only

    if args.dry_run:
        print("DRY RUN MODE — no database changes will be made.\n")

    # ── Parse CSV ──
    print(f"Parsing CSV: {CSV_PATH}")
    senate, house, statewide = parse_csv()
    print(f"  Senate candidates: {len(senate)}")
    print(f"  House candidates: {len(house)}")
    print(f"  Statewide candidates: {len(statewide)}")

    party_s = Counter(c['party'] for c in senate)
    party_h = Counter(c['party'] for c in house)
    print(f"  Senate: D={party_s.get('D', 0)}, R={party_s.get('R', 0)}")
    print(f"  House:  D={party_h.get('D', 0)}, R={party_h.get('R', 0)}")

    # ── Check existing candidacies ──
    existing = run_sql("""
        SELECT COUNT(*) as cnt FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'MD' AND e.election_year = 2026
    """)
    if existing[0]['cnt'] > 0:
        print(f"\n  WARNING: MD already has {existing[0]['cnt']} candidacies!")
        print(f"  Aborting to prevent duplicates.")
        sys.exit(1)

    # ── Load all existing candidates for name matching ──
    print(f"\nLoading existing candidates for name matching...")
    all_candidates = load_existing_candidates()
    print(f"  {len(all_candidates)} candidates in DB")

    total_new_cands = 0
    total_candidacies = 0

    # ══════════════════════════════════════════════════════════════════════
    # Legislative
    # ══════════════════════════════════════════════════════════════════════
    if do_legislative:
        print(f"\n{'=' * 60}")
        print("LEGISLATIVE RACES")
        print(f"{'=' * 60}")

        print(f"\nLoading legislative DB context...")
        single_seat_map, multi_seat_map, election_map, incumbent_map = load_legislative_context()
        total_seats = len(single_seat_map) + sum(len(v) for v in multi_seat_map.values())
        print(f"  Seats with 2026 elections: {total_seats}")
        print(f"    Single-seat: {len(single_seat_map)}")
        print(f"    Multi-member seats: {sum(len(v) for v in multi_seat_map.values())}")
        print(f"  Incumbents: {len(incumbent_map)}")

        # Senate
        print(f"\n  --- State Senate ---")
        s_matched, s_unmatched = match_legislative_candidates(
            senate, 'State Senate', single_seat_map, multi_seat_map,
            election_map, incumbent_map, all_candidates
        )
        print(f"  Matched: {len(s_matched)}, Unmatched: {len(s_unmatched)}")
        if s_unmatched:
            for dist, name, party, reason in s_unmatched[:10]:
                print(f"    District {dist}: {name} ({party}) — {reason}")

        inc_count = sum(1 for m in s_matched if m['is_incumbent'])
        reuse_count = sum(1 for m in s_matched if m['candidate_id'] is not None)
        print(f"  Incumbents: {inc_count}, Existing candidates matched: {reuse_count}")

        if s_matched:
            new_c, cand_c = insert_candidacies(s_matched, all_candidates, dry_run=args.dry_run)
            total_new_cands += new_c
            total_candidacies += cand_c

        # House
        print(f"\n  --- House of Delegates ---")
        h_matched, h_unmatched = match_legislative_candidates(
            house, 'State House', single_seat_map, multi_seat_map,
            election_map, incumbent_map, all_candidates
        )
        print(f"  Matched: {len(h_matched)}, Unmatched: {len(h_unmatched)}")
        if h_unmatched:
            reasons = Counter(u[3] for u in h_unmatched)
            for reason, count in reasons.most_common():
                print(f"    Unmatched ({reason}): {count}")
            for dist, name, party, reason in h_unmatched[:10]:
                print(f"    District {dist}: {name} ({party}) — {reason}")

        inc_count = sum(1 for m in h_matched if m['is_incumbent'])
        reuse_count = sum(1 for m in h_matched if m['candidate_id'] is not None)
        print(f"  Incumbents: {inc_count}, Existing candidates matched: {reuse_count}")

        if h_matched:
            new_c, cand_c = insert_candidacies(h_matched, all_candidates, dry_run=args.dry_run)
            total_new_cands += new_c
            total_candidacies += cand_c

    # ══════════════════════════════════════════════════════════════════════
    # Statewide
    # ══════════════════════════════════════════════════════════════════════
    if do_statewide:
        print(f"\n{'=' * 60}")
        print("STATEWIDE RACES")
        print(f"{'=' * 60}")

        print(f"\nLoading statewide DB context...")
        sw_seat_map, sw_election_map, sw_incumbent_map = load_statewide_context()
        print(f"  Statewide seats with 2026 elections: {len(sw_seat_map)}")
        for ot, sid in sorted(sw_seat_map.items()):
            inc = sw_incumbent_map.get(sid)
            inc_str = f" (incumbent: {inc[1]})" if inc else ""
            print(f"    {ot}: seat_id={sid}{inc_str}")

        sw_matched, sw_unmatched = match_statewide_candidates(
            statewide, sw_seat_map, sw_election_map, sw_incumbent_map, all_candidates
        )
        print(f"\n  Matched: {len(sw_matched)}, Unmatched: {len(sw_unmatched)}")
        if sw_unmatched:
            for loc, name, party, reason in sw_unmatched:
                print(f"    {loc}: {name} ({party}) — {reason}")

        # Print matches by office
        by_office = defaultdict(list)
        for m in sw_matched:
            by_office[m['office']].append(m)
        for office in sorted(by_office.keys()):
            cands = by_office[office]
            d_count = sum(1 for c in cands if c['party'] == 'D')
            r_count = sum(1 for c in cands if c['party'] == 'R')
            inc_count = sum(1 for c in cands if c['is_incumbent'])
            print(f"    {office}: {len(cands)} (D:{d_count}, R:{r_count}, inc:{inc_count})")

        if sw_matched:
            new_c, cand_c = insert_candidacies(sw_matched, all_candidates, dry_run=args.dry_run)
            total_new_cands += new_c
            total_candidacies += cand_c

    # ══════════════════════════════════════════════════════════════════════
    # Summary
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 60}")
    print(f"SUMMARY")
    print(f"{'=' * 60}")
    print(f"  New candidates created: {total_new_cands}")
    print(f"  Candidacies inserted: {total_candidacies}")

    if not args.dry_run:
        verify()

    print("\nDone!")


if __name__ == '__main__':
    main()
