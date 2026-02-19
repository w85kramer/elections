#!/usr/bin/env python3
"""
Fix NE candidate party affiliations in the database.

NE is officially nonpartisan, but candidates have actual party affiliations
that Ballotpedia tracks. This script:
  1. Fetches NE election result pages from Ballotpedia (2010-2024)
  2. Extracts candidate names and parties
  3. Updates candidacies.party in the database

Usage:
    python3 scripts/fix_ne_parties.py --dry-run
    python3 scripts/fix_ne_parties.py
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

# ─────────────────────────────────────────────
# NE candidate → party mapping from Ballotpedia
# ─────────────────────────────────────────────

# These are compiled from BP election result pages and member listings.
# Format: { 'normalized_name': 'party' }
# We'll match by normalized name across all NE candidacies.

KNOWN_PARTIES = {}

# ─────────────────────────────────────────────
# BP FETCHING
# ─────────────────────────────────────────────

BP_YEARS = [2024, 2022, 2020, 2018, 2016, 2014, 2012, 2010]
BP_URL = 'https://en.wikipedia.org/api/rest_v1/page/html/{title}'
BP_DIRECT = 'https://ballotpedia.org/{title}'

PARTY_MAP = {
    'Republican': 'R', 'Democratic': 'D', 'Democrat': 'D',
    'Libertarian': 'L', 'Green': 'G', 'Independent': 'I',
    'Nonpartisan': 'NP', 'Constitution': 'Con',
    'R': 'R', 'D': 'D', 'L': 'L', 'I': 'I', 'NP': 'NP',
    'N': 'NP',
}


def normalize_name(name):
    """Normalize name for matching."""
    if not name:
        return ''
    name = name.strip()
    name = re.sub(r'\s+(Jr\.?|Sr\.?|III|II|IV)$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+[A-Z]\.\s+', ' ', name)
    name = re.sub(r'^[A-Z]\.\s+', '', name)
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    name = re.sub(r'\.', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name.lower()


def fetch_bp_page(title):
    """Fetch a Ballotpedia page via Wikipedia REST API."""
    url = BP_URL.format(title=title)
    try:
        resp = httpx.get(url, headers={'User-Agent': 'ElectionsTracker/1.0'}, timeout=30, follow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    # Fallback to direct BP fetch
    url2 = BP_DIRECT.format(title=title)
    try:
        resp = httpx.get(url2, headers={'User-Agent': 'ElectionsTracker/1.0'}, timeout=30, follow_redirects=True)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return None


def parse_ne_election_page(html, year):
    """Parse candidate parties from a NE election results page."""
    candidates = {}  # name → party

    # Strategy 1: Look for structured data with party indicators
    # BP typically has tables with candidate names and party indicators
    # Pattern: candidate name followed by (R), (D), etc.
    for match in re.finditer(r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?(?:\s+(?:de|von|Van|Mc|Mac|O\')?[A-Z][a-z]+)+)\s*\(([A-Z]+)\)', html):
        name = match.group(1).strip()
        party_code = match.group(2)
        if party_code in PARTY_MAP:
            candidates[normalize_name(name)] = PARTY_MAP[party_code]

    # Strategy 2: Look for party mentions in table cells
    # Pattern: "Republican" or "Democratic" near candidate names in tables
    for match in re.finditer(
        r'(?:class="[^"]*(?:Republican|Democratic|Libertarian|Independent|Nonpartisan)[^"]*"[^>]*>|'
        r'(?:Republican|Democratic|Democrat|Libertarian|Independent|Nonpartisan)\s*</)',
        html
    ):
        # Get surrounding context to find the candidate name
        start = max(0, match.start() - 500)
        end = min(len(html), match.end() + 500)
        context = html[start:end]

        # Extract party
        party_match = re.search(r'(Republican|Democratic|Democrat|Libertarian|Independent|Nonpartisan)', match.group(0))
        if not party_match:
            continue
        party = PARTY_MAP.get(party_match.group(1))
        if not party:
            continue

        # Find candidate name near this party indicator
        # Look for linked names
        for name_match in re.finditer(r'>([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?(?:\s+(?:de|von|Van|Mc|Mac|O\')?[A-Z][a-z]+)+)<', context):
            cand_name = name_match.group(1).strip()
            if len(cand_name) > 5 and cand_name not in ('General Election', 'Primary Election', 'Nebraska State', 'United States'):
                norm = normalize_name(cand_name)
                if norm not in candidates:
                    candidates[norm] = party

    return candidates


def fetch_all_ne_parties():
    """Fetch NE candidate parties from BP election pages."""
    all_parties = {}

    for year in BP_YEARS:
        title = f'Nebraska_State_Senate_elections,_{year}'
        print(f'  Fetching {year}...')
        html = fetch_bp_page(title)
        if html:
            parties = parse_ne_election_page(html, year)
            print(f'    Found {len(parties)} candidates with party data')
            all_parties.update(parties)
        else:
            print(f'    WARN: Could not fetch {title}')
        time.sleep(1)  # Be polite

    return all_parties


# ─────────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────────

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
            wait = 10 * (attempt + 1)
            print(f'    Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR ({resp.status_code}): {resp.text[:500]}')
        return None
    return None


def esc(s):
    if s is None:
        return None
    return str(s).replace("'", "''")


def main():
    parser = argparse.ArgumentParser(description='Fix NE candidate party affiliations')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    print('Phase 1: Fetching NE candidate parties from Ballotpedia...')
    bp_parties = fetch_all_ne_parties()
    print(f'  Total unique candidates with party data: {len(bp_parties)}')

    # Also add current member data (from BP current membership page)
    # This is the most reliable source for current members
    current_members = {
        normalize_name('Bob Hallstrom'): 'R',
        normalize_name('Robert Clements'): 'R',
        normalize_name('Victor Rountree'): 'NP',
        normalize_name('R. Brad von Gillern'): 'R',
        normalize_name('Brad von Gillern'): 'R',
        normalize_name('Margo Juarez'): 'D',
        normalize_name('Machaela Cavanaugh'): 'D',
        normalize_name('Dunixi Guereca'): 'D',
        normalize_name('Megan Hunt'): 'NP',
        normalize_name('John Cavanaugh'): 'D',
        normalize_name('Wendy DeBoer'): 'D',
        normalize_name('Terrell McKinney'): 'D',
        normalize_name('Merv Riepe'): 'R',
        normalize_name('Ashlei Spivey'): 'D',
        normalize_name('John Arch'): 'R',
        normalize_name('Dave Wordekemper'): 'R',
        normalize_name('Ben Hansen'): 'R',
        normalize_name('Glen Meyer'): 'R',
        normalize_name('Christy Armendariz'): 'R',
        normalize_name('Robert Dover'): 'R',
        normalize_name('Rob Dover'): 'R',
        normalize_name('John Fredrickson'): 'D',
        normalize_name('Beau Ballard'): 'R',
        normalize_name('Mike Moser'): 'R',
        normalize_name('Jared Storm'): 'R',
        normalize_name('Jana Hughes'): 'R',
        normalize_name('Carolyn Bosn'): 'R',
        normalize_name('George Dungan III'): 'D',
        normalize_name('George Dungan'): 'D',
        normalize_name('Jason Prokop'): 'D',
        normalize_name('Jane Raybould'): 'D',
        normalize_name('Eliot Bostar'): 'D',
        normalize_name('Myron Dorn'): 'R',
        normalize_name('Kathleen Kauth'): 'R',
        normalize_name('Tom Brandt'): 'R',
        normalize_name('Dan Lonowski'): 'R',
        normalize_name('Loren Lippincott'): 'R',
        normalize_name('Dan Quick'): 'D',
        normalize_name('Rick Holdcroft'): 'R',
        normalize_name('Stan Clouse'): 'R',
        normalize_name('Stanley Clouse'): 'R',
        normalize_name('Dave Murman'): 'R',
        normalize_name('Tony Sorrentino'): 'R',
        normalize_name('Barry DeKay'): 'R',
        normalize_name('Fred Meyer'): 'R',
        normalize_name('Mike Jacobson'): 'R',
        normalize_name('Michael Jacobson'): 'R',
        normalize_name('Tanya Storer'): 'R',
        normalize_name('Teresa Ibach'): 'R',
        normalize_name('Rita Sanders'): 'R',
        normalize_name('Danielle Conrad'): 'D',
        normalize_name('Paul Strommen'): 'R',
        normalize_name('Brian Hardin'): 'R',
        normalize_name('Bob Andersen'): 'R',
    }

    # 2024 general election candidates (including losers)
    candidates_2024 = {
        normalize_name('Dennis Schaardt'): 'R',
        normalize_name('Felix Ungerman'): 'D',
        normalize_name('Gilbert Ayala'): 'R',
        normalize_name('Tim Pendrell'): 'R',
        normalize_name('Julia Palzer'): 'R',
        normalize_name('Nick Batter'): 'R',
        normalize_name('Roxie Kracl'): 'R',
        normalize_name('Mike Albrecht'): 'R',
        normalize_name('Jeanne Reigle'): 'D',
        normalize_name('Seth Derner'): 'D',
        normalize_name('Dennis Fujan'): 'R',
        normalize_name('Nicki Behmer-Popp'): 'D',
        normalize_name('Dawn Liphardt'): 'D',
        normalize_name('Mary Ann Folchert'): 'D',
        normalize_name('Michelle Smith'): 'D',
        normalize_name('Raymond Aguilar'): 'R',
        normalize_name('Lana Peister'): 'D',
        normalize_name('Allison Heimes'): 'R',
        normalize_name('Ethan Clark'): 'R',
        normalize_name('Daniel McKeon'): 'D',
        normalize_name('Tony Tangwall'): 'D',
        normalize_name('Sarah Centineo'): 'D',
        normalize_name('Larry Bolinger'): 'D',
        normalize_name('Jen Day'): 'D',
        normalize_name('Jennifer Day'): 'D',
        normalize_name('Robert Hallstrom'): 'R',
        normalize_name('Robert Dover'): 'R',
        normalize_name('Michael Jacobson'): 'R',
        normalize_name('Stanley Clouse'): 'R',
    }

    # Merge all known parties (current members take precedence)
    all_known = {}
    all_known.update(bp_parties)
    all_known.update(candidates_2024)
    all_known.update(current_members)

    print(f'\nPhase 2: Matching against DB candidacies...')

    # Get all NE candidacies with NULL or NP party
    rows = run_sql("""
        SELECT cy.id as candidacy_id, c.full_name, cy.party, e.election_year, e.election_type
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON cy.candidate_id = c.id
        WHERE st.abbreviation = 'NE'
          AND s.office_level = 'Legislative'
          AND (cy.party IS NULL OR cy.party = 'NP')
        ORDER BY e.election_year DESC, c.full_name
    """)

    if not rows:
        print('  No NE candidacies to update')
        return

    print(f'  Found {len(rows)} NE candidacies with NULL/NP party')

    # Match and build updates
    updates = []
    unmatched = []
    for r in rows:
        norm = normalize_name(r['full_name'])
        party = all_known.get(norm)
        if party and party != r.get('party'):
            updates.append({
                'id': r['candidacy_id'],
                'name': r['full_name'],
                'year': r['election_year'],
                'old_party': r.get('party'),
                'new_party': party,
            })
        else:
            unmatched.append(r)

    print(f'  Matched: {len(updates)} candidacies to update')
    print(f'  Unmatched: {len(unmatched)} candidacies (no party data found)')

    if updates:
        # Show sample updates
        print('\n  Sample updates:')
        for u in updates[:10]:
            print(f"    {u['name']} ({u['year']}): {u['old_party']} → {u['new_party']}")
        if len(updates) > 10:
            print(f'    ... and {len(updates) - 10} more')

    if unmatched:
        print('\n  Unmatched candidates (sample):')
        seen = set()
        for u in unmatched[:15]:
            name = u['full_name']
            if name not in seen:
                print(f"    {name} ({u['election_year']} {u['election_type']})")
                seen.add(name)

    if args.dry_run:
        print('\n  DRY RUN — no changes made')
        return

    if not updates:
        print('\n  Nothing to update')
        return

    # Also update seat_terms.party for NE legislative seats
    print(f'\nPhase 3: Updating {len(updates)} candidacies...')

    # Batch updates (50 at a time)
    batch_size = 50
    updated = 0
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i+batch_size]
        cases = ' '.join(
            f"WHEN {u['id']} THEN '{u['new_party']}'"
            for u in batch
        )
        ids = ','.join(str(u['id']) for u in batch)
        sql = f"""
            UPDATE candidacies
            SET party = CASE id {cases} END
            WHERE id IN ({ids})
        """
        result = run_sql(sql)
        if result is not None:
            updated += len(batch)
        else:
            print(f'    WARN: batch update failed at index {i}')

    print(f'  Updated {updated} candidacies')

    # Also update seat_terms.party for current NE holders
    print('\nPhase 4: Updating seat_terms.party for current NE holders...')
    st_updates = run_sql("""
        SELECT stm.id as term_id, c.full_name, stm.party
        FROM seat_terms stm
        JOIN seats s ON stm.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON stm.candidate_id = c.id
        WHERE st.abbreviation = 'NE'
          AND s.office_level = 'Legislative'
          AND stm.party = 'NP'
    """)

    if st_updates:
        st_batch = []
        for r in st_updates:
            norm = normalize_name(r['full_name'])
            party = all_known.get(norm)
            if party and party != 'NP':
                st_batch.append({'id': r['term_id'], 'party': party})

        if st_batch:
            cases = ' '.join(f"WHEN {u['id']} THEN '{u['party']}'" for u in st_batch)
            ids = ','.join(str(u['id']) for u in st_batch)
            run_sql(f"UPDATE seat_terms SET party = CASE id {cases} END WHERE id IN ({ids})")
            print(f'  Updated {len(st_batch)} seat_terms')

    print('\nDone.')


if __name__ == '__main__':
    main()
