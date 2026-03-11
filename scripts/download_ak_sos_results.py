#!/usr/bin/env python3
"""
Download Alaska Secretary of State election results (2004-2018).

Fetches official certified results from the AK Division of Elections website,
parses the simple HTML tables, and saves structured JSON to /tmp.

Available data:
  - Generals:  2004, 2006, 2008, 2012, 2014, 2016, 2018
  - Primaries: 2004, 2006, 2010, 2012, 2014, 2016, 2018

URL pattern: https://www.elections.alaska.gov/results/{CODE}/data/results.htm
  General codes: 04GENR, 06GENR, ...
  Primary codes: 04PRIM, 06PRIM, ...

Usage:
    python3 scripts/download_ak_sos_results.py                # Download all
    python3 scripts/download_ak_sos_results.py --year 2012    # Single year
    python3 scripts/download_ak_sos_results.py --dry-run      # Show URLs only
"""

import sys
import os
import json
import re
import argparse
import time
from html.parser import HTMLParser

import httpx

BASE_URL = 'https://www.elections.alaska.gov/results'
OUTPUT_PATH = '/tmp/ak_sos_results.json'

# All available election pages (verified via HTTP probes)
ELECTIONS = [
    # (year, type_code, url_code, election_type_db)
    # Generals
    (2004, 'general', '04GENR', 'General'),
    (2006, 'general', '06GENR', 'General'),
    (2008, 'general', '08GENR', 'General'),
    (2012, 'general', '12GENR', 'General'),
    (2014, 'general', '14GENR', 'General'),
    (2016, 'general', '16GENR', 'General'),
    (2018, 'general', '18GENR', 'General'),
    # Primaries
    (2004, 'primary', '04PRIM', 'Primary'),
    (2006, 'primary', '06PRIM', 'Primary'),
    (2010, 'primary', '10PRIM', 'Primary'),
    (2012, 'primary', '12PRIM', 'Primary'),
    (2014, 'primary', '14PRIM', 'Primary'),
    (2016, 'primary', '16PRIM', 'Primary'),
    (2018, 'primary', '18PRIM', 'Primary'),
]

# Map SoS primary party codes to DB election types
# (R) → Primary_R
# (ADL), (D-C), (C) → Primary (the non-R primary ballot)
NON_R_CODES = {'ADL', 'D-C', 'C', 'DC'}

# AK election dates (general = first Tuesday after first Monday in November,
# primary varied — looked up from SoS headers)
ELECTION_DATES = {
    (2004, 'General'): '2004-11-02',
    (2006, 'General'): '2006-11-07',
    (2008, 'General'): '2008-11-04',
    (2012, 'General'): '2012-11-06',
    (2014, 'General'): '2014-11-04',
    (2016, 'General'): '2016-11-08',
    (2018, 'General'): '2018-11-06',
    (2004, 'Primary'): '2004-08-24',
    (2004, 'Primary_R'): '2004-08-24',
    (2006, 'Primary'): '2006-08-22',
    (2006, 'Primary_R'): '2006-08-22',
    (2010, 'Primary'): '2010-08-24',
    (2010, 'Primary_R'): '2010-08-24',
    (2012, 'Primary'): '2012-08-28',
    (2012, 'Primary_R'): '2012-08-28',
    (2014, 'Primary'): '2014-08-19',
    (2014, 'Primary_R'): '2014-08-19',
    (2016, 'Primary'): '2016-08-16',
    (2016, 'Primary_R'): '2016-08-16',
    (2018, 'Primary'): '2018-08-21',
    (2018, 'Primary_R'): '2018-08-21',
}


def fetch_page(url):
    """Fetch HTML page, skipping SSL verification (AK SoS cert issues)."""
    resp = httpx.get(url, verify=False, timeout=30, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


def parse_races(html):
    """Parse AK SoS results HTML into structured race data.

    Returns list of dicts with keys:
        race_title, chamber, district_letter_or_number,
        primary_party_code (None for generals),
        num_precincts, precincts_reporting, total_votes,
        candidates: [{name, party, votes, pct}]
    """
    races = []

    # Split by race tables — each race is wrapped in:
    # <table width="100%" border="1"><tr><td align=center><table ...>
    # We split on the outer table boundary
    race_blocks = re.split(r'<table width="100%" border(?:="1")?>', html)

    for block in race_blocks[1:]:  # skip preamble
        # Extract race title from <th> (some years omit closing </th>)
        th_match = re.search(r'<th[^>]*>([^<]+)', block)
        if not th_match:
            continue
        title = th_match.group(1).strip()

        # Filter for state legislative races only
        leg_match = re.match(
            r'(SENATE|HOUSE) DISTRICT (\w+)(?:\s*\(([^)]+)\))?$', title
        )
        if not leg_match:
            continue

        chamber_raw = leg_match.group(1)
        district_id = leg_match.group(2)
        primary_code = leg_match.group(3)  # None for generals

        chamber = 'Senate' if chamber_raw == 'SENATE' else 'House'

        # Parse metadata rows
        num_precincts = None
        precincts_reporting = None
        total_votes = None

        np_match = re.search(r'Number of Precincts.*?<td align=right>(\d+)', block, re.DOTALL)
        if np_match:
            num_precincts = int(np_match.group(1))

        pr_match = re.search(r'Precincts Reporting.*?<td align=right>(\d+)', block, re.DOTALL)
        if pr_match:
            precincts_reporting = int(pr_match.group(1))

        tv_match = re.search(r'Total Votes.*?<td align=right>(\d+)', block, re.DOTALL)
        if tv_match:
            total_votes = int(tv_match.group(1))

        # Parse candidate rows (after the <hr>)
        hr_pos = block.find('<hr>')
        if hr_pos == -1:
            continue
        cand_html = block[hr_pos:]

        candidates = []
        # Candidate rows: <tr><td align=left>Name</td><td align=left>PARTY</td><td align=right>VOTES</td><td align=right>PCT%</td></tr>
        cand_pattern = re.compile(
            r'<tr>\s*<td align=left>([^<]+)</td>\s*'
            r'<td[^>]*>([^<]*)</td>\s*'
            r'<td align=right>(\d+)</td>\s*'
            r'<td align=right[^>]*>([^<]*)</td>',
            re.DOTALL
        )
        for m in cand_pattern.finditer(cand_html):
            name = m.group(1).strip()
            party = m.group(2).strip()
            votes = int(m.group(3))
            pct_str = m.group(4).strip().rstrip('%')
            try:
                pct = float(pct_str) if pct_str and pct_str != 'N/A' else None
            except ValueError:
                pct = None

            is_write_in = name.lower().startswith('write-in')

            candidates.append({
                'name': name,
                'party': party if party else None,
                'votes': votes,
                'pct': pct,
                'is_write_in': is_write_in,
            })

        races.append({
            'race_title': title,
            'chamber': chamber,
            'district': district_id,
            'primary_party_code': primary_code,
            'num_precincts': num_precincts,
            'precincts_reporting': precincts_reporting,
            'total_votes': total_votes,
            'candidates': candidates,
        })

    return races


def determine_election_type(is_primary, primary_code):
    """Map SoS primary party code to DB election_type."""
    if not is_primary:
        return 'General'
    if primary_code == 'R':
        return 'Primary_R'
    # ADL, D-C, C, DC → non-Republican primary
    return 'Primary'


def main():
    parser = argparse.ArgumentParser(description='Download AK SoS election results')
    parser.add_argument('--year', type=int, help='Single year to download')
    parser.add_argument('--dry-run', action='store_true', help='Show URLs only')
    args = parser.parse_args()

    elections = ELECTIONS
    if args.year:
        elections = [e for e in elections if e[0] == args.year]
        if not elections:
            print(f'No available data for {args.year}')
            sys.exit(1)

    if args.dry_run:
        for year, etype, code, _ in elections:
            url = f'{BASE_URL}/{code}/data/results.htm'
            print(f'  {year} {etype:8s} → {url}')
        return

    all_results = []
    for year, etype, code, _ in elections:
        url = f'{BASE_URL}/{code}/data/results.htm'
        is_primary = etype == 'primary'
        print(f'Fetching {year} {etype}... ', end='', flush=True)

        try:
            html = fetch_page(url)
        except Exception as e:
            print(f'ERROR: {e}')
            continue

        races = parse_races(html)
        print(f'{len(races)} state legislative races')

        for race in races:
            election_type = determine_election_type(is_primary, race['primary_party_code'])
            election_date = ELECTION_DATES.get((year, election_type))

            all_results.append({
                'year': year,
                'election_type': election_type,
                'election_date': election_date,
                'chamber': race['chamber'],
                'district': race['district'],
                'num_precincts': race['num_precincts'],
                'precincts_reporting': race['precincts_reporting'],
                'total_votes': race['total_votes'],
                'candidates': race['candidates'],
            })

        # Brief delay between requests
        time.sleep(0.5)

    # Summary
    from collections import Counter
    by_type = Counter((r['year'], r['election_type']) for r in all_results)
    print(f'\nTotal: {len(all_results)} races')
    for (yr, et), cnt in sorted(by_type.items()):
        print(f'  {yr} {et:15s} {cnt} races')

    # Save
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f'\nSaved to {OUTPUT_PATH}')


if __name__ == '__main__':
    main()
