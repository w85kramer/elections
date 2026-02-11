"""
Download 2025 general election results for Virginia and New Jersey from Ballotpedia.

Downloads individual district pages for VA House of Delegates and NJ Assembly,
plus statewide pages for Governor, Lt. Governor, and Attorney General.
Parses results_table votebox HTML and outputs JSON for populate_2025_results.py.

Usage:
    python3 scripts/download_2025_results.py
    python3 scripts/download_2025_results.py --state VA
    python3 scripts/download_2025_results.py --state NJ
"""
import sys
import os
import re
import json
import time
import argparse
import html as htmlmod

import httpx

CACHE_DIR = '/tmp/bp_2025'
OUTPUT_PATH = '/tmp/2025_election_results.json'

PARTY_MAP = {
    'D': 'D', 'R': 'R', 'I': 'I', 'L': 'L', 'G': 'G',
}


def download_page(url, cache_key):
    """Download a Ballotpedia page with caching."""
    cache_path = os.path.join(CACHE_DIR, f'{cache_key}.html')
    if os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            return f.read()

    try:
        resp = httpx.get(
            url,
            headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'},
            follow_redirects=True,
            timeout=30
        )
        if resp.status_code != 200:
            print(f'    WARNING: HTTP {resp.status_code} for {url}')
            return None
        with open(cache_path, 'w', encoding='utf-8') as f:
            f.write(resp.text)
        return resp.text
    except Exception as e:
        print(f'    WARNING: Download failed for {url}: {e}')
        return None


def parse_results_table(html_text, election_label, max_winners=1):
    """
    Parse a results_table votebox from Ballotpedia HTML.

    Args:
        html_text: Full HTML of the page
        election_label: Text to find the correct results section (e.g.,
            "general election for Virginia House of Delegates District 1 on November 4, 2025")
        max_winners: Expected number of winners (2 for NJ Assembly multi-member)

    Returns:
        dict with 'candidates' list and 'total_votes', or None if not found
    """
    # Find the section matching election_label
    idx = html_text.find(election_label)
    if idx == -1:
        # Try case-insensitive
        idx_lower = html_text.lower().find(election_label.lower())
        if idx_lower == -1:
            return None
        idx = idx_lower

    # Find the results_table after this label
    rt_idx = html_text.find('results_table', idx)
    if rt_idx == -1 or rt_idx > idx + 2000:
        return None

    # Find end of table
    table_end = html_text.find('</table>', rt_idx)
    if table_end == -1:
        return None

    table_html = html_text[rt_idx:table_end + 10]

    candidates = []

    # Parse candidate rows
    for row_match in re.finditer(
        r'<tr\s+class="results_row\s*(winner)?\s*"[^>]*>(.*?)</tr>',
        table_html, re.DOTALL
    ):
        is_winner = row_match.group(1) == 'winner'
        row_html = row_match.group(2)

        # Get candidate name cell
        name_cell = re.search(
            r'class="votebox-results-cell--text"[^>]*>(.*?)</td>',
            row_html, re.DOTALL
        )
        if not name_cell:
            continue

        cell_html = name_cell.group(1)

        # Check for "Other/Write-in votes" row (skip it)
        if 'Other/Write-in' in cell_html or 'Write-in' in cell_html:
            continue

        # Check for incumbent (bold+underline around the link)
        is_incumbent = bool(re.search(r'<b><u><a', cell_html))

        # Extract name from link
        name_link = re.search(r'<a\s+href="[^"]*"[^>]*>(.*?)</a>', cell_html)
        if not name_link:
            continue
        name = htmlmod.unescape(name_link.group(1).strip())

        # Extract party from "(D)", "(R)", etc.
        party_match = re.search(r'\(([A-Z])\)', cell_html)
        party = party_match.group(1) if party_match else None
        if party and party in PARTY_MAP:
            party = PARTY_MAP[party]

        # Extract vote percentage
        pct_match = re.search(r'class="percentage_number">([\d.]+)</div>', row_html)
        vote_pct = float(pct_match.group(1)) if pct_match else None

        # Extract vote count (last votebox-results-cell--number)
        votes_matches = re.findall(
            r'class="votebox-results-cell--number">([\d,]+)</td>',
            row_html
        )
        votes = int(votes_matches[-1].replace(',', '')) if votes_matches else None

        candidates.append({
            'name': name,
            'party': party,
            'is_incumbent': is_incumbent,
            'is_winner': is_winner,
            'votes': votes,
            'vote_pct': vote_pct,
        })

    # Extract total votes (search past end of table; some tables are very large)
    total_match = re.search(
        r'Total\s+votes:\s*([\d,]+)',
        html_text[rt_idx:table_end + 2000]
    )
    total_votes = int(total_match.group(1).replace(',', '')) if total_match else None

    if not candidates:
        return None

    return {
        'candidates': candidates,
        'total_votes': total_votes,
    }


def process_va_hod():
    """Download and parse all 100 VA House of Delegates districts."""
    print("\n=== VA House of Delegates ===")
    results = []

    for dist in range(1, 101):
        url = f'https://ballotpedia.org/Virginia_House_of_Delegates_District_{dist}'
        cache_key = f'va_hod_{dist}'

        html = download_page(url, cache_key)
        if not html:
            print(f'  District {dist}: FAILED to download')
            continue

        label = f'general election for Virginia House of Delegates District {dist} on November 4, 2025'
        parsed = parse_results_table(html, label)

        if not parsed:
            # Try alternate label format
            label2 = f'General election for Virginia House of Delegates District {dist}'
            parsed = parse_results_table(html, label2)

        if not parsed:
            print(f'  District {dist}: No results found')
            continue

        result = {
            'state': 'VA',
            'chamber': 'House of Delegates',
            'district_number': str(dist),
            'office_type': 'State House',
            'total_votes': parsed['total_votes'],
            'candidates': parsed['candidates'],
        }
        results.append(result)

        winner_names = [c['name'] for c in parsed['candidates'] if c['is_winner']]
        if dist % 20 == 1 or dist <= 3:
            print(f'  District {dist}: {len(parsed["candidates"])} candidates, '
                  f'winner: {", ".join(winner_names)}, '
                  f'total: {parsed["total_votes"]}')

        time.sleep(0.5)

    print(f'  Total: {len(results)} districts parsed')
    return results


def process_nj_assembly():
    """Download and parse all 40 NJ Assembly districts (2 seats each)."""
    print("\n=== NJ Assembly ===")
    results = []

    for dist in range(1, 41):
        url = f'https://ballotpedia.org/New_Jersey_General_Assembly_District_{dist}'
        cache_key = f'nj_asm_{dist}'

        html = download_page(url, cache_key)
        if not html:
            print(f'  District {dist}: FAILED to download')
            continue

        label = f'general election for New Jersey General Assembly District {dist} on November 4, 2025'
        parsed = parse_results_table(html, label, max_winners=2)

        if not parsed:
            label2 = f'General election for New Jersey General Assembly District {dist}'
            parsed = parse_results_table(html, label2, max_winners=2)

        if not parsed:
            print(f'  District {dist}: No results found')
            continue

        result = {
            'state': 'NJ',
            'chamber': 'Assembly',
            'district_number': str(dist),
            'office_type': 'State House',
            'total_votes': parsed['total_votes'],
            'candidates': parsed['candidates'],
        }
        results.append(result)

        winners = [c['name'] for c in parsed['candidates'] if c['is_winner']]
        if dist % 10 == 1 or dist <= 3:
            print(f'  District {dist}: {len(parsed["candidates"])} candidates, '
                  f'winners: {", ".join(winners)}, '
                  f'total: {parsed["total_votes"]}')

        time.sleep(0.5)

    print(f'  Total: {len(results)} districts parsed')
    return results


def process_va_statewide():
    """Download and parse VA statewide races (Gov, Lt Gov, AG)."""
    print("\n=== VA Statewide ===")
    results = []

    pages = [
        ('Virginia_gubernatorial_election,_2025', 'Governor',
         'general election for Governor of Virginia'),
        ('Virginia_lieutenant_gubernatorial_election,_2025', 'Lt. Governor',
         'general election for Lieutenant Governor of Virginia'),
        ('Virginia_Attorney_General_election,_2025', 'Attorney General',
         'general election for Attorney General of Virginia'),
    ]

    for page_name, office_type, label in pages:
        url = f'https://ballotpedia.org/{page_name}'
        cache_key = f'va_sw_{office_type.replace(" ", "_").replace(".", "").lower()}'

        html = download_page(url, cache_key)
        if not html:
            print(f'  {office_type}: FAILED to download')
            continue

        parsed = parse_results_table(html, label)
        if not parsed:
            # Try with capital G
            parsed = parse_results_table(html, label.replace('general', 'General'))
        if not parsed:
            print(f'  {office_type}: No results found')
            continue

        result = {
            'state': 'VA',
            'chamber': 'Statewide',
            'district_number': 'Statewide',
            'office_type': office_type,
            'total_votes': parsed['total_votes'],
            'candidates': parsed['candidates'],
        }
        results.append(result)

        winner = [c['name'] for c in parsed['candidates'] if c['is_winner']]
        print(f'  {office_type}: {len(parsed["candidates"])} candidates, '
              f'winner: {", ".join(winner)}, total: {parsed["total_votes"]}')

        time.sleep(0.5)

    return results


def process_nj_statewide():
    """Download and parse NJ statewide races (Gov + Lt Gov joint page)."""
    print("\n=== NJ Statewide ===")
    results = []

    url = 'https://ballotpedia.org/New_Jersey_gubernatorial_and_lieutenant_gubernatorial_election,_2025'
    cache_key = 'nj_sw_gov_ltgov'

    html = download_page(url, cache_key)
    if not html:
        print('  FAILED to download NJ Gov page')
        return results

    # Governor
    label = 'General election for Governor of New Jersey'
    parsed = parse_results_table(html, label)
    if not parsed:
        parsed = parse_results_table(html, label.replace('General', 'general'))
    if parsed:
        result = {
            'state': 'NJ',
            'chamber': 'Statewide',
            'district_number': 'Statewide',
            'office_type': 'Governor',
            'total_votes': parsed['total_votes'],
            'candidates': parsed['candidates'],
        }
        results.append(result)
        winner = [c['name'] for c in parsed['candidates'] if c['is_winner']]
        print(f'  Governor: {len(parsed["candidates"])} candidates, '
              f'winner: {", ".join(winner)}, total: {parsed["total_votes"]}')
    else:
        print('  Governor: No results found')

    time.sleep(0.3)

    # Lt. Governor
    label = 'General election for Lieutenant Governor of New Jersey'
    parsed = parse_results_table(html, label)
    if not parsed:
        parsed = parse_results_table(html, label.replace('General', 'general'))
    if parsed:
        result = {
            'state': 'NJ',
            'chamber': 'Statewide',
            'district_number': 'Statewide',
            'office_type': 'Lt. Governor',
            'total_votes': parsed['total_votes'],
            'candidates': parsed['candidates'],
        }
        results.append(result)
        winner = [c['name'] for c in parsed['candidates'] if c['is_winner']]
        print(f'  Lt. Governor: {len(parsed["candidates"])} candidates, '
              f'winner: {", ".join(winner)}, total: {parsed["total_votes"]}')
    else:
        print('  Lt. Governor: No results found')

    return results


def main():
    parser = argparse.ArgumentParser(description='Download 2025 election results from Ballotpedia')
    parser.add_argument('--state', type=str, choices=['VA', 'NJ'],
                        help='Process a single state')
    parser.add_argument('--output', type=str, default=OUTPUT_PATH,
                        help='Output JSON path')
    args = parser.parse_args()

    os.makedirs(CACHE_DIR, exist_ok=True)

    all_results = []

    if not args.state or args.state == 'VA':
        all_results.extend(process_va_hod())
        all_results.extend(process_va_statewide())

    if not args.state or args.state == 'NJ':
        all_results.extend(process_nj_assembly())
        all_results.extend(process_nj_statewide())

    # Summary
    print(f'\n{"=" * 60}')
    print(f'SUMMARY')
    print(f'{"=" * 60}')
    print(f'Total race records: {len(all_results)}')

    from collections import Counter
    by_state = Counter(r['state'] for r in all_results)
    for state, count in sorted(by_state.items()):
        print(f'  {state}: {count}')

    by_type = Counter((r['state'], r['office_type']) for r in all_results)
    for (state, otype), count in sorted(by_type.items()):
        print(f'  {state} {otype}: {count}')

    # Count total candidates and winners
    total_candidates = sum(len(r['candidates']) for r in all_results)
    total_winners = sum(sum(1 for c in r['candidates'] if c['is_winner']) for r in all_results)
    print(f'Total candidates: {total_candidates}')
    print(f'Total winners: {total_winners}')

    # Check for races without winners
    no_winner = [r for r in all_results if not any(c['is_winner'] for c in r['candidates'])]
    if no_winner:
        print(f'\nWARNING: {len(no_winner)} races without a winner:')
        for r in no_winner:
            print(f'  {r["state"]} {r["office_type"]} District {r["district_number"]}')

    # Write output
    with open(args.output, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f'\nWritten to {args.output}')


if __name__ == '__main__':
    main()
