"""
Download Wikipedia "List of {office} of {State}" pages for statewide offices.

Supports multiple office types with state-specific URL pattern fallbacks.
Saves HTML files to elections/tmp/ for parsing by parse_statewide_wiki.py.

Usage:
    python3 scripts/download_statewide_wiki.py --office ag                  # Attorney General
    python3 scripts/download_statewide_wiki.py --office lt_gov              # Lieutenant Governor
    python3 scripts/download_statewide_wiki.py --office sos                 # Secretary of State
    python3 scripts/download_statewide_wiki.py --office treasurer           # Treasurer
    python3 scripts/download_statewide_wiki.py --office ag --state CA       # Single state
    python3 scripts/download_statewide_wiki.py --office ag --no-cache       # Force re-download
"""
import os
import sys
import time
import argparse

import httpx

HTML_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'tmp')

STATE_NAMES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming'
}

# States with elected statewide officials by office
ELECTED_STATES = {
    'ag': sorted(set(STATE_NAMES.keys()) - {'AK', 'HI', 'ME', 'NH', 'NJ', 'TN', 'WY'}),
    'lt_gov': sorted(set(STATE_NAMES.keys()) - {'AZ', 'ME', 'NH', 'OR', 'WY', 'TN', 'WV'}),
    'sos': sorted(set(STATE_NAMES.keys()) - {
        'AK', 'HI', 'UT',  # N/A
        'DE', 'FL', 'MD', 'MN', 'NJ', 'NY', 'OK', 'PA', 'TN', 'TX', 'VA', 'WI',  # Appointed
    }),
    'treasurer': sorted(set(STATE_NAMES.keys()) - {
        'NY', 'TX',  # N/A
        'AK', 'GA', 'HI', 'KY', 'MD', 'MI', 'MN', 'NJ', 'TN', 'VA', 'WA', 'WI',  # Appointed
    }),
}

# Wikipedia URL patterns to try for each office type
# {state} is replaced with underscored state name (e.g., "New_York")
URL_PATTERNS = {
    'ag': [
        'https://en.wikipedia.org/wiki/{state}_Attorney_General',
        'https://en.wikipedia.org/wiki/Attorney_General_of_{state}',
        'https://en.wikipedia.org/wiki/List_of_attorneys_general_of_{state}',
    ],
    'lt_gov': [
        'https://en.wikipedia.org/wiki/Lieutenant_Governor_of_{state}',
        'https://en.wikipedia.org/wiki/List_of_lieutenant_governors_of_{state}',
        'https://en.wikipedia.org/wiki/{state}_Lieutenant_Governor',
    ],
    'sos': [
        'https://en.wikipedia.org/wiki/Secretary_of_State_of_{state}',
        'https://en.wikipedia.org/wiki/{state}_Secretary_of_State',
        'https://en.wikipedia.org/wiki/List_of_secretaries_of_state_of_{state}',
    ],
    'treasurer': [
        'https://en.wikipedia.org/wiki/{state}_State_Treasurer',
        'https://en.wikipedia.org/wiki/State_Treasurer_of_{state}',
        'https://en.wikipedia.org/wiki/List_of_treasurers_of_{state}',
        'https://en.wikipedia.org/wiki/{state}_Treasurer',
    ],
}

# State-specific URL overrides where standard patterns don't work
# Value is the Wikipedia page title (for REST API)
URL_OVERRIDES = {
    'ag_MO': 'List_of_attorneys_general_of_Missouri',
    'ag_ND': 'List_of_attorneys_general_of_North_Dakota',
}

OFFICE_LABELS = {
    'ag': 'Attorney General',
    'lt_gov': 'Lieutenant Governor',
    'sos': 'Secretary of State',
    'treasurer': 'Treasurer',
}

# Wikipedia REST API — requires descriptive User-Agent per API policy
HEADERS = {
    'User-Agent': 'ElectionsProject/1.0 (state elections tracker; contact: w85kramer@users.noreply.github.com)',
}

# Use REST API (main website returns 403 from server IPs)
API_BASE = 'https://en.wikipedia.org/api/rest_v1/page/html'


def file_name(office, state_abbr):
    """Generate the HTML filename for a given office and state."""
    state_name = STATE_NAMES[state_abbr]
    label = OFFICE_LABELS[office]
    return f'{label} of {state_name} - Wikipedia.html'


def download_page(title, retries=2):
    """Download a Wikipedia page via REST API. Returns (HTML text, title) or (None, None)."""
    url = f'{API_BASE}/{title}'
    for attempt in range(retries + 1):
        try:
            resp = httpx.get(
                url, headers=HEADERS, follow_redirects=True,
                timeout=httpx.Timeout(20.0, connect=10.0)
            )
            if resp.status_code == 200:
                return resp.text, title
            if resp.status_code == 404:
                return None, None  # Don't retry 404s
            print(f'    HTTP {resp.status_code} for {title}', flush=True)
        except Exception as e:
            print(f'    Error (attempt {attempt+1}): {e}', flush=True)
        if attempt < retries:
            time.sleep(2 * (attempt + 1))
    return None, None


def download_state(office, state_abbr, no_cache=False):
    """Download the Wikipedia page for a given office/state. Returns (path, status)."""
    fname = file_name(office, state_abbr)
    path = os.path.join(HTML_DIR, fname)
    state_name = STATE_NAMES[state_abbr]

    # Check cache
    if not no_cache and os.path.exists(path) and os.path.getsize(path) > 5000:
        return path, 'cached'

    # Check overrides
    override_key = f'{office}_{state_abbr}'
    if override_key in URL_OVERRIDES:
        titles = [URL_OVERRIDES[override_key]]
    else:
        state_wiki = state_name.replace(' ', '_')
        # Build Wikipedia page titles to try (REST API uses titles, not full URLs)
        titles = [p.format(state=state_wiki).split('/wiki/')[-1]
                  for p in URL_PATTERNS[office]]

    # Try each title pattern
    for title in titles:
        html, resolved = download_page(title)
        if html and len(html) > 5000:
            # Verify it's actually about this office (not a disambiguation page)
            if 'wikitable' in html.lower() or OFFICE_LABELS[office].lower() in html.lower():
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(html)
                return path, f'downloaded ({resolved})'

    return None, 'FAILED'


def main():
    parser = argparse.ArgumentParser(description='Download Wikipedia statewide officeholder pages')
    parser.add_argument('--office', required=True, choices=OFFICE_LABELS.keys(),
                        help='Office type to download')
    parser.add_argument('--state', type=str, help='Download single state (abbreviation)')
    parser.add_argument('--no-cache', action='store_true', help='Force re-download')
    parser.add_argument('--all-states', action='store_true',
                        help='Download all 50 states, not just elected')
    args = parser.parse_args()

    office = args.office
    label = OFFICE_LABELS[office]

    if args.state:
        states = [args.state.upper()]
    elif args.all_states:
        states = sorted(STATE_NAMES.keys())
    else:
        states = ELECTED_STATES[office]

    print(f'\n{"=" * 60}')
    print(f'Downloading {label} pages from Wikipedia')
    print(f'States: {len(states)}')
    print(f'{"=" * 60}\n')

    os.makedirs(HTML_DIR, exist_ok=True)

    results = {'cached': [], 'downloaded': [], 'failed': []}

    for i, abbr in enumerate(states):
        state_name = STATE_NAMES[abbr]
        print(f'  [{i+1}/{len(states)}] {abbr} ({state_name})...', end=' ', flush=True)

        path, status = download_state(office, abbr, no_cache=args.no_cache)

        if status == 'cached':
            results['cached'].append(abbr)
            print('cached', flush=True)
        elif status.startswith('downloaded'):
            results['downloaded'].append(abbr)
            print(status, flush=True)
            time.sleep(1)  # Rate limit
        else:
            results['failed'].append(abbr)
            print('FAILED', flush=True)

    # Summary
    print(f'\n{"=" * 60}')
    print(f'SUMMARY: {label}')
    print(f'{"=" * 60}')
    print(f'  Cached: {len(results["cached"])}')
    print(f'  Downloaded: {len(results["downloaded"])}')
    print(f'  Failed: {len(results["failed"])}')
    if results['failed']:
        print(f'  Failed states: {", ".join(results["failed"])}')
        print(f'\n  For failed states, manually save the Wikipedia page to:')
        for abbr in results['failed']:
            print(f'    {HTML_DIR}/{file_name(office, abbr)}')


if __name__ == '__main__':
    main()
