"""
Research seat gap details by fetching individual Ballotpedia district pages.

For each identified gap (filled vacancy, true replacement, new vacancy),
fetches the BP district page and extracts:
  - Current officeholder details
  - Previous officeholder / vacancy reason
  - How current holder was installed (appointment, special election, regular election)

Input: /tmp/seat_gaps_report.json (from audit_seat_gaps.py)
Output: /tmp/seat_gap_details.json

Usage:
    python3 scripts/research_seat_gaps.py
    python3 scripts/research_seat_gaps.py --state VA
    python3 scripts/research_seat_gaps.py --category filled_vacancy
"""
import sys
import os
import re
import json
import time
import argparse
import html as htmlmod

import httpx
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

CACHE_DIR = '/tmp/bp_districts'
INPUT_PATH = '/tmp/seat_gaps_report.json'
OUTPUT_PATH = '/tmp/seat_gap_details.json'

# ══════════════════════════════════════════════════════════════════════
# BP URL BUILDING
# ══════════════════════════════════════════════════════════════════════

STATE_NAMES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New_Hampshire', 'NJ': 'New_Jersey', 'NM': 'New_Mexico', 'NY': 'New_York',
    'NC': 'North_Carolina', 'ND': 'North_Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode_Island', 'SC': 'South_Carolina',
    'SD': 'South_Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West_Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming',
}

CHAMBER_URL = {
    'Senate': 'State_Senate',
    'House': 'House_of_Representatives',
    'Assembly': 'State_Assembly',
    'House of Delegates': 'House_of_Delegates',
    'Legislature': 'Legislature',
}

# State-specific chamber URL overrides
CHAMBER_URL_OVERRIDES = {
    ('NJ', 'Assembly'): 'General_Assembly',
    ('NE', 'Legislature'): 'Legislature',
}

def build_bp_district_url(state, chamber, district):
    """Build the BP URL for an individual district page."""
    state_name = STATE_NAMES[state]

    # Handle NE unicameral
    if state == 'NE':
        return f'https://ballotpedia.org/Nebraska_State_Senate_District_{district}'

    # Chamber name in URL
    chamber_key = (state, chamber)
    if chamber_key in CHAMBER_URL_OVERRIDES:
        chamber_part = CHAMBER_URL_OVERRIDES[chamber_key]
    else:
        chamber_part = CHAMBER_URL.get(chamber, chamber.replace(' ', '_'))

    # District part — for named districts (MA, NH, VT), this gets complex
    # For now, handle numeric districts
    district_part = district

    return f'https://ballotpedia.org/{state_name}_{chamber_part}_District_{district_part}'

# ══════════════════════════════════════════════════════════════════════
# PAGE FETCHING
# ══════════════════════════════════════════════════════════════════════

def fetch_district_page(state, chamber, district, use_cache=True):
    """Fetch a BP district page."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_key = f'{state}_{chamber}_{district}'.replace(' ', '_').replace('/', '_')
    cache_file = os.path.join(CACHE_DIR, f'{cache_key}.html')

    if use_cache and os.path.exists(cache_file):
        with open(cache_file, 'r', encoding='utf-8') as f:
            return f.read()

    url = build_bp_district_url(state, chamber, district)
    print(f'  Fetching {url}')

    try:
        resp = httpx.get(url, follow_redirects=True, timeout=30,
                         headers={'User-Agent': 'Mozilla/5.0 (compatible; ElectionsBot/1.0)'})
        if resp.status_code == 202:
            print(f'    Got 202, retrying in 5s...')
            time.sleep(5)
            resp = httpx.get(url, follow_redirects=True, timeout=30,
                             headers={'User-Agent': 'Mozilla/5.0 (compatible; ElectionsBot/1.0)'})
        if resp.status_code == 404:
            print(f'    404 Not Found')
            return None
        resp.raise_for_status()
    except Exception as e:
        print(f'    Error: {e}')
        return None

    html = resp.text
    with open(cache_file, 'w', encoding='utf-8') as f:
        f.write(html)

    time.sleep(1.5)
    return html

def strip_html(text):
    """Remove HTML tags and decode entities."""
    text = re.sub(r'<[^>]+>', '', text)
    text = htmlmod.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# ══════════════════════════════════════════════════════════════════════
# PAGE PARSING
# ══════════════════════════════════════════════════════════════════════

def parse_district_page(html, state, chamber, district):
    """
    Parse a BP district page for officeholder history.

    Returns dict with:
      current_holder, party, assumed_office,
      predecessor, predecessor_party, vacancy_reason,
      installation_method (appointed/elected/special_election)
    """
    if not html:
        return None

    result = {
        'current_holder': None,
        'party': None,
        'assumed_office': None,
        'predecessor': None,
        'predecessor_party': None,
        'vacancy_reason': None,
        'installation_method': None,
        'notes': '',
    }

    # Look for "appointed" text near current holder
    appointed_patterns = [
        r'appointed.*?to\s+(?:the\s+)?(?:seat|office|district)',
        r'was\s+appointed\s+(?:to|by)',
        r'Governor.*?appointed',
        r'appointed\s+by\s+(?:the\s+)?(?:Governor|governor)',
    ]
    for pat in appointed_patterns:
        if re.search(pat, html, re.IGNORECASE):
            result['installation_method'] = 'appointed'
            # Try to extract who appointed
            m = re.search(r'appointed\s+by\s+(?:the\s+)?(?:Governor\s+)?([A-Z][a-z]+\s+[A-Z][a-z]+)', html)
            if m:
                result['notes'] += f'Appointed by {m.group(1)}. '
            break

    # Look for special election mentions
    special_patterns = [
        r'special\s+election',
        r'won\s+(?:a\s+)?special',
    ]
    for pat in special_patterns:
        if re.search(pat, html, re.IGNORECASE):
            if not result['installation_method']:
                result['installation_method'] = 'special_election'
            break

    # Look for vacancy reason
    reason_patterns = [
        (r'(?:resigned|resignation)', 'resigned'),
        (r'(?:died|death|passed away)', 'died'),
        (r'(?:expelled|expulsion|removed)', 'removed'),
        (r'(?:appointed to|took a position|left to serve)', 'appointed_elsewhere'),
        (r'(?:elected to|won election to)\s+(?:the\s+)?(?:Senate|House|Congress|U\.S\.)', 'appointed_elsewhere'),
        (r'(?:term.?limited|term limit)', 'term_expired'),
    ]
    for pat, reason in reason_patterns:
        if re.search(pat, html, re.IGNORECASE):
            if not result['vacancy_reason']:
                result['vacancy_reason'] = reason
            break

    # Try to extract current holder from the "officeholder" infobox
    # BP uses various formats; look for key patterns
    holder_match = re.search(
        r'(?:Current\s+officeholder|Incumbent)[^<]*?<[^>]*?>([^<]+)</a>',
        html, re.IGNORECASE
    )
    if holder_match:
        result['current_holder'] = strip_html(holder_match.group(1))

    return result

# ══════════════════════════════════════════════════════════════════════
# DB QUERIES
# ══════════════════════════════════════════════════════════════════════

def run_query(sql):
    """Execute SQL via Supabase Management API."""
    resp = httpx.post(
        f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
        json={'query': sql},
        timeout=30,
    )
    if resp.status_code == 429:
        time.sleep(5)
        return run_query(sql)
    resp.raise_for_status()
    return resp.json()

# ══════════════════════════════════════════════════════════════════════
# RESEARCH LOGIC
# ══════════════════════════════════════════════════════════════════════

# Known nickname/name-change cases (NOT real replacements)
# These are flagged as "true replacements" by the audit but are actually the same person
SAME_PERSON_OVERRIDES = {
    # (state, chamber, district): 'reason'
    ('CO', 'Senate', '11'): 'nickname',       # Thomas "Tony" Exum
    ('MS', 'House', '113'): 'nickname',        # Henry "Hank" Zuber
    ('NC', 'Senate', '4'): 'nickname',         # Eldon "Buck" Newton
    ('OH', 'Senate', '8'): 'nickname',         # Louis "Bill" Blessing
    ('VA', 'House of Delegates', '6'): 'nickname',  # R.C. "Rip" Sullivan
    ('ME', 'House', '146'): 'family',          # Walter Runte Jr. / Gerry Runte (likely family)
    ('WV', 'House of Delegates', '47'): 'nickname',  # Denny "Ray" Canterbury
}

# Known married name changes
NAME_CHANGES = {
    ('AK', 'House', '40'): 'married_name',     # Robyn Burke née Frier
    ('MA', 'House', '23'): 'married_name',      # Brandy Fluker Oakley née Fluker-Reid
    ('VA', 'House of Delegates', '36'): 'married_name',  # Ellen Campbell née McLaughlin
    ('NM', 'House', '30'): 'nickname',          # Elizabeth/Diane Torres-Velasquez (same person?)
}

# Known stale OpenStates data (our DB is out of date, not a real gap)
STALE_DATA = {
    ('WA', 'House', '26'): 'stale_openstates',
    ('WA', 'House', '33'): 'stale_openstates',
    ('WA', 'House', '34'): 'stale_openstates',
    ('WA', 'House', '41'): 'stale_openstates',
    ('NH', 'House', 'Coos-5'): 'stale_openstates',
}

def classify_replacement(gap_item):
    """Classify a name mismatch as nickname, name_change, stale, or real_replacement."""
    key = (gap_item['state'], gap_item['chamber'], gap_item['district'])

    if key in SAME_PERSON_OVERRIDES:
        return SAME_PERSON_OVERRIDES[key]
    if key in NAME_CHANGES:
        return NAME_CHANGES[key]
    if key in STALE_DATA:
        return STALE_DATA[key]

    return 'real_replacement'

def research_gap(gap_type, item, use_cache=True):
    """
    Research a single gap item.

    Returns a detail dict ready for /tmp/seat_gap_details.json
    """
    state = item['state']
    chamber = item['chamber']
    district = item['district']

    detail = {
        'gap_type': gap_type,
        'state': state,
        'chamber': chamber,
        'district': district,
        'seat_id': item.get('seat_id'),
        'seat_label': item.get('seat_label', f'{state} {chamber} {district}'),
        'seat_term_id': item.get('seat_term_id'),
    }

    if gap_type == 'filled_vacancy':
        detail['new_holder'] = item['bp_name']
        detail['new_holder_party'] = item['bp_party']
        detail['assumed_office'] = item['assumed_office']
        detail['action'] = 'create_seat_term'

        # Determine installation method from date
        assumed = item.get('assumed_office', '')
        if 'January' in assumed and ('2025' in assumed or '2023' in assumed or '2026' in assumed):
            # January of odd year or early even year — likely regular election or appointment
            # Check if assumed date is standard inauguration
            detail['start_reason'] = 'appointed'  # default, will verify
        else:
            detail['start_reason'] = 'appointed'

    elif gap_type == 'vacancy_new':
        detail['former_holder'] = item.get('db_holder', '')
        detail['former_party'] = item.get('db_party', '')
        detail['has_special'] = item.get('has_special_election', False)
        detail['action'] = 'close_seat_term'
        detail['end_reason'] = None  # need to research

    elif gap_type == 'name_mismatch':
        classification = classify_replacement(item)
        detail['classification'] = classification
        detail['bp_name'] = item.get('bp_name', '')
        detail['db_holder'] = item.get('db_holder', '')
        detail['assumed_office'] = item.get('assumed_office', '')

        if classification in ('nickname', 'family'):
            detail['action'] = 'update_name'  # Just fix the name in DB
        elif classification == 'married_name':
            detail['action'] = 'update_name'
        elif classification == 'stale_openstates':
            detail['action'] = 'update_holder'  # Replace with correct person
        else:
            detail['action'] = 'replace_holder'  # Close old term, create new

    # Fetch BP district page for additional context
    if gap_type in ('filled_vacancy', 'vacancy_new') or \
       (gap_type == 'name_mismatch' and detail.get('classification') == 'real_replacement'):
        html = fetch_district_page(state, chamber, district, use_cache=use_cache)
        if html:
            parsed = parse_district_page(html, state, chamber, district)
            if parsed:
                if parsed['installation_method']:
                    detail['start_reason'] = parsed['installation_method']
                if parsed['vacancy_reason']:
                    detail['end_reason'] = parsed['vacancy_reason']
                if parsed['notes']:
                    detail['notes'] = parsed['notes']

    return detail

# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Research seat gap details')
    parser.add_argument('--state', help='Filter to single state')
    parser.add_argument('--category', help='Only research one category: filled_vacancy, vacancy_new, name_mismatch')
    parser.add_argument('--no-cache', action='store_true', help='Force re-download')
    args = parser.parse_args()

    use_cache = not args.no_cache

    if not os.path.exists(INPUT_PATH):
        print(f'ERROR: {INPUT_PATH} not found. Run audit_seat_gaps.py first.')
        sys.exit(1)

    with open(INPUT_PATH) as f:
        report = json.load(f)

    details = []

    # 1. Filled vacancies
    if not args.category or args.category == 'filled_vacancy':
        items = report['filled_vacancy']
        if args.state:
            items = [i for i in items if i['state'] == args.state.upper()]
        print(f'\n═══ Researching {len(items)} filled vacancies ═══')
        for item in items:
            print(f'  {item["state"]} {item["chamber"]} {item["district"]}: {item["bp_name"]}')
            detail = research_gap('filled_vacancy', item, use_cache)
            details.append(detail)

    # 2. New vacancies
    if not args.category or args.category == 'vacancy_new':
        items = report['vacancy_new']
        if args.state:
            items = [i for i in items if i['state'] == args.state.upper()]
        print(f'\n═══ Researching {len(items)} new vacancies ═══')
        for item in items:
            print(f'  {item["state"]} {item["chamber"]} {item["district"]}: was {item.get("db_holder", "?")}')
            detail = research_gap('vacancy_new', item, use_cache)
            details.append(detail)

    # 3. True replacements (from name mismatches with different last names)
    if not args.category or args.category == 'name_mismatch':
        items = []
        for r in report['name_mismatch']:
            bp = r.get('bp_name', '')
            db = r.get('db_holder', '')
            if not bp or not db or bp == '(not in BP)' or db == '(no unmatched DB seat)':
                continue
            bp_last = bp.split()[-1] if bp else ''
            db_last = db.split()[-1] if db else ''
            if bp_last != db_last:
                items.append(r)
        if args.state:
            items = [i for i in items if i['state'] == args.state.upper()]
        print(f'\n═══ Researching {len(items)} name mismatches ═══')
        for item in items:
            classification = classify_replacement(item)
            print(f'  {item["state"]} {item["chamber"]} {item["district"]}: '
                  f'BP="{item["bp_name"]}" vs DB="{item["db_holder"]}" [{classification}]')
            detail = research_gap('name_mismatch', item, use_cache)
            details.append(detail)

    # Merge with existing details if processing single state/category
    if args.state or args.category:
        if os.path.exists(OUTPUT_PATH):
            with open(OUTPUT_PATH) as f:
                existing = json.load(f)
            # Remove entries we're replacing
            filter_keys = set()
            for d in details:
                filter_keys.add((d['state'], d['chamber'], d['district'], d['gap_type']))
            existing = [e for e in existing
                       if (e['state'], e['chamber'], e['district'], e['gap_type']) not in filter_keys]
            existing.extend(details)
            details = existing

    # Save
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(details, f, indent=2, default=str)

    # Summary
    print(f'\n{"═"*60}')
    print(f'RESEARCH SUMMARY')
    print(f'{"═"*60}')
    by_action = {}
    for d in details:
        action = d.get('action', 'unknown')
        by_action[action] = by_action.get(action, 0) + 1
    for action, count in sorted(by_action.items()):
        print(f'  {action}: {count}')
    print(f'  Total: {len(details)}')
    print(f'\nOutput: {OUTPUT_PATH}')

if __name__ == '__main__':
    main()
