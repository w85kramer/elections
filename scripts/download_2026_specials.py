"""
Download 2026 special election data from Ballotpedia.

Downloads individual district pages for all 58 state legislative special elections
in 2026, parses votebox HTML for completed elections, and outputs JSON for
populate_2026_specials.py.

Unlike the 2025 version (all completed), 2026 is a mix:
  Cat 1: General held (~20 races)
  Cat 2: Primary held, general upcoming (~14 races)
  Cat 3: Nothing held yet (~24 races)

Usage:
    python3 scripts/download_2026_specials.py
    python3 scripts/download_2026_specials.py --state VA
    python3 scripts/download_2026_specials.py --state LA
"""
import sys
import os
import re
import json
import time
import argparse
import html as htmlmod

import httpx

CACHE_DIR = '/tmp/bp_2026_specials'
OUTPUT_PATH = '/tmp/2026_special_results.json'

PARTY_MAP = {
    'D': 'D', 'R': 'R', 'I': 'I', 'L': 'L', 'G': 'G',
}

# ══════════════════════════════════════════════════════════════════════
# COMPLETE LIST OF 2026 SPECIAL ELECTIONS (58 races across 22 states)
# ══════════════════════════════════════════════════════════════════════
# category:
#   1 = general already held
#   2 = primary held, general upcoming
#   3 = nothing held yet
#   'overlap' = overlaps existing 2026 General (don't create new election)
#
# For LA, the "primary" is actually a jungle primary = our Special election.
# primary_date for LA = jungle primary date; general_date = runoff date.

SPECIAL_ELECTIONS = [
    # ── ALABAMA ──
    {'state': 'AL', 'chamber': 'House', 'district': '63', 'bp_district': '63',
     'former_incumbent': 'Cynthia Almond', 'vacancy_reason': 'resigned',
     'general_date': '2026-01-13', 'primary_date': '2025-09-30', 'runoff_date': None,
     'category': 1},
    {'state': 'AL', 'chamber': 'House', 'district': '38', 'bp_district': '38',
     'former_incumbent': 'Debbie Hamby Wood', 'vacancy_reason': 'resigned',
     'general_date': '2026-02-03', 'primary_date': '2025-10-21', 'runoff_date': None,
     'category': 1},

    # ── ARKANSAS ──
    {'state': 'AR', 'chamber': 'Senate', 'district': '26', 'bp_district': '26',
     'former_incumbent': 'Gary Stubblefield', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-03', 'primary_date': '2026-01-06', 'runoff_date': None,
     'category': 2},
    {'state': 'AR', 'chamber': 'House', 'district': '70', 'bp_district': '70',
     'former_incumbent': 'Carlton Wing', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-03', 'primary_date': '2026-01-06', 'runoff_date': None,
     'category': 2},

    # ── COLORADO ──
    {'state': 'CO', 'chamber': 'Senate', 'district': '17', 'bp_district': '17',
     'former_incumbent': 'Sonya Jaquez Lewis', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-11-03', 'primary_date': '2026-06-30', 'runoff_date': None,
     'category': 3},
    {'state': 'CO', 'chamber': 'Senate', 'district': '29', 'bp_district': '29',
     'former_incumbent': 'Janet Buckner', 'vacancy_reason': 'resigned',
     'general_date': '2026-11-03', 'primary_date': '2026-06-30', 'runoff_date': None,
     'category': 3},
    {'state': 'CO', 'chamber': 'Senate', 'district': '31', 'bp_district': '31',
     'former_incumbent': 'Chris Hansen', 'vacancy_reason': 'resigned',
     'general_date': '2026-11-03', 'primary_date': '2026-06-30', 'runoff_date': None,
     'category': 3},

    # ── CONNECTICUT ──
    {'state': 'CT', 'chamber': 'House', 'district': '25', 'bp_district': '25',
     'former_incumbent': 'Bobby Sanchez', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-01-06', 'primary_date': None, 'runoff_date': None,
     'category': 1},
    {'state': 'CT', 'chamber': 'House', 'district': '139', 'bp_district': '139',
     'former_incumbent': 'Kevin Ryan', 'vacancy_reason': 'resigned',
     'general_date': '2026-01-13', 'primary_date': None, 'runoff_date': None,
     'category': 1},

    # ── FLORIDA ──
    {'state': 'FL', 'chamber': 'House', 'district': '87', 'bp_district': '87',
     'former_incumbent': 'Mike Caruso', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-03-24', 'primary_date': '2026-01-13', 'runoff_date': None,
     'category': 2},
    {'state': 'FL', 'chamber': 'House', 'district': '52', 'bp_district': '52',
     'former_incumbent': 'John Temple', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-24', 'primary_date': '2026-01-13', 'runoff_date': None,
     'category': 2},
    {'state': 'FL', 'chamber': 'House', 'district': '51', 'bp_district': '51',
     'former_incumbent': 'Josie Tomkow', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-24', 'primary_date': '2026-01-13', 'runoff_date': None,
     'category': 2},
    {'state': 'FL', 'chamber': 'Senate', 'district': '14', 'bp_district': '14',
     'former_incumbent': 'Jay Collins', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-03-24', 'primary_date': '2026-01-13', 'runoff_date': None,
     'category': 2},

    # ── GEORGIA ──
    {'state': 'GA', 'chamber': 'Senate', 'district': '18', 'bp_district': '18',
     'former_incumbent': 'John F. Kennedy', 'vacancy_reason': 'resigned',
     'general_date': '2026-01-20', 'primary_date': None, 'runoff_date': '2026-02-17',
     'category': 1},
    {'state': 'GA', 'chamber': 'House', 'district': '94', 'bp_district': '94',
     'former_incumbent': 'Karen Bennett', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-10', 'primary_date': None, 'runoff_date': None,
     'category': 3},
    {'state': 'GA', 'chamber': 'House', 'district': '130', 'bp_district': '130',
     'former_incumbent': 'Lynn Heffner', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-10', 'primary_date': None, 'runoff_date': None,
     'category': 3},
    {'state': 'GA', 'chamber': 'Senate', 'district': '53', 'bp_district': '53',
     'former_incumbent': 'Colton Moore', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-10', 'primary_date': None, 'runoff_date': None,
     'category': 3},

    # ── LOUISIANA ──
    # LA jungle primary: the "primary" (Feb 7) is our Special election.
    # If >50% winner, done. If not, runoff (Mar 14) = Special_Runoff.
    {'state': 'LA', 'chamber': 'Senate', 'district': '3', 'bp_district': '3',
     'former_incumbent': 'Joseph Bouie', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-14', 'primary_date': '2026-02-07', 'runoff_date': None,
     'category': 2},
    {'state': 'LA', 'chamber': 'House', 'district': '37', 'bp_district': '37',
     'former_incumbent': 'Troy Romero', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-14', 'primary_date': '2026-02-07', 'runoff_date': None,
     'category': 2},
    {'state': 'LA', 'chamber': 'House', 'district': '60', 'bp_district': '60',
     'former_incumbent': 'Chad Brown', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-14', 'primary_date': '2026-02-07', 'runoff_date': None,
     'category': 2},
    {'state': 'LA', 'chamber': 'House', 'district': '97', 'bp_district': '97',
     'former_incumbent': 'Matthew Willard', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-03-14', 'primary_date': '2026-02-07', 'runoff_date': None,
     'category': 2},
    {'state': 'LA', 'chamber': 'House', 'district': '100', 'bp_district': '100',
     'former_incumbent': 'Jason Hughes', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-14', 'primary_date': '2026-02-07', 'runoff_date': None,
     'category': 2},
    {'state': 'LA', 'chamber': 'House', 'district': '69', 'bp_district': '69',
     'former_incumbent': 'Paula Davis', 'vacancy_reason': 'resigned',
     'general_date': '2026-04-18', 'primary_date': '2026-03-14', 'runoff_date': None,
     'category': 3},

    # ── MAINE ──
    {'state': 'ME', 'chamber': 'House', 'district': '94', 'bp_district': '94',
     'former_incumbent': 'Kristen Cloutier', 'vacancy_reason': 'resigned',
     'general_date': '2026-02-24', 'primary_date': None, 'runoff_date': None,
     'category': 3},
    {'state': 'ME', 'chamber': 'House', 'district': '29', 'bp_district': '29',
     'former_incumbent': 'Kathy Javner', 'vacancy_reason': 'resigned',
     'general_date': '2026-06-09', 'primary_date': None, 'runoff_date': None,
     'category': 3},

    # ── MASSACHUSETTS ──
    {'state': 'MA', 'chamber': 'Senate', 'district': '1st Middlesex', 'bp_district': '1st_Middlesex',
     'former_incumbent': 'Edward Kennedy', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-03', 'primary_date': '2026-02-03', 'runoff_date': None,
     'category': 2},
    {'state': 'MA', 'chamber': 'House', 'district': '5th Essex', 'bp_district': '5th_Essex',
     'former_incumbent': 'Ann-Margaret Ferrante', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-31', 'primary_date': '2026-03-03', 'runoff_date': None,
     'category': 3},

    # ── MICHIGAN ──
    {'state': 'MI', 'chamber': 'Senate', 'district': '35', 'bp_district': '35',
     'former_incumbent': 'Kristen McDonald Rivet', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-05-05', 'primary_date': '2026-02-03', 'runoff_date': None,
     'category': 2},

    # ── MINNESOTA ──
    {'state': 'MN', 'chamber': 'House', 'district': '47A', 'bp_district': '47A',
     'former_incumbent': 'Amanda Hemmingsen-Jaeger', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-01-27', 'primary_date': '2025-12-16', 'runoff_date': None,
     'category': 1},
    {'state': 'MN', 'chamber': 'House', 'district': '64A', 'bp_district': '64A',
     'former_incumbent': 'Kaohly Her', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-01-27', 'primary_date': '2025-12-16', 'runoff_date': None,
     'category': 1},

    # ── NEBRASKA ──
    {'state': 'NE', 'chamber': 'Legislature', 'district': '41', 'bp_district': '41',
     'former_incumbent': 'Daniel McKeon', 'vacancy_reason': 'resigned',
     'general_date': '2026-11-03', 'primary_date': '2026-05-12', 'runoff_date': None,
     'category': 3},

    # ── NEW HAMPSHIRE ──
    {'state': 'NH', 'chamber': 'House', 'district': 'Carroll-7', 'bp_district': 'Carroll_County_District_7',
     'former_incumbent': 'Glenn Cordelli', 'vacancy_reason': 'died',
     'general_date': '2026-03-10', 'primary_date': '2026-01-20', 'runoff_date': None,
     'category': 2},

    # ── NEW MEXICO ──
    {'state': 'NM', 'chamber': 'Senate', 'district': '33', 'bp_district': '33',
     'former_incumbent': 'Nicholas Paul', 'vacancy_reason': 'resigned',
     'general_date': '2026-11-03', 'primary_date': '2026-06-02', 'runoff_date': None,
     'category': 3},

    # ── NEW YORK ──
    {'state': 'NY', 'chamber': 'Senate', 'district': '47', 'bp_district': '47',
     'former_incumbent': 'Brad Hoylman-Sigal', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-02-03', 'primary_date': None, 'runoff_date': None,
     'category': 1},
    {'state': 'NY', 'chamber': 'Senate', 'district': '61', 'bp_district': '61',
     'former_incumbent': 'Sean Ryan', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-02-03', 'primary_date': None, 'runoff_date': None,
     'category': 1},
    {'state': 'NY', 'chamber': 'Assembly', 'district': '74', 'bp_district': '74',
     'former_incumbent': 'Harvey Epstein', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-02-03', 'primary_date': None, 'runoff_date': None,
     'category': 1},
    {'state': 'NY', 'chamber': 'Assembly', 'district': '36', 'bp_district': '36',
     'former_incumbent': 'Zohran Mamdani', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-02-03', 'primary_date': None, 'runoff_date': None,
     'category': 1},

    # ── NORTH DAKOTA ──
    {'state': 'ND', 'chamber': 'House', 'district': '26', 'bp_district': '26',
     'former_incumbent': 'Jeremy Olson', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-11-03', 'primary_date': '2026-06-09', 'runoff_date': None,
     'category': 3},
    {'state': 'ND', 'chamber': 'House', 'district': '42', 'bp_district': '42',
     'former_incumbent': 'Emily O\'Brien', 'vacancy_reason': 'resigned',
     'general_date': '2026-11-03', 'primary_date': '2026-06-09', 'runoff_date': None,
     'category': 3},

    # ── OKLAHOMA ──
    {'state': 'OK', 'chamber': 'House', 'district': '35', 'bp_district': '35',
     'former_incumbent': 'Ty Burns', 'vacancy_reason': 'resigned',
     'general_date': '2026-02-10', 'primary_date': '2025-12-09', 'runoff_date': None,
     'category': 1},
    {'state': 'OK', 'chamber': 'House', 'district': '92', 'bp_district': '92',
     'former_incumbent': 'Forrest Bennett', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-11-03', 'primary_date': '2026-06-16', 'runoff_date': None,
     'category': 'overlap'},
    {'state': 'OK', 'chamber': 'Senate', 'district': '17', 'bp_district': '17',
     'former_incumbent': 'Shane Jett', 'vacancy_reason': 'resigned',
     'general_date': '2026-11-03', 'primary_date': '2026-06-16', 'runoff_date': None,
     'category': 3},

    # ── PENNSYLVANIA ──
    {'state': 'PA', 'chamber': 'House', 'district': '22', 'bp_district': '22',
     'former_incumbent': 'Joshua Siegel', 'vacancy_reason': 'resigned',
     'general_date': '2026-02-24', 'primary_date': None, 'runoff_date': None,
     'category': 3},
    {'state': 'PA', 'chamber': 'House', 'district': '42', 'bp_district': '42',
     'former_incumbent': 'Dan Miller', 'vacancy_reason': 'resigned',
     'general_date': '2026-02-24', 'primary_date': None, 'runoff_date': None,
     'category': 3},
    {'state': 'PA', 'chamber': 'House', 'district': '79', 'bp_district': '79',
     'former_incumbent': 'Louis Schmitt Jr.', 'vacancy_reason': 'resigned',
     'general_date': '2026-03-17', 'primary_date': None, 'runoff_date': None,
     'category': 3},
    {'state': 'PA', 'chamber': 'House', 'district': '193', 'bp_district': '193',
     'former_incumbent': 'Torren Ecker', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-03-17', 'primary_date': None, 'runoff_date': None,
     'category': 3},
    {'state': 'PA', 'chamber': 'House', 'district': '196', 'bp_district': '196',
     'former_incumbent': 'Seth Grove', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-05-19', 'primary_date': None, 'runoff_date': None,
     'category': 3},

    # ── SOUTH CAROLINA ──
    {'state': 'SC', 'chamber': 'House', 'district': '98', 'bp_district': '98',
     'former_incumbent': 'Chris Murphy', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-01-06', 'primary_date': '2025-11-04', 'runoff_date': None,
     'category': 1},

    # ── TEXAS ──
    {'state': 'TX', 'chamber': 'Senate', 'district': '4', 'bp_district': '4',
     'former_incumbent': 'Brandon Creighton', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-05-02', 'primary_date': None, 'runoff_date': None,
     'category': 3},

    # ── VIRGINIA ──
    {'state': 'VA', 'chamber': 'Senate', 'district': '15', 'bp_district': '15',
     'former_incumbent': 'Ghazala Hashmi', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-01-06', 'primary_date': None, 'runoff_date': None,
     'category': 1},
    {'state': 'VA', 'chamber': 'House of Delegates', 'district': '77', 'bp_district': '77',
     'former_incumbent': 'Michael Jones', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-01-06', 'primary_date': None, 'runoff_date': None,
     'category': 1},
    {'state': 'VA', 'chamber': 'House of Delegates', 'district': '11', 'bp_district': '11',
     'former_incumbent': 'David Bulova', 'vacancy_reason': 'resigned',
     'general_date': '2026-01-13', 'primary_date': None, 'runoff_date': None,
     'category': 1},
    {'state': 'VA', 'chamber': 'House of Delegates', 'district': '23', 'bp_district': '23',
     'former_incumbent': 'Candi King', 'vacancy_reason': 'resigned',
     'general_date': '2026-01-13', 'primary_date': None, 'runoff_date': None,
     'category': 1},
    {'state': 'VA', 'chamber': 'House of Delegates', 'district': '17', 'bp_district': '17',
     'former_incumbent': 'Mark Sickles', 'vacancy_reason': 'resigned',
     'general_date': '2026-01-20', 'primary_date': None, 'runoff_date': None,
     'category': 1},
    {'state': 'VA', 'chamber': 'Senate', 'district': '39', 'bp_district': '39',
     'former_incumbent': 'Adam Ebbin', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-02-10', 'primary_date': None, 'runoff_date': None,
     'category': 1},
    {'state': 'VA', 'chamber': 'House of Delegates', 'district': '5', 'bp_district': '5',
     'former_incumbent': 'Elizabeth Bennett-Parker', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2026-02-10', 'primary_date': None, 'runoff_date': None,
     'category': 1},

    # ── WEST VIRGINIA ──
    {'state': 'WV', 'chamber': 'Senate', 'district': '3', 'bp_district': '3',
     'former_incumbent': 'Donna Boley', 'vacancy_reason': 'resigned',
     'general_date': '2026-11-03', 'primary_date': '2026-05-12', 'runoff_date': None,
     'category': 3},
    {'state': 'WV', 'chamber': 'Senate', 'district': '17', 'bp_district': '17',
     'former_incumbent': 'Eric Nelson', 'vacancy_reason': 'resigned',
     'general_date': '2026-11-03', 'primary_date': '2026-05-12', 'runoff_date': None,
     'category': 3},
]


# ══════════════════════════════════════════════════════════════════════
# STATE-SPECIFIC URL BUILDERS
# ══════════════════════════════════════════════════════════════════════

STATE_NAMES = {
    'AL': 'Alabama', 'AR': 'Arkansas', 'CO': 'Colorado', 'CT': 'Connecticut',
    'FL': 'Florida', 'GA': 'Georgia', 'LA': 'Louisiana',
    'ME': 'Maine', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota',
    'NE': 'Nebraska', 'NH': 'New_Hampshire', 'NM': 'New_Mexico',
    'NY': 'New_York', 'ND': 'North_Dakota',
    'OK': 'Oklahoma', 'PA': 'Pennsylvania',
    'SC': 'South_Carolina', 'TX': 'Texas',
    'VA': 'Virginia', 'WV': 'West_Virginia',
}

CHAMBER_URL_NAMES = {
    'AL': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'AR': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'CO': {'Senate': 'State_Senate'},
    'CT': {'House': 'House_of_Representatives'},
    'FL': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'GA': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'LA': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'ME': {'House': 'House_of_Representatives'},
    'MA': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'MI': {'Senate': 'State_Senate'},
    'MN': {'House': 'House_of_Representatives'},
    'NE': {'Legislature': 'State_Legislature'},
    'NH': {'House': 'House_of_Representatives'},
    'NM': {'Senate': 'State_Senate'},
    'NY': {'Assembly': 'State_Assembly', 'Senate': 'State_Senate'},
    'ND': {'House': 'House_of_Representatives'},
    'OK': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'PA': {'House': 'House_of_Representatives'},
    'SC': {'House': 'House_of_Representatives'},
    'TX': {'Senate': 'State_Senate'},
    'VA': {'House of Delegates': 'House_of_Delegates', 'Senate': 'State_Senate'},
    'WV': {'Senate': 'State_Senate'},
}


def build_bp_url(race):
    """Build Ballotpedia district page URL for a special election race."""
    state = race['state']
    chamber = race['chamber']
    bp_district = race['bp_district']
    state_name = STATE_NAMES[state]

    chamber_url = CHAMBER_URL_NAMES.get(state, {}).get(chamber, chamber)

    if state == 'MA':
        if chamber == 'Senate':
            return f'https://ballotpedia.org/{state_name}_{chamber_url}_{bp_district}_District'
        else:
            return f'https://ballotpedia.org/{state_name}_House_of_Representatives_{bp_district}_District'
    elif state == 'NH':
        parts = bp_district.split('_')
        if 'County' in parts:
            county_idx = parts.index('County')
            county = '_'.join(parts[:county_idx])
            num = parts[-1]
            return f'https://ballotpedia.org/{state_name}_{chamber_url}_District_{county}_{num}'
        return f'https://ballotpedia.org/{state_name}_{chamber_url}_{bp_district}'
    elif state == 'NE':
        return f'https://ballotpedia.org/Nebraska_{chamber_url}_District_{bp_district}'
    else:
        return f'https://ballotpedia.org/{state_name}_{chamber_url}_District_{bp_district}'


# ══════════════════════════════════════════════════════════════════════
# DOWNLOAD + PARSE
# ══════════════════════════════════════════════════════════════════════

def download_page(url, cache_key, max_retries=3):
    """Download a Ballotpedia page with caching and 202 retry."""
    cache_path = os.path.join(CACHE_DIR, f'{cache_key}.html')
    if os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            return f.read()

    for attempt in range(max_retries):
        try:
            resp = httpx.get(
                url,
                headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'},
                follow_redirects=True,
                timeout=30
            )
            if resp.status_code == 200:
                with open(cache_path, 'w', encoding='utf-8') as f:
                    f.write(resp.text)
                return resp.text
            elif resp.status_code == 202:
                if attempt < max_retries - 1:
                    time.sleep(3)
                    continue
                print(f'    WARNING: HTTP 202 (CDN warming) for {url} after {max_retries} retries')
                return None
            else:
                print(f'    WARNING: HTTP {resp.status_code} for {url}')
                return None
        except Exception as e:
            print(f'    WARNING: Download failed for {url}: {e}')
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            return None
    return None


def parse_results_table(html_text, election_label, max_winners=1):
    """Parse a results_table votebox from Ballotpedia HTML."""
    idx = html_text.find(election_label)
    if idx == -1:
        idx_lower = html_text.lower().find(election_label.lower())
        if idx_lower == -1:
            return None
        idx = idx_lower

    before_start = max(0, idx - 300)
    check_text = html_text[before_start:idx + 1500].lower()
    if 'canceled' in check_text or 'cancelled' in check_text:
        cancel_idx = check_text.find('canceled')
        if cancel_idx == -1:
            cancel_idx = check_text.find('cancelled')
        label_pos = idx - before_start
        if abs(cancel_idx - label_pos) < 400:
            return None

    rt_idx = html_text.find('results_table', idx)
    if rt_idx == -1 or rt_idx > idx + 3000:
        return None

    table_end = html_text.find('</table>', rt_idx)
    if table_end == -1:
        return None

    table_html = html_text[rt_idx:table_end + 10]
    candidates = []

    for row_match in re.finditer(
        r'<tr\s+class="results_row\s*(winner)?\s*"[^>]*>(.*?)</tr>',
        table_html, re.DOTALL
    ):
        is_winner = row_match.group(1) == 'winner'
        row_html = row_match.group(2)

        name_cell = re.search(
            r'class="votebox-results-cell--text"[^>]*>(.*?)</td>',
            row_html, re.DOTALL
        )
        if not name_cell:
            continue

        cell_html = name_cell.group(1)

        if 'Other/Write-in' in cell_html or 'Write-in' in cell_html:
            continue

        is_incumbent = bool(re.search(r'<b><u><a', cell_html))

        name_link = re.search(r'<a\s+href="[^"]*"[^>]*>(.*?)</a>', cell_html)
        if not name_link:
            continue
        name = htmlmod.unescape(name_link.group(1).strip())

        party_match = re.search(r'\(([A-Z])\)', cell_html)
        party = party_match.group(1) if party_match else None
        if party and party in PARTY_MAP:
            party = PARTY_MAP[party]

        pct_match = re.search(r'class="percentage_number">([\d.]+)</div>', row_html)
        vote_pct = float(pct_match.group(1)) if pct_match else None

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


def build_election_labels(race):
    """Build label patterns to search for on the Ballotpedia page."""
    state = race['state']
    state_name = STATE_NAMES[state].replace('_', ' ')
    chamber = race['chamber']
    bp_district = race['bp_district'].replace('_', ' ')

    # Build the chamber name as BP uses it
    if state == 'MA':
        if chamber == 'Senate':
            chamber_str = f'{state_name} State Senate {bp_district} District'
        else:
            chamber_str = f'{state_name} House of Representatives {bp_district} District'
    elif state == 'NH':
        parts = bp_district.replace('_', ' ').split()
        if 'County' in parts:
            ci = parts.index('County')
            county = ' '.join(parts[:ci])
            num = parts[-1]
            nh_label = f'{county} {num}'
        else:
            nh_label = bp_district.replace('_', ' ')
        chamber_str = f'{state_name} House of Representatives {nh_label}'
    elif state == 'NE':
        chamber_str = f'{state_name} State Legislature District {bp_district}'
    elif chamber == 'Assembly':
        chamber_str = f'{state_name} State Assembly District {bp_district}'
    elif chamber == 'House of Delegates':
        chamber_str = f'{state_name} House of Delegates District {bp_district}'
    elif chamber == 'Senate':
        chamber_str = f'{state_name} State Senate District {bp_district}'
    else:
        chamber_str = f'{state_name} House of Representatives District {bp_district}'

    labels = {}

    # Special general election
    labels['Special'] = [
        f'Special general election for {chamber_str}',
        f'special general election for {chamber_str}',
        f'Special election for {chamber_str}',
        f'special election for {chamber_str}',
    ]

    # LA jungle primary = the special election itself
    if state == 'LA':
        labels['Special'] = [
            f'Special Nonpartisan  primary election  for {chamber_str}',
            f'Special nonpartisan primary for {chamber_str}',
            f'special nonpartisan primary for {chamber_str}',
            f'Special nonpartisan blanket primary for {chamber_str}',
            f'Special Nonpartisan  primary for {chamber_str}',
            f'Special election for {chamber_str}',
            f'special election for {chamber_str}',
        ]
        # LA runoff (if needed)
        labels['Special_Runoff'] = [
            f'Special general election for {chamber_str}',
            f'special general election for {chamber_str}',
            f'Special runoff for {chamber_str}',
            f'special runoff for {chamber_str}',
            f'Special general election runoff for {chamber_str}',
        ]
    else:
        # Standard primary states
        if race.get('primary_date'):
            labels['Special_Primary_D'] = [
                f'Special Democratic primary for {chamber_str}',
                f'special Democratic primary for {chamber_str}',
                f'Special Democratic primary election for {chamber_str}',
            ]
            labels['Special_Primary_R'] = [
                f'Special Republican primary for {chamber_str}',
                f'special Republican primary for {chamber_str}',
                f'Special Republican primary election for {chamber_str}',
            ]

        if race.get('runoff_date'):
            labels['Special_Runoff'] = [
                f'Special general runoff election for {chamber_str}',
                f'special general runoff election for {chamber_str}',
                f'Special general election runoff for {chamber_str}',
                f'special general election runoff for {chamber_str}',
                f'Special runoff for {chamber_str}',
                f'Special runoff election for {chamber_str}',
            ]
            labels['Special_Primary_Runoff_R'] = [
                f'Special Republican primary runoff for {chamber_str}',
                f'special Republican primary runoff for {chamber_str}',
            ]
            labels['Special_Primary_Runoff_D'] = [
                f'Special Democratic primary runoff for {chamber_str}',
                f'special Democratic primary runoff for {chamber_str}',
            ]

    return labels


def build_state_special_url(state):
    """Build URL for the state-level special elections summary page on BP."""
    state_name = STATE_NAMES[state]
    return f'https://ballotpedia.org/{state_name}_state_legislative_special_elections,_2026'


def try_parse_elections(html, race, labels):
    """Try to parse election results from HTML using the given label patterns."""
    elections = []
    for etype, patterns in labels.items():
        parsed = None
        for label in patterns:
            parsed = parse_results_table(html, label)
            if parsed:
                break

        if parsed:
            if etype == 'Special':
                if race['state'] == 'LA':
                    # For LA, the "Special" (jungle primary) date is the primary_date
                    date = race['primary_date']
                else:
                    date = race['general_date']
            elif etype in ('Special_Primary_Runoff_D', 'Special_Primary_Runoff_R'):
                date = race['runoff_date']
            elif 'Primary' in etype:
                date = race['primary_date']
            elif 'Runoff' in etype:
                if race['state'] == 'LA':
                    date = race['general_date']  # LA runoff = general_date
                else:
                    date = race['runoff_date']
            else:
                date = race['general_date']

            elections.append({
                'type': etype,
                'date': date,
                'total_votes': parsed['total_votes'],
                'candidates': parsed['candidates'],
            })
    return elections


# Cache for state-level special election pages
_state_special_pages = {}


def process_race(race):
    """Download and parse results for a single special election race."""
    state = race['state']
    chamber = race['chamber']
    district = race['district']
    category = race['category']
    race_key = f'{state}_{chamber.replace(" ", "")}_{district}'

    # Skip overlap races (they use existing elections)
    if category == 'overlap':
        print(f'  {race_key}: OVERLAP — will close seat_term only, no new elections')
        return {
            'state': state,
            'chamber': chamber,
            'district': district,
            'former_incumbent': race['former_incumbent'],
            'vacancy_reason': race['vacancy_reason'],
            'category': category,
            'general_date': race['general_date'],
            'primary_date': race.get('primary_date'),
            'elections': [],
        }

    # 1. Try the district page first
    url = build_bp_url(race)
    cache_key = f'special_{race_key}'.replace('/', '_').replace('-', '_')

    html = download_page(url, cache_key)
    labels = build_election_labels(race)

    elections = []
    if html:
        elections = try_parse_elections(html, race, labels)

    # 2. If no Special general found on district page, try state special elections page
    if not any(e['type'] == 'Special' for e in elections):
        if state not in _state_special_pages:
            state_url = build_state_special_url(state)
            state_cache_key = f'state_specials_{state}'
            _state_special_pages[state] = download_page(state_url, state_cache_key)
            time.sleep(0.5)

        state_html = _state_special_pages.get(state)
        if state_html:
            state_elections = try_parse_elections(state_html, race, labels)
            found_types = {e['type'] for e in elections}
            for se in state_elections:
                if se['type'] not in found_types:
                    elections.append(se)

    # 3. Check for "not required" / canceled general (primary winner won outright)
    if (not any(e['type'] == 'Special' for e in elections)
        and any('Primary' in e['type'] for e in elections)
        and html):
        html_lower = html.lower()
        general_skipped = (
            'not required' in html_lower
            or 'was not necessary' in html_lower
            or 'won outright in the primary' in html_lower
            or 'was canceled after only' in html_lower
            or 'general election was canceled' in html_lower
        )
        if general_skipped:
            for e in elections:
                if 'Primary' in e['type']:
                    e['type'] = 'Special'
                    e['primary_promoted'] = True
                    print(f'  {race_key}: General not required — primary winner won outright')
                    break

    # 4. Check for canceled elections (winner won without appearing on ballot)
    if not any(e['type'] == 'Special' for e in elections) and html:
        canceled_match = re.search(
            r'canceled.*?<a\s+href="[^"]*"[^>]*>([^<]+)</a>\s*\(([A-Z])\)\s*won',
            html, re.DOTALL | re.IGNORECASE
        )
        if not canceled_match:
            canceled_match = re.search(
                r'<a\s+href="[^"]*"[^>]*>([^<]+)</a>\s*\(([A-Z])\)\s*won\s+the\s+election\s+without',
                html, re.DOTALL | re.IGNORECASE
            )
        if canceled_match:
            winner_name = htmlmod.unescape(canceled_match.group(1).strip())
            winner_party = canceled_match.group(2)
            elections.append({
                'type': 'Special',
                'date': race['general_date'],
                'total_votes': 0,
                'candidates': [{
                    'name': winner_name,
                    'party': winner_party,
                    'is_incumbent': False,
                    'is_winner': True,
                    'votes': 0,
                    'vote_pct': 100.0,
                }],
                'canceled': True,
            })
            print(f'  {race_key}: CANCELED — {winner_name} ({winner_party}) won unopposed')

    # Build result even if no elections parsed (Cat 2/3 upcoming races)
    result = {
        'state': state,
        'chamber': chamber,
        'district': district,
        'former_incumbent': race['former_incumbent'],
        'vacancy_reason': race['vacancy_reason'],
        'category': category,
        'general_date': race['general_date'],
        'primary_date': race.get('primary_date'),
        'runoff_date': race.get('runoff_date'),
        'elections': elections,
    }

    # Quick summary
    if not elections:
        if category == 1:
            if not html:
                print(f'  {race_key}: Cat {category} — FAILED to download')
            else:
                print(f'  {race_key}: Cat {category} — No elections found (may not have results yet)')
        else:
            print(f'  {race_key}: Cat {category} — No elections found (upcoming)')
    else:
        special_gen = next((e for e in elections if e['type'] == 'Special'), None)
        if special_gen:
            winners = [c['name'] for c in special_gen['candidates'] if c['is_winner']]
            print(f'  {race_key}: Cat {category} — {len(elections)} phases, '
                  f'winner: {", ".join(winners) if winners else "NONE"}, '
                  f'total: {special_gen["total_votes"]}')
        else:
            types = [e['type'] for e in elections]
            print(f'  {race_key}: Cat {category} — {len(elections)} phases ({", ".join(types)}) (no Special general)')

    return result


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Download 2026 special election data')
    parser.add_argument('--state', type=str,
                        help='Process a single state (e.g., VA, LA)')
    parser.add_argument('--output', type=str, default=OUTPUT_PATH,
                        help='Output JSON path')
    args = parser.parse_args()

    os.makedirs(CACHE_DIR, exist_ok=True)

    races = SPECIAL_ELECTIONS
    if args.state:
        races = [r for r in races if r['state'] == args.state.upper()]
        if not races:
            print(f'No special elections found for {args.state}')
            sys.exit(1)

    print(f'Processing {len(races)} special election races...')
    if args.state:
        print(f'Filtered to state: {args.state.upper()}')

    all_results = []
    failed = []

    from collections import Counter
    by_state = Counter(r['state'] for r in races)
    by_category = Counter(str(r['category']) for r in races)
    print(f'\nRaces by state:')
    for st, cnt in sorted(by_state.items()):
        print(f'  {st}: {cnt}')
    print(f'\nRaces by category:')
    for cat, cnt in sorted(by_category.items()):
        print(f'  {cat}: {cnt}')

    for i, race in enumerate(races):
        result = process_race(race)
        if result:
            all_results.append(result)
        else:
            failed.append(f"{race['state']} {race['chamber']} {race['district']}")

        if i < len(races) - 1:
            time.sleep(0.5)

    # Summary
    print(f'\n{"=" * 60}')
    print(f'SUMMARY')
    print(f'{"=" * 60}')
    print(f'Total races processed: {len(all_results)} / {len(races)}')

    if failed:
        print(f'\nFailed ({len(failed)}):')
        for f_race in failed:
            print(f'  {f_race}')

    # Count election types
    etype_counts = Counter()
    total_candidates = 0
    total_winners = 0
    for r in all_results:
        for e in r['elections']:
            etype_counts[e['type']] += 1
            total_candidates += len(e['candidates'])
            total_winners += sum(1 for c in e['candidates'] if c['is_winner'])

    print(f'\nElection records by type:')
    for etype, cnt in sorted(etype_counts.items()):
        print(f'  {etype}: {cnt}')
    print(f'Total election records: {sum(etype_counts.values())}')
    print(f'Total candidates: {total_candidates}')
    print(f'Total winners: {total_winners}')

    # Races by category with results
    cat_stats = Counter()
    for r in all_results:
        cat = str(r.get('category', '?'))
        has_results = len(r['elections']) > 0
        cat_stats[f'Cat {cat} {"with" if has_results else "without"} results'] += 1
    print(f'\nCategory breakdown:')
    for label, cnt in sorted(cat_stats.items()):
        print(f'  {label}: {cnt}')

    # Winners by party
    party_wins = Counter()
    for r in all_results:
        special_gen = next((e for e in r['elections'] if e['type'] == 'Special'), None)
        if special_gen:
            for c in special_gen['candidates']:
                if c['is_winner']:
                    party_wins[c['party']] += 1
    if party_wins:
        print(f'\nSpecial general winners by party:')
        for p, cnt in party_wins.most_common():
            print(f'  {p}: {cnt}')

    # Write output
    with open(args.output, 'w') as f_out:
        json.dump(all_results, f_out, indent=2)
    print(f'\nWritten to {args.output}')


if __name__ == '__main__':
    main()
