#!/usr/bin/env python3
"""Build all partisan standalone maps from states_summary.json.
Reads SVG state paths from ag_partisan.html as template.
Outputs legend-on-top layout matching the ballot measures map pattern.

Maps generated:
  1. trifectas_partisan.html
  2. legislatures_partisan.html
  3. governors_partisan.html
  4. governors_2026.html
  5. ltgov_partisan.html
  6. ag_partisan.html
  7. sos_partisan.html
"""

import re, json

SITE_DIR = '/home/billkramer/elections/site'

# Read state summary data
with open(f'{SITE_DIR}/data/states_summary.json') as f:
    summary = json.load(f)
STATES_DATA = summary['states']

# Read AG map as SVG template (has all 50 state paths)
with open(f'{SITE_DIR}/ag_partisan.html') as f:
    template = f.read()

# Extract state path data
state_paths = {}
for m in re.finditer(r'<g class="state-group" data-state="(\w{2})"[^>]*>(.*?)\n\s*</g>', template, re.DOTALL):
    abbr = m.group(1)
    inner = m.group(2)
    fill_match = re.search(r'<g class="state-fill"[^>]*>(.*?)</g>', inner, re.DOTALL)
    stroke_match = re.search(r'<g class="state-stroke"[^>]*>(.*?)</g>', inner, re.DOTALL)
    extra = ''
    stroke_g_match = re.search(r'<g class="state-stroke"[^>]*>.*?</g>', inner, re.DOTALL)
    if stroke_g_match:
        remainder = inner[stroke_g_match.end():]
        extra_paths = re.findall(r'<path [^>]+/>', remainder)
        if extra_paths:
            extra = '\n      '.join(extra_paths)
    if fill_match and stroke_match:
        state_paths[abbr] = {
            'fill_content': fill_match.group(1).strip(),
            'stroke_content': stroke_match.group(1).strip(),
            'extra': extra,
        }

# Extract label elements
labels = re.findall(r'<text[^>]*class="state-label"[^>]*>\w{2}</text>', template)

STATE_NAMES = {
    'AL':'Alabama','AK':'Alaska','AZ':'Arizona','AR':'Arkansas','CA':'California',
    'CO':'Colorado','CT':'Connecticut','DE':'Delaware','FL':'Florida','GA':'Georgia',
    'HI':'Hawaii','ID':'Idaho','IL':'Illinois','IN':'Indiana','IA':'Iowa',
    'KS':'Kansas','KY':'Kentucky','LA':'Louisiana','ME':'Maine','MD':'Maryland',
    'MA':'Massachusetts','MI':'Michigan','MN':'Minnesota','MS':'Mississippi','MO':'Missouri',
    'MT':'Montana','NE':'Nebraska','NV':'Nevada','NH':'New Hampshire','NJ':'New Jersey',
    'NM':'New Mexico','NY':'New York','NC':'North Carolina','ND':'North Dakota','OH':'Ohio',
    'OK':'Oklahoma','OR':'Oregon','PA':'Pennsylvania','RI':'Rhode Island','SC':'South Carolina',
    'SD':'South Dakota','TN':'Tennessee','TX':'Texas','UT':'Utah','VT':'Vermont',
    'VA':'Virginia','WA':'Washington','WV':'West Virginia','WI':'Wisconsin','WY':'Wyoming'
}
ALL_STATES = sorted(STATE_NAMES.keys())

# NE unicameral chamber name
LOWER_NAMES = {'CA':'Assembly','NV':'Assembly','NY':'Assembly','WI':'Assembly','NJ':'Assembly',
               'MD':'House of Delegates','VA':'House of Delegates','WV':'House of Delegates',
               'NE':'Legislature'}

def get_lower_name(abbr):
    return LOWER_NAMES.get(abbr, 'House')

def get_upper_name(abbr):
    if abbr == 'NE':
        return None  # unicameral
    return 'Senate'

# ── Map configuration functions ──────────────────────────────────────

def get_trifecta_config():
    """Trifectas: D trifecta, R trifecta, Divided."""
    categories = {}
    for abbr, st in STATES_DATA.items():
        tri = st.get('trifecta', '')
        if 'Democratic' in tri:
            categories[abbr] = 'dem_trifecta'
        elif 'Republican' in tri:
            categories[abbr] = 'gop_trifecta'
        else:
            categories[abbr] = 'divided'

    counts = {k: sum(1 for v in categories.values() if v == k)
              for k in ['dem_trifecta', 'gop_trifecta', 'divided']}

    colors = {'dem_trifecta': '#6cb3d2', 'gop_trifecta': '#e50963', 'divided': '#8b567f'}
    legend = [
        ('dem_trifecta', f'Democratic Trifecta ({counts["dem_trifecta"]})'),
        ('gop_trifecta', f'Republican Trifecta ({counts["gop_trifecta"]})'),
        ('divided', f'Divided Government ({counts["divided"]})'),
    ]

    def tooltip_fn(abbr, cat):
        st = STATES_DATA[abbr]
        gov = st.get('governor', {})
        ch = st.get('chambers', {})
        tri_label = st.get('trifecta', 'Unknown')
        upper_ctrl = ch.get('Senate', {})
        lower_name = get_lower_name(abbr)
        lower_key = lower_name if lower_name in ch else 'House'  # fallback
        # Find the right key
        for k in ch:
            if k != 'Senate':
                lower_key = k
                break
        return {
            'state': abbr, 'name': STATE_NAMES[abbr],
            'trifecta': tri_label, 'category': cat,
            'governor': gov.get('name', ''), 'govParty': gov.get('party', ''),
        }

    return {
        'title': 'Trifectas <span class="separator">|</span> 2026 Partisan Breakdown',
        'filename': 'trifectas_partisan.html',
        'categories': categories,
        'colors': colors,
        'legend': legend,
        'tooltip_fn': tooltip_fn,
        'source': 'Source: MultiState. Data as of <span id="data-date"></span>.',
        'nav_active': 'trifectas',
        'tooltip_template': 'trifecta',
    }


def get_governors_config():
    """Governors: D control, R control."""
    categories = {}
    for abbr, st in STATES_DATA.items():
        gov = st.get('governor', {})
        party = gov.get('party', '')
        categories[abbr] = 'dem' if party == 'D' else 'gop'

    counts = {k: sum(1 for v in categories.values() if v == k) for k in ['dem', 'gop']}
    colors = {'dem': '#276181', 'gop': '#e50963'}
    legend = [
        ('dem', f'Democratic Control ({counts["dem"]})'),
        ('gop', f'Republican Control ({counts["gop"]})'),
    ]

    def tooltip_fn(abbr, cat):
        gov = STATES_DATA[abbr].get('governor', {})
        return {
            'state': abbr, 'name': STATE_NAMES[abbr],
            'governor': gov.get('name', ''), 'party': gov.get('party', ''),
            'partyLabel': 'Democratic' if gov.get('party') == 'D' else 'Republican',
        }

    return {
        'title': 'Governors <span class="separator">|</span> Partisan Breakdown',
        'filename': 'governors_partisan.html',
        'categories': categories,
        'colors': colors,
        'legend': legend,
        'tooltip_fn': tooltip_fn,
        'source': 'Source: MultiState. Data as of <span id="data-date"></span>.',
        'nav_active': 'governors',
        'tooltip_template': 'governor',
    }


def get_governors_2026_config():
    """Governors 2026: D incumbent, D open, R incumbent, R open, no election."""
    categories = {}
    for abbr, st in STATES_DATA.items():
        gov = st.get('governor', {})
        party = gov.get('party', '')
        next_el = gov.get('next_election')
        forecast = gov.get('forecast')
        has_election = (next_el == 2026)

        if not has_election:
            categories[abbr] = 'no_election'
        else:
            # Term-limited / open seat detection: forecast label doesn't determine this
            # We'll check if the forecast implies open seat (Solid/Very Likely + term limit)
            # For simplicity, use the existing data-info pattern from the original map
            # Open seat = governor can't run again (term-limited or not seeking)
            # We need to derive this from the data; for now, classify based on existing map data
            categories[abbr] = f'{"dem" if party == "D" else "gop"}_incumbent'

    # Override with known open seats (term-limited governors in 2026)
    # These are governors who cannot or will not run again
    OPEN_SEATS = {
        'AK', 'AL', 'CA', 'CO', 'FL', 'GA', 'HI', 'IA', 'KS', 'ME',
        'MI', 'MN', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'SD', 'TN', 'WI', 'WY',
    }
    for abbr in OPEN_SEATS:
        if abbr in categories and categories[abbr] != 'no_election':
            party = STATES_DATA[abbr].get('governor', {}).get('party', '')
            categories[abbr] = f'{"dem" if party == "D" else "gop"}_open'

    counts = {}
    for k in ['dem_incumbent', 'dem_open', 'gop_incumbent', 'gop_open', 'no_election']:
        counts[k] = sum(1 for v in categories.values() if v == k)

    colors = {
        'dem_incumbent': '#276181', 'dem_open': '#6cb3d2',
        'gop_incumbent': '#e50963', 'gop_open': '#e86987',
        'no_election': '#e4e4e4',
    }
    legend = [
        ('dem_incumbent', f'Democratic Incumbent ({counts["dem_incumbent"]})'),
        ('dem_open', f'Democratic Open Seat ({counts["dem_open"]})'),
        ('gop_incumbent', f'Republican Incumbent ({counts["gop_incumbent"]})'),
        ('gop_open', f'Republican Open Seat ({counts["gop_open"]})'),
        ('no_election', f'No 2026 Election ({counts["no_election"]})'),
    ]

    def tooltip_fn(abbr, cat):
        gov = STATES_DATA[abbr].get('governor', {})
        cat_labels = {
            'dem_incumbent': 'Democratic Incumbent', 'dem_open': 'Democratic Open Seat',
            'gop_incumbent': 'Republican Incumbent', 'gop_open': 'Republican Open Seat',
            'no_election': 'No 2026 Election',
        }
        return {
            'state': abbr, 'name': STATE_NAMES[abbr],
            'governor': gov.get('name', ''), 'party': gov.get('party', ''),
            'forecast': gov.get('forecast'),
            'category': cat, 'categoryLabel': cat_labels.get(cat, ''),
            'hasElection': cat != 'no_election',
        }

    return {
        'title': 'Governors <span class="separator">|</span> 2026 Elections &amp; Open Seats',
        'filename': 'governors_2026.html',
        'categories': categories,
        'colors': colors,
        'legend': legend,
        'tooltip_fn': tooltip_fn,
        'source': 'Source: MultiState. Data as of <span id="data-date"></span>.<br>In 2026, 36 gubernatorial seats are up for election.',
        'nav_active': 'governors_2026',
        'tooltip_template': 'governor_2026',
    }


def get_ltgov_config():
    """Lt. Governors: D, R, no office, vacant."""
    categories = {}
    for abbr, st in STATES_DATA.items():
        officers = st.get('statewide_officers', [])
        ltgov = next((o for o in officers if o['office'] == 'Lt. Governor'), None)
        if ltgov is None or ltgov.get('name') is None:
            categories[abbr] = 'none'
        elif ltgov.get('party') == 'D':
            categories[abbr] = 'dem'
        elif ltgov.get('party') == 'R':
            categories[abbr] = 'gop'
        else:
            categories[abbr] = 'none'

    counts = {k: sum(1 for v in categories.values() if v == k) for k in ['dem', 'gop', 'none']}
    colors = {'dem': '#276181', 'gop': '#e50963', 'none': '#e4e4e4'}
    legend = [
        ('dem', f'Democratic Control ({counts["dem"]})'),
        ('gop', f'Republican Control ({counts["gop"]})'),
        ('none', f'No Lt. Governor ({counts["none"]})'),
    ]

    def tooltip_fn(abbr, cat):
        officers = STATES_DATA[abbr].get('statewide_officers', [])
        ltgov = next((o for o in officers if o['office'] == 'Lt. Governor'), None)
        name = ltgov.get('name') if ltgov else None
        party = ltgov.get('party') if ltgov else None
        return {
            'state': abbr, 'name': STATE_NAMES[abbr],
            'ltgov': name, 'party': party,
            'partyLabel': 'Democratic' if party == 'D' else ('Republican' if party == 'R' else 'None'),
        }

    return {
        'title': 'Lt. Governors <span class="separator">|</span> Partisan Breakdown',
        'filename': 'ltgov_partisan.html',
        'categories': categories,
        'colors': colors,
        'legend': legend,
        'tooltip_fn': tooltip_fn,
        'source': 'Source: MultiState. Data as of <span id="data-date"></span>.',
        'nav_active': 'ltgov',
        'tooltip_template': 'officer',
    }


def get_ag_config():
    """Attorneys General: D, R."""
    categories = {}
    for abbr, st in STATES_DATA.items():
        officers = st.get('statewide_officers', [])
        ag = next((o for o in officers if o['office'] == 'Attorney General'), None)
        if ag and ag.get('party') == 'D':
            categories[abbr] = 'dem'
        else:
            categories[abbr] = 'gop'

    counts = {k: sum(1 for v in categories.values() if v == k) for k in ['dem', 'gop']}
    colors = {'dem': '#276181', 'gop': '#e50963'}
    legend = [
        ('dem', f'Democratic Control ({counts["dem"]})'),
        ('gop', f'Republican Control ({counts["gop"]})'),
    ]

    def tooltip_fn(abbr, cat):
        officers = STATES_DATA[abbr].get('statewide_officers', [])
        ag = next((o for o in officers if o['office'] == 'Attorney General'), None)
        name = ag.get('name') if ag else None
        party = ag.get('party') if ag else None
        return {
            'state': abbr, 'name': STATE_NAMES[abbr],
            'ag': name, 'party': party,
            'partyLabel': 'Democratic' if party == 'D' else 'Republican',
        }

    return {
        'title': 'Attorneys General <span class="separator">|</span> Partisan Breakdown',
        'filename': 'ag_partisan.html',
        'categories': categories,
        'colors': colors,
        'legend': legend,
        'tooltip_fn': tooltip_fn,
        'source': 'Source: MultiState. Data as of <span id="data-date"></span>.',
        'nav_active': 'ag',
        'tooltip_template': 'officer',
    }


def get_sos_config():
    """Secretaries of State: D, R, no position, vacant."""
    categories = {}
    for abbr, st in STATES_DATA.items():
        officers = st.get('statewide_officers', [])
        sos = next((o for o in officers if o['office'] == 'Secretary of State'), None)
        if sos is None or sos.get('name') is None:
            categories[abbr] = 'none'
        elif sos.get('party') == 'D':
            categories[abbr] = 'dem'
        elif sos.get('party') == 'R':
            categories[abbr] = 'gop'
        else:
            categories[abbr] = 'none'

    counts = {k: sum(1 for v in categories.values() if v == k) for k in ['dem', 'gop', 'none']}
    colors = {'dem': '#276181', 'gop': '#e50963', 'none': '#e4e4e4'}
    legend_items = [
        ('dem', f'Democratic Control ({counts["dem"]})'),
        ('gop', f'Republican Control ({counts["gop"]})'),
    ]
    if counts['none'] > 0:
        legend_items.append(('none', f'No SoS Position ({counts["none"]})'))

    def tooltip_fn(abbr, cat):
        officers = STATES_DATA[abbr].get('statewide_officers', [])
        sos = next((o for o in officers if o['office'] == 'Secretary of State'), None)
        name = sos.get('name') if sos else None
        party = sos.get('party') if sos else None
        return {
            'state': abbr, 'name': STATE_NAMES[abbr],
            'sos': name, 'party': party,
            'partyLabel': 'Democratic' if party == 'D' else ('Republican' if party == 'R' else 'None'),
        }

    return {
        'title': 'Secretaries of State <span class="separator">|</span> Partisan Breakdown',
        'filename': 'sos_partisan.html',
        'categories': categories,
        'colors': colors,
        'legend': legend_items,
        'tooltip_fn': tooltip_fn,
        'source': 'Source: MultiState. Data as of <span id="data-date"></span>.',
        'nav_active': 'sos',
        'tooltip_template': 'officer',
    }


def get_legislatures_config():
    """Legislatures: D control, R control, Split."""
    categories = {}
    for abbr, st in STATES_DATA.items():
        ch = st.get('chambers', {})
        if abbr == 'NE':
            # Unicameral — classify based on party majority
            leg = ch.get('Legislature', ch.get('Senate', {}))
            categories[abbr] = 'gop'  # NE is nominally nonpartisan but R-majority
            continue
        # Find upper and lower chambers
        upper = ch.get('Senate', {})
        lower = None
        for k, v in ch.items():
            if k != 'Senate':
                lower = v
                break
        if not lower:
            lower = {}
        # Determine control by majority
        upper_ctrl = 'D' if upper.get('d', 0) > upper.get('r', 0) else ('R' if upper.get('r', 0) > upper.get('d', 0) else 'T')
        lower_ctrl = 'D' if lower.get('d', 0) > lower.get('r', 0) else ('R' if lower.get('r', 0) > lower.get('d', 0) else 'T')
        if upper_ctrl == lower_ctrl and upper_ctrl in ('D', 'R'):
            categories[abbr] = 'dem' if upper_ctrl == 'D' else 'gop'
        else:
            categories[abbr] = 'split'

    counts = {k: sum(1 for v in categories.values() if v == k) for k in ['dem', 'gop', 'split']}
    colors = {'dem': '#276181', 'gop': '#e50963', 'split': '#8b567f'}
    legend = [
        ('dem', f'Democratic Control ({counts["dem"]})'),
        ('gop', f'Republican Control ({counts["gop"]})'),
        ('split', f'Split Control ({counts["split"]})'),
    ]

    def tooltip_fn(abbr, cat):
        st = STATES_DATA[abbr]
        ch = st.get('chambers', {})
        upper_name = get_upper_name(abbr) or 'Legislature'
        lower_name = get_lower_name(abbr)
        upper = ch.get('Senate', ch.get('Legislature', {}))
        lower = None
        for k, v in ch.items():
            if k != 'Senate':
                lower = v
                lower_name = k
                break
        info = {
            'state': abbr, 'name': STATE_NAMES[abbr], 'category': cat,
        }
        if abbr == 'NE':
            leg = ch.get('Legislature', ch.get('Senate', {}))
            info['chamber'] = 'Legislature (Unicameral)'
            info['d'] = leg.get('d', 0)
            info['r'] = leg.get('r', 0)
        else:
            info['upperChamber'] = upper_name
            info['upperD'] = upper.get('d', 0)
            info['upperR'] = upper.get('r', 0)
            info['lowerChamber'] = lower_name
            info['lowerD'] = lower.get('d', 0) if lower else 0
            info['lowerR'] = lower.get('r', 0) if lower else 0
        return info

    return {
        'title': 'State Legislatures <span class="separator">|</span> Partisan Breakdown',
        'filename': 'legislatures_partisan.html',
        'categories': categories,
        'colors': colors,
        'legend': legend,
        'tooltip_fn': tooltip_fn,
        'source': 'Source: MultiState. Data as of <span id="data-date"></span>.',
        'nav_active': 'legislatures',
        'tooltip_template': 'legislature',
    }


# ── HTML generation ──────────────────────────────────────────────────

NAV_LINKS = [
    ('trifectas', 'trifectas_partisan.html', 'Trifectas'),
    ('legislatures', 'legislatures_partisan.html', 'Legislatures'),
    ('governors_2026', 'governors_2026.html', '2026 Governors'),
    ('governors', 'governors_partisan.html', 'Governors'),
    ('ltgov', 'ltgov_partisan.html', 'Lt. Governors'),
    ('ag', 'ag_partisan.html', 'Attorneys General'),
    ('sos', 'sos_partisan.html', 'Secretaries of State'),
    ('sep1', None, None),
    ('swing', 'swing-calculator.html', 'Swing Calc'),
    ('sep2', None, None),
    ('bm_init', 'ballot-measures-initiative-map.html', 'Initiative Auth.'),
    ('bm_ref', 'ballot-measures-referendum-map.html', 'Referendum Auth.'),
    ('sep3', None, None),
    ('bm_101', 'ballot-measures-101.html', '101 Guide'),
]


def build_nav_html(active_key):
    parts = []
    for key, href, label in NAV_LINKS:
        if href is None:
            parts.append('    <span class="nav-sep">|</span>')
        else:
            active = ' active' if key == active_key else ''
            parts.append(f'    <a class="nav-link{active}" href="{href}">{label}</a>')
    return '\n'.join(parts)


def build_map_html(config):
    """Build a complete standalone map HTML page."""
    categories = config['categories']
    colors = config['colors']
    tooltip_fn = config['tooltip_fn']

    # Build state SVG groups
    state_groups = []
    for abbr in ALL_STATES:
        if abbr not in state_paths:
            continue
        cat = categories.get(abbr, list(colors.keys())[0])
        color = colors[cat]
        data_info = json.dumps(tooltip_fn(abbr, cat))
        paths = state_paths[abbr]
        extra = f'\n      {paths["extra"]}' if paths['extra'] else ''
        group = f'''<g class="state-group" data-state="{abbr}" data-category="{cat}" data-info='{data_info}'>
      <g class="state-fill" fill="{color}">{paths['fill_content']}</g>
      <g class="state-stroke" fill="none" stroke="#ffffff" stroke-width="1.0">{paths['stroke_content']}</g>{extra}

    </g>'''
        state_groups.append(group)

    states_svg = '\n'.join(state_groups)
    labels_svg = '\n          '.join(labels)

    # Build legend
    legend_html = ''
    for cat, label in config['legend']:
        legend_html += f'''      <div class="legend-row">
        <div class="legend-swatch" style="background:{colors[cat]}"></div>
        <div class="legend-label">{label}</div>
      </div>\n'''

    nav_html = build_nav_html(config['nav_active'])

    # Build tooltip JS based on template type
    tt = config['tooltip_template']
    if tt == 'trifecta':
        tooltip_js = """
        let html = '<div class="tt-state">' + info.name + '</div>';
        html += '<div class="tt-type">' + info.trifecta + '</div>';
        if (info.governor) {
          const cls = info.govParty === 'D' ? 'tt-party-d' : 'tt-party-r';
          html += '<div class="tt-governor">Gov. ' + info.governor + ' <span class="' + cls + '">(' + info.govParty + ')</span></div>';
        }"""
    elif tt == 'governor':
        tooltip_js = """
        let html = '<div class="tt-state">' + info.name + '</div>';
        const cls = info.party === 'D' ? 'tt-party-d' : 'tt-party-r';
        html += '<div class="tt-governor">Gov. ' + info.governor + ' <span class="' + cls + '">(' + info.partyLabel + ')</span></div>';"""
    elif tt == 'governor_2026':
        tooltip_js = """
        let html = '<div class="tt-state">' + info.name + '</div>';
        html += '<div class="tt-type">' + info.categoryLabel + '</div>';
        if (info.governor) {
          const cls = info.party === 'D' ? 'tt-party-d' : 'tt-party-r';
          html += '<div class="tt-governor">Gov. ' + info.governor + ' <span class="' + cls + '">(' + info.party + ')</span></div>';
        }
        if (info.forecast) {
          html += '<div class="tt-forecast">Forecast: ' + info.forecast + '</div>';
        }"""
    elif tt == 'legislature':
        tooltip_js = """
        let html = '<div class="tt-state">' + info.name + '</div>';
        if (info.chamber) {
          html += '<div class="tt-type">' + info.chamber + ': <span class="tt-party-d">' + info.d + 'D</span> / <span class="tt-party-r">' + info.r + 'R</span></div>';
        } else {
          html += '<div class="tt-type">' + info.upperChamber + ': <span class="tt-party-d">' + info.upperD + 'D</span> / <span class="tt-party-r">' + info.upperR + 'R</span></div>';
          html += '<div class="tt-type">' + info.lowerChamber + ': <span class="tt-party-d">' + info.lowerD + 'D</span> / <span class="tt-party-r">' + info.lowerR + 'R</span></div>';
        }"""
    else:  # officer (ltgov, ag, sos)
        tooltip_js = """
        let html = '<div class="tt-state">' + info.name + '</div>';
        const officerName = info.ltgov || info.ag || info.sos;
        if (officerName) {
          const cls = info.party === 'D' ? 'tt-party-d' : 'tt-party-r';
          html += '<div class="tt-governor">' + officerName + ' <span class="' + cls + '">(' + info.partyLabel + ')</span></div>';
        } else {
          html += '<div class="tt-type" style="opacity:0.7">No office / Vacant</div>';
        }"""

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{config["title"].replace('<span class="separator">|</span>', '|').replace('&amp;', '&')} | State Elections Tracker</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Work+Sans:wght@400;600;700;800&family=Merriweather:wght@300;400;700;900&display=swap" rel="stylesheet">
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: 'Work Sans', sans-serif; background: #ffffff; color: #043858; }}
  .map-page {{ max-width: 1400px; margin: 0 auto; padding: 40px 40px 30px; position: relative; }}
  .map-title {{
    font-family: 'Work Sans', sans-serif; font-weight: 800; font-size: 38px;
    color: #043858; margin-bottom: 24px; line-height: 1.1;
  }}
  .map-title .separator {{ color: #9bb5c4; font-weight: 400; margin: 0 6px; }}
  .map-layout {{ display: flex; flex-direction: column; align-items: center; }}
  .map-legend {{ display: flex; flex-wrap: wrap; gap: 6px 24px; margin-bottom: 12px; justify-content: center; }}
  .map-main {{ width: 100%; max-width: 1200px; margin-top: -20px; }}
  .legend-row {{ display: flex; align-items: center; gap: 10px; }}
  .legend-label {{ font-family: 'Work Sans', sans-serif; font-weight: 600; font-size: 15px; color: #043858; line-height: 1.3; white-space: nowrap; }}
  .legend-swatch {{ width: 22px; height: 22px; border-radius: 2px; flex-shrink: 0; }}
  .source-text {{
    font-family: 'Merriweather', serif; font-weight: 300; font-size: 13px;
    color: #666; line-height: 1.6; margin-top: 8px; text-align: center;
  }}
  .source-text strong {{ font-weight: 700; }}
  svg.us-map {{ width: 100%; height: auto; display: block; }}
  .state-group {{ cursor: pointer; }}
  .state-group:hover .state-fill path {{ filter: brightness(0.85); }}
  .state-group:hover .state-stroke path {{ stroke: #043858; stroke-width: 2; }}
  .state-label {{
    font-family: 'Work Sans', sans-serif; font-weight: 700; font-size: 24px;
    fill: white; text-anchor: middle; dominant-baseline: central; pointer-events: none;
  }}
  .tooltip {{
    position: fixed; background: #043858; color: white; padding: 12px 16px;
    border-radius: 6px; font-family: 'Work Sans', sans-serif; font-size: 13px;
    line-height: 1.5; pointer-events: none; opacity: 0; transition: opacity 0.15s;
    z-index: 1000; max-width: 300px; box-shadow: 0 4px 12px rgba(0,0,0,0.2);
  }}
  .tooltip.visible {{ opacity: 1; }}
  .tooltip .tt-state {{ font-weight: 700; font-size: 15px; margin-bottom: 4px; }}
  .tooltip .tt-type {{ font-weight: 400; opacity: 0.9; }}
  .tooltip .tt-governor {{ font-weight: 400; opacity: 0.9; }}
  .tooltip .tt-forecast {{
    margin-top: 6px; padding-top: 6px;
    border-top: 1px solid rgba(255,255,255,0.2); font-weight: 600; font-size: 12px;
  }}
  .tt-party-d {{ color: #6cd3d2; }}
  .tt-party-r {{ color: #e86987; }}
  .logo-area {{ position: absolute; bottom: 30px; right: 40px; }}
  .logo-area img {{ width: 50px; height: auto; }}
  @media (max-width: 900px) {{
    .map-title {{ font-size: 28px; }}
    .map-legend {{ gap: 4px 16px; }}
    .legend-label {{ font-size: 13px; }}
  }}

  .site-nav {{
    display: flex;
    align-items: center;
    gap: 16px;
    margin-bottom: 20px;
    padding-bottom: 12px;
    border-bottom: 1px solid #ddd;
    flex-wrap: wrap;
  }}
  .site-nav a.nav-home {{
    display: flex;
    align-items: center;
    gap: 8px;
    text-decoration: none;
    color: #043858;
    font-weight: 800;
    font-size: 16px;
    margin-right: 8px;
  }}
  .site-nav a.nav-home img {{ width: 32px; height: auto; }}
  .site-nav .nav-sep {{ color: #ccc; font-size: 14px; }}
  .site-nav a.nav-link {{
    font-size: 13px;
    font-weight: 600;
    color: #276181;
    text-decoration: none;
    padding: 4px 0;
  }}
  .site-nav a.nav-link:hover {{ text-decoration: underline; }}
  .site-nav a.nav-link.active {{ color: #043858; border-bottom: 2px solid #043858; }}

  /* Embed mode */
  body.embed .site-nav {{ display: none; }}
  body.embed .map-title {{ display: none; }}
  body.embed .map-page {{ padding: 16px 16px 10px; }}
  body.embed .map-main {{ margin-top: -10px; }}
  body.embed .logo-area {{ display: none; }}
  body.embed .source-text {{ font-size: 11px; }}
  body.embed .legend-label {{ font-size: 13px; }}
  body.embed .legend-swatch {{ width: 18px; height: 18px; }}
</style>
</head>
<body>
<script>if (window.location.search.includes('embed')) document.body.classList.add('embed');</script>
<div class="map-page">
  <div class="site-nav">
    <a class="nav-home" href="index.html"><img src="assets/multistate-logomark.png" alt="">Elections Tracker</a>
    <span class="nav-sep">|</span>
{nav_html}
  </div>
  <h1 class="map-title">{config["title"]}</h1>
  <div class="map-layout">
    <div class="map-legend">
{legend_html}    </div>
    <div class="map-main">
      <svg class="us-map" viewBox="0 0 2207.76 1115.45" xmlns="http://www.w3.org/2000/svg">
        <g id="states">
          {states_svg}
        </g>
        <g id="labels">
          {labels_svg}
        </g>
      </svg>
    </div>
    <div class="source-text">
{config["source"]}
    </div>
  </div>
  <div class="logo-area">
    <img src="assets/multistate-logomark.png" alt="MultiState" />
  </div>
</div>
<div class="tooltip" id="tooltip"></div>
<script>
    const tooltip = document.getElementById('tooltip');
    document.querySelectorAll('.state-group').forEach(g => {{
      g.addEventListener('mouseenter', e => {{
        const raw = g.getAttribute('data-info');
        if (!raw) return;
        const info = JSON.parse(raw);
        {tooltip_js}
        tooltip.innerHTML = html;
        tooltip.classList.add('visible');
      }});
      g.addEventListener('mousemove', e => {{
        tooltip.style.left = (e.clientX + 14) + 'px';
        tooltip.style.top = (e.clientY + 14) + 'px';
      }});
      g.addEventListener('mouseleave', () => tooltip.classList.remove('visible'));
      g.addEventListener('click', () => {{
        const st = g.getAttribute('data-state');
        if (st) window.location.href = 'state.html?st=' + st;
      }});
    }});
    const now = new Date();
    const months = ['Jan.','Feb.','Mar.','Apr.','May','June','July','Aug.','Sept.','Oct.','Nov.','Dec.'];
    const dateEl = document.getElementById('data-date');
    if (dateEl) dateEl.textContent = months[now.getMonth()] + ' ' + now.getDate() + ', ' + now.getFullYear();
</script>
</body>
</html>'''

    outpath = f'{SITE_DIR}/{config["filename"]}'
    with open(outpath, 'w') as f:
        f.write(html)
    print(f'  Written: {config["filename"]}')


# ── Main ─────────────────────────────────────────────────────────────

if __name__ == '__main__':
    configs = [
        ('Trifectas', get_trifecta_config),
        ('Governors (partisan)', get_governors_config),
        ('Governors 2026', get_governors_2026_config),
        ('Lt. Governors', get_ltgov_config),
        ('Attorneys General', get_ag_config),
        ('Secretaries of State', get_sos_config),
        ('Legislatures', get_legislatures_config),
    ]

    for name, fn in configs:
        print(f'Building {name} map...')
        config = fn()
        build_map_html(config)

    print('Done! All 7 maps rebuilt.')
