#!/usr/bin/env python3
"""Build inline map HTML blocks for 101 pages.
Each block contains an SVG map with JS-driven tab switching between views.

Outputs to /tmp/inline_{topic}_block.html for each topic.
These blocks get injected into the corresponding 101 pages.

Topics:
  - governors: Partisan + 2026 Elections tabs
  - legislatures: Partisan + Trifectas tabs
  - trifectas: redirects to legislatures (same data, different framing)
  - ag: Partisan only (single view, no tabs)
  - ltgov: Partisan only (single view, no tabs)
  - sos: Partisan only (single view, no tabs)
"""

import re, json

SITE_DIR = '/home/billkramer/elections/site'

# Read state summary data
with open(f'{SITE_DIR}/data/states_summary.json') as f:
    summary = json.load(f)
STATES_DATA = summary['states']

# Read a generated map to get SVG with correct state groups
with open(f'{SITE_DIR}/ag_partisan.html') as f:
    content = f.read()

# Extract the full SVG
svg_match = re.search(r'(<svg class="us-map".*?</svg>)', content, re.DOTALL)
svg = svg_match.group(1)

# Strip data-category and data-info attributes (JS will handle coloring)
svg = re.sub(r' data-category="[^"]*"', '', svg)
svg = re.sub(r" data-info='[^']*'", '', svg)

# Reset all fills to gray
svg = re.sub(r'(<g class="state-fill" fill=")[^"]*"', r'\1#d9d9d9"', svg)

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

LOWER_NAMES = {'CA':'Assembly','NV':'Assembly','NY':'Assembly','WI':'Assembly','NJ':'Assembly',
               'MD':'House of Delegates','VA':'House of Delegates','WV':'House of Delegates',
               'NE':'Legislature'}


def js_obj(d):
    """Format a dict as a compact JS object literal."""
    return ','.join(f"{k}:'{v}'" for k, v in sorted(d.items()))


def get_governor_states():
    """Returns (partisan_states, 2026_states) as JS-ready dicts."""
    partisan = {}
    for abbr, st in STATES_DATA.items():
        party = st.get('governor', {}).get('party', '')
        partisan[abbr] = 'dem' if party == 'D' else 'gop'

    # 2026 election categories
    OPEN_SEATS = {
        'AK', 'AL', 'CA', 'CO', 'FL', 'GA', 'HI', 'IA', 'KS', 'ME',
        'MI', 'MN', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'SD', 'TN', 'WI', 'WY',
    }
    elec_2026 = {}
    for abbr, st in STATES_DATA.items():
        gov = st.get('governor', {})
        party = gov.get('party', '')
        next_el = gov.get('next_election')
        if next_el != 2026:
            elec_2026[abbr] = 'no_election'
        elif abbr in OPEN_SEATS:
            elec_2026[abbr] = f'{"dem" if party == "D" else "gop"}_open'
        else:
            elec_2026[abbr] = f'{"dem" if party == "D" else "gop"}_incumbent'

    return partisan, elec_2026


def get_legislature_states():
    """Returns (partisan_states, trifecta_states)."""
    partisan = {}
    for abbr, st in STATES_DATA.items():
        ch = st.get('chambers', {})
        if abbr == 'NE':
            partisan[abbr] = 'gop'
            continue
        upper = ch.get('Senate', {})
        lower = None
        for k, v in ch.items():
            if k != 'Senate':
                lower = v
                break
        if not lower:
            lower = {}
        upper_ctrl = 'D' if upper.get('d', 0) > upper.get('r', 0) else 'R'
        lower_ctrl = 'D' if lower.get('d', 0) > lower.get('r', 0) else 'R'
        if upper_ctrl == lower_ctrl:
            partisan[abbr] = 'dem' if upper_ctrl == 'D' else 'gop'
        else:
            partisan[abbr] = 'split'

    trifecta = {}
    for abbr, st in STATES_DATA.items():
        tri = st.get('trifecta', '')
        if 'Democratic' in tri:
            trifecta[abbr] = 'dem_trifecta'
        elif 'Republican' in tri:
            trifecta[abbr] = 'gop_trifecta'
        else:
            trifecta[abbr] = 'divided'

    return partisan, trifecta


def get_officer_states(office_key):
    """Returns states dict for a statewide officer."""
    states = {}
    for abbr, st in STATES_DATA.items():
        officers = st.get('statewide_officers', [])
        officer = next((o for o in officers if o['office'] == office_key), None)
        if officer and officer.get('name') and officer.get('party') == 'D':
            states[abbr] = 'dem'
        elif officer and officer.get('name') and officer.get('party') == 'R':
            states[abbr] = 'gop'
        else:
            states[abbr] = 'none'
    return states


def build_governors_block():
    """Governors: 2 tabs — Partisan + 2026 Elections."""
    partisan, elec_2026 = get_governor_states()

    # For partisan view, only need states != default (gop since it's majority)
    # Actually just include all for clarity since there are only 2 categories
    p_states = {k: v for k, v in partisan.items() if v == 'dem'}
    e_states = elec_2026  # all states needed since 5 categories

    map_svg = svg.replace('class="us-map"', 'class="us-map" id="gov-map"', 1)

    return f'''    <!-- Inline Governors Map -->
    <div class="inline-map-section" style="margin:28px 0">
      <div class="inline-map-tabs" style="display:flex;gap:8px;margin-bottom:16px">
        <button class="inline-tab active" data-view="partisan" onclick="switchInlineMap('partisan')">Partisan Breakdown</button>
        <button class="inline-tab" data-view="elections" onclick="switchInlineMap('elections')">2026 Elections</button>
      </div>
      <div class="inline-map-wrapper">
        <div class="inline-map-legend" id="inline-legend"></div>
        <div class="inline-map-main">
          {map_svg}
        </div>
      </div>
      <div class="inline-map-source" id="inline-source"></div>
    </div>
    <div class="inline-tooltip" id="inline-tooltip"></div>

    <script>
    (function() {{
      const PARTISAN = {{
        colors: {{ dem: '#276181', gop: '#e50963' }},
        defaultCategory: 'gop',
        legend: [['dem', 'Democratic Control'], ['gop', 'Republican Control']],
        source: 'Source: MultiState.',
        states: {{ {js_obj(p_states)} }},
        tooltips: {{ dem: 'Democratic Governor', gop: 'Republican Governor' }}
      }};

      const ELECTIONS = {{
        colors: {{ dem_incumbent: '#276181', dem_open: '#6cb3d2', gop_incumbent: '#e50963', gop_open: '#e86987', no_election: '#e4e4e4' }},
        defaultCategory: 'no_election',
        legend: [['dem_incumbent','Dem. Incumbent'],['dem_open','Dem. Open Seat'],['gop_incumbent','GOP Incumbent'],['gop_open','GOP Open Seat'],['no_election','No 2026 Election']],
        source: 'Source: MultiState. 36 gubernatorial seats up in 2026.',
        states: {{ {js_obj(e_states)} }},
        tooltips: {{ dem_incumbent:'Democratic Incumbent', dem_open:'Democratic Open Seat', gop_incumbent:'Republican Incumbent', gop_open:'Republican Open Seat', no_election:'No 2026 Gubernatorial Election' }}
      }};

      const NAMES = {json.dumps(STATE_NAMES)};
      const CONFIGS = {{ partisan: PARTISAN, elections: ELECTIONS }};
      let currentView = 'partisan';

      window.switchInlineMap = function(view) {{
        currentView = view;
        const config = CONFIGS[view];
        document.querySelectorAll('.inline-tab').forEach(t => t.classList.toggle('active', t.getAttribute('data-view') === view));
        document.querySelectorAll('#gov-map .state-group').forEach(g => {{
          const st = g.getAttribute('data-state');
          const cat = config.states[st] || config.defaultCategory;
          g.querySelectorAll('.state-fill').forEach(fg => fg.setAttribute('fill', config.colors[cat]));
          g.querySelectorAll(':scope > path').forEach(p => p.setAttribute('fill', config.colors[cat]));
        }});
        let legendHtml = '';
        for (const [cat, label] of config.legend) legendHtml += '<div class="inline-legend-row"><div class="inline-legend-swatch" style="background:' + config.colors[cat] + '"></div><div class="inline-legend-label">' + label + '</div></div>';
        document.getElementById('inline-legend').innerHTML = legendHtml;
        document.getElementById('inline-source').textContent = config.source;
      }};

      const tooltip = document.getElementById('inline-tooltip');
      document.querySelectorAll('#gov-map .state-group').forEach(g => {{
        g.addEventListener('mouseenter', e => {{
          const st = g.getAttribute('data-state');
          const config = CONFIGS[currentView];
          const cat = config.states[st] || config.defaultCategory;
          tooltip.innerHTML = '<div class="inline-tt-state">' + (NAMES[st]||st) + '</div><div class="inline-tt-type">' + (config.tooltips[cat]||cat) + '</div>';
          tooltip.classList.add('visible');
        }});
        g.addEventListener('mousemove', e => {{ tooltip.style.left = (e.clientX+14)+'px'; tooltip.style.top = (e.clientY+14)+'px'; }});
        g.addEventListener('mouseleave', () => tooltip.classList.remove('visible'));
      }});
      switchInlineMap('partisan');
    }})();
    </script>'''


def build_legislatures_block():
    """Legislatures/Trifectas: 2 tabs — Legislative Control + Trifectas."""
    partisan, trifecta = get_legislature_states()

    # Only store non-default states
    p_states = {k: v for k, v in partisan.items() if v != 'gop'}
    t_states = {k: v for k, v in trifecta.items() if v != 'gop_trifecta'}

    map_svg = svg.replace('class="us-map"', 'class="us-map" id="leg-map"', 1)

    return f'''    <!-- Inline Legislatures Map -->
    <div class="inline-map-section" style="margin:28px 0">
      <div class="inline-map-tabs" style="display:flex;gap:8px;margin-bottom:16px">
        <button class="inline-tab active" data-view="partisan" onclick="switchLegMap('partisan')">Legislative Control</button>
        <button class="inline-tab" data-view="trifecta" onclick="switchLegMap('trifecta')">Trifectas</button>
      </div>
      <div class="inline-map-wrapper">
        <div class="inline-map-legend" id="leg-legend"></div>
        <div class="inline-map-main">
          {map_svg}
        </div>
      </div>
      <div class="inline-map-source" id="leg-source"></div>
    </div>
    <div class="inline-tooltip" id="leg-tooltip"></div>

    <script>
    (function() {{
      const PARTISAN = {{
        colors: {{ dem: '#276181', gop: '#e50963', split: '#8b567f' }},
        defaultCategory: 'gop',
        legend: [['dem', 'Democratic Control'], ['gop', 'Republican Control'], ['split', 'Split Control']],
        source: 'Source: MultiState.',
        states: {{ {js_obj(p_states)} }},
        tooltips: {{ dem: 'Democratic-controlled legislature', gop: 'Republican-controlled legislature', split: 'Split legislature control' }}
      }};

      const TRIFECTA = {{
        colors: {{ dem_trifecta: '#6cb3d2', gop_trifecta: '#e50963', divided: '#8b567f' }},
        defaultCategory: 'gop_trifecta',
        legend: [['dem_trifecta', 'Democratic Trifecta'], ['gop_trifecta', 'Republican Trifecta'], ['divided', 'Divided Government']],
        source: 'Source: MultiState.',
        states: {{ {js_obj(t_states)} }},
        tooltips: {{ dem_trifecta: 'Democratic trifecta (governor + both chambers)', gop_trifecta: 'Republican trifecta (governor + both chambers)', divided: 'Divided government (split party control)' }}
      }};

      const NAMES = {json.dumps(STATE_NAMES)};
      const CONFIGS = {{ partisan: PARTISAN, trifecta: TRIFECTA }};
      let currentView = 'partisan';

      window.switchLegMap = function(view) {{
        currentView = view;
        const config = CONFIGS[view];
        document.querySelectorAll('#leg-map').forEach(m => m.closest('.inline-map-section').querySelectorAll('.inline-tab').forEach(t => t.classList.toggle('active', t.getAttribute('data-view') === view)));
        document.querySelectorAll('#leg-map .state-group').forEach(g => {{
          const st = g.getAttribute('data-state');
          const cat = config.states[st] || config.defaultCategory;
          g.querySelectorAll('.state-fill').forEach(fg => fg.setAttribute('fill', config.colors[cat]));
          g.querySelectorAll(':scope > path').forEach(p => p.setAttribute('fill', config.colors[cat]));
        }});
        let legendHtml = '';
        for (const [cat, label] of config.legend) legendHtml += '<div class="inline-legend-row"><div class="inline-legend-swatch" style="background:' + config.colors[cat] + '"></div><div class="inline-legend-label">' + label + '</div></div>';
        document.getElementById('leg-legend').innerHTML = legendHtml;
        document.getElementById('leg-source').textContent = config.source;
      }};

      const tooltip = document.getElementById('leg-tooltip');
      document.querySelectorAll('#leg-map .state-group').forEach(g => {{
        g.addEventListener('mouseenter', e => {{
          const st = g.getAttribute('data-state');
          const config = CONFIGS[currentView];
          const cat = config.states[st] || config.defaultCategory;
          tooltip.innerHTML = '<div class="inline-tt-state">' + (NAMES[st]||st) + '</div><div class="inline-tt-type">' + (config.tooltips[cat]||cat) + '</div>';
          tooltip.classList.add('visible');
        }});
        g.addEventListener('mousemove', e => {{ tooltip.style.left = (e.clientX+14)+'px'; tooltip.style.top = (e.clientY+14)+'px'; }});
        g.addEventListener('mouseleave', () => tooltip.classList.remove('visible'));
      }});
      switchLegMap('partisan');
    }})();
    </script>'''


def build_officer_block(topic, office_key, title, map_id):
    """Single-view officer map (no tabs)."""
    states = get_officer_states(office_key)
    has_none = any(v == 'none' for v in states.values())

    # Only store non-default states
    default = 'gop'
    non_default = {k: v for k, v in states.items() if v != default}

    colors = "dem: '#276181', gop: '#e50963'"
    legend = "[['dem', 'Democratic Control'], ['gop', 'Republican Control']"
    tooltips = "dem: 'Democratic', gop: 'Republican'"
    if has_none:
        colors += ", none: '#e4e4e4'"
        legend += ", ['none', 'No Office / Vacant']"
        tooltips += ", none: 'No office or vacant'"
    legend += "]"

    map_svg = svg.replace('class="us-map"', f'class="us-map" id="{map_id}"', 1)

    return f'''    <!-- Inline {title} Map -->
    <div class="inline-map-section" style="margin:28px 0">
      <div class="inline-map-wrapper">
        <div class="inline-map-legend" id="{map_id}-legend"></div>
        <div class="inline-map-main">
          {map_svg}
        </div>
      </div>
      <div class="inline-map-source" id="{map_id}-source">Source: MultiState.</div>
    </div>
    <div class="inline-tooltip" id="{map_id}-tooltip"></div>

    <script>
    (function() {{
      const CONFIG = {{
        colors: {{ {colors} }},
        defaultCategory: '{default}',
        legend: {legend},
        states: {{ {js_obj(non_default)} }},
        tooltips: {{ {tooltips} }}
      }};

      const NAMES = {json.dumps(STATE_NAMES)};

      // Color the map
      document.querySelectorAll('#{map_id} .state-group').forEach(g => {{
        const st = g.getAttribute('data-state');
        const cat = CONFIG.states[st] || CONFIG.defaultCategory;
        const color = CONFIG.colors[cat];
        g.querySelectorAll('.state-fill').forEach(fg => fg.setAttribute('fill', color));
        g.querySelectorAll(':scope > path').forEach(p => p.setAttribute('fill', color));
      }});

      // Legend
      let legendHtml = '';
      for (const [cat, label] of CONFIG.legend) legendHtml += '<div class="inline-legend-row"><div class="inline-legend-swatch" style="background:' + CONFIG.colors[cat] + '"></div><div class="inline-legend-label">' + label + '</div></div>';
      document.getElementById('{map_id}-legend').innerHTML = legendHtml;

      // Tooltip
      const tooltip = document.getElementById('{map_id}-tooltip');
      document.querySelectorAll('#{map_id} .state-group').forEach(g => {{
        g.addEventListener('mouseenter', e => {{
          const st = g.getAttribute('data-state');
          const cat = CONFIG.states[st] || CONFIG.defaultCategory;
          tooltip.innerHTML = '<div class="inline-tt-state">' + (NAMES[st]||st) + '</div><div class="inline-tt-type">' + (CONFIG.tooltips[cat]||cat) + '</div>';
          tooltip.classList.add('visible');
        }});
        g.addEventListener('mousemove', e => {{ tooltip.style.left = (e.clientX+14)+'px'; tooltip.style.top = (e.clientY+14)+'px'; }});
        g.addEventListener('mouseleave', () => tooltip.classList.remove('visible'));
      }});
    }})();
    </script>'''


# ── Main ─────────────────────────────────────────────────────────────

if __name__ == '__main__':
    blocks = {
        'governors': ('Governors', build_governors_block),
        'legislatures': ('Legislatures/Trifectas', build_legislatures_block),
        'ag': ('Attorneys General', lambda: build_officer_block('ag', 'Attorney General', 'Attorneys General', 'ag-map')),
        'ltgov': ('Lt. Governors', lambda: build_officer_block('ltgov', 'Lt. Governor', 'Lt. Governors', 'ltgov-map')),
        'sos': ('Secretaries of State', lambda: build_officer_block('sos', 'Secretary of State', 'Secretaries of State', 'sos-map')),
    }

    for topic, (label, fn) in blocks.items():
        print(f'Building inline {label} block...')
        html = fn()
        outpath = f'/tmp/inline_{topic}_block.html'
        with open(outpath, 'w') as f:
            f.write(html)
        print(f'  Written: {outpath} ({len(html)} chars)')

    print('Done! All inline blocks generated.')
