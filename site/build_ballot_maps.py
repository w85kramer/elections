#!/usr/bin/env python3
"""Build ballot measure initiative + referendum maps from AG partisan map template.
Reads state classification data from data/ballot_auth.json (exported from DB)."""

import re, json

# Read ballot authorization data from DB export
with open('/home/billkramer/elections/site/data/ballot_auth.json') as f:
    auth_data = json.load(f)

# Read the AG map template
with open('/home/billkramer/elections/site/ag_partisan.html', 'r') as f:
    template = f.read()

# Extract just the SVG portion (state paths + labels)
svg_match = re.search(r'(<svg class="us-map".*?</svg>)', template, re.DOTALL)
svg_block = svg_match.group(1)

# Get all state path data
state_paths = {}
for m in re.finditer(r'<g class="state-group" data-state="(\w{2})"[^>]*>(.*?)\n\s*</g>', template, re.DOTALL):
    abbr = m.group(1)
    inner = m.group(2)
    # Extract fill group content (may contain multiple paths)
    fill_match = re.search(r'<g class="state-fill"[^>]*>(.*?)</g>', inner, re.DOTALL)
    stroke_match = re.search(r'<g class="state-stroke"[^>]*>(.*?)</g>', inner, re.DOTALL)
    # Extract any extra paths outside fill/stroke groups (e.g. small state label paths)
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
            'extra': extra
        }

# Extract labels section
labels = re.findall(r'<text[^>]*class="state-label"[^>]*>\w{2}</text>', template)

# State name lookup
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

# Build data dicts from JSON
INITIATIVE_COLORS = auth_data['initiative']['colors']
REFERENDUM_COLORS = auth_data['referendum']['colors']

# Initiative tooltips per category
INITIATIVE_TOOLTIPS = {
    'both': 'Both statutory and constitutional initiatives',
    'statutory': 'Statutory initiatives only',
    'constitutional': 'Constitutional initiatives only',
    'none': 'Does not authorize citizen initiatives',
}

REFERENDUM_TOOLTIPS = {
    'both': 'Legislative referendum for both statutes and constitutional amendments',
    'amendments_only': 'Legislative referendum for constitutional amendments only',
    'statutes_only': 'Legislative referendum for statutes only',
}

INITIATIVE_NOTES = {k: v for k, v in auth_data['notes'].items() if k != 'DE'}
REFERENDUM_NOTES = {'DE': auth_data['notes'].get('DE', '')}


def build_map_html(title, subtitle, state_categories, colors, default_category, tooltips, notes, legend_items, nav_active, filename):
    """Build a complete map HTML page."""

    # Build state SVG groups
    state_groups = []
    for abbr in ALL_STATES:
        if abbr not in state_paths:
            continue

        category = state_categories.get(abbr, default_category)
        color = colors[category]
        type_desc = tooltips.get(category, '')
        note = notes.get(abbr, '')

        data_info = json.dumps({
            'state': abbr,
            'name': STATE_NAMES.get(abbr, abbr),
            'type': type_desc,
            'category': category,
            'note': note,
        })

        paths = state_paths[abbr]
        extra = ''
        if paths['extra']:
            extra = f'\n      {paths["extra"]}'

        group = f'''<g class="state-group" data-state="{abbr}" data-category="{category}" data-info='{data_info}'>
      <g class="state-fill" fill="{color}">{paths['fill_content']}</g>
      <g class="state-stroke" fill="none" stroke="#ffffff" stroke-width="1.0">{paths['stroke_content']}</g>{extra}

    </g>'''
        state_groups.append(group)

    states_svg = '\n'.join(state_groups)
    labels_svg = '\n          '.join(labels)

    # Build legend HTML (horizontal layout)
    legend_html = ''
    for cat, label in legend_items.items():
        legend_html += f'''      <div class="legend-row">
        <div class="legend-swatch" style="background:{colors[cat]}"></div>
        <div class="legend-label">{label}</div>
      </div>\n'''

    # Determine active nav
    init_active = ' active' if nav_active == 'initiative' else ''
    ref_active = ' active' if nav_active == 'referendum' else ''

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} | State Elections Tracker</title>
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
  .tooltip .tt-note {{ font-weight: 400; opacity: 0.7; font-size: 12px; margin-top: 4px; font-style: italic; }}
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

  /* Embed mode: strip chrome when loaded in iframe via ?embed */
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
    <a class="nav-link" href="trifectas_partisan.html">Trifectas</a>
    <a class="nav-link" href="legislatures_partisan.html">Legislatures</a>
    <a class="nav-link" href="governors_2026.html">2026 Governors</a>
    <a class="nav-link" href="governors_partisan.html">Governors</a>
    <a class="nav-link" href="ltgov_partisan.html">Lt. Governors</a>
    <a class="nav-link" href="ag_partisan.html">Attorneys General</a>
    <a class="nav-link" href="sos_partisan.html">Secretaries of State</a>
    <span class="nav-sep">|</span>
    <a class="nav-link" href="swing-calculator.html">Swing Calc</a>
    <span class="nav-sep">|</span>
    <a class="nav-link{init_active}" href="ballot-measures-initiative-map.html">Initiative Auth.</a>
    <a class="nav-link{ref_active}" href="ballot-measures-referendum-map.html">Referendum Auth.</a>
    <span class="nav-sep">|</span>
    <a class="nav-link" href="ballot-measures-101.html">101 Guide</a>
  </div>
  <h1 class="map-title">{title}</h1>
  <div class="map-layout">
    <div class="map-legend">
{legend_html}    </div>
    <div class="map-main">
      <svg class="us-map" viewBox="480 0 1760 1115.45" xmlns="http://www.w3.org/2000/svg">
        <g id="states">
          {states_svg}
        </g>
        <g id="labels">
          {labels_svg}
        </g>
      </svg>
    </div>
    <div class="source-text">
Source: MultiState. Data as of <span id="data-date"></span>.{subtitle}
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
        let html = '<div class="tt-state">' + info.name + '</div>';
        html += '<div class="tt-type">' + info.type + '</div>';
        if (info.note) {{
          html += '<div class="tt-note">' + info.note + '</div>';
        }}
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
    document.getElementById('data-date').textContent = months[now.getMonth()] + ' ' + now.getDate() + ', ' + now.getFullYear();
</script>
</body>
</html>'''

    with open(f'/home/billkramer/elections/site/{filename}', 'w') as f:
        f.write(html)
    print(f'  Written: {filename}')


# ============================================================
# Build Initiative Map
# ============================================================
print('Building Initiative Authorization map...')
init_states = auth_data['initiative']['states']
build_map_html(
    title='Ballot Measures <span class="separator">|</span> Initiative Authorization',
    subtitle='<br><strong>Note:</strong> The Mississippi state supreme court blocked the state\'s constitutional initiative process in 2021.',
    state_categories=init_states,
    colors=INITIATIVE_COLORS,
    default_category='none',
    tooltips=INITIATIVE_TOOLTIPS,
    notes=INITIATIVE_NOTES,
    legend_items=auth_data['initiative']['legend'],
    nav_active='initiative',
    filename='ballot-measures-initiative-map.html',
)

# ============================================================
# Build Referendum Map
# ============================================================
print('Building Referendum Authorization map...')
ref_states = auth_data['referendum']['states']
build_map_html(
    title='Ballot Measures <span class="separator">|</span> Referendum Authorization',
    subtitle='',
    state_categories=ref_states,
    colors=REFERENDUM_COLORS,
    default_category='amendments_only',
    tooltips=REFERENDUM_TOOLTIPS,
    notes=REFERENDUM_NOTES,
    legend_items=auth_data['referendum']['legend'],
    nav_active='referendum',
    filename='ballot-measures-referendum-map.html',
)

print('Done!')
