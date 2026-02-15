#!/usr/bin/env python3
"""Build placeholder pages for each topic's three-view structure:
  1. Data page (placeholder)
  2. Analytics page (placeholder)
  3. 101 Guide page (with inline map)

Mirrors the ballot measures pattern:
  ballot-measures.html → ballot-measures-analytics.html → ballot-measures-101.html

Pages generated per topic:
  - {topic}.html (data placeholder)
  - {topic}-analytics.html (analytics placeholder)
  - {topic}-101.html (101 guide with inline map)
"""

import os

SITE_DIR = '/home/billkramer/elections/site'


def read_inline_block(topic):
    """Read inline map block from /tmp/."""
    path = f'/tmp/inline_{topic}_block.html'
    if os.path.exists(path):
        with open(path) as f:
            return f.read()
    return '    <p style="color:#999;font-style:italic">Map block not yet generated. Run build_inline_maps.py first.</p>'


# ── Topic definitions ────────────────────────────────────────────────

TOPICS = [
    {
        'slug': 'governors',
        'label': 'Governors',
        'title_101': 'Governors 101',
        'subtitle_101': 'A Guide to Gubernatorial Power in the American States',
        'title_data': 'Governors',
        'subtitle_data': '2026 gubernatorial races, incumbents, and partisan breakdown',
        'title_analytics': 'Governors Analytics',
        'subtitle_analytics': 'Trends, historical data, and analysis of gubernatorial races',
        'inline_topic': 'governors',
        'map_links': [
            ('governors_partisan.html', 'Partisan Map'),
            ('governors_2026.html', '2026 Races Map'),
        ],
        'placeholder_101': 'This guide will cover the role of the governor, powers and authorities, term limits, succession rules, and the 2026 gubernatorial landscape.',
        'placeholder_data': 'This page will feature 2026 gubernatorial race data, incumbent information, and key race ratings.',
        'placeholder_analytics': 'This page will feature historical gubernatorial trends, term limit analysis, and partisan shift data.',
    },
    {
        'slug': 'legislatures',
        'label': 'Legislatures',
        'title_101': 'State Legislatures 101',
        'subtitle_101': 'A Guide to Legislative Power in the American States',
        'title_data': 'State Legislatures',
        'subtitle_data': 'Chamber control, seat counts, and the 2026 legislative landscape',
        'title_analytics': 'Legislatures Analytics',
        'subtitle_analytics': 'Trends, historical data, and analysis of state legislative control',
        'inline_topic': 'legislatures',
        'map_links': [
            ('legislatures_partisan.html', 'Partisan Map'),
        ],
        'placeholder_101': 'This guide will cover legislative structure, session calendars, the legislative process, redistricting, and partisan dynamics.',
        'placeholder_data': 'This page will feature state legislative seat counts, chamber control status, and 2026 races.',
        'placeholder_analytics': 'This page will feature historical legislative control trends, seat flip analysis, and redistricting impacts.',
    },
    {
        'slug': 'trifectas',
        'label': 'Trifectas',
        'title_101': 'Trifectas 101',
        'subtitle_101': 'A Guide to Unified Party Control in the American States',
        'title_data': 'Trifectas',
        'subtitle_data': 'Unified party control of state government across all 50 states',
        'title_analytics': 'Trifectas Analytics',
        'subtitle_analytics': 'Trends, historical data, and analysis of trifecta control',
        'inline_topic': 'legislatures',  # reuse legislatures map (trifecta tab)
        'map_links': [
            ('trifectas_partisan.html', 'Trifecta Map'),
        ],
        'placeholder_101': 'This guide will cover what trifectas are, why they matter for policy, historical trends, and the states to watch in 2026.',
        'placeholder_data': 'This page will feature current trifecta status, states at risk of flipping, and policy implications.',
        'placeholder_analytics': 'This page will feature historical trifecta trends, policy output analysis, and 2026 projections.',
    },
    {
        'slug': 'ag',
        'label': 'Attorneys General',
        'title_101': 'Attorneys General 101',
        'subtitle_101': 'A Guide to the Role of State Attorneys General',
        'title_data': 'Attorneys General',
        'subtitle_data': 'Partisan breakdown and 2026 races for state attorneys general',
        'title_analytics': 'Attorneys General Analytics',
        'subtitle_analytics': 'Trends, historical data, and analysis of AG races',
        'inline_topic': 'ag',
        'map_links': [
            ('ag_partisan.html', 'Partisan Map'),
        ],
        'placeholder_101': 'This guide will cover the role of the attorney general, selection methods, key powers, multistate litigation, and the political significance of the office.',
        'placeholder_data': 'This page will feature current AGs, 2026 races, and selection method details.',
        'placeholder_analytics': 'This page will feature historical AG partisan trends, multistate litigation patterns, and election analysis.',
    },
    {
        'slug': 'ltgov',
        'label': 'Lt. Governors',
        'title_101': 'Lt. Governors 101',
        'subtitle_101': 'A Guide to the Role of Lieutenant Governors',
        'title_data': 'Lt. Governors',
        'subtitle_data': 'Partisan breakdown, selection methods, and powers across all 50 states',
        'title_analytics': 'Lt. Governors Analytics',
        'subtitle_analytics': 'Trends, historical data, and analysis of lt. governor races',
        'inline_topic': 'ltgov',
        'map_links': [
            ('ltgov_partisan.html', 'Partisan Map'),
        ],
        'placeholder_101': 'This guide will cover the role of the lieutenant governor, selection methods, powers and duties, and how the office varies across states.',
        'placeholder_data': 'This page will feature current lt. governors, selection methods, and 2026 races.',
        'placeholder_analytics': 'This page will feature historical lt. governor trends and selection method analysis.',
    },
    {
        'slug': 'sos',
        'label': 'Secretaries of State',
        'title_101': 'Secretaries of State 101',
        'subtitle_101': 'A Guide to the Role of State Secretaries of State',
        'title_data': 'Secretaries of State',
        'subtitle_data': 'Partisan breakdown, election administration, and 2026 races',
        'title_analytics': 'Secretaries of State Analytics',
        'subtitle_analytics': 'Trends, historical data, and analysis of SoS races',
        'inline_topic': 'sos',
        'map_links': [
            ('sos_partisan.html', 'Partisan Map'),
        ],
        'placeholder_101': 'This guide will cover the role of the secretary of state, election administration responsibilities, business filing duties, and the political significance of the office.',
        'placeholder_data': 'This page will feature current secretaries of state, election admin roles, and 2026 races.',
        'placeholder_analytics': 'This page will feature historical SoS partisan trends and election administration analysis.',
    },
]


def build_subnav(slug, active, elections_label='2026 Elections'):
    """Build subnav HTML. active is one of 'overview', 'elections', 'analytics'."""
    items = [
        (f'{slug}-101.html', 'Overview', 'overview'),
        (f'{slug}.html', elections_label, 'elections'),
        (f'{slug}-analytics.html', 'Trends & Analytics', 'analytics'),
    ]
    html = ''
    for href, label, key in items:
        cls = ' class="active"' if key == active else ''
        html += f'    <a href="{href}"{cls}>{label}</a>\n'
    return html


def build_placeholder_page(topic, page_type):
    """Build a placeholder page (data or analytics)."""
    slug = topic['slug']
    label = topic['label']

    if page_type == 'data':
        filename = f'{slug}.html'
        title = topic['title_data']
        subtitle = topic['subtitle_data']
        placeholder_text = topic['placeholder_data']
        bc_trail = f'<a href="{slug}-101.html">{label}</a> &rsaquo; 2026 Elections'
        active = 'elections'
    elif page_type == 'analytics':
        filename = f'{slug}-analytics.html'
        title = topic['title_analytics']
        subtitle = topic['subtitle_analytics']
        placeholder_text = topic['placeholder_analytics']
        bc_trail = f'<a href="{slug}-101.html">{label}</a> &rsaquo; Trends &amp; Analytics'
        active = 'analytics'

    subnav_html = build_subnav(slug, active)

    # Map links
    map_links_html = ''
    for href, map_label in topic.get('map_links', []):
        map_links_html += f'      <a href="{href}" style="display:inline-block;padding:6px 14px;border:1px solid var(--ms-border);border-radius:var(--ms-radius);font-size:13px;font-weight:600;text-decoration:none;color:var(--ms-dem-blue);transition:all 0.15s">{map_label} &rarr;</a>\n'

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} | State Elections Tracker</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Work+Sans:wght@400;600;700;800&family=Merriweather:wght@300;400;700;900&display=swap" rel="stylesheet">
<link rel="stylesheet" href="css/common.css">
<style>
  .subnav {{
    display: flex;
    gap: 8px;
    margin-bottom: 32px;
    flex-wrap: wrap;
  }}
  .subnav a {{
    display: inline-block;
    padding: 8px 18px;
    border: 2px solid var(--ms-border);
    border-radius: var(--ms-radius);
    font-family: 'Work Sans', sans-serif;
    font-weight: 600;
    font-size: 13px;
    text-decoration: none;
    color: var(--ms-text-light);
    transition: all 0.15s;
  }}
  .subnav a:hover {{ border-color: var(--ms-navy); color: var(--ms-navy); }}
  .subnav a.active {{ background: var(--ms-navy); border-color: var(--ms-navy); color: white; }}
  .placeholder-notice {{
    background: #fff8e1;
    border: 2px dashed #d0ac60;
    border-radius: var(--ms-radius);
    padding: 24px;
    margin: 32px 0;
    text-align: center;
  }}
  .placeholder-notice p {{
    font-family: 'Work Sans', sans-serif;
    font-weight: 600;
    font-size: 14px;
    color: #856404;
    margin-bottom: 8px;
  }}
  .placeholder-notice p:last-child {{ margin-bottom: 0; }}
  .map-links {{
    display: flex;
    gap: 8px;
    margin-top: 16px;
    justify-content: center;
  }}
</style>
</head>
<body>
<div class="page-container">

  <div class="site-header">
    <a href="index.html"><img src="assets/multistate-logomark.png" alt="MultiState"></a>
    <a href="index.html" class="site-title">State Elections Tracker</a>
  </div>

  <div class="breadcrumb">
    <a href="index.html">Home</a> &rsaquo; {bc_trail}
  </div>

  <div class="subnav">
{subnav_html}  </div>

  <h1 class="heading-xl" style="margin-bottom: 8px">{title}</h1>
  <p class="subtitle mb-16">{subtitle}</p>

  <div class="placeholder-notice">
    <p>Content coming soon.</p>
    <p style="font-weight:400;font-size:13px;color:#666">{placeholder_text}</p>
    <div class="map-links">
{map_links_html}    </div>
  </div>

  <div class="site-footer">
    <img src="assets/multistate-logomark.png" alt="MultiState">
    <div class="footer-text">State Elections Tracker &mdash; Data compiled by MultiState</div>
  </div>

</div>
</body>
</html>'''

    outpath = f'{SITE_DIR}/{filename}'
    with open(outpath, 'w') as f:
        f.write(html)
    print(f'  Written: {filename}')


def build_101_page(topic):
    """Build a 101 guide page with inline map."""
    slug = topic['slug']
    label = topic['label']
    inline_block = read_inline_block(topic['inline_topic'])
    subnav_html = build_subnav(slug, 'overview')
    css_prefix = 'inline'

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{topic["title_101"]} | State Elections Tracker</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Work+Sans:wght@400;600;700;800&family=Merriweather:wght@300;400;700;900&display=swap" rel="stylesheet">
<link rel="stylesheet" href="css/common.css">
<style>
  .article-body {{
    max-width: 820px;
  }}
  .article-body p {{
    font-family: 'Merriweather', serif;
    font-weight: 300;
    font-size: 15px;
    line-height: 1.8;
    color: var(--ms-text);
    margin-bottom: 20px;
  }}
  .article-body h2 {{
    font-family: 'Work Sans', sans-serif;
    font-weight: 800;
    font-size: 26px;
    color: var(--ms-navy);
    margin-top: 48px;
    margin-bottom: 16px;
    line-height: 1.2;
  }}
  .article-body h3 {{
    font-family: 'Work Sans', sans-serif;
    font-weight: 700;
    font-size: 19px;
    color: var(--ms-navy);
    margin-top: 32px;
    margin-bottom: 12px;
    line-height: 1.3;
  }}
  .article-body ul, .article-body ol {{
    font-family: 'Merriweather', serif;
    font-weight: 300;
    font-size: 15px;
    line-height: 1.8;
    color: var(--ms-text);
    margin-bottom: 20px;
    padding-left: 24px;
  }}
  .article-body li {{ margin-bottom: 8px; }}
  .article-body a {{
    color: var(--ms-dem-blue);
    text-decoration: none;
    font-weight: 400;
  }}
  .article-body a:hover {{ text-decoration: underline; }}
  .lead {{
    font-size: 17px !important;
    font-weight: 400 !important;
    color: var(--ms-text) !important;
    line-height: 1.7 !important;
    margin-bottom: 32px !important;
  }}
  .subnav {{
    display: flex;
    gap: 8px;
    margin-bottom: 32px;
    flex-wrap: wrap;
  }}
  .subnav a {{
    display: inline-block;
    padding: 8px 18px;
    border: 2px solid var(--ms-border);
    border-radius: var(--ms-radius);
    font-family: 'Work Sans', sans-serif;
    font-weight: 600;
    font-size: 13px;
    text-decoration: none;
    color: var(--ms-text-light);
    transition: all 0.15s;
  }}
  .subnav a:hover {{ border-color: var(--ms-navy); color: var(--ms-navy); }}
  .subnav a.active {{ background: var(--ms-navy); border-color: var(--ms-navy); color: white; }}

  .{css_prefix}-map-section {{ width: 100%; }}
  .{css_prefix}-tab {{
    padding: 8px 16px;
    border: 2px solid var(--ms-border);
    border-radius: var(--ms-radius);
    font-family: 'Work Sans', sans-serif;
    font-weight: 600;
    font-size: 13px;
    cursor: pointer;
    background: white;
    color: var(--ms-text-light);
    transition: all 0.15s;
  }}
  .{css_prefix}-tab:hover {{ border-color: var(--ms-navy); color: var(--ms-navy); }}
  .{css_prefix}-tab.active {{ background: var(--ms-navy); border-color: var(--ms-navy); color: white; }}
  .{css_prefix}-map-wrapper {{
    display: flex;
    flex-direction: column;
  }}
  .{css_prefix}-map-legend {{
    display: flex;
    flex-wrap: wrap;
    gap: 6px 20px;
    margin-bottom: 12px;
    justify-content: center;
  }}
  .{css_prefix}-map-main {{ margin-top: -20px; width: 100%; }}
  .{css_prefix}-map-main svg {{ width: 100%; height: auto; display: block; }}
  .{css_prefix}-legend-row {{ display: flex; align-items: center; gap: 8px; }}
  .{css_prefix}-legend-swatch {{ width: 16px; height: 16px; border-radius: 2px; flex-shrink: 0; }}
  .{css_prefix}-legend-label {{
    font-family: 'Work Sans', sans-serif;
    font-weight: 600;
    font-size: 13px;
    color: var(--ms-navy);
    line-height: 1.3;
    white-space: nowrap;
  }}
  .{css_prefix}-map-source {{
    font-family: 'Merriweather', serif;
    font-weight: 300;
    font-size: 12px;
    color: #666;
    margin-top: 8px;
  }}
  .state-group {{ cursor: pointer; }}
  .state-group:hover .state-fill path {{ filter: brightness(0.85); }}
  .state-group:hover .state-stroke path {{ stroke: #043858; stroke-width: 2; }}
  .state-label {{
    font-family: 'Work Sans', sans-serif; font-weight: 700; font-size: 24px;
    fill: white; text-anchor: middle; dominant-baseline: central; pointer-events: none;
  }}
  .{css_prefix}-tooltip {{
    position: fixed; background: #043858; color: white; padding: 12px 16px;
    border-radius: 6px; font-family: 'Work Sans', sans-serif; font-size: 13px;
    line-height: 1.5; pointer-events: none; opacity: 0; transition: opacity 0.15s;
    z-index: 1000; max-width: 300px; box-shadow: 0 4px 12px rgba(0,0,0,0.2);
  }}
  .{css_prefix}-tooltip.visible {{ opacity: 1; }}
  .{css_prefix}-tt-state {{ font-weight: 700; font-size: 15px; margin-bottom: 4px; }}
  .{css_prefix}-tt-type {{ font-weight: 400; opacity: 0.9; }}

  .placeholder-notice {{
    background: #fff8e1;
    border: 2px dashed #d0ac60;
    border-radius: var(--ms-radius);
    padding: 24px;
    margin: 32px 0;
    text-align: center;
  }}
  .placeholder-notice p {{
    font-family: 'Work Sans', sans-serif !important;
    font-weight: 600 !important;
    font-size: 14px !important;
    color: #856404 !important;
    margin-bottom: 8px !important;
  }}
  .placeholder-notice p:last-child {{ margin-bottom: 0 !important; }}

  @media (max-width: 768px) {{
    .article-body h2 {{ font-size: 22px; margin-top: 36px; }}
    .lead {{ font-size: 15px !important; }}
    .{css_prefix}-map-section {{ width: 100%; }}
    .{css_prefix}-legend-label {{ font-size: 12px; }}
  }}
</style>
</head>
<body>
<div class="page-container">

  <div class="site-header">
    <a href="index.html"><img src="assets/multistate-logomark.png" alt="MultiState"></a>
    <a href="index.html" class="site-title">State Elections Tracker</a>
  </div>

  <div class="breadcrumb">
    <a href="index.html">Home</a> &rsaquo; {label}
  </div>

  <div class="subnav">
{subnav_html}  </div>

  <h1 class="heading-xl" style="margin-bottom: 8px">{topic["title_101"]}</h1>
  <p class="subtitle mb-16">{topic["subtitle_101"]}</p>

  <div class="article-body">

{inline_block}

    <div class="placeholder-notice">
      <p>Detailed guide content coming soon.</p>
      <p style="font-weight:400 !important;font-size:13px !important;color:#666 !important">{topic["placeholder_101"]}</p>
    </div>

  </div>

  <div class="site-footer">
    <img src="assets/multistate-logomark.png" alt="MultiState">
    <div class="footer-text">State Elections Tracker &mdash; Data compiled by MultiState</div>
  </div>

</div>
</body>
</html>'''

    outpath = f'{SITE_DIR}/{slug}-101.html'
    with open(outpath, 'w') as f:
        f.write(html)
    print(f'  Written: {slug}-101.html')


# ── Main ─────────────────────────────────────────────────────────────

# Topics with hand-crafted pages (skip generating these)
CUSTOM_PAGES = {'governors'}  # All 3 governor pages are hand-crafted

if __name__ == '__main__':
    generated = 0
    for topic in TOPICS:
        name = topic['label']
        slug = topic['slug']
        if slug in CUSTOM_PAGES:
            print(f'Skipping {name} (all pages hand-crafted)')
            continue
        print(f'Building {name} pages...')
        build_placeholder_page(topic, 'data')
        build_placeholder_page(topic, 'analytics')
        build_101_page(topic)
        generated += 3
    print(f'Done! {generated} pages generated.')
