#!/usr/bin/env python3
"""Build inline ballot measure count map for ballot-measures.html.

Outputs to /tmp/inline_ballot_count_block.html — a self-contained block
with CSS, SVG map, gradient legend, tooltip, and JS for dynamic recoloring
when the user switches year tabs.

The map shows a blue gradient based on how many ballot measures each state
has on the ballot for the selected year.
"""

import re

SITE_DIR = '/home/billkramer/elections/site'

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

# Reset floating label path fills (small states: HI, MD, DE, NJ, CT, RI, MA, NH, VT)
# These are direct-child <path> elements with hardcoded fill colors from source map
def reset_floating_labels(svg_str):
    """Find paths that are direct children of state-groups (not inside state-fill/stroke)
    and reset their fill to gray."""
    # Match path elements with fill attr that appear after </g> closings within state-groups
    # Pattern: after state-stroke </g>, find <path...fill="...".../>
    return re.sub(
        r'(</g>\s*<path\s[^>]*?)fill="[^"]*"',
        r'\1fill="#d9d9d9"',
        svg_str
    )

svg = reset_floating_labels(svg)

# Give it a unique ID
svg = svg.replace('class="us-map"', 'class="us-map" id="bm-count-map"', 1)


def build_block():
    return f'''    <!-- Inline Ballot Measure Count Map -->
    <style>
      .bm-map-section {{ width: 100%; margin: 28px 0; }}
      .bm-map-wrapper {{
        display: flex;
        flex-direction: column;
      }}
      .bm-map-main {{ margin-top: -20px; width: 100%; }}
      .bm-map-main svg {{ width: 100%; height: auto; display: block; }}
      .bm-gradient-legend {{
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
        padding: 0 4px;
        margin-bottom: 4px;
      }}
      .bm-gradient-legend .legend-title {{
        font-family: 'Work Sans', sans-serif;
        font-weight: 700;
        font-size: 13px;
        color: var(--ms-navy);
        margin-right: 4px;
      }}
      .bm-gradient-legend .legend-label {{
        font-family: 'Work Sans', sans-serif;
        font-weight: 600;
        font-size: 12px;
        color: var(--ms-text-light);
      }}
      .bm-gradient-bar {{
        height: 14px;
        width: 180px;
        border-radius: 3px;
        background: linear-gradient(to right, #cfe2f3, #9fc5e8, #6fa8dc, #3d85c6, #0b5394, #073763);
      }}
      .bm-gray-swatch {{
        width: 14px;
        height: 14px;
        border-radius: 2px;
        background: #d9d9d9;
        flex-shrink: 0;
      }}
      .bm-map-source {{
        font-family: 'Work Sans', sans-serif;
        font-size: 12px;
        color: var(--ms-text-light);
        padding: 4px;
      }}
      #bm-tooltip {{
        position: fixed; background: #043858; color: white; padding: 12px 16px;
        border-radius: 6px; font-family: 'Work Sans', sans-serif; font-size: 13px;
        line-height: 1.5; pointer-events: none; opacity: 0; transition: opacity 0.15s;
        z-index: 1000; max-width: 300px; box-shadow: 0 4px 12px rgba(0,0,0,0.2);
      }}
      #bm-tooltip.visible {{ opacity: 1; }}
      .bm-tt-state {{ font-weight: 700; font-size: 15px; margin-bottom: 4px; }}
      .bm-tt-count {{ font-weight: 400; opacity: 0.9; }}
      @media (max-width: 768px) {{
        .bm-gradient-bar {{ width: 120px; }}
      }}
    </style>
    <div class="bm-map-section">
      <div class="bm-map-wrapper">
        <div class="bm-gradient-legend">
          <span class="legend-title">Measures on the Ballot</span>
          <div class="bm-gray-swatch"></div>
          <span class="legend-label">0</span>
          <div class="bm-gradient-bar"></div>
          <span class="legend-label" id="bm-legend-max">—</span>
        </div>
        <div class="bm-map-main">
          {svg}
        </div>
      </div>
      <div class="bm-map-source" id="bm-map-source"></div>
    </div>
    <div id="bm-tooltip"></div>'''


if __name__ == '__main__':
    html = build_block()
    outpath = '/tmp/inline_ballot_count_block.html'
    with open(outpath, 'w') as f:
        f.write(html)
    print(f'Written: {outpath} ({len(html)} chars)')
