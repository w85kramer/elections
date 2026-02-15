#!/usr/bin/env python3
"""Extract the inline map HTML block for the ballot measures 101 page.
Produces a single SVG with JS-driven tab switching between initiative/referendum views.
Reads state classification data from data/ballot_auth.json (exported from DB)."""

import re, json

# Read ballot authorization data from DB export
with open('/home/billkramer/elections/site/data/ballot_auth.json') as f:
    auth_data = json.load(f)

# Read a generated map to get the SVG with correct state groups
with open('/home/billkramer/elections/site/ballot-measures-initiative-map.html') as f:
    content = f.read()

# Extract the full SVG
svg_match = re.search(r'(<svg class="us-map".*?</svg>)', content, re.DOTALL)
svg = svg_match.group(1)

# Strip data-category and data-info attributes (JS will handle coloring)
svg = re.sub(r' data-category="[^"]*"', '', svg)
svg = re.sub(r" data-info='[^']*'", '', svg)

# Reset all fills to gray placeholder
svg = re.sub(r'(<g class="state-fill" fill=")[^"]*"', r'\1#d9d9d9"', svg)

# Change the SVG id for namespacing
svg = svg.replace('class="us-map"', 'class="us-map" id="bm-map"', 1)

# Build JS state objects from JSON data
# Only include states that differ from the default category (smaller JS output)
init_states = {st: cat for st, cat in auth_data['initiative']['states'].items() if cat != 'none'}
ref_states = {st: cat for st, cat in auth_data['referendum']['states'].items() if cat != 'amendments_only'}

def js_obj(d):
    """Format a dict as a JS object literal."""
    return ','.join(f"{k}:'{v}'" for k, v in sorted(d.items()))

init_colors = auth_data['initiative']['colors']
ref_colors = auth_data['referendum']['colors']

# Build the output HTML block
output = f'''    <!-- Inline Ballot Measures Map -->
    <div class="bm-map-section" style="margin:28px 0">
      <div class="bm-map-tabs" style="display:flex;gap:8px;margin-bottom:16px">
        <button class="bm-tab active" data-view="initiative" onclick="switchBmMap('initiative')">Initiative Authorization</button>
        <button class="bm-tab" data-view="referendum" onclick="switchBmMap('referendum')">Referendum Authorization</button>
      </div>
      <div class="bm-map-wrapper">
        <div class="bm-map-legend" id="bm-legend"></div>
        <div class="bm-map-main">
          {svg}
        </div>
      </div>
      <div class="bm-map-source" id="bm-source"></div>
    </div>
    <div class="bm-tooltip" id="bm-tooltip"></div>

    <script>
    (function() {{
      const INITIATIVE = {{
        colors: {{ both: '{init_colors["both"]}', statutory: '{init_colors["statutory"]}', constitutional: '{init_colors["constitutional"]}', none: '{init_colors["none"]}' }},
        defaultCategory: 'none',
        legend: [
          ['both', '{auth_data["initiative"]["legend"]["both"]}'],
          ['statutory', '{auth_data["initiative"]["legend"]["statutory"]}'],
          ['constitutional', '{auth_data["initiative"]["legend"]["constitutional"]}'],
          ['none', '{auth_data["initiative"]["legend"]["none"]}']
        ],
        source: 'Source: MultiState. Note: The Mississippi state supreme court blocked the state\\'s constitutional initiative process in 2021.',
        states: {{ {js_obj(init_states)} }},
        tooltips: {{
          both:'Both statutory and constitutional initiatives',
          statutory:'Statutory initiatives only',
          constitutional:'Constitutional initiatives only',
          none:'Does not authorize citizen initiatives'
        }},
        notes: {{ {','.join(f"{k}:'{v}'" for k, v in auth_data['notes'].items() if k != 'DE')} }}
      }};

      const REFERENDUM = {{
        colors: {{ both: '{ref_colors["both"]}', amendments_only: '{ref_colors["amendments_only"]}', statutes_only: '{ref_colors["statutes_only"]}' }},
        defaultCategory: 'amendments_only',
        legend: [
          ['both', '{auth_data["referendum"]["legend"]["both"]}'],
          ['amendments_only', '{auth_data["referendum"]["legend"]["amendments_only"]}'],
          ['statutes_only', '{auth_data["referendum"]["legend"]["statutes_only"]}']
        ],
        source: 'Source: MultiState.',
        states: {{ {js_obj(ref_states)} }},
        tooltips: {{
          both:'Legislative referendum for both statutes and constitutional amendments',
          amendments_only:'Legislative referendum for constitutional amendments only',
          statutes_only:'Legislative referendum for statutes only'
        }},
        notes: {{
          DE:'Only state where legislature can amend constitution without voter approval'
        }}
      }};

      const NAMES = {{
        AL:'Alabama',AK:'Alaska',AZ:'Arizona',AR:'Arkansas',CA:'California',
        CO:'Colorado',CT:'Connecticut',DE:'Delaware',FL:'Florida',GA:'Georgia',
        HI:'Hawaii',ID:'Idaho',IL:'Illinois',IN:'Indiana',IA:'Iowa',
        KS:'Kansas',KY:'Kentucky',LA:'Louisiana',ME:'Maine',MD:'Maryland',
        MA:'Massachusetts',MI:'Michigan',MN:'Minnesota',MS:'Mississippi',MO:'Missouri',
        MT:'Montana',NE:'Nebraska',NV:'Nevada',NH:'New Hampshire',NJ:'New Jersey',
        NM:'New Mexico',NY:'New York',NC:'North Carolina',ND:'North Dakota',OH:'Ohio',
        OK:'Oklahoma',OR:'Oregon',PA:'Pennsylvania',RI:'Rhode Island',SC:'South Carolina',
        SD:'South Dakota',TN:'Tennessee',TX:'Texas',UT:'Utah',VT:'Vermont',
        VA:'Virginia',WA:'Washington',WV:'West Virginia',WI:'Wisconsin',WY:'Wyoming'
      }};

      let currentView = 'initiative';

      function getConfig() {{ return currentView === 'initiative' ? INITIATIVE : REFERENDUM; }}

      window.switchBmMap = function(view) {{
        currentView = view;
        const config = getConfig();

        // Update tab styles
        document.querySelectorAll('.bm-tab').forEach(t => {{
          if (t.getAttribute('data-view') === view) {{
            t.classList.add('active');
          }} else {{
            t.classList.remove('active');
          }}
        }});

        // Recolor states
        document.querySelectorAll('#bm-map .state-group').forEach(g => {{
          const st = g.getAttribute('data-state');
          const cat = config.states[st] || config.defaultCategory;
          const color = config.colors[cat];
          g.querySelectorAll('.state-fill').forEach(fg => fg.setAttribute('fill', color));
          // Also recolor floating label paths (small states like CT, DE, HI, etc.)
          g.querySelectorAll(':scope > path').forEach(p => p.setAttribute('fill', color));
        }});

        // Update legend
        let legendHtml = '';
        for (const [cat, label] of config.legend) {{
          legendHtml += '<div class="bm-legend-row"><div class="bm-legend-swatch" style="background:' + config.colors[cat] + '"></div><div class="bm-legend-label">' + label + '</div></div>';
        }}
        document.getElementById('bm-legend').innerHTML = legendHtml;

        // Update source
        document.getElementById('bm-source').textContent = config.source;
      }};

      // Tooltip
      const tooltip = document.getElementById('bm-tooltip');
      document.querySelectorAll('#bm-map .state-group').forEach(g => {{
        g.addEventListener('mouseenter', e => {{
          const st = g.getAttribute('data-state');
          const config = getConfig();
          const cat = config.states[st] || config.defaultCategory;
          const name = NAMES[st] || st;
          let html = '<div class="bm-tt-state">' + name + '</div>';
          html += '<div class="bm-tt-type">' + (config.tooltips[cat] || cat) + '</div>';
          const note = (config.notes || {{}})[st];
          if (note) html += '<div class="bm-tt-note">' + note + '</div>';
          tooltip.innerHTML = html;
          tooltip.classList.add('visible');
        }});
        g.addEventListener('mousemove', e => {{
          tooltip.style.left = (e.clientX + 14) + 'px';
          tooltip.style.top = (e.clientY + 14) + 'px';
        }});
        g.addEventListener('mouseleave', () => tooltip.classList.remove('visible'));
      }});

      // Initialize
      switchBmMap('initiative');
    }})();
    </script>'''

with open('/tmp/inline_map_block.html', 'w') as f:
    f.write(output)

print(f'Inline map block written: {len(output)} chars')
