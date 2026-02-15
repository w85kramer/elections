#!/usr/bin/env python3
"""
Extract state-to-fill-color mappings from ballot measure authorization SVG maps.

These SVGs encode US states as vector paths (no IDs, no text elements).
States are identified by matching path centroids to known geographic positions.

Colors used:
  #189d91 (Teal)   - One category of authorization
  #043858 (Navy)   - Another category
  #88567f (Purple) - A third category 
  #d9d9d9 (Gray)   - None / not applicable

Alaska is rendered as an embedded PNG image, not a vector path.
Hawaii is represented by multiple small island paths.
Small NE states have both actual (tiny) shapes AND inset label boxes.
"""

import re
import math
import sys


def parse_path_bbox(d):
    """Parse SVG path 'd' attribute and return bounding box (min_x, min_y, max_x, max_y)."""
    tokens = re.findall(r'[a-zA-Z]|[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?', d)
    x, y = 0.0, 0.0
    start_x, start_y = 0.0, 0.0
    min_x, min_y = float('inf'), float('inf')
    max_x, max_y = float('-inf'), float('-inf')
    i = 0
    cmd = 'M'
    while i < len(tokens):
        t = tokens[i]
        if t.isalpha():
            cmd = t
            i += 1
            if cmd in ('Z', 'z'):
                x, y = start_x, start_y
            continue
        try:
            if cmd == 'M':
                x, y = float(tokens[i]), float(tokens[i+1])
                start_x, start_y = x, y; i += 2; cmd = 'L'
            elif cmd == 'm':
                x += float(tokens[i]); y += float(tokens[i+1])
                start_x, start_y = x, y; i += 2; cmd = 'l'
            elif cmd == 'L':
                x, y = float(tokens[i]), float(tokens[i+1]); i += 2
            elif cmd == 'l':
                x += float(tokens[i]); y += float(tokens[i+1]); i += 2
            elif cmd == 'H':
                x = float(tokens[i]); i += 1
            elif cmd == 'h':
                x += float(tokens[i]); i += 1
            elif cmd == 'V':
                y = float(tokens[i]); i += 1
            elif cmd == 'v':
                y += float(tokens[i]); i += 1
            elif cmd == 'C':
                x, y = float(tokens[i+4]), float(tokens[i+5]); i += 6
            elif cmd == 'c':
                x += float(tokens[i+4]); y += float(tokens[i+5]); i += 6
            elif cmd in ('Q', 'S'):
                x, y = float(tokens[i+2]), float(tokens[i+3]); i += 4
            elif cmd in ('q', 's'):
                x += float(tokens[i+2]); y += float(tokens[i+3]); i += 4
            elif cmd == 'A':
                x, y = float(tokens[i+5]), float(tokens[i+6]); i += 7
            elif cmd == 'a':
                x += float(tokens[i+5]); y += float(tokens[i+6]); i += 7
            else:
                i += 1; continue
        except (IndexError, ValueError):
            i += 1; continue
        min_x = min(min_x, x); min_y = min(min_y, y)
        max_x = max(max_x, x); max_y = max(max_y, y)
    return (min_x, min_y, max_x, max_y)


def get_state_paths(svg_content):
    """Extract state-colored paths from SVG content with their bounding boxes."""
    paths = re.findall(r'<path\s+fill="(#[0-9a-fA-F]{6})"\s+d="([^"]+)"', svg_content)
    state_colors = {'#043858', '#189d91', '#88567f', '#d9d9d9'}
    
    result = {}
    for i, (color, d) in enumerate(paths):
        if color not in state_colors:
            continue
        bbox = parse_path_bbox(d)
        cx = (bbox[0] + bbox[2]) / 2
        cy = (bbox[1] + bbox[3]) / 2
        w = bbox[2] - bbox[0]
        h = bbox[3] - bbox[1]
        result[i] = {'color': color, 'cx': cx, 'cy': cy, 'w': w, 'h': h}
    return result


# ============================================================
# Path index -> State mapping
# ============================================================
# Derived from careful geographic analysis:
# 1. White text labels (state abbreviations as vector paths) are positioned
#    at each state's centroid. Their positions identify which state each is.
# 2. Each label was matched to the nearest state-colored path.
# 3. Additional unmatched paths were identified as: NE state shapes,
#    NE inset boxes, Hawaii islands, and Michigan's Upper Peninsula.
#
# The path indices are consistent across both SVG files since they share
# identical geometry (only fill colors differ).

# Main continental state shapes (matched via white text labels)
LABEL_IDENTIFIED = {
    # West Coast
    15: 'WA',   # Washington - far northwest
    40: 'OR',   # Oregon - below WA
    28: 'CA',   # California - tall, west coast
    
    # Mountain West
    29: 'ID',   # Idaho - tall narrow, inland NW
    5:  'NV',   # Nevada - tall, west
    52: 'UT',   # Utah - between NV and CO
    41: 'AZ',   # Arizona - southwest
    
    # Mountain Interior
    53: 'MT',   # Montana - wide, north
    7:  'WY',   # Wyoming - medium rectangle
    16: 'CO',   # Colorado - square-ish, central
    8:  'NM',   # New Mexico - southwest, east of AZ
    
    # Great Plains
    17: 'ND',   # North Dakota - northern plains
    30: 'SD',   # South Dakota - below ND
    42: 'NE',   # Nebraska - wide flat, central
    9:  'KS',   # Kansas - wide flat, south of NE
    54: 'OK',   # Oklahoma - panhandle shape, south of KS
    31: 'TX',   # Texas - very large, south central
    
    # Upper Midwest
    55: 'MN',   # Minnesota - tall, upper midwest
    10: 'IA',   # Iowa - between MN and MO
    33: 'MO',   # Missouri - central
    11: 'AR',   # Arkansas - south of MO
    4:  'LA',   # Louisiana - gulf coast
    
    # Great Lakes
    34: 'WI',   # Wisconsin - east of MN
    56: 'IL',   # Illinois - narrow, tall
    43: 'MI',   # Michigan - Lower Peninsula
    
    # East Central
    13: 'IN',   # Indiana - between IL and OH
    45: 'KY',   # Kentucky - wide east-west
    57: 'TN',   # Tennessee - wide east-west, south of KY
    
    # Southeast
    3:  'MS',   # Mississippi - narrow, gulf coast
    18: 'AL',   # Alabama - east of MS
    12: 'GA',   # Georgia - east of AL
    36: 'OH',   # Ohio - northeast of KY
    35: 'SC',   # South Carolina - coast
    32: 'FL',   # Florida - large peninsula
    
    # Mid-Atlantic / Appalachian
    58: 'WV',   # West Virginia - irregular shape
    39: 'VA',   # Virginia - wide east-west
    46: 'NC',   # North Carolina - wide east-west, coast
    14: 'PA',   # Pennsylvania - mid-atlantic
    19: 'NY',   # New York - northeast, large
    1:  'ME',   # Maine - far northeast
}

# Small NE state actual shapes (not inset boxes)
NE_STATE_SHAPES = {
    6:  'MA',   # Massachusetts - (2034,454) 96x51
    50: 'VT',   # Vermont - (2026,395) 46x97, tall narrow
    60: 'NH',   # New Hampshire - (1989,403) 47x89, tall narrow
    49: 'CT',   # Connecticut - (2011,489) 49x48
    37: 'RI',   # Rhode Island - (2041,476) 22x28, tiny
    59: 'NJ',   # New Jersey - (1967,546) 38x88, elongated
    20: 'DE',   # Delaware - (1960,590) 30x49, tiny
    47: 'MD',   # Maryland - (1911,602) 128x61, wider
    48: 'MD2',  # Maryland piece 2 - (1918,607) 7x8 (Chesapeake bay area)
    38: 'DC',   # DC - (1964,646) 16x36, tiny
}

# NE inset label boxes (small rectangles with state colors)
NE_INSET_BOXES = {
    103: 'VT',   # (1989,344) - northernmost inset
    104: 'NH',   # (2073,426) - below VT
    105: 'MA',   # (2107,465) - below NH
    107: 'CT',   # (2084,489) - east of NJ
    106: 'NJ',   # (2051,508) - below CT
    108: 'DE',   # (2008,545) - below NJ
    110: 'MD',   # (1997,597) - below DE
    109: 'DC',   # (1997,634) - southernmost inset
}

# Hawaii islands (multiple small paths in bottom-center area)
HAWAII_ISLANDS = {
    21: 'HI',   # Big Island - (1020,1049) 45x53, largest
    22: 'HI',   # Maui - (987,1001) 26x18
    23: 'HI',   # Kahoolawe - (967,1000) 9x8
    24: 'HI',   # Lanai/Molokai - (964,988) 22x7
    25: 'HI',   # Oahu - (930,974) 22x18
    26: 'HI',   # Niihau - (851,958) 7x8
    27: 'HI',   # Kauai - (873,952) 17x14
    102: 'HI',  # HI inset label box - (966,1047) 22x16
}

# Alaska inset (separate path, different from embedded image)
ALASKA = {
    51: 'AK',   # (756,995) 255x219 - Alaska inset in bottom-left
}

# Michigan Upper Peninsula (second path for MI)
MI_UPPER = {
    44: 'MI',   # (1617,384) 162x85 - Upper Peninsula
}


def get_state_color_map(svg_file):
    """
    Extract state -> color mapping from an SVG file.
    
    For states with multiple paths (HI islands, MI upper peninsula, MD pieces),
    uses the primary/largest path's color. For NE states with both actual shapes
    and inset boxes, the inset box color is authoritative (actual shapes may be 
    too small to color distinctly).
    """
    with open(svg_file, 'r') as f:
        content = f.read()
    
    paths = get_state_paths(content)
    
    # Build state -> color mapping
    # Priority: NE inset boxes > main state shapes > small NE shapes
    state_colors = {}
    
    # 1. Main continental shapes (most reliable)
    for idx, state in LABEL_IDENTIFIED.items():
        if idx in paths:
            state_colors[state] = paths[idx]['color']
    
    # 2. Alaska
    for idx, state in ALASKA.items():
        if idx in paths:
            state_colors[state] = paths[idx]['color']
    
    # 3. Michigan Upper Peninsula (should match Lower Peninsula)
    # Just verify consistency; don't override
    
    # 4. NE state shapes (for states not yet assigned)
    for idx, state in NE_STATE_SHAPES.items():
        if state.endswith('2'):
            continue  # skip secondary pieces
        if state not in state_colors and idx in paths:
            state_colors[state] = paths[idx]['color']
    
    # 5. NE inset boxes (override with these - they're the definitive color)
    for idx, state in NE_INSET_BOXES.items():
        if idx in paths:
            state_colors[state] = paths[idx]['color']
    
    # 6. Hawaii (all islands should be same color; use largest)
    for idx in [21, 22, 25]:  # Big Island, Maui, Oahu
        if idx in paths:
            state_colors['HI'] = paths[idx]['color']
            break
    
    return state_colors


def main():
    SVG_FILES = [
        ('/home/billkramer/elections/map examples/Ballot Measures _ Initiative Authorization (Master - Live).svg',
         'Initiative Authorization'),
        ('/home/billkramer/elections/map examples/Ballot Measures _ Referendum Authorization (Master - Live).svg',
         'Referendum Authorization'),
    ]
    
    COLOR_NAMES = {
        '#189d91': 'Teal (Both)',
        '#043858': 'Navy',
        '#88567f': 'Purple',
        '#d9d9d9': 'Gray (None)',
    }
    
    all_results = {}
    
    for svg_file, label in SVG_FILES:
        print(f"\n{'='*70}")
        print(f"  {label}")
        print(f"  File: {svg_file.split('/')[-1]}")
        print(f"{'='*70}")
        
        state_colors = get_state_color_map(svg_file)
        all_results[label] = state_colors
        
        # Group states by color
        by_color = {}
        for state, color in sorted(state_colors.items()):
            by_color.setdefault(color, []).append(state)
        
        print(f"\nStates by category:")
        for color in ['#189d91', '#043858', '#88567f', '#d9d9d9']:
            if color in by_color:
                states = sorted(by_color[color])
                cname = COLOR_NAMES.get(color, color)
                print(f"\n  {cname} ({color}) - {len(states)} states:")
                # Print in rows of 10
                for i in range(0, len(states), 10):
                    print(f"    {', '.join(states[i:i+10])}")
        
        print(f"\n  Total states mapped: {len(state_colors)}")
        
        # Print full mapping sorted by state
        print(f"\n  Full state -> color mapping:")
        for state in sorted(state_colors.keys()):
            color = state_colors[state]
            cname = COLOR_NAMES.get(color, color)
            print(f"    {state}: {color} ({cname})")
    
    # Compare the two maps
    print(f"\n{'='*70}")
    print(f"  COMPARISON: States that differ between the two maps")
    print(f"{'='*70}")
    
    if len(all_results) == 2:
        labels = list(all_results.keys())
        map1, map2 = all_results[labels[0]], all_results[labels[1]]
        all_states = sorted(set(map1.keys()) | set(map2.keys()))
        
        same_count = 0
        diff_count = 0
        for state in all_states:
            c1 = map1.get(state, 'N/A')
            c2 = map2.get(state, 'N/A')
            if c1 != c2:
                n1 = COLOR_NAMES.get(c1, c1)
                n2 = COLOR_NAMES.get(c2, c2)
                print(f"  {state}: {n1} -> {n2}")
                diff_count += 1
            else:
                same_count += 1
        
        print(f"\n  Same in both: {same_count} states")
        print(f"  Different:    {diff_count} states")


if __name__ == '__main__':
    main()
