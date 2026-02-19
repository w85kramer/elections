#!/usr/bin/env python3
"""
Download Census TIGER/Line shapefiles for state legislative districts,
convert to GeoJSON, and simplify for web use.

Prerequisites:
    sudo apt install gdal-bin       # for ogr2ogr
    npm install -g mapshaper        # for simplification

Generates:
    site/data/geo/{ST}_upper.json   — State Senate boundaries
    site/data/geo/{ST}_lower.json   — State House/Assembly boundaries

Usage:
    python3 scripts/download_district_maps.py                  # All 50 states
    python3 scripts/download_district_maps.py --state PA       # Single state
    python3 scripts/download_district_maps.py --dry-run        # Show what would download
    python3 scripts/download_district_maps.py --upper-only     # Senate only
    python3 scripts/download_district_maps.py --lower-only     # House only
"""

import argparse
import os
import subprocess
import sys
import shutil
import tempfile
import zipfile
import urllib.request

# State FIPS codes
FIPS = {
    'AL':'01','AK':'02','AZ':'04','AR':'05','CA':'06','CO':'08','CT':'09',
    'DE':'10','FL':'12','GA':'13','HI':'15','ID':'16','IL':'17','IN':'18',
    'IA':'19','KS':'20','KY':'21','LA':'22','ME':'23','MD':'24','MA':'25',
    'MI':'26','MN':'27','MS':'28','MO':'29','MT':'30','NE':'31','NV':'32',
    'NH':'33','NJ':'34','NM':'35','NY':'36','NC':'37','ND':'38','OH':'39',
    'OK':'40','OR':'41','PA':'42','RI':'44','SC':'45','SD':'46','TN':'47',
    'TX':'48','UT':'49','VT':'50','VA':'51','WA':'53','WV':'54','WI':'55',
    'WY':'56',
}

# NE is unicameral — no separate upper/lower distinction
# DC (11) is not included (no state legislature)
UNICAMERAL_STATES = {'NE'}

TIGER_BASE = 'https://www2.census.gov/geo/tiger/TIGER2024'
OUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'site', 'data', 'geo')
SIMPLIFY_PCT = '15%'  # mapshaper simplification level


def check_tools():
    """Verify required tools are installed."""
    # mapshaper can handle both shapefile→GeoJSON conversion and simplification
    if shutil.which('mapshaper') is None:
        print('ERROR: mapshaper not found. Install it:')
        print('  npm install -g mapshaper')
        sys.exit(1)


def download_and_convert(state, chamber_type, dry_run=False):
    """Download shapefile, convert to GeoJSON, simplify.

    Args:
        state: 2-letter state abbreviation
        chamber_type: 'upper' (Senate) or 'lower' (House)
    """
    fips = FIPS[state]
    tiger_dir = 'SLDU' if chamber_type == 'upper' else 'SLDL'
    tiger_prefix = 'sldu' if chamber_type == 'upper' else 'sldl'
    url = f'{TIGER_BASE}/{tiger_dir}/tl_2024_{fips}_{tiger_prefix}.zip'
    out_file = os.path.join(OUT_DIR, f'{state}_{chamber_type}.json')

    if dry_run:
        print(f'  Would download: {url}')
        print(f'  Would write:    {out_file}')
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, 'shapefile.zip')

        # Download
        print(f'    Downloading {state} {chamber_type}...')
        try:
            urllib.request.urlretrieve(url, zip_path)
        except Exception as e:
            print(f'    WARN: Failed to download {url}: {e}')
            return

        # Extract
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(tmpdir)

        # Find .shp file
        shp_files = [f for f in os.listdir(tmpdir) if f.endswith('.shp')]
        if not shp_files:
            print(f'    WARN: No .shp file found for {state} {chamber_type}')
            return
        shp_path = os.path.join(tmpdir, shp_files[0])

        # Convert shapefile to GeoJSON and simplify in one pass with mapshaper
        # mapshaper handles .shp → GeoJSON conversion + reprojection + simplification
        result = subprocess.run([
            'mapshaper', shp_path,
            '-proj', 'wgs84',
            '-simplify', SIMPLIFY_PCT,
            '-o', out_file, 'format=geojson'
        ], capture_output=True, text=True)
        if result.returncode != 0:
            print(f'    WARN: mapshaper failed for {state} {chamber_type}: {result.stderr[:200]}')
            return

        size_kb = os.path.getsize(out_file) / 1024
        print(f'    {state}_{chamber_type}.json: {size_kb:.0f} KB')


def main():
    parser = argparse.ArgumentParser(description='Download Census TIGER district boundaries')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--state', type=str, help='Single state (2-letter abbreviation)')
    parser.add_argument('--upper-only', action='store_true', help='Only download upper chambers (Senate)')
    parser.add_argument('--lower-only', action='store_true', help='Only download lower chambers (House)')
    args = parser.parse_args()

    if not args.dry_run:
        check_tools()

    os.makedirs(OUT_DIR, exist_ok=True)

    states = [args.state.upper()] if args.state else sorted(FIPS.keys())

    chambers = []
    if not args.lower_only:
        chambers.append('upper')
    if not args.upper_only:
        chambers.append('lower')

    total = 0
    for state in states:
        if state not in FIPS:
            print(f'  WARN: Unknown state {state}, skipping')
            continue

        print(f'  {state}:')
        for ch in chambers:
            # NE is unicameral — only has "upper" (Legislature maps as SLDU)
            if state in UNICAMERAL_STATES and ch == 'lower':
                continue
            download_and_convert(state, ch, dry_run=args.dry_run)
            total += 1

    print(f'\nProcessed {total} files.')
    if not args.dry_run:
        print(f'Output: {OUT_DIR}/')

if __name__ == '__main__':
    main()
