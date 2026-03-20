#!/usr/bin/env python3
"""
Download comprehensive election data from the Massachusetts Secretary of State
election statistics API (electionstats.state.ma.us).

Covers all state-level offices from 1970 to present:
  - State Senate (office_id=9)
  - State Representative (office_id=8)
  - Governor (office_id=3)
  - Lieutenant Governor (office_id=4)
  - Attorney General (office_id=12)
  - Secretary of the Commonwealth (office_id=45)
  - Treasurer (office_id=53)
  - Auditor (office_id=90)

Output: /tmp/ma_sos_elections.json

Usage:
  python3 scripts/download_ma_sos_elections.py
  python3 scripts/download_ma_sos_elections.py --office 9          # State Senate only
  python3 scripts/download_ma_sos_elections.py --year-from 2012    # 2012 onwards
  python3 scripts/download_ma_sos_elections.py --year-from 2014 --year-to 2014  # Single year
"""

import argparse
import json
import time
import urllib.request
import urllib.error

BASE_URL = "https://electionstats.state.ma.us/elections/search"

OFFICES = {
    9:  "State Senate",
    8:  "State Representative",
    3:  "Governor",
    4:  "Lieutenant Governor",
    12: "Attorney General",
    45: "Secretary of the Commonwealth",
    53: "Treasurer",
    90: "Auditor",
}

# Year ranges available per office
OFFICE_YEAR_RANGES = {
    9:  (1970, 2026),
    8:  (1970, 2026),
    3:  (1970, 2022),
    4:  (1970, 2022),
    12: (1970, 2022),
    45: (1970, 2022),
    53: (1970, 2022),
    90: (1970, 2022),
}

OUTPUT_FILE = "/tmp/ma_sos_elections.json"


def fetch_elections(office_id, year_from, year_to, retries=3):
    """Fetch election data from the MA SoS JSON API."""
    url = (
        f"{BASE_URL}/year_from:{year_from}/year_to:{year_to}"
        f"/office_id:{office_id}/show_details:1.json"
    )
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                elections = data.get("output", [])
                return elections
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as e:
            wait = 3 * (attempt + 1)
            print(f"    Retry {attempt+1}/{retries} after error: {e} (waiting {wait}s)")
            time.sleep(wait)
    print(f"    FAILED after {retries} retries: office={office_id} {year_from}-{year_to}")
    return []


def main():
    parser = argparse.ArgumentParser(description="Download MA SoS election data")
    parser.add_argument("--office", type=int, help="Single office_id to download")
    parser.add_argument("--year-from", type=int, default=1970, help="Start year (default 1970)")
    parser.add_argument("--year-to", type=int, default=2026, help="End year (default 2026)")
    parser.add_argument("--output", default=OUTPUT_FILE, help="Output file path")
    parser.add_argument("--chunk-size", type=int, default=10,
                        help="Years per API request (default 10)")
    args = parser.parse_args()

    offices_to_fetch = {args.office: OFFICES[args.office]} if args.office else OFFICES

    all_elections = []
    stats = {}

    for office_id, office_name in offices_to_fetch.items():
        office_min, office_max = OFFICE_YEAR_RANGES[office_id]
        yr_from = max(args.year_from, office_min)
        yr_to = min(args.year_to, office_max)

        if yr_from > yr_to:
            print(f"Skipping {office_name}: no data in range {args.year_from}-{args.year_to}")
            continue

        print(f"\n{'='*60}")
        print(f"Downloading: {office_name} (office_id={office_id}), {yr_from}-{yr_to}")
        print(f"{'='*60}")

        office_count = 0
        # Chunk into multi-year batches to avoid huge responses
        y = yr_from
        while y <= yr_to:
            chunk_end = min(y + args.chunk_size - 1, yr_to)
            print(f"  {y}-{chunk_end}...", end=" ", flush=True)
            elections = fetch_elections(office_id, y, chunk_end)
            # Tag each election with our office metadata
            for e in elections:
                e["_office_id"] = office_id
                e["_office_name"] = office_name
            print(f"{len(elections)} elections")
            all_elections.extend(elections)
            office_count += len(elections)
            y = chunk_end + 1
            time.sleep(0.5)  # Be polite

        stats[office_name] = office_count
        print(f"  Subtotal: {office_count} elections")

    # Write output
    with open(args.output, "w") as f:
        json.dump(all_elections, f, indent=2)

    print(f"\n{'='*60}")
    print(f"DOWNLOAD COMPLETE")
    print(f"{'='*60}")
    print(f"Total elections: {len(all_elections)}")
    for name, count in stats.items():
        print(f"  {name}: {count}")
    print(f"Written to: {args.output}")


if __name__ == "__main__":
    main()
