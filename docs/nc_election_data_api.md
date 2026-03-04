# North Carolina Election Results Data (NCSBE)

## Overview

North Carolina uses a file-based system hosted on S3/CDN for election night reporting. No authentication required. Data is served as JSON arrays in `.txt` files.

- **Frontend**: https://er.ncsbe.gov/
- **Data files**: https://er.ncsbe.gov/enr/{YYYYMMDD}/data/
- **Platform**: NC State Board of Elections (custom)

## Key Data Files

All under `https://er.ncsbe.gov/enr/{YYYYMMDD}/data/`

### 1. Office Types
```
GET /enr/{date}/data/office.txt
```
Returns office group definitions (FED, NCS, NCH, JUD, etc.) — not date-specific, same across elections.

### 2. Contest List
```
GET /enr/{date}/data/contest.txt
```
Returns all contests for the election with:
- `cni` — Contest ID number
- `cnm` — Contest name (e.g., "NC HOUSE OF REPRESENTATIVES DISTRICT 001")
- `ogl` — Office group label (NCH, NCS, FED, JUD, etc.)
- `crt` — Number of counties in district

### 3. Statewide Results
```
GET /enr/{date}/data/results_0.txt
```
Returns **all results** statewide (one row per candidate per contest) with:
- `cnm` — Contest name with party (e.g., "NC HOUSE OF REPRESENTATIVES DISTRICT 001 - REP (VOTE FOR 1)")
- `bnm` — Ballot name (candidate name as it appears on ballot)
- `pty` — Party (REP, DEM, LIB, etc.)
- `vct` — Vote count (total)
- `pct` — Percentage (decimal, e.g., 0.5253 = 52.53%)
- `prt`/`ptl` — Precincts reporting / total
- `evc` — Early votes, `ovc` — Election day votes, `avc` — Absentee votes, `pvc` — Provisional votes
- `ogl` — Office group label
- `gid` — Group ID, `lid` — List ID

### 4. County-Level Results
```
GET /enr/{date}/data/results_{countyId}.txt
```
Same format as statewide but filtered to a specific county.

### 5. County Status
```
GET /enr/{date}/data/county.txt
```
County metadata and reporting status.

## Data Structure

### Result Row
```json
{
  "cid": "0",
  "cnm": "NC HOUSE OF REPRESENTATIVES DISTRICT 001 - REP (VOTE FOR 1)",
  "vfr": "1",
  "gid": "1507",
  "lid": "1507",
  "bnm": "Edward C. Goodwin",
  "dtx": "",
  "pty": "REP",
  "vct": "5069",
  "pct": "0.5253",
  "prt": "43",
  "ptl": "43",
  "evc": "2967",
  "ovc": "2083",
  "avc": "19",
  "pvc": "0",
  "col": "E0E4CC",
  "ogl": "NCH",
  "ref": "0"
}
```

## Usage Pattern

```python
import requests, json

DATE = "20260303"  # YYYYMMDD
BASE = f"https://er.ncsbe.gov/enr/{DATE}/data"

# Get all statewide results in one call
results = requests.get(f"{BASE}/results_0.txt").json()

# Filter for NC House/Senate
nc_house = [r for r in results if r['ogl'] == 'NCH']
nc_senate = [r for r in results if r['ogl'] == 'NCS']

# Group by contest
from collections import defaultdict
contests = defaultdict(list)
for r in nc_house + nc_senate:
    contests[r['cnm']].append(r)

# Process each contest
for cnm, candidates in contests.items():
    if len(candidates) > 1:  # Contested
        for c in candidates:
            name = c['bnm']
            votes = int(c['vct'])
            pct = float(c['pct']) * 100
            party = c['pty']
```

## Contest Name Parsing

Format: `NC {CHAMBER} DISTRICT {NNN} - {PARTY} (VOTE FOR {N})`

Examples:
- `NC HOUSE OF REPRESENTATIVES DISTRICT 001 - REP (VOTE FOR 1)`
- `NC STATE SENATE DISTRICT 01 - DEM (VOTE FOR 1)`

**Note**: District numbers have leading zeros (001, 01) — strip them for DB matching.

## Office Group Labels

| Label | Description |
|-------|-------------|
| NCH | NC House of Representatives |
| NCS | NC State Senate |
| FED | Federal (US Senate, US House) |
| COS | Council of State (Governor, AG, etc.) |
| JUD | Judicial |
| REF | Referenda |
| CCL | Cross-County Local |
| LOC | All Local |

## Notes

- **Simple file-based access** — no complex API, just JSON files on S3/CDN
- **All results in one file** (`results_0.txt`) makes this very easy to parse
- **Percentages are decimals** (0.5253), not percentages (52.53) — multiply by 100
- **Vote counts are strings** — need `int()` conversion
- **NC has NO primary runoff** — simple plurality wins
- **Candidate names may include**: suffixes (Jr., III), nicknames in quotes, middle initials, "DECEASED" annotation
- **2,633 precincts total** in 2026 primary (100 counties)
- **Date format**: YYYYMMDD in URL path (e.g., 20260303 for March 3, 2026)

## Discovered: March 4, 2026
Tested against the 2026 Primary Election (March 3, 2026) results.
