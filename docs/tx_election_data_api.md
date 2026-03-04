# Texas Election Results API (Civix / GoElect)

## Overview

Texas uses the **Civix GoElect** platform for official election night reporting. The data is served through a combination of a legacy SOAP/REST service and a newer S3-backed JSON API. All data is base64-encoded in transit.

- **Frontend**: https://goelect.txelections.civixapps.com/ivis-enr-ui/races
- **New API base**: https://goelect.txelections.civixapps.com/api-ivis-system/api
- **Legacy service**: https://teamrv-test.sos.texas.gov/ElectionResults/Service/
- **RSS service**: https://electionet.pcctg.net/ElectionResults/rss/login
- **Platform vendor**: Civix (civixapps.com)
- **Auto-refresh**: 5 seconds (configured in Home data)

## Key API Endpoints

All under `https://goelect.txelections.civixapps.com/api-ivis-system/api/s3/`

### 1. Election Constants (List All Elections)
```
GET /s3/enr/electionConstants
```
Returns base64-encoded JSON with:
- `electionInfo` — nested by year → type code → election objects
- `currentelectionInfo` — currently active election
- Type codes: P=Primary, S=Special, SR=Special Runoff

### 2. Election Data (Full Results)
```
GET /s3/enr/election/{electionId}
```
Returns JSON with multiple base64-encoded sections:
- `Home` — reporting status, precincts, last update time, refresh interval
- `OfficeSummary` — statewide totals per office with candidate votes
- `Districted` — **district-level races** (State Rep, State Senate, judicial)
- `Federal` — US Congress and US Senate races
- `StateWide` — statewide offices (Governor, AG, etc.)
- `StateWideQ` — statewide propositions/ballot measures
- `Race` — race index (IDs and names, no vote data)
- `Lookups` — reference data
- `ReportList` — available report formats

### 3. County-Level Data
```
GET /s3/enr/election/countyInfo/{electionId}
```

### 4. State Map GIS Data
```
GET /s3/enr/stateMap/{mapId}
```

### 5. Election Reports
```
GET /s3/enr/electionReports/{electionId}/{reportType}/{format}
```

## Data Structure

### Decoded Race Object (from Districted/Federal/StateWide sections)
```json
{
  "id": 4856,
  "N": "STATE REPRESENTATIVE DISTRICT 1",
  "O": 80,
  "SO": 1,
  "Candidates": [
    {
      "ID": 30276,
      "N": "JOSH BRAY",
      "P": "REP",
      "V": 14400,
      "EV": 7200,
      "PE": 49,
      "C": "#E30202",
      "O": 2,
      "LN": "BRAY",
      "FN": "JOSH"
    }
  ],
  "T": 29313
}
```

Field key:
- `N` = Name, `V` = Votes, `EV` = Early Votes, `PE` = Percentage, `T` = Total votes
- `P` = Party (REP/DEM), `C` = Color hex, `O` = Order
- `LN` = Last Name, `FN` = First Name
- `(I)` suffix on candidate name indicates incumbent

### Home Object (Reporting Status)
```json
{
  "ElecDate": "03032026",
  "CountiesReporting": {"CR": 253, "CT": 254},
  "LastUpdatedTime": "Mar 04, 2026 15:38:10",
  "RefreshTime": 5,
  "PrecinctsReporting": {"PR": 708, "PT": 846}
}
```

## Usage Pattern

```python
import requests, json, base64

API = "https://goelect.txelections.civixapps.com/api-ivis-system/api/s3"

# 1. Get election list
constants = requests.get(f"{API}/enr/electionConstants").json()
election_info = json.loads(base64.b64decode(constants['upload']))

# 2. Get results for a specific election
data = requests.get(f"{API}/enr/election/53813").json()

# 3. Decode sections
home = json.loads(base64.b64decode(data['Home']))
districted = json.loads(base64.b64decode(data['Districted']))

# 4. Extract state rep races
for race in districted['Races']:
    if 'STATE REPRESENTATIVE' in race['N']:
        name = race['N']
        total = race['T']
        for cand in race['Candidates']:
            print(f"{cand['N']}: {cand['V']} votes ({cand['PE']}%)")
```

## 2026 Election IDs

| ID | Name | Date |
|----|------|------|
| 53813 | 2026 Republican Primary | 2026-03-03 |
| 53814 | 2026 Democratic Primary | 2026-03-03 |
| 54613 | 2026 Special Runoff SD-9 | 2026 |
| 54612 | 2026 Special Runoff CD-18 | 2026 |

## Notes

- **No authentication required** — all endpoints are open
- **Base64 encoding**: All data sections are base64-encoded JSON strings. The outer JSON has string values that must be decoded.
- **Early votes**: The `EV` field on each candidate gives the early/absentee vote count separately
- **253/254 counties reporting** as of March 4, 2026 — results are essentially final but not yet certified
- **HD-52 R primary**: 0 votes reported (the 1 unreporting county) — excluded from import
- **Incumbents marked** with `(I)` suffix in candidate name
- **TX runoff threshold**: >50% required to win primary; otherwise top 2 advance to runoff
- **12 runoff races**: 5 Republican + 7 Democratic state legislative primaries heading to runoff

## Discovered: March 4, 2026
Tested against the 2026 Republican and Democratic Primary results.
