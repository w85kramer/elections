# Arkansas Election Results API (TotalResults ENR)

## Overview

Arkansas uses the **TotalResults ENR** platform by **KnowInk** for official election night reporting. The platform exposes a fully open REST API (no authentication required) that returns structured JSON data with real-time election results.

- **Frontend**: https://enr.totalresults.com/arkansas/
- **Results API**: https://enr-results-api.totalresults.com
- **Data CDN**: https://enr-data.azureedge.us (Azure CDN — used for GIS/map data)
- **Config**: https://enr.totalresults.com/arkansas/config.json
- **Auto-refresh interval**: 900 seconds (15 minutes) during election night
- **Platform vendor**: KnowInk (totalresults.com)

## Key API Endpoints

All endpoints use `cId=arkansas` as the client identifier (derived from `clientState` in config).

### 1. List Elections
```
GET https://enr-results-api.totalresults.com/Election/GetElectionList?cId=arkansas
```
Returns all available elections with IDs, names, and dates. Recent elections:
- `7f77a178-af02-40ec-92db-c5cc50882c68` — 2026 Preferential Primary (2026-03-03)
- `55355810-8dde-40ff-a1d2-5b8675226873` — 2026 Special General (2026-03-03)
- `1846` — 2024 General (2024-11-05)
- Historical elections back to 2012 are listed but may not have detailed results data

### 2. Election Info (Turnout Summary)
```
GET https://enr-results-api.totalresults.com/Election/GetElectionInfo?cId=arkansas&electionID={electionId}
```
Returns: versionID, lastUpdated, isOfficial flag, turnout (registered voters, precincts reporting, total ballots cast, reporting percent, vote percent).

### 3. Contest Search List (Contest Names + Candidate Names)
```
GET https://enr-results-api.totalresults.com/Contest/GetContestSearchList?cId=arkansas&electionID={electionId}
```
Returns all contests with:
- Contest IDs, names, type codes, vote-for count
- **Candidate details**: choice IDs, names, party IDs, colors, write-in flags
- Search list for the frontend search/filter feature

**Contest type codes**: Federal, Statewide, State Senate, State Representative, Judge, County, City, School, Other

### 4. Contest Results (Vote Totals by Type)
```
GET https://enr-results-api.totalresults.com/Contest/GetContestResults?cId=arkansas&electionID={electionId}&contestType={type}
```
Where `{type}` is one of: `Federal`, `Statewide`, `State Senate`, `State Representative`, `Judge`, `County`, `City`, `School`, `Other`

Returns per contest: totalVotes, precinctsReporting, totalPrecincts, and choices array with totalVotes, votePercent, isWinner for each candidate.

**IMPORTANT**: The choices in results are returned in the same order as in the contest search list, but without names or IDs. You must cross-reference with the search list to get candidate names.

### 5. Single Contest Results
```
GET https://enr-results-api.totalresults.com/Contest/GetSingleContestResults?cId=arkansas&electionID={electionId}&contestType={type}&contestID={contestId}
```

### 6. Turnout Data
```
GET https://enr-results-api.totalresults.com/Turnout/GetTurnout?cId=arkansas&electionID={electionId}
```
Returns county-level turnout with registered voters and ballots cast per location.

## Data Structure

### Contest from Search List
```json
{
  "contestId": "33a9de2e-...",
  "contestName": "REP State Representative Dist. 05",
  "contestTypeCode": "State Representative",
  "contestOrder": 67,
  "voteFor": 1,
  "choices": {
    "2b2e9e8b-...": {
      "id": "2b2e9e8b-...",
      "name": "Mike Bishop",
      "partyID": "4c1a540e-...",
      "color": "rgb(160, 38, 38)",
      "isWriteIn": false,
      "isWinner": false
    }
  }
}
```

### Contest from Results
```json
{
  "totalVotes": 4839,
  "precinctsReporting": 10,
  "totalPrecincts": 10,
  "choices": [
    {"totalVotes": 1563, "votePercent": 32.3, "isWinner": false},
    {"totalVotes": 629, "votePercent": 13.0, "isWinner": false}
  ]
}
```

## Usage Pattern for Election Night

```python
import requests

BASE = "https://enr-results-api.totalresults.com"
CID = "arkansas"

# 1. Get election ID
elections = requests.get(f"{BASE}/Election/GetElectionList?cId={CID}").json()
# Find the target election (e.g., isDefault=True for the current one)
election_id = next(e['electionID'] for e in elections if e['isDefault'])

# 2. Get contest list with candidate names
search = requests.get(f"{BASE}/Contest/GetContestSearchList?cId={CID}&electionID={election_id}").json()
contests = search['response']['contests']

# 3. Get results for state legislative races
for contest_type in ['State Senate', 'State Representative']:
    results = requests.get(f"{BASE}/Contest/GetContestResults?cId={CID}&electionID={election_id}&contestType={contest_type}").json()
    for cid, rdata in results['response']['contests'].items():
        ci = contests[cid]
        list_choices = list(ci['choices'].values())
        for i, rc in enumerate(rdata['choices']):
            candidate_name = list_choices[i]['name']
            votes = rc['totalVotes']
            pct = rc['votePercent']
            is_winner = rc['isWinner']

# 4. Check update status
info = requests.get(f"{BASE}/Election/GetElectionInfo?cId={CID}&electionID={election_id}").json()
version = info['versionID']
last_updated = info['lastUpdated']
is_official = info['isOfficial']
pct_reporting = info['turnout']['reportingPercent']
```

## Notes

- **No authentication required** — all endpoints are fully open
- **Historical data**: Election IDs for past elections (2012-2024) are listed but detailed contest results may not be populated
- **FullDataFile.json**: The CDN pattern `{GIS_URL}/results/arkansas/{electionId}/FullDataFile.json` exists in the JS code but returned 404 for the 2026 primary — may only be generated for some elections or during active reporting
- **County-level breakdown**: Available by adding `&locationId={countyFIPS}` to contest results call, but the state-wide aggregate is the most useful
- **isWinner flag**: Set server-side when results are finalized — wasn't set for any 2026 primary candidates even at 100% reporting (possibly not marked until certified)
- **Version tracking**: Each update increments `versionID` (e.g., "v3273") — useful for polling only when data changes
- **TX and NC do NOT use TotalResults** — they have different reporting platforms
- **Config curiosity**: `arkLogin: "Razorbacks"` is a demo/preview login code (used in the frontend for preview mode access)

## Discovered: March 4, 2026
Tested against the 2026 Preferential Primary (March 3, 2026) results.
