# Election Night Process

Step-by-step process for importing official election results from state SoS websites.

**Primary script:** `scripts/import_primary_results.py`
**API research:** `docs/nc_election_data_api.md`, `docs/ar_election_data_api.md`, `docs/tx_election_data_api.md`

## State Results Sources

### NC (North Carolina) — FULLY AUTOMATED
- **Platform:** NC State Board of Elections (custom, file-based)
- **Frontend:** https://er.ncsbe.gov/?election_dt=03/03/2026
- **API:** `https://er.ncsbe.gov/enr/{YYYYMMDD}/data/results_0.txt`
- **Format:** Clean JSON arrays — one row per candidate per contest
- **No auth required.** Simple file-based access on S3/CDN.
- **Date format:** YYYYMMDD (no dashes) in URL path
- **Key fields:** `cnm` (contest name), `bnm` (ballot name), `vct` (votes), `pct` (percentage as decimal), `ogl` (office group: NCH/NCS/COS/FED)
- **Statewide results:** County ID 0 = statewide totals (all results in one file)
- **Update cadence:** Updated as counties submit; check `county.txt` for last upload timestamp
- **Covers:** State House (NCH), State Senate (NCS), Council of State (COS), Federal (FED)
- **Full API docs:** `docs/nc_election_data_api.md`

```bash
# Quick test
curl -s "https://er.ncsbe.gov/enr/20260303/data/county.txt" | python3 -m json.tool | head

# Dry run
python3 scripts/import_primary_results.py --state NC --dry-run

# Apply updates
python3 scripts/import_primary_results.py --state NC
```

### AR (Arkansas) — FULLY AUTOMATED
- **Platform:** TotalResults ENR by KnowInk
- **Frontend:** https://enr.totalresults.com/arkansas/
- **API:** `https://enr-results-api.totalresults.com`
- **No auth required.** Fully open REST API with structured JSON responses.
- **Key endpoints:**
  - `Election/GetElectionList?cId=arkansas` — list all elections with IDs
  - `Contest/GetContestSearchList?cId=arkansas&electionID={id}` — contest + candidate names
  - `Contest/GetContestResults?cId=arkansas&electionID={id}&contestType={type}` — vote totals
- **Contest types:** `State Senate`, `State Representative`, `Federal`, `Statewide`, `Judge`, `County`
- **IMPORTANT:** Results and search list use separate endpoints. Choices in results are ordered to match search list but have no names — must cross-reference by index.
- **Auto-refresh:** 900 seconds (15 min) during election night
- **isWinner flag:** Set server-side when finalized — may not be set until certified
- **2026 Election IDs:**
  - `7f77a178-af02-40ec-92db-c5cc50882c68` — 2026 Preferential Primary (Mar 3)
  - `55355810-8dde-40ff-a1d2-5b8675226873` — 2026 Special General (Mar 3)
- **Full API docs:** `docs/ar_election_data_api.md`

```bash
# Quick test
curl -s "https://enr-results-api.totalresults.com/Election/GetElectionList?cId=arkansas" | python3 -m json.tool | head

# Dry run
python3 scripts/import_primary_results.py --state AR --dry-run

# Apply updates
python3 scripts/import_primary_results.py --state AR
```

### TX (Texas) — FULLY AUTOMATED
- **Platform:** Civix GoElect
- **Frontend:** https://goelect.txelections.civixapps.com/ivis-enr-ui/races
- **API:** `https://goelect.txelections.civixapps.com/api-ivis-system/api/s3/enr/`
- **No auth required.** Data is base64-encoded JSON.
- **Key endpoints:**
  - `enr/electionConstants` — list all elections with IDs
  - `enr/election/{electionId}` — full results (multiple base64-encoded sections)
- **Data sections:** `Home` (status), `Districted` (state leg + judicial), `Federal`, `StateWide`, `StateWideQ` (propositions)
- **Base64 decoding required:** Each section value is a base64-encoded JSON string
- **Race data format:** `N` (name), `V` (votes), `EV` (early votes), `PE` (percentage), `P` (party), `LN`/`FN` (last/first name), `T` (total votes)
- **Incumbents:** Marked with `(I)` suffix in candidate name
- **TX runoff threshold:** >50% required to win primary; otherwise top 2 advance to runoff
- **Auto-refresh:** 5 seconds during election night
- **Separate elections for each party:** R primary and D primary have different election IDs
- **2026 Election IDs:**
  - `53813` — 2026 Republican Primary (Mar 3)
  - `53814` — 2026 Democratic Primary (Mar 3)
- **NOTE:** Cloudflare blocks the frontend from server IPs, but the API endpoints work fine with direct requests
- **Full API docs:** `docs/tx_election_data_api.md`

```bash
# Quick test (returns base64-encoded JSON)
curl -s "https://goelect.txelections.civixapps.com/api-ivis-system/api/s3/enr/electionConstants" | python3 -c "import sys,json,base64; d=json.load(sys.stdin); print(json.dumps(json.loads(base64.b64decode(d['upload'])),indent=2)[:500])"

# Dry run
python3 scripts/import_primary_results.py --state TX --dry-run

# Apply updates
python3 scripts/import_primary_results.py --state TX
```

## Election Night Checklist

### Before results start coming in
1. Verify the SoS results URL is live (use quick test commands above)
2. Confirm all candidates are in the database
3. Run `--dry-run` to pre-check candidate name matching

### As results come in
1. Run `--dry-run` first to verify candidate matching
2. Review any UNMATCHED candidates — may need manual name fixes in DB
3. Run without `--dry-run` to update vote counts
4. Check `result_status` — script sets to 'Called' when votes are updated

### After all precincts report
1. Run the import one final time to get final unofficial totals
2. Re-export affected states:
   ```bash
   python3 scripts/export_site_data.py --state XX
   python3 scripts/export_district_data.py --state XX
   ```
3. Commit and push to deploy

### After certification
1. Update `result_status` to 'Certified' for all elections
2. Run a final re-export

## Name Matching

The script uses fuzzy name matching with:
- Nickname lookup table (Edward→Ed, William→Bill, James→Jimmy, etc.)
- Parenthetical nickname extraction: "W. H. (Bill) Morris" matches "Bill Morris"
- Middle-name-as-first-name: "A. Reece Pyrtle, Jr." matches "Reece Pyrtle"
- Suffix removal: Jr., Sr., II, III, IV
- Annotation removal: "- DECEASED", "- WITHDRAWN"
- Incumbent marker removal: "(I)" suffix

If a candidate doesn't match, the script reports it. Fix the DB name or add to the nickname table in the script.

## Key Dates (2026)

| Date | States | Type |
|------|--------|------|
| Mar 3 | AR, NC, TX | Primary |
| May 5 | IN, OH | Primary |
| May 12 | NE, WV | Primary |
| May 19 | GA, KY, OR, PA | Primary |
| Jun 2 | AL, MS, MT, NJ, NM, SD | Primary |
| Jun 9 | IA, ME, SC, VA | Primary |
| Jun 16 | DC, ND | Primary |
| Jun 23 | CO, MD, NY, UT | Primary |
| Aug 4 | KS, MI, MO, TN, WA | Primary |
| Aug 11 | CT, MN, VT, WI | Primary |
| Aug 18 | AK, AZ, FL, WY | Primary |
| Sep 8 | DE, NH, RI | Primary |
| Sep 15 | MA | Primary |
| Nov 3 | ALL | General |

## Adding New States

To add a new state to the automated import:

1. Find the state's official results API/data source (check `docs/` for existing research)
2. Add a download function: `{st}_download(election_date)` → returns raw data
3. Add a parse function: `{st}_parse_results(data, chamber_filter)` → returns standardized contest dicts
4. Add entry to `STATE_HANDLERS` dict with `download`, `parse`, and `default_date`
5. Test with `--dry-run`

The matching and DB update logic (`match_and_update`) is state-agnostic — it works with any standardized contest format.

### Research approach for new states
1. Open the state's SoS election results page in browser
2. Open browser dev tools → Network tab
3. Look for JSON/API calls as the page loads results
4. Document the API endpoints in `docs/{st}_election_data_api.md`
5. Test if endpoints work from command line (`curl`)
