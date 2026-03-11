# State Filing Data Sources & Processes

Reference for collecting official candidate filing data from each state's Secretary of State (or equivalent). This supplements Ballotpedia data with official sources.

## Completed States

### Illinois (IL)
- **Filing deadline**: Nov 3, 2025
- **Primary**: Mar 18, 2026
- **Official source**: Illinois State Board of Elections (ISBE) — https://www.elections.il.gov/
- **Data access method**: Chrome extension extraction from ASP.NET paginated table
  - Navigate to candidate search, set page size to "All", use Chrome Claude to extract TSV
  - Statewide and legislative candidates on separate pages
- **Format**: Web table → TSV (Name in "LAST, FIRST" format, Party, Office, District, Status)
- **Notes**:
  - Names in ALL CAPS "LAST, FIRST" format — need normalize_name() conversion
  - Status field includes "Active" and "Withdrawn" — filter out withdrawn
  - No bulk download option; ASP.NET renders server-side
  - Created `scripts/populate_il_candidates.py` for import
  - Found duplicate candidates in DB after import — IL data is authoritative over BP

### Arkansas (AR)
- **Filing deadline**: Nov 12, 2025
- **Primary**: Mar 3, 2026 (completed)
- **Official source**: AR SoS — https://candidates.arkansas.gov/
- **Data access method**: CSV download via WordPress REST API
  - Site has search interface; CSV export available
  - State Reps and State Senate downloaded as separate CSVs
- **Format**: CSV with columns: Candidate Name, Position/Office, Party Affiliation, Filing Date
- **Notes**:
  - Names include title prefixes ("Representative", "State Representative", "Justice of the Peace")
  - Filing dates as YYYY-MM-DD
  - AR primary already held — this was verification only
  - Found Brad Simon special election edge case (SD-26)
  - Fixed Stubblefield death record (died Sep 2, 2025, was recorded as resigned)

### Texas (TX)
- **Filing deadline**: Dec 8, 2025
- **Primary**: Mar 3, 2026 (completed)
- **Status**: Skipped — primary already held, results collected

### North Carolina (NC)
- **Filing deadline**: Dec 19, 2025
- **Primary**: Mar 3, 2026 (completed)
- **Status**: Skipped — primary already held, results collected

### Utah (UT)
- **Filing deadline**: Jan 8, 2026
- **Primary**: Jun 23, 2026
- **Official source**: Utah Lt. Governor — https://vote.utah.gov/
- **Data access method**: Excel spreadsheet download
  - Direct .xlsx file available from election filing page
  - Parsed with openpyxl
- **Format**: XLSX with columns: Candidate, Office, Party, Status
- **Notes**:
  - Includes Forward Party, Constitution Party, Unaffiliated candidates
  - Status field shows "Active" or "Withdrew"
  - 287 candidates (278 active, 9 withdrew)
  - Saved parsed data to `/tmp/ut_2026_candidates.json`

### Kentucky (KY)
- **Filing deadline**: Jan 9, 2026
- **Primary**: May 19, 2026
- **Official source**: KY SoS — https://web.sos.ky.gov/CandidateFilings/
- **Data access method**: HTML page parsing (ASP.NET)
  - State Reps: `default.aspx?elecid=86&id=12`
  - State Senate: `default.aspx?elecid=86&id=11`
  - Also has Excel export button
  - HTML parsed with Python HTMLParser
- **Format**: HTML table with columns: Name, Email/Address, Office, District, Party, Date Filed
- **Notes**:
  - District numbers have ordinal suffixes ("1st", "2nd") — strip before matching
  - 198 House + 42 Senate = 240 total candidates
  - Found 3 Jr. suffix mismatches, 2 withdrawn candidates (O'Brien, Miller)
  - Merged Jim Gooch duplicate candidate records

### Alabama (AL)
- **Filing deadline**: Jan 23, 2026
- **Primary**: May 19, 2026
- **Official source**: AL SoS — https://www.sos.alabama.gov/alabama-votes/voter/election-information/2026
- **Data access method**: State Certification PDFs from SoS website
  - Republican: `2026StateCertificationOfRepublicanCandidates.pdf` (20 pages, signed Mar 6, 2026)
  - Democratic: `2026StateCertificationOfDemocraticCandidates.pdf` (16 pages, signed Mar 6, 2026)
  - Download via `curl -ksSL` (SoS site has SSL certificate issues, needs `--insecure` flag)
  - Links found on the 2026 Election Information page
- **Format**: Scanned PDF tables with columns: Office, District/Jurisdiction, Place, Ballot Name
- **Notes**:
  - AL SoS website has SSL certificate errors — WebFetch fails, need `curl -k`
  - Party websites (algop.org, aldemocrats.org) have candidate lists BUT with incorrect district numbering — don't trust those
  - Official SoS PDFs are the authoritative source with correct district numbers
  - 250 state legislature candidates (41 R Senate, 25 D Senate, 108 R House, 76 D House)
  - Marked 8 candidates as withdrawn (in DB from Ballotpedia but not in official certification)
  - 1 name spelling difference: SoS PDF has "Livingstoon" vs DB "Livingston" (PDF typo)
  - Alabama qualifies candidates through party certification to SoS, not direct SoS filing
  - No districts 52-60 or 71-72 have Republican candidates (majority-minority districts)

### West Virginia (WV)
- **Filing deadline**: Jan 31, 2026
- **Primary**: May 12, 2026
- **Official source**: WV SoS — https://apps.sos.wv.gov/elections/candidate-search/
- **Data access method**: CSV export from candidate search
  - The 2026 year doesn't appear in the dropdown, but there's a "2026 Candidates Listing" link on the page
  - Click that link, then use the "Export" button to download CSV
  - The streaming filings page (`services.sos.wv.gov/Elections/CandidateFilings`) times out
- **Format**: CSV with columns: Name, Legal Name, Party, County, Race, District/Circuit, Division, Magisterial, City, State, Residence County, MailingAddress, Filing Date, CampaignPhoneNumber, Email
- **Notes**:
  - Names in ALL CAPS — use `.title()` for display
  - Has "Legal Name" column (full legal name) in addition to ballot name
  - Includes Libertarian and Independent candidates (filter to D/R for primary matching)
  - 331 state legislature candidates (66 Senate, 265 House of Delegates)
  - Marked 23 DB candidates as withdrawn, added 7 new
  - Kathryn Weiland switched parties (R → D) in HD-17
  - Some filing dates after Jan 31 deadline (Feb 2-3) — possibly allowed by extension
  - WV Senate has dual seats (A and B) per district — unexpired terms run as special elections
  - SD-3 and SD-17 Seat B had special election candidacies; created Special_Primary_D/R elections for Seat B
  - Cleaned up duplicate Barnhart/Harshbarger candidate records (SD-3 Seat B)

### Nebraska (NE)
- **Filing deadline**: Mar 2, 2026
- **Primary**: May 12, 2026
- **Official source**: NE SoS — https://sos.nebraska.gov/elections
- **Data access method**: Direct Excel download
  - URL: `sos.nebraska.gov/sites/default/files/doc/elections/2026/Statewide_Candidate_Filing_List.xlsx`
  - No auth required, direct HTTP GET
- **Format**: XLSX with 3 sheets (Primary, General, Petitions). Columns: Office, District Name, Term, Vote For, Party, Candidate Name, City, Incumbency Status, Mailing Address, Phone/Email
- **Notes**:
  - Unicameral nonpartisan legislature — chamber = "Legislature" in DB, party = NP for all
  - 25 even-numbered districts up in 2026 + LD-41 special election
  - 61 legislature candidates added (58 regular + 3 special)
  - 17 statewide candidates added (Governor, SoS, Treasurer, AG, Auditor — D and R)
  - 2 "Legal Marijuana NOW" party governor candidates skipped (no primary elections for that party)
  - DB had elections but zero candidacies before this import
  - Incumbency status provided in SoS data

### Idaho (ID)
- **Filing deadline**: Feb 27, 2026
- **Primary**: May 19, 2026
- **Official source**: ID SoS — https://run.voteidaho.gov/search
- **Data access method**: Excel export from SERVIS candidate search
  - Search page at `run.voteidaho.gov/search` (Cloudflare-protected, no automated access)
  - Must search manually in browser, then export to Excel
  - Separate exports for "State Legislature" and "Statewide" district types
  - User downloaded files manually to ~/Downloads
- **Format**: XLSX with columns: Ballot Name, Election Date, District Type, Office, District, Seat/Zone, Party Affiliation, Filing Date, Write-In, Withdrawal Date, plus mailing address
- **Notes**:
  - openpyxl `read_only=True` mode returns empty data — must use `read_only=False`
  - House uses Seat A/B system (2 seats per district, 35 districts = 70 total)
  - 272 legislature candidates (262 active, 10 withdrawn), 30 statewide
  - Official SoS data has Withdrawal Date column — reliable withdrawal indicator
  - Marked 44 total as withdrawn, added 101 new candidates
  - Some candidates switched chambers/districts between BP data and official filing (Galaviz House→Senate, McCann House→Senate, etc.)
  - Third-party candidates (I, L, C) present but not in primary elections
  - LESSON: Do not use third-party news compilations — always get official .gov data, even if manual download required

### Maryland (MD)
- **Filing deadline**: Feb 24, 2026
- **Primary**: Jun 23, 2026
- **Official source**: MD State Board of Elections — https://elections.maryland.gov/elections/2026/primary_candidates/index.html
- **Data access method**: Direct CSV downloads (no auth required)
  - Senate: `2026_GP_statesenatorbydistrict_candidatelist.csv`
  - Delegates: `2026_GP_houseofdelegatesbydistrict_candidatelist.csv`
  - Statewide: `2026_GP_statewide_candidatelist.csv`
  - All at `elections.maryland.gov/elections/2026/primary_candidates/`
- **Format**: CSV (UTF-8 with BOM — use `encoding='utf-8-sig'`), 35 columns
  - Key: Office Name, Contest Run By District Name and Number, Candidate Ballot Last Name and Suffix, Candidate First Name and Middle Name, Office Political Party, Candidate Status
- **Notes**:
  - BOM character on first column header — must use `utf-8-sig` encoding
  - Status field: Active, Withdrawn, Deceased, Disqualified
  - Senate: 100 active candidates across 47 districts
  - Delegates: 330 active candidates across 71 districts (sub-districts: 1A, 1B, 1C, etc.)
  - Had many duplicate candidacy records in DB from Ballotpedia (removed 6 duplicates)
  - Marked 6 withdrawn, added 7 new Senate candidates
  - Delegate candidacies not yet in DB — multi-member districts need Seat A assignment convention
  - Multi-member districts (3-seat): all candidates run at-large, assign to Seat A for DB
  - Data updated regularly (page shows last update date)

### Indiana (IN)
- **Filing deadline**: Feb 6, 2026
- **Primary**: May 5, 2026
- **Official source**: IN SoS Election Division — https://www.in.gov/sos/elections/candidate-information/
- **Data access method**: Excel download
  - Direct .xlsx file: `Primay-Candidate-Mailing-List-2.25.2026.xlsx` (note typo in filename)
  - 13,115 rows total including all local races; filter to STATE REPRESENTATIVE / STATE SENATOR
- **Format**: XLSX with columns: OFFICE, CANDIDATE NAME, POLITICAL PARTY, DISTRICT, DATE FILED
- **Notes**:
  - 304 state legislature candidates (216 House, 88 Senate across 100+25 districts)
  - No statewide executive races in 2026 (those are presidential-year elections in IN)
  - District numbers embedded in text string (e.g., "State Representative, District 001")
  - Marked 10 DB candidates as withdrawn (not in official SoS list)
  - Fixed spelling: Novhad → Nouhad Melki (HD-36)
  - Suffix formatting differences (Jr., II, III, Sr.) between SoS and Ballotpedia — matched by last name

### New Mexico (NM)
- **Filing deadline**: Feb 3, 2026 (statewide), Mar 10, 2026 (all races)
- **Primary**: Jun 2, 2026
- **Official source**: NM SoS SERVIS Portal — https://candidateportal.servis.sos.state.nm.us/CandidateList.aspx?eid=2911&cty=99
- **Data access method**: CSV/Excel/PDF export from Telerik RadGrid
  - Portal at servis.sos.state.nm.us (not sos.nm.gov)
  - Election ID 2911 = 2026 Primary, cty=99 = all counties
  - Export buttons for XLS, PDF, CSV built into the grid
- **Format**: 23 columns including Contest, District, Full Name, First/Middle/Last Name, Party (DEM/REP), Filing Date, Status
- **Notes**:
  - Names in ALL CAPS, separate first/middle/last columns available
  - Status field: "Qualified" or "Disqualified"
  - Statewide candidates available after Feb 3 deadline; legislative after Mar 10
  - As of Mar 9: 47 statewide/congressional candidates on portal
  - Verified Governor + Lt Gov candidates; added 8 new statewide candidacies (AG, SoS, Auditor, Treasurer)
  - Ken Miyagishima withdrew from D primary to run as independent (Feb 2, 2026)
  - Commissioner of Public Lands candidates exist but no election in DB yet
  - Legislative verification pending after Mar 10 deadline

### Ohio (OH)
- **Filing deadline**: Feb 4, 2026
- **Primary**: May 5, 2026
- **Official source (statewide)**: OH SoS press release — https://www.ohiosos.gov/media-center/press-releases/2026/2026-02-19/
- **Data access method (statewide)**: Press release text (SoS website blocks automated access, 403)
  - User copied press release text directly
  - Lists candidates who met minimum signature requirements
- **Format**: Text list grouped by office, with candidate name and party
- **Notes**:
  - **Statewide verified**: Governor/Lt Gov (paired tickets), AG, SoS, Auditor, Treasurer
  - 4 candidates in DB didn't qualify (Turner/Turner Gov/LtGov pair, Antani and Zuren for Treasurer)
  - Marked 4 as Withdrawn_Pre_Ballot (didn't meet signature requirements)
  - Fixed spelling: Elliot → Elliott Forhan (AG)
  - No new candidates to add (all SoS-qualified candidates already in DB)
  - **Legislature: Using Ballotpedia data only** — OH has no central state filing list
  - Legislature filing is decentralized across 88 county Boards of Elections
  - Each county publishes its own PDF candidate list in a different format (tabular, text, varying columns)
  - Investigated Franklin (Columbus), Stark (Canton), Cuyahoga (Cleveland), Hamilton (Cincinnati), Erie county BOE PDFs
  - Formats are too inconsistent for automated parsing — would require 88 separate parsers
  - Decision: accept BP data for OH legislature, revisit after May 5 primary if needed
  - Franklin County PDF (rev. 3/6/2026) confirmed: Elizabeth G. Richards (HD-02) Withdrawn, plus write-in candidates (Bernadine Kennedy Kent HD-03, Richard Cole HD-05, Kelly Hunter-Kalagidis HD-08)

### Georgia (GA)
- **Qualifying period**: Mar 2-6, 2026
- **Primary**: May 19, 2026
- **Official source**: GA SoS MVP Portal — https://mvp.sos.ga.gov/s/qualifying-candidate-information
- **Data access method**: CSV download from MVP portal
  - Salesforce-based SPA — requires browser interaction, but has a download button
  - Separate downloads for legislative and statewide candidates
  - Filter by election year and contest type
- **Format**: CSV with columns: Contest Name, County, Municipality, Candidate Name, Candidate Status, Political Party, Qualified Date, Incumbent, Occupation, Email Address, Website
- **Notes**:
  - Contest Name format: "State House, District 16 (D)" — includes party of contest
  - Names in ALL CAPS — use `.title()` for display
  - Status values: Qualified, Withdrawn
  - GA uses "qualifying" not "filing" — candidates pay fee during qualifying period
  - 537 qualified D/R legislature candidates (146 Senate, 391 House), 3 withdrawn
  - 47 statewide candidates (Governor, Lt Gov, SoS, AG, School Super, Commissioners of Agriculture/Insurance/Labor)
  - Marked 45 DB candidates as withdrawn (not in official qualified list)
  - Added 379 new legislature + 47 new statewide candidates
  - One formatting quirk: District 151 missing space before parenthesis — regex needs `\s*` not ` `
  - DB office_type uses "Lt. Governor" (abbreviated), "Agriculture Commissioner" etc. — map from SoS names
  - Created `scripts/populate_ga_candidates.py` for import

### Montana (MT)
- **Filing deadline**: Mar 4, 2026
- **Primary**: Jun 2, 2026
- **Official source**: MT SoS — https://candidatefiling.mt.gov/candidatefiling/CandidateList.aspx?e=450002928
- **Data access method**: CSV export from ASP.NET Telerik RadGrid
  - Page has "Export" button — downloads full candidate list as CSV
  - Includes all race types (federal, statewide, judicial, legislative)
  - Filter to District Type = "House" or "Senate" for state legislature
- **Format**: CSV (UTF-8 with BOM) with columns: Status, District Type, District, Race, Term Type, Term Length, Name, Mailing Address, Email/Web Address, Phone, Filing Date, Party Preference, Ballot Order
- **Notes**:
  - BOM on first column header — use `encoding='utf-8-sig'`
  - Names in ALL CAPS, incumbents prefixed with `*`
  - Status values: FILED, WITHDRAWN, PENDING PETITION
  - Party codes: REP, DEM, LIB, IND
  - 309 filed state legislature candidates (69 Senate, 240 House) + 5 LIB + 9 withdrawn + 3 petition
  - 306 D/R candidates imported (68 Senate, 238 House)
  - Marked 12 DB candidates as withdrawn (not in official SoS list)
  - 7 chamber/district switches detected (candidates moved between House↔Senate or changed districts)
    - Valynda Holland HD-20→SD-11, Scott Rosenzweig HD-57→SD-29, Kathleen Williams HD-31→SD-31
    - Becky Edwards HD-61→SD-32, Stephen LaPraim HD-17→SD-42, Jennifer Carlson SD-34→HD-68
    - George Nikolakakos SD-11→SD-12
  - Name matching challenges: multi-word last names (Fire Thunder), hyphens (Seekins-Crowe vs Seekins Crowe), first name variants (Anthony/Tony Rosales), maiden name changes (Lisa Fowler → Lisa Verlanic-Fowler)
  - Created `scripts/populate_mt_candidates.py` for import

### Tennessee (TN)
- **Filing deadline**: Mar 10, 2026
- **Primary**: May 5, 2026
- **Official source**: TN SoS — https://sos.tn.gov/elections/2026-candidate-lists
- **Data access method**: Direct Excel download (no auth)
  - Senate: `sos-prod.tnsosgovfiles.com/.../TNSenate_Filed_2026-03-10.xlsx`
  - House: `sos-prod.tnsosgovfiles.com/.../TNHouse_Filed_2026-03-10.xlsx`
- **Format**: XLSX with 6 columns: Office, Candidate, Party, City, Filed, Status
- **Notes**:
  - 35 Senate + 223 House = 258 total candidates (132R, 111D, 15I)
  - District naming: "Tennessee Senate District N", "Tennessee House of Representatives District N"
  - Status: "Signatures Approved" for all
  - 15 Independents skipped (no primary election for them)
  - 3 withdrawals: Adam Stallings (HD-2), Jerome Moon (HD-8), Randy McNally (SD-5, Senate President Pro Tem)
  - 126 new candidates added
  - Name aliases handled: Gary Hicks/Hicks Jr, Esther Helton/Helton Haynes, Harold Love/Love Jr, G.A. Hardaway/Goffrey Hardaway
  - Created `scripts/populate_tn_candidates.py`

### Oregon (OR)
- **Filing deadline**: Mar 10, 2026
- **Primary**: May 19, 2026
- **Official source**: OR SoS ORESTAR — https://secure.sos.state.or.us/orestar/CFSearchPage.do
- **Data access method**: Excel export (XLS format) from ORESTAR search
  - Click "Search Current Election" then "Export" button
  - Session-based — requires browser interaction
  - Uses xlrd library (old .xls format)
- **Format**: XLS with 52 columns. Key: Office Group, Office (district), Party Descr, Cand Ballot Name, First/Last Name, Qlf Ind, Witdrw Date
- **Notes**:
  - 191 state legislature candidates (89R, 102D), all qualified
  - Senate: 43 candidates, House: 148 candidates
  - 15 withdrawals marked, 63 new candidates added
  - Created `scripts/populate_or_candidates.py`

### Pennsylvania (PA)
- **Filing deadline**: Mar 10, 2026
- **Primary**: May 19, 2026
- **Official source**: PA DoS — https://www.pavoterservices.pa.gov/electioninfo/ElectionInfo.aspx
- **Data access method**: Excel export from candidate database (browser only, Incapsula bot protection)
  - Separate downloads for House and Senate
  - `Election Info (1).xlsx` = House (378 rows), `Election Info (2).xlsx` = Senate (59 rows)
- **Format**: XLSX, row 1 = title "Election Info", row 2 = headers (Name, Office, District Name, Party, Municipality, County)
  - Names in ALL CAPS "LAST, FIRST MIDDLE" format
  - District: "19th Legislative District" or "20th Senatorial District"
- **Notes**:
  - 435 candidates (377 House + 58 Senate)
  - Only 16 existing BP candidacies — this was essentially a full fresh import
  - 2 withdrawals (Justin Byers HD-9, Joseph Leckenby HD-42)
  - 421 new candidates added
  - Created `scripts/populate_pa_candidates.py`

### New Mexico — Legislature (NM)
- **Filing deadline**: Mar 10, 2026 (legislative)
- **Primary**: Jun 2, 2026
- **Official source**: NM SoS SERVIS Portal — https://candidateportal.servis.sos.state.nm.us/CandidateList.aspx?eid=2911&cty=99
- **Data access method**: CSV export from Telerik RadGrid (manual browser download)
- **Format**: CSV with 19 columns. Key: Contest, District, First/Last Name, Party (DEM/REP), Status
- **Notes**:
  - 125 State Rep + 2 State Senator (SD-33 only) = 127 legislative candidates
  - 3 rows with bad party values skipped (suffix leaked into Party column: " II", " JR")
  - SD-33 appears to be a special election (all 42 Senate seats have 4-year terms, last elected 2024) — 2 candidates skipped pending special election setup
  - 2 withdrawals (Corrine Barraza HD-30, Benjamin Luna HD-53)
  - 113 new candidates added
  - Created `scripts/populate_nm_leg_candidates.py`

## Pending States (in filing deadline order)

| State | Filing Deadline | Primary | SoS Website | Notes |
|-------|----------------|---------|-------------|-------|
| NM (statewide) | Feb 3, 2026 | Jun 2 | sos.nm.gov | Statewide verified ✓ |
| OH | Feb 4, 2026 | May 5 | ohiosos.gov | Statewide verified ✓. Legislature: BP data only (88 county BOEs, inconsistent PDF formats). |
| IN | Feb 6, 2026 | May 5 | in.gov/sos | Verified ✓ |
| MD | Feb 24, 2026 | Jun 23 | elections.maryland.gov | Senate verified ✓. Delegates: 330 candidates parsed, need bulk import (multi-member district complexity). |
| ID | Feb 27, 2026 | May 19 | sos.idaho.gov | Verified ✓ |
| NE | Mar 2, 2026 | May 12 | sos.nebraska.gov | Verified ✓ |
| MT | Mar 4, 2026 | Jun 2 | sosmt.gov | Verified ✓ |
| CA | Mar 6, 2026 (ext. Mar 11) | Jun 2 | sos.ca.gov | BP data loaded (31). Certified list publishes **Mar 26** — follow up then. Top-two primary. |
| GA | Mar 2-6, 2026 | May 19 | sos.ga.gov | Verified ✓ |
| NM (full) | Mar 10, 2026 | Jun 2 | sos.nm.gov | Legislature verified ✓ (113 new, 2 withdrawn). SD-33 special election needs setup. |
| OR | Mar 10, 2026 | May 19 | sos.oregon.gov | Verified ✓ (63 new, 15 withdrawn) |
| PA | Mar 10, 2026 | May 19 | dos.pa.gov | Verified ✓ (421 new, 2 withdrawn) |
| TN | Mar 10, 2026 | May 5 | sos.tn.gov | Verified ✓ (126 new, 3 withdrawn incl. Randy McNally) |
