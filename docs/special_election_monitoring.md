# Special Election & Vacancy Monitoring Process

## Purpose

Regularly check for new state legislative vacancies and special elections so our database stays current. Vacancies can occur at any time (resignations, deaths, appointments) and special elections are scheduled on varying timelines by state.

## Frequency

- **Weekly**: Quick check of primary sources during active periods (Jan-Nov)
- **Biweekly**: Sufficient during quieter periods (Dec)
- **Immediately after**: Any major political event (mass resignations, natural disasters, etc.)

## Primary Sources (check in order)

### 1. Ballotpedia 2026 Election Calendar (Google Sheet)
- **URL**: https://docs.google.com/spreadsheets/d/16td_nOU3bsrNyb5l_G-14cuP26M4LZrXh9Z-GTuCpP0/
- **What to check**: New rows for special elections, updated dates, new vacancies
- **Frequency**: Weekly
- **Notes**: Living document updated by BP editors. Has "Earliest expected data availability" column. Includes local elections — filter to state legislative and statewide.

### 2. Ballotpedia State Legislative Special Elections Pages
- **URL pattern**: `https://ballotpedia.org/{State}_state_legislative_special_elections,_{year}`
- **What to check**: Compare listed races against our DB. Look for new entries.
- **Frequency**: Weekly
- **Master list**: `https://ballotpedia.org/State_legislative_special_elections,_2026`
- **Notes**: BP is usually the fastest aggregator for new vacancies. Pages organized by state with dates, candidates, and results.

### 3. Ballotpedia Elections Calendar Page
- **URL**: `https://ballotpedia.org/Elections_calendar`
- **What to check**: Upcoming election dates for the next 30-60 days
- **Frequency**: Weekly
- **Notes**: Good for catching elections we might have dates wrong for.

### 4. State Secretary of State / Election Board Websites
- **When**: After BP flags a new special election, verify dates and candidates from official source
- **What to check**: Official proclamation, filing deadlines, candidate lists, certified results
- **Notes**: See `filing_data_sources.md` for per-state official sources. These are authoritative for dates and certified results.

### 5. NH House Clerk — Resignations, Deaths, Special Elections (PDF)
- **URL**: `https://gc.nh.gov/house/aboutthehouse/RDSE.pdf`
- **What to check**: Official list of all NH House vacancies, resignations, deaths, and special elections for the current session. Updated periodically by the Clerk's office.
- **Frequency**: Monthly (or whenever doing NH-specific checks)
- **Notes**: NH House has 400 members across 203 multi-member districts — vacancies are frequent and hard to track from BP alone. This is the authoritative source during legislative sessions. As of March 2026: 8 vacancies (7 resignations + 1 death), only 2 specials held so far.

### 6. National Conference of State Legislatures (NCSL)
- **URL**: `https://www.ncsl.org/research/elections-and-campaigns/2026-state-legislative-special-elections.aspx`
- **What to check**: Cross-reference vacancy list against BP
- **Frequency**: Monthly
- **Notes**: Sometimes has vacancy announcements before BP pages are updated.

## Monitoring Checklist

### New Vacancies
For each new vacancy found:

- [ ] Identify the state, chamber, district
- [ ] Research and record the vacancy reason:
  - **Resigned** — why? (new job, scandal, health, personal, ran for other office)
  - **Died** — when?
  - **Expelled** — by vote of chamber
  - **Appointed to other office** — which office?
  - **Recalled** — by voters
  - **Other** (felony conviction, residency change, etc.)
- [ ] Record the previous officeholder's name and party
- [ ] End their `seat_terms` record with correct `end_date` and `end_reason`
- [ ] Update `seats.current_holder` (set to NULL if vacant, or appointee name if appointed)
- [ ] Check if a special election has been called yet (some states have long lead times)

### New Special Elections
For each new special election found:

- [ ] Create `elections` record with correct `seat_id`, `election_type`, `election_date`
- [ ] Add vacancy context in `elections.notes` (who left, why, when)
- [ ] If candidates are known, create `candidates` + `candidacies` records
- [ ] For states with primaries before specials, create both Special_Primary and Special records
- [ ] For jungle primary states (LA, CA, WA), use appropriate election_type

### Election Results
For elections with newly available results:

- [ ] Update `candidacies` with `votes_received`, `vote_percentage`, `result` (Won/Lost/Advanced)
- [ ] Update `elections.result_status` (Called → Certified as appropriate)
- [ ] If result triggers a runoff, create `Special_Runoff` election + candidacies for advancing candidates
- [ ] For winners: create `seat_terms` record, update `seats.current_holder`
- [ ] Re-export affected state data:
  ```bash
  python3 scripts/export_site_data.py --state XX
  python3 scripts/export_district_data.py --state XX
  ```
- [ ] Commit and push updated JSON files

## State-Specific Notes

### Jungle Primary States
- **Louisiana**: All candidates on one ballot. If no one gets 50%+, top 2 go to runoff. Use `Special` for initial, `Special_Runoff` for runoff.
- **California**: Top-two primary system. Similar to LA.
- **Washington**: Top-two primary.

### Odd-Year Election States
- **Virginia**: Elections in odd years. Specials can fall on regular election days.
- **New Jersey**: Elections in odd years.

### States with Appointment-Only (No Special Elections)
Some states fill vacancies by appointment only (no special election). Still track the vacancy and appointee in `seat_terms`.

### District Naming Conventions
- **Massachusetts**: BP uses "5th Essex" style; our DB uses numeric district numbers
- **Minnesota**: BP uses "47A"/"47B" sub-districts; our DB uses sequential numbers (HD-93, HD-94)
- **New Hampshire**: BP uses county-based names ("Belknap-1"); our DB uses numeric
- **Vermont**: Similar to NH with county-based names

## Cross-Reference Query

Run this SQL to compare our DB against an expected count:

```sql
-- Count special elections by state for current year
SELECT st.abbreviation, COUNT(*) as specials
FROM elections e
JOIN seats s ON e.seat_id = s.id
JOIN districts d ON s.district_id = d.id
JOIN states st ON d.state_id = st.id
WHERE e.election_type IN ('Special', 'Special_Primary', 'Special_Runoff',
                           'Special_Primary_D', 'Special_Primary_R', 'Special_Runoff_R')
AND e.election_year = 2026
GROUP BY st.abbreviation
ORDER BY st.abbreviation;
```

## Result Status Flow

```
NULL → Called → Certified
                ↘ (if runoff needed) → create Special_Runoff election
```

- **NULL**: Election scheduled, no results yet
- **Called**: Results reported by media/unofficial sources
- **Certified**: Official results certified by state authority
