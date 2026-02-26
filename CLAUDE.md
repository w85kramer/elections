# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**State Elections Tracker** — a comprehensive database and dashboard for US state-level elections. Covers all 50 states including:
- ~7,383 state legislative seats (state house + state senate)
- Governors, Attorneys General
- Ballot measures
- Primaries, general elections, special elections, and runoffs

The project focuses on state-level races (not local/municipal). The 2026 cycle is the primary focus, but the system supports historical and future data.

## Architecture

### Database (Supabase / PostgreSQL)
- Core data store is a Supabase project (project ref: `pikcvwulzfxgwfcfssxc`)
- Credentials stored in `.env` (never commit this file)
- **Database access**: Direct PostgreSQL connection is IPv6-only (unreachable from most environments). Use the **Supabase Management API** instead:
  ```
  POST https://api.supabase.com/v1/projects/{ref}/database/query
  Authorization: Bearer {SUPABASE_MANAGEMENT_TOKEN}
  Body: {"query": "SELECT ..."}
  ```
- For REST API reads/writes to table data, use the standard Supabase REST API with the service role key
- Schema defined in `schema.sql`; original template in `state_elections_database_template.xlsx`
- 9 tables: `states` → `districts` → `seats` → `elections` → `candidacies` (+ `candidates`), `seat_terms` (officeholder history), `ballot_measures`, `forecasts`
- `seat_terms` tracks every officeholder's tenure per seat (start/end dates, reason for start/end). `seats.current_holder` is kept as a convenience cache; `seat_terms WHERE end_date IS NULL` is the source of truth
- 1 view: `dashboard_view` (auto-joins candidacies + ballot measures)
- All PKs are auto-incrementing integers; categorical fields use CHECK constraints
- RLS enabled on all tables with permissive policies

### Frontend (planned)
- Website and dashboard with interactive maps and charts
- Maps: state legislative district boundaries using GeoJSON (Mapbox or Leaflet)
- Charts/graphics: election results, seat counts, partisan breakdowns
- Connected directly to Supabase via its REST API or client library

### Data Update Workflow
- Regular cadence for checking and collecting new election information
- Proposed changes are reviewed and approved before being pushed to the database
- Low volume in winter (special elections only), high volume spring/summer (primaries), peak in November (general election)

### Live Data vs. Static Exports
- **District pages** fetch uncertified 2026 election data live from Supabase PostgREST (`site/js/supabase.js`), so election results appear on district pages without re-exporting
- **State pages** and the **dashboard** rely entirely on static JSON exports — they must be re-exported to reflect changes
- **After adding special election results**: Always re-export the affected state(s) immediately. Special election winners typically assume office within days, so the state page will show stale vacancy data until re-exported. Run:
  ```
  python3 scripts/export_site_data.py --state XX
  python3 scripts/export_district_data.py --state XX
  ```
  Then commit and push the updated JSON files.
- **General elections** are less urgent — winners aren't sworn in until the following January, so there's time to do a full re-export after certifying results

## Data Sources

- **Primary trusted source**: [Ballotpedia](https://ballotpedia.org) for election data, candidate info, office details
- Supabase REST API for data reads/writes; Management API for SQL operations

## Key Complexity

Each state has its own election rules. The data model must handle:
- Different primary types: closed, open, semi-closed, jungle/top-two, nonpartisan
- Runoff elections (required in some states when no candidate hits a threshold)
- Multi-member districts (multiple seats elected from one district)
- Special elections (vacancies filled on irregular schedules)
- Varying term lengths (2-year vs 4-year state senate terms, staggered classes)
- Off-cycle elections (VA, NJ, LA hold elections in odd years)
