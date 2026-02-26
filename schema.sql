-- ============================================================
-- STATE ELECTIONS DATABASE — PostgreSQL / Supabase Schema
-- ============================================================
-- Converted from state_elections_database_template.xlsx
-- Auto-incrementing integer PKs; human-readable labels as columns
-- Dashboard_View implemented as a PostgreSQL VIEW
-- ============================================================

-- ============================================================
-- 1. STATES
-- ============================================================
CREATE TABLE states (
    id              SERIAL PRIMARY KEY,
    state_name      TEXT NOT NULL UNIQUE,
    abbreviation    VARCHAR(2) NOT NULL UNIQUE,
    senate_seats    INTEGER,
    house_seats     INTEGER,
    senate_term_years   INTEGER,
    house_term_years    INTEGER,
    uses_jungle_primary     BOOLEAN DEFAULT FALSE,
    has_runoffs             BOOLEAN DEFAULT FALSE,
    has_multimember_districts BOOLEAN DEFAULT FALSE,
    gov_term_years      INTEGER,
    gov_term_limit      TEXT,
    next_gov_election_year  INTEGER,
    notes           TEXT
);

-- ============================================================
-- 2. DISTRICTS
-- ============================================================
CREATE TABLE districts (
    id              SERIAL PRIMARY KEY,
    state_id        INTEGER NOT NULL REFERENCES states(id) ON DELETE CASCADE,
    office_level    TEXT NOT NULL CHECK (office_level IN ('Legislative', 'Statewide')),
    chamber         TEXT CHECK (chamber IN (
                        'Senate', 'House', 'Assembly', 'House of Delegates',
                        'Legislature', 'Council', 'Statewide'
                    )),
    district_number TEXT,               -- e.g., '1', '30', 'At-Large'
    district_name   TEXT,               -- display name, e.g. 'HD-1', 'SD-3', '1st Barnstable'
    num_seats       INTEGER DEFAULT 1,  -- >1 for multi-member districts
    is_floterial    BOOLEAN DEFAULT FALSE, -- TRUE for NH overlay districts
    pres_2024_margin    TEXT,           -- e.g., '+12.3' or '-15.7'
    pres_2024_winner    VARCHAR(1),     -- 'R' or 'D'
    partisan_lean       TEXT,           -- e.g., 'R+8', 'D+11'
    redistricting_cycle TEXT,           -- first election year using this map:
                                       --   '2022' = current (2020 Census), '2012' = previous (2010 Census)
                                       --   'permanent' = statewide districts (no redistricting)
    notes           TEXT
);

CREATE INDEX idx_districts_state_id ON districts(state_id);
CREATE INDEX idx_districts_office_level ON districts(office_level);

-- ============================================================
-- 3. SEATS
-- ============================================================
CREATE TABLE seats (
    id              SERIAL PRIMARY KEY,
    district_id     INTEGER NOT NULL REFERENCES districts(id) ON DELETE CASCADE,
    office_level    TEXT NOT NULL CHECK (office_level IN ('Legislative', 'Statewide')),
    office_type     TEXT NOT NULL CHECK (office_type IN (
                        'State Senate', 'State House', 'State Legislature',
                        'Governor', 'Lt. Governor', 'Attorney General',
                        'Secretary of State', 'Treasurer', 'Controller',
                        'Auditor', 'Insurance Commissioner',
                        'Superintendent of Public Instruction',
                        'Agriculture Commissioner', 'Labor Commissioner',
                        'Other Statewide'
                    )),
    seat_label      TEXT NOT NULL,       -- e.g., 'VA Senate 1', 'CA Governor'
    seat_designator VARCHAR(5),          -- e.g., 'A', 'B', 'C' for multi-member
    term_length_years   INTEGER,
    election_class  TEXT,               -- e.g., '1', '2' for staggered senate terms
    next_regular_election_year  INTEGER,
    current_holder      TEXT,
    current_holder_party TEXT,
    current_holder_caucus TEXT,          -- effective partisan alignment (may differ from party)
    selection_method TEXT CHECK (selection_method IN (
                        'Elected', 'Appointed', 'Ex_Officio', 'Not_Applicable'
                    )),                 -- how this seat is filled
    notes           TEXT
);

CREATE INDEX idx_seats_district_id ON seats(district_id);
CREATE INDEX idx_seats_office_level ON seats(office_level);
CREATE INDEX idx_seats_office_type ON seats(office_type);

-- ============================================================
-- 4. ELECTIONS
-- ============================================================
CREATE TABLE elections (
    id              SERIAL PRIMARY KEY,
    seat_id         INTEGER NOT NULL REFERENCES seats(id) ON DELETE CASCADE,
    election_date   DATE,
    election_year   INTEGER NOT NULL,
    election_type   TEXT NOT NULL CHECK (election_type IN (
                        'General', 'General_Runoff',
                        'Primary', 'Primary_D', 'Primary_R', 'Primary_Nonpartisan',
                        'Special', 'Special_Primary', 'Special_Runoff',
                        'Recall'
                    )),
    related_election_id INTEGER REFERENCES elections(id),  -- links primary→general, runoff→parent
    filing_deadline     DATE,
    forecast_rating     TEXT,            -- latest/summary rating
    forecast_source     TEXT,
    pres_margin_this_cycle  TEXT,        -- presidential margin in this district
    previous_result_margin  TEXT,        -- prior election margin
    result_status       TEXT CHECK (result_status IN (
                            'Counting', 'Called', 'Certified'
                        )),                 -- NULL=not yet held, Counting=votes coming in, Called=projected winner, Certified=official
    total_votes_cast    INTEGER,
    is_open_seat        BOOLEAN,         -- TRUE if no incumbent running, NULL=unknown
    notes           TEXT
);

CREATE INDEX idx_elections_seat_id ON elections(seat_id);
CREATE INDEX idx_elections_election_year ON elections(election_year);
CREATE INDEX idx_elections_election_type ON elections(election_type);
CREATE INDEX idx_elections_election_date ON elections(election_date);
CREATE INDEX idx_elections_related ON elections(related_election_id);

-- ============================================================
-- 5. CANDIDATES
-- ============================================================
CREATE TABLE candidates (
    id              SERIAL PRIMARY KEY,
    full_name       TEXT NOT NULL,
    first_name      TEXT,
    last_name       TEXT,
    gender          VARCHAR(1),
    date_of_birth   DATE,
    hometown        TEXT,
    notes           TEXT
);

CREATE INDEX idx_candidates_last_name ON candidates(last_name);

-- ============================================================
-- 6. CANDIDACIES (junction: candidate × election)
-- ============================================================
CREATE TABLE candidacies (
    id              SERIAL PRIMARY KEY,
    election_id     INTEGER NOT NULL REFERENCES elections(id) ON DELETE CASCADE,
    candidate_id    INTEGER NOT NULL REFERENCES candidates(id) ON DELETE CASCADE,
    party           TEXT,
    caucus          TEXT,               -- effective partisan alignment (may differ from party, e.g. NE)
    candidate_status TEXT CHECK (candidate_status IN (
                        'Announced', 'Filed', 'Active',
                        'Withdrawn_Pre_Ballot', 'Withdrawn_Post_Ballot',
                        'Disqualified', 'Write-In'
                    )),
    is_incumbent    BOOLEAN DEFAULT FALSE,
    is_major        BOOLEAN DEFAULT FALSE,
    is_write_in     BOOLEAN DEFAULT FALSE,
    filing_date     DATE,
    withdrawal_date DATE,
    votes_received  INTEGER,
    vote_percentage NUMERIC(5,2),
    rcv_round_eliminated INTEGER,       -- RCV: round in which candidate was eliminated (NULL=winner or non-RCV)
    rcv_final_votes INTEGER,            -- RCV: votes in final round (votes_received = first round)
    rcv_final_percentage NUMERIC(5,2),  -- RCV: percentage in final round
    result          TEXT CHECK (result IN (
                        'Won', 'Lost', 'Runoff', 'Advanced',
                        'Withdrawn', 'Disqualified', 'Pending'
                    )),
    endorsements    TEXT,
    notes           TEXT
);

CREATE INDEX idx_candidacies_election_id ON candidacies(election_id);
CREATE INDEX idx_candidacies_candidate_id ON candidacies(candidate_id);
CREATE INDEX idx_candidacies_party ON candidacies(party);
CREATE INDEX idx_candidacies_result ON candidacies(result);

-- ============================================================
-- 7. BALLOT MEASURES (standalone)
-- ============================================================
CREATE TABLE ballot_measures (
    id              SERIAL PRIMARY KEY,
    state_id        INTEGER NOT NULL REFERENCES states(id) ON DELETE CASCADE,
    election_date   DATE,
    election_year   INTEGER NOT NULL,
    measure_type    TEXT CHECK (measure_type IN (
                        'Initiated Constitutional Amendment',
                        'Initiated State Statute',
                        'Legislative Constitutional Amendment',
                        'Legislative Referendum',
                        'Veto Referendum',
                        'Commission Referred',
                        'Advisory Question',
                        'Bond Measure',
                        'Tax Measure',
                        'Recall'
                    )),
    measure_number      TEXT,            -- e.g., 'Issue 1', 'Prop 36'
    short_title         TEXT NOT NULL,
    description         TEXT,
    subject_category    TEXT,
    sponsor_type        TEXT CHECK (sponsor_type IN (
                            'Citizen', 'Legislature', 'Commission',
                            'Governor', 'Other'
                        )),
    placed_by           TEXT,
    signature_threshold INTEGER,
    signatures_submitted INTEGER,
    qualified_date      DATE,
    status              TEXT CHECK (status IN (
                            'Proposed', 'Signature Gathering',
                            'Signatures Submitted', 'Qualified',
                            'On Ballot', 'Passed', 'Failed',
                            'Withdrawn', 'Disqualified',
                            'Passed (but superseded)', 'Court Challenge'
                        )),
    votes_yes           INTEGER,
    votes_no            INTEGER,
    yes_percentage      NUMERIC(5,2),
    result              TEXT CHECK (result IN (
                            'Passed', 'Failed', 'Pending',
                            'Withdrawn', 'Court Overturned'
                        )),
    passage_threshold   TEXT,            -- e.g., 'Simple majority', '60%'
    fiscal_impact_estimate TEXT,
    key_supporters      TEXT,
    key_opponents       TEXT,
    forecast_rating     TEXT,
    notes               TEXT
);

CREATE INDEX idx_ballot_measures_state_id ON ballot_measures(state_id);
CREATE INDEX idx_ballot_measures_election_year ON ballot_measures(election_year);
CREATE INDEX idx_ballot_measures_status ON ballot_measures(status);

-- ============================================================
-- 8. SEAT TERMS (officeholder history)
-- ============================================================
CREATE TABLE seat_terms (
    id              SERIAL PRIMARY KEY,
    seat_id         INTEGER NOT NULL REFERENCES seats(id) ON DELETE CASCADE,
    candidate_id    INTEGER NOT NULL REFERENCES candidates(id) ON DELETE CASCADE,
    party           TEXT,
    start_date      DATE,
    end_date        DATE,               -- NULL = current holder
    start_reason    TEXT CHECK (start_reason IN (
                        'elected', 'appointed', 'party_switch',
                        'redistricted', 'succeeded'
                    )),
    end_reason      TEXT CHECK (end_reason IN (
                        'term_expired', 'resigned', 'lost_election',
                        'died', 'removed', 'appointed_elsewhere',
                        'redistricted', 'party_switch'
                    )),
    caucus          TEXT,               -- effective partisan alignment (may differ from party)
    election_id     INTEGER REFERENCES elections(id),  -- NULL for appointments
    notes           TEXT
);

CREATE INDEX idx_seat_terms_seat_id ON seat_terms(seat_id);
CREATE INDEX idx_seat_terms_candidate_id ON seat_terms(candidate_id);
CREATE INDEX idx_seat_terms_party ON seat_terms(party);
CREATE INDEX idx_seat_terms_current ON seat_terms(seat_id) WHERE end_date IS NULL;
CREATE INDEX idx_seat_terms_dates ON seat_terms(start_date, end_date);

-- ============================================================
-- 8b. PARTY SWITCHES (event log for officials who changed party)
-- ============================================================
CREATE TABLE party_switches (
    id              SERIAL PRIMARY KEY,
    candidate_id    INTEGER NOT NULL REFERENCES candidates(id) ON DELETE CASCADE,
    seat_id         INTEGER REFERENCES seats(id) ON DELETE SET NULL,
    state_id        INTEGER NOT NULL REFERENCES states(id) ON DELETE CASCADE,
    chamber         TEXT,
    old_party       TEXT NOT NULL,
    new_party       TEXT NOT NULL,
    old_caucus      TEXT,
    new_caucus      TEXT,
    switch_date     DATE,               -- exact date if known (nullable for older records)
    switch_year     INTEGER NOT NULL,    -- always populated
    source_url      TEXT,
    bp_profile_url  TEXT,
    is_current      BOOLEAN DEFAULT FALSE,
    notes           TEXT,
    UNIQUE(candidate_id, switch_year, old_party, new_party)
);

CREATE INDEX idx_party_switches_candidate ON party_switches(candidate_id);
CREATE INDEX idx_party_switches_state ON party_switches(state_id);
CREATE INDEX idx_party_switches_year ON party_switches(switch_year);

-- ============================================================
-- 9. FORECASTS (time-series ratings)
-- ============================================================
CREATE TABLE forecasts (
    id              SERIAL PRIMARY KEY,
    election_id     INTEGER REFERENCES elections(id) ON DELETE CASCADE,
    measure_id      INTEGER REFERENCES ballot_measures(id) ON DELETE CASCADE,
    source          TEXT NOT NULL,
    rating          TEXT NOT NULL,
    date_of_forecast DATE NOT NULL,
    previous_rating TEXT,
    notes           TEXT,
    -- Must reference exactly one of election or measure
    CONSTRAINT fk_forecast_target CHECK (
        (election_id IS NOT NULL AND measure_id IS NULL) OR
        (election_id IS NULL AND measure_id IS NOT NULL)
    )
);

CREATE INDEX idx_forecasts_election_id ON forecasts(election_id);
CREATE INDEX idx_forecasts_measure_id ON forecasts(measure_id);
CREATE INDEX idx_forecasts_date ON forecasts(date_of_forecast);

-- ============================================================
-- 10. CHAMBER CONTROL (governance status per legislative chamber)
-- ============================================================
CREATE TABLE chamber_control (
    id              SERIAL PRIMARY KEY,
    state_id        INTEGER NOT NULL REFERENCES states(id) ON DELETE CASCADE,
    chamber         TEXT NOT NULL,  -- matches districts.chamber values
    effective_date  DATE NOT NULL,  -- when this control arrangement started
    control_status  TEXT NOT NULL CHECK (control_status IN (
                        'D', 'R', 'Coalition', 'Tied',
                        'Power_Sharing', 'Nonpartisan'
                    )),
    d_seats         INTEGER,
    r_seats         INTEGER,
    other_seats     INTEGER DEFAULT 0,
    vacant_seats    INTEGER DEFAULT 0,
    total_seats     INTEGER NOT NULL,
    majority_threshold INTEGER,     -- seats needed for majority (floor(total/2) + 1)
    presiding_officer       TEXT,   -- name of Speaker/President/etc.
    presiding_officer_title TEXT,   -- 'Speaker', 'President', 'President Pro Tem'
    presiding_officer_party TEXT,   -- party of presiding officer
    coalition_desc  TEXT,           -- e.g., '14D + 5I + 2R bipartisan coalition'
    forecast_rating TEXT,           -- pre-election forecast (e.g., 'Solid R', 'Toss-Up')
    notes           TEXT,
    UNIQUE(state_id, chamber, effective_date)
);

CREATE INDEX idx_chamber_control_state ON chamber_control(state_id);
CREATE INDEX idx_chamber_control_current ON chamber_control(effective_date);

-- ============================================================
-- 11. SUPERMAJORITY THRESHOLDS (per-chamber constitutional thresholds)
-- ============================================================
CREATE TABLE supermajority_thresholds (
    id              SERIAL PRIMARY KEY,
    state_id        INTEGER NOT NULL REFERENCES states(id),
    chamber         TEXT NOT NULL,
    veto_override   TEXT,       -- e.g., '2/3 Elected (66.67%)', 'Majority Elected (50%)'
    budget_passage  TEXT,
    taxes           TEXT,       -- threshold to impose or increase taxes
    const_amend     TEXT,       -- threshold to pass constitutional amendments
    quorum          TEXT,
    other_circumstances TEXT,
    const_authority TEXT,       -- constitutional citation
    notes           TEXT,
    UNIQUE(state_id, chamber)
);

CREATE INDEX idx_supermajority_state ON supermajority_thresholds(state_id);

-- ============================================================
-- 12. TRIFECTAS (historical trifecta/split government tracking)
-- ============================================================
CREATE TABLE trifectas (
    id              SERIAL PRIMARY KEY,
    state_id        INTEGER NOT NULL REFERENCES states(id),
    year            INTEGER NOT NULL,
    governor_party  TEXT CHECK (governor_party IN ('Republican', 'Democrat', 'Independent')),
    legislature_status TEXT CHECK (legislature_status IN ('Republican', 'Democrat', 'Split')),
    trifecta_status TEXT NOT NULL CHECK (trifecta_status IN ('Republican', 'Democrat', 'Split')),
    notes           TEXT,
    UNIQUE(state_id, year)
);

CREATE INDEX idx_trifectas_state_year ON trifectas(state_id, year);
CREATE INDEX idx_trifectas_year ON trifectas(year);

-- ============================================================
-- 12. DASHBOARD VIEW (auto-generated, read-only)
-- ============================================================
CREATE OR REPLACE VIEW dashboard_view AS

-- Candidacy records
SELECT
    'Candidacy'                     AS record_type,
    cy.id                           AS record_id,
    e.election_year,
    e.election_date,
    e.election_type,
    st.abbreviation                 AS state,
    s.office_level,
    CASE
        WHEN s.office_level = 'Statewide' THEN s.office_type
        ELSE d.chamber
    END                             AS chamber_or_office,
    COALESCE(d.district_number, 'At-Large') AS district,
    s.seat_label,
    c.full_name                     AS candidate_or_measure_name,
    cy.party                        AS party_or_sponsor,
    cy.candidate_status             AS status,
    cy.is_incumbent,
    cy.votes_received               AS votes_for,
    e.total_votes_cast              AS total_votes,
    cy.vote_percentage              AS pct,
    cy.result,
    e.result_status,
    e.forecast_rating,
    e.pres_margin_this_cycle        AS pres_margin,
    cy.notes
FROM candidacies cy
JOIN elections e   ON cy.election_id = e.id
JOIN seats s      ON e.seat_id = s.id
JOIN districts d  ON s.district_id = d.id
JOIN states st    ON d.state_id = st.id
JOIN candidates c ON cy.candidate_id = c.id

UNION ALL

-- Ballot measure records
SELECT
    'Ballot Measure'                AS record_type,
    bm.id                           AS record_id,
    bm.election_year,
    bm.election_date,
    'General'                       AS election_type,
    st.abbreviation                 AS state,
    'Statewide'                     AS office_level,
    bm.measure_type                 AS chamber_or_office,
    'Statewide'                     AS district,
    bm.measure_number               AS seat_label,
    bm.short_title                  AS candidate_or_measure_name,
    bm.sponsor_type                 AS party_or_sponsor,
    bm.status,
    NULL::BOOLEAN                   AS is_incumbent,
    bm.votes_yes                    AS votes_for,
    (COALESCE(bm.votes_yes, 0) + COALESCE(bm.votes_no, 0))  AS total_votes,
    bm.yes_percentage               AS pct,
    bm.result,
    NULL                            AS result_status,
    bm.forecast_rating,
    NULL                            AS pres_margin,
    bm.notes
FROM ballot_measures bm
JOIN states st ON bm.state_id = st.id;

-- ============================================================
-- 11. HELPER: Enable Row Level Security (Supabase default)
-- ============================================================
-- RLS policies should be configured in Supabase dashboard
-- based on your auth requirements. For now, enable RLS
-- on all tables (Supabase best practice):
ALTER TABLE states ENABLE ROW LEVEL SECURITY;
ALTER TABLE districts ENABLE ROW LEVEL SECURITY;
ALTER TABLE seats ENABLE ROW LEVEL SECURITY;
ALTER TABLE elections ENABLE ROW LEVEL SECURITY;
ALTER TABLE candidates ENABLE ROW LEVEL SECURITY;
ALTER TABLE candidacies ENABLE ROW LEVEL SECURITY;
ALTER TABLE ballot_measures ENABLE ROW LEVEL SECURITY;
ALTER TABLE forecasts ENABLE ROW LEVEL SECURITY;
ALTER TABLE seat_terms ENABLE ROW LEVEL SECURITY;
ALTER TABLE chamber_control ENABLE ROW LEVEL SECURITY;
ALTER TABLE party_switches ENABLE ROW LEVEL SECURITY;

-- Create permissive policies for authenticated access
-- (adjust these based on your actual auth needs)
CREATE POLICY "Allow full access" ON states FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow full access" ON districts FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow full access" ON seats FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow full access" ON elections FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow full access" ON candidates FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow full access" ON candidacies FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow full access" ON ballot_measures FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow full access" ON forecasts FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow full access" ON seat_terms FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow full access" ON chamber_control FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow full access" ON party_switches FOR ALL USING (true) WITH CHECK (true);

-- ============================================================
-- TRIGGERS
-- ============================================================

-- When a seat_term is inserted or updated with end_date IS NULL,
-- sync the seat's current_holder_party/caucus and clear is_open_seat
-- on future General elections for that seat.
CREATE OR REPLACE FUNCTION sync_seat_on_term_change()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.end_date IS NULL THEN
    UPDATE seats
    SET current_holder_party = NEW.party,
        current_holder_caucus = NEW.caucus
    WHERE id = NEW.seat_id;

    UPDATE elections
    SET is_open_seat = false
    WHERE seat_id = NEW.seat_id
      AND election_type = 'General'
      AND election_date > CURRENT_DATE
      AND is_open_seat = true;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sync_seat_on_term_change
  AFTER INSERT OR UPDATE ON seat_terms
  FOR EACH ROW
  EXECUTE FUNCTION sync_seat_on_term_change();
