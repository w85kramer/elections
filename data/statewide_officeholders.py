"""
Current Elected Statewide Officeholders — All 280 seats
========================================================
As of February 8, 2026.

Data sourced from Claude's training data, verified via web searches (Feb 2026)
against Ballotpedia, NGA, NAAG, NASS, and official state websites.

Key changes since January 2025:
- 2024 election winners (took office Jan 2025): DE, IN, MO, MT, NC, ND, NH, UT, VT, WA, WV governors + various down-ballot
- 2025 election winners (took office Jan 2026): NJ, VA governors + VA down-ballot
- Mid-term successions: SD Gov (Rhoden succeeded Noem), FL AG (Uthmeier replaced Moody),
  MO AG (Hanaway replaced Bailey), GA Labor (Holmes replaced Thompson), TX Comptroller (Hancock replaced Hegar),
  OK Supt (Fields replaced Walters), ND Supt (Bachmeier replaced Baesler)
- AZ Lt. Gov seat is VACANT (new office, first election 2026)
- OR Superintendent no longer elected (abolished 2012) — seat should be reclassified

Notes on party codes:
- 'R' = Republican, 'D' = Democrat, 'NP' = Nonpartisan (for offices that are officially nonpartisan)
- caucus field used when party affiliation differs from party label

Each entry: {state, office_type, name, first_name, last_name, party, caucus, start_date, start_reason}
office_type must match the CHECK constraint in the seats table.
"""

OFFICEHOLDERS = [
    # ══════════════════════════════════════════════════════════════════
    # GOVERNORS (50)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'AL', 'office_type': 'Governor', 'name': 'Kay Ivey', 'first_name': 'Kay', 'last_name': 'Ivey', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-16', 'start_reason': 'elected'},
    {'state': 'AK', 'office_type': 'Governor', 'name': 'Mike Dunleavy', 'first_name': 'Mike', 'last_name': 'Dunleavy', 'party': 'R', 'caucus': 'R', 'start_date': '2022-12-05', 'start_reason': 'elected'},
    {'state': 'AZ', 'office_type': 'Governor', 'name': 'Katie Hobbs', 'first_name': 'Katie', 'last_name': 'Hobbs', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'AR', 'office_type': 'Governor', 'name': 'Sarah Huckabee Sanders', 'first_name': 'Sarah', 'last_name': 'Sanders', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-10', 'start_reason': 'elected'},
    {'state': 'CA', 'office_type': 'Governor', 'name': 'Gavin Newsom', 'first_name': 'Gavin', 'last_name': 'Newsom', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-06', 'start_reason': 'elected'},
    {'state': 'CO', 'office_type': 'Governor', 'name': 'Jared Polis', 'first_name': 'Jared', 'last_name': 'Polis', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-10', 'start_reason': 'elected'},
    {'state': 'CT', 'office_type': 'Governor', 'name': 'Ned Lamont', 'first_name': 'Ned', 'last_name': 'Lamont', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-04', 'start_reason': 'elected'},
    {'state': 'DE', 'office_type': 'Governor', 'name': 'Matt Meyer', 'first_name': 'Matt', 'last_name': 'Meyer', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-21', 'start_reason': 'elected'},
    {'state': 'FL', 'office_type': 'Governor', 'name': 'Ron DeSantis', 'first_name': 'Ron', 'last_name': 'DeSantis', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-03', 'start_reason': 'elected'},
    {'state': 'GA', 'office_type': 'Governor', 'name': 'Brian Kemp', 'first_name': 'Brian', 'last_name': 'Kemp', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'HI', 'office_type': 'Governor', 'name': 'Josh Green', 'first_name': 'Josh', 'last_name': 'Green', 'party': 'D', 'caucus': 'D', 'start_date': '2022-12-05', 'start_reason': 'elected'},
    {'state': 'ID', 'office_type': 'Governor', 'name': 'Brad Little', 'first_name': 'Brad', 'last_name': 'Little', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'IL', 'office_type': 'Governor', 'name': 'JB Pritzker', 'first_name': 'JB', 'last_name': 'Pritzker', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'IN', 'office_type': 'Governor', 'name': 'Mike Braun', 'first_name': 'Mike', 'last_name': 'Braun', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-13', 'start_reason': 'elected'},
    {'state': 'IA', 'office_type': 'Governor', 'name': 'Kim Reynolds', 'first_name': 'Kim', 'last_name': 'Reynolds', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-13', 'start_reason': 'elected'},
    {'state': 'KS', 'office_type': 'Governor', 'name': 'Laura Kelly', 'first_name': 'Laura', 'last_name': 'Kelly', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'KY', 'office_type': 'Governor', 'name': 'Andy Beshear', 'first_name': 'Andy', 'last_name': 'Beshear', 'party': 'D', 'caucus': 'D', 'start_date': '2023-12-12', 'start_reason': 'elected'},
    {'state': 'LA', 'office_type': 'Governor', 'name': 'Jeff Landry', 'first_name': 'Jeff', 'last_name': 'Landry', 'party': 'R', 'caucus': 'R', 'start_date': '2024-01-08', 'start_reason': 'elected'},
    {'state': 'ME', 'office_type': 'Governor', 'name': 'Janet Mills', 'first_name': 'Janet', 'last_name': 'Mills', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-04', 'start_reason': 'elected'},
    {'state': 'MD', 'office_type': 'Governor', 'name': 'Wes Moore', 'first_name': 'Wes', 'last_name': 'Moore', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-18', 'start_reason': 'elected'},
    {'state': 'MA', 'office_type': 'Governor', 'name': 'Maura Healey', 'first_name': 'Maura', 'last_name': 'Healey', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-05', 'start_reason': 'elected'},
    {'state': 'MI', 'office_type': 'Governor', 'name': 'Gretchen Whitmer', 'first_name': 'Gretchen', 'last_name': 'Whitmer', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-01', 'start_reason': 'elected'},
    {'state': 'MN', 'office_type': 'Governor', 'name': 'Tim Walz', 'first_name': 'Tim', 'last_name': 'Walz', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'MS', 'office_type': 'Governor', 'name': 'Tate Reeves', 'first_name': 'Tate', 'last_name': 'Reeves', 'party': 'R', 'caucus': 'R', 'start_date': '2024-01-09', 'start_reason': 'elected'},
    {'state': 'MO', 'office_type': 'Governor', 'name': 'Mike Kehoe', 'first_name': 'Mike', 'last_name': 'Kehoe', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-13', 'start_reason': 'elected'},
    {'state': 'MT', 'office_type': 'Governor', 'name': 'Greg Gianforte', 'first_name': 'Greg', 'last_name': 'Gianforte', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-06', 'start_reason': 'elected'},
    {'state': 'NE', 'office_type': 'Governor', 'name': 'Jim Pillen', 'first_name': 'Jim', 'last_name': 'Pillen', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-05', 'start_reason': 'elected'},
    {'state': 'NV', 'office_type': 'Governor', 'name': 'Joe Lombardo', 'first_name': 'Joe', 'last_name': 'Lombardo', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'NH', 'office_type': 'Governor', 'name': 'Kelly Ayotte', 'first_name': 'Kelly', 'last_name': 'Ayotte', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-09', 'start_reason': 'elected'},
    {'state': 'NJ', 'office_type': 'Governor', 'name': 'Mikie Sherrill', 'first_name': 'Mikie', 'last_name': 'Sherrill', 'party': 'D', 'caucus': 'D', 'start_date': '2026-01-20', 'start_reason': 'elected'},
    {'state': 'NM', 'office_type': 'Governor', 'name': 'Michelle Lujan Grisham', 'first_name': 'Michelle', 'last_name': 'Lujan Grisham', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-01', 'start_reason': 'elected'},
    {'state': 'NY', 'office_type': 'Governor', 'name': 'Kathy Hochul', 'first_name': 'Kathy', 'last_name': 'Hochul', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-01', 'start_reason': 'elected'},
    {'state': 'NC', 'office_type': 'Governor', 'name': 'Josh Stein', 'first_name': 'Josh', 'last_name': 'Stein', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-01', 'start_reason': 'elected'},
    {'state': 'ND', 'office_type': 'Governor', 'name': 'Kelly Armstrong', 'first_name': 'Kelly', 'last_name': 'Armstrong', 'party': 'R', 'caucus': 'R', 'start_date': '2024-12-15', 'start_reason': 'elected'},
    {'state': 'OH', 'office_type': 'Governor', 'name': 'Mike DeWine', 'first_name': 'Mike', 'last_name': 'DeWine', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'OK', 'office_type': 'Governor', 'name': 'Kevin Stitt', 'first_name': 'Kevin', 'last_name': 'Stitt', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'OR', 'office_type': 'Governor', 'name': 'Tina Kotek', 'first_name': 'Tina', 'last_name': 'Kotek', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'PA', 'office_type': 'Governor', 'name': 'Josh Shapiro', 'first_name': 'Josh', 'last_name': 'Shapiro', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-17', 'start_reason': 'elected'},
    {'state': 'RI', 'office_type': 'Governor', 'name': 'Dan McKee', 'first_name': 'Dan', 'last_name': 'McKee', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-03', 'start_reason': 'elected'},
    {'state': 'SC', 'office_type': 'Governor', 'name': 'Henry McMaster', 'first_name': 'Henry', 'last_name': 'McMaster', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-11', 'start_reason': 'elected'},
    {'state': 'SD', 'office_type': 'Governor', 'name': 'Larry Rhoden', 'first_name': 'Larry', 'last_name': 'Rhoden', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-25', 'start_reason': 'succeeded'},
    {'state': 'TN', 'office_type': 'Governor', 'name': 'Bill Lee', 'first_name': 'Bill', 'last_name': 'Lee', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-21', 'start_reason': 'elected'},
    {'state': 'TX', 'office_type': 'Governor', 'name': 'Greg Abbott', 'first_name': 'Greg', 'last_name': 'Abbott', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-17', 'start_reason': 'elected'},
    {'state': 'UT', 'office_type': 'Governor', 'name': 'Spencer Cox', 'first_name': 'Spencer', 'last_name': 'Cox', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-06', 'start_reason': 'elected'},
    {'state': 'VT', 'office_type': 'Governor', 'name': 'Phil Scott', 'first_name': 'Phil', 'last_name': 'Scott', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-09', 'start_reason': 'elected'},
    {'state': 'VA', 'office_type': 'Governor', 'name': 'Abigail Spanberger', 'first_name': 'Abigail', 'last_name': 'Spanberger', 'party': 'D', 'caucus': 'D', 'start_date': '2026-01-17', 'start_reason': 'elected'},
    {'state': 'WA', 'office_type': 'Governor', 'name': 'Bob Ferguson', 'first_name': 'Bob', 'last_name': 'Ferguson', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-15', 'start_reason': 'elected'},
    {'state': 'WV', 'office_type': 'Governor', 'name': 'Patrick Morrisey', 'first_name': 'Patrick', 'last_name': 'Morrisey', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-13', 'start_reason': 'elected'},
    {'state': 'WI', 'office_type': 'Governor', 'name': 'Tony Evers', 'first_name': 'Tony', 'last_name': 'Evers', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'WY', 'office_type': 'Governor', 'name': 'Mark Gordon', 'first_name': 'Mark', 'last_name': 'Gordon', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},

    # ══════════════════════════════════════════════════════════════════
    # LT. GOVERNORS (44 elected; AZ vacant — first election 2026)
    # Excluding: ME, NH, OR, WY (no office); TN, WV (ex officio)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'AL', 'office_type': 'Lt. Governor', 'name': 'Will Ainsworth', 'first_name': 'Will', 'last_name': 'Ainsworth', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-16', 'start_reason': 'elected'},
    {'state': 'AK', 'office_type': 'Lt. Governor', 'name': 'Nancy Dahlstrom', 'first_name': 'Nancy', 'last_name': 'Dahlstrom', 'party': 'R', 'caucus': 'R', 'start_date': '2022-12-05', 'start_reason': 'elected'},
    # AZ Lt. Gov seat is VACANT — new office created by Prop 131 (2022), first election Nov 2026
    {'state': 'AR', 'office_type': 'Lt. Governor', 'name': 'Leslie Rutledge', 'first_name': 'Leslie', 'last_name': 'Rutledge', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-10', 'start_reason': 'elected'},
    {'state': 'CA', 'office_type': 'Lt. Governor', 'name': 'Eleni Kounalakis', 'first_name': 'Eleni', 'last_name': 'Kounalakis', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-07', 'start_reason': 'elected'},
    {'state': 'CO', 'office_type': 'Lt. Governor', 'name': 'Dianne Primavera', 'first_name': 'Dianne', 'last_name': 'Primavera', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-08', 'start_reason': 'elected'},
    {'state': 'CT', 'office_type': 'Lt. Governor', 'name': 'Susan Bysiewicz', 'first_name': 'Susan', 'last_name': 'Bysiewicz', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-09', 'start_reason': 'elected'},
    {'state': 'DE', 'office_type': 'Lt. Governor', 'name': 'Kyle Evans Gay', 'first_name': 'Kyle', 'last_name': 'Evans Gay', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-21', 'start_reason': 'elected'},
    {'state': 'FL', 'office_type': 'Lt. Governor', 'name': 'Jay Collins', 'first_name': 'Jay', 'last_name': 'Collins', 'party': 'R', 'caucus': 'R', 'start_date': '2025-08-12', 'start_reason': 'appointed'},
    {'state': 'GA', 'office_type': 'Lt. Governor', 'name': 'Burt Jones', 'first_name': 'Burt', 'last_name': 'Jones', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'HI', 'office_type': 'Lt. Governor', 'name': 'Sylvia Luke', 'first_name': 'Sylvia', 'last_name': 'Luke', 'party': 'D', 'caucus': 'D', 'start_date': '2022-12-05', 'start_reason': 'elected'},
    {'state': 'ID', 'office_type': 'Lt. Governor', 'name': 'Scott Bedke', 'first_name': 'Scott', 'last_name': 'Bedke', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'IL', 'office_type': 'Lt. Governor', 'name': 'Juliana Stratton', 'first_name': 'Juliana', 'last_name': 'Stratton', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-14', 'start_reason': 'elected'},
    {'state': 'IN', 'office_type': 'Lt. Governor', 'name': 'Micah Beckwith', 'first_name': 'Micah', 'last_name': 'Beckwith', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-13', 'start_reason': 'elected'},
    {'state': 'IA', 'office_type': 'Lt. Governor', 'name': 'Chris Cournoyer', 'first_name': 'Chris', 'last_name': 'Cournoyer', 'party': 'R', 'caucus': 'R', 'start_date': '2024-12-16', 'start_reason': 'appointed'},
    {'state': 'KS', 'office_type': 'Lt. Governor', 'name': 'David Toland', 'first_name': 'David', 'last_name': 'Toland', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'KY', 'office_type': 'Lt. Governor', 'name': 'Jacqueline Coleman', 'first_name': 'Jacqueline', 'last_name': 'Coleman', 'party': 'D', 'caucus': 'D', 'start_date': '2023-12-12', 'start_reason': 'elected'},
    {'state': 'LA', 'office_type': 'Lt. Governor', 'name': 'Billy Nungesser', 'first_name': 'Billy', 'last_name': 'Nungesser', 'party': 'R', 'caucus': 'R', 'start_date': '2016-01-11', 'start_reason': 'elected'},
    {'state': 'MD', 'office_type': 'Lt. Governor', 'name': 'Aruna Miller', 'first_name': 'Aruna', 'last_name': 'Miller', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-18', 'start_reason': 'elected'},
    {'state': 'MA', 'office_type': 'Lt. Governor', 'name': 'Kim Driscoll', 'first_name': 'Kim', 'last_name': 'Driscoll', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-05', 'start_reason': 'elected'},
    {'state': 'MI', 'office_type': 'Lt. Governor', 'name': 'Garlin Gilchrist', 'first_name': 'Garlin', 'last_name': 'Gilchrist', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-01', 'start_reason': 'elected'},
    {'state': 'MN', 'office_type': 'Lt. Governor', 'name': 'Peggy Flanagan', 'first_name': 'Peggy', 'last_name': 'Flanagan', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-07', 'start_reason': 'elected'},
    {'state': 'MS', 'office_type': 'Lt. Governor', 'name': 'Delbert Hosemann', 'first_name': 'Delbert', 'last_name': 'Hosemann', 'party': 'R', 'caucus': 'R', 'start_date': '2020-01-09', 'start_reason': 'elected'},
    {'state': 'MO', 'office_type': 'Lt. Governor', 'name': 'David Wasinger', 'first_name': 'David', 'last_name': 'Wasinger', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-13', 'start_reason': 'elected'},
    {'state': 'MT', 'office_type': 'Lt. Governor', 'name': 'Kristen Juras', 'first_name': 'Kristen', 'last_name': 'Juras', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-06', 'start_reason': 'elected'},
    {'state': 'NE', 'office_type': 'Lt. Governor', 'name': 'Joe Kelly', 'first_name': 'Joe', 'last_name': 'Kelly', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-05', 'start_reason': 'elected'},
    {'state': 'NV', 'office_type': 'Lt. Governor', 'name': 'Stavros Anthony', 'first_name': 'Stavros', 'last_name': 'Anthony', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'NJ', 'office_type': 'Lt. Governor', 'name': 'Dale Caldwell', 'first_name': 'Dale', 'last_name': 'Caldwell', 'party': 'D', 'caucus': 'D', 'start_date': '2026-01-20', 'start_reason': 'elected'},
    {'state': 'NM', 'office_type': 'Lt. Governor', 'name': 'Howie Morales', 'first_name': 'Howie', 'last_name': 'Morales', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-01', 'start_reason': 'elected'},
    {'state': 'NY', 'office_type': 'Lt. Governor', 'name': 'Antonio Delgado', 'first_name': 'Antonio', 'last_name': 'Delgado', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-01', 'start_reason': 'elected'},
    {'state': 'NC', 'office_type': 'Lt. Governor', 'name': 'Rachel Hunt', 'first_name': 'Rachel', 'last_name': 'Hunt', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-01', 'start_reason': 'elected'},
    {'state': 'ND', 'office_type': 'Lt. Governor', 'name': 'Michelle Strinden', 'first_name': 'Michelle', 'last_name': 'Strinden', 'party': 'R', 'caucus': 'R', 'start_date': '2024-12-15', 'start_reason': 'elected'},
    {'state': 'OH', 'office_type': 'Lt. Governor', 'name': 'Jim Tressel', 'first_name': 'Jim', 'last_name': 'Tressel', 'party': 'R', 'caucus': 'R', 'start_date': '2025-02-14', 'start_reason': 'appointed'},
    {'state': 'OK', 'office_type': 'Lt. Governor', 'name': 'Matt Pinnell', 'first_name': 'Matt', 'last_name': 'Pinnell', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-14', 'start_reason': 'elected'},
    {'state': 'PA', 'office_type': 'Lt. Governor', 'name': 'Austin Davis', 'first_name': 'Austin', 'last_name': 'Davis', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-17', 'start_reason': 'elected'},
    {'state': 'RI', 'office_type': 'Lt. Governor', 'name': 'Sabina Matos', 'first_name': 'Sabina', 'last_name': 'Matos', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-03', 'start_reason': 'elected'},
    {'state': 'SC', 'office_type': 'Lt. Governor', 'name': 'Pamela Evette', 'first_name': 'Pamela', 'last_name': 'Evette', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-09', 'start_reason': 'elected'},
    {'state': 'SD', 'office_type': 'Lt. Governor', 'name': 'Tony Venhuizen', 'first_name': 'Tony', 'last_name': 'Venhuizen', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-30', 'start_reason': 'appointed'},
    {'state': 'TX', 'office_type': 'Lt. Governor', 'name': 'Dan Patrick', 'first_name': 'Dan', 'last_name': 'Patrick', 'party': 'R', 'caucus': 'R', 'start_date': '2015-01-20', 'start_reason': 'elected'},
    {'state': 'UT', 'office_type': 'Lt. Governor', 'name': 'Deidre Henderson', 'first_name': 'Deidre', 'last_name': 'Henderson', 'party': 'R', 'caucus': 'R', 'start_date': '2021-01-04', 'start_reason': 'elected'},
    {'state': 'VT', 'office_type': 'Lt. Governor', 'name': 'John Rodgers', 'first_name': 'John', 'last_name': 'Rodgers', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-09', 'start_reason': 'elected'},
    {'state': 'VA', 'office_type': 'Lt. Governor', 'name': 'Ghazala Hashmi', 'first_name': 'Ghazala', 'last_name': 'Hashmi', 'party': 'D', 'caucus': 'D', 'start_date': '2026-01-17', 'start_reason': 'elected'},
    {'state': 'WA', 'office_type': 'Lt. Governor', 'name': 'Denny Heck', 'first_name': 'Denny', 'last_name': 'Heck', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-15', 'start_reason': 'elected'},
    {'state': 'WI', 'office_type': 'Lt. Governor', 'name': 'Sara Rodriguez', 'first_name': 'Sara', 'last_name': 'Rodriguez', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-02', 'start_reason': 'elected'},

    # ══════════════════════════════════════════════════════════════════
    # ATTORNEYS GENERAL (43 elected)
    # Excluding: AK, HI, ME, NH, NJ, TN, WY (appointed)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'AL', 'office_type': 'Attorney General', 'name': 'Steve Marshall', 'first_name': 'Steve', 'last_name': 'Marshall', 'party': 'R', 'caucus': 'R', 'start_date': '2017-02-10', 'start_reason': 'appointed'},
    {'state': 'AZ', 'office_type': 'Attorney General', 'name': 'Kris Mayes', 'first_name': 'Kris', 'last_name': 'Mayes', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'AR', 'office_type': 'Attorney General', 'name': 'Tim Griffin', 'first_name': 'Tim', 'last_name': 'Griffin', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-10', 'start_reason': 'elected'},
    {'state': 'CA', 'office_type': 'Attorney General', 'name': 'Rob Bonta', 'first_name': 'Rob', 'last_name': 'Bonta', 'party': 'D', 'caucus': 'D', 'start_date': '2021-04-23', 'start_reason': 'appointed'},
    {'state': 'CO', 'office_type': 'Attorney General', 'name': 'Phil Weiser', 'first_name': 'Phil', 'last_name': 'Weiser', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-08', 'start_reason': 'elected'},
    {'state': 'CT', 'office_type': 'Attorney General', 'name': 'William Tong', 'first_name': 'William', 'last_name': 'Tong', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-09', 'start_reason': 'elected'},
    {'state': 'DE', 'office_type': 'Attorney General', 'name': 'Kathy Jennings', 'first_name': 'Kathy', 'last_name': 'Jennings', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-01', 'start_reason': 'elected'},
    {'state': 'FL', 'office_type': 'Attorney General', 'name': 'James Uthmeier', 'first_name': 'James', 'last_name': 'Uthmeier', 'party': 'R', 'caucus': 'R', 'start_date': '2025-02-18', 'start_reason': 'appointed'},
    {'state': 'GA', 'office_type': 'Attorney General', 'name': 'Chris Carr', 'first_name': 'Chris', 'last_name': 'Carr', 'party': 'R', 'caucus': 'R', 'start_date': '2016-11-01', 'start_reason': 'appointed'},
    {'state': 'ID', 'office_type': 'Attorney General', 'name': 'Raul Labrador', 'first_name': 'Raul', 'last_name': 'Labrador', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'IL', 'office_type': 'Attorney General', 'name': 'Kwame Raoul', 'first_name': 'Kwame', 'last_name': 'Raoul', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-15', 'start_reason': 'elected'},
    {'state': 'IN', 'office_type': 'Attorney General', 'name': 'Todd Rokita', 'first_name': 'Todd', 'last_name': 'Rokita', 'party': 'R', 'caucus': 'R', 'start_date': '2021-01-11', 'start_reason': 'elected'},
    {'state': 'IA', 'office_type': 'Attorney General', 'name': 'Brenna Bird', 'first_name': 'Brenna', 'last_name': 'Bird', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'KS', 'office_type': 'Attorney General', 'name': 'Kris Kobach', 'first_name': 'Kris', 'last_name': 'Kobach', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'KY', 'office_type': 'Attorney General', 'name': 'Russell Coleman', 'first_name': 'Russell', 'last_name': 'Coleman', 'party': 'R', 'caucus': 'R', 'start_date': '2024-01-01', 'start_reason': 'elected'},
    {'state': 'LA', 'office_type': 'Attorney General', 'name': 'Liz Murrill', 'first_name': 'Liz', 'last_name': 'Murrill', 'party': 'R', 'caucus': 'R', 'start_date': '2024-01-08', 'start_reason': 'elected'},
    {'state': 'MD', 'office_type': 'Attorney General', 'name': 'Anthony Brown', 'first_name': 'Anthony', 'last_name': 'Brown', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-03', 'start_reason': 'elected'},
    {'state': 'MA', 'office_type': 'Attorney General', 'name': 'Andrea Campbell', 'first_name': 'Andrea', 'last_name': 'Campbell', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-04', 'start_reason': 'elected'},
    {'state': 'MI', 'office_type': 'Attorney General', 'name': 'Dana Nessel', 'first_name': 'Dana', 'last_name': 'Nessel', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-01', 'start_reason': 'elected'},
    {'state': 'MN', 'office_type': 'Attorney General', 'name': 'Keith Ellison', 'first_name': 'Keith', 'last_name': 'Ellison', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-07', 'start_reason': 'elected'},
    {'state': 'MS', 'office_type': 'Attorney General', 'name': 'Lynn Fitch', 'first_name': 'Lynn', 'last_name': 'Fitch', 'party': 'R', 'caucus': 'R', 'start_date': '2020-01-09', 'start_reason': 'elected'},
    {'state': 'MO', 'office_type': 'Attorney General', 'name': 'Catherine Hanaway', 'first_name': 'Catherine', 'last_name': 'Hanaway', 'party': 'R', 'caucus': 'R', 'start_date': '2025-09-08', 'start_reason': 'appointed'},
    {'state': 'MT', 'office_type': 'Attorney General', 'name': 'Austin Knudsen', 'first_name': 'Austin', 'last_name': 'Knudsen', 'party': 'R', 'caucus': 'R', 'start_date': '2021-01-04', 'start_reason': 'elected'},
    {'state': 'NE', 'office_type': 'Attorney General', 'name': 'Mike Hilgers', 'first_name': 'Mike', 'last_name': 'Hilgers', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-05', 'start_reason': 'elected'},
    {'state': 'NV', 'office_type': 'Attorney General', 'name': 'Aaron Ford', 'first_name': 'Aaron', 'last_name': 'Ford', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-07', 'start_reason': 'elected'},
    {'state': 'NM', 'office_type': 'Attorney General', 'name': 'Raul Torrez', 'first_name': 'Raul', 'last_name': 'Torrez', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-01', 'start_reason': 'elected'},
    {'state': 'NY', 'office_type': 'Attorney General', 'name': 'Letitia James', 'first_name': 'Letitia', 'last_name': 'James', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-01', 'start_reason': 'elected'},
    {'state': 'NC', 'office_type': 'Attorney General', 'name': 'Jeff Jackson', 'first_name': 'Jeff', 'last_name': 'Jackson', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-01', 'start_reason': 'elected'},
    {'state': 'ND', 'office_type': 'Attorney General', 'name': 'Drew Wrigley', 'first_name': 'Drew', 'last_name': 'Wrigley', 'party': 'R', 'caucus': 'R', 'start_date': '2022-02-09', 'start_reason': 'appointed'},
    {'state': 'OH', 'office_type': 'Attorney General', 'name': 'Dave Yost', 'first_name': 'Dave', 'last_name': 'Yost', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-14', 'start_reason': 'elected'},
    {'state': 'OK', 'office_type': 'Attorney General', 'name': 'Gentner Drummond', 'first_name': 'Gentner', 'last_name': 'Drummond', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'OR', 'office_type': 'Attorney General', 'name': 'Dan Rayfield', 'first_name': 'Dan', 'last_name': 'Rayfield', 'party': 'D', 'caucus': 'D', 'start_date': '2024-12-31', 'start_reason': 'elected'},
    {'state': 'PA', 'office_type': 'Attorney General', 'name': 'Dave Sunday', 'first_name': 'Dave', 'last_name': 'Sunday', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-21', 'start_reason': 'elected'},
    {'state': 'RI', 'office_type': 'Attorney General', 'name': 'Peter Neronha', 'first_name': 'Peter', 'last_name': 'Neronha', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-01', 'start_reason': 'elected'},
    {'state': 'SC', 'office_type': 'Attorney General', 'name': 'Alan Wilson', 'first_name': 'Alan', 'last_name': 'Wilson', 'party': 'R', 'caucus': 'R', 'start_date': '2011-01-12', 'start_reason': 'elected'},
    {'state': 'SD', 'office_type': 'Attorney General', 'name': 'Marty Jackley', 'first_name': 'Marty', 'last_name': 'Jackley', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'TX', 'office_type': 'Attorney General', 'name': 'Ken Paxton', 'first_name': 'Ken', 'last_name': 'Paxton', 'party': 'R', 'caucus': 'R', 'start_date': '2015-01-05', 'start_reason': 'elected'},
    {'state': 'UT', 'office_type': 'Attorney General', 'name': 'Derek Brown', 'first_name': 'Derek', 'last_name': 'Brown', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-07', 'start_reason': 'elected'},
    {'state': 'VT', 'office_type': 'Attorney General', 'name': 'Charity Clark', 'first_name': 'Charity', 'last_name': 'Clark', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-05', 'start_reason': 'elected'},
    {'state': 'VA', 'office_type': 'Attorney General', 'name': 'Jay Jones', 'first_name': 'Jay', 'last_name': 'Jones', 'party': 'D', 'caucus': 'D', 'start_date': '2026-01-17', 'start_reason': 'elected'},
    {'state': 'WA', 'office_type': 'Attorney General', 'name': 'Nick Brown', 'first_name': 'Nick', 'last_name': 'Brown', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-13', 'start_reason': 'elected'},
    {'state': 'WV', 'office_type': 'Attorney General', 'name': 'John B. McCuskey', 'first_name': 'John', 'last_name': 'McCuskey', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-13', 'start_reason': 'elected'},
    {'state': 'WI', 'office_type': 'Attorney General', 'name': 'Josh Kaul', 'first_name': 'Josh', 'last_name': 'Kaul', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-07', 'start_reason': 'elected'},

    # ══════════════════════════════════════════════════════════════════
    # SECRETARIES OF STATE (35 elected)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'AL', 'office_type': 'Secretary of State', 'name': 'Wes Allen', 'first_name': 'Wes', 'last_name': 'Allen', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-16', 'start_reason': 'elected'},
    {'state': 'AZ', 'office_type': 'Secretary of State', 'name': 'Adrian Fontes', 'first_name': 'Adrian', 'last_name': 'Fontes', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'AR', 'office_type': 'Secretary of State', 'name': 'Cole Jester', 'first_name': 'Cole', 'last_name': 'Jester', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-01', 'start_reason': 'appointed'},
    {'state': 'CA', 'office_type': 'Secretary of State', 'name': 'Shirley Weber', 'first_name': 'Shirley', 'last_name': 'Weber', 'party': 'D', 'caucus': 'D', 'start_date': '2021-01-29', 'start_reason': 'appointed'},
    {'state': 'CO', 'office_type': 'Secretary of State', 'name': 'Jena Griswold', 'first_name': 'Jena', 'last_name': 'Griswold', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-08', 'start_reason': 'elected'},
    {'state': 'CT', 'office_type': 'Secretary of State', 'name': 'Stephanie Thomas', 'first_name': 'Stephanie', 'last_name': 'Thomas', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-04', 'start_reason': 'elected'},
    {'state': 'GA', 'office_type': 'Secretary of State', 'name': 'Brad Raffensperger', 'first_name': 'Brad', 'last_name': 'Raffensperger', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-14', 'start_reason': 'elected'},
    {'state': 'ID', 'office_type': 'Secretary of State', 'name': 'Phil McGrane', 'first_name': 'Phil', 'last_name': 'McGrane', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'IL', 'office_type': 'Secretary of State', 'name': 'Alexi Giannoulias', 'first_name': 'Alexi', 'last_name': 'Giannoulias', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'IN', 'office_type': 'Secretary of State', 'name': 'Diego Morales', 'first_name': 'Diego', 'last_name': 'Morales', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-01', 'start_reason': 'elected'},
    {'state': 'IA', 'office_type': 'Secretary of State', 'name': 'Paul Pate', 'first_name': 'Paul', 'last_name': 'Pate', 'party': 'R', 'caucus': 'R', 'start_date': '2015-01-01', 'start_reason': 'elected'},
    {'state': 'KS', 'office_type': 'Secretary of State', 'name': 'Scott Schwab', 'first_name': 'Scott', 'last_name': 'Schwab', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-14', 'start_reason': 'elected'},
    {'state': 'KY', 'office_type': 'Secretary of State', 'name': 'Michael Adams', 'first_name': 'Michael', 'last_name': 'Adams', 'party': 'R', 'caucus': 'R', 'start_date': '2020-01-06', 'start_reason': 'elected'},
    {'state': 'LA', 'office_type': 'Secretary of State', 'name': 'Nancy Landry', 'first_name': 'Nancy', 'last_name': 'Landry', 'party': 'R', 'caucus': 'R', 'start_date': '2024-01-08', 'start_reason': 'elected'},
    {'state': 'MA', 'office_type': 'Secretary of State', 'name': 'William Galvin', 'first_name': 'William', 'last_name': 'Galvin', 'party': 'D', 'caucus': 'D', 'start_date': '1995-01-18', 'start_reason': 'elected'},
    {'state': 'MI', 'office_type': 'Secretary of State', 'name': 'Jocelyn Benson', 'first_name': 'Jocelyn', 'last_name': 'Benson', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-01', 'start_reason': 'elected'},
    {'state': 'MN', 'office_type': 'Secretary of State', 'name': 'Steve Simon', 'first_name': 'Steve', 'last_name': 'Simon', 'party': 'D', 'caucus': 'D', 'start_date': '2015-01-05', 'start_reason': 'elected'},
    {'state': 'MO', 'office_type': 'Secretary of State', 'name': 'Denny Hoskins', 'first_name': 'Denny', 'last_name': 'Hoskins', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-13', 'start_reason': 'elected'},
    {'state': 'MS', 'office_type': 'Secretary of State', 'name': 'Michael Watson', 'first_name': 'Michael', 'last_name': 'Watson', 'party': 'R', 'caucus': 'R', 'start_date': '2020-01-09', 'start_reason': 'elected'},
    {'state': 'MT', 'office_type': 'Secretary of State', 'name': 'Christi Jacobsen', 'first_name': 'Christi', 'last_name': 'Jacobsen', 'party': 'R', 'caucus': 'R', 'start_date': '2021-01-04', 'start_reason': 'elected'},
    {'state': 'NE', 'office_type': 'Secretary of State', 'name': 'Bob Evnen', 'first_name': 'Bob', 'last_name': 'Evnen', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-10', 'start_reason': 'elected'},
    {'state': 'NV', 'office_type': 'Secretary of State', 'name': 'Cisco Aguilar', 'first_name': 'Cisco', 'last_name': 'Aguilar', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'NM', 'office_type': 'Secretary of State', 'name': 'Maggie Toulouse Oliver', 'first_name': 'Maggie', 'last_name': 'Toulouse Oliver', 'party': 'D', 'caucus': 'D', 'start_date': '2016-12-09', 'start_reason': 'appointed'},
    {'state': 'NC', 'office_type': 'Secretary of State', 'name': 'Elaine Marshall', 'first_name': 'Elaine', 'last_name': 'Marshall', 'party': 'D', 'caucus': 'D', 'start_date': '1997-01-11', 'start_reason': 'elected'},
    {'state': 'ND', 'office_type': 'Secretary of State', 'name': 'Michael Howe', 'first_name': 'Michael', 'last_name': 'Howe', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-01', 'start_reason': 'elected'},
    {'state': 'OH', 'office_type': 'Secretary of State', 'name': 'Frank LaRose', 'first_name': 'Frank', 'last_name': 'LaRose', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-12', 'start_reason': 'elected'},
    {'state': 'OR', 'office_type': 'Secretary of State', 'name': 'Tobias Read', 'first_name': 'Tobias', 'last_name': 'Read', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-06', 'start_reason': 'elected'},
    {'state': 'RI', 'office_type': 'Secretary of State', 'name': 'Gregg Amore', 'first_name': 'Gregg', 'last_name': 'Amore', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-03', 'start_reason': 'elected'},
    {'state': 'SC', 'office_type': 'Secretary of State', 'name': 'Mark Hammond', 'first_name': 'Mark', 'last_name': 'Hammond', 'party': 'R', 'caucus': 'R', 'start_date': '2003-01-15', 'start_reason': 'elected'},
    {'state': 'SD', 'office_type': 'Secretary of State', 'name': 'Monae Johnson', 'first_name': 'Monae', 'last_name': 'Johnson', 'party': 'R', 'caucus': 'R', 'start_date': '2022-12-05', 'start_reason': 'appointed'},
    {'state': 'VT', 'office_type': 'Secretary of State', 'name': 'Sarah Copeland Hanzas', 'first_name': 'Sarah', 'last_name': 'Copeland Hanzas', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-05', 'start_reason': 'elected'},
    {'state': 'WA', 'office_type': 'Secretary of State', 'name': 'Steve Hobbs', 'first_name': 'Steve', 'last_name': 'Hobbs', 'party': 'D', 'caucus': 'D', 'start_date': '2021-11-22', 'start_reason': 'appointed'},
    {'state': 'WV', 'office_type': 'Secretary of State', 'name': 'Kris Warner', 'first_name': 'Kris', 'last_name': 'Warner', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-13', 'start_reason': 'elected'},
    {'state': 'WI', 'office_type': 'Secretary of State', 'name': 'Sarah Godlewski', 'first_name': 'Sarah', 'last_name': 'Godlewski', 'party': 'D', 'caucus': 'D', 'start_date': '2023-03-17', 'start_reason': 'appointed'},
    {'state': 'WY', 'office_type': 'Secretary of State', 'name': 'Chuck Gray', 'first_name': 'Chuck', 'last_name': 'Gray', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},

    # ══════════════════════════════════════════════════════════════════
    # TREASURERS (36 elected)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'AL', 'office_type': 'Treasurer', 'name': 'Young Boozer III', 'first_name': 'Young', 'last_name': 'Boozer', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-16', 'start_reason': 'elected'},
    {'state': 'AZ', 'office_type': 'Treasurer', 'name': 'Kimberly Yee', 'first_name': 'Kimberly', 'last_name': 'Yee', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-07', 'start_reason': 'elected'},
    {'state': 'AR', 'office_type': 'Treasurer', 'name': 'John Thurston', 'first_name': 'John', 'last_name': 'Thurston', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-01', 'start_reason': 'elected'},
    {'state': 'CA', 'office_type': 'Treasurer', 'name': 'Fiona Ma', 'first_name': 'Fiona', 'last_name': 'Ma', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-07', 'start_reason': 'elected'},
    {'state': 'CO', 'office_type': 'Treasurer', 'name': 'Dave Young', 'first_name': 'Dave', 'last_name': 'Young', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-08', 'start_reason': 'elected'},
    {'state': 'CT', 'office_type': 'Treasurer', 'name': 'Erick Russell', 'first_name': 'Erick', 'last_name': 'Russell', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-04', 'start_reason': 'elected'},
    {'state': 'DE', 'office_type': 'Treasurer', 'name': 'Colleen Davis', 'first_name': 'Colleen', 'last_name': 'Davis', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-01', 'start_reason': 'elected'},
    {'state': 'FL', 'office_type': 'Treasurer', 'name': 'Blaise Ingoglia', 'first_name': 'Blaise', 'last_name': 'Ingoglia', 'party': 'R', 'caucus': 'R', 'start_date': '2025-07-21', 'start_reason': 'appointed'},
    {'state': 'ID', 'office_type': 'Treasurer', 'name': 'Julie Ellsworth', 'first_name': 'Julie', 'last_name': 'Ellsworth', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-07', 'start_reason': 'elected'},
    {'state': 'IL', 'office_type': 'Treasurer', 'name': 'Michael Frerichs', 'first_name': 'Michael', 'last_name': 'Frerichs', 'party': 'D', 'caucus': 'D', 'start_date': '2015-01-12', 'start_reason': 'elected'},
    {'state': 'IN', 'office_type': 'Treasurer', 'name': 'Daniel Elliott', 'first_name': 'Daniel', 'last_name': 'Elliott', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-01', 'start_reason': 'elected'},
    {'state': 'IA', 'office_type': 'Treasurer', 'name': 'Roby Smith', 'first_name': 'Roby', 'last_name': 'Smith', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-01', 'start_reason': 'elected'},
    {'state': 'KS', 'office_type': 'Treasurer', 'name': 'Steven Johnson', 'first_name': 'Steven', 'last_name': 'Johnson', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'KY', 'office_type': 'Treasurer', 'name': 'Mark Metcalf', 'first_name': 'Mark', 'last_name': 'Metcalf', 'party': 'R', 'caucus': 'R', 'start_date': '2024-01-01', 'start_reason': 'elected'},
    {'state': 'LA', 'office_type': 'Treasurer', 'name': 'John Fleming', 'first_name': 'John', 'last_name': 'Fleming', 'party': 'R', 'caucus': 'R', 'start_date': '2024-01-08', 'start_reason': 'elected'},
    {'state': 'MA', 'office_type': 'Treasurer', 'name': 'Deborah Goldberg', 'first_name': 'Deborah', 'last_name': 'Goldberg', 'party': 'D', 'caucus': 'D', 'start_date': '2015-01-21', 'start_reason': 'elected'},
    {'state': 'MS', 'office_type': 'Treasurer', 'name': 'David McRae', 'first_name': 'David', 'last_name': 'McRae', 'party': 'R', 'caucus': 'R', 'start_date': '2020-01-09', 'start_reason': 'elected'},
    {'state': 'MO', 'office_type': 'Treasurer', 'name': 'Vivek Malek', 'first_name': 'Vivek', 'last_name': 'Malek', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-17', 'start_reason': 'appointed'},
    {'state': 'NE', 'office_type': 'Treasurer', 'name': 'Joey Spellerberg', 'first_name': 'Joey', 'last_name': 'Spellerberg', 'party': 'R', 'caucus': 'R', 'start_date': '2025-11-06', 'start_reason': 'appointed'},
    {'state': 'NV', 'office_type': 'Treasurer', 'name': 'Zach Conine', 'first_name': 'Zach', 'last_name': 'Conine', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-07', 'start_reason': 'elected'},
    {'state': 'NM', 'office_type': 'Treasurer', 'name': 'Laura Montoya', 'first_name': 'Laura', 'last_name': 'Montoya', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-01', 'start_reason': 'elected'},
    {'state': 'NC', 'office_type': 'Treasurer', 'name': 'Brad Briner', 'first_name': 'Brad', 'last_name': 'Briner', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-01', 'start_reason': 'elected'},
    {'state': 'ND', 'office_type': 'Treasurer', 'name': 'Thomas Beadle', 'first_name': 'Thomas', 'last_name': 'Beadle', 'party': 'R', 'caucus': 'R', 'start_date': '2021-01-01', 'start_reason': 'elected'},
    {'state': 'OH', 'office_type': 'Treasurer', 'name': 'Robert Sprague', 'first_name': 'Robert', 'last_name': 'Sprague', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-14', 'start_reason': 'elected'},
    {'state': 'OK', 'office_type': 'Treasurer', 'name': 'Todd Russ', 'first_name': 'Todd', 'last_name': 'Russ', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'OR', 'office_type': 'Treasurer', 'name': 'Elizabeth Steiner', 'first_name': 'Elizabeth', 'last_name': 'Steiner', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-06', 'start_reason': 'elected'},
    {'state': 'PA', 'office_type': 'Treasurer', 'name': 'Stacy Garrity', 'first_name': 'Stacy', 'last_name': 'Garrity', 'party': 'R', 'caucus': 'R', 'start_date': '2021-01-19', 'start_reason': 'elected'},
    {'state': 'RI', 'office_type': 'Treasurer', 'name': 'James Diossa', 'first_name': 'James', 'last_name': 'Diossa', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-03', 'start_reason': 'elected'},
    {'state': 'SC', 'office_type': 'Treasurer', 'name': 'Curtis Loftis Jr.', 'first_name': 'Curtis', 'last_name': 'Loftis', 'party': 'R', 'caucus': 'R', 'start_date': '2011-01-12', 'start_reason': 'elected'},
    {'state': 'SD', 'office_type': 'Treasurer', 'name': 'Josh Haeder', 'first_name': 'Josh', 'last_name': 'Haeder', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-07', 'start_reason': 'elected'},
    {'state': 'UT', 'office_type': 'Treasurer', 'name': 'Marlo Oaks', 'first_name': 'Marlo', 'last_name': 'Oaks', 'party': 'R', 'caucus': 'R', 'start_date': '2021-07-20', 'start_reason': 'appointed'},
    {'state': 'VT', 'office_type': 'Treasurer', 'name': 'Michael Pieciak', 'first_name': 'Michael', 'last_name': 'Pieciak', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-05', 'start_reason': 'elected'},
    {'state': 'WA', 'office_type': 'Treasurer', 'name': 'Mike Pellicciotti', 'first_name': 'Mike', 'last_name': 'Pellicciotti', 'party': 'D', 'caucus': 'D', 'start_date': '2021-01-11', 'start_reason': 'elected'},
    {'state': 'WV', 'office_type': 'Treasurer', 'name': 'Larry Pack', 'first_name': 'Larry', 'last_name': 'Pack', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-13', 'start_reason': 'elected'},
    {'state': 'WI', 'office_type': 'Treasurer', 'name': 'John Leiber', 'first_name': 'John', 'last_name': 'Leiber', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'WY', 'office_type': 'Treasurer', 'name': 'Curt Meier', 'first_name': 'Curt', 'last_name': 'Meier', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-07', 'start_reason': 'elected'},

    # ══════════════════════════════════════════════════════════════════
    # AUDITORS (24 elected)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'AL', 'office_type': 'Auditor', 'name': 'Andrew Sorrell', 'first_name': 'Andrew', 'last_name': 'Sorrell', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-16', 'start_reason': 'elected'},
    {'state': 'AR', 'office_type': 'Auditor', 'name': 'Dennis Milligan', 'first_name': 'Dennis', 'last_name': 'Milligan', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-10', 'start_reason': 'elected'},
    {'state': 'DE', 'office_type': 'Auditor', 'name': 'Lydia York', 'first_name': 'Lydia', 'last_name': 'York', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-03', 'start_reason': 'elected'},
    {'state': 'IN', 'office_type': 'Auditor', 'name': 'Elise Nieshalla', 'first_name': 'Elise', 'last_name': 'Nieshalla', 'party': 'R', 'caucus': 'R', 'start_date': '2023-12-01', 'start_reason': 'appointed'},
    {'state': 'IA', 'office_type': 'Auditor', 'name': 'Rob Sand', 'first_name': 'Rob', 'last_name': 'Sand', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-01', 'start_reason': 'elected'},
    {'state': 'KY', 'office_type': 'Auditor', 'name': 'Allison Ball', 'first_name': 'Allison', 'last_name': 'Ball', 'party': 'R', 'caucus': 'R', 'start_date': '2024-01-02', 'start_reason': 'elected'},
    {'state': 'MA', 'office_type': 'Auditor', 'name': 'Diana DiZoglio', 'first_name': 'Diana', 'last_name': 'DiZoglio', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-04', 'start_reason': 'elected'},
    {'state': 'MN', 'office_type': 'Auditor', 'name': 'Julie Blaha', 'first_name': 'Julie', 'last_name': 'Blaha', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-07', 'start_reason': 'elected'},
    {'state': 'MO', 'office_type': 'Auditor', 'name': 'Scott Fitzpatrick', 'first_name': 'Scott', 'last_name': 'Fitzpatrick', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'MS', 'office_type': 'Auditor', 'name': 'Shad White', 'first_name': 'Shad', 'last_name': 'White', 'party': 'R', 'caucus': 'R', 'start_date': '2018-07-17', 'start_reason': 'appointed'},
    {'state': 'MT', 'office_type': 'Auditor', 'name': 'James Brown', 'first_name': 'James', 'last_name': 'Brown', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-06', 'start_reason': 'elected'},
    {'state': 'NE', 'office_type': 'Auditor', 'name': 'Mike Foley', 'first_name': 'Mike', 'last_name': 'Foley', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-05', 'start_reason': 'elected'},
    {'state': 'NM', 'office_type': 'Auditor', 'name': 'Joseph Maestas', 'first_name': 'Joseph', 'last_name': 'Maestas', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-01', 'start_reason': 'elected'},
    {'state': 'NC', 'office_type': 'Auditor', 'name': 'Dave Boliek', 'first_name': 'Dave', 'last_name': 'Boliek', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-01', 'start_reason': 'elected'},
    {'state': 'ND', 'office_type': 'Auditor', 'name': 'Josh Gallion', 'first_name': 'Josh', 'last_name': 'Gallion', 'party': 'R', 'caucus': 'R', 'start_date': '2016-12-15', 'start_reason': 'appointed'},
    {'state': 'OH', 'office_type': 'Auditor', 'name': 'Keith Faber', 'first_name': 'Keith', 'last_name': 'Faber', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-14', 'start_reason': 'elected'},
    {'state': 'OK', 'office_type': 'Auditor', 'name': 'Cindy Byrd', 'first_name': 'Cindy', 'last_name': 'Byrd', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-14', 'start_reason': 'elected'},
    {'state': 'PA', 'office_type': 'Auditor', 'name': 'Timothy DeFoor', 'first_name': 'Timothy', 'last_name': 'DeFoor', 'party': 'R', 'caucus': 'R', 'start_date': '2021-01-19', 'start_reason': 'elected'},
    {'state': 'SD', 'office_type': 'Auditor', 'name': 'Rich Sattgast', 'first_name': 'Rich', 'last_name': 'Sattgast', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-07', 'start_reason': 'elected'},
    {'state': 'UT', 'office_type': 'Auditor', 'name': 'Tina Cannon', 'first_name': 'Tina', 'last_name': 'Cannon', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-06', 'start_reason': 'elected'},
    {'state': 'VT', 'office_type': 'Auditor', 'name': 'Doug Hoffer', 'first_name': 'Doug', 'last_name': 'Hoffer', 'party': 'D', 'caucus': 'D', 'start_date': '2013-01-10', 'start_reason': 'elected'},
    {'state': 'WA', 'office_type': 'Auditor', 'name': 'Pat McCarthy', 'first_name': 'Pat', 'last_name': 'McCarthy', 'party': 'D', 'caucus': 'D', 'start_date': '2017-01-11', 'start_reason': 'elected'},
    {'state': 'WV', 'office_type': 'Auditor', 'name': 'Mark Hunt', 'first_name': 'Mark', 'last_name': 'Hunt', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-13', 'start_reason': 'elected'},
    {'state': 'WY', 'office_type': 'Auditor', 'name': 'Kristi Racines', 'first_name': 'Kristi', 'last_name': 'Racines', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-07', 'start_reason': 'elected'},

    # ══════════════════════════════════════════════════════════════════
    # CONTROLLERS / COMPTROLLERS (9 elected)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'CA', 'office_type': 'Controller', 'name': 'Malia Cohen', 'first_name': 'Malia', 'last_name': 'Cohen', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'CT', 'office_type': 'Controller', 'name': 'Sean Scanlon', 'first_name': 'Sean', 'last_name': 'Scanlon', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-04', 'start_reason': 'elected'},
    {'state': 'ID', 'office_type': 'Controller', 'name': 'Brandon Woolf', 'first_name': 'Brandon', 'last_name': 'Woolf', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'IL', 'office_type': 'Controller', 'name': 'Susana Mendoza', 'first_name': 'Susana', 'last_name': 'Mendoza', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-14', 'start_reason': 'elected'},
    {'state': 'MD', 'office_type': 'Controller', 'name': 'Brooke Lierman', 'first_name': 'Brooke', 'last_name': 'Lierman', 'party': 'D', 'caucus': 'D', 'start_date': '2023-01-16', 'start_reason': 'elected'},
    {'state': 'NV', 'office_type': 'Controller', 'name': 'Andy Matthews', 'first_name': 'Andy', 'last_name': 'Matthews', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'NY', 'office_type': 'Controller', 'name': 'Thomas DiNapoli', 'first_name': 'Thomas', 'last_name': 'DiNapoli', 'party': 'D', 'caucus': 'D', 'start_date': '2007-02-07', 'start_reason': 'appointed'},
    {'state': 'SC', 'office_type': 'Controller', 'name': 'Brian Gaines', 'first_name': 'Brian', 'last_name': 'Gaines', 'party': 'D', 'caucus': 'D', 'start_date': '2023-05-12', 'start_reason': 'appointed'},
    {'state': 'TX', 'office_type': 'Controller', 'name': 'Kelly Hancock', 'first_name': 'Kelly', 'last_name': 'Hancock', 'party': 'R', 'caucus': 'R', 'start_date': '2025-07-01', 'start_reason': 'appointed'},

    # ══════════════════════════════════════════════════════════════════
    # SUPERINTENDENTS OF PUBLIC INSTRUCTION (12 elected)
    # Note: OR Superintendent is no longer elected (abolished 2012), but
    # the DB has an elected seat for it. We skip OR here.
    # ══════════════════════════════════════════════════════════════════
    {'state': 'AZ', 'office_type': 'Superintendent of Public Instruction', 'name': 'Tom Horne', 'first_name': 'Tom', 'last_name': 'Horne', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'CA', 'office_type': 'Superintendent of Public Instruction', 'name': 'Tony Thurmond', 'first_name': 'Tony', 'last_name': 'Thurmond', 'party': 'NP', 'caucus': 'D', 'start_date': '2019-01-07', 'start_reason': 'elected'},
    {'state': 'GA', 'office_type': 'Superintendent of Public Instruction', 'name': 'Richard Woods', 'first_name': 'Richard', 'last_name': 'Woods', 'party': 'R', 'caucus': 'R', 'start_date': '2015-01-12', 'start_reason': 'elected'},
    {'state': 'ID', 'office_type': 'Superintendent of Public Instruction', 'name': 'Debbie Critchfield', 'first_name': 'Debbie', 'last_name': 'Critchfield', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-02', 'start_reason': 'elected'},
    {'state': 'MT', 'office_type': 'Superintendent of Public Instruction', 'name': 'Susie Hedalen', 'first_name': 'Susie', 'last_name': 'Hedalen', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-06', 'start_reason': 'elected'},
    {'state': 'NC', 'office_type': 'Superintendent of Public Instruction', 'name': 'Mo Green', 'first_name': 'Mo', 'last_name': 'Green', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-01', 'start_reason': 'elected'},
    {'state': 'ND', 'office_type': 'Superintendent of Public Instruction', 'name': 'Levi Bachmeier', 'first_name': 'Levi', 'last_name': 'Bachmeier', 'party': 'NP', 'caucus': 'R', 'start_date': '2025-11-24', 'start_reason': 'appointed'},
    {'state': 'OK', 'office_type': 'Superintendent of Public Instruction', 'name': 'Lindel Fields', 'first_name': 'Lindel', 'last_name': 'Fields', 'party': 'R', 'caucus': 'R', 'start_date': '2025-10-07', 'start_reason': 'appointed'},
    {'state': 'SC', 'office_type': 'Superintendent of Public Instruction', 'name': 'Ellen Weaver', 'first_name': 'Ellen', 'last_name': 'Weaver', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-11', 'start_reason': 'elected'},
    {'state': 'WA', 'office_type': 'Superintendent of Public Instruction', 'name': 'Chris Reykdal', 'first_name': 'Chris', 'last_name': 'Reykdal', 'party': 'NP', 'caucus': 'D', 'start_date': '2017-01-11', 'start_reason': 'elected'},
    {'state': 'WI', 'office_type': 'Superintendent of Public Instruction', 'name': 'Jill Underly', 'first_name': 'Jill', 'last_name': 'Underly', 'party': 'NP', 'caucus': 'D', 'start_date': '2021-07-06', 'start_reason': 'elected'},

    # ══════════════════════════════════════════════════════════════════
    # INSURANCE COMMISSIONERS (11 elected)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'CA', 'office_type': 'Insurance Commissioner', 'name': 'Ricardo Lara', 'first_name': 'Ricardo', 'last_name': 'Lara', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-07', 'start_reason': 'elected'},
    {'state': 'DE', 'office_type': 'Insurance Commissioner', 'name': 'Trinidad Navarro', 'first_name': 'Trinidad', 'last_name': 'Navarro', 'party': 'D', 'caucus': 'D', 'start_date': '2017-01-03', 'start_reason': 'elected'},
    {'state': 'GA', 'office_type': 'Insurance Commissioner', 'name': 'John King', 'first_name': 'John', 'last_name': 'King', 'party': 'R', 'caucus': 'R', 'start_date': '2019-07-01', 'start_reason': 'appointed'},
    {'state': 'KS', 'office_type': 'Insurance Commissioner', 'name': 'Vicki Schmidt', 'first_name': 'Vicki', 'last_name': 'Schmidt', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-14', 'start_reason': 'elected'},
    {'state': 'LA', 'office_type': 'Insurance Commissioner', 'name': 'Tim Temple', 'first_name': 'Tim', 'last_name': 'Temple', 'party': 'R', 'caucus': 'R', 'start_date': '2024-01-08', 'start_reason': 'elected'},
    {'state': 'MS', 'office_type': 'Insurance Commissioner', 'name': 'Mike Chaney', 'first_name': 'Mike', 'last_name': 'Chaney', 'party': 'R', 'caucus': 'R', 'start_date': '2008-01-10', 'start_reason': 'elected'},
    {'state': 'MT', 'office_type': 'Insurance Commissioner', 'name': 'James Brown', 'first_name': 'James', 'last_name': 'Brown', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-06', 'start_reason': 'elected'},
    {'state': 'NC', 'office_type': 'Insurance Commissioner', 'name': 'Mike Causey', 'first_name': 'Mike', 'last_name': 'Causey', 'party': 'R', 'caucus': 'R', 'start_date': '2017-01-01', 'start_reason': 'elected'},
    {'state': 'ND', 'office_type': 'Insurance Commissioner', 'name': 'Jon Godfread', 'first_name': 'Jon', 'last_name': 'Godfread', 'party': 'R', 'caucus': 'R', 'start_date': '2017-01-03', 'start_reason': 'elected'},
    {'state': 'OK', 'office_type': 'Insurance Commissioner', 'name': 'Glen Mulready', 'first_name': 'Glen', 'last_name': 'Mulready', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-14', 'start_reason': 'elected'},
    {'state': 'WA', 'office_type': 'Insurance Commissioner', 'name': 'Patty Kuderer', 'first_name': 'Patty', 'last_name': 'Kuderer', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-15', 'start_reason': 'elected'},

    # ══════════════════════════════════════════════════════════════════
    # AGRICULTURE COMMISSIONERS (12 elected)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'AL', 'office_type': 'Agriculture Commissioner', 'name': 'Rick Pate', 'first_name': 'Rick', 'last_name': 'Pate', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-14', 'start_reason': 'elected'},
    {'state': 'FL', 'office_type': 'Agriculture Commissioner', 'name': 'Wilton Simpson', 'first_name': 'Wilton', 'last_name': 'Simpson', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-03', 'start_reason': 'elected'},
    {'state': 'GA', 'office_type': 'Agriculture Commissioner', 'name': 'Tyler Harper', 'first_name': 'Tyler', 'last_name': 'Harper', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-09', 'start_reason': 'elected'},
    {'state': 'IA', 'office_type': 'Agriculture Commissioner', 'name': 'Mike Naig', 'first_name': 'Mike', 'last_name': 'Naig', 'party': 'R', 'caucus': 'R', 'start_date': '2018-03-01', 'start_reason': 'appointed'},
    {'state': 'KY', 'office_type': 'Agriculture Commissioner', 'name': 'Jonathan Shell', 'first_name': 'Jonathan', 'last_name': 'Shell', 'party': 'R', 'caucus': 'R', 'start_date': '2024-01-01', 'start_reason': 'elected'},
    {'state': 'LA', 'office_type': 'Agriculture Commissioner', 'name': 'Mike Strain', 'first_name': 'Mike', 'last_name': 'Strain', 'party': 'R', 'caucus': 'R', 'start_date': '2008-01-14', 'start_reason': 'elected'},
    {'state': 'MS', 'office_type': 'Agriculture Commissioner', 'name': 'Andy Gipson', 'first_name': 'Andy', 'last_name': 'Gipson', 'party': 'R', 'caucus': 'R', 'start_date': '2018-04-02', 'start_reason': 'appointed'},
    {'state': 'NC', 'office_type': 'Agriculture Commissioner', 'name': 'Steve Troxler', 'first_name': 'Steve', 'last_name': 'Troxler', 'party': 'R', 'caucus': 'R', 'start_date': '2005-02-08', 'start_reason': 'elected'},
    {'state': 'ND', 'office_type': 'Agriculture Commissioner', 'name': 'Doug Goehring', 'first_name': 'Doug', 'last_name': 'Goehring', 'party': 'R', 'caucus': 'R', 'start_date': '2009-04-06', 'start_reason': 'appointed'},
    {'state': 'SC', 'office_type': 'Agriculture Commissioner', 'name': 'Hugh Weathers', 'first_name': 'Hugh', 'last_name': 'Weathers', 'party': 'R', 'caucus': 'R', 'start_date': '2004-09-14', 'start_reason': 'appointed'},
    {'state': 'TX', 'office_type': 'Agriculture Commissioner', 'name': 'Sid Miller', 'first_name': 'Sid', 'last_name': 'Miller', 'party': 'R', 'caucus': 'R', 'start_date': '2015-01-02', 'start_reason': 'elected'},
    {'state': 'WV', 'office_type': 'Agriculture Commissioner', 'name': 'Kent Leonhardt', 'first_name': 'Kent', 'last_name': 'Leonhardt', 'party': 'R', 'caucus': 'R', 'start_date': '2017-01-16', 'start_reason': 'elected'},

    # ══════════════════════════════════════════════════════════════════
    # LABOR COMMISSIONERS (4 elected)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'GA', 'office_type': 'Labor Commissioner', 'name': 'Barbara Rivera Holmes', 'first_name': 'Barbara', 'last_name': 'Rivera Holmes', 'party': 'R', 'caucus': 'R', 'start_date': '2025-04-04', 'start_reason': 'appointed'},
    {'state': 'NC', 'office_type': 'Labor Commissioner', 'name': 'Luke Farley', 'first_name': 'Luke', 'last_name': 'Farley', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-02', 'start_reason': 'elected'},
    {'state': 'OK', 'office_type': 'Labor Commissioner', 'name': 'Leslie Osborn', 'first_name': 'Leslie', 'last_name': 'Osborn', 'party': 'R', 'caucus': 'R', 'start_date': '2019-01-14', 'start_reason': 'elected'},
    {'state': 'OR', 'office_type': 'Labor Commissioner', 'name': 'Christina Stephenson', 'first_name': 'Christina', 'last_name': 'Stephenson', 'party': 'NP', 'caucus': 'D', 'start_date': '2023-01-02', 'start_reason': 'elected'},
]


# ══════════════════════════════════════════════════════════════════
# VACANT SEATS (elected but no current holder)
# ══════════════════════════════════════════════════════════════════
VACANT_SEATS = [
    # AZ Lt. Gov created by Prop 131 (2022); first election Nov 2026; seat currently unfilled
    {'state': 'AZ', 'office_type': 'Lt. Governor'},
    # OR Superintendent — no longer elected (abolished 2012); should be reclassified
    {'state': 'OR', 'office_type': 'Superintendent of Public Instruction'},
]


# ══════════════════════════════════════════════════════════════════
# VERIFICATION
# ══════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    from collections import Counter

    # Count by office type
    by_office = Counter(o['office_type'] for o in OFFICEHOLDERS)
    print("Officeholders by office type:")
    for office, count in sorted(by_office.items(), key=lambda x: -x[1]):
        print(f"  {office}: {count}")
    print(f"  TOTAL: {len(OFFICEHOLDERS)}")

    # Party distribution
    by_party = Counter(o['party'] for o in OFFICEHOLDERS)
    print(f"\nParty distribution: {dict(by_party)}")

    # Check for duplicates (same state + office_type)
    seen = set()
    dupes = []
    for o in OFFICEHOLDERS:
        key = (o['state'], o['office_type'])
        if key in seen:
            dupes.append(key)
        seen.add(key)
    if dupes:
        print(f"\nDUPLICATES FOUND: {dupes}")
    else:
        print(f"\nNo duplicates found. {len(seen)} unique (state, office) pairs.")

    # Vacant seats
    print(f"\nVacant seats: {len(VACANT_SEATS)}")
    for v in VACANT_SEATS:
        print(f"  {v['state']} {v['office_type']}")

    print(f"\nTotal seats covered: {len(OFFICEHOLDERS)} filled + {len(VACANT_SEATS)} vacant = {len(OFFICEHOLDERS) + len(VACANT_SEATS)}")
