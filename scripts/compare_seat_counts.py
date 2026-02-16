#!/usr/bin/env python3
"""
Compare legislative seat count data between spreadsheets and Supabase database.

Spreadsheet sources:
  1. Comparison 2022 vs 2020 Elections Data.xlsx — has 2021 (post-2020) and 2023 (post-2022) snapshots
  2. Comparison 2024 vs 2022 Elections Data.xlsx — has 2024 (pre-2024=post-2022) and 2025 (post-2024) snapshots

Database source:
  chamber_control table (currently only has effective_date='2025-01-01')
  districts table (num_seats field to infer SMD vs MMD)

DB data is loaded from JSON files (exported from Supabase MCP) rather than direct API calls.
"""

import json
import os
import openpyxl

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
FILE_2022 = "/home/billkramer/Downloads/Comparison 2022 vs 2020 Elections Data.xlsx"
FILE_2024 = "/home/billkramer/Downloads/Comparison 2024 vs 2022 Elections Data.xlsx"

# State name -> abbreviation mapping for the 2024 file (which uses full names)
STATE_NAME_TO_ABBR = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY",
}

# --- Embedded DB data (from Supabase MCP queries) ---

STATES_DATA = [
    {"id":2,"abbreviation":"AK","state_name":"Alaska"},{"id":1,"abbreviation":"AL","state_name":"Alabama"},
    {"id":4,"abbreviation":"AR","state_name":"Arkansas"},{"id":3,"abbreviation":"AZ","state_name":"Arizona"},
    {"id":5,"abbreviation":"CA","state_name":"California"},{"id":6,"abbreviation":"CO","state_name":"Colorado"},
    {"id":7,"abbreviation":"CT","state_name":"Connecticut"},{"id":8,"abbreviation":"DE","state_name":"Delaware"},
    {"id":9,"abbreviation":"FL","state_name":"Florida"},{"id":10,"abbreviation":"GA","state_name":"Georgia"},
    {"id":11,"abbreviation":"HI","state_name":"Hawaii"},{"id":15,"abbreviation":"IA","state_name":"Iowa"},
    {"id":12,"abbreviation":"ID","state_name":"Idaho"},{"id":13,"abbreviation":"IL","state_name":"Illinois"},
    {"id":14,"abbreviation":"IN","state_name":"Indiana"},{"id":16,"abbreviation":"KS","state_name":"Kansas"},
    {"id":17,"abbreviation":"KY","state_name":"Kentucky"},{"id":18,"abbreviation":"LA","state_name":"Louisiana"},
    {"id":21,"abbreviation":"MA","state_name":"Massachusetts"},{"id":20,"abbreviation":"MD","state_name":"Maryland"},
    {"id":19,"abbreviation":"ME","state_name":"Maine"},{"id":22,"abbreviation":"MI","state_name":"Michigan"},
    {"id":23,"abbreviation":"MN","state_name":"Minnesota"},{"id":25,"abbreviation":"MO","state_name":"Missouri"},
    {"id":24,"abbreviation":"MS","state_name":"Mississippi"},{"id":26,"abbreviation":"MT","state_name":"Montana"},
    {"id":33,"abbreviation":"NC","state_name":"North Carolina"},{"id":34,"abbreviation":"ND","state_name":"North Dakota"},
    {"id":27,"abbreviation":"NE","state_name":"Nebraska"},{"id":29,"abbreviation":"NH","state_name":"New Hampshire"},
    {"id":30,"abbreviation":"NJ","state_name":"New Jersey"},{"id":31,"abbreviation":"NM","state_name":"New Mexico"},
    {"id":28,"abbreviation":"NV","state_name":"Nevada"},{"id":32,"abbreviation":"NY","state_name":"New York"},
    {"id":35,"abbreviation":"OH","state_name":"Ohio"},{"id":36,"abbreviation":"OK","state_name":"Oklahoma"},
    {"id":37,"abbreviation":"OR","state_name":"Oregon"},{"id":38,"abbreviation":"PA","state_name":"Pennsylvania"},
    {"id":39,"abbreviation":"RI","state_name":"Rhode Island"},{"id":40,"abbreviation":"SC","state_name":"South Carolina"},
    {"id":41,"abbreviation":"SD","state_name":"South Dakota"},{"id":42,"abbreviation":"TN","state_name":"Tennessee"},
    {"id":43,"abbreviation":"TX","state_name":"Texas"},{"id":44,"abbreviation":"UT","state_name":"Utah"},
    {"id":46,"abbreviation":"VA","state_name":"Virginia"},{"id":45,"abbreviation":"VT","state_name":"Vermont"},
    {"id":47,"abbreviation":"WA","state_name":"Washington"},{"id":49,"abbreviation":"WI","state_name":"Wisconsin"},
    {"id":48,"abbreviation":"WV","state_name":"West Virginia"},{"id":50,"abbreviation":"WY","state_name":"Wyoming"},
]

CHAMBER_CONTROL_DATA = [
    {"abbreviation":"AK","chamber":"House","effective_date":"2025-01-01","d_seats":19,"r_seats":21,"other_seats":0,"vacant_seats":0,"total_seats":40,"control_status":"Coalition"},
    {"abbreviation":"AK","chamber":"Senate","effective_date":"2025-01-01","d_seats":9,"r_seats":11,"other_seats":0,"vacant_seats":0,"total_seats":20,"control_status":"Coalition"},
    {"abbreviation":"AL","chamber":"House","effective_date":"2025-01-01","d_seats":29,"r_seats":76,"other_seats":0,"vacant_seats":0,"total_seats":105,"control_status":"R"},
    {"abbreviation":"AL","chamber":"Senate","effective_date":"2025-01-01","d_seats":8,"r_seats":27,"other_seats":0,"vacant_seats":0,"total_seats":35,"control_status":"R"},
    {"abbreviation":"AR","chamber":"House","effective_date":"2025-01-01","d_seats":19,"r_seats":80,"other_seats":0,"vacant_seats":1,"total_seats":100,"control_status":"R"},
    {"abbreviation":"AR","chamber":"Senate","effective_date":"2025-01-01","d_seats":6,"r_seats":28,"other_seats":0,"vacant_seats":1,"total_seats":35,"control_status":"R"},
    {"abbreviation":"AZ","chamber":"House","effective_date":"2025-01-01","d_seats":27,"r_seats":33,"other_seats":0,"vacant_seats":0,"total_seats":60,"control_status":"R"},
    {"abbreviation":"AZ","chamber":"Senate","effective_date":"2025-01-01","d_seats":13,"r_seats":17,"other_seats":0,"vacant_seats":0,"total_seats":30,"control_status":"R"},
    {"abbreviation":"CA","chamber":"Assembly","effective_date":"2025-01-01","d_seats":60,"r_seats":20,"other_seats":0,"vacant_seats":0,"total_seats":80,"control_status":"D"},
    {"abbreviation":"CA","chamber":"Senate","effective_date":"2025-01-01","d_seats":30,"r_seats":10,"other_seats":0,"vacant_seats":0,"total_seats":40,"control_status":"D"},
    {"abbreviation":"CO","chamber":"House","effective_date":"2025-01-01","d_seats":43,"r_seats":22,"other_seats":0,"vacant_seats":0,"total_seats":65,"control_status":"D"},
    {"abbreviation":"CO","chamber":"Senate","effective_date":"2025-01-01","d_seats":23,"r_seats":12,"other_seats":0,"vacant_seats":0,"total_seats":35,"control_status":"D"},
    {"abbreviation":"CT","chamber":"House","effective_date":"2025-01-01","d_seats":102,"r_seats":49,"other_seats":0,"vacant_seats":0,"total_seats":151,"control_status":"D"},
    {"abbreviation":"CT","chamber":"Senate","effective_date":"2025-01-01","d_seats":25,"r_seats":11,"other_seats":0,"vacant_seats":0,"total_seats":36,"control_status":"D"},
    {"abbreviation":"DE","chamber":"House","effective_date":"2025-01-01","d_seats":27,"r_seats":14,"other_seats":0,"vacant_seats":0,"total_seats":41,"control_status":"D"},
    {"abbreviation":"DE","chamber":"Senate","effective_date":"2025-01-01","d_seats":15,"r_seats":6,"other_seats":0,"vacant_seats":0,"total_seats":21,"control_status":"D"},
    {"abbreviation":"FL","chamber":"House","effective_date":"2025-01-01","d_seats":33,"r_seats":84,"other_seats":0,"vacant_seats":3,"total_seats":120,"control_status":"R"},
    {"abbreviation":"FL","chamber":"Senate","effective_date":"2025-01-01","d_seats":12,"r_seats":27,"other_seats":0,"vacant_seats":1,"total_seats":40,"control_status":"R"},
    {"abbreviation":"GA","chamber":"House","effective_date":"2025-01-01","d_seats":79,"r_seats":99,"other_seats":0,"vacant_seats":2,"total_seats":180,"control_status":"R"},
    {"abbreviation":"GA","chamber":"Senate","effective_date":"2025-01-01","d_seats":23,"r_seats":31,"other_seats":0,"vacant_seats":2,"total_seats":56,"control_status":"R"},
    {"abbreviation":"HI","chamber":"House","effective_date":"2025-01-01","d_seats":42,"r_seats":9,"other_seats":0,"vacant_seats":0,"total_seats":51,"control_status":"D"},
    {"abbreviation":"HI","chamber":"Senate","effective_date":"2025-01-01","d_seats":22,"r_seats":3,"other_seats":0,"vacant_seats":0,"total_seats":25,"control_status":"D"},
    {"abbreviation":"IA","chamber":"House","effective_date":"2025-01-01","d_seats":33,"r_seats":67,"other_seats":0,"vacant_seats":0,"total_seats":100,"control_status":"R"},
    {"abbreviation":"IA","chamber":"Senate","effective_date":"2025-01-01","d_seats":17,"r_seats":33,"other_seats":0,"vacant_seats":0,"total_seats":50,"control_status":"R"},
    {"abbreviation":"ID","chamber":"House","effective_date":"2025-01-01","d_seats":9,"r_seats":61,"other_seats":0,"vacant_seats":0,"total_seats":70,"control_status":"R"},
    {"abbreviation":"ID","chamber":"Senate","effective_date":"2025-01-01","d_seats":6,"r_seats":29,"other_seats":0,"vacant_seats":0,"total_seats":35,"control_status":"R"},
    {"abbreviation":"IL","chamber":"House","effective_date":"2025-01-01","d_seats":78,"r_seats":40,"other_seats":0,"vacant_seats":0,"total_seats":118,"control_status":"D"},
    {"abbreviation":"IL","chamber":"Senate","effective_date":"2025-01-01","d_seats":40,"r_seats":19,"other_seats":0,"vacant_seats":0,"total_seats":59,"control_status":"D"},
    {"abbreviation":"IN","chamber":"House","effective_date":"2025-01-01","d_seats":30,"r_seats":70,"other_seats":0,"vacant_seats":0,"total_seats":100,"control_status":"R"},
    {"abbreviation":"IN","chamber":"Senate","effective_date":"2025-01-01","d_seats":10,"r_seats":40,"other_seats":0,"vacant_seats":0,"total_seats":50,"control_status":"R"},
    {"abbreviation":"KS","chamber":"House","effective_date":"2025-01-01","d_seats":37,"r_seats":88,"other_seats":0,"vacant_seats":0,"total_seats":125,"control_status":"R"},
    {"abbreviation":"KS","chamber":"Senate","effective_date":"2025-01-01","d_seats":9,"r_seats":31,"other_seats":0,"vacant_seats":0,"total_seats":40,"control_status":"R"},
    {"abbreviation":"KY","chamber":"House","effective_date":"2025-01-01","d_seats":20,"r_seats":80,"other_seats":0,"vacant_seats":0,"total_seats":100,"control_status":"R"},
    {"abbreviation":"KY","chamber":"Senate","effective_date":"2025-01-01","d_seats":6,"r_seats":32,"other_seats":0,"vacant_seats":0,"total_seats":38,"control_status":"R"},
    {"abbreviation":"LA","chamber":"House","effective_date":"2025-01-01","d_seats":29,"r_seats":71,"other_seats":0,"vacant_seats":5,"total_seats":105,"control_status":"R"},
    {"abbreviation":"LA","chamber":"Senate","effective_date":"2025-01-01","d_seats":10,"r_seats":28,"other_seats":0,"vacant_seats":1,"total_seats":39,"control_status":"R"},
    {"abbreviation":"MA","chamber":"House","effective_date":"2025-01-01","d_seats":133,"r_seats":25,"other_seats":0,"vacant_seats":2,"total_seats":160,"control_status":"D"},
    {"abbreviation":"MA","chamber":"Senate","effective_date":"2025-01-01","d_seats":34,"r_seats":5,"other_seats":0,"vacant_seats":1,"total_seats":40,"control_status":"D"},
    {"abbreviation":"MD","chamber":"House of Delegates","effective_date":"2025-01-01","d_seats":102,"r_seats":39,"other_seats":0,"vacant_seats":0,"total_seats":141,"control_status":"D"},
    {"abbreviation":"MD","chamber":"Senate","effective_date":"2025-01-01","d_seats":34,"r_seats":13,"other_seats":0,"vacant_seats":0,"total_seats":47,"control_status":"D"},
    {"abbreviation":"ME","chamber":"House","effective_date":"2025-01-01","d_seats":77,"r_seats":72,"other_seats":0,"vacant_seats":2,"total_seats":151,"control_status":"D"},
    {"abbreviation":"ME","chamber":"Senate","effective_date":"2025-01-01","d_seats":20,"r_seats":15,"other_seats":0,"vacant_seats":0,"total_seats":35,"control_status":"D"},
    {"abbreviation":"MI","chamber":"House","effective_date":"2025-01-01","d_seats":52,"r_seats":58,"other_seats":0,"vacant_seats":0,"total_seats":110,"control_status":"R"},
    {"abbreviation":"MI","chamber":"Senate","effective_date":"2025-01-01","d_seats":19,"r_seats":18,"other_seats":0,"vacant_seats":1,"total_seats":38,"control_status":"D"},
    {"abbreviation":"MN","chamber":"House","effective_date":"2025-01-01","d_seats":67,"r_seats":67,"other_seats":0,"vacant_seats":0,"total_seats":134,"control_status":"Power_Sharing"},
    {"abbreviation":"MN","chamber":"Senate","effective_date":"2025-01-01","d_seats":34,"r_seats":33,"other_seats":0,"vacant_seats":0,"total_seats":67,"control_status":"D"},
    {"abbreviation":"MO","chamber":"House","effective_date":"2025-01-01","d_seats":52,"r_seats":106,"other_seats":0,"vacant_seats":5,"total_seats":163,"control_status":"R"},
    {"abbreviation":"MO","chamber":"Senate","effective_date":"2025-01-01","d_seats":10,"r_seats":24,"other_seats":0,"vacant_seats":0,"total_seats":34,"control_status":"R"},
    {"abbreviation":"MS","chamber":"House","effective_date":"2025-01-01","d_seats":43,"r_seats":79,"other_seats":0,"vacant_seats":0,"total_seats":122,"control_status":"R"},
    {"abbreviation":"MS","chamber":"Senate","effective_date":"2025-01-01","d_seats":18,"r_seats":34,"other_seats":0,"vacant_seats":0,"total_seats":52,"control_status":"R"},
    {"abbreviation":"MT","chamber":"House","effective_date":"2025-01-01","d_seats":42,"r_seats":58,"other_seats":0,"vacant_seats":0,"total_seats":100,"control_status":"R"},
    {"abbreviation":"MT","chamber":"Senate","effective_date":"2025-01-01","d_seats":18,"r_seats":32,"other_seats":0,"vacant_seats":0,"total_seats":50,"control_status":"R"},
    {"abbreviation":"NC","chamber":"House","effective_date":"2025-01-01","d_seats":49,"r_seats":71,"other_seats":0,"vacant_seats":0,"total_seats":120,"control_status":"R"},
    {"abbreviation":"NC","chamber":"Senate","effective_date":"2025-01-01","d_seats":20,"r_seats":30,"other_seats":0,"vacant_seats":0,"total_seats":50,"control_status":"R"},
    {"abbreviation":"ND","chamber":"House","effective_date":"2025-01-01","d_seats":11,"r_seats":83,"other_seats":0,"vacant_seats":0,"total_seats":94,"control_status":"R"},
    {"abbreviation":"ND","chamber":"Senate","effective_date":"2025-01-01","d_seats":5,"r_seats":42,"other_seats":0,"vacant_seats":0,"total_seats":47,"control_status":"R"},
    {"abbreviation":"NE","chamber":"Legislature","effective_date":"2025-01-01","d_seats":15,"r_seats":33,"other_seats":1,"vacant_seats":0,"total_seats":49,"control_status":"R"},
    {"abbreviation":"NH","chamber":"House","effective_date":"2025-01-01","d_seats":175,"r_seats":217,"other_seats":0,"vacant_seats":8,"total_seats":400,"control_status":"R"},
    {"abbreviation":"NH","chamber":"Senate","effective_date":"2025-01-01","d_seats":8,"r_seats":16,"other_seats":0,"vacant_seats":0,"total_seats":24,"control_status":"R"},
    {"abbreviation":"NJ","chamber":"Assembly","effective_date":"2025-01-01","d_seats":57,"r_seats":23,"other_seats":0,"vacant_seats":0,"total_seats":80,"control_status":"D"},
    {"abbreviation":"NJ","chamber":"Senate","effective_date":"2025-01-01","d_seats":25,"r_seats":15,"other_seats":0,"vacant_seats":0,"total_seats":40,"control_status":"D"},
    {"abbreviation":"NM","chamber":"House","effective_date":"2025-01-01","d_seats":44,"r_seats":26,"other_seats":0,"vacant_seats":0,"total_seats":70,"control_status":"D"},
    {"abbreviation":"NM","chamber":"Senate","effective_date":"2025-01-01","d_seats":26,"r_seats":16,"other_seats":0,"vacant_seats":0,"total_seats":42,"control_status":"D"},
    {"abbreviation":"NV","chamber":"Assembly","effective_date":"2025-01-01","d_seats":27,"r_seats":15,"other_seats":0,"vacant_seats":0,"total_seats":42,"control_status":"D"},
    {"abbreviation":"NV","chamber":"Senate","effective_date":"2025-01-01","d_seats":13,"r_seats":8,"other_seats":0,"vacant_seats":0,"total_seats":21,"control_status":"D"},
    {"abbreviation":"NY","chamber":"Assembly","effective_date":"2025-01-01","d_seats":103,"r_seats":47,"other_seats":0,"vacant_seats":0,"total_seats":150,"control_status":"D"},
    {"abbreviation":"NY","chamber":"Senate","effective_date":"2025-01-01","d_seats":41,"r_seats":22,"other_seats":0,"vacant_seats":0,"total_seats":63,"control_status":"D"},
    {"abbreviation":"OH","chamber":"House","effective_date":"2025-01-01","d_seats":34,"r_seats":65,"other_seats":0,"vacant_seats":0,"total_seats":99,"control_status":"R"},
    {"abbreviation":"OH","chamber":"Senate","effective_date":"2025-01-01","d_seats":9,"r_seats":24,"other_seats":0,"vacant_seats":0,"total_seats":33,"control_status":"R"},
    {"abbreviation":"OK","chamber":"House","effective_date":"2025-01-01","d_seats":18,"r_seats":81,"other_seats":0,"vacant_seats":2,"total_seats":101,"control_status":"R"},
    {"abbreviation":"OK","chamber":"Senate","effective_date":"2025-01-01","d_seats":8,"r_seats":40,"other_seats":0,"vacant_seats":0,"total_seats":48,"control_status":"R"},
    {"abbreviation":"OR","chamber":"House","effective_date":"2025-01-01","d_seats":37,"r_seats":23,"other_seats":0,"vacant_seats":0,"total_seats":60,"control_status":"D"},
    {"abbreviation":"OR","chamber":"Senate","effective_date":"2025-01-01","d_seats":18,"r_seats":12,"other_seats":0,"vacant_seats":0,"total_seats":30,"control_status":"D"},
    {"abbreviation":"PA","chamber":"House","effective_date":"2025-01-01","d_seats":100,"r_seats":98,"other_seats":0,"vacant_seats":5,"total_seats":203,"control_status":"D"},
    {"abbreviation":"PA","chamber":"Senate","effective_date":"2025-01-01","d_seats":23,"r_seats":27,"other_seats":0,"vacant_seats":0,"total_seats":50,"control_status":"R"},
    {"abbreviation":"RI","chamber":"House","effective_date":"2025-01-01","d_seats":64,"r_seats":11,"other_seats":0,"vacant_seats":0,"total_seats":75,"control_status":"D"},
    {"abbreviation":"RI","chamber":"Senate","effective_date":"2025-01-01","d_seats":34,"r_seats":4,"other_seats":0,"vacant_seats":0,"total_seats":38,"control_status":"D"},
    {"abbreviation":"SC","chamber":"House","effective_date":"2025-01-01","d_seats":36,"r_seats":88,"other_seats":0,"vacant_seats":0,"total_seats":124,"control_status":"R"},
    {"abbreviation":"SC","chamber":"Senate","effective_date":"2025-01-01","d_seats":12,"r_seats":34,"other_seats":0,"vacant_seats":0,"total_seats":46,"control_status":"R"},
    {"abbreviation":"SD","chamber":"House","effective_date":"2025-01-01","d_seats":5,"r_seats":65,"other_seats":0,"vacant_seats":0,"total_seats":70,"control_status":"R"},
    {"abbreviation":"SD","chamber":"Senate","effective_date":"2025-01-01","d_seats":3,"r_seats":32,"other_seats":0,"vacant_seats":0,"total_seats":35,"control_status":"R"},
    {"abbreviation":"TN","chamber":"House","effective_date":"2025-01-01","d_seats":24,"r_seats":75,"other_seats":0,"vacant_seats":0,"total_seats":99,"control_status":"R"},
    {"abbreviation":"TN","chamber":"Senate","effective_date":"2025-01-01","d_seats":6,"r_seats":27,"other_seats":0,"vacant_seats":0,"total_seats":33,"control_status":"R"},
    {"abbreviation":"TX","chamber":"House","effective_date":"2025-01-01","d_seats":62,"r_seats":88,"other_seats":0,"vacant_seats":0,"total_seats":150,"control_status":"R"},
    {"abbreviation":"TX","chamber":"Senate","effective_date":"2025-01-01","d_seats":11,"r_seats":18,"other_seats":0,"vacant_seats":2,"total_seats":31,"control_status":"R"},
    {"abbreviation":"UT","chamber":"House","effective_date":"2025-01-01","d_seats":14,"r_seats":61,"other_seats":0,"vacant_seats":0,"total_seats":75,"control_status":"R"},
    {"abbreviation":"UT","chamber":"Senate","effective_date":"2025-01-01","d_seats":6,"r_seats":22,"other_seats":1,"vacant_seats":0,"total_seats":29,"control_status":"R"},
    {"abbreviation":"VA","chamber":"House of Delegates","effective_date":"2025-01-01","d_seats":64,"r_seats":36,"other_seats":0,"vacant_seats":0,"total_seats":100,"control_status":"D"},
    {"abbreviation":"VA","chamber":"Senate","effective_date":"2025-01-01","d_seats":21,"r_seats":19,"other_seats":0,"vacant_seats":0,"total_seats":40,"control_status":"D"},
    {"abbreviation":"VT","chamber":"House","effective_date":"2025-01-01","d_seats":92,"r_seats":57,"other_seats":0,"vacant_seats":1,"total_seats":150,"control_status":"D"},
    {"abbreviation":"VT","chamber":"Senate","effective_date":"2025-01-01","d_seats":17,"r_seats":13,"other_seats":0,"vacant_seats":0,"total_seats":30,"control_status":"D"},
    {"abbreviation":"WA","chamber":"House","effective_date":"2025-01-01","d_seats":59,"r_seats":39,"other_seats":0,"vacant_seats":0,"total_seats":98,"control_status":"D"},
    {"abbreviation":"WA","chamber":"Senate","effective_date":"2025-01-01","d_seats":30,"r_seats":19,"other_seats":0,"vacant_seats":0,"total_seats":49,"control_status":"D"},
    {"abbreviation":"WI","chamber":"Assembly","effective_date":"2025-01-01","d_seats":45,"r_seats":54,"other_seats":0,"vacant_seats":0,"total_seats":99,"control_status":"R"},
    {"abbreviation":"WI","chamber":"Senate","effective_date":"2025-01-01","d_seats":15,"r_seats":18,"other_seats":0,"vacant_seats":0,"total_seats":33,"control_status":"R"},
    {"abbreviation":"WV","chamber":"House of Delegates","effective_date":"2025-01-01","d_seats":9,"r_seats":90,"other_seats":0,"vacant_seats":1,"total_seats":100,"control_status":"R"},
    {"abbreviation":"WV","chamber":"Senate","effective_date":"2025-01-01","d_seats":2,"r_seats":32,"other_seats":0,"vacant_seats":0,"total_seats":34,"control_status":"R"},
    {"abbreviation":"WY","chamber":"House","effective_date":"2025-01-01","d_seats":6,"r_seats":56,"other_seats":0,"vacant_seats":0,"total_seats":62,"control_status":"R"},
    {"abbreviation":"WY","chamber":"Senate","effective_date":"2025-01-01","d_seats":2,"r_seats":29,"other_seats":0,"vacant_seats":0,"total_seats":31,"control_status":"R"},
]

# District data from DB: has_mmd indicates if any district has num_seats > 1
DISTRICT_MMD_DATA = [
    {"abbreviation":"AK","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":40,"total_seats":40},
    {"abbreviation":"AK","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":20,"total_seats":20},
    {"abbreviation":"AL","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":105,"total_seats":105},
    {"abbreviation":"AL","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":35,"total_seats":35},
    {"abbreviation":"AR","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":100,"total_seats":100},
    {"abbreviation":"AR","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":35,"total_seats":35},
    {"abbreviation":"AZ","chamber":"House","has_mmd":True,"all_smd":False,"num_districts":30,"total_seats":60},
    {"abbreviation":"AZ","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":30,"total_seats":30},
    {"abbreviation":"CA","chamber":"Assembly","has_mmd":False,"all_smd":True,"num_districts":80,"total_seats":80},
    {"abbreviation":"CA","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":40,"total_seats":40},
    {"abbreviation":"CO","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":65,"total_seats":65},
    {"abbreviation":"CO","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":35,"total_seats":35},
    {"abbreviation":"CT","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":151,"total_seats":151},
    {"abbreviation":"CT","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":36,"total_seats":36},
    {"abbreviation":"DE","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":41,"total_seats":41},
    {"abbreviation":"DE","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":21,"total_seats":21},
    {"abbreviation":"FL","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":120,"total_seats":120},
    {"abbreviation":"FL","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":40,"total_seats":40},
    {"abbreviation":"GA","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":180,"total_seats":180},
    {"abbreviation":"GA","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":56,"total_seats":56},
    {"abbreviation":"HI","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":51,"total_seats":51},
    {"abbreviation":"HI","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":25,"total_seats":25},
    {"abbreviation":"IA","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":100,"total_seats":100},
    {"abbreviation":"IA","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":50,"total_seats":50},
    {"abbreviation":"ID","chamber":"House","has_mmd":True,"all_smd":False,"num_districts":35,"total_seats":70},
    {"abbreviation":"ID","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":35,"total_seats":35},
    {"abbreviation":"IL","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":118,"total_seats":118},
    {"abbreviation":"IL","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":59,"total_seats":59},
    {"abbreviation":"IN","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":100,"total_seats":100},
    {"abbreviation":"IN","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":50,"total_seats":50},
    {"abbreviation":"KS","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":125,"total_seats":125},
    {"abbreviation":"KS","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":40,"total_seats":40},
    {"abbreviation":"KY","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":100,"total_seats":100},
    {"abbreviation":"KY","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":38,"total_seats":38},
    {"abbreviation":"LA","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":105,"total_seats":105},
    {"abbreviation":"LA","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":39,"total_seats":39},
    {"abbreviation":"MA","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":160,"total_seats":160},
    {"abbreviation":"MA","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":40,"total_seats":40},
    {"abbreviation":"MD","chamber":"House of Delegates","has_mmd":True,"all_smd":False,"num_districts":71,"total_seats":141},
    {"abbreviation":"MD","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":47,"total_seats":47},
    {"abbreviation":"ME","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":151,"total_seats":151},
    {"abbreviation":"ME","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":35,"total_seats":35},
    {"abbreviation":"MI","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":110,"total_seats":110},
    {"abbreviation":"MI","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":38,"total_seats":38},
    {"abbreviation":"MN","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":134,"total_seats":134},
    {"abbreviation":"MN","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":67,"total_seats":67},
    {"abbreviation":"MO","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":163,"total_seats":163},
    {"abbreviation":"MO","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":34,"total_seats":34},
    {"abbreviation":"MS","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":122,"total_seats":122},
    {"abbreviation":"MS","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":52,"total_seats":52},
    {"abbreviation":"MT","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":100,"total_seats":100},
    {"abbreviation":"MT","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":50,"total_seats":50},
    {"abbreviation":"NC","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":120,"total_seats":120},
    {"abbreviation":"NC","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":50,"total_seats":50},
    {"abbreviation":"ND","chamber":"House","has_mmd":True,"all_smd":False,"num_districts":47,"total_seats":94},
    {"abbreviation":"ND","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":47,"total_seats":47},
    {"abbreviation":"NE","chamber":"Legislature","has_mmd":False,"all_smd":True,"num_districts":49,"total_seats":49},
    {"abbreviation":"NH","chamber":"House","has_mmd":True,"all_smd":False,"num_districts":200,"total_seats":400},
    {"abbreviation":"NH","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":24,"total_seats":24},
    {"abbreviation":"NJ","chamber":"Assembly","has_mmd":True,"all_smd":False,"num_districts":40,"total_seats":80},
    {"abbreviation":"NJ","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":40,"total_seats":40},
    {"abbreviation":"NM","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":70,"total_seats":70},
    {"abbreviation":"NM","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":42,"total_seats":42},
    {"abbreviation":"NV","chamber":"Assembly","has_mmd":False,"all_smd":True,"num_districts":42,"total_seats":42},
    {"abbreviation":"NV","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":21,"total_seats":21},
    {"abbreviation":"NY","chamber":"Assembly","has_mmd":False,"all_smd":True,"num_districts":150,"total_seats":150},
    {"abbreviation":"NY","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":63,"total_seats":63},
    {"abbreviation":"OH","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":99,"total_seats":99},
    {"abbreviation":"OH","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":33,"total_seats":33},
    {"abbreviation":"OK","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":101,"total_seats":101},
    {"abbreviation":"OK","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":48,"total_seats":48},
    {"abbreviation":"OR","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":60,"total_seats":60},
    {"abbreviation":"OR","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":30,"total_seats":30},
    {"abbreviation":"PA","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":203,"total_seats":203},
    {"abbreviation":"PA","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":50,"total_seats":50},
    {"abbreviation":"RI","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":75,"total_seats":75},
    {"abbreviation":"RI","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":38,"total_seats":38},
    {"abbreviation":"SC","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":124,"total_seats":124},
    {"abbreviation":"SC","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":46,"total_seats":46},
    {"abbreviation":"SD","chamber":"House","has_mmd":True,"all_smd":False,"num_districts":35,"total_seats":70},
    {"abbreviation":"SD","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":35,"total_seats":35},
    {"abbreviation":"TN","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":99,"total_seats":99},
    {"abbreviation":"TN","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":33,"total_seats":33},
    {"abbreviation":"TX","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":150,"total_seats":150},
    {"abbreviation":"TX","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":31,"total_seats":31},
    {"abbreviation":"UT","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":75,"total_seats":75},
    {"abbreviation":"UT","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":29,"total_seats":29},
    {"abbreviation":"VA","chamber":"House of Delegates","has_mmd":False,"all_smd":True,"num_districts":100,"total_seats":100},
    {"abbreviation":"VA","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":40,"total_seats":40},
    {"abbreviation":"VT","chamber":"House","has_mmd":True,"all_smd":False,"num_districts":109,"total_seats":150},
    {"abbreviation":"VT","chamber":"Senate","has_mmd":True,"all_smd":False,"num_districts":15,"total_seats":30},
    {"abbreviation":"WA","chamber":"House","has_mmd":True,"all_smd":False,"num_districts":49,"total_seats":98},
    {"abbreviation":"WA","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":49,"total_seats":49},
    {"abbreviation":"WI","chamber":"Assembly","has_mmd":False,"all_smd":True,"num_districts":99,"total_seats":99},
    {"abbreviation":"WI","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":33,"total_seats":33},
    {"abbreviation":"WV","chamber":"House of Delegates","has_mmd":False,"all_smd":True,"num_districts":100,"total_seats":100},
    {"abbreviation":"WV","chamber":"Senate","has_mmd":True,"all_smd":False,"num_districts":17,"total_seats":34},
    {"abbreviation":"WY","chamber":"House","has_mmd":False,"all_smd":True,"num_districts":62,"total_seats":62},
    {"abbreviation":"WY","chamber":"Senate","has_mmd":False,"all_smd":True,"num_districts":31,"total_seats":31},
]

# DB chamber names that differ from spreadsheet "House" convention
# Spreadsheet always uses "House" and "Senate" (+ "Unicameral" for NE)
# DB uses: "Assembly" (CA, NJ, NV, NY, WI), "House of Delegates" (MD, VA, WV), "Legislature" (NE)
DB_CHAMBER_ALIASES = {
    # (abbr, ss_chamber) -> db_chamber
    ("CA", "House"): "Assembly",
    ("NJ", "House"): "Assembly",
    ("NV", "House"): "Assembly",
    ("NY", "House"): "Assembly",
    ("WI", "House"): "Assembly",
    ("MD", "House"): "House of Delegates",
    ("VA", "House"): "House of Delegates",
    ("WV", "House"): "House of Delegates",
    ("NE", "Unicameral"): "Legislature",
}


def safe_int(val):
    """Convert a cell value to int, treating None/empty as 0."""
    if val is None:
        return 0
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def normalize_chamber(chamber_str):
    """Normalize chamber names for comparison."""
    if not chamber_str:
        return ""
    c = chamber_str.strip()
    if c.lower() == "unicameral":
        return "Unicameral"
    elif c.lower() == "house":
        return "House"
    elif c.lower() == "senate":
        return "Senate"
    return c


def read_spreadsheet_data(filepath, state_col_is_name=False):
    """
    Read legislative seat data from a spreadsheet.

    The "after" columns (later year) are cols 10-14 (1-indexed), indexes 9-13.
    The "before" columns (earlier year) are cols 4-8 (1-indexed), indexes 3-7.
    """
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb["Legislatures"]

    records = {}
    for row in ws.iter_rows(min_row=3, values_only=True):
        state_raw = row[0]
        chamber_raw = row[1]
        dist_type = row[2]

        if not state_raw or not chamber_raw:
            continue

        state_str = str(state_raw).strip()
        if state_str.lower().startswith("total"):
            continue

        if state_col_is_name:
            abbr = STATE_NAME_TO_ABBR.get(state_str)
            if not abbr and len(row) > 27 and row[27]:
                abbr = str(row[27]).strip()
            if not abbr:
                print(f"  WARNING: Could not map state name '{state_str}', skipping")
                continue
        else:
            abbr = state_str.upper()

        chamber = normalize_chamber(str(chamber_raw))

        key = (abbr, chamber)
        records[key] = {
            "total_after": safe_int(row[9]),
            "d_after": safe_int(row[10]),
            "r_after": safe_int(row[11]),
            "other_after": safe_int(row[12]),
            "vacant_after": safe_int(row[13]),
            "total_before": safe_int(row[3]),
            "d_before": safe_int(row[4]),
            "r_before": safe_int(row[5]),
            "other_before": safe_int(row[6]),
            "vacant_before": safe_int(row[7]),
            "dist_type": str(dist_type).strip() if dist_type else "",
        }

    wb.close()
    return records


def find_db_key(abbr, ss_chamber, db_lookup):
    """Find the matching DB key for a spreadsheet (abbr, chamber) pair."""
    # Direct match
    if (abbr, ss_chamber) in db_lookup:
        return (abbr, ss_chamber)
    # Try alias
    alias = DB_CHAMBER_ALIASES.get((abbr, ss_chamber))
    if alias and (abbr, alias) in db_lookup:
        return (abbr, alias)
    return None


def main():
    print("=" * 80)
    print("LEGISLATIVE SEAT COUNT COMPARISON: Spreadsheets vs Supabase Database")
    print("=" * 80)
    print()

    # ------------------------------------------------------------------
    # 1. Read spreadsheet data
    # ------------------------------------------------------------------
    print("--- Reading spreadsheet data ---")
    print()

    data_2022 = read_spreadsheet_data(FILE_2022, state_col_is_name=False)
    print(f"  File 1 (2022 vs 2020): {len(data_2022)} chamber records")
    print(f"    'Before' columns = 2021 (post-2020 election composition)")
    print(f"    'After' columns  = 2023 (post-2022 election composition)")

    data_2024 = read_spreadsheet_data(FILE_2024, state_col_is_name=True)
    print(f"  File 2 (2024 vs 2022): {len(data_2024)} chamber records")
    print(f"    'Before' columns = 2024 (pre-2024 election = post-2022 composition)")
    print(f"    'After' columns  = 2025 (post-2024 election composition)")
    print()

    # ------------------------------------------------------------------
    # 2. Cross-check: File 1 "after" (2023) should match File 2 "before" (2024)
    # ------------------------------------------------------------------
    print("--- Cross-check: File 1 post-2022 (2023) vs File 2 pre-2024 (2024) ---")
    print("  (These should be the same snapshot: post-2022 election composition,")
    print("   but File 2 may reflect intervening special elections/vacancies)")
    print()
    cross_match = 0
    cross_mismatches = []
    for key in sorted(data_2022.keys()):
        if key not in data_2024:
            cross_mismatches.append(f"  {key[0]} {key[1]}: present in File 1 but missing from File 2")
            continue
        r1 = data_2022[key]
        r2 = data_2024[key]
        diffs = []
        for field, label in [
            ("total", "Total"), ("d", "D"), ("r", "R"), ("other", "Other"), ("vacant", "Vacant")
        ]:
            v1 = r1[f"{field}_after"]
            v2 = r2[f"{field}_before"]
            if v1 != v2:
                diffs.append(f"{label}: File1-2023={v1} vs File2-2024={v2}")
        if diffs:
            cross_mismatches.append(f"  {key[0]} {key[1]}: {', '.join(diffs)}")
        else:
            cross_match += 1

    print(f"  Perfect matches: {cross_match}")
    print(f"  Discrepancies:   {len(cross_mismatches)}")
    if cross_mismatches:
        print()
        for m in cross_mismatches:
            print(m)
    print()

    # ------------------------------------------------------------------
    # 3. Build DB lookup
    # ------------------------------------------------------------------
    print("--- Loading database data ---")
    print()

    state_name_map = {r["abbreviation"]: r["state_name"] for r in STATES_DATA}
    print(f"  States: {len(state_name_map)}")

    # Build DB chamber_control lookup: (abbr, chamber) -> record
    db_data = {}
    for row in CHAMBER_CONTROL_DATA:
        if row["effective_date"] == "2025-01-01":
            key = (row["abbreviation"], row["chamber"])
            db_data[key] = {
                "total": row["total_seats"] or 0,
                "d": row["d_seats"] or 0,
                "r": row["r_seats"] or 0,
                "other": row["other_seats"] or 0,
                "vacant": row["vacant_seats"] or 0,
                "control_status": row["control_status"],
            }

    dates = set(r["effective_date"] for r in CHAMBER_CONTROL_DATA)
    print(f"  Chamber control records: {len(db_data)} (effective dates: {sorted(dates)})")

    # Build district MMD lookup
    db_dist_map = {}
    for row in DISTRICT_MMD_DATA:
        chamber = row["chamber"]
        key = (row["abbreviation"], chamber)
        db_dist_map[key] = {
            "has_mmd": row["has_mmd"],
            "all_smd": row["all_smd"],
            "num_districts": row["num_districts"],
            "total_seats": row["total_seats"],
        }
    print(f"  District records: {len(db_dist_map)} state/chamber combos")
    print()

    # ------------------------------------------------------------------
    # 4. Compare: Spreadsheet 2025 (post-2024) vs DB 2025-01-01
    # ------------------------------------------------------------------
    print("=" * 80)
    print("COMPARISON: Spreadsheet post-2024 (2025) vs DB chamber_control 2025-01-01")
    print("=" * 80)
    print()

    perfect_matches = 0
    discrepancies = []
    ss_only = []
    db_matched_keys = set()

    for key in sorted(data_2024.keys()):
        abbr, ss_chamber = key
        ss = data_2024[key]

        db_key = find_db_key(abbr, ss_chamber, db_data)
        if not db_key:
            ss_only.append(key)
            continue

        db_matched_keys.add(db_key)
        db = db_data[db_key]

        diffs = []
        if ss["total_after"] != db["total"]:
            diffs.append(f"Total: SS={ss['total_after']} DB={db['total']}")
        if ss["d_after"] != db["d"]:
            diffs.append(f"D: SS={ss['d_after']} DB={db['d']}")
        if ss["r_after"] != db["r"]:
            diffs.append(f"R: SS={ss['r_after']} DB={db['r']}")
        if ss["other_after"] != db["other"]:
            diffs.append(f"Other/I: SS={ss['other_after']} DB={db['other']}")
        if ss["vacant_after"] != db["vacant"]:
            diffs.append(f"Vacant: SS={ss['vacant_after']} DB={db['vacant']}")

        if diffs:
            discrepancies.append({
                "state": abbr,
                "state_name": state_name_map.get(abbr, abbr),
                "ss_chamber": ss_chamber,
                "db_chamber": db_key[1],
                "diffs": diffs,
                "ss": ss,
                "db": db,
            })
        else:
            perfect_matches += 1

    # Check for DB-only records (not matched by any spreadsheet entry)
    db_only = []
    for key in sorted(db_data.keys()):
        if key not in db_matched_keys:
            db_only.append(key)

    total_compared = perfect_matches + len(discrepancies)
    print(f"  Chambers compared:  {total_compared}")
    print(f"  Perfect matches:    {perfect_matches}")
    print(f"  With discrepancies: {len(discrepancies)}")
    print(f"  In spreadsheet only (not in DB): {len(ss_only)}")
    print(f"  In DB only (not in spreadsheet): {len(db_only)}")
    print()

    if ss_only:
        print("  Chambers in spreadsheet but NOT in DB:")
        for key in ss_only:
            print(f"    {key[0]} {key[1]}")
        print()

    if db_only:
        print("  Chambers in DB but NOT in spreadsheet:")
        for key in db_only:
            print(f"    {key[0]} {key[1]}")
        print()

    if discrepancies:
        print(f"  --- Seat Count Discrepancies ({len(discrepancies)}) ---")
        print()
        header = f"  {'State':<6} {'SS Chamber':<12} {'DB Chamber':<20} {'Differences'}"
        print(header)
        print(f"  {'-----':<6} {'---------':<12} {'----------':<20} {'-----------'}")
        for d in discrepancies:
            ch_note = ""
            if d["ss_chamber"] != d["db_chamber"]:
                ch_note = f" (DB: {d['db_chamber']})"
            print(f"  {d['state']:<6} {d['ss_chamber']:<12} {d['db_chamber']:<20} {'; '.join(d['diffs'])}")

        print()
        print("  Detailed breakdown of discrepancies:")
        print()
        for d in discrepancies:
            ss = d["ss"]
            db = d["db"]
            chamber_label = d["ss_chamber"]
            if d["ss_chamber"] != d["db_chamber"]:
                chamber_label = f"{d['ss_chamber']} -> DB: {d['db_chamber']}"
            print(f"  {d['state']} {chamber_label} ({d['state_name']}):")
            print(f"    Spreadsheet: Total={ss['total_after']}, D={ss['d_after']}, R={ss['r_after']}, Other={ss['other_after']}, Vacant={ss['vacant_after']}")
            print(f"    Database:    Total={db['total']}, D={db['d']}, R={db['r']}, Other={db['other']}, Vacant={db['vacant']}")
            print(f"    DB control:  {db['control_status']}")
            # Show the net delta
            d_diff = ss['d_after'] - db['d']
            r_diff = ss['r_after'] - db['r']
            o_diff = ss['other_after'] - db['other']
            v_diff = ss['vacant_after'] - db['vacant']
            print(f"    Delta (SS-DB): D={d_diff:+d}, R={r_diff:+d}, Other={o_diff:+d}, Vacant={v_diff:+d}")
            print()
    else:
        print("  No discrepancies found - all seat counts match perfectly!")
        print()

    # ------------------------------------------------------------------
    # 5. District type comparison (SMD vs MMD)
    # ------------------------------------------------------------------
    print("=" * 80)
    print("DISTRICT TYPE COMPARISON (SMD vs MMD)")
    print("=" * 80)
    print()
    print("  Note: The districts table has no 'district_type' column, but we can infer")
    print("  SMD vs MMD from the 'num_seats' field on each district record.")
    print("  SMD = all districts have num_seats=1; MMD = some districts have num_seats>1.")
    print()

    dist_match = 0
    dist_mismatch = []

    for key in sorted(data_2024.keys()):
        abbr, ss_chamber = key
        ss_type = data_2024[key]["dist_type"]

        # Find matching DB district record
        db_dist_key = find_db_key(abbr, ss_chamber, db_dist_map)
        if not db_dist_key:
            continue

        db_info = db_dist_map[db_dist_key]
        db_is_smd = db_info["all_smd"]
        db_is_mmd = db_info["has_mmd"]

        if ss_type == "SMD" and db_is_smd:
            dist_match += 1
        elif ss_type == "MMD" and db_is_mmd:
            dist_match += 1
        else:
            inferred = "SMD" if db_is_smd else "MMD"
            dist_mismatch.append({
                "abbr": abbr,
                "ss_chamber": ss_chamber,
                "db_chamber": db_dist_key[1],
                "ss_type": ss_type,
                "db_inferred": inferred,
                "num_districts": db_info["num_districts"],
                "total_seats": db_info["total_seats"],
            })

    print(f"  District type matches: {dist_match}")
    print(f"  District type mismatches: {len(dist_mismatch)}")
    if dist_mismatch:
        print()
        print(f"  {'State':<6} {'SS Chamber':<12} {'SS Type':<8} {'DB Inferred':<12} {'#Dists':<8} {'Total Seats'}")
        print(f"  {'-----':<6} {'---------':<12} {'-------':<8} {'-----------':<12} {'------':<8} {'-----------'}")
        for m in dist_mismatch:
            print(f"  {m['abbr']:<6} {m['ss_chamber']:<12} {m['ss_type']:<8} {m['db_inferred']:<12} {m['num_districts']:<8} {m['total_seats']}")
    print()

    # ------------------------------------------------------------------
    # 6. Historical snapshot coverage
    # ------------------------------------------------------------------
    print("=" * 80)
    print("HISTORICAL SNAPSHOT COVERAGE")
    print("=" * 80)
    print()

    print("  Snapshots available in spreadsheets:")
    print("    - 2021-01-01 (post-2020 election) -- File 1 'before' columns (99 chambers)")
    print("    - 2023-01-01 (post-2022 election) -- File 1 'after' columns (99 chambers)")
    print("    - 2024 (pre-2024, ~= post-2022 + specials) -- File 2 'before' columns (99 chambers)")
    print("    - 2025-01-01 (post-2024 election) -- File 2 'after' columns (99 chambers)")
    print()

    print("  Snapshots in chamber_control database table:")
    for d in sorted(dates):
        count = sum(1 for r in CHAMBER_CONTROL_DATA if r["effective_date"] == d)
        print(f"    - {d}: {count} records")
    print()

    missing_snapshots = []
    for label, year in [
        ("2021-01-01 (post-2020 election)", "2021"),
        ("2023-01-01 (post-2022 election)", "2023"),
    ]:
        if not any(d.startswith(year) for d in dates):
            missing_snapshots.append(label)

    if missing_snapshots:
        print(f"  MISSING from database ({len(missing_snapshots)} historical snapshots):")
        for m in missing_snapshots:
            print(f"    - {m}")
        print()
        print("  These could be imported from the spreadsheets to build a historical record")
        print("  of chamber composition changes across election cycles.")
    else:
        print("  All historical snapshots are present in the database.")
    print()

    # ------------------------------------------------------------------
    # 7. Summary totals across snapshots
    # ------------------------------------------------------------------
    print("=" * 80)
    print("SPREADSHEET SEAT COUNT TOTALS (by snapshot)")
    print("=" * 80)
    print()

    for label, data, field_prefix in [
        ("2021 (post-2020)", data_2022, "before"),
        ("2023 (post-2022)", data_2022, "after"),
        ("2025 (post-2024)", data_2024, "after"),
    ]:
        total_d = sum(r[f"d_{field_prefix}"] for r in data.values())
        total_r = sum(r[f"r_{field_prefix}"] for r in data.values())
        total_other = sum(r[f"other_{field_prefix}"] for r in data.values())
        total_vacant = sum(r[f"vacant_{field_prefix}"] for r in data.values())
        total_seats = sum(r[f"total_{field_prefix}"] for r in data.values())
        accounted = total_d + total_r + total_other + total_vacant
        print(f"  {label}:")
        print(f"    Total Seats: {total_seats}")
        print(f"    D: {total_d}, R: {total_r}, Other: {total_other}, Vacant: {total_vacant}")
        print(f"    Sum (D+R+Other+Vacant): {accounted} {'(matches total)' if accounted == total_seats else f'(MISMATCH: total={total_seats})'}")
        print()

    # DB totals for comparison
    db_total_d = sum(r["d"] for r in db_data.values())
    db_total_r = sum(r["r"] for r in db_data.values())
    db_total_other = sum(r["other"] for r in db_data.values())
    db_total_vacant = sum(r["vacant"] for r in db_data.values())
    db_total = sum(r["total"] for r in db_data.values())
    db_accounted = db_total_d + db_total_r + db_total_other + db_total_vacant
    print(f"  Database 2025-01-01:")
    print(f"    Total Seats: {db_total}")
    print(f"    D: {db_total_d}, R: {db_total_r}, Other: {db_total_other}, Vacant: {db_total_vacant}")
    print(f"    Sum (D+R+Other+Vacant): {db_accounted} {'(matches total)' if db_accounted == db_total else f'(MISMATCH: total={db_total})'}")
    print()

    # ------------------------------------------------------------------
    # 8. Chamber name mapping reference
    # ------------------------------------------------------------------
    print("=" * 80)
    print("CHAMBER NAME MAPPING (Spreadsheet -> Database)")
    print("=" * 80)
    print()
    print("  States where the spreadsheet 'House' maps to a different DB chamber name:")
    print()
    for (abbr, ss_ch), db_ch in sorted(DB_CHAMBER_ALIASES.items()):
        print(f"    {abbr}: SS '{ss_ch}' -> DB '{db_ch}'")
    print()

    print("=" * 80)
    print("DONE")
    print("=" * 80)


if __name__ == "__main__":
    main()
