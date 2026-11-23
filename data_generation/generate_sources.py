"""Generate the three small, hand-curated source datasets plus the answer key.

Run:  python data_generation/generate_sources.py

Why hand-curated instead of random?
  Each record tests one specific edge case: an abbreviation, a duplicate, a conflict,
  a near-miss. With ~80 records, every one can be traced through Bronze -> Silver -> Gold.
  The answer key records which physical site each record really describes, so match
  quality can be measured (precision and recall) instead of eyeballed.

Outputs
  data/raw/network_inventory/network_inventory.csv   (Source 1, CSV)
  data/raw/rf_planning/rf_planning.json             (Source 2, JSON Lines, nested)
  data/raw/field_operations/field_operations.parquet (Source 3, Parquet, typed)
  data/ground_truth/true_sites.csv                  (the real physical sites; no system has this)
  data/ground_truth/ground_truth.csv                (source record -> true site, plus the edge case it tests)
"""
from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
TRUTH = ROOT / "data" / "ground_truth"

# ---------------------------------------------------------------------------
# The real world: physical cell sites. No source system has this table.
# It is only used to document the data and to score the matching.
# ---------------------------------------------------------------------------
TRUE_SITES = [
    # key,   borough,         true name,                    true address,                       zip,    lat,        lon
    ("S01", "BRONX",         "Grand Concourse North",       "1250 GRAND CONCOURSE",             "10456", 40.834712, -73.916521),
    ("S02", "BRONX",         "Grand Concourse South",       "1100 GRAND CONCOURSE",             "10456", 40.829874, -73.919612),
    ("S03", "BRONX",         "Fordham Webster",             "2501 WEBSTER AVENUE",              "10458", 40.861023, -73.889015),
    ("S04", "BRONX",         "Hunts Point",                 "890 HUNTS POINT AVENUE",           "10474", 40.813044, -73.887120),
    ("S05", "BRONX",         "Yankee Stadium",              "1 EAST 161ST STREET",              "10451", 40.829643, -73.926175),
    ("S06", "MANHATTAN",     "Times Square Broadway",       "1600 BROADWAY",                    "10019", 40.761245, -73.983711),
    ("S07", "MANHATTAN",     "Columbia Broadway",           "2880 BROADWAY",                    "10025", 40.805812, -73.964533),
    ("S08", "MANHATTAN",     "Harlem 125th",                "125 WEST 125TH STREET",            "10027", 40.809410, -73.948012),
    ("S09", "MANHATTAN",     "South Street Seaport",        "200 WATER STREET",                 "10038", 40.706712, -74.004533),
    ("S10", "BROOKLYN",      "Bushwick Broadway",           "1600 BROADWAY",                    "11207", 40.689211, -73.918734),
    ("S11", "BROOKLYN",      "Atlantic Boerum Hill",        "445 ATLANTIC AVENUE",              "11217", 40.686511, -73.981045),
    ("S12", "BROOKLYN",      "DUMBO Main Street",           "1 MAIN STREET",                    "11201", 40.703312, -73.990321),
    ("S13", "BROOKLYN",      "Coney Island Ocean Parkway",  "2900 OCEAN PARKWAY",               "11235", 40.579023, -73.969011),
    ("S14", "QUEENS",        "Queens Blvd Woodside",        "61-15 QUEENS BOULEVARD",           "11377", 40.743021, -73.905034),
    ("S15", "QUEENS",        "Queens Blvd Forest Hills",    "108-20 QUEENS BOULEVARD",          "11375", 40.721534, -73.844012),
    ("S16", "QUEENS",        "Flushing Main Street",        "37-02 MAIN STREET",                "11354", 40.760011, -73.830023),
    ("S17", "QUEENS",        "Jamaica Avenue",              "160-02 JAMAICA AVENUE",            "11432", 40.704034, -73.798012),
    ("S18", "STATEN ISLAND", "St George Ferry",             "1 BAY STREET",                     "10301", 40.643045, -74.074012),
    ("S19", "STATEN ISLAND", "Staten Island Mall",          "2655 RICHMOND AVENUE",             "10314", 40.581034, -74.166023),
    ("S20", "BRONX",         "Bronx River Parkway (planned)", None,                              None,    40.841000, -73.873000),
    ("S21", "MANHATTAN",     "Harlem River Drive",          "2300 HARLEM RIVER DRIVE",          "10035", 40.802011, -73.931045),
    ("S22", "MANHATTAN",     "Roosevelt Island",            "500 MAIN STREET",                  "10044", 40.761500, -73.950000),
    ("S23", "BRONX",         "Pelham Bay (decommissioned)", "3000 BRUCKNER BOULEVARD",          "10461", 40.845023, -73.828011),
    ("S24", "MANHATTAN",     "Times Square 7th Avenue",     "729 7TH AVENUE",                   "10019", 40.760512, -73.983012),
]

# ---------------------------------------------------------------------------
# Source 1: Network Inventory (CSV). System of record for "what equipment is
# installed where". Hand-typed by many people over many years -> messy text.
# ---------------------------------------------------------------------------
INV_COLS = ["site_id", "site_name", "street_address", "city", "state", "zip_code",
            "latitude", "longitude", "equipment_vendor", "equipment_model",
            "site_status", "last_updated"]

# (row..., true_site, edge case this record tests)
INVENTORY = [
    (["NY-BRONX-017", "BRONX GRAND CONCOURSE", "1250 Grand Concourse", "Bronx", "NY", "10456", "40.834712", "-73.916521", "Ericsson", "AIR 6449", "ACTIVE", "2026-09-12"], "S01", "Primary record for the running example site"),
    (["NY-BX-0017", "Bronx - Grand Concourse (legacy)", "1250 GRAND CONCOURSE", "BRONX", "NY", "10456-2201", "40.8347", "-73.9165", "Ericsson", "AIR 3246", "ACTIVE", "2023-02-01"], "S01", "Entity duplicate INSIDE one source: legacy ID from an old migration, ZIP+4, 4-decimal coordinates, stale equipment"),
    (["NY-BRONX-018", "Grand Concourse South", "1100 Grand Concourse", "Bronx", "NY", "10456", "40.829874", "-73.919612", "Nokia", "AirScale", "ACTIVE", "2026-08-30"], "S02", "Hard negative: same street, same ZIP, ~560 m from S01, different site"),
    (["NY-BRONX-022", "Fordham / Webster", "2501 Webster Avenue", "Bronx", "NY", "10458", "40.861023", "-73.889015", "Samsung", "MT6402", "ACTIVE", "2026-07-19"], "S03", "Punctuation in name; 'Avenue' spelled out"),
    (["NY-BRONX-031", "Hunts Point Market", "890 Hunts Point Ave.", "Bronx", "NY", "10474", "40.813044", "-73.887120", "Ericsson", "AIR 6449", "ACTIVE", "2026-06-02"], "S04", "Trailing period in 'Ave.'"),
    (["NY-BRONX-009", "Yankee Stadium", "1 E 161st St", "Bronx", "NY", "10451", "40.829643", "-73.926175", "Ericsson", "AIR 6488", "ACTIVE", "2026-09-01"], "S05", "Directional abbreviation 'E'"),
    (["NY-MANH-004", "Times Square Broadway", "1600 Broadway", "New York", "NY", "10019", "40.761245", "-73.983711", "Ericsson", "AIR 6488", "ACTIVE", "2026-09-15"], "S06", "Same street address as S10 in Brooklyn"),
    (["NY-MANH-006", "Times Sq - 7th Ave", "729 7th Ave", "New York", "NY", "10019", "40.760512", "-73.983012", "Ericsson", "AIR 6449", "ACTIVE", "2026-05-11"], "S24", "Hard negative: ~100 m from S06, same ZIP, similar name 'Times Sq'"),
    (["NY-MANH-011", "Columbia Univ Broadway", "2880 Broadway", "New York", "NY", "10025", "40.805812", "-73.964533", "Nokia", "AirScale", "ACTIVE", "2026-04-22"], "S07", "Name differs from RF ('Morningside Hts')"),
    (["NY-MANH-019", "  Harlem 125th St ", "125 W 125th St", "New York", "NY", "10027", "40.809410", "-73.948012", "Samsung", "MT6402", "ACTIVE", "09/03/2026"], "S08", "Leading/trailing spaces; US date format"),
    (["NY-MANH-019", "  Harlem 125th St ", "125 W 125th St", "New York", "NY", "10027", "40.809410", "-73.948012", "Samsung", "MT6402", "ACTIVE", "09/03/2026"], "S08", "EXACT duplicate row (file loaded twice)"),
    (["NY-MANH-027", "South St Seaport", "200 Water Street", "New York", "NY", "10038", "40.706712", "-74.004533", "Ericsson", "AIR 3246", "MAINTENANCE", "2026-10-01"], "S09", "RF coordinates for this site are ~190 m off"),
    (["NY-MANH-033", "Harlem River Drive", "2300 Harlem River Dr", "New York", "NY", "10035", "40.802011", "-73.931045", "Ericsson", "AIR 3246", "ACTIVE", "2025-12-12"], "S21", "Exists ONLY in inventory (no RF plan, no work orders)"),
    (["NY-BKLYN-002", "Bushwick Broadway", "1600 Broadway", "Brooklyn", "NY", "11207", "40.689211", "-73.918734", "Nokia", "AirScale", "ACTIVE", "2026-03-03"], "S10", "Same street address as S06 but different borough: address alone is NOT a key"),
    (["NY-BKLYN-009", "Atlantic Ave Boerum Hill", "445 Atlantic Ave", "Brooklyn", "NY", "11217", "", "", "Ericsson", "AIR 6449", "ACTIVE", "2026-02-14"], "S11", "Missing coordinates (filled from RF during survivorship)"),
    (["NY-BKLYN-014", "DUMBO Main Street", "1 Main St", "Brooklyn", "NY", "11201", "40.703312", "-73.990321", "Samsung", "MT6402", "ACTIVE", "2026-08-08"], "S12", "'Main St' also exists in Flushing and Roosevelt Island"),
    (["NY-BKLYN-020", "Coney Island Ocean Pkwy", "2900 Ocean Pkwy", "Brooklyn", "NY", "11235", "-73.969011", "40.579023", "Nokia", "AirScale", "ACTIVE", "2026-01-30"], "S13", "Latitude and longitude SWAPPED"),
    (["NY-QNS-003", "Queens Blvd - Woodside", "61-15 Queens Blvd", "Woodside", "NY", "11377", "40.743021", "-73.905034", "Ericsson", "AIR 6449", "ACTIVE", "2026-07-07"], "S14", "Queens hyphenated house number; city is a neighborhood, not the borough"),
    (["NY-QNS-008", "Queens Blvd - Forest Hills", "108-20 Queens Boulevard", "Forest Hills", "NY", "11375", "40.721534", "-73.844012", "Ericsson", "AIR 6449", "ACTIVE", "2026-07-07"], "S15", "Name very similar to S14 (only neighborhood differs)"),
    (["NY-QNS-012", "Flushing Main St", "37-02 Main Street", "Flushing", "NY", "", "40.760011", "-73.830023", "Samsung", "MT6402", "ACTIVE", "2026-06-18"], "S16", "Missing ZIP code"),
    (["NY-QNS-017", "Jamaica Ave", "160-02 Jamaica Ave", "Jamaica", "NY", "11432", "40.704034", "-73.798012", "Nokia", "AirScale", "ACTIVE", "2026-05-05"], "S17", "RF record for this site has no coordinates"),
    (["NY-SI-001", "St George Ferry Terminal", "1 Bay St", "Staten Island", "NY", "10301", "40.643045", "-74.074012", "Ericsson", "AIR 3246", "ACTIVE", "2026-04-04"], "S18", "'St' means Saint in the name but Street in the address"),
    (["NY-SI-006", "Staten Island Mall", "2655 Richmond Ave", "Staten Island", "NY", "10314", "40.581034", "-74.166023", "Nokia", "AirScale", "ACTIVE", "2026-03-21"], "S19", "Clean record"),
    (["NY-BRONX-040", "Pelham Bay Park", "3000 Bruckner Blvd", "Bronx", "NY", "10461", "40.845023", "-73.828011", "Ericsson", "AIR 3246", "DECOMMISSIONED", "2026-10-15"], "S23", "Decommissioned site: still must be resolved, flagged in Gold"),
]

# ---------------------------------------------------------------------------
# Source 2: RF Planning (JSON Lines). Owned by radio engineers. Authoritative for
# coordinates (surveyed) and technology. Has NO street address. Nested structure.
# ---------------------------------------------------------------------------
def rf(rf_id, desc, lat, lon, region, tech, antennas, status, modified):
    return {
        "rf_site_id": rf_id,
        "site_description": desc,
        "location": {"latitude": lat, "longitude": lon, "region": region},
        "technology": tech,
        "antennas": [{"sector": s, "azimuth_deg": az, "height_m": h, "model": m} for s, az, h, m in antennas],
        "planning_status": status,
        "last_modified": modified,
    }

A3 = lambda model, h: [("A", 0, h, model), ("B", 120, h, model), ("C", 240, h, model)]

RF_PLANNING = [
    (rf("RF-10017", "Bronx Grand Concourse", 40.83472, -73.91648, "NYC-BX", ["LTE", "5G-NR"], A3("AIR 6449", 32.0), "ON_AIR", "2026-09-20T10:00:00Z"), "S01", "Running example: name matches inventory, coordinates ~4 m apart"),
    (rf("RF-10018", "Grand Concourse & E 167th", 40.82991, -73.91958, "NYC-BX", ["LTE"], A3("AirScale", 28.5), "ON_AIR", "2026-08-01T12:00:00Z"), "S02", "Described by cross street, not by 'South'"),
    (rf("RF-10022", "Fordham Rd / Webster Av", 40.86105, -73.88898, "NYC-BX", ["LTE", "5G-NR"], A3("MT6402", 30.0), "ON_AIR", "2026-07-01T09:30:00Z"), "S03", "Abbreviations 'Rd', 'Av'"),
    (rf("RF-10031", "Hunts Pt", 40.81301, -73.88715, "NYC-BX", ["LTE"], A3("AIR 6449", 25.0), "ON_AIR", "2026-06-10T08:00:00Z"), "S04", "Very short name; coordinates carry the match"),
    (rf("RF-10009", "Yankee Stad. - River Ave", 40.82969, -73.92612, "NYC-BX", ["LTE", "5G-NR", "5G-mmW"], A3("AIR 6488", 40.0), "ON_AIR", "2026-09-05T15:00:00Z"), "S05", "Truncated word 'Stad.'"),
    (rf("RF-10231", "Midtown 1600 Broadway", 40.76125, -73.98370, "NYC-MN", ["LTE", "5G-NR"], A3("AIR 6488", 45.0), "ON_AIR", "2026-01-10T10:00:00Z"), "S06", "OLD version of RF-10231 (same ID appears twice)"),
    (rf("RF-10231", "Midtown 1600 Broadway Times Sq", 40.76125, -73.98370, "NYC-MN", ["LTE", "5G-NR", "5G-mmW"], A3("AIR 6488", 45.0), "ON_AIR", "2026-08-02T10:00:00Z"), "S06", "NEW version of RF-10231: keep the latest by last_modified"),
    (rf("RF-10232", "Times Sq 7th Av", 40.76049, -73.98305, "NYC-MN", ["LTE", "5G-NR"], A3("AIR 6449", 38.0), "ON_AIR", "2026-05-15T10:00:00Z"), "S24", "Hard negative vs S06 (~100 m)"),
    (rf("RF-10240", "Morningside Hts Broadway", 40.80585, -73.96462, "NYC-MN", ["LTE"], A3("AirScale", 30.0), "ON_AIR", "2026-04-25T10:00:00Z"), "S07", "Different neighborhood name than inventory"),
    (rf("RF-10245", "Harlem 125", 40.80945, -73.94797, "NYC-MN", ["LTE", "5G-NR"], A3("MT6402", 27.0), "ON_AIR", "2026-09-04T10:00:00Z"), "S08", "Short name"),
    (rf("RF-10251", "Seaport / Water St", 40.70790, -74.00290, "NYC-MN", ["LTE"], A3("AIR 3246", 22.0), "ON_AIR", "2026-10-02T10:00:00Z"), "S09", "Coordinates ~190 m off: distance must be graded, not exact"),
    (rf("RF-20002", "Bushwick Broadway", 40.68918, -73.91877, "NYC-BK", ["LTE"], A3("AirScale", 26.0), "ON_AIR", "2026-03-05T10:00:00Z"), "S10", "Name identical to inventory"),
    (rf("RF-20009", "Boerum Hill Atlantic", 40.68652, -73.98103, "NYC-BK", ["LTE", "5G-NR"], A3("AIR 6449", 29.0), "ON_AIR", "2026-02-15T10:00:00Z"), "S11", "Word order differs from inventory: token similarity helps"),
    (rf("RF-20014", "Dumbo", 40.70335, -73.99028, "NYC-BK", ["LTE", "5G-NR"], A3("MT6402", 24.0), "ON_AIR", "2026-08-09T10:00:00Z"), "S12", "Mixed case 'Dumbo' vs 'DUMBO'"),
    (rf("RF-20020", "Coney Isl Ocean Pkwy", 40.57905, -73.96897, "NYC-BK", ["LTE"], A3("AirScale", 20.0), "ON_AIR", "2026-01-31T10:00:00Z"), "S13", "Correct coordinates (inventory has them swapped)"),
    (rf("RF-30003", "Woodside Queens Blvd", 40.74300, -73.90507, "NYC-QN", ["LTE", "5G-NR"], A3("AIR 6449", 31.0), "ON_AIR", "2026-07-08T10:00:00Z"), "S14", "Word order differs"),
    (rf("RF-30008", "Forest Hills Queens Blvd", 40.72150, -73.84398, "NYC-QN", ["LTE", "5G-NR"], A3("AIR 6449", 31.0), "ON_AIR", "2026-07-08T10:00:00Z"), "S15", "Similar to RF-30003 except neighborhood"),
    (rf("RF-30012", "Flushing", 40.76004, -73.83000, "NYC-QN", ["LTE"], A3("MT6402", 26.0), "ON_AIR", "2026-06-19T10:00:00Z"), "S16", "One-word name"),
    (rf("RF-30017", "Jamaica Center", None, None, "NYC-QN", ["LTE"], A3("AirScale", 26.0), "ON_AIR", "2026-05-06T10:00:00Z"), "S17", "NO coordinates: only weak name evidence -> likely manual review"),
    (rf("RF-30020", "Roosevelt Island", 40.76150, -73.95000, "NYC-MN", ["LTE"], A3("AIR 6449", 35.0), "ON_AIR", "2026-04-11T10:00:00Z"), "S22", "Site missing from inventory (data-quality gap)"),
    (rf("RF-40001", "St. George", 40.64301, -74.07398, "NYC-SI", ["LTE"], A3("AIR 3246", 23.0), "ON_AIR", "2026-04-05T10:00:00Z"), "S18", "'St.' = Saint"),
    (rf("RF-40006", "SI Mall", 40.58099, -74.16599, "NYC-SI", ["LTE", "5G-NR"], A3("AirScale", 27.0), "ON_AIR", "2026-03-22T10:00:00Z"), "S19", "Acronym 'SI' = Staten Island"),
    (rf("RF-10050", "Bronx River Pkwy (planned)", 40.84100, -73.87300, "NYC-BX", ["5G-NR"], [], "PLANNED", "2026-10-20T10:00:00Z"), "S20", "Planned, not built: exists ONLY in RF; no antennas yet"),
    (rf("RF-10040", "Pelham Bay", 40.84505, -73.82797, "NYC-BX", ["LTE"], A3("AIR 3246", 25.0), "RETIRED", "2026-10-16T10:00:00Z"), "S23", "Retired site"),
]

# ---------------------------------------------------------------------------
# Source 3: Field Operations (Parquet). Work orders typed by technicians on a
# phone. One site has MANY work orders. Address is one free-text string.
# No coordinates. Most recent truth about equipment actually on the tower.
# ---------------------------------------------------------------------------
FO_SCHEMA = pa.schema([
    ("work_order_id", pa.string()),
    ("site_description", pa.string()),
    ("technician_address", pa.string()),
    ("work_type", pa.string()),
    ("maintenance_status", pa.string()),
    ("equipment_info", pa.string()),
    ("technician_id", pa.string()),
    ("opened_at", pa.timestamp("us")),
    ("closed_at", pa.timestamp("us")),
    ("record_updated_at", pa.timestamp("us")),
])

def ts(s):
    return datetime.fromisoformat(s) if s else None

def wo(wid, desc, addr, wtype, status, equip, tech, opened, closed, updated):
    return {"work_order_id": wid, "site_description": desc, "technician_address": addr,
            "work_type": wtype, "maintenance_status": status, "equipment_info": equip,
            "technician_id": tech, "opened_at": ts(opened), "closed_at": ts(closed),
            "record_updated_at": ts(updated)}

FIELD_OPS = [
    (wo("WO-55001", "Bx Grand Concourse Site", "1250 Grand Concourse, Bronx, NY 10456", "PREVENTIVE", "COMPLETED", "Ericsson AIR 6449", "T-104", "2026-03-02 09:15", "2026-03-02 13:40", "2026-03-02 13:41"), "S01", "Running example: 'Bx' abbreviation, 'Site' noise word"),
    (wo("WO-55002", "GRAND CONCOURSE - BRONX", "1250 GRAND CONCRS BX NY 10456", "EQUIPMENT_SWAP", "COMPLETED", "Nokia AirScale (replaced Ericsson AIR 6449)", "T-221", "2026-10-05 08:00", "2026-10-06 16:20", "2026-10-06 16:25"), "S01", "CONFLICT: equipment swapped to Nokia AFTER inventory was last updated; misspelled 'CONCRS'"),
    (wo("WO-55028", "Concourse", "Grand Concourse, Bronx NY", "CORRECTIVE", "COMPLETED", "Ericsson AIR 6449", "T-104", "2026-05-14 10:00", "2026-05-14 12:00", "2026-05-14 12:05"), "S01", "AMBIGUOUS: no house number, no ZIP. Could be S01 or S02 -> manual review"),
    (wo("WO-55003", "Grand Concourse S.", "1100 Grand Concourse Bronx NY 10456", "CORRECTIVE", "COMPLETED", "Nokia AirScale", "T-221", "2026-08-11 07:30", "2026-08-11 11:00", "2026-08-11 11:02"), "S02", "Hard negative vs S01: only the house number differs"),
    (wo("WO-55004", "Fordham Webster Av", "2501 Webster Av, Bronx NY 10458", "CORRECTIVE", "OPEN", "Samsung MT6402", "T-310", "2026-10-20 09:00", None, "2026-10-20 09:01"), "S03", "Same work order sent twice: this is the OLD status"),
    (wo("WO-55004", "Fordham Webster Av", "2501 Webster Av, Bronx NY 10458", "CORRECTIVE", "COMPLETED", "Samsung MT6402", "T-310", "2026-10-20 09:00", "2026-10-22 15:30", "2026-10-22 15:31"), "S03", "Same work order, NEW status: keep latest by record_updated_at"),
    (wo("WO-55030", "Fordham", "2501 Webster Ave Bronx 10458", "PREVENTIVE", "COMPLETED", "Samsung MT6402", "T-310", "2026-02-10 09:00", "2026-02-10 10:30", "2026-02-10 10:31"), "S03", "Missing state 'NY'"),
    (wo("WO-55005", "Hunts Point Mkt", "890 Hunts Pt Ave, Bronx, NY 10474", "PREVENTIVE", "COMPLETED", "ERICSSON AIR6449", "T-104", "2026-06-01 08:00", "2026-06-01 09:45", "2026-06-01 09:46"), "S04", "'Pt' = Point; 'AIR6449' without space"),
    (wo("WO-55006", "Yankee Stadium", "1 East 161st Street, Bronx NY 10451", "INSTALL", "COMPLETED", "Ericsson AIR 6488 + mmW radio", "T-415", "2026-08-28 06:00", "2026-08-29 18:00", "2026-08-29 18:05"), "S05", "'East' spelled out vs 'E' in inventory"),
    (wo("WO-55007", "Yankee Stad", "1 E 161 St, Bronx, NY", "CORRECTIVE", "IN_PROGRESS", "Ericsson AIR 6488", "T-415", "2026-10-28 13:00", None, "2026-10-28 13:05"), "S05", "No ZIP; '161' without 'st' suffix"),
    (wo("WO-55008", "Times Square Bway rooftop", "1600 Bway, New York NY 10019", "PREVENTIVE", "COMPLETED", "Ericsson AIR 6488", "T-502", "2026-09-09 22:00", "2026-09-10 02:00", "2026-09-10 02:03"), "S06", "'Bway' = Broadway"),
    (wo("WO-55009", "Times Sq 7th Ave", "729 Seventh Avenue, New York, NY 10019", "CORRECTIVE", "COMPLETED", "Ericsson", "T-502", "2026-07-17 23:00", "2026-07-18 01:00", "2026-07-18 01:02"), "S24", "'Seventh' vs '7th'"),
    (wo("WO-55010", "Columbia Broadway", "2880 Broadway, New York, NY 10025", "PREVENTIVE", "COMPLETED", "Nokia AirScale", "T-502", "2026-04-20 08:00", "2026-04-20 10:00", "2026-04-20 10:01"), "S07", "Clean"),
    (wo("WO-55011", "Harlem 125th", "125 West 125th Street, NY NY 10027", "PREVENTIVE", "COMPLETED", "Samsung", "T-310", "2026-03-15 09:00", "2026-03-15 11:00", "2026-03-15 11:01"), "S08", "'NY NY' instead of 'New York'"),
    (wo("WO-55012", "Harlem 125th", "125 W 125th St Ste 4, New York NY 10027", "CORRECTIVE", "OPEN", "Samsung MT6402", "T-310", "2026-10-30 08:00", None, "2026-10-30 08:02"), "S08", "Unit 'Ste 4' must be stripped"),
    (wo("WO-55013", "Seaport", "200 Water St, New York NY 10038", "CORRECTIVE", "IN_PROGRESS", "Ericsson AIR 3246", "T-502", "2026-10-29 07:00", None, "2026-10-29 07:04"), "S09", "Clean address"),
    (wo("WO-55014", "Broadway Bushwick", "1600 Broadway, Brooklyn, NY 11207", "PREVENTIVE", "COMPLETED", "Nokia AirScale", "T-608", "2026-03-04 09:00", "2026-03-04 12:00", "2026-03-04 12:01"), "S10", "Address identical to S06 except city/ZIP"),
    (wo("WO-55015", "Atlantic Ave", "445 Atlantic Avenue, Brooklyn NY 11217", "CORRECTIVE", "COMPLETED", "Ericsson AIR 6449", "T-608", "2026-02-16 10:00", "2026-02-16 14:00", "2026-02-16 14:01"), "S11", "Clean"),
    (wo("WO-55016", "DUMBO", "1 Main Street, Brooklyn, NY 11201", "PREVENTIVE", "COMPLETED", "Samsung MT6402", "T-608", "2026-08-07 08:00", "2026-08-07 09:00", "2026-08-07 09:01"), "S12", "'1 Main Street' - compare with Roosevelt Island '500 Main St'"),
    (wo("WO-55017", "Coney Island", "2900 Ocean Parkway, Brooklyn NY 11235", "PREVENTIVE", "COMPLETED", "Nokia", "T-608", "2026-01-29 08:00", "2026-01-29 10:00", "2026-01-29 10:01"), "S13", "'Parkway' vs 'Pkwy'"),
    (wo("WO-55018", "Woodside", "61-15 Queens Blvd Fl 3, Woodside NY 11377", "PREVENTIVE", "COMPLETED", "Ericsson AIR 6449", "T-701", "2026-07-06 08:00", "2026-07-06 12:00", "2026-07-06 12:01"), "S14", "Floor 'Fl 3' must be stripped"),
    (wo("WO-55020", "Queens Blvd site", "61 15 Queens Boulevard, Queens NY 11377", "CORRECTIVE", "OPEN", "Ericsson", "T-701", "2026-10-31 08:00", None, "2026-10-31 08:01"), "S14", "Hyphen lost ('61 15'); vague description matches S14 AND S15"),
    (wo("WO-55019", "Forest Hills", "108-20 Queens Blvd, Forest Hills, NY 11375", "CORRECTIVE", "COMPLETED", "Ericsson AIR 6449", "T-701", "2026-07-02 08:00", "2026-07-02 11:00", "2026-07-02 11:01"), "S15", "Clean"),
    (wo("WO-55021", "Flushing Main", "37-02 Main St, Flushing, NY 11354", "PREVENTIVE", "COMPLETED", "Samsung MT6402", "T-701", "2026-06-17 08:00", "2026-06-17 10:00", "2026-06-17 10:01"), "S16", "Has the ZIP that inventory is missing"),
    (wo("WO-55022", "Jamaica Ave", "160-02 Jamaica Avenue, Jamaica NY 11432", "PREVENTIVE", "COMPLETED", "Nokia AirScale", "T-701", "2026-05-04 08:00", "2026-05-04 10:00", "2026-05-04 10:01"), "S17", "Clean"),
    (wo("WO-55023", "St George", "1 Bay Street, Staten Island, NY 10301", "PREVENTIVE", "COMPLETED", "Ericsson AIR 3246", "T-808", "2026-04-03 08:00", "2026-04-03 09:30", "2026-04-03 09:31"), "S18", "Clean"),
    (wo("WO-55024", "SI Mall", "2655 Richmond Avenue, Staten Is NY 10314", "CORRECTIVE", "COMPLETED", "Nokia AirScale", "T-808", "2026-03-20 08:00", "2026-03-20 12:00", "2026-03-20 12:01"), "S19", "'Staten Is' abbreviation"),
    (wo("WO-55025", "Pelham Bay decom", "3000 Bruckner Blvd, Bronx NY 10461", "DECOMMISSION", "COMPLETED", "Ericsson AIR 3246 removed", "T-104", "2026-10-14 08:00", "2026-10-15 17:00", "2026-10-15 17:01"), "S23", "Decommission work order"),
    (wo("WO-55026", "Roosevelt Island Tram", "500 Main St, Roosevelt Island, NY 10044", "PREVENTIVE", "COMPLETED", "Ericsson AIR 6449", "T-502", "2026-04-10 08:00", "2026-04-10 10:00", "2026-04-10 10:01"), "S22", "Site NOT in inventory; links only to RF by name"),
    (wo("WO-55027", "Roosevelt Isl", "500 Main Street, New York NY 10044", "CORRECTIVE", "OPEN", "Ericsson", "T-502", "2026-10-25 08:00", None, "2026-10-25 08:01"), "S22", "Same site, city written as 'New York'"),
]


def write_inventory():
    out = RAW / "network_inventory" / "network_inventory.csv"
    with out.open("w", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(INV_COLS)
        for row, _, _ in INVENTORY:
            w.writerow(row)
    return out


def write_rf():
    out = RAW / "rf_planning" / "rf_planning.json"
    with out.open("w") as f:
        for rec, _, _ in RF_PLANNING:
            f.write(json.dumps(rec) + "\n")   # JSON Lines: one JSON object per line
    return out


def write_field_ops():
    out = RAW / "field_operations" / "field_operations.parquet"
    table = pa.Table.from_pylist([rec for rec, _, _ in FIELD_OPS], schema=FO_SCHEMA)
    pq.write_table(table, out)
    return out


def write_truth():
    with (TRUTH / "true_sites.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["true_site_key", "borough", "true_name", "true_address", "zip_code", "latitude", "longitude"])
        w.writerows(TRUE_SITES)

    rows, seen = [], set()
    for system, data, id_of in [
        ("NETWORK_INVENTORY", INVENTORY, lambda r: r[0]),
        ("RF_PLANNING", RF_PLANNING, lambda r: r["rf_site_id"]),
        ("FIELD_OPERATIONS", FIELD_OPS, lambda r: r["work_order_id"]),
    ]:
        for rec, site, lesson in data:
            rows.append([system, id_of(rec), site, lesson])
            seen.add(site)
    with (TRUTH / "ground_truth.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["source_system", "source_record_id", "true_site_key", "lesson"])
        w.writerows(rows)
    assert seen <= {s[0] for s in TRUE_SITES}


if __name__ == "__main__":
    for p in (RAW / "network_inventory", RAW / "rf_planning", RAW / "field_operations", TRUTH):
        p.mkdir(parents=True, exist_ok=True)
    print("wrote", write_inventory().relative_to(ROOT), f"({len(INVENTORY)} rows)")
    print("wrote", write_rf().relative_to(ROOT), f"({len(RF_PLANNING)} rows)")
    print("wrote", write_field_ops().relative_to(ROOT), f"({len(FIELD_OPS)} rows)")
    write_truth()
    print("wrote data/ground_truth/true_sites.csv and ground_truth.csv")
