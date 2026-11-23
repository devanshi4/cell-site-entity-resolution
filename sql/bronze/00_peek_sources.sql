-- =====================================================================
-- Bronze profiling: look at the three sources before changing anything.
-- Temporary views only; nothing is written to disk.
-- =====================================================================

-- Source 1: CSV. inferSchema is off so every column stays STRING: Bronze keeps the raw text
-- (a ZIP like 10456-2201 or a 4-decimal coordinate must not be "fixed" on read).
CREATE OR REPLACE TEMPORARY VIEW raw_inventory
USING csv
OPTIONS (path '${RAW_ROOT}/network_inventory', header 'true', inferSchema 'false');

-- Source 2: JSON Lines with nested location and antenna arrays.
CREATE OR REPLACE TEMPORARY VIEW raw_rf
USING json
OPTIONS (path '${RAW_ROOT}/rf_planning');

-- Source 3: Parquet, already typed (timestamps included).
CREATE OR REPLACE TEMPORARY VIEW raw_field_ops
USING parquet
OPTIONS (path '${RAW_ROOT}/field_operations');

-- Schema each source arrives with.
DESCRIBE raw_inventory;
DESCRIBE raw_rf;
DESCRIBE raw_field_ops;

-- Grain check: more rows than distinct IDs means an ID repeats (versions or resends).
SELECT 'NETWORK_INVENTORY' AS source, COUNT(*) AS row_count, COUNT(DISTINCT site_id)       AS distinct_ids FROM raw_inventory
UNION ALL
SELECT 'RF_PLANNING',               COUNT(*),              COUNT(DISTINCT rf_site_id)                     FROM raw_rf
UNION ALL
SELECT 'FIELD_OPERATIONS',          COUNT(*),              COUNT(DISTINCT work_order_id)                  FROM raw_field_ops;

-- Running example: every record mentioning "Concourse" in each source.
SELECT site_id, site_name, street_address, zip_code, latitude, longitude, equipment_model, last_updated
FROM raw_inventory
WHERE lower(site_name) LIKE '%concourse%' OR lower(street_address) LIKE '%concourse%';


SELECT rf_site_id, site_description, location.latitude, location.longitude, location.region,
       technology, size(antennas) AS antenna_count, last_modified
FROM raw_rf
WHERE lower(site_description) LIKE '%concourse%';

SELECT work_order_id, site_description, technician_address, work_type, equipment_info, record_updated_at
FROM raw_field_ops
WHERE lower(site_description) LIKE '%concourse%' OR lower(technician_address) LIKE '%concourse%'
ORDER BY record_updated_at;
