-- =====================================================================
-- Lesson 1: Look at the three sources BEFORE changing anything.
-- No tables are created. A TEMPORARY VIEW is only a named query that
-- lives inside this Spark session; nothing is written to disk.
-- =====================================================================

-- Source 1: CSV. A CSV file has no types; every value is text.
-- header 'true'      -> first line holds column names
-- inferSchema 'false'-> keep every column as STRING (Bronze preserves the raw text)
CREATE OR REPLACE TEMPORARY VIEW raw_inventory
USING csv
OPTIONS (path '${RAW_ROOT}/network_inventory', header 'true', inferSchema 'false');

-- Source 2: JSON Lines. Spark scans the file once to discover the nested structure.
CREATE OR REPLACE TEMPORARY VIEW raw_rf
USING json
OPTIONS (path '${RAW_ROOT}/rf_planning');

-- Source 3: Parquet. The schema (with real TIMESTAMP types) is stored inside the file footer.
CREATE OR REPLACE TEMPORARY VIEW raw_field_ops
USING parquet
OPTIONS (path '${RAW_ROOT}/field_operations');

-- What types did each source give us?
DESCRIBE raw_inventory;
DESCRIBE raw_rf;
DESCRIBE raw_field_ops;

-- How many rows, and how many distinct IDs? (row count > distinct IDs means repeated IDs)
SELECT 'NETWORK_INVENTORY' AS source, COUNT(*) AS row_count, COUNT(DISTINCT site_id)       AS distinct_ids FROM raw_inventory
UNION ALL
SELECT 'RF_PLANNING',               COUNT(*),              COUNT(DISTINCT rf_site_id)                     FROM raw_rf
UNION ALL
SELECT 'FIELD_OPERATIONS',          COUNT(*),              COUNT(DISTINCT work_order_id)                  FROM raw_field_ops;

-- The running example: every record that mentions "Concourse", in each source.
SELECT site_id, site_name, street_address, zip_code, latitude, longitude, equipment_model, last_updated
FROM raw_inventory
WHERE lower(site_name) LIKE '%concourse%' OR lower(street_address) LIKE '%concourse%';

-- location.latitude reaches INSIDE the nested struct; size() counts array elements.
SELECT rf_site_id, site_description, location.latitude, location.longitude, location.region,
       technology, size(antennas) AS antenna_count, last_modified
FROM raw_rf
WHERE lower(site_description) LIKE '%concourse%';

SELECT work_order_id, site_description, technician_address, work_type, equipment_info, record_updated_at
FROM raw_field_ops
WHERE lower(site_description) LIKE '%concourse%' OR lower(technician_address) LIKE '%concourse%'
ORDER BY record_updated_at;
