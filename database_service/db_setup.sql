-- will keep historical record of all sensor data datapoints
-- will be an append-mode table, meaning all records will be kept and the table will get continuously larger
CREATE TABLE IF NOT EXISTS airquality_raw (
    record_id SERIAL PRIMARY KEY,
    station_id INT,
    pollutant_id INT,
    pollutant_name TEXT,
    unit TEXT,
    value FLOAT,
    timestamp_start TIMESTAMP,
    timestamp_end TIMESTAMP,
    ingestion_timestamp TIMESTAMP
);

-- will contain aggregated sensor data
-- will be update-mode table, meaning it will be overwritten as soon as new data are available
CREATE TABLE IF NOT EXISTS airquality_aggregated (
    pollutant_id INT,
    pollutant_name TEXT,
    unit TEXT,
    avg_value FLOAT,
    min_value FLOAT,
    max_value FLOAT,
    last_updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS airquality_metadata (
    id INT,
    code TEXT,
    symbol TEXT,
    unit TEXT,
    name TEXT
);

COPY airquality_metadata (id, code, symbol, unit, name)
FROM '/metadata/components_metadata.csv'
DELIMITER ','
CSV HEADER;