-- will keep historical record of all sensor data datapoints
-- will be an append-mode table, meaning all records will be kept and the table will get continuously larger
CREATE TABLE IF NOT EXISTS airquality_raw (
    record_id SERIAL PRIMARY KEY,
    station_id INT,
    pollutant_id INT,
    pollutant_name TEXT,
    pollutant_symbol TEXT,
    unit TEXT,
    value FLOAT,
    timestamp_start TIMESTAMP,
    timestamp_end TIMESTAMP,
    ingestion_timestamp TIMESTAMP
);

-- will contain aggregated sensor data
CREATE TABLE IF NOT EXISTS airquality_aggregated (
    pollutant_id INT,
    pollutant_name TEXT,
    pollutant_symbol TEXT,
    unit TEXT,
    max_value FLOAT,
    min_value FLOAT,
    avg_value FLOAT,
    last_updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS airquality_metadata (
    id INT,
    code TEXT,
    symbol TEXT,
    unit TEXT,
    name TEXT
);
