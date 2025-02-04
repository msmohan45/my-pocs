-- CREATE TABLE IF NOT EXISTS events (
--     id TEXT,
--     name TEXT,
--     value TEXT,
--     timestamp TIMESTAMP
-- );

CREATE TABLE IF NOT EXISTS events (
    event_timestamp TIMESTAMP,      -- Milliseconds timestamp
    start_time BIGINT,           -- Microseconds timestamp
    end_time BIGINT,             -- Microseconds timestamp
    batch_no FLOAT,              -- Batch number
    lead_oxide INTEGER,          -- Lead Oxide value
    density FLOAT,               -- Density measurement
    pentration FLOAT,            -- Pentration measurement
    mixer_type TEXT,             -- Mixer type (Auto, Manual, Semi-Auto)
    source_event_time BIGINT,    -- Source event timestamp in milliseconds
    database TEXT,               -- Database name
    schema TEXT,                 -- Schema name
    table_name TEXT,                  -- Table name
    operation CHAR(1)            -- Operation type (c=Create, u=Update, d=Delete, r=Read)
    --PRIMARY KEY (event_timestamp, table_name)  -- Ensuring uniqueness
);
