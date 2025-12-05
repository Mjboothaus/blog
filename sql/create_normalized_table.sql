-- This table must be created/replaced externally by your Python parsing logic
-- Here just example schema definition for reference only
-- Actual data insertion done via Python pandas df.to_sql or CREATE OR REPLACE TABLE AS

CREATE TABLE IF NOT EXISTS passenger_normalized_data (
    url VARCHAR,
    title VARCHAR,
    given_name VARCHAR,
    family_name VARCHAR,
    pclass INTEGER,
    survival INTEGER,
    sex VARCHAR,
    age INTEGER,
    age_text VARCHAR,
    ticket VARCHAR,
    fare_text VARCHAR,
    cabin VARCHAR,
    embarked VARCHAR,
    boat VARCHAR,
    body_text VARCHAR,
    home_dest VARCHAR,
    nationality VARCHAR,
    marital_status VARCHAR,
    occupation VARCHAR,
    biography TEXT,
    photo_url VARCHAR,
    extraction_notes VARCHAR
);
