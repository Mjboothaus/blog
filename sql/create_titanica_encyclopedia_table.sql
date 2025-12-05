-- Create the titanica_encyclopedia table from passenger_normalized_data
-- Matches the schema and cleaning logic of titanic_kaggle for consistency, excluding parch
-- Designed for semi-manual matching in Google Sheets to identify 8 unique records and patch ages
DROP TABLE IF EXISTS titanica_encyclopedia;

CREATE TABLE titanica_encyclopedia (
    url VARCHAR,
    name VARCHAR,
    pclass INTEGER,
    survival INTEGER,  -- INTEGER to support NULL values
    sex VARCHAR,
    age DOUBLE,
    ticket VARCHAR,  -- VARCHAR to handle mixed formats (e.g., 'PC 17605')
    fare DOUBLE,    -- DOUBLE to align with Kaggle dataset
    cabin VARCHAR,
    embarked VARCHAR,
    boat VARCHAR,
    body INTEGER,
    home_dest VARCHAR,
    extraction_notes VARCHAR,
    surname VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    first4_firstname VARCHAR NOT NULL,
    sex_lower VARCHAR NOT NULL,
    age_int INTEGER NOT NULL,
);

INSERT INTO titanica_encyclopedia
SELECT
    url,
    TRIM(CONCAT_WS(' ',
        NULLIF(title, 'NOT_AVAILABLE'),
        NULLIF(given_name, 'NOT_AVAILABLE'),
        COALESCE(NULLIF(family_name, 'NOT_AVAILABLE'), 'unk')
    )) AS name,
    COALESCE(TRY_CAST(REGEXP_REPLACE(
        COALESCE(NULLIF(pclass, 'NOT_AVAILABLE'), '0'), 
        '[^0-9]', ''
    ) AS INTEGER), 0) AS pclass,
    TRY_CAST(NULLIF(survival, 'NOT_AVAILABLE') AS INTEGER) AS survival,
    sex,
    age,
    CAST(COALESCE(ticket, '') AS VARCHAR) AS ticket,
    TRY_CAST(NULLIF(fare_text, 'NOT_AVAILABLE') AS DOUBLE) AS fare,
    cabin,
    embarked,
    boat,
    TRY_CAST(NULLIF(body_text, 'NOT_AVAILABLE') AS INTEGER) AS body,
    home_dest,
    extraction_notes,
    LOWER(REGEXP_REPLACE(
        COALESCE(NULLIF(family_name, 'NOT_AVAILABLE'), 'unk'),
        '[^a-zA-Z0-9]', '', 'g'
    )) AS surname,
    CASE 
        WHEN REGEXP_SPLIT_TO_ARRAY(
            COALESCE(NULLIF(name, 'NOT_AVAILABLE'), 'unk'), ', '
        )[2] IS NOT NULL 
        THEN LOWER(CASE 
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Mr.%' THEN 'mr'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Mrs.%' THEN 'mrs'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Miss%' THEN 'miss'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Ms%' THEN 'ms'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Dr.%' THEN 'dr'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Capt.%' THEN 'capt'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Master%' THEN 'master'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Rev.%' THEN 'rev'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Col.%' THEN 'col'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Major%' THEN 'major'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Mlle%' THEN 'mlle'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Mme%' THEN 'mme'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Sir%' THEN 'sir'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Lady%' THEN 'lady'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Jonkheer%' THEN 'jonkheer'
            WHEN REGEXP_SPLIT_TO_ARRAY(name, ', ')[2] ILIKE 'Don%' THEN 'don'
            ELSE 'unk'
        END)
        ELSE 'unk'
    END AS title,
    LOWER(REGEXP_REPLACE(
        COALESCE(
            SUBSTR(
                TRIM(SPLIT_PART(
                    REGEXP_SPLIT_TO_ARRAY(
                        COALESCE(NULLIF(name, 'NOT_AVAILABLE'), 'unk'),
                        ', '
                    )[2], ' ', 2
                )),
                1, 4
            ),
            'unk'
        ),
        '[^a-z0-9]', '', 'g'
    )) AS first4_firstname,
    CASE LOWER(sex)
        WHEN 'male' THEN 'm'
        WHEN 'female' THEN 'f'
        ELSE 'u'
    END AS sex_lower,
    COALESCE(CAST(FLOOR(age) AS INTEGER), -1) AS age_int
FROM passenger_normalized_data;