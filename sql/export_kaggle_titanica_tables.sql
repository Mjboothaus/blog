-- Export titanic_kaggle and titanica_encyclopedia to a single CSV for Google Sheets
-- Combines both tables with a 'source' column for distinction, sorted by reliable keys for semi-manual matching
-- Includes key fields for reconciliation: identifier, pclass, title, sex_lower, surname, first4_firstname, name, age_int, ticket, fare, embarked
-- Excludes sibsp and parch to align with titanica_encyclopedia schema
-- Saves to Google Drive tmp folder for macOS
WITH kaggle_export AS (
  SELECT 
    'titanic_kaggle' AS source,
    passengerid AS identifier,
    pclass,
    title,
    sex_lower,
    surname,
    first4_firstname,
    name,
    age_int,
    ticket,
    fare,
    embarked
  FROM titanic_kaggle
  WHERE surname != 'unk' AND first4_firstname != 'unk'
  ORDER BY pclass, sex_lower, title, surname, first4_firstname, age_int
),
encyclopedia_export AS (
  SELECT 
    'titanica_encyclopedia' AS source,
    name AS identifier,
    pclass,
    title,
    sex_lower,
    surname,
    first4_firstname,
    name,
    age_int,
    ticket,
    fare,
    embarked
  FROM titanica_encyclopedia
  WHERE surname != 'unk' AND first4_firstname != 'unk' AND pclass != 0
  ORDER BY pclass, sex_lower, title, surname, first4_firstname, age_int
)
-- Combine for export
SELECT * FROM kaggle_export
UNION ALL
SELECT * FROM encyclopedia_export
ORDER BY source, pclass, sex_lower, title, surname, first4_firstname, age_int;

-- Export to CSV in Google Drive tmp folder
COPY (
  SELECT * FROM kaggle_export
  UNION ALL
  SELECT * FROM encyclopedia_export
  ORDER BY source, pclass, sex_lower, title, surname, first4_firstname, age_int
) TO '/Users/mjboothaus/Library/CloudStorage/GoogleDrive-mjboothaus@gmail.com/My Drive/tmp/titanic_reconcile.csv' WITH (HEADER, DELIMITER ',');