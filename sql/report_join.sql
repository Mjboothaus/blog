-- Total and distinct counts
SELECT 
  'titanic_all' AS table_name,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT join_key) AS distinct_keys
FROM titanic_all

UNION ALL

SELECT 
  'titanic_classical' AS table_name,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT join_key) AS distinct_keys
FROM titanic_classical;

-- Count of matching keys (inner join)
SELECT COUNT(*) AS matched_keys
FROM (
  SELECT DISTINCT join_key FROM titanic_all
  INTERSECT
  SELECT DISTINCT join_key FROM titanic_classical
) AS matched;

-- Keys only in titanic_all
SELECT join_key
FROM (
  SELECT DISTINCT join_key FROM titanic_all
)
EXCEPT
SELECT DISTINCT join_key FROM titanic_classical;

-- Keys only in titanic_classical
SELECT join_key
FROM (
  SELECT DISTINCT join_key FROM titanic_classical
)
EXCEPT
SELECT DISTINCT join_key FROM titanic_all;

-- Duplicate keys in titanic_all
SELECT join_key, COUNT(*) AS count
FROM titanic_all
GROUP BY join_key
HAVING COUNT(*) > 1;

-- Duplicate keys in titanic_classical
SELECT join_key, COUNT(*) AS count
FROM titanic_classical
GROUP BY join_key
HAVING COUNT(*) > 1;
