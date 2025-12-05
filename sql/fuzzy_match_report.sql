WITH fuzzy_matches AS (
  SELECT
    a.rowid AS all_row,
    b.rowid AS classical_row,
    a.name AS all_name,
    b.name AS classical_name,
    a.pclass AS all_pclass,
    b.pclass AS classical_pclass,
    jaro_similarity(lower(a.name), lower(b.name)) AS name_similarity
  FROM titanic_all a
  LEFT JOIN titanic_classical b ON a.pclass = b.pclass
  WHERE jaro_similarity(lower(a.name), lower(b.name)) > 0.8
),
match_classes AS (
  SELECT *,
    CASE
      WHEN name_similarity >= 0.95 THEN 'MATCHED'
      WHEN name_similarity >= 0.85 THEN 'UNSURE'
      ELSE 'DIFFERENT'
    END AS match_class
  FROM fuzzy_matches
),
all_matched_rows AS (
  SELECT DISTINCT all_row FROM match_classes WHERE match_class != 'DIFFERENT'
),
classical_matched_rows AS (
  SELECT DISTINCT classical_row FROM match_classes WHERE match_class != 'DIFFERENT'
),
extra_in_all AS (
  SELECT rowid AS all_row, name FROM titanic_all
  WHERE rowid NOT IN (SELECT all_row FROM all_matched_rows)
),
missing_in_all AS (
  SELECT rowid AS classical_row, name AS classical_name FROM titanic_classical
  WHERE rowid NOT IN (SELECT classical_row FROM classical_matched_rows)
)
SELECT 
  'MATCHES' AS report_section,
  * 
FROM match_classes
UNION ALL
SELECT 
  'EXTRA_IN_ALL' AS report_section,
  all_row, NULL, name, NULL, NULL, NULL, NULL, NULL
FROM extra_in_all
UNION ALL
SELECT 
  'MISSING_IN_ALL' AS report_section,
  NULL, classical_row, NULL, NULL, classical_name, NULL, NULL, NULL
FROM missing_in_all
ORDER BY report_section, all_row NULLS LAST, classical_row NULLS LAST;
