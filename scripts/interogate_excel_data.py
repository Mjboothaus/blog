from pathlib import Path
import pandas as pd
import duckdb

# Define paths using pathlib
data_dir = Path('data')
train_path = data_dir / 'train.csv'
test_path = data_dir / 'test.csv'
excel_path = data_dir / 'TitanicDatasets_Compared.xlsx'

# Check if files exist
for path in [train_path, test_path, excel_path]:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

# Load Excel file (sheet: titanic_merged, skiprows=1)
print(f"Loading Excel file: {excel_path}, sheet: titanic_merged")
df_merged = pd.read_excel(excel_path, sheet_name='titanic_merged', skiprows=1)

# Display metadata using Pandas
print("\n=== Metadata from Pandas ===")
print("Shape (rows, columns):", df_merged.shape)
print("\nColumn Names and Data Types:")
print(df_merged.dtypes)
print("\nSample Data (first 10 rows):")
print(df_merged.head(10))

# Check for duplicate PassengerId
duplicates = df_merged[df_merged['PassengerId'].duplicated(keep=False)]
print("\nDuplicate PassengerId Check:")
if not duplicates.empty:
    print("Found duplicates:")
    print(duplicates[['PassengerId', 'Name', 'Age_x', 'Age_y', 'Speculation']])
else:
    print("No duplicate PassengerId found.")

# Load into DuckDB
con = duckdb.connect(':memory:')
con.register('titanic_merged', df_merged)

# DuckDB metadata
print("\n=== Metadata from DuckDB ===")
describe_result = con.execute("DESCRIBE titanic_merged").df()
print("Table Description (columns, types, etc.):")
print(describe_result)

row_count = con.execute("SELECT COUNT(*) FROM titanic_merged").fetchone()[0]
print("\nTotal Rows:", row_count)

# Sample from DuckDB
sample_df = con.execute("SELECT * FROM titanic_merged LIMIT 10").df()
print("\nSample Data from DuckDB (first 10 rows):")
print(sample_df)

# Load train.csv and test.csv into DuckDB
df_train = pd.read_csv(train_path)
df_test = pd.read_csv(test_path)

# Combine train and test
df_full_original = pd.concat([df_train, df_test], ignore_index=True)
con.register('full_original', df_full_original)

# Integrity checks
print("\n=== Integrity Comparison ===")
unique_check_merged = con.execute("""
    SELECT COUNT(PassengerId) AS total, COUNT(DISTINCT PassengerId) AS unique_ids
    FROM titanic_merged
""").df()
print("Unique PassengerId Check in titanic_merged:")
print(unique_check_merged)

unique_check_original = con.execute("""
    SELECT COUNT(PassengerId) AS total, COUNT(DISTINCT PassengerId) AS unique_ids
    FROM full_original
""").df()
print("\nUnique PassengerId Check in full_original (train + test):")
print(unique_check_original)

# Compare ages (using Age_x as original, Age_y as corrected)
diff_query = """
    SELECT m.PassengerId, m.Name, o.Age AS Original_Age, m.Age_y AS Corrected_Age,
           ABS(o.Age - m.Age_y) AS Age_Diff
    FROM titanic_merged m
    INNER JOIN full_original o ON m.PassengerId = o.PassengerId
    WHERE o.Age IS NOT NULL AND m.Age_y IS NOT NULL AND ABS(o.Age - m.Age_y) > 0
    ORDER BY Age_Diff DESC
    LIMIT 20
"""
diff_df = con.execute(diff_query).df()
print("\nSample Age Differences (top 20 by diff):")
print(diff_df)

# Summary stats on differences
summary_query = """
    SELECT AVG(ABS(o.Age - m.Age_y)) AS avg_diff,
           MAX(ABS(o.Age - m.Age_y)) AS max_diff,
           COUNT(*) AS count_diffs
    FROM titanic_merged m
    INNER JOIN full_original o ON m.PassengerId = o.PassengerId
    WHERE o.Age IS NOT NULL AND m.Age_y IS NOT NULL AND ABS(o.Age - m.Age_y) > 0
"""
summary_diff = con.execute(summary_query).df()
print("\nSummary of Age Differences:")
print(summary_diff)

# Check KaggleAge consistency with Age_x
kaggle_age_check = con.execute("""
    SELECT COUNT(*) AS count_mismatch
    FROM titanic_merged m
    WHERE m.Age_x != m.KaggleAge AND m.Age_x IS NOT NULL AND m.KaggleAge IS NOT NULL
""").df()
print("\nKaggleAge vs. Age_x Mismatch Check:")
print(kaggle_age_check)