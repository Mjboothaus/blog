from pathlib import Path
import pandas as pd
import duckdb

# Paths
data_dir = Path('data')
excel_path = data_dir / 'TitanicDatasets_Compared.xlsx'
train_path = data_dir / 'train.csv'
test_path = data_dir / 'test.csv'

# Load merged (left side)
df_merged = pd.read_excel(excel_path, sheet_name='titanic_merged', skiprows=1)
df_merged = df_merged.drop_duplicates(subset='PassengerId', keep='first')  # Handle duplicates as before

# Load original
df_train = pd.read_csv(train_path)
df_test = pd.read_csv(test_path)
df_original = pd.concat([df_train, df_test], ignore_index=True)

# DuckDB setup
con = duckdb.connect(':memory:')
con.register('merged', df_merged)
con.register('original', df_original)

# Full correspondence query (check key columns for mismatches)
correspondence_query = """
    SELECT m.PassengerId, m.Name,
           m.Age_x AS Merged_Age, o.Age AS Original_Age,
           m.Pclass AS Merged_Pclass, o.Pclass AS Original_Pclass,
           m.Sex AS Merged_Sex, o.Sex AS Original_Sex,
           m.SibSp AS Merged_SibSp, o.SibSp AS Original_SibSp,
           m.Parch AS Merged_Parch, o.Parch AS Original_Parch,
           m.Fare AS Merged_Fare, o.Fare AS Original_Fare,
           m.Embarked AS Merged_Embarked, o.Embarked AS Original_Embarked,
           CASE WHEN m.Age_x != o.Age OR m.Pclass != o.Pclass OR m.Sex != o.Sex
                OR m.SibSp != o.SibSp OR m.Parch != o.Parch OR m.Fare != o.Fare
                OR m.Embarked != o.Embarked THEN 'Mismatch' ELSE 'Match' END AS Overall_Match
    FROM merged m
    INNER JOIN original o ON m.PassengerId = o.PassengerId
    ORDER BY m.PassengerId
    LIMIT 50  -- Adjust limit as needed
"""

# Summary of mismatches
summary_query = """
    SELECT COUNT(*) AS Total_Records,
           SUM(CASE WHEN m.Age_x != o.Age THEN 1 ELSE 0 END) AS Age_Mismatches,
           SUM(CASE WHEN m.Pclass != o.Pclass THEN 1 ELSE 0 END) AS Pclass_Mismatches,
           SUM(CASE WHEN m.Sex != o.Sex THEN 1 ELSE 0 END) AS Sex_Mismatches,
           SUM(CASE WHEN m.SibSp != o.SibSp THEN 1 ELSE 0 END) AS SibSp_Mismatches,
           SUM(CASE WHEN m.Parch != o.Parch THEN 1 ELSE 0 END) AS Parch_Mismatches,
           SUM(CASE WHEN m.Fare != o.Fare THEN 1 ELSE 0 END) AS Fare_Mismatches,
           SUM(CASE WHEN m.Embarked != o.Embarked THEN 1 ELSE 0 END) AS Embarked_Mismatches
    FROM merged m
    INNER JOIN original o ON m.PassengerId = o.PassengerId
"""

comparison_df = con.execute(correspondence_query).df()
summary_df = con.execute(summary_query).df()

print("\nSample Correspondence Check (first 50 rows):")
print(comparison_df)
print("\nSummary of Mismatches Across Key Columns:")
print(summary_df)