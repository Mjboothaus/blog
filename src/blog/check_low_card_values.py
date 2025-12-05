import duckdb

con = duckdb.connect("data/titanica_raw.duckdb")

columns_to_check = ["pclass", "sex", "embarked", "survived"]

for col in columns_to_check:
    query = f"""
    SELECT '{col}' AS column_name, source_table, {col}
    FROM (
        SELECT 'titanic_all' AS source_table, {col} FROM titanic_all
        UNION
        SELECT 'titanic_classical' AS source_table, {col} FROM titanic_classical
    )
    GROUP BY column_name, source_table, {col}
    ORDER BY column_name, source_table, {col};
    """
    df = con.execute(query).df()
    print(f"Distinct values for column '{col}':")
    print(df)
    print()
    print("-" * 40)


# columns = ["embarked", "sex", "pclass"]
# tables = ["titanic_all", "titanic_classical"]

# for col in columns:
#     for tbl in tables:
#         query = f"""
#         SELECT '{tbl}' AS table_name, {col} AS category, COUNT(*) AS count
#         FROM {tbl}
#         GROUP BY {col}
#         ORDER BY count DESC;
#         """
#         print(f"Counts of distinct values for {col} in {tbl}:")
#         print(con.execute(query).df())
#         print()

con.close()
