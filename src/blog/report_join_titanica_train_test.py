import duckdb


def run_report(db_path: str, sql_path: str):
    conn = duckdb.connect(db_path)
    with open(sql_path, "r") as f:
        sql_script = f.read()

    # Execute all queries in the file
    # Split by semicolon to execute sequentially
    queries = [q.strip() for q in sql_script.split(";") if q.strip()]

    print("Join Report\n-----------")
    for i, q in enumerate(queries):
        print(f"\nQuery {i + 1}:\n{q}")
        try:
            result = conn.execute(q).fetchdf()
            print(result)
        except Exception as e:
            print(f"Error: {e}")

    conn.close()


if __name__ == "__main__":
    db_file = "data/titanica_raw.duckdb"  # Change to your DuckDB file path
    sql_file = "sql/report_join.sql"  # Path to the .sql file with queries
    run_report(db_file, sql_file)
