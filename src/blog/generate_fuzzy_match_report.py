import duckdb
import pandas as pd


def execute_sql_from_file(con, filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()
    con.execute(sql)


def generate_fuzzy_match_report(
    db_path="data/titanica_raw.duckdb",
    sql_path="sql/fuzzy_match_report.sql",
    output_path="fuzzy_match_report.csv",
):
    con = duckdb.connect(db_path)

    execute_sql_from_file(con, sql_path)
    df_report = con.execute("SELECT * FROM fuzzy_matches_report;").df()
    df_report.to_csv(output_path, index=False)
    print(f"Report saved to {output_path}")
    con.close()
    return df_report


if __name__ == "__main__":
    report = generate_fuzzy_match_report()
    print(report.head())
