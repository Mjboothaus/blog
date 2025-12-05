"""
Process raw Titanic data (e.g., from Encyclopedia Titanica) to create a cleaned table
for matching with the Kaggle Titanic dataset (titanic_kaggle). The script parses HTML data
from passenger_normalized_data, cleans names (surname, first name, title), standardizes fields,
and stores the result in a DuckDB database. The output is used for semi-manual matching in
Google Sheets to identify unique records (expect 8 in titanica_encyclopedia not in titanic_kaggle)
and patch missing ages.

Key features:
- Parses HTML to extract passenger details (name, class, survival, etc.).
- Cleans surnames and first names using alphanumeric filtering.
- Extracts titles (e.g., Mr, Mrs, Miss) from names, aligning with Kaggle dataset.
- Ensures ticket is stored as VARCHAR, fare as DOUBLE, survival as INTEGER with NULL support.
- Outputs to DuckDB for downstream analysis and Google Sheets export.
"""

import os
import re

import duckdb
import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger

# Configuration variables
DATABASE_PATH = "data/titanica_raw.duckdb"
NORMALIZED_TABLE = "passenger_normalized_data"
ENCYCLOPEDIA_TABLE = "titanica_encyclopedia"
EXTENDED_TABLE = "titanic_extended"
SQL_DIR = "sql"


def execute_sql_from_file(con, filepath):
    """Execute SQL from a file, handling exceptions gracefully."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            sql = f.read().strip()  # Strip whitespace to avoid empty SQL errors
        con.execute(sql)
        logger.info(f"Successfully executed SQL from {filepath}")
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        raise
    except duckdb.Error as e:
        logger.error(f"SQL execution error in {filepath}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error executing SQL from {filepath}: {str(e)}")
        raise


class TitanicaRawTransformer:
    def __init__(
        self,
        db_path=DATABASE_PATH,
        normalized_table=NORMALIZED_TABLE,
        encyclopedia_table=ENCYCLOPEDIA_TABLE,
        extended_table=EXTENDED_TABLE,
        sql_dir=SQL_DIR,
    ):
        self.db_path = db_path
        self.con = duckdb.connect(db_path)
        self.normalized_table = normalized_table
        self.encyclopedia_table = encyclopedia_table
        self.extended_table = extended_table
        self.sql_dir = sql_dir

    def parse_passenger_page(self, raw_html):
        soup = BeautifulSoup(raw_html, "html.parser")
        summary = soup.find("div", id="summary")
        bio_div = (
            soup.find("div", id="biography")
            or soup.find("div", class_="biosection")
            or soup.find("div", class_="biography")
        )
        img_tag = None
        if summary:
            img_tag = summary.find("img")
        if not img_tag and bio_div:
            img_tag = bio_div.find("img")
        result = {
            "title": "NOT_AVAILABLE",
            "given_name": "NOT_AVAILABLE",
            "family_name": "NOT_AVAILABLE",
            "pclass": "NOT_AVAILABLE",
            "survival": "NOT_AVAILABLE",
            "sex": "NOT_AVAILABLE",
            "age": None,
            "age_text": "NOT_AVAILABLE",
            "ticket": "NOT_AVAILABLE",
            "fare_text": "NOT_AVAILABLE",
            "cabin": "NOT_AVAILABLE",
            "embarked": "NOT_AVAILABLE",
            "boat": "NOT_AVAILABLE",
            "body_text": "NOT_AVAILABLE",
            "home_dest": "NOT_AVAILABLE",
            "nationality": "NOT_AVAILABLE",
            "marital_status": "NOT_AVAILABLE",
            "occupation": "NOT_AVAILABLE",
            "biography": None,
            "photo_url": None,
            "extraction_notes": "",
        }
        try:
            if not summary:
                result["extraction_notes"] += "Missing summary section. "
                return result
            # Title, Given name, Family name
            honorific_span = summary.find("span", itemprop="honorificPrefix")
            result["title"] = (
                honorific_span.text.strip() if honorific_span else "NOT_AVAILABLE"
            )
            given_name_span = summary.find("span", itemprop="givenName")
            result["given_name"] = (
                given_name_span.text.strip() if given_name_span else "NOT_AVAILABLE"
            )
            family_name_span = summary.find("span", itemprop="familyName")
            result["family_name"] = (
                family_name_span.text.strip() if family_name_span else "NOT_AVAILABLE"
            )
            # Passenger class
            pcs = {"1st": 1, "2nd": 2, "3rd": 3}
            for k, v in pcs.items():
                if k + " Class Passengers" in str(summary):
                    result["pclass"] = v
                    break
            # Survival flag
            if summary.find("a", href=re.compile(r"titanic-survivors")):
                result["survival"] = 1
            elif summary.find("a", href=re.compile(r"titanic-victims")):
                result["survival"] = 0
            # Sex, Age, Age_text
            age_div = None
            for div in summary.find_all("div"):
                strong = div.find("strong")
                if strong and "Age" in strong.text:
                    age_div = div
                    break
            if age_div:
                age_link = age_div.find("a")
                if age_link and age_link.text:
                    result["age_text"] = age_link.text.strip()
                    age_number = re.search(r"(\d+)", result["age_text"])
                    if age_number:
                        result["age"] = int(age_number.group(1))
                sex_span = age_div.find("span", itemprop="gender")
                if sex_span:
                    sex_text = sex_span.text.strip().lower()
                    if sex_text in ["male", "female"]:
                        result["sex"] = sex_text
            # Ticket & fare_text
            ticket_div = None
            for div in summary.find_all("div"):
                strong = div.find("strong")
                if strong and "Ticket No" in strong.text:
                    ticket_div = div
                    break
            if ticket_div:
                ticket_match = re.search(
                    r"Ticket No\.?\s*([^\s,]+)", ticket_div.text.strip()
                )
                if ticket_match:
                    result["ticket"] = ticket_match.group(1)
                fare_text_match = re.search(
                    r"£[0-9]+(?: [0-9]+s)?(?: [0-9]+d)?", ticket_div.text
                )
                if fare_text_match:
                    result["fare_text"] = fare_text_match.group(0)
            # Cabin
            cabin_div = None
            for div in summary.find_all("div"):
                strong = div.find("strong")
                if strong and "Cabin No" in strong.text:
                    cabin_div = div
                    break
            if cabin_div:
                span = cabin_div.find("span")
                if span:
                    result["cabin"] = span.text.strip()
            # Embarked
            embarked_div = None
            for div in summary.find_all("div"):
                strong = div.find("strong")
                if strong and "Embarked" in strong.text:
                    embarked_div = div
                    break
            if embarked_div:
                a_tag = embarked_div.find("a")
                if a_tag:
                    result["embarked"] = a_tag.text.strip()
            # Boat
            rescued_div = None
            for div in summary.find_all("div"):
                strong = div.find("strong")
                if strong and "Rescued" in strong.text:
                    rescued_div = div
                    break
            if rescued_div:
                a_tag = rescued_div.find("a")
                if a_tag:
                    boat_number = re.search(r"boat\s*(\d+)", a_tag.text.strip(), re.I)
                    if boat_number:
                        result["boat"] = boat_number.group(1)
            # Body text
            body_div = None
            for div in summary.find_all("div"):
                strong = div.find("strong")
                if strong and "Body" in strong.text:
                    body_div = div
                    break
            if body_div:
                result["body_text"] = body_div.get_text(" ", strip=True)
            # Home destination
            last_residence_div = None
            destination_div = None
            home_parts = []
            for div in summary.find_all("div"):
                strong = div.find("strong")
                if strong:
                    if "Last Residence" in strong.text:
                        last_residence_div = div
                    elif "Destination" in strong.text:
                        destination_div = div
            if last_residence_div:
                a_tag = last_residence_div.find("a")
                if a_tag:
                    home_parts.append(a_tag.text.strip())
            if destination_div:
                a_tag = destination_div.find("a")
                if a_tag:
                    home_parts.append(a_tag.text.strip())
            result["home_dest"] = (
                " / ".join(home_parts) if home_parts else "NOT_AVAILABLE"
            )
            # Nationality
            nationality_div = None
            for div in summary.find_all("div"):
                strong = div.find("strong")
                if strong and "Nationality" in strong.text:
                    nationality_div = div
                    break
            if nationality_div:
                span = nationality_div.find("span", itemprop="nationality")
                if span:
                    result["nationality"] = span.text.strip()
            # Marital Status
            marital_div = None
            for div in summary.find_all("div"):
                strong = div.find("strong")
                if strong and "Marital Status" in strong.text:
                    marital_div = div
                    break
            marital_status = "NOT_AVAILABLE"
            if marital_div:
                a_tag = marital_div.find("a")
                if a_tag:
                    marital_status = a_tag.text.strip()
                else:
                    marital_status = (
                        marital_div.get_text(strip=True)
                        .replace("Marital Status", "")
                        .strip()
                    )
            # infer child if missing marital and age <=13
            if (
                marital_status == "NOT_AVAILABLE"
                and result["age"] is not None
                and result["age"] <= 13
            ):
                marital_status = "Child"
            elif marital_status == "NOT_AVAILABLE":
                marital_status = "Unknown"
            result["marital_status"] = marital_status
            # Occupation
            occupation_div = None
            for div in summary.find_all("div"):
                strong = div.find("strong")
                if strong and "Occupation" in strong.text:
                    occupation_div = div
                    break
            if occupation_div:
                span = occupation_div.find("span", itemprop="jobTitle")
                if span:
                    result["occupation"] = span.text.strip()
                else:
                    result["occupation"] = (
                        occupation_div.get_text(strip=True)
                        .replace("Occupation", "")
                        .strip()
                    )
            # Biography
            if bio_div:
                paragraphs = bio_div.find_all("p")
                para_texts = [p.get_text(separator=" ", strip=True) for p in paragraphs]
                bio_text = "\n\n".join(para_texts)
                if len(bio_text) > 5000:
                    bio_text = bio_text[:5000] + " ... [truncated]"
                result["biography"] = bio_text
            else:
                result["biography"] = None
                result["extraction_notes"] += "Biography missing. "
            # Photo URL
            src = None
            if img_tag and img_tag.has_attr("src"):
                src = img_tag["src"]
                if src.startswith("/"):
                    src = "https://www.encyclopedia-titanica.org" + src
                result["photo_url"] = src
        except Exception as e:
            result["extraction_notes"] += f"Error parsing: {str(e)}. "
        return result

    def build_normalized_table(self):
        df_raw = self.con.execute("SELECT url, raw_html FROM titanica_raw").df()
        parsed_rows = []
        for _, row in df_raw.iterrows():
            fields = self.parse_passenger_page(row["raw_html"])
            fields["url"] = row["url"]
            parsed_rows.append(fields)
        df_fields = pd.DataFrame(parsed_rows)
        # Drop existing table if exists
        self.con.execute(f"DROP TABLE IF EXISTS {self.normalized_table}")
        # Create table from DataFrame
        self.con.register("temp_df", df_fields)
        self.con.execute(
            f"CREATE TABLE {self.normalized_table} AS SELECT * FROM temp_df"
        )
        self.con.unregister("temp_df")
        return df_fields

    def create_titanica_encyclopedia_table(self):
        sql_path = os.path.join(self.sql_dir, "create_titanica_encyclopedia_table.sql")
        execute_sql_from_file(self.con, sql_path)

    def build_extended_table(self):
        sql_path = os.path.join(self.sql_dir, "create_extended_table.sql")
        execute_sql_from_file(self.con, sql_path)

    def pretty_inspect_samples(self, table_name, n=5):
        df = self.con.execute(f"SELECT * FROM {table_name} LIMIT {n}").df()
        for idx, row in df.iterrows():
            print(f"\nSample {idx + 1}:")
            for col in df.columns:
                print(
                    f" {col}: {row[col] if row[col] is not None else '[NOT_AVAILABLE]'}"
                )
            print("-" * 40)

    def close(self):
        self.con.close()


if __name__ == "__main__":
    transformer = TitanicaRawTransformer()
    # Build & replace normalized parsed base table
    transformer.build_normalized_table()
    # Build & replace titanica_encyclopedia table
    transformer.create_titanica_encyclopedia_table()
    # Build & replace extended table with bio, photo, etc.
    # transformer.build_extended_table()
    # Inspect sample rows
    print("\n--- Encyclopedia Samples ---")
    transformer.pretty_inspect_samples(transformer.encyclopedia_table, n=22)
    transformer.close()
