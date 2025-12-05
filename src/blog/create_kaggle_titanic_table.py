"""
Process the Kaggle Titanic competition dataset (train.csv and test.csv) to create a cleaned table
for matching with other Titanic datasets (e.g., Encyclopedia Titanica). The script loads train and test
CSVs, combines them, cleans names (surname, first name, title), standardizes fields, and stores the
result in a DuckDB database. The output is used for semi-manual matching in Excel to identify unique
records and patch missing ages.

Key features:
- Cleans surnames and first names using Unicode normalization and alphanumeric filtering.
- Extracts titles (e.g., Mr, Mrs, Miss) from names.
- Ensures ticket is stored as VARCHAR.
- Sets Survived to NULL for test set data (as it's the target to predict).
- Ensures fare is stored as DOUBLE.
- Aligns DataFrame columns with table schema to prevent insertion errors.
- Outputs to Excel and DuckDB for downstream analysis.
"""

import re
import unicodedata
import duckdb
import pandas as pd

# Configuration variables
DATABASE_PATH = "data/titanica_raw.duckdb"
KAGGLE_TABLE = "titanic_kaggle"
TRAIN_CSV = "data/train.csv"
TEST_CSV = "data/test.csv"
OUTPUT_EXCEL = "data/titanic_kaggle_analysis.xlsx"

TITLES = [
    "mr",
    "mrs",
    "miss",
    "ms",
    "dr",
    "capt",
    "master",
    "rev",
    "col",
    "major",
    "mlle",
    "mme",
    "sir",
    "lady",
    "jonkheer",
    "don",
]


def sex_abbrev(s):
    """Convert sex to abbreviated form: male -> m, female -> f, else u."""
    s = str(s).strip().lower()
    return "m" if s == "male" else "f" if s == "female" else "u"


def clean_text(s):
    """Clean text: lowercase, remove spaces and non-alphanumeric characters."""
    if pd.isna(s):
        return "unk"
    s = str(s).lower().replace(" ", "")
    return re.sub(r"[^a-z0-9]", "", s)


def clean_surname(full_name, substr_len=7):
    """Extract and clean surname: normalize accents, keep alphanumeric, truncate."""
    if pd.isna(full_name):
        return "unk"
    surname = full_name.split(",")[0].strip()
    nfkd_form = unicodedata.normalize("NFKD", surname)
    without_accents = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    alphanumeric_only = re.sub(r"[^a-zA-Z0-9]", "", without_accents)
    return alphanumeric_only.lower()[:substr_len] if alphanumeric_only else "unk"


def extract_title(full_name):
    """Extract title from name (e.g., Mr, Mrs, Miss) or return 'unk'."""
    if pd.isna(full_name):
        return "unk"
    parts = full_name.split(",")
    if len(parts) > 1:
        after_comma = parts[1].strip()
        words = after_comma.split()
        if words:
            title = words[0].replace(".", "").lower()
            if title in TITLES:
                return title
    return "unk"


def extract_actual_firstname(full_name, substr_len=4):
    """Extract first name after titles, clean, and truncate to 4 characters."""
    if pd.isna(full_name):
        return "unk"
    parts = full_name.split(",")
    after_comma = parts[1].strip() if len(parts) > 1 else parts[0].strip()
    words = after_comma.split()
    i = 0
    while i < len(words) and words[i].replace(".", "").lower() in TITLES:
        i += 1
    first_name = words[i] if i < len(words) else "unk"
    cleaned = re.sub(r"[^a-z0-9]", "", first_name.lower())
    return cleaned[:substr_len] if cleaned else "unk"


def make_join_key(row):
    """Create a join key from pclass, sex, first name, surname, and age."""
    return "_".join(
        [
            str(row["pclass"]) if pd.notna(row["pclass"]) else "0",
            row["sex_lower"],
            clean_text(row["first4_firstname"]),
            clean_text(row["surname"]),
            str(row["age_int"]) if pd.notna(row["age_int"]) else "-1",
        ]
    )


# Load and prep dataset
df_train = pd.read_csv(TRAIN_CSV)
df_test = pd.read_csv(TEST_CSV)

# Ensure Survived is NULL for test set (target to predict)
df_test["Survived"] = pd.NA  # Always set to NULL for test set

df_all = pd.concat([df_train, df_test], ignore_index=True, sort=False)
df_all.columns = [c.lower() for c in df_all.columns]

# Clean name fields
df_all["surname"] = df_all["name"].apply(clean_surname)
df_all["title"] = df_all["name"].apply(extract_title)
df_all["sex_lower"] = df_all["sex"].apply(sex_abbrev)
df_all["first4_firstname"] = df_all["name"].apply(extract_actual_firstname)
df_all["age_int"] = df_all["age"].apply(lambda x: int(x) if pd.notna(x) else -1)
df_all["ticket"] = (
    df_all["ticket"].astype(str).replace("nan", "")
)  # Ensure VARCHAR for ticket
df_all["fare"] = df_all["fare"].astype(float)  # Ensure DOUBLE for fare
df_all["survived"] = df_all["survived"].apply(
    lambda x: int(x) if pd.notna(x) else None
)  # Ensure INTEGER or NULL
df_all["join_key"] = df_all.apply(make_join_key, axis=1)

# Reorder columns to match CREATE TABLE schema
df_all = df_all[
    [
        "passengerid",
        "survived",
        "pclass",
        "name",
        "sex",
        "age",
        "sibsp",
        "parch",
        "ticket",
        "fare",
        "cabin",
        "embarked",
        "surname",
        "title",
        "first4_firstname",
        "sex_lower",
        "age_int",
        "join_key",
    ]
]

# Export to Excel
df_all.to_excel(OUTPUT_EXCEL, index=False)
print(f"Data exported to {OUTPUT_EXCEL}")

# Check for duplicates
duplicates = df_all.duplicated(
    subset=["pclass", "sex_lower", "first4_firstname", "surname", "age_int"], keep=False
)

# Store in DuckDB
con = duckdb.connect(DATABASE_PATH)
con.execute(f"DROP TABLE IF EXISTS {KAGGLE_TABLE}")
con.execute(f"""
CREATE TABLE {KAGGLE_TABLE} (
    passengerid INTEGER,
    survived INTEGER,  -- INTEGER to support NULL for test set
    pclass INTEGER,
    name VARCHAR,
    sex VARCHAR,
    age DOUBLE,
    sibsp INTEGER,
    parch INTEGER,
    ticket VARCHAR,
    fare DOUBLE,
    cabin VARCHAR,
    embarked VARCHAR,
    surname VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    first4_firstname VARCHAR NOT NULL,
    sex_lower VARCHAR NOT NULL,
    age_int INTEGER NOT NULL,
    join_key VARCHAR NOT NULL,
    PRIMARY KEY (pclass, sex_lower, first4_firstname, surname, age_int)
);
""")
if duplicates.any():
    print("Duplicates found:")
    print(
        df_all[duplicates].sort_values(
            ["pclass", "sex_lower", "first4_firstname", "surname", "age_int"]
        )
    )
else:
    con.register("df_all", df_all)
    con.execute(f"INSERT INTO {KAGGLE_TABLE} SELECT * FROM df_all")
    con.unregister("df_all")
con.close()
