# %% [markdown]
# # Titanic Dataset Veracity Analysis
#
# This notebook compares the Kaggle Titanic dataset with external sources (Wikipedia and TitanicFacts.net) to assess data completeness and accuracy, focusing on passenger age information.

# %% [markdown]
# ## 1. Imports and Setup

# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
from difflib import get_close_matches

sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)

# %% [markdown]
# ## 2. Data Loading


# %%
def load_kaggle_data(train_path="train.csv", test_path="test.csv"):
    """Load and concatenate Kaggle Titanic train and test datasets."""
    train = pd.read_csv(train_path)
    test = pd.read_csv(test_path)
    return pd.concat([train, test], ignore_index=True)


def load_wikipedia_data():
    """Load Titanic passenger tables from Wikipedia."""
    url = "https://en.m.wikipedia.org/wiki/Passengers_of_the_RMS_Titanic"
    return pd.read_html(url, header=0)


def load_titanicfacts_data():
    """Load Titanic passenger tables from TitanicFacts.net."""
    url = "http://www.titanicfacts.net/titanic-passenger-list.html"
    return pd.read_html(url, header=0)


# %%
titanic_kaggle = load_kaggle_data()
wiki_tables = load_wikipedia_data()
facts_tables = load_titanicfacts_data()

# %% [markdown]
# ## 3. Data Exploration

# %%
print("Kaggle data shape:", titanic_kaggle.shape)
display(titanic_kaggle.head())

for i, table in enumerate(facts_tables):
    print(f"Facts table {i} shape: {table.shape}")

# %% [markdown]
# ## 4. Name Parsing Utilities


# %%
def extract_name_parts(name):
    """
    Parse a Titanic passenger name into surname, title, firstname, and othernames.
    Example: "Braund, Mr. Owen Harris"
    """
    pattern = r"^(?P<surname>[^,]+),\s*(?P<title>[\w\.]+)\s*(?P<firstname>[^\(]*)\s*(?:\((?P<othernames>[^)]+)\))?"
    match = re.match(pattern, name)
    if match:
        return (
            match.group("surname"),
            match.group("title"),
            match.group("firstname").strip(),
            match.group("othernames"),
        )
    return None, None, None, None


# Apply to Kaggle data
name_parts = titanic_kaggle["Name"].apply(lambda n: extract_name_parts(n))
titanic_kaggle[["Surname", "Title", "Firstname", "Othernames"]] = pd.DataFrame(
    name_parts.tolist(), index=titanic_kaggle.index
)

# %% [markdown]
# ## 5. Age Cleaning Utilities


# %%
def convert_age(age):
    """
    Convert age strings (possibly in months) to float years.
    """
    if pd.isnull(age):
        return np.nan
    if isinstance(age, (int, float)):
        return float(age)
    age = str(age).strip().lower()
    # e.g., "6m" means 6 months
    m = re.match(r"^(\d+)(m)?$", age)
    if m:
        value = float(m.group(1))
        if m.group(2):  # 'm' for months
            return value / 12.0
        return value
    try:
        return float(age)
    except Exception:
        return np.nan


# %% [markdown]
# ## 6. Prepare TitanicFacts Data

# %%
# Combine all facts tables into one DataFrame
titanic_facts = pd.concat(facts_tables, ignore_index=True)
titanic_facts["Age"] = titanic_facts["Age"].apply(convert_age)

# Some tables have "First Names" and "Surname", others may have "Name"
if "First Names" in titanic_facts.columns and "Surname" in titanic_facts.columns:
    titanic_facts["FullName"] = (
        titanic_facts["Surname"].astype(str)
        + ", "
        + titanic_facts["First Names"].astype(str)
    )
elif "Name" in titanic_facts.columns:
    titanic_facts["FullName"] = titanic_facts["Name"]
else:
    titanic_facts["FullName"] = titanic_facts["Surname"]

# %% [markdown]
# ## 7. Age Distribution Comparison

# %%
sns.histplot(
    titanic_kaggle["Age"].dropna(), bins=30, color="blue", label="Kaggle", kde=False
)
sns.histplot(
    titanic_facts["Age"].dropna(),
    bins=30,
    color="orange",
    label="Facts",
    kde=False,
    alpha=0.6,
)
plt.xlabel("Age")
plt.ylabel("Count")
plt.title("Age Distribution: Kaggle vs TitanicFacts")
plt.legend()
plt.show()

# %% [markdown]
# ## 8. Fuzzy Name Matching (Kaggle to Facts)


# %%
def get_best_match(name, candidates, cutoff=0.85):
    matches = get_close_matches(name, candidates, n=1, cutoff=cutoff)
    return matches[0] if matches else None


# For matching, use Kaggle's "Name" and Facts' "FullName"
kaggle_names = titanic_kaggle["Name"].tolist()
facts_names = titanic_facts["FullName"].tolist()

# For demonstration, match first 10 Kaggle names
for i in range(10):
    kaggle_name = kaggle_names[i]
    match = get_best_match(kaggle_name, facts_names)
    print(f"{kaggle_name} --> {match}")

# %% [markdown]
# ## 9. Merge Datasets on Fuzzy Name Match

# %%
# Build a mapping from Kaggle name to Facts name
name_map = {name: get_best_match(name, facts_names) for name in kaggle_names}
titanic_kaggle["Facts_FullName"] = titanic_kaggle["Name"].map(name_map)

# Merge on matched names
titanic_merged = pd.merge(
    titanic_kaggle,
    titanic_facts,
    left_on="Facts_FullName",
    right_on="FullName",
    suffixes=("_Kaggle", "_Facts"),
    how="left",
)

# %% [markdown]
# ## 10. Age Difference Analysis

# %%
titanic_merged["Age_Kaggle"] = titanic_merged["Age_Kaggle"].apply(convert_age)
titanic_merged["Age_Facts"] = titanic_merged["Age_Facts"].apply(convert_age)
titanic_merged["AgeDiff"] = titanic_merged["Age_Kaggle"] - titanic_merged["Age_Facts"]

age_diff = titanic_merged["AgeDiff"].dropna()
print(age_diff.describe())

sns.histplot(age_diff, bins=30, kde=False)
plt.xlabel("Age Difference (Kaggle - Facts)")
plt.title("Histogram of Age Differences")
plt.show()

# %% [markdown]
# ## 11. Save Merged Data

# %%
titanic_merged.to_csv("titanic_merged.csv", index=False)
print("Merged dataset saved as titanic_merged.csv")

# %% [markdown]
# ## 12. Summary

# - The Kaggle dataset contains fewer passengers and more missing ages than the TitanicFacts data.
# - Most ages are consistent between sources, but some discrepancies exist.
# - Name matching is approximate and may not be perfect due to differences in formatting.
# - The merged dataset can be further analyzed or cleaned as needed.
