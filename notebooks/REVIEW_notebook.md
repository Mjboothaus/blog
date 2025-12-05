# Review of the notebook `TitanicNotebok-GitHub.ipynb`

##  Examination of the veracity of the Titanic dataset in Kaggle

# Titanic Dataset Veracity Analysis

This project examines the reliability and completeness of the Titanic passenger dataset from Kaggle by comparing it with alternative data sources, including Wikipedia and TitanicFacts.net. The code performs data cleaning, feature extraction, merging, and statistical analysis to highlight discrepancies and similarities between datasets.

## Features

- **Loads and explores** Kaggle Titanic datasets (train and test splits).
- **Extracts name components** (title, surname, first name, other names) from the "Name" field.
- **Downloads and processes** Titanic passenger lists from Wikipedia and TitanicFacts.net.
- **Cleans and standardizes** age data, converting strings to floats and handling missing values.
- **Compares** the Kaggle dataset to external sources, matching passenger names using fuzzy string matching.
- **Analyzes and visualizes** differences in age data between datasets.
- **Exports** merged and processed datasets for further analysis.


## Usage

1. **Install dependencies:**

```bash
pip install pandas numpy matplotlib seaborn
```

2. **Download the Kaggle Titanic datasets** (`train.csv` and `test.csv`) and place them in the working directory.
3. **Run the notebook/script**. The code will:
    - Load datasets
    - Merge and process data
    - Visualize age distributions
    - Attempt to match passengers across datasets
    - Output merged data and summary statistics

## Notable Code Sections

- **extract_names**: Parses the "Name" field into title, surname, first name, and other names.
- **convert_age**: Converts age values to floats, handling months (e.g., "6m" as 0.5 years).
- **Fuzzy matching**: Uses `difflib.get_close_matches` to align passengers between datasets based on name similarity.
- **Visualization**: Plots age distributions and differences for comparison.


## Limitations

- Name matching is approximate and may not always be accurate due to differences in naming conventions.
- Some data cleaning steps are basic and could be improved for robustness.

---

# Code Review \& Suggested Improvements

Below are actionable suggestions to improve code quality, maintainability, and accuracy.

---

## 1. **General Structure and Readability**

**Issues:**

- The code is written as a linear script with minimal modularization.
- Some functions (e.g., `extract_names`) are brittle and may not handle all name formats.
- Use of magic numbers and unclear variable names (e.g., `return_type` in `extract_names`).
- Unused imports (`difflib` is commented out, but used later).
- Some plotting code is outdated (`sns.distplot` is deprecated).

**Improvements:**

- Modularize code into functions (e.g., data loading, cleaning, feature extraction, matching, visualization).
- Use more robust parsing for names (possibly with regex).
- Replace deprecated plotting functions with current alternatives (`sns.histplot`).
- Add docstrings and comments for clarity.
- Use logging instead of print statements for better traceability.

---

## 2. **Name Extraction**

**Issues:**

- The `extract_names` function is fragile and does not handle all name formats.
- Relies on specific string positions and may fail for edge cases.

**Improvements:**

- Use regular expressions for more reliable parsing.
- Return a dictionary with named fields instead of using `return_type`.

**Example Refactor:**

```python
import re

def extract_names(name):
    # Example: "Braund, Mr. Owen Harris"
    match = re.match(r"^(?P<surname>[^,]+),\s*(?P<title>\w+)\.?\s*(?P<firstname>[^(]+)?(?:\s*\((?P<othernames>[^)]+)\))?", name)
    if match:
        return match.groupdict()
    return {'surname': None, 'title': None, 'firstname': None, 'othernames': None}
```

And then expand DataFrame with:

```python
name_parts = titanic_data_kaggle['Name'].apply(extract_names).apply(pd.Series)
titanic_data_kaggle = pd.concat([titanic_data_kaggle, name_parts], axis=1)
```


---

## 3. **Age Conversion**

**Issues:**

- The `convert_age` function is not robust to all possible age formats.
- Does not handle missing or malformed values gracefully.

**Improvements:**

- Use regex to extract numeric values and handle months/years explicitly.
- Use `pd.to_numeric(..., errors='coerce')` for conversion.

**Example Refactor:**

```python
def convert_age(age):
    if pd.isnull(age):
        return np.nan
    if isinstance(age, str):
        m = re.match(r"(\d+)(m)?", age.lower())
        if m:
            value = float(m.group(1))
            if m.group(2):  # 'm' for months
                return value / 12.0
            return value
    try:
        return float(age)
    except Exception:
        return np.nan
```


---

## 4. **Name Matching**

**Issues:**

- Uses `difflib` but the import is commented out.
- Matching is slow and not vectorized.
- No threshold for match quality.

**Improvements:**

- Import `difflib` at the top.
- Use a threshold for match quality.
- Consider using `fuzzywuzzy` or `rapidfuzz` for better performance and accuracy.
- Vectorize matching where possible.

**Example:**

```python
from difflib import get_close_matches

def match_name(name, candidates, cutoff=0.8):
    matches = get_close_matches(name, candidates, n=1, cutoff=cutoff)
    return matches[0] if matches else None
```


---

## 5. **Visualization**

**Issues:**

- Uses deprecated `sns.distplot`.
- Some plots are not labeled or saved.

**Improvements:**

- Use `sns.histplot` or `sns.kdeplot`.
- Add axis labels, titles, and legends.
- Optionally save figures for reporting.

**Example:**

```python
sns.histplot(titanic_training_data_kaggle["Age"].dropna(), bins=30)
plt.xlim(0, 80)
plt.xlabel("Age")
plt.title("Age Distribution in Titanic Kaggle Training Set")
plt.show()
```


---

## 6. **Data Merging and Output**

**Issues:**

- Merges are performed without checking for key uniqueness.
- Output files are overwritten without warning.

**Improvements:**

- Ensure merge keys are unique or handle duplicates appropriately.
- Add checks before writing output files.

---

## 7. **Code Organization**

**Improvements:**

- Organize code into reusable functions or classes.
- Use a `main()` function or Jupyter notebook cells with clear section headers.
- Add a requirements.txt file for dependencies.

---

## 8. **Error Handling and Logging**

**Improvements:**

- Replace bare `except:` with specific exception handling.
- Use logging for errors and progress updates.

---

## 9. **Documentation**

**Improvements:**

- Add docstrings to all functions.
- Comment complex logic.
- Reference data sources clearly.

---

# Example: Refactored Snippet

Here’s a more robust, modular approach for some sections:

```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
from difflib import get_close_matches

def load_kaggle_data(train_path, test_path):
    train = pd.read_csv(train_path)
    test = pd.read_csv(test_path)
    return pd.concat([train, test], ignore_index=True)

def extract_names(name):
    match = re.match(r"^(?P<surname>[^,]+),\s*(?P<title>\w+)\.?\s*(?P<firstname>[^(]+)?(?:\s*\((?P<othernames>[^)]+)\))?", name)
    if match:
        return match.groupdict()
    return {'surname': None, 'title': None, 'firstname': None, 'othernames': None}

def convert_age(age):
    if pd.isnull(age):
        return np.nan
    if isinstance(age, str):
        m = re.match(r"(\d+)(m)?", age.lower())
        if m:
            value = float(m.group(1))
            if m.group(2):  # months
                return value / 12.0
            return value
    try:
        return float(age)
    except Exception:
        return np.nan

def match_name(name, candidates, cutoff=0.8):
    matches = get_close_matches(name, candidates, n=1, cutoff=cutoff)
    return matches[0] if matches else None

def main():
    sns.set_theme()
    titanic_data_kaggle = load_kaggle_data("train.csv", "test.csv")
    name_parts = titanic_data_kaggle['Name'].apply(extract_names).apply(pd.Series)
    titanic_data_kaggle = pd.concat([titanic_data_kaggle, name_parts], axis=1)
    # Continue with further processing...

if __name__ == "__main__":
    main()
```


---

# Summary

- **README.md** has been provided to explain the project.
- **Code review** identifies key issues and suggests improvements in parsing, matching, visualization, and structure.
- **Refactored snippets** demonstrate more robust, maintainable approaches.