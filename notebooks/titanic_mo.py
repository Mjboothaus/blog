import marimo

__generated_with = "0.15.2"
app = marimo.App()


@app.cell
def _():
    import re
    from pathlib import Path
    import time

    import duckdb
    import logfire
    import marimo as mo
    import matplotlib.pyplot as plt
    import pandas as pd
    import requests
    import seaborn as sns
    from bs4 import BeautifulSoup
    from fuzzywuzzy import fuzz
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (
        accuracy_score,
        confusion_matrix,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )
    from sklearn.model_selection import (
        StratifiedKFold,
        cross_validate,
        train_test_split,
    )
    from tenacity import retry, stop_after_attempt, wait_exponential
    from xgboost import XGBClassifier
    return (
        BeautifulSoup,
        Path,
        duckdb,
        fuzz,
        logfire,
        pd,
        re,
        requests,
        retry,
        stop_after_attempt,
        time,
        wait_exponential,
    )


@app.cell
def initialize_logfire(logfire):
    """Initialize Logfire for experiment tracking."""

    logfire.configure(local=True)
    print("Logfire configured.")
    return


@app.cell
def setup_paths(Path):
    """Define file paths using pathlib."""
    data_dir = Path("data")
    excel_path = data_dir / "TitanicDatasets_Compared.xlsx"
    train_path = data_dir / "train.csv"
    test_path = data_dir / "test.csv"

    # Check files
    for path in [train_path, test_path, excel_path]:
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

    print("Paths verified:", train_path, test_path, excel_path)
    return excel_path, test_path, train_path


@app.cell
def load_data(duckdb, excel_path, pd, test_path, train_path):
    """Load merged and original datasets into DuckDB."""
    # Load merged data
    df_merged = pd.read_excel(excel_path, sheet_name="titanic_merged", skiprows=1)
    print("Merged Data Shape:", df_merged.shape)
    print("\nMerged Data Columns:", df_merged.columns.tolist())
    print("\nMerged Data Sample (first 5 rows):")
    print(df_merged[["PassengerId", "Name", "Age_x", "Age_y", "Speculation"]].head())

    # Load original datasets
    df_train = pd.read_csv(train_path)
    df_test = pd.read_csv(test_path)
    df_original = pd.concat([df_train, df_test], ignore_index=True)
    print("\nOriginal Data Shape:", df_original.shape)

    # DuckDB setup
    con = duckdb.connect(":memory:")
    con.register("titanic_merged", df_merged)
    con.register("original", df_original)
    return con, df_merged


@app.cell
def check_duplicates(df_merged):
    """Check duplicates, noting James Kelly case."""
    duplicates = df_merged[df_merged["PassengerId"].duplicated(keep=False)]
    print("\nDuplicate PassengerId Check:")
    if not duplicates.empty:
        print("Found duplicates (e.g., two distinct James Kellys):")
        print(
            duplicates[
                [
                    "PassengerId",
                    "Name",
                    "Age_x",
                    "Age_y",
                    "Speculation",
                    "Boarded",
                    "Ticket",
                ]
            ]
        )
    else:
        print("No duplicates found.")

    # Do not remove duplicates to keep both James Kellys
    print(
        "\nKeeping all rows, including duplicates for distinct people. Rows:",
        len(df_merged),
    )
    return


@app.cell
def generate_unique_id(con):
    """Generate unique identifier using name + age + boarded/ticket."""
    con.execute("""
        CREATE TABLE titanic_merged_with_key AS
        SELECT *,
               LOWER(Name) || '_' || CAST(Age_y AS VARCHAR) || '_' || Boarded || '_' || Ticket AS unique_key
        FROM titanic_merged
    """)

    unique_check = con.execute("""
        SELECT COUNT(*) AS total, COUNT(DISTINCT unique_key) AS unique_keys
        FROM titanic_merged_with_key
    """).df()
    print("\nUnique Key Check (name + age + boarded + ticket):")
    print(unique_check)

    # Sample unique keys
    sample_keys = con.execute("""
        SELECT PassengerId, Name, Age_y, Boarded, Ticket, unique_key
        FROM titanic_merged_with_key
        WHERE Name LIKE '%James Kelly%'
        OR PassengerId = 892
    """).df()
    print("\nSample Unique Keys for Duplicates (e.g., James Kelly):")
    print(sample_keys)
    return


@app.cell
def _(con):
    con.sql("SHOW TABLES;")
    return


@app.cell
def align_with_titanica(con):
    """Align with scraped Titanica data using unique key."""
    # Assume all_passengers from scrape_titanica cell
    # For alignment, clean and match on unique_key or fuzzy name + age
    # (Add code from previous verification, assuming scraped data loaded)
    print("Alignment with Titanica data using unique key...")
    # Example query
    alignment_query = """
        SELECT m.PassengerId, m.Name, m.Age_y, m.unique_key AS merged_key,
               t.Name AS titanica_name, t.Age AS titanica_age
        FROM titanic_merged_with_key m
        LEFT JOIN titanica_passengers t ON LOWER(m.Name) = LOWER(t.Name) AND m.Age_y = CAST(t.Age AS DOUBLE)
        WHERE m.Name LIKE '%James Kelly%'
    """
    alignment_df = con.execute(alignment_query).df()
    print("\nAlignment for James Kelly:")
    print(alignment_df)
    return


@app.cell
def scrape_titanica(
    BeautifulSoup,
    con,
    pd,
    re,
    requests,
    retry,
    stop_after_attempt,
    wait_exponential,
):
    """Scrape Encyclopedia Titanica passenger lists."""

    def scrape_titanica_old():
        """Scrape Encyclopedia Titanica passenger lists."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        urls = {
            'first': 'https://www.encyclopedia-titanica.org/titanic-first-class-passengers/',
            'second': 'https://www.encyclopedia-titanica.org/titanic-second-class-passengers/',
            'third': 'https://www.encyclopedia-titanica.org/titanic-third-class-passengers/'
        }

        def clean_name(name):
            if not isinstance(name, str):
                return ''
            name = re.sub(r'\b(Mr\.|Mrs\.|Miss\.|Master\.|Dr\.|Rev\.|Col\.|Major\.|Ms\.|)\b', '', name)
            name = re.sub(r'\s+', ' ', name.strip())
            name = name.encode('ascii', 'ignore').decode('ascii')
            return name.lower()

        @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
        def scrape_passenger_table(url):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table')
                if not table:
                    print(f"No table found on {url}")
                    return pd.DataFrame()
            
                rows = []
                for tr in table.find_all('tr')[1:]:
                    tds = tr.find_all('td')
                    if len(tds) >= 4:
                        name_link = tds[0].find('a')
                        name = name_link.text.strip() if name_link else tds[0].text.strip()
                        individual_url = name_link['href'] if name_link else ''
                        age = tds[1].text.strip()
                        rows.append({
                            'Name': name,
                            'Cleaned_Name': clean_name(name),
                            'Age': age,
                            'Individual_URL': f"https://www.encyclopedia-titanica.org{individual_url}"
                        })
                return pd.DataFrame(rows)
            except Exception as e:
                print(f"Error scraping {url}: {e}")
                return pd.DataFrame()

        all_passengers = pd.DataFrame()
        for class_type, url in urls.items():
            df_class = scrape_passenger_table(url)
            df_class['Class'] = class_type.capitalize()
            all_passengers = pd.concat([all_passengers, df_class], ignore_index=True)
    
        print(f"\nExtracted {len(all_passengers)} passengers from Encyclopedia Titanica:")
        print(all_passengers.head(10))
    
        con.register('titanica_passengers', all_passengers)
        con.execute("CREATE OR REPLACE TABLE titanica_passengers AS SELECT * FROM all_passengers")
        return all_passengers, con
    return (scrape_titanica_old,)


@app.cell
def _(scrape_titanica_old):
    all_passengers = scrape_titanica_old()
    return (all_passengers,)


@app.cell
def _(all_passengers):
    all_passengers
    return


@app.cell
def _(
    BeautifulSoup,
    con,
    pd,
    re,
    requests,
    retry,
    stop_after_attempt,
    time,
    wait_exponential,
):
    def scrape_titanica():
        """Scrape Encyclopedia Titanica passenger lists and individual pages."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        urls = {
            'first': 'https://www.encyclopedia-titanica.org/titanic-first-class-passengers/',
            'second': 'https://www.encyclopedia-titanica.org/titanic-second-class-passengers/',
            'third': 'https://www.encyclopedia-titanica.org/titanic-third-class-passengers/'
        }

        def clean_name(name):
            if not isinstance(name, str):
                return ''
            name = re.sub(r'\b(Mr\.|Mrs\.|Miss\.|Master\.|Dr\.|Rev\.|Col\.|Major\.|Ms\.|)\b', '', name)
            name = re.sub(r'\s+', ' ', name.strip())
            name = name.encode('ascii', 'ignore').decode('ascii')
            return name.lower()

        @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
        def scrape_passenger_table(url):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table')
                if not table:
                    print(f"No table found on {url}")
                    return pd.DataFrame()
            
                rows = []
                for tr in table.find_all('tr')[1:]:
                    tds = tr.find_all('td')
                    if len(tds) >= 4:
                        name_link = tds[0].find('a')
                        name = name_link.text.strip() if name_link else tds[0].text.strip()
                        individual_url = name_link['href'] if name_link else ''
                        age = tds[1].text.strip()
                        rows.append({
                            'Name': name,
                            'Cleaned_Name': clean_name(name),
                            'Age': age,
                            'Individual_URL': f"https://www.encyclopedia-titanica.org{individual_url}",
                            'Class': ''
                        })
                return pd.DataFrame(rows)
            except Exception as e:
                print(f"Error scraping {url}: {e}")
                return pd.DataFrame()

        @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
        def scrape_individual_page(url):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
            
                data = {}
                # Extract metadata fields (e.g., Age, Class, Ticket)
                metadata = soup.find_all('div', class_='meta-item')
                for item in metadata:
                    label = item.find('span', class_='label')
                    value = item.find('span', class_='value')
                    if label and value:
                        key = label.text.strip().lower().replace(' ', '_')
                        data[key] = value.text.strip()
            
                # Extract biographical text
                bio = soup.find('div', class_='biography')
                data['biography'] = bio.text.strip() if bio else ''
            
                return data
            except Exception as e:
                print(f"Error scraping individual page {url}: {e}")
                return {}

        all_passengers = pd.DataFrame()
        for class_type, url in urls.items():
            df_class = scrape_passenger_table(url)
            df_class['Class'] = class_type.capitalize()
            all_passengers = pd.concat([all_passengers, df_class], ignore_index=True)
    
        # Scrape individual pages
        individual_data = []
        for idx, row in all_passengers.iterrows():
            if row['Individual_URL']:
                data = scrape_individual_page(row['Individual_URL'])
                data['Name'] = row['Name']
                data['Cleaned_Name'] = row['Cleaned_Name']
                data['Class'] = row['Class']
                data['Individual_URL'] = row['Individual_URL']
                individual_data.append(data)
                time.sleep(1)  # Avoid rate limiting
            if idx % 100 == 0:
                print(f"Scraped {idx} individual pages...")
    
        df_individual = pd.DataFrame(individual_data)
        print(f"\nExtracted {len(df_individual)} passengers from Encyclopedia Titanica individual pages:")
        print(df_individual.head(10))
    
        con.register('titanica_individual', df_individual)
        con.execute("CREATE OR REPLACE TABLE titanica_individual AS SELECT * FROM df_individual")
        return df_individual, con
    return (scrape_titanica,)


@app.cell
def _(scrape_titanica):
    df_individual, _ = scrape_titanica()
    return


@app.cell
def verify_titanica(clean_name, fuzz, pd):
    """Verify merged data against scraped Titanica data."""

    def verify_titanica(con, df_merged_clean, all_passengers):
        """Verify merged data against scraped Titanica data."""
        df_merged_clean['Cleaned_Name'] = df_merged_clean['Name'].apply(clean_name)
        con.register('titanic_merged_clean', df_merged_clean)
    
        verification_query = """
            SELECT m.PassengerId, m.Name AS Merged_Name, m.Age_y AS Merged_Age, 
                   t.Name AS Titanica_Name, t.Age AS Titanica_Age
            FROM titanic_merged_clean m
            LEFT JOIN titanica_passengers t ON m.Cleaned_Name = t.Cleaned_Name
            WHERE m.Age_y IS NOT NULL AND t.Age IS NOT NULL
            LIMIT 20
        """
        verification_df = con.execute(verification_query).df()
        print("\nVerification Against Encyclopedia Titanica:")
        print(verification_df if not verification_df.empty else "No matches found.")
    
        if verification_df.empty and not all_passengers.empty:
            print("\nTrying fuzzy matching...")
            merged_names = df_merged_clean[['PassengerId', 'Name', 'Cleaned_Name', 'Age_y']].dropna(subset=['Age_y'])
            titanica_names = all_passengers[['Name', 'Cleaned_Name', 'Age']].dropna(subset=['Age'])
        
            matches = []
            for _, m_row in merged_names.iterrows():
                best_score = 0
                best_match = None
                for _, t_row in titanica_names.iterrows():
                    score = fuzz.ratio(m_row['Cleaned_Name'], t_row['Cleaned_Name'])
                    if score > best_score and score > 80:
                        best_score = score
                        best_match = t_row
                if best_match is not None:
                    matches.append({
                        'PassengerId': m_row['PassengerId'],
                        'Merged_Name': m_row['Name'],
                        'Merged_Age': m_row['Age_y'],
                        'Titanica_Name': best_match['Name'],
                        'Titanica_Age': best_match['Age'],
                        'Match_Score': best_score
                    })
        
            fuzzy_matches = pd.DataFrame(matches)
            print("\nFuzzy Matching Results (Top 20):")
            print(fuzzy_matches.head(20))
        return verification_df, fuzzy_matches
    return (verify_titanica,)


@app.cell
def _():
    return


@app.cell
def _(all_passengers, con, df_merged_clean, verify_titanica):
    verification_df, fuzzy_matches = verify_titanica(con, df_merged_clean, all_passengers)
    return


@app.cell
def analyze_ages():
    """Analyze age differences between original and corrected data."""
    # (Same as previous)
    return


@app.cell
def run_experiments():
    """Run modeling experiments."""
    # (Same as previous, but note: since keeping duplicates, adjust if dataset size changes)
    # If keeping 1311 rows, update train_corrected to include all
    return


@app.cell
def compute_flips():
    """Compute prediction flips."""
    # (Same as previous)
    return


@app.cell
def visualize_ages():
    """Visualize original vs. corrected ages."""
    # (Same as previous)
    return


if __name__ == "__main__":
    app.run()
