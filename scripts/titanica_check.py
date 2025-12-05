from pathlib import Path
import pandas as pd
import duckdb
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

# Paths
data_dir = Path('data')
excel_path = data_dir / 'TitanicDatasets_Compared.xlsx'

# Headers to mimic browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
}

# URLs
urls = {
    'first': 'https://www.encyclopedia-titanica.org/titanic-first-class-passengers/',
    'second': 'https://www.encyclopedia-titanica.org/titanic-second-class-passengers/',
    'third': 'https://www.encyclopedia-titanica.org/titanic-third-class-passengers/'
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def scrape_passenger_table(url):
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        table = soup.find('table')  # Adjust class if needed (inspect HTML)
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
                hometown_boarded = tds[2].text.strip()
                fate = tds[3].text.strip()
                rows.append({
                    'Name': name,
                    'Age': age,
                    'Hometown_Boarded': hometown_boarded,
                    'Fate': fate,
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

# Load into DuckDB
con = duckdb.connect(':memory:')
con.register('titanica_passengers', all_passengers)
con.execute("CREATE OR REPLACE TABLE titanica_passengers AS SELECT * FROM all_passengers")

# Load merged for verification
df_merged = pd.read_excel(excel_path, sheet_name='titanic_merged', skiprows=1)
df_merged = df_merged.drop_duplicates(subset='PassengerId', keep='first')
con.register('titanic_merged', df_merged)

# Verification query
verification_query = """
    SELECT m.PassengerId, m.Name, m.Age_y AS Merged_Age, t.Age AS Titanica_Age
    FROM titanic_merged m
    LEFT JOIN titanica_passengers t ON LOWER(m.Name) = LOWER(t.Name)
    WHERE m.Age_y IS NOT NULL AND t.Age IS NOT NULL
    LIMIT 20
"""
print("\nVerification Against Encyclopedia Titanica:")
print(con.execute(verification_query).df())