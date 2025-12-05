import re
import time
import random
import httpx
import pandas as pd
import json
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
import duckdb
from tqdm import tqdm
from pathlib import Path
from loguru import logger


class TitanicaScraper:
    def __init__(self, data_dir="data", db_name="titanica.db", clear_db=False):
        """
        Initialize the scraper with a database connection in the specified data directory.
        Creates the data directory and table if they don't exist.
        Args:
            data_dir (str): Directory for database and logs.
            db_name (str): Database file name.
            clear_db (bool): If True, clear the titanica_individual table before scraping.
        """
        # Set up logging
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        log_file = self.data_dir / "scraper.log"
        logger.add(log_file, level="DEBUG", format="{time} {level} {message}")

        # Initialize temporary JSON file
        self.temp_file = self.data_dir / "scraped_temp.json"
        if self.temp_file.exists():
            self.temp_file.unlink()  # Clear previous temp file
            logger.info(f"Cleared existing temporary file {self.temp_file}")

        # Delete existing database file if clear_db is True
        self.db_path = self.data_dir / db_name
        if clear_db and self.db_path.exists():
            self.db_path.unlink()
            logger.info(f"Deleted existing database file {self.db_path}")

        # Initialize database
        self.con = duckdb.connect(str(self.db_path))
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.urls = {
            "First": "https://www.encyclopedia-titanica.org/titanic-first-class-passengers/",
            "Second": "https://www.encyclopedia-titanica.org/titanic-second-class-passengers/",
            "Third": "https://www.encyclopedia-titanica.org/titanic-third-class-passengers/",
        }

        # Create table if not exists
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS titanica_individual (
                Name VARCHAR,
                Cleaned_Name VARCHAR,
                Class VARCHAR,
                Individual_URL VARCHAR UNIQUE,
                age VARCHAR,
                class_dept VARCHAR,
                ticket_number VARCHAR,
                passenger_fare VARCHAR,
                embarked VARCHAR,
                occupation VARCHAR,
                hometown VARCHAR,
                born VARCHAR,
                died VARCHAR,
                biography VARCHAR
            )
        """)

        # Clear table if requested
        if clear_db:
            self.con.execute("DELETE FROM titanica_individual")
            self.con.commit()
            logger.info("Cleared titanica_individual table")

        logger.info(f"Initialized scraper with database at {self.db_path}")

    def clean_name(self, name):
        """
        Clean the name by removing titles, extra spaces, and non-ASCII characters.
        """
        if not isinstance(name, str):
            return ""
        name = re.sub(
            r"\b(Mr\.|Mrs\.|Miss\.|Master\.|Dr\.|Rev\.|Col\.|Major\.|Ms\.|)\b", "", name
        )
        name = re.sub(r"\s+", " ", name.strip())
        name = name.encode("ascii", "ignore").decode("ascii")
        return name.lower()

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=30)
    )
    def scrape_passenger_table(self, url, class_type):
        """
        Scrape the passenger table from a class page.
        """
        try:
            with httpx.Client(timeout=15.0) as client:
                response = client.get(url, headers=self.headers)
                response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table")
            if not table:
                logger.warning(f"No table found on {url}")
                return pd.DataFrame()

            rows = []
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all("td")
                if len(tds) >= 4:
                    name_link = tds[0].find("a")
                    name = name_link.text.strip() if name_link else tds[0].text.strip()
                    individual_url = name_link["href"] if name_link else ""
                    age = tds[1].text.strip()
                    rows.append(
                        {
                            "Name": name,
                            "Cleaned_Name": self.clean_name(name),
                            "Age": age,
                            "Individual_URL": f"https://www.encyclopedia-titanica.org{individual_url}",
                            "Class": class_type,
                        }
                    )
            df = pd.DataFrame(rows)
            logger.info(f"Scraped {len(df)} passengers from {class_type} class page.")
            return df
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
            raise

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=30)
    )
    def scrape_individual_page(self, url):
        """
        Scrape data from an individual passenger page, ensuring all table columns are populated.
        """
        try:
            with httpx.Client(timeout=15.0) as client:
                response = client.get(url, headers=self.headers)
                response.raise_for_status()
            logger.debug(
                f"Successfully fetched {url} with status {response.status_code}"
            )

            soup = BeautifulSoup(response.text, "html.parser")

            # Initialize data with all expected columns
            data = {
                "age": "",
                "class_dept": "",
                "ticket_number": "",
                "passenger_fare": "",
                "embarked": "",
                "occupation": "",
                "hometown": "",
                "born": "",
                "died": "",
                "biography": "",
                "Individual_URL": url,
            }

            # Scrape metadata from div#summary
            summary = soup.find("div", id="summary")
            extracted_metadata = {}
            if summary:
                for div in summary.find_all("div", recursive=False):
                    strong = div.find("strong")
                    if strong:
                        key = (
                            strong.text.strip()
                            .lower()
                            .replace(" ", "_")
                            .replace(":", "")
                            .replace("/", "_")
                            .replace(".", "")
                            .replace("no", "number")
                        )
                        if key == "ticket_no" or key == "ticket_number":
                            key = "ticket_number"
                        value = (
                            div.get_text(strip=True)[len(strong.get_text(strip=True)) :]
                            .lstrip(": ")
                            .strip()
                        )
                        if key in data:
                            data[key] = value
                            extracted_metadata[key] = value
            else:
                logger.warning(f"No summary section found on {url}")

            logger.debug(f"Extracted metadata for {url}: {extracted_metadata}")

            # Scrape biography (try id first, then classes)
            bio = (
                soup.find("div", id="biography")
                or soup.find("div", class_="biosection")
                or soup.find("div", class_="biography")
            )
            data["biography"] = bio.get_text(strip=True) if bio else ""
            logger.debug(
                f"Biography length for {url}: {len(data['biography'])} characters"
            )

            # Write to temporary JSON file
            with open(self.temp_file, "a", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
                f.write("\n")  # JSON Lines format
            logger.debug(f"Wrote scraped data for {url} to {self.temp_file}")

            return data
        except Exception as e:
            logger.error(f"Failed to scrape {url}: {str(e)}", exc_info=True)
            raise

    def get_scraped_urls(self):
        """
        Get set of already scraped individual URLs from the database.
        """
        try:
            scraped_urls = self.con.execute(
                "SELECT Individual_URL FROM titanica_individual"
            ).fetchall()
            urls = set(url[0] for url in scraped_urls if url[0])
            logger.debug(f"Retrieved {len(urls)} scraped URLs from database")
            return urls
        except Exception as e:
            logger.error(f"Error retrieving scraped URLs: {str(e)}", exc_info=True)
            return set()

    def scrape_all(self, sample_size=None, clear_db=False):
        """
        Scrape all class lists and then individual pages incrementally with progress bars.
        Skips already scraped URLs for restartability.
        Args:
            sample_size (int, optional): Limit the number of individual pages to scrape.
            clear_db (bool): If True, clear the titanica_individual table before scraping.
        """
        # Clear database if requested
        if clear_db:
            self.con.execute("DELETE FROM titanica_individual")
            self.con.commit()
            logger.info("Cleared titanica_individual table before scraping")

        scraped_urls = self.get_scraped_urls()
        all_passengers = pd.DataFrame()

        # Scrape class lists with progress bar
        for class_type, url in tqdm(self.urls.items(), desc="Scraping class pages"):
            df_class = self.scrape_passenger_table(url, class_type)
            all_passengers = pd.concat([all_passengers, df_class], ignore_index=True)

        logger.info(
            f"Extracted {len(all_passengers)} passengers from Encyclopedia Titanica class pages."
        )
        print(
            f"\nExtracted {len(all_passengers)} passengers from Encyclopedia Titanica class pages."
        )
        print(all_passengers.head(10))

        # Scrape individual pages with progress bar
        to_scrape = [
            row
            for _, row in all_passengers.iterrows()
            if row["Individual_URL"] and row["Individual_URL"] not in scraped_urls
        ]
        if sample_size is not None:
            to_scrape = to_scrape[:sample_size]
            logger.info(f"Limiting scrape to {sample_size} individual pages.")

        for row in tqdm(to_scrape, desc="Scraping individual pages"):
            url = row["Individual_URL"]
            try:
                data = self.scrape_individual_page(url)
                if data:
                    data["Name"] = row["Name"]
                    data["Cleaned_Name"] = row["Cleaned_Name"]
                    data["Class"] = row["Class"]
                    data["Individual_URL"] = url

                    # Log data before insertion
                    logger.debug(f"Data to insert for {url}: {data}")

                    # Insert using parameterized query
                    query = """
                        INSERT INTO titanica_individual (
                            Name, Cleaned_Name, Class, Individual_URL, age, class_dept,
                            ticket_number, passenger_fare, embarked, occupation, hometown,
                            born, died, biography
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params = [
                        data["Name"],
                        data["Cleaned_Name"],
                        data["Class"],
                        data["Individual_URL"],
                        data["age"],
                        data["class_dept"],
                        data["ticket_number"],
                        data["passenger_fare"],
                        data["embarked"],
                        data["occupation"],
                        data["hometown"],
                        data["born"],
                        data["died"],
                        data["biography"],
                    ]
                    logger.debug(
                        f"Executing query for {url}: {query} with params: {params[:4]}..."
                    )  # Log partial params for brevity

                    try:
                        self.con.execute(query, params)
                        self.con.commit()
                        # Verify insertion
                        inserted = self.con.execute(
                            "SELECT COUNT(*) FROM titanica_individual WHERE Individual_URL = ?",
                            [url],
                        ).fetchone()[0]
                        row_count = self.con.execute(
                            "SELECT COUNT(*) FROM titanica_individual"
                        ).fetchone()[0]
                        if inserted > 0:
                            logger.info(
                                f"Successfully inserted data for {url} into database (total rows: {row_count})"
                            )
                            scraped_urls.add(url)
                            print(f"Scraped and saved: {row['Name']} ({url})")
                        else:
                            logger.error(
                                f"Insertion verification failed for {url}: No rows inserted"
                            )
                    except Exception as e:
                        logger.error(
                            f"Database insertion failed for {url}: {str(e)}",
                            exc_info=True,
                        )
                        raise

            except Exception as e:
                logger.error(
                    f"Failed to scrape or insert {url}: {str(e)}", exc_info=True
                )
                print(f"Failed to scrape or insert {url}: {e}")

            time.sleep(
                random.uniform(0.25, 1.75)
            )  # Random delay between 0.25 and 1.75 seconds

        # Load final data with a new connection
        self.con.close()
        final_con = duckdb.connect(str(self.db_path))
        df_individual = final_con.execute("SELECT * FROM titanica_individual").df()
        final_con.close()
        logger.info(f"Extracted {len(df_individual)} individual entries from database.")
        print(f"\nExtracted {len(df_individual)} individual entries from database.")
        print(df_individual.head(10))

        return df_individual

    def close(self):
        """
        Close the database connection.
        """
        self.con.close()
        logger.info("Closed database connection.")


# Example usage
if __name__ == "__main__":
    scraper = TitanicaScraper(data_dir="data", clear_db=True)
    try:
        df = scraper.scrape_all(
            sample_size=5, clear_db=True
        )  # Scrape only 5 individual pages for testing
    finally:
        scraper.close()
