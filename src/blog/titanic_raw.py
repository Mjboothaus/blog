import httpx
import duckdb
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd
from loguru import logger
import random
import time
from diskcache import Cache


class TitanicaRawScraper:
    def __init__(
        self,
        data_dir="data",
        db_name="titanica_raw.duckdb",
        reset=False,
        cache_dir="cache",
    ):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = self.data_dir / db_name

        if reset and self.db_path.exists():
            self.db_path.unlink()
            print(f"Deleted old database {self.db_path}")

        self.con = duckdb.connect(str(self.db_path))
        self._init_db()

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.class_urls = {
            "First": "https://www.encyclopedia-titanica.org/titanic-first-class-passengers/",
            "Second": "https://www.encyclopedia-titanica.org/titanic-second-class-passengers/",
            "Third": "https://www.encyclopedia-titanica.org/titanic-third-class-passengers/",
        }

        # Initialize diskcache Cache instance
        self.cache = Cache(cache_dir)  # will create a 'cache' directory by default

    def _init_db(self):
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS titanica_raw (
                url VARCHAR PRIMARY KEY,
                raw_html TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("Initialized titanica_raw table.")

    def _fetch_url(self, url):
        """Fetch URL content using cache to avoid repeated requests."""
        if url in self.cache:
            print(f"Using cached content for {url}")
            return self.cache[url]
        else:
            print(f"Fetching new content for {url}")
            with httpx.Client(timeout=15.0) as client:
                response = client.get(url, headers=self.headers)
                response.raise_for_status()
                html = response.text
                self.cache[url] = html
                return html

    def scrape_class_pages(self):
        """Scrape all three passenger class pages to get individual passenger URLs."""
        all_urls = []
        try:
            for class_name, url in self.class_urls.items():
                print(f"Scraping class page: {class_name}")
                html = self._fetch_url(url)
                soup = BeautifulSoup(html, "html.parser")
                table = soup.find("table")
                if not table:
                    print(f"No table found on {url}")
                    continue
                for tr in table.find_all("tr")[1:]:
                    tds = tr.find_all("td")
                    if len(tds) >= 4:
                        link = tds[0].find("a")
                        if link and "href" in link.attrs:
                            full_url = (
                                f"https://www.encyclopedia-titanica.org{link['href']}"
                            )
                            all_urls.append(full_url)
            print(f"Found {len(all_urls)} individual passenger URLs from class pages.")
        except Exception as e:
            print(f"Error scraping class pages: {e}")
        return all_urls

    def scrape_urls(self, urls=None, sample_limit=5):
        """Scrape raw HTML for a list of URLs or scrape all if urls is None."""
        if urls is None:
            print(
                "No URLs provided. Fetching all individual passenger URLs from class pages..."
            )
            urls = self.scrape_class_pages()
        else:
            print(f"Scraping {len(urls)} provided URLs.")

        limit = min(sample_limit, len(urls)) if sample_limit is not None else len(urls)
        for url in urls[:limit]:
            if (
                self.con.execute(
                    "SELECT COUNT(*) FROM titanica_raw WHERE url = ?", [url]
                ).fetchone()[0]
                > 0
            ):
                print(f"Already scraped {url} (in DB)")
                continue
            try:
                html = self._fetch_url(url)
                self.con.execute(
                    "INSERT INTO titanica_raw (url, raw_html) VALUES (?, ?) ON CONFLICT DO NOTHING",
                    [url, html],
                )
                print(f"Saved {url} to DB")
                # optional polite delay
                if not url in self.cache:
                    time.sleep(random.uniform(0.3, 1.2))
            except Exception as e:
                print(f"Failed to scrape {url}: {e}")

    def view_sample(self, n=3):
        """Show pretty parsed HTML for n records."""
        rows = self.con.execute(
            "SELECT url, raw_html FROM titanica_raw LIMIT ?", [n]
        ).fetchall()
        for idx, (url, html) in enumerate(rows):
            print(f"\n==== Sample {idx + 1}: {url} ====")
            soup = BeautifulSoup(html, "html.parser")
            summary = soup.find("div", id="summary")
            bio = (
                soup.find("div", id="biography")
                or soup.find("div", class_="biosection")
                or soup.find("div", class_="biography")
            )
            if summary:
                print("\n--- Summary ---")
                print(summary.prettify())
            if bio:
                print("\n--- Biography ---")
                print(bio.prettify())
            print("\n--- End of Sample ---")

    def close(self):
        self.con.close()
        self.cache.close()


if __name__ == "__main__":
    RESET_DB = True  # Set to True to start fresh each run
    SAMPLE_LIMIT = None  # Number of individual pages to scrape (None for all)

    scraper = TitanicaRawScraper(reset=RESET_DB)
    try:
        scraper.scrape_urls(urls=None, sample_limit=SAMPLE_LIMIT)
        scraper.view_sample(n=3)
    finally:
        scraper.close()
