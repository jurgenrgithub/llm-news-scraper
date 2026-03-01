"""AFL Club Website Scraper - Tier 1 sources.

All 18 AFL clubs + AFL official use the same CMS with /news/{id}/{slug} pattern.
"""

import logging
import re
import time
from typing import List, Dict, Optional, Set

import httpx

from scraper.config import API_BASE_URL, FETCH_DELAY_SECONDS
from scraper.content_fetcher import ContentFetcher

logger = logging.getLogger(__name__)

# All AFL club domains (same CMS stack)
CLUB_DOMAINS = [
    "afl.com.au",            # Official AFL
    "adelaidefc.com.au",     # Adelaide Crows
    "lions.com.au",          # Brisbane Lions
    "carltonfc.com.au",      # Carlton Blues
    "collingwoodfc.com.au",  # Collingwood Magpies
    "essendonfc.com.au",     # Essendon Bombers
    "fremantlefc.com.au",    # Fremantle Dockers
    "geelongcats.com.au",    # Geelong Cats
    "goldcoastfc.com.au",    # Gold Coast Suns
    "gwsgiants.com.au",      # GWS Giants
    "hawthornfc.com.au",     # Hawthorn Hawks
    "melbournefc.com.au",    # Melbourne Demons
    "nmfc.com.au",           # North Melbourne Kangaroos
    "portadelaidefc.com.au", # Port Adelaide Power
    "richmondfc.com.au",     # Richmond Tigers
    "saints.com.au",         # St Kilda Saints
    "sydneyswans.com.au",    # Sydney Swans
    "westcoasteagles.com.au",# West Coast Eagles
    "westernbulldogs.com.au",# Western Bulldogs
]

# Map domain to source name
DOMAIN_SOURCE_MAP = {
    "afl.com.au": "AFL Official",
    "adelaidefc.com.au": "Adelaide Crows",
    "lions.com.au": "Brisbane Lions",
    "carltonfc.com.au": "Carlton",
    "collingwoodfc.com.au": "Collingwood",
    "essendonfc.com.au": "Essendon",
    "fremantlefc.com.au": "Fremantle",
    "geelongcats.com.au": "Geelong",
    "goldcoastfc.com.au": "Gold Coast",
    "gwsgiants.com.au": "GWS Giants",
    "hawthornfc.com.au": "Hawthorn",
    "melbournefc.com.au": "Melbourne",
    "nmfc.com.au": "North Melbourne",
    "portadelaidefc.com.au": "Port Adelaide",
    "richmondfc.com.au": "Richmond",
    "saints.com.au": "St Kilda",
    "sydneyswans.com.au": "Sydney Swans",
    "westcoasteagles.com.au": "West Coast",
    "westernbulldogs.com.au": "Western Bulldogs",
}


class ClubScraper:
    """Scraper for AFL club websites."""

    # Pattern: /news/1234567/article-slug
    ARTICLE_PATTERN = re.compile(r'/news/(\d{5,8})/[a-z0-9-]+')

    def __init__(self, domains: List[str] = None):
        self.domains = domains or CLUB_DOMAINS
        self.client = httpx.Client(
            timeout=30,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-AU,en;q=0.9",
            },
        )
        self.fetcher = ContentFetcher()
        self.api_client = httpx.Client(timeout=30)

    def scrape_all_clubs(self) -> Dict:
        """Scrape all club websites."""
        stats = {
            "domains_scraped": 0,
            "articles_found": 0,
            "articles_ingested": 0,
            "duplicates": 0,
            "errors": 0,
        }

        for domain in self.domains:
            try:
                result = self.scrape_club(domain)
                stats["domains_scraped"] += 1
                stats["articles_found"] += result["found"]
                stats["articles_ingested"] += result["ingested"]
                stats["duplicates"] += result["duplicates"]
                stats["errors"] += result["errors"]
            except Exception as e:
                logger.error(f"Failed to scrape {domain}: {e}")
                stats["errors"] += 1

            # Rate limit between domains
            time.sleep(2.0)

        return stats

    def scrape_club(self, domain: str) -> Dict:
        """Scrape a single club's news page."""
        result = {"found": 0, "ingested": 0, "duplicates": 0, "errors": 0}
        source = DOMAIN_SOURCE_MAP.get(domain, domain)

        logger.info(f"Scraping {source} ({domain})")

        try:
            # Fetch news listing page
            urls = self._get_article_urls(domain)
            result["found"] = len(urls)
            logger.info(f"  Found {len(urls)} article URLs")

            # Process each article
            for url in urls:
                try:
                    ingest_result = self._process_article(url, source)
                    if ingest_result == "ingested":
                        result["ingested"] += 1
                    elif ingest_result == "duplicate":
                        result["duplicates"] += 1
                    else:
                        result["errors"] += 1

                    time.sleep(FETCH_DELAY_SECONDS)

                except Exception as e:
                    logger.warning(f"  Error processing {url}: {e}")
                    result["errors"] += 1

        except Exception as e:
            logger.error(f"Error fetching news list from {domain}: {e}")
            result["errors"] += 1

        return result

    def _get_article_urls(self, domain: str) -> List[str]:
        """Get article URLs from club's /news page."""
        url = f"https://www.{domain}/news"

        try:
            response = self.client.get(url)
            response.raise_for_status()
            html = response.text

            # Find all /news/{id}/{slug} links
            matches = self.ARTICLE_PATTERN.findall(html)

            # Reconstruct full URLs, dedupe
            seen_ids: Set[str] = set()
            urls = []

            for article_id in matches:
                if article_id not in seen_ids:
                    seen_ids.add(article_id)
                    # Find full link for this ID
                    full_match = re.search(
                        rf'/news/{article_id}/[a-z0-9-]+',
                        html
                    )
                    if full_match:
                        full_url = f"https://www.{domain}{full_match.group()}"
                        urls.append(full_url)

            return urls[:20]  # Limit to most recent 20

        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return []

    def _process_article(self, url: str, source: str) -> str:
        """Fetch and ingest a single article. Returns: 'ingested', 'duplicate', or 'error'."""
        # Fetch article content using existing fetcher
        content = self.fetcher.fetch(url)

        if not content or not content.get("body"):
            logger.warning(f"  No content from {url}")
            return "error"

        # Ingest via API
        try:
            response = self.api_client.post(
                f"{API_BASE_URL}/articles/ingest",
                json={
                    "url": url,
                    "title": content.get("title", ""),
                    "body": content["body"],
                    "source": source,
                },
            )
            response.raise_for_status()
            result = response.json()

            if result.get("status") == "duplicate":
                return "duplicate"

            logger.info(f"  Ingested: {content.get('title', url)[:50]}...")
            return "ingested"

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:  # Conflict = duplicate
                return "duplicate"
            logger.error(f"  API error: {e}")
            return "error"
        except Exception as e:
            logger.error(f"  Ingest error: {e}")
            return "error"

    def close(self):
        """Clean up resources."""
        self.client.close()
        self.fetcher.close()
        self.api_client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def main():
    """Run club scraper standalone."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logger.info("=" * 50)
    logger.info("AFL Club Website Scraper")
    logger.info("=" * 50)

    with ClubScraper() as scraper:
        stats = scraper.scrape_all_clubs()

    logger.info("\n" + "=" * 50)
    logger.info("SCRAPE COMPLETE")
    logger.info(f"  Domains scraped: {stats['domains_scraped']}")
    logger.info(f"  Articles found:  {stats['articles_found']}")
    logger.info(f"  Articles ingested: {stats['articles_ingested']}")
    logger.info(f"  Duplicates:      {stats['duplicates']}")
    logger.info(f"  Errors:          {stats['errors']}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
