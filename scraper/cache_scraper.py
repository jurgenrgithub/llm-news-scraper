"""Cache-first scraper - fetches and stores raw HTML only.

Discovers URLs from all sources (clubs, RSS, DDG) and caches raw HTML
to PostgreSQL. No content extraction or processing.
"""

import logging
import re
import time
from typing import Dict, List, Set

import httpx

from scraper.config import (
    CLUB_DOMAINS,
    DDG_ENABLED,
    DDG_MAX_PLAYERS,
    FETCH_DELAY_SECONDS,
    LOG_LEVEL,
    RSS_FEEDS,
)
from scraper.page_cache import PageCache
from scraper.rss_monitor import RSSMonitor
from scraper.ddg_search import DDGSearch
from scraper.date_extractor import DateExtractor

logger = logging.getLogger(__name__)

# Map club domains to source names
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

# AFL injury list URL
AFL_INJURY_URL = "https://www.afl.com.au/matches/injury-list"


class CacheScraper:
    """Scraper that caches raw HTML to PostgreSQL."""

    # Pattern for club news articles: /news/1234567/article-slug
    ARTICLE_PATTERN = re.compile(r'/news/(\d{5,8})/[a-z0-9-]+')

    def __init__(self):
        self.cache = PageCache()
        self.date_extractor = DateExtractor()
        self.http = httpx.Client(
            timeout=30,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-AU,en;q=0.9",
            },
        )
        self.rss = RSSMonitor()
        self.ddg = DDGSearch() if DDG_ENABLED else None

    def run(self) -> Dict:
        """Run full cache cycle."""
        stats = {
            "urls_discovered": 0,
            "already_cached": 0,
            "fetched": 0,
            "cached": 0,
            "errors": 0,
        }

        logger.info("=" * 50)
        logger.info("Cache Scraper - Raw HTML Collection")
        logger.info("=" * 50)

        # Phase 1: Discover URLs from all sources
        logger.info("\n--- Phase 1: URL Discovery ---")
        url_infos = self._discover_all_urls()
        stats["urls_discovered"] = len(url_infos)
        logger.info(f"Total URLs discovered: {len(url_infos)}")

        # Phase 2: Filter to uncached URLs
        logger.info("\n--- Phase 2: Filter Uncached ---")
        urls_only = [u["url"] for u in url_infos]
        uncached_urls = set(self.cache.get_uncached_urls(urls_only))
        to_fetch = [u for u in url_infos if u["url"] in uncached_urls]
        stats["already_cached"] = len(url_infos) - len(to_fetch)
        logger.info(f"Already cached: {stats['already_cached']}")
        logger.info(f"To fetch: {len(to_fetch)}")

        # Phase 3: Fetch and cache
        logger.info("\n--- Phase 3: Fetch & Cache ---")
        for i, url_info in enumerate(to_fetch, 1):
            url = url_info["url"]
            logger.info(f"[{i}/{len(to_fetch)}] {url[:70]}...")

            try:
                response = self.http.get(url)
                stats["fetched"] += 1

                if response.status_code == 200:
                    # Extract article publication date from HTML
                    published_at = self.date_extractor.extract(response.text)

                    page_id = self.cache.store(
                        url=url,
                        html=response.text,
                        source_type=url_info["source_type"],
                        source_name=url_info.get("source_name"),
                        http_status=response.status_code,
                        published_at=published_at,
                    )
                    if page_id:
                        stats["cached"] += 1
                        date_str = published_at.strftime("%Y-%m-%d") if published_at else "no date"
                        logger.info(f"  -> Cached (ID: {page_id}, {date_str})")
                    else:
                        logger.info(f"  -> Skipped (duplicate)")
                else:
                    logger.warning(f"  -> HTTP {response.status_code}")
                    stats["errors"] += 1

                time.sleep(FETCH_DELAY_SECONDS)

            except Exception as e:
                logger.error(f"  -> Error: {e}")
                stats["errors"] += 1

        # Summary
        logger.info("\n" + "=" * 50)
        logger.info("CACHE COMPLETE")
        logger.info(f"  URLs discovered:  {stats['urls_discovered']}")
        logger.info(f"  Already cached:   {stats['already_cached']}")
        logger.info(f"  Fetched:          {stats['fetched']}")
        logger.info(f"  Cached:           {stats['cached']}")
        logger.info(f"  Errors:           {stats['errors']}")
        logger.info("=" * 50)

        return stats

    def _discover_all_urls(self) -> List[Dict]:
        """Discover URLs from all sources."""
        all_urls = []

        # 1. Club websites (Tier 1)
        logger.info("Discovering club website URLs...")
        club_urls = self._discover_club_urls()
        all_urls.extend(club_urls)
        logger.info(f"  Club URLs: {len(club_urls)}")

        # 2. RSS feeds (Tier 2)
        logger.info("Discovering RSS feed URLs...")
        rss_urls = self._discover_rss_urls()
        all_urls.extend(rss_urls)
        logger.info(f"  RSS URLs: {len(rss_urls)}")

        # 3. DDG search (Tier 3)
        if self.ddg:
            logger.info("Discovering DDG search URLs...")
            ddg_urls = self._discover_ddg_urls()
            all_urls.extend(ddg_urls)
            logger.info(f"  DDG URLs: {len(ddg_urls)}")

        # 4. AFL injury list (Lane 0)
        logger.info("Adding AFL injury list...")
        all_urls.append({
            "url": AFL_INJURY_URL,
            "source_type": "injury_list",
            "source_name": "AFL Official",
        })

        # Deduplicate by URL
        return self._dedupe_urls(all_urls)

    def _discover_club_urls(self) -> List[Dict]:
        """Discover article URLs from all club websites."""
        urls = []

        for domain in CLUB_DOMAINS:
            source_name = DOMAIN_SOURCE_MAP.get(domain, domain)

            try:
                news_url = f"https://www.{domain}/news"
                response = self.http.get(news_url)

                if response.status_code != 200:
                    logger.warning(f"  {domain}: HTTP {response.status_code}")
                    continue

                html = response.text

                # Find article URLs
                matches = self.ARTICLE_PATTERN.findall(html)
                seen_ids: Set[str] = set()

                for article_id in matches:
                    if article_id not in seen_ids:
                        seen_ids.add(article_id)
                        # Find full URL for this ID
                        full_match = re.search(
                            rf'/news/{article_id}/[a-z0-9-]+',
                            html
                        )
                        if full_match:
                            full_url = f"https://www.{domain}{full_match.group()}"
                            urls.append({
                                "url": full_url,
                                "source_type": "club",
                                "source_name": source_name,
                            })

                logger.info(f"  {source_name}: {len(seen_ids)} articles")
                time.sleep(0.5)  # Rate limit between domains

            except Exception as e:
                logger.warning(f"  {domain}: Error - {e}")

        return urls

    def _discover_rss_urls(self) -> List[Dict]:
        """Discover article URLs from RSS feeds."""
        urls = []

        articles = self.rss.fetch_all_feeds()

        for article in articles:
            if article.get("url"):
                urls.append({
                    "url": article["url"],
                    "source_type": "rss",
                    "source_name": article.get("source", "Unknown"),
                })

        return urls

    def _discover_ddg_urls(self) -> List[Dict]:
        """Discover article URLs from DDG search."""
        urls = []

        # Search for top players
        # For now, use a static list of high-profile players
        top_players = [
            "Marcus Bontempelli", "Nick Daicos", "Errol Gulden",
            "Zak Butters", "Connor Rozee", "Lachie Neale",
            "Christian Petracca", "Clayton Oliver", "Patrick Cripps",
            "Isaac Heeney", "Max Gawn", "Toby Greene",
        ]

        for player in top_players[:DDG_MAX_PLAYERS]:
            try:
                articles = self.ddg.search_player(player, max_results=5)
                for article in articles:
                    if article.get("url"):
                        urls.append({
                            "url": article["url"],
                            "source_type": "ddg",
                            "source_name": article.get("source", "Unknown"),
                        })
            except Exception as e:
                logger.warning(f"  DDG error for {player}: {e}")

        return urls

    def _dedupe_urls(self, url_infos: List[Dict]) -> List[Dict]:
        """Remove duplicate URLs, keeping first occurrence."""
        seen: Set[str] = set()
        unique = []

        for info in url_infos:
            url = info.get("url")
            if url and url not in seen:
                seen.add(url)
                unique.append(info)

        return unique

    def close(self):
        """Clean up resources."""
        self.http.close()
        self.rss.close()
        if self.ddg:
            self.ddg.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def main():
    """Entry point."""
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    with CacheScraper() as scraper:
        stats = scraper.run()

        # Show cache stats
        cache_stats = scraper.cache.get_stats()
        logger.info(f"\nTotal cached pages: {cache_stats['total']}")


if __name__ == "__main__":
    main()
