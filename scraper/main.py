"""AFL News Scraper - Main entry point.

Orchestrates:
1. RSS feed sweep
2. DDG player search backfill
3. Content fetching
4. Article ingestion to llm-news-service
"""

import logging
import sys
import time
from typing import Dict, List, Set

import httpx

from scraper.config import (
    API_BASE_URL,
    DDG_ENABLED,
    DDG_MAX_PLAYERS,
    FETCH_DELAY_SECONDS,
    LOG_LEVEL,
)
from scraper.rss_monitor import RSSMonitor
from scraper.ddg_search import DDGSearch
from scraper.content_fetcher import ContentFetcher

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class AFLNewsScraper:
    """Main scraper orchestrator."""

    def __init__(self):
        self.api_url = API_BASE_URL
        self.rss = RSSMonitor()
        self.ddg = DDGSearch() if DDG_ENABLED else None
        self.fetcher = ContentFetcher()
        self.http = httpx.Client(timeout=30)

    def run(self) -> Dict:
        """Run complete scrape cycle."""
        stats = {
            "rss_found": 0,
            "ddg_found": 0,
            "fetched": 0,
            "ingested": 0,
            "duplicates": 0,
            "errors": 0,
        }

        logger.info("=" * 50)
        logger.info("AFL News Scraper starting...")
        logger.info(f"API endpoint: {self.api_url}")
        logger.info("=" * 50)

        # Phase 1: RSS sweep
        logger.info("\n--- Phase 1: RSS Sweep ---")
        rss_articles = self.rss.fetch_all_feeds()
        stats["rss_found"] = len(rss_articles)
        logger.info(f"Found {len(rss_articles)} articles from RSS feeds")

        # Phase 2: DDG backfill (if enabled)
        ddg_articles = []
        if self.ddg:
            logger.info("\n--- Phase 2: DDG Backfill ---")
            ddg_articles = self._ddg_backfill()
            stats["ddg_found"] = len(ddg_articles)
            logger.info(f"Found {len(ddg_articles)} articles from DDG")

        # Combine and dedupe
        all_articles = self._dedupe_articles(rss_articles + ddg_articles)
        logger.info(f"\nCombined: {len(all_articles)} unique articles")

        # Phase 3: Fetch and ingest
        logger.info("\n--- Phase 3: Fetch & Ingest ---")
        for i, article in enumerate(all_articles, 1):
            logger.info(f"[{i}/{len(all_articles)}] {article['url'][:60]}...")

            try:
                # Fetch full content
                content = self.fetcher.fetch(article["url"])
                if not content:
                    stats["errors"] += 1
                    continue
                stats["fetched"] += 1

                # Ingest via API
                result = self._ingest_article(
                    url=article["url"],
                    title=content.get("title", article.get("title", "")),
                    body=content["body"],
                    source=article.get("source"),
                    published_at=article.get("published_at"),
                )

                if result:
                    if result.get("status") == "duplicate":
                        stats["duplicates"] += 1
                    else:
                        stats["ingested"] += 1
                        logger.info(f"  -> Ingested (ID: {result.get('article_id')})")
                else:
                    stats["errors"] += 1

                # Rate limiting
                time.sleep(FETCH_DELAY_SECONDS)

            except Exception as e:
                logger.error(f"  -> Error: {e}")
                stats["errors"] += 1

        # Summary
        logger.info("\n" + "=" * 50)
        logger.info("SCRAPE COMPLETE")
        logger.info(f"  RSS found:   {stats['rss_found']}")
        logger.info(f"  DDG found:   {stats['ddg_found']}")
        logger.info(f"  Fetched:     {stats['fetched']}")
        logger.info(f"  Ingested:    {stats['ingested']}")
        logger.info(f"  Duplicates:  {stats['duplicates']}")
        logger.info(f"  Errors:      {stats['errors']}")
        logger.info("=" * 50)

        return stats

    def _ddg_backfill(self) -> List[Dict]:
        """Search DDG for player articles."""
        articles = []

        # Get monitored players from API
        players = self._get_monitored_players()
        logger.info(f"Searching DDG for {len(players)} players")

        for player in players[:DDG_MAX_PLAYERS]:
            try:
                player_articles = self.ddg.search_player(player["name"])
                articles.extend(player_articles)
            except Exception as e:
                logger.warning(f"DDG error for {player['name']}: {e}")

        return articles

    def _get_monitored_players(self) -> List[Dict]:
        """Get list of players from API."""
        try:
            response = self.http.get(
                f"{self.api_url}/entities",
                params={"domain": "afl", "entity_type": "player", "limit": 50}
            )
            if response.status_code == 200:
                data = response.json()
                return [{"name": e["name"]} for e in data.get("entities", [])]
        except Exception as e:
            logger.error(f"Could not fetch players from API: {e}")

        return []

    def _dedupe_articles(self, articles: List[Dict]) -> List[Dict]:
        """Remove duplicate URLs."""
        seen: Set[str] = set()
        unique = []
        for article in articles:
            url = article.get("url")
            if url and url not in seen:
                seen.add(url)
                unique.append(article)
        return unique

    def _ingest_article(
        self,
        url: str,
        title: str,
        body: str,
        source: str = None,
        published_at: str = None,
    ) -> Dict:
        """POST article to ingestion API."""
        try:
            response = self.http.post(
                f"{self.api_url}/articles/ingest",
                json={
                    "url": url,
                    "title": title,
                    "body": body,
                    "source": source,
                    "published_at": published_at,
                },
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ingest error: {e}")
            return None

    def close(self):
        """Clean up resources."""
        self.rss.close()
        if self.ddg:
            self.ddg.close()
        self.fetcher.close()
        self.http.close()


def main():
    """Entry point."""
    scraper = AFLNewsScraper()
    try:
        stats = scraper.run()
        # Exit with error code if too many errors
        if stats["errors"] > stats["ingested"]:
            sys.exit(1)
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
