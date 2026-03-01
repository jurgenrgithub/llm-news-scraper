"""DuckDuckGo Search for player-specific AFL news.

Uses duckduckgo-search library which handles anti-bot measures.
"""

import logging
import time
from typing import List, Dict

from duckduckgo_search import DDGS

from scraper.config import DDG_DELAY_SECONDS

logger = logging.getLogger(__name__)


class DDGSearch:
    """Search DuckDuckGo for player-specific AFL articles."""

    def __init__(self):
        self.delay = DDG_DELAY_SECONDS

    def search_player(
        self,
        player_name: str,
        max_results: int = 10,
    ) -> List[Dict]:
        """Search for recent news about a player."""
        query = f'{player_name} AFL news'
        return self._search_news(query, max_results)

    def search_injury_news(
        self,
        player_name: str,
        max_results: int = 5,
    ) -> List[Dict]:
        """Search specifically for injury news about a player."""
        query = f'{player_name} AFL injury'
        return self._search_news(query, max_results)

    def search_trade_news(
        self,
        player_name: str,
        max_results: int = 5,
    ) -> List[Dict]:
        """Search for trade-related news about a player."""
        query = f'{player_name} AFL trade'
        return self._search_news(query, max_results)

    def _search_news(self, query: str, max_results: int = 10) -> List[Dict]:
        """Execute DDG news search and parse results."""
        articles = []

        try:
            # Use DDGS news search
            results = DDGS().news(query, max_results=max_results)
            articles = self._parse_results(results)
            logger.info(f"DDG found {len(articles)} results for: {query[:50]}...")

            # Rate limiting
            time.sleep(self.delay)

        except Exception as e:
            logger.error(f"DDG search error: {e}")

        return articles

    def _parse_results(self, results: List[Dict]) -> List[Dict]:
        """Parse DDG library results into our article format."""
        articles = []

        for r in results:
            url = r.get("url", "")

            # Filter to news domains
            if not self._is_news_url(url):
                continue

            articles.append({
                "url": url,
                "title": r.get("title", ""),
                "source": self._extract_source(url),
                "published_at": r.get("date"),
            })

        return articles

    def _is_news_url(self, url: str) -> bool:
        """Check if URL is from a news source."""
        news_domains = [
            "afl.com.au",
            "foxsports.com.au",
            "sen.com.au",
            "heraldsun.com.au",
            "theage.com.au",
            "abc.net.au",
            "news.com.au",
            "smh.com.au",
            "sportingnews.com",
            "espn.com",
            "7news.com.au",
            "9news.com.au",
            "triplem.com.au",
            "zerohanger.com",
        ]

        url_lower = url.lower()
        return any(domain in url_lower for domain in news_domains)

    def _extract_source(self, url: str) -> str:
        """Extract source name from URL."""
        domain_map = {
            "afl.com.au": "AFL.com.au",
            "foxsports.com.au": "Fox Sports",
            "sen.com.au": "SEN",
            "heraldsun.com.au": "Herald Sun",
            "theage.com.au": "The Age",
            "abc.net.au": "ABC News",
            "news.com.au": "News.com.au",
            "smh.com.au": "Sydney Morning Herald",
            "sportingnews.com": "Sporting News",
            "espn.com": "ESPN",
            "7news.com.au": "7 News",
            "9news.com.au": "9 News",
            "triplem.com.au": "Triple M",
            "zerohanger.com": "Zero Hanger",
        }

        url_lower = url.lower()
        for domain, name in domain_map.items():
            if domain in url_lower:
                return name

        return "Unknown"

    def close(self):
        """No cleanup needed for library-based implementation."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
