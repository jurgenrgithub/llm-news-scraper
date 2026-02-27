"""DuckDuckGo Search for player-specific AFL news."""

import html
import logging
import re
import time
import urllib.parse
from typing import List, Dict, Optional

import httpx

from scraper.config import DDG_DELAY_SECONDS

logger = logging.getLogger(__name__)


class DDGSearch:
    """Search DuckDuckGo for player-specific AFL articles."""

    def __init__(self):
        self.client = httpx.Client(
            timeout=20,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            },
        )
        self.delay = DDG_DELAY_SECONDS

    def search_player(
        self,
        player_name: str,
        max_results: int = 10,
    ) -> List[Dict]:
        """Search for recent news about a player."""
        query = f'"{player_name}" AFL news'
        return self._search(query, max_results)

    def search_injury_news(
        self,
        player_name: str,
        max_results: int = 5,
    ) -> List[Dict]:
        """Search specifically for injury news about a player."""
        query = f'"{player_name}" AFL injury OR injured OR hamstring OR calf'
        return self._search(query, max_results)

    def search_trade_news(
        self,
        player_name: str,
        max_results: int = 5,
    ) -> List[Dict]:
        """Search for trade-related news about a player."""
        query = f'"{player_name}" AFL trade OR contract OR delisted'
        return self._search(query, max_results)

    def _search(self, query: str, max_results: int = 10) -> List[Dict]:
        """Execute DDG search and parse results."""
        articles = []

        try:
            # Use DDG HTML search
            encoded_query = urllib.parse.quote_plus(query)
            url = f"https://html.duckduckgo.com/html/?q={encoded_query}"

            response = self.client.get(url)
            response.raise_for_status()

            # Parse results
            articles = self._parse_results(response.text, max_results)
            logger.info(f"DDG found {len(articles)} results for: {query[:50]}...")

            # Rate limiting
            time.sleep(self.delay)

        except Exception as e:
            logger.error(f"DDG search error: {e}")

        return articles

    def _parse_results(self, html_content: str, max_results: int) -> List[Dict]:
        """Parse DDG HTML results into article dicts."""
        articles = []

        # Extract result links and titles
        # DDG HTML format: <a class="result__a" href="...">title</a>
        pattern = r'class="result__a"[^>]*href="([^"]+)"[^>]*>([^<]+)</a>'
        matches = re.findall(pattern, html_content)

        for url, title in matches[:max_results]:
            # Clean URL (DDG sometimes wraps URLs)
            if url.startswith("//duckduckgo.com/l/?"):
                actual_url = self._extract_redirect_url(url)
                if actual_url:
                    url = actual_url

            # Skip non-news URLs
            if not self._is_news_url(url):
                continue

            articles.append({
                "url": url,
                "title": self._clean_text(title),
                "source": self._extract_source(url),
                "published_at": None,  # DDG doesn't provide dates
            })

        return articles

    def _extract_redirect_url(self, ddg_url: str) -> Optional[str]:
        """Extract actual URL from DDG redirect URL."""
        try:
            parsed = urllib.parse.urlparse(ddg_url)
            params = urllib.parse.parse_qs(parsed.query)
            if "uddg" in params:
                return urllib.parse.unquote(params["uddg"][0])
        except Exception:
            pass
        return None

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
        }

        url_lower = url.lower()
        for domain, name in domain_map.items():
            if domain in url_lower:
                return name

        return "Unknown"

    def _clean_text(self, text: str) -> str:
        """Clean text from HTML entities and whitespace."""
        if not text:
            return ""
        text = html.unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def close(self):
        """Close HTTP client."""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
