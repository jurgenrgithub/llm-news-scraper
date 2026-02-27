"""RSS Feed Monitor for AFL news sources."""

import logging
import re
from datetime import datetime
from typing import List, Dict, Optional
from xml.etree import ElementTree as ET

import httpx

from scraper.config import RSS_FEEDS

logger = logging.getLogger(__name__)


class RSSMonitor:
    """Monitors RSS feeds for AFL news."""

    def __init__(self, feeds: List[Dict] = None):
        self.feeds = feeds or RSS_FEEDS
        self.client = httpx.Client(
            timeout=15,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; AFLNewsBot/1.0)",
            },
        )

    def fetch_all_feeds(self) -> List[Dict]:
        """Fetch articles from all configured feeds."""
        all_articles = []

        for feed in sorted(self.feeds, key=lambda x: x.get("priority", 99)):
            try:
                articles = self.fetch_feed(feed["url"], feed.get("source"))
                all_articles.extend(articles)
                logger.info(f"Fetched {len(articles)} from {feed['source']}")
            except Exception as e:
                logger.warning(f"Failed to fetch {feed['source']}: {e}")

        return all_articles

    def fetch_feed(self, url: str, source: str = None) -> List[Dict]:
        """Fetch and parse a single RSS feed."""
        try:
            response = self.client.get(url)
            response.raise_for_status()
            return self._parse_rss(response.text, source)
        except Exception as e:
            logger.error(f"RSS fetch error {url}: {e}")
            return []

    def _parse_rss(self, xml_content: str, source: str = None) -> List[Dict]:
        """Parse RSS XML into article dicts."""
        articles = []

        try:
            root = ET.fromstring(xml_content)

            # Handle both RSS 2.0 and Atom feeds
            items = root.findall(".//item") or root.findall(
                ".//{http://www.w3.org/2005/Atom}entry"
            )

            for item in items:
                article = self._parse_item(item, source)
                if article and self._is_relevant(article):
                    articles.append(article)

        except ET.ParseError as e:
            logger.error(f"RSS parse error: {e}")

        return articles

    def _parse_item(self, item: ET.Element, source: str = None) -> Optional[Dict]:
        """Parse a single RSS item."""
        # RSS 2.0 format
        title = self._get_text(item, "title")
        link = self._get_text(item, "link")
        pub_date = self._get_text(item, "pubDate")
        description = self._get_text(item, "description")

        # Atom format fallback
        if not link:
            link_elem = item.find("{http://www.w3.org/2005/Atom}link")
            if link_elem is not None:
                link = link_elem.get("href")

        if not title:
            title = self._get_text(item, "{http://www.w3.org/2005/Atom}title")

        if not pub_date:
            pub_date = self._get_text(item, "{http://www.w3.org/2005/Atom}published")
            if not pub_date:
                pub_date = self._get_text(item, "{http://www.w3.org/2005/Atom}updated")

        if not link or not title:
            return None

        return {
            "url": link.strip(),
            "title": self._clean_text(title),
            "description": self._clean_text(description) if description else None,
            "source": source,
            "published_at": self._parse_date(pub_date),
        }

    def _get_text(self, elem: ET.Element, tag: str) -> Optional[str]:
        """Safely get text from element."""
        child = elem.find(tag)
        return child.text if child is not None and child.text else None

    def _clean_text(self, text: str) -> str:
        """Clean HTML and whitespace from text."""
        if not text:
            return ""
        # Remove HTML tags
        text = re.sub(r"<[^>]+>", "", text)
        # Normalize whitespace
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Parse various date formats to ISO."""
        if not date_str:
            return None

        # Common RSS date formats
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",  # RFC 822
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%S%z",  # ISO 8601
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
            "%d %b %Y %H:%M:%S",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.isoformat()
            except ValueError:
                continue

        return None

    def _is_relevant(self, article: Dict) -> bool:
        """Check if article is likely AFL-relevant."""
        text = f"{article.get('title', '')} {article.get('description', '')}".lower()

        # Must contain AFL-related keywords
        afl_keywords = [
            "afl", "footy", "football", "player", "coach", "club",
            "round", "match", "game", "injury", "trade", "draft",
            "premiership", "finals", "mcg", "marvel", "optus",
        ]

        return any(kw in text for kw in afl_keywords)

    def close(self):
        """Close HTTP client."""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
