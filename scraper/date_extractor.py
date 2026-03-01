"""Extract article publication dates from HTML content."""

import re
from datetime import datetime, timezone
from typing import Optional

# Optional BeautifulSoup import for fallback methods
try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False


class DateExtractor:
    """Extract article publication dates from HTML."""

    # JSON-LD pattern: "datePublished":"2026-02-28T11:57:00Z"
    JSON_LD_PATTERN = re.compile(r'"datePublished"\s*:\s*"([^"]+)"')

    # Meta tag pattern: content="2026-02-28T11:57:00Z"
    META_PATTERN = re.compile(
        r'property=["\']article:published_time["\'][^>]*content=["\']([^"\']+)["\']'
        r'|content=["\']([^"\']+)["\'][^>]*property=["\']article:published_time["\']'
    )

    def extract(self, html: str) -> Optional[datetime]:
        """Extract publication date from HTML, trying multiple methods."""

        # Method 1: JSON-LD (most reliable, fastest)
        date = self._extract_json_ld(html)
        if date:
            return date

        # Method 2: Meta tag via regex (fast)
        date = self._extract_meta_regex(html)
        if date:
            return date

        # Method 3: BeautifulSoup fallback (slower but thorough)
        if HAS_BS4:
            date = self._extract_with_soup(html)
            if date:
                return date

        return None

    def _extract_json_ld(self, html: str) -> Optional[datetime]:
        """Extract from JSON-LD datePublished field."""
        match = self.JSON_LD_PATTERN.search(html)
        if match:
            return self._parse_iso_date(match.group(1))
        return None

    def _extract_meta_regex(self, html: str) -> Optional[datetime]:
        """Extract from article:published_time meta tag via regex."""
        match = self.META_PATTERN.search(html)
        if match:
            # Either group 1 or 2 will have the content
            date_str = match.group(1) or match.group(2)
            if date_str:
                return self._parse_iso_date(date_str)
        return None

    def _extract_with_soup(self, html: str) -> Optional[datetime]:
        """Extract using BeautifulSoup for complex cases."""
        soup = BeautifulSoup(html, 'html.parser')

        # Try meta tag
        meta = soup.find('meta', property='article:published_time')
        if meta and meta.get('content'):
            date = self._parse_iso_date(meta['content'])
            if date:
                return date

        # Try og:published_time
        meta = soup.find('meta', property='og:published_time')
        if meta and meta.get('content'):
            date = self._parse_iso_date(meta['content'])
            if date:
                return date

        # Try time element
        time_el = soup.find('time', datetime=True)
        if time_el:
            date = self._parse_iso_date(time_el['datetime'])
            if date:
                return date

        return None

    def _parse_iso_date(self, date_str: str) -> Optional[datetime]:
        """Parse ISO 8601 date string to datetime."""
        if not date_str:
            return None

        # Normalize timezone format
        date_str = date_str.strip()
        date_str = date_str.replace('+00:00', '+0000')
        date_str = date_str.replace('-00:00', '+0000')

        formats = [
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                # Ensure timezone aware (default to UTC)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue

        return None
