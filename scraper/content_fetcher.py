"""Content Fetcher - Extract full article text from URLs."""

import html as html_lib
import logging
import re
from typing import Dict, Optional

from bs4 import BeautifulSoup
import httpx

from scraper.config import FETCH_TIMEOUT

logger = logging.getLogger(__name__)


class ContentFetcher:
    """Fetches and extracts article content from URLs."""

    def __init__(self):
        self.client = httpx.Client(
            timeout=FETCH_TIMEOUT,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-AU,en;q=0.9",
            },
        )

    def fetch(self, url: str) -> Optional[Dict]:
        """Fetch and extract article content from URL."""
        try:
            response = self.client.get(url)
            response.raise_for_status()
            html = response.text

            # Extract content
            title = self._extract_title(html)
            body = self._extract_body(html, url)

            if not body or len(body) < 100:
                logger.warning(f"No substantial content found: {url}")
                return None

            return {
                "title": title,
                "body": body,
                "url": url,
            }

        except Exception as e:
            logger.error(f"Fetch error {url}: {e}")
            return None

    def _extract_title(self, html: str) -> str:
        """Extract article title from HTML."""
        # Try og:title first
        og_match = re.search(r'<meta[^>]*property="og:title"[^>]*content="([^"]+)"', html)
        if og_match:
            return self._clean_text(og_match.group(1))

        # Try <title> tag
        title_match = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE)
        if title_match:
            title = self._clean_text(title_match.group(1))
            # Remove site name suffix
            title = re.sub(r"\s*[|\-–—]\s*[^|\-–—]+$", "", title)
            return title

        return ""

    def _extract_body(self, html: str, url: str) -> str:
        """Extract article body text from HTML."""
        # Site-specific extractors
        if "afl.com.au" in url:
            return self._extract_afl_com(html)
        elif "foxsports.com.au" in url:
            return self._extract_foxsports(html)
        elif "sen.com.au" in url:
            return self._extract_sen(html)
        elif "heraldsun.com.au" in url or "theage.com.au" in url:
            return self._extract_news_corp(html)
        elif "abc.net.au" in url:
            return self._extract_abc(html)
        else:
            return self._extract_generic(html)

    def _extract_afl_com(self, html: str) -> str:
        """Extract from AFL.com.au and club site articles using BeautifulSoup."""
        soup = BeautifulSoup(html, "html.parser")

        # Find article-body div (used by all AFL club sites)
        body_div = soup.find("div", class_=lambda x: x and "article-body" in x)
        if body_div:
            # Remove script/style elements
            for tag in body_div.find_all(["script", "style"]):
                tag.decompose()
            return body_div.get_text(separator=" ", strip=True)

        # Fallback to article tag
        article = soup.find("article")
        if article:
            for tag in article.find_all(["script", "style"]):
                tag.decompose()
            return article.get_text(separator=" ", strip=True)

        return ""

    def _extract_foxsports(self, html: str) -> str:
        """Extract from Fox Sports articles."""
        match = re.search(
            r'<div[^>]*class="[^"]*story-body[^"]*"[^>]*>(.*?)</div>',
            html, re.DOTALL | re.IGNORECASE
        )
        if match:
            return self._html_to_text(match.group(1))
        return self._extract_generic(html)

    def _extract_sen(self, html: str) -> str:
        """Extract from SEN articles."""
        match = re.search(
            r'<div[^>]*class="[^"]*content-body[^"]*"[^>]*>(.*?)</div>',
            html, re.DOTALL | re.IGNORECASE
        )
        if match:
            return self._html_to_text(match.group(1))
        return self._extract_article_tag(html)

    def _extract_news_corp(self, html: str) -> str:
        """Extract from News Corp sites (Herald Sun, The Age)."""
        patterns = [
            r'<div[^>]*class="[^"]*story_body[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*article__body[^"]*"[^>]*>(.*?)</div>',
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
            if match:
                return self._html_to_text(match.group(1))
        return self._extract_generic(html)

    def _extract_abc(self, html: str) -> str:
        """Extract from ABC News articles."""
        patterns = [
            r'<div[^>]*class="[^"]*article-text[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*data-component="BodyBlock"[^>]*>(.*?)</div>',
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
            if match:
                return self._html_to_text(match.group(1))
        return self._extract_article_tag(html)

    def _extract_article_tag(self, html: str) -> str:
        """Extract from <article> tag."""
        match = re.search(r"<article[^>]*>(.*?)</article>", html, re.DOTALL | re.IGNORECASE)
        if match:
            return self._html_to_text(match.group(1))
        return ""

    def _extract_generic(self, html: str) -> str:
        """Generic article extraction using common patterns."""
        patterns = [
            r'<article[^>]*>(.*?)</article>',
            r'<div[^>]*class="[^"]*(?:article|post|content|story)[^"]*-(?:body|content|text)[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*(?:entry-content|post-content|article-content)[^"]*"[^>]*>(.*?)</div>',
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
            if match:
                text = self._html_to_text(match.group(1) if "(" in pattern else match.group(0))
                if len(text) > 200:
                    return text

        # Last resort: extract all <p> tags
        paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html, re.DOTALL | re.IGNORECASE)
        if paragraphs:
            return "\n\n".join(self._html_to_text(p) for p in paragraphs)

        return ""

    def _html_to_text(self, html: str) -> str:
        """Convert HTML to plain text."""
        if not html:
            return ""

        # Remove script and style
        html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)

        # Convert <br> and </p> to newlines
        html = re.sub(r"<br[^>]*>", "\n", html, flags=re.IGNORECASE)
        html = re.sub(r"</p>", "\n\n", html, flags=re.IGNORECASE)

        # Remove all other tags
        html = re.sub(r"<[^>]+>", "", html)

        # Decode HTML entities
        text = html_lib.unescape(html)

        # Normalize whitespace
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()

        return text

    def _clean_text(self, text: str) -> str:
        """Clean text from HTML entities."""
        if not text:
            return ""
        return html_lib.unescape(text).strip()

    def close(self):
        """Close HTTP client."""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
