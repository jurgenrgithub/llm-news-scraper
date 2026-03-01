"""Raw HTML Page Cache - stores fetched pages to PostgreSQL.

Simple cache layer: URL in, HTML stored, deduplication by URL hash.
"""

import hashlib
import logging
from contextlib import contextmanager
from typing import Dict, List, Optional, Set

import psycopg2
from psycopg2.extras import RealDictCursor

from scraper.config import DB_CONFIG

logger = logging.getLogger(__name__)


def url_hash(url: str) -> str:
    """SHA256 hash of URL for deduplication."""
    return hashlib.sha256(url.encode()).hexdigest()


def content_hash(html: str) -> str:
    """SHA256 hash of HTML content."""
    return hashlib.sha256(html.encode()).hexdigest()


class PageCache:
    """Database layer for raw HTML page caching."""

    def __init__(self, db_config: Dict = None):
        self.db_config = db_config or DB_CONFIG
        self._conn = None

    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor."""
        conn = psycopg2.connect(**self.db_config)
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def has_url(self, url: str) -> bool:
        """Check if URL is already cached."""
        hash_val = url_hash(url)
        with self.get_cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM page_cache WHERE url_hash = %s",
                (hash_val,)
            )
            return cursor.fetchone() is not None

    def get_cached_hashes(self, urls: List[str]) -> Set[str]:
        """Get set of URL hashes that are already cached."""
        if not urls:
            return set()

        hashes = [url_hash(u) for u in urls]

        with self.get_cursor() as cursor:
            cursor.execute(
                "SELECT url_hash FROM page_cache WHERE url_hash = ANY(%s)",
                (hashes,)
            )
            return {row["url_hash"] for row in cursor.fetchall()}

    def get_uncached_urls(self, urls: List[str]) -> List[str]:
        """Filter to URLs not yet cached."""
        if not urls:
            return []

        cached_hashes = self.get_cached_hashes(urls)
        return [u for u in urls if url_hash(u) not in cached_hashes]

    def store(
        self,
        url: str,
        html: str,
        source_type: str,
        source_name: str = None,
        http_status: int = 200,
    ) -> Optional[int]:
        """Store raw HTML page, return page_cache.id."""
        hash_url = url_hash(url)
        hash_content = content_hash(html)

        with self.get_cursor() as cursor:
            try:
                cursor.execute(
                    """INSERT INTO page_cache (
                        url, url_hash, raw_html, content_hash,
                        source_type, source_name, http_status, content_length
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url_hash) DO NOTHING
                    RETURNING id""",
                    (
                        url,
                        hash_url,
                        html,
                        hash_content,
                        source_type,
                        source_name,
                        http_status,
                        len(html),
                    )
                )
                result = cursor.fetchone()
                return result["id"] if result else None
            except Exception as e:
                logger.error(f"Error storing page {url}: {e}")
                return None

    def get(self, url: str) -> Optional[Dict]:
        """Retrieve cached page by URL."""
        hash_val = url_hash(url)
        with self.get_cursor() as cursor:
            cursor.execute(
                """SELECT id, url, raw_html, content_hash,
                          source_type, source_name, http_status,
                          content_length, fetched_at
                   FROM page_cache WHERE url_hash = %s""",
                (hash_val,)
            )
            return cursor.fetchone()

    def get_stats(self) -> Dict:
        """Get cache statistics by source."""
        with self.get_cursor() as cursor:
            cursor.execute(
                """SELECT source_type, source_name, COUNT(*) as count,
                          MAX(fetched_at) as latest
                   FROM page_cache
                   GROUP BY source_type, source_name
                   ORDER BY source_type, count DESC"""
            )
            rows = cursor.fetchall()

            cursor.execute("SELECT COUNT(*) as total FROM page_cache")
            total = cursor.fetchone()["total"]

            return {
                "total": total,
                "by_source": [dict(r) for r in rows],
            }

    def count(self) -> int:
        """Get total cached pages."""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM page_cache")
            return cursor.fetchone()["count"]
