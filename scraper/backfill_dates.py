"""Backfill publication dates for existing cached pages.

Run this once to extract dates from all cached HTML pages.

Usage:
    python -m scraper.backfill_dates
"""

import logging
import psycopg2

from scraper.config import DB_CONFIG
from scraper.date_extractor import DateExtractor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def backfill_dates():
    """Extract and store publication dates for all cached pages."""
    extractor = DateExtractor()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Get all pages without published_at
    cur.execute("""
        SELECT id, url, raw_html, source_type
        FROM page_cache
        WHERE published_at IS NULL
        ORDER BY id
    """)

    pages = cur.fetchall()
    logger.info(f"Found {len(pages)} pages without published_at")

    extracted = 0
    failed = 0

    for page_id, url, raw_html, source_type in pages:
        try:
            date = extractor.extract(raw_html)

            if date:
                cur.execute(
                    "UPDATE page_cache SET published_at = %s WHERE id = %s",
                    (date, page_id)
                )
                extracted += 1
                if extracted % 50 == 0:
                    logger.info(f"  Processed {extracted} pages...")
                    conn.commit()
            else:
                failed += 1
                logger.debug(f"  No date found for: {url[:60]}...")

        except Exception as e:
            failed += 1
            logger.warning(f"  Error processing {page_id}: {e}")

    conn.commit()
    cur.close()
    conn.close()

    logger.info("=" * 50)
    logger.info("BACKFILL COMPLETE")
    logger.info(f"  Total processed: {len(pages)}")
    logger.info(f"  Dates extracted: {extracted}")
    logger.info(f"  Failed/no date:  {failed}")
    logger.info(f"  Success rate:    {100 * extracted / len(pages):.1f}%")
    logger.info("=" * 50)


def show_stats():
    """Show extraction statistics by source type."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            source_type,
            COUNT(*) as total,
            COUNT(published_at) as with_date,
            ROUND(100.0 * COUNT(published_at) / COUNT(*), 1) as pct
        FROM page_cache
        GROUP BY source_type
        ORDER BY source_type
    """)

    print("\nExtraction Results by Source Type:")
    print("-" * 50)
    print(f"{'Source':<15} {'Total':>8} {'With Date':>10} {'%':>8}")
    print("-" * 50)

    for row in cur.fetchall():
        print(f"{row[0]:<15} {row[1]:>8} {row[2]:>10} {row[3]:>7}%")

    cur.close()
    conn.close()


if __name__ == "__main__":
    backfill_dates()
    show_stats()
