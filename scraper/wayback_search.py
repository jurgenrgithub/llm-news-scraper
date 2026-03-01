"""Historical Article Search via Wayback Machine.

Searches Internet Archive Wayback Machine for historical AFL player articles
and ingests them into llm-news-service for processing.

Usage:
    python -m scraper.wayback_search --pilot --api-url http://192.168.6.75:8787
    python -m scraper.wayback_search --season 2024 --full-season --all-players
    python -m scraper.wayback_search --season 2024 --round 12 --dry-run
"""

import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Dict, Optional, Tuple

import click
import httpx
import psycopg2
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Rate limiting
CDX_DELAY_SECONDS = 1.0
FETCH_DELAY_SECONDS = 1.5

# Wayback Machine endpoints
CDX_API_URL = "http://web.archive.org/cdx/search/cdx"
ARCHIVE_BASE_URL = "https://web.archive.org/web"

# News source paths to search
NEWS_SOURCES = [
    ("afl.com.au", "/news"),
    ("foxsports.com.au", "/afl"),
    ("sen.com.au", "/news"),
    ("theage.com.au", "/sport/afl"),
    ("heraldsun.com.au", "/sport/afl"),
]

# Database config
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "192.168.6.170"),
    "database": os.environ.get("DB_NAME", "llm_news"),
    "user": os.environ.get("DB_USER", "llm_news"),
    "password": os.environ.get("DB_PASSWORD", "llm_news_dev_2026"),
}

# Pilot configuration (for backwards compatibility)
PILOT_PLAYERS = [
    "Marcus Bontempelli",
    "Max Gawn",
    "Tristan Xerri",
    "Zak Butters",
    "Nick Daicos",
    "Brodie Grundy",
    "Lachie Neale",
    "Josh Dunkley",
    "Harry Sheezel",
    "Andrew Brayshaw",
]

PILOT_ROUNDS = [
    {"season": 2024, "round": 12, "start": "2024-05-30", "end": "2024-06-02", "round_id": 47},
    {"season": 2024, "round": 18, "start": "2024-07-11", "end": "2024-07-14", "round_id": 48},
    {"season": 2024, "round": 24, "start": "2024-08-22", "end": "2024-08-25", "round_id": 49},
    {"season": 2025, "round": 6,  "start": "2025-04-17", "end": "2025-04-20", "round_id": 56},
    {"season": 2025, "round": 12, "start": "2025-05-29", "end": "2025-06-01", "round_id": 57},
    {"season": 2025, "round": 18, "start": "2025-07-10", "end": "2025-07-13", "round_id": 58},
]

# AFL Season start dates
SEASON_STARTS = {
    2024: date(2024, 3, 7),
    2025: date(2025, 3, 6),
}


def generate_season_rounds(season: int) -> List[Dict]:
    """Generate round configs for a full AFL season (24 rounds)."""
    if season not in SEASON_STARTS:
        raise ValueError(f"Season {season} not configured. Add to SEASON_STARTS.")

    rounds = []
    start = SEASON_STARTS[season]

    for r in range(1, 25):  # Rounds 1-24
        round_start = start + timedelta(weeks=r-1)
        round_end = round_start + timedelta(days=3)

        rounds.append({
            "season": season,
            "round": r,
            "start": round_start.isoformat(),
            "end": round_end.isoformat(),
            "round_id": None,  # Will lookup from DB
        })

    return rounds


def load_players_from_db() -> List[str]:
    """Load all AFL player names from database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            # Get canonical names
            cur.execute("""
                SELECT canonical_name FROM entities
                WHERE domain = 'afl' AND entity_type = 'player'
            """)
            players = [row[0] for row in cur.fetchall()]

            # Get aliases
            cur.execute("""
                SELECT a.alias FROM entity_aliases a
                JOIN entities e ON a.entity_id = e.id
                WHERE e.domain = 'afl' AND e.entity_type = 'player'
            """)
            aliases = [row[0] for row in cur.fetchall()]

        conn.close()
        return players + aliases
    except Exception as e:
        logger.warning(f"Could not load players from DB: {e}")
        return []


def lookup_round_id(season: int, round_num: int) -> Optional[int]:
    """Lookup round_id from database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            # Join with seasons table to get season year
            cur.execute("""
                SELECT r.id FROM rounds r
                JOIN seasons s ON r.season_id = s.id
                WHERE s.year = %s AND r.round = %s
            """, (season, round_num))
            row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        logger.warning(f"Could not lookup round_id: {e}")
        return None


@dataclass
class ArchivedArticle:
    """Represents an archived article from Wayback Machine."""
    url: str
    archive_url: str
    timestamp: str
    title: str
    body: str
    source: str
    published_at: str
    players_mentioned: List[str]


class WaybackHistoricalSearch:
    """Search Wayback Machine for historical AFL articles."""

    def __init__(self, api_base_url: str, all_players: bool = False):
        self.api_base_url = api_base_url
        self.http_client = httpx.Client(timeout=30, follow_redirects=True)
        self.seen_digests = set()
        self.seen_urls = set()

        # Load player list
        if all_players:
            players = load_players_from_db()
            if not players:
                logger.warning("No players from DB, falling back to pilot list")
                players = PILOT_PLAYERS
            logger.info(f"Loaded {len(players)} players from database")
        else:
            players = PILOT_PLAYERS

        # Compile player name patterns
        self.player_patterns = []
        seen = set()
        for player in players:
            if player.lower() in seen:
                continue
            seen.add(player.lower())
            # Full name and surname only
            pattern = re.compile(
                rf'\b{re.escape(player)}\b|\b{re.escape(player.split()[-1])}\b',
                re.IGNORECASE
            )
            self.player_patterns.append((player, pattern))

    def search_cdx(
        self,
        domain: str,
        path: str,
        start_date: str,
        end_date: str,
        limit: int = 50
    ) -> List[Dict]:
        """Query CDX API for archived URLs.

        Args:
            domain: e.g., "afl.com.au"
            path: e.g., "/news"
            start_date: YYYYMMDD format
            end_date: YYYYMMDD format
            limit: Max results to return

        Returns:
            List of snapshot dicts with timestamp, original URL, etc.
        """
        url = f"{domain}{path}"

        params = {
            "url": url,
            "matchType": "prefix",
            "output": "json",
            "from": start_date,
            "to": end_date,
            "filter": "statuscode:200",
            "filter": "mimetype:text/html",
            "collapse": "digest",  # Dedupe identical content
            "fl": "timestamp,original,digest,statuscode",
            "limit": limit,
        }

        try:
            response = self.http_client.get(CDX_API_URL, params=params)
            response.raise_for_status()

            data = response.json()
            if len(data) < 2:
                return []

            # First row is headers
            headers = data[0]
            snapshots = []
            for row in data[1:]:
                snapshot = dict(zip(headers, row))
                # Skip if we've seen this digest
                if snapshot.get("digest") in self.seen_digests:
                    continue
                self.seen_digests.add(snapshot.get("digest"))
                snapshots.append(snapshot)

            return snapshots

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limited by CDX API, waiting 60s...")
                time.sleep(60)
            else:
                logger.error(f"CDX API error: {e}")
            return []
        except Exception as e:
            logger.error(f"CDX API error: {e}")
            return []

    def fetch_archived_content(self, timestamp: str, original_url: str) -> Optional[str]:
        """Fetch HTML content from archived snapshot.

        Uses `id_` modifier to get raw content without URL rewriting.
        """
        # Skip if we've seen this URL
        if original_url in self.seen_urls:
            return None
        self.seen_urls.add(original_url)

        archive_url = f"{ARCHIVE_BASE_URL}/{timestamp}id_/{original_url}"

        try:
            response = self.http_client.get(
                archive_url,
                headers={"User-Agent": "FantasyEdge/1.0 (+https://github.com/fantasyedge)"}
            )
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.debug(f"Fetch error {archive_url}: {e}")
            return None

    def extract_article(self, html: str, url: str, timestamp: str) -> Optional[ArchivedArticle]:
        """Extract article content from HTML.

        Returns None if not a valid article or no players mentioned.
        """
        try:
            soup = BeautifulSoup(html, "html.parser")

            # Remove script/style elements
            for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
                tag.decompose()

            # Get title
            title = ""
            if soup.title:
                title = soup.title.get_text(strip=True)
            if not title:
                h1 = soup.find("h1")
                if h1:
                    title = h1.get_text(strip=True)

            # Get body text
            body_text = soup.get_text(separator=" ", strip=True)

            # Clean up whitespace
            body_text = re.sub(r'\s+', ' ', body_text)

            # Skip if too short (not a real article)
            if len(body_text) < 500:
                return None

            # Check for player mentions
            players_mentioned = self.find_mentioned_players(body_text)
            if not players_mentioned:
                return None

            # Extract source from URL
            source = self._extract_source(url)

            # Convert timestamp to date
            published_at = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}"

            return ArchivedArticle(
                url=url,
                archive_url=f"{ARCHIVE_BASE_URL}/{timestamp}/{url}",
                timestamp=timestamp,
                title=title[:500],  # Limit title length
                body=body_text[:10000],  # Limit body length
                source=source,
                published_at=published_at,
                players_mentioned=players_mentioned,
            )

        except Exception as e:
            logger.debug(f"Extract error: {e}")
            return None

    def find_mentioned_players(self, text: str) -> List[str]:
        """Find which players are mentioned in text."""
        mentioned = []
        for player_name, pattern in self.player_patterns:
            if pattern.search(text):
                mentioned.append(player_name)
        return mentioned

    def _extract_source(self, url: str) -> str:
        """Extract source name from URL."""
        source_map = {
            "afl.com.au": "AFL.com.au",
            "foxsports.com.au": "Fox Sports",
            "sen.com.au": "SEN",
            "heraldsun.com.au": "Herald Sun",
            "theage.com.au": "The Age",
            "abc.net.au": "ABC News",
            "news.com.au": "News.com.au",
            "smh.com.au": "Sydney Morning Herald",
        }
        url_lower = url.lower()
        for domain, name in source_map.items():
            if domain in url_lower:
                return name
        return "Unknown"

    def ingest_article(self, article: ArchivedArticle, round_id: int) -> bool:
        """POST article to llm-news-service API."""
        payload = {
            "url": article.url,
            "title": article.title,
            "body": article.body,
            "source": article.source,
            "published_at": article.published_at,
            "round_id": round_id,
        }

        try:
            response = self.http_client.post(
                f"{self.api_base_url}/articles/ingest",
                json=payload,
            )
            if response.status_code in (200, 201):
                logger.info(f"Ingested: {article.title[:50]}...")
                return True
            elif response.status_code == 409:
                logger.debug(f"Duplicate: {article.url}")
                return False
            else:
                logger.warning(f"Ingest failed ({response.status_code}): {article.url}")
                return False
        except Exception as e:
            logger.error(f"Ingest error: {e}")
            return False

    def search_round(
        self,
        round_info: Dict,
        max_per_source: int = 20,
        dry_run: bool = False
    ) -> Dict:
        """Search for articles for a specific round.

        Returns stats dict with counts.
        """
        stats = {
            "cdx_queries": 0,
            "snapshots_found": 0,
            "fetched": 0,
            "matched": 0,
            "ingested": 0,
        }

        # Calculate search window (7 days before round to round end)
        round_start = date.fromisoformat(round_info["start"])
        round_end = date.fromisoformat(round_info["end"])
        search_start = round_start - timedelta(days=7)

        start_date = search_start.strftime("%Y%m%d")
        end_date = round_end.strftime("%Y%m%d")

        # Get round_id (from config or DB lookup)
        round_id = round_info.get("round_id")
        if round_id is None:
            round_id = lookup_round_id(round_info["season"], round_info["round"])

        logger.info(f"Searching {round_info['season']} R{round_info['round']} "
                    f"({start_date} to {end_date}), round_id={round_id}")

        for domain, path in NEWS_SOURCES:
            logger.info(f"  {domain}{path}...")

            # Query CDX API
            snapshots = self.search_cdx(domain, path, start_date, end_date, max_per_source)
            stats["cdx_queries"] += 1
            stats["snapshots_found"] += len(snapshots)

            time.sleep(CDX_DELAY_SECONDS)

            # Fetch and process each snapshot
            for snapshot in snapshots:
                timestamp = snapshot.get("timestamp")
                original_url = snapshot.get("original")

                if not timestamp or not original_url:
                    continue

                html = self.fetch_archived_content(timestamp, original_url)
                if not html:
                    continue
                stats["fetched"] += 1

                article = self.extract_article(html, original_url, timestamp)
                if not article:
                    continue
                stats["matched"] += 1

                if dry_run:
                    logger.info(f"    [DRY RUN] Would ingest: {article.title[:60]}...")
                    logger.info(f"      Players: {', '.join(article.players_mentioned[:5])}")
                else:
                    if self.ingest_article(article, round_id):
                        stats["ingested"] += 1

                time.sleep(FETCH_DELAY_SECONDS)

        return stats

    def close(self):
        """Clean up resources."""
        self.http_client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


@click.command()
@click.option("--pilot", is_flag=True, help="Run pilot search (10 players, 6 rounds)")
@click.option("--season", "-s", type=int, help="Season to search (2024 or 2025)")
@click.option("--round", "-r", "round_num", type=int, help="Single round to search")
@click.option("--full-season", is_flag=True, help="Search all 24 rounds for the season")
@click.option("--all-players", is_flag=True, help="Match all AFL players from database")
@click.option("--max-per-source", "-m", type=int, default=30,
              help="Max articles per source per round")
@click.option("--dry-run", is_flag=True, help="Preview only, don't ingest")
@click.option("--api-url", default="http://192.168.6.75:8787", help="API base URL")
def main(pilot, season, round_num, full_season, all_players, max_per_source, dry_run, api_url):
    """Search Wayback Machine for historical AFL articles."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Determine rounds to search
    if pilot:
        rounds_to_search = PILOT_ROUNDS
    elif season and full_season:
        rounds_to_search = generate_season_rounds(season)
    elif season and round_num:
        # Single specific round
        rounds_to_search = [r for r in PILOT_ROUNDS
                           if r["season"] == season and r["round"] == round_num]
        if not rounds_to_search:
            # Generate single round config
            if season in SEASON_STARTS:
                start = SEASON_STARTS[season] + timedelta(weeks=round_num-1)
                rounds_to_search = [{
                    "season": season,
                    "round": round_num,
                    "start": start.isoformat(),
                    "end": (start + timedelta(days=3)).isoformat(),
                    "round_id": None,
                }]
            else:
                click.echo(f"Season {season} not configured")
                return
    elif season:
        rounds_to_search = [r for r in PILOT_ROUNDS if r["season"] == season]
    else:
        click.echo("Specify --pilot, --season with --full-season, or --season with --round")
        return

    click.echo("=" * 60)
    click.echo("Wayback Machine Historical Search")
    click.echo("=" * 60)
    click.echo(f"Rounds: {len(rounds_to_search)}")
    click.echo(f"Sources: {len(NEWS_SOURCES)}")
    click.echo(f"All players: {all_players}")
    click.echo(f"Max per source: {max_per_source}")
    click.echo(f"Dry run: {dry_run}")
    click.echo(f"API: {api_url}")
    click.echo()

    total_stats = {
        "cdx_queries": 0,
        "snapshots_found": 0,
        "fetched": 0,
        "matched": 0,
        "ingested": 0,
    }

    with WaybackHistoricalSearch(api_url, all_players=all_players) as searcher:
        click.echo(f"Player patterns loaded: {len(searcher.player_patterns)}")
        click.echo()

        for round_info in rounds_to_search:
            click.echo(f"\n--- {round_info['season']} Round {round_info['round']} ---")

            stats = searcher.search_round(round_info, max_per_source, dry_run)

            for key in total_stats:
                total_stats[key] += stats[key]

            click.echo(f"  Snapshots: {stats['snapshots_found']}, "
                       f"Fetched: {stats['fetched']}, "
                       f"Matched: {stats['matched']}, "
                       f"Ingested: {stats['ingested']}")

    click.echo("\n" + "=" * 60)
    click.echo("Search Complete!")
    click.echo("=" * 60)
    click.echo(f"  CDX queries: {total_stats['cdx_queries']}")
    click.echo(f"  Snapshots found: {total_stats['snapshots_found']}")
    click.echo(f"  Pages fetched: {total_stats['fetched']}")
    click.echo(f"  Player matches: {total_stats['matched']}")
    click.echo(f"  Articles ingested: {total_stats['ingested']}")


if __name__ == "__main__":
    main()
