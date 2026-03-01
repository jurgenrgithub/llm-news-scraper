"""Lane 0: AFL Official Injury List Scraper.

Scrapes https://www.afl.com.au/matches/injury-list and writes directly
to llm-news-service database with signal_strength='strong'.

This bypasses the article pipeline for official structured data.
"""

import json
import logging
import os
import re
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple

import httpx
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# Database config from environment or defaults
DB_CONFIG = {
    "host": os.getenv("LLM_NEWS_DB_HOST", "192.168.6.75"),
    "port": int(os.getenv("LLM_NEWS_DB_PORT", "5432")),
    "dbname": os.getenv("LLM_NEWS_DB_NAME", "llm_news"),
    "user": os.getenv("LLM_NEWS_DB_USER", "llm_news"),
    "password": os.getenv("LLM_NEWS_DB_PASSWORD", ""),
}

AFL_INJURY_URL = "https://www.afl.com.au/matches/injury-list"

# Team name variations
TEAM_ALIASES = {
    "adelaide": "Adelaide Crows",
    "brisbane": "Brisbane Lions",
    "carlton": "Carlton",
    "collingwood": "Collingwood",
    "essendon": "Essendon",
    "fremantle": "Fremantle",
    "geelong": "Geelong Cats",
    "gold coast": "Gold Coast Suns",
    "gws": "GWS Giants",
    "hawthorn": "Hawthorn",
    "melbourne": "Melbourne",
    "north melbourne": "North Melbourne",
    "port adelaide": "Port Adelaide",
    "richmond": "Richmond",
    "st kilda": "St Kilda",
    "sydney": "Sydney Swans",
    "west coast": "West Coast Eagles",
    "western bulldogs": "Western Bulldogs",
}


@contextmanager
def get_cursor():
    """Context manager for database cursor."""
    conn = psycopg2.connect(**DB_CONFIG)
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


def get_current_round_id() -> Optional[int]:
    """Get current round ID from database."""
    with get_cursor() as cursor:
        cursor.execute("SELECT get_current_round() AS round_id")
        result = cursor.fetchone()
        return result["round_id"] if result else None


def get_injury_dimension_id() -> int:
    """Get injury_status dimension ID."""
    with get_cursor() as cursor:
        cursor.execute(
            "SELECT id FROM dimensions WHERE code = 'injury_status'"
        )
        result = cursor.fetchone()
        if not result:
            raise ValueError("injury_status dimension not found")
        return result["id"]


def resolve_player(player_name: str, team_name: str = None) -> Optional[str]:
    """Resolve player name to entity UUID.

    Uses exact match first, then alias match, then fuzzy match.
    """
    with get_cursor() as cursor:
        # Try exact canonical name match
        cursor.execute(
            """SELECT id FROM entities
               WHERE domain = 'afl' AND entity_type = 'player'
               AND LOWER(canonical_name) = LOWER(%s)""",
            (player_name,)
        )
        result = cursor.fetchone()
        if result:
            return str(result["id"])

        # Try alias match
        cursor.execute(
            """SELECT e.id FROM entities e
               JOIN entity_aliases a ON e.id = a.entity_id
               WHERE e.domain = 'afl' AND e.entity_type = 'player'
               AND LOWER(a.alias) = LOWER(%s)""",
            (player_name,)
        )
        result = cursor.fetchone()
        if result:
            return str(result["id"])

        # Try partial match (last name)
        parts = player_name.split()
        if len(parts) >= 2:
            last_name = parts[-1]
            cursor.execute(
                """SELECT id, canonical_name FROM entities
                   WHERE domain = 'afl' AND entity_type = 'player'
                   AND LOWER(canonical_name) LIKE LOWER(%s)
                   LIMIT 5""",
                (f"% {last_name}",)
            )
            results = cursor.fetchall()
            if len(results) == 1:
                return str(results[0]["id"])
            elif len(results) > 1 and team_name:
                # Multiple matches, need team context (would need team in entities)
                logger.warning(f"Multiple matches for {player_name}, using first")
                return str(results[0]["id"])

        logger.warning(f"Could not resolve player: {player_name}")
        return None


def parse_injury_list(html: str) -> List[Dict]:
    """Parse AFL injury list HTML into structured data.

    Returns list of:
        {player, team, injury, return_date, severity}
    """
    injuries = []
    current_team = None

    # Pattern for team headers (e.g., "Adelaide Crows", "Brisbane Lions")
    team_pattern = re.compile(
        r'<h[23][^>]*>([^<]*(?:Crows|Lions|Blues|Magpies|Bombers|Dockers|Cats|'
        r'Suns|Giants|Hawks|Demons|Kangaroos|Power|Tigers|Saints|Swans|Eagles|Bulldogs)[^<]*)</h[23]>',
        re.IGNORECASE
    )

    # More generic team pattern
    team_pattern2 = re.compile(
        r'<(?:h[23]|div)[^>]*class="[^"]*team[^"]*"[^>]*>([^<]+)</(?:h[23]|div)>',
        re.IGNORECASE
    )

    # Row patterns (multiple variations for different table structures)
    row_patterns = [
        # Pattern 1: Standard table row
        re.compile(
            r'<tr[^>]*>\s*<td[^>]*>([^<]+)</td>\s*<td[^>]*>([^<]+)</td>\s*<td[^>]*>([^<]+)</td>',
            re.IGNORECASE | re.DOTALL
        ),
        # Pattern 2: Nested structure
        re.compile(
            r'player["\']?\s*:\s*["\']([^"\']+)["\'].*?injury["\']?\s*:\s*["\']([^"\']+)["\'].*?'
            r'(?:return|estimated)["\']?\s*:\s*["\']([^"\']+)["\']',
            re.IGNORECASE | re.DOTALL
        ),
    ]

    # Try to find team sections
    team_matches = list(team_pattern.finditer(html))
    if not team_matches:
        team_matches = list(team_pattern2.finditer(html))

    # Split by team sections
    for i, team_match in enumerate(team_matches):
        team_name = team_match.group(1).strip()

        # Normalize team name
        team_lower = team_name.lower()
        for alias, canonical in TEAM_ALIASES.items():
            if alias in team_lower:
                team_name = canonical
                break

        # Get section content until next team or end
        start = team_match.end()
        end = team_matches[i + 1].start() if i + 1 < len(team_matches) else len(html)
        section = html[start:end]

        # Extract injury rows from this section
        for pattern in row_patterns:
            for row_match in pattern.finditer(section):
                player = row_match.group(1).strip()
                injury = row_match.group(2).strip()
                return_date = row_match.group(3).strip()

                # Skip header rows
                if player.lower() in ('player', 'name', ''):
                    continue

                # Determine severity
                severity = estimate_severity(return_date)

                injuries.append({
                    "player": player,
                    "team": team_name,
                    "injury": injury,
                    "return_date": return_date,
                    "severity": severity,
                })

    logger.info(f"Parsed {len(injuries)} injuries from HTML")
    return injuries


def estimate_severity(return_date: str) -> str:
    """Estimate injury severity from return date text."""
    text = return_date.lower()

    if any(x in text for x in ['season', 'indefinite', 'unknown']):
        return 'severe'
    elif any(x in text for x in ['test', 'uncertain', 'tba', 'tbc']):
        return 'moderate'
    elif any(x in text for x in ['week', '1-2', '2-3', '3-4']):
        return 'moderate'
    elif any(x in text for x in ['day', 'available', 'round']):
        return 'minor'
    else:
        return 'moderate'


def upsert_injury_snapshot(
    entity_id: str,
    dimension_id: int,
    round_id: int,
    injury_data: Dict
) -> bool:
    """Insert or update injury snapshot in weekly_snapshots."""
    summary = f"{injury_data['injury']} - {injury_data['return_date']}"

    ml_features = {
        "injury_type": injury_data["injury"],
        "return_estimate": injury_data["return_date"],
        "severity": injury_data["severity"],
        "source": "afl_official",
    }

    with get_cursor() as cursor:
        cursor.execute(
            """INSERT INTO weekly_snapshots (
                entity_id, dimension_id, round_id,
                summary, sentiment, signal_strength,
                fantasy_impact, ml_features, confidence,
                article_count, source_article_ids, model_version
            ) VALUES (
                %s, %s, %s,
                %s, 'negative', 'strong',
                %s, %s, 1.0,
                0, '{}', 'lane0_official'
            )
            ON CONFLICT (entity_id, dimension_id, round_id) DO UPDATE SET
                summary = EXCLUDED.summary,
                sentiment = EXCLUDED.sentiment,
                signal_strength = EXCLUDED.signal_strength,
                fantasy_impact = EXCLUDED.fantasy_impact,
                ml_features = EXCLUDED.ml_features,
                confidence = EXCLUDED.confidence,
                model_version = EXCLUDED.model_version,
                generated_at = NOW()
            """,
            (
                entity_id,
                dimension_id,
                round_id,
                summary,
                f"Injury concern: {injury_data['severity']} - {injury_data['return_date']}",
                json.dumps(ml_features),
            )
        )
        return True


def scrape_injury_list() -> Dict:
    """Main entry point: scrape AFL injury list and update database."""
    stats = {
        "injuries_found": 0,
        "players_resolved": 0,
        "snapshots_created": 0,
        "errors": 0,
    }

    logger.info("=" * 50)
    logger.info("Lane 0: AFL Official Injury List Scraper")
    logger.info("=" * 50)

    # Get current round
    round_id = get_current_round_id()
    if not round_id:
        logger.error("Could not determine current round")
        stats["errors"] += 1
        return stats

    logger.info(f"Current round ID: {round_id}")

    # Get injury dimension
    dimension_id = get_injury_dimension_id()
    logger.info(f"Injury dimension ID: {dimension_id}")

    # Fetch injury list page
    try:
        client = httpx.Client(
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }
        )
        response = client.get(AFL_INJURY_URL)
        response.raise_for_status()
        html = response.text
        client.close()
    except Exception as e:
        logger.error(f"Failed to fetch injury list: {e}")
        stats["errors"] += 1
        return stats

    # Parse injuries
    injuries = parse_injury_list(html)
    stats["injuries_found"] = len(injuries)

    # Process each injury
    for injury in injuries:
        player_name = injury["player"]
        team_name = injury.get("team")

        # Resolve player to entity
        entity_id = resolve_player(player_name, team_name)
        if not entity_id:
            logger.warning(f"  Skipping unresolved: {player_name}")
            continue

        stats["players_resolved"] += 1

        # Upsert snapshot
        try:
            upsert_injury_snapshot(entity_id, dimension_id, round_id, injury)
            stats["snapshots_created"] += 1
            logger.info(f"  {player_name}: {injury['injury']} ({injury['return_date']})")
        except Exception as e:
            logger.error(f"  Error saving {player_name}: {e}")
            stats["errors"] += 1

    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("LANE 0 COMPLETE")
    logger.info(f"  Injuries found:     {stats['injuries_found']}")
    logger.info(f"  Players resolved:   {stats['players_resolved']}")
    logger.info(f"  Snapshots created:  {stats['snapshots_created']}")
    logger.info(f"  Errors:             {stats['errors']}")
    logger.info("=" * 50)

    return stats


def main():
    """Entry point for standalone execution."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    scrape_injury_list()


if __name__ == "__main__":
    main()
