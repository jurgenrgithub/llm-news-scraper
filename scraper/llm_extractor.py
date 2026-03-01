"""
LLM-based player mention extraction from cached articles.
Uses Claude CLI via subprocess for extraction.
"""

import argparse
import json
import logging
import subprocess
import time
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from scraper.config import DB_CONFIG
from scraper.content_fetcher import ContentFetcher

logger = logging.getLogger(__name__)

# Source tier mapping
SOURCE_TIERS = {
    # Official sources
    "afl.com.au": "official",
    "AFL Official": "official",
    "adelaidefc.com.au": "official",
    "Adelaide Crows": "official",
    "lions.com.au": "official",
    "Brisbane Lions": "official",
    "carltonfc.com.au": "official",
    "Carlton": "official",
    "collingwoodfc.com.au": "official",
    "Collingwood": "official",
    "essendonfc.com.au": "official",
    "Essendon": "official",
    "fremantlefc.com.au": "official",
    "Fremantle": "official",
    "geelongcats.com.au": "official",
    "Geelong": "official",
    "goldcoastfc.com.au": "official",
    "Gold Coast": "official",
    "gwsgiants.com.au": "official",
    "GWS Giants": "official",
    "hawthornfc.com.au": "official",
    "Hawthorn": "official",
    "melbournefc.com.au": "official",
    "Melbourne": "official",
    "nmfc.com.au": "official",
    "North Melbourne": "official",
    "portadelaidefc.com.au": "official",
    "Port Adelaide": "official",
    "richmondfc.com.au": "official",
    "Richmond": "official",
    "saints.com.au": "official",
    "St Kilda": "official",
    "sydneyswans.com.au": "official",
    "Sydney Swans": "official",
    "westcoasteagles.com.au": "official",
    "West Coast": "official",
    "westernbulldogs.com.au": "official",
    "Western Bulldogs": "official",
    # Major sources
    "The Age": "major",
    "theage.com.au": "major",
    "Sydney Morning Herald": "major",
    "smh.com.au": "major",
    "ABC News": "major",
    "abc.net.au": "major",
    "Fox Sports": "major",
    "foxsports.com.au": "major",
    "Herald Sun": "major",
    "heraldsun.com.au": "major",
    "7NEWS": "major",
    "7news.com.au": "major",
    "news.com.au": "major",
    "ESPN": "major",
    "espn.com.au": "major",
    "SEN": "major",
    "sen.com.au": "major",
}

EXTRACTION_PROMPT = '''You are an AFL news analyst extracting player intelligence for SuperCoach fantasy.

Read the ARTICLE and extract every FANTASY-RELEVANT player mention. Output a JSON object:

{
  "article_id": "{article_id}",
  "source": "{source_name}",
  "source_url": "{source_url}",
  "article_date": "{article_date}",
  "ingest_timestamp": "{ingest_timestamp}",
  "mentions": [
    {
      "player_id": null,
      "player": "Full Name",
      "team": "Team Name",
      "match_type": "exact|alias|initials",
      "match_snippet": "Exact sentence/phrase that triggered match",
      "signal": "injury|selection|form|role|contract",
      "signal_strength": 0.0-1.0,
      "summary": "One-sentence - what happened",
      "availability": 0.0-1.0,
      "impact_weeks": null,
      "fantasy_impact_score": -100 to 100,
      "action": "start|bench|monitor|no_action",
      "sentiment": "positive|negative|neutral",
      "confidence": 0.0-1.0,
      "quote": "Key quote if any",
      "is_official_source": {is_official},
      "source_tier": "{source_tier}"
    }
  ],
  "errors": [],
  "processing_ms": 0
}

AVAILABILITY SCALE:
- 0.0 = Ruled out, season over, delisted
- 0.2 = Likely out 4+ weeks, surgery required
- 0.4 = Out 1-3 weeks, injury concern
- 0.6 = Test/doubt, may play reduced minutes
- 0.8 = Available but managing load, slight concern
- 1.0 = Fully fit, no concerns

FANTASY_IMPACT_SCORE:
- Negative = bad for fantasy owners (-100 = disaster, -50 = significant concern)
- Zero = neutral
- Positive = good for fantasy owners (+50 = boost, +100 = major upside)

SOURCE TIER (already set, use as provided):
- official = AFL.com.au, club websites
- major = The Age, ABC, Herald Sun, Fox Sports
- social = Twitter, player posts
- other = blogs, fan sites

RULES:
1. Only include players with fantasy-relevant signals. Skip generic mentions.
2. Set signal_strength >= 0.9 if source is official or headline contains the signal.
3. Use confidence to reflect extraction ambiguity (< 0.6 = needs review).
4. For injuries, include injury details in summary.
5. match_snippet must be the EXACT text from the article.

For injury signals, also populate these fields in the mention:
- injury_type: "soft tissue|bone|concussion|illness|managed load|unknown"
- body_part: "hamstring|knee|ankle|shoulder|calf|groin|back|head|other"
- severity: "minor|moderate|serious|season-ending"
- expected_return: "this week|1-2 weeks|3-4 weeks|6+ weeks|unknown"
- surgery_probability: 0.0-1.0
- playing_through: true|false
- historical_concern: true|false
- recommended_followup: "check_team_sheet|check_official_injury_list|monitor_journalists|medical_update"

ARTICLE:
{article_text}

Output ONLY valid JSON, no markdown, no explanation.'''


class LLMExtractor:
    """Extract player mentions using Claude CLI."""

    def __init__(self, db_config: Dict = None):
        self.db_config = db_config or DB_CONFIG
        self.content_fetcher = ContentFetcher()

    def get_connection(self):
        """Get database connection."""
        return psycopg2.connect(**self.db_config)

    def get_unprocessed_articles(self, limit: int = 10) -> List[Dict]:
        """Get articles that haven't been extracted yet."""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, url, raw_html, source_type, source_name, published_at
                    FROM page_cache
                    WHERE extracted_at IS NULL
                      AND source_type != 'injury_list'
                    ORDER BY published_at DESC NULLS LAST
                    LIMIT %s
                """, (limit,))
                return [dict(row) for row in cur.fetchall()]

    def get_article_by_id(self, article_id: int) -> Optional[Dict]:
        """Get single article by ID."""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, url, raw_html, source_type, source_name, published_at
                    FROM page_cache
                    WHERE id = %s
                """, (article_id,))
                row = cur.fetchone()
                return dict(row) if row else None

    def extract_text(self, html: str, url: str) -> str:
        """Extract article text from HTML."""
        return self.content_fetcher._extract_body(html, url)

    def get_source_tier(self, source_name: str, url: str) -> tuple:
        """Determine source tier and official status."""
        # Check source name first
        if source_name in SOURCE_TIERS:
            tier = SOURCE_TIERS[source_name]
            return tier, tier == "official"

        # Check URL domain
        for domain, tier in SOURCE_TIERS.items():
            if domain in url:
                return tier, tier == "official"

        return "other", False

    def build_prompt(self, article: Dict, text: str) -> str:
        """Build extraction prompt with article metadata."""
        source_tier, is_official = self.get_source_tier(
            article.get("source_name", ""),
            article.get("url", "")
        )

        article_date = ""
        if article.get("published_at"):
            article_date = article["published_at"].strftime("%Y-%m-%d")

        return EXTRACTION_PROMPT.format(
            article_id=str(article["id"]),
            source_name=article.get("source_name", "Unknown"),
            source_url=article.get("url", ""),
            article_date=article_date,
            ingest_timestamp=datetime.utcnow().isoformat() + "Z",
            is_official=str(is_official).lower(),
            source_tier=source_tier,
            article_text=text[:12000]  # Limit context size
        )

    def call_claude(self, prompt: str) -> str:
        """Call Claude CLI and return response."""
        try:
            result = subprocess.run(
                ["claude", "-p", prompt, "--output-format", "text"],
                capture_output=True,
                text=True,
                timeout=180,
                encoding="utf-8"
            )

            if result.returncode != 0:
                logger.error(f"Claude CLI error: {result.stderr}")
                return None

            return result.stdout.strip()

        except subprocess.TimeoutExpired:
            logger.error("Claude CLI timeout")
            return None
        except Exception as e:
            logger.error(f"Claude CLI exception: {e}")
            return None

    def parse_response(self, response: str) -> Optional[Dict]:
        """Parse JSON response from Claude."""
        if not response:
            return None

        # Try to extract JSON from response
        try:
            # Direct JSON parse
            return json.loads(response)
        except json.JSONDecodeError:
            pass

        # Try to find JSON in response (sometimes wrapped in markdown)
        try:
            start = response.find("{")
            end = response.rfind("}") + 1
            if start >= 0 and end > start:
                return json.loads(response[start:end])
        except json.JSONDecodeError:
            pass

        logger.error(f"Failed to parse response: {response[:200]}...")
        return None

    def process_article(self, article: Dict) -> Dict:
        """Process single article through LLM extraction."""
        start_time = time.time()
        result = {
            "article_id": str(article["id"]),
            "mentions": [],
            "errors": [],
            "processing_ms": 0
        }

        # Extract text
        text = self.extract_text(article["raw_html"], article["url"])
        if len(text) < 100:
            result["errors"].append({
                "article_id": str(article["id"]),
                "reason": "Article too short"
            })
            return result

        # Build prompt
        prompt = self.build_prompt(article, text)

        # Call Claude
        response = self.call_claude(prompt)
        if not response:
            result["errors"].append({
                "article_id": str(article["id"]),
                "reason": "Claude CLI failed"
            })
            return result

        # Parse response
        parsed = self.parse_response(response)
        if not parsed:
            result["errors"].append({
                "article_id": str(article["id"]),
                "reason": "Failed to parse JSON response"
            })
            return result

        result = parsed
        result["processing_ms"] = int((time.time() - start_time) * 1000)

        return result

    def store_mentions(self, article_id: int, result: Dict):
        """Store extracted mentions to database."""
        mentions = result.get("mentions", [])
        if not mentions:
            return 0

        stored = 0
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for mention in mentions:
                    try:
                        cur.execute("""
                            INSERT INTO player_mentions (
                                article_id, source_url, source_name, source_tier,
                                is_official_source, article_date,
                                player_name, team, match_type, match_snippet,
                                signal_type, signal_strength, summary, quote,
                                availability, impact_weeks, fantasy_impact_score,
                                action, sentiment, confidence,
                                injury_type, body_part, severity, expected_return,
                                surgery_probability, playing_through, historical_concern,
                                recommended_followup, processing_ms, model_version
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            ON CONFLICT (article_id, player_name, signal_type)
                            DO UPDATE SET
                                summary = EXCLUDED.summary,
                                availability = EXCLUDED.availability,
                                confidence = EXCLUDED.confidence,
                                extracted_at = NOW()
                        """, (
                            article_id,
                            result.get("source_url", ""),
                            result.get("source", ""),
                            mention.get("source_tier", "other"),
                            mention.get("is_official_source", False),
                            result.get("article_date"),
                            mention.get("player", ""),
                            mention.get("team", ""),
                            mention.get("match_type", "unmatched"),
                            mention.get("match_snippet", ""),
                            mention.get("signal", ""),
                            mention.get("signal_strength"),
                            mention.get("summary", ""),
                            mention.get("quote", ""),
                            mention.get("availability"),
                            mention.get("impact_weeks"),
                            mention.get("fantasy_impact_score"),
                            mention.get("action", "no_action"),
                            mention.get("sentiment", "neutral"),
                            mention.get("confidence"),
                            mention.get("injury_type"),
                            mention.get("body_part"),
                            mention.get("severity"),
                            mention.get("expected_return"),
                            mention.get("surgery_probability"),
                            mention.get("playing_through"),
                            mention.get("historical_concern"),
                            mention.get("recommended_followup"),
                            result.get("processing_ms", 0),
                            "claude-cli-v1"
                        ))
                        stored += 1
                    except Exception as e:
                        logger.error(f"Error storing mention: {e}")

                conn.commit()

        return stored

    def mark_extracted(self, article_id: int, error: str = None):
        """Mark article as extracted."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                if error:
                    cur.execute("""
                        UPDATE page_cache
                        SET extraction_error = %s
                        WHERE id = %s
                    """, (error, article_id))
                else:
                    cur.execute("""
                        UPDATE page_cache
                        SET extracted_at = NOW()
                        WHERE id = %s
                    """, (article_id,))
                conn.commit()

    def run(self, batch_size: int = 10, article_id: int = None):
        """Process articles through LLM extraction."""
        if article_id:
            # Single article mode
            article = self.get_article_by_id(article_id)
            if not article:
                logger.error(f"Article {article_id} not found")
                return

            articles = [article]
        else:
            # Batch mode
            articles = self.get_unprocessed_articles(batch_size)

        if not articles:
            logger.info("No unprocessed articles found")
            return

        logger.info(f"Processing {len(articles)} articles...")

        total_mentions = 0
        total_errors = 0

        for i, article in enumerate(articles, 1):
            logger.info(f"[{i}/{len(articles)}] Article {article['id']}: {article['url'][:60]}...")

            try:
                result = self.process_article(article)

                if result.get("errors"):
                    for err in result["errors"]:
                        logger.warning(f"  Error: {err.get('reason')}")
                    total_errors += 1
                    self.mark_extracted(article["id"], result["errors"][0].get("reason"))
                else:
                    mentions = result.get("mentions", [])
                    stored = self.store_mentions(article["id"], result)
                    total_mentions += stored
                    logger.info(f"  Extracted {len(mentions)} mentions, stored {stored}")
                    self.mark_extracted(article["id"])

            except Exception as e:
                logger.error(f"  Exception: {e}")
                total_errors += 1
                self.mark_extracted(article["id"], str(e))

        logger.info("=" * 50)
        logger.info(f"EXTRACTION COMPLETE")
        logger.info(f"  Articles processed: {len(articles)}")
        logger.info(f"  Total mentions: {total_mentions}")
        logger.info(f"  Errors: {total_errors}")
        logger.info("=" * 50)

    def close(self):
        """Clean up resources."""
        self.content_fetcher.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="LLM Player Mention Extraction")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of articles to process")
    parser.add_argument("--article-id", type=int, help="Process single article by ID")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    extractor = LLMExtractor()
    try:
        extractor.run(batch_size=args.batch_size, article_id=args.article_id)
    finally:
        extractor.close()


if __name__ == "__main__":
    main()
