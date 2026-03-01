"""
Player name matching to canonical roster.

Matches extracted player names to database IDs using:
1. Exact match
2. Case-insensitive match
3. Initials expansion (e.g., "C. Petracca" -> "Christian Petracca")
4. Team confirmation for disambiguation
"""

import logging
import re
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# Team name normalization
TEAM_ALIASES = {
    "Adelaide": ["Adelaide", "Adelaide Crows", "Crows"],
    "Brisbane": ["Brisbane", "Brisbane Lions", "Lions"],
    "Carlton": ["Carlton", "Carlton Blues", "Blues"],
    "Collingwood": ["Collingwood", "Collingwood Magpies", "Magpies", "Pies"],
    "Essendon": ["Essendon", "Essendon Bombers", "Bombers", "Dons"],
    "Fremantle": ["Fremantle", "Fremantle Dockers", "Dockers", "Freo"],
    "Geelong": ["Geelong", "Geelong Cats", "Cats"],
    "Gold Coast": ["Gold Coast", "Gold Coast Suns", "Suns"],
    "GWS": ["GWS", "GWS Giants", "Greater Western Sydney", "Giants"],
    "Hawthorn": ["Hawthorn", "Hawthorn Hawks", "Hawks"],
    "Melbourne": ["Melbourne", "Melbourne Demons", "Demons"],
    "North Melbourne": ["North Melbourne", "North Melbourne Kangaroos", "Kangaroos", "Roos", "North"],
    "Port Adelaide": ["Port Adelaide", "Port Adelaide Power", "Power", "Port"],
    "Richmond": ["Richmond", "Richmond Tigers", "Tigers"],
    "St Kilda": ["St Kilda", "St Kilda Saints", "Saints"],
    "Sydney": ["Sydney", "Sydney Swans", "Swans"],
    "West Coast": ["West Coast", "West Coast Eagles", "Eagles"],
    "Western Bulldogs": ["Western Bulldogs", "Bulldogs", "Footscray", "Dogs"],
}

# Build reverse lookup
TEAM_NORMALIZE = {}
for canonical, aliases in TEAM_ALIASES.items():
    for alias in aliases:
        TEAM_NORMALIZE[alias.lower()] = canonical


class PlayerMatcher:
    """Match extracted player names to canonical database IDs."""

    def __init__(self, db_config: Dict):
        """Initialize with database config for players table."""
        self.db_config = db_config
        self._cache: Dict[str, Tuple[Optional[int], str]] = {}
        self._players: List[Dict] = []
        self._loaded = False

    def _load_players(self):
        """Load all players from database."""
        if self._loaded:
            return

        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT id, name, team
                        FROM players
                        ORDER BY name
                    """)
                    self._players = [dict(row) for row in cur.fetchall()]
                    self._loaded = True
                    logger.info(f"Loaded {len(self._players)} players from roster")
        except Exception as e:
            logger.error(f"Failed to load players: {e}")
            self._players = []

    def normalize_team(self, team: str) -> str:
        """Normalize team name to canonical form."""
        if not team:
            return ""
        return TEAM_NORMALIZE.get(team.lower().strip(), team)

    def match(self, name: str, team: str = None) -> Tuple[Optional[int], str]:
        """
        Match player name to database ID.

        Args:
            name: Player name from extraction
            team: Optional team name for disambiguation

        Returns:
            Tuple of (player_id, match_type)
            match_type is one of: 'exact', 'alias', 'initials', 'unmatched'
        """
        if not name:
            return None, "unmatched"

        # Check cache
        cache_key = f"{name.lower()}|{(team or '').lower()}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Load players if needed
        self._load_players()

        # Normalize inputs
        name_clean = name.strip()
        name_lower = name_clean.lower()
        team_normalized = self.normalize_team(team) if team else None

        # 1. Exact match
        result = self._exact_match(name_clean, team_normalized)
        if result:
            self._cache[cache_key] = result
            return result

        # 2. Case-insensitive match
        result = self._case_insensitive_match(name_lower, team_normalized)
        if result:
            self._cache[cache_key] = result
            return result

        # 3. Initials match (e.g., "C. Petracca")
        if "." in name:
            result = self._initials_match(name_clean, team_normalized)
            if result:
                self._cache[cache_key] = result
                return result

        # 4. Partial match (last name only)
        result = self._partial_match(name_clean, team_normalized)
        if result:
            self._cache[cache_key] = result
            return result

        # No match
        result = (None, "unmatched")
        self._cache[cache_key] = result
        return result

    def _exact_match(self, name: str, team: str = None) -> Optional[Tuple[int, str]]:
        """Find exact name match."""
        for player in self._players:
            if player["name"] == name:
                if team and player["team"] != team:
                    continue
                return player["id"], "exact"
        return None

    def _case_insensitive_match(self, name_lower: str, team: str = None) -> Optional[Tuple[int, str]]:
        """Find case-insensitive match."""
        matches = []
        for player in self._players:
            if player["name"].lower() == name_lower:
                if team and player["team"] != team:
                    continue
                matches.append(player)

        if len(matches) == 1:
            return matches[0]["id"], "alias"
        elif len(matches) > 1 and team:
            # Multiple matches but team specified - take first with matching team
            for m in matches:
                if m["team"] == team:
                    return m["id"], "alias"

        return None

    def _initials_match(self, name: str, team: str = None) -> Optional[Tuple[int, str]]:
        """Match names with initials like 'C. Petracca' or 'C Petracca'."""
        # Parse initial pattern
        match = re.match(r"^([A-Z])\.?\s+(.+)$", name)
        if not match:
            return None

        initial = match.group(1).upper()
        surname = match.group(2).lower()

        matches = []
        for player in self._players:
            parts = player["name"].split()
            if len(parts) >= 2:
                first_initial = parts[0][0].upper() if parts[0] else ""
                player_surname = " ".join(parts[1:]).lower()

                if first_initial == initial and player_surname == surname:
                    if team and player["team"] != team:
                        continue
                    matches.append(player)

        if len(matches) == 1:
            return matches[0]["id"], "initials"
        elif len(matches) > 1 and team:
            for m in matches:
                if m["team"] == team:
                    return m["id"], "initials"

        return None

    def _partial_match(self, name: str, team: str = None) -> Optional[Tuple[int, str]]:
        """Match by surname only (requires team for disambiguation)."""
        if not team:
            return None  # Too ambiguous without team

        name_lower = name.lower()
        matches = []

        for player in self._players:
            if player["team"] != team:
                continue

            # Check if name is contained in player name (surname match)
            if name_lower in player["name"].lower():
                matches.append(player)

            # Check if player surname matches
            parts = player["name"].split()
            if len(parts) >= 2:
                surname = " ".join(parts[1:]).lower()
                if surname == name_lower:
                    matches.append(player)

        if len(matches) == 1:
            return matches[0]["id"], "alias"

        return None

    def match_bulk(self, mentions: List[Dict]) -> List[Dict]:
        """
        Match multiple player mentions and add player_id.

        Args:
            mentions: List of mention dicts with 'player' and 'team' keys

        Returns:
            Same list with 'player_id' and 'match_type' populated
        """
        for mention in mentions:
            player_id, match_type = self.match(
                mention.get("player", ""),
                mention.get("team")
            )
            mention["player_id"] = player_id
            mention["match_type"] = match_type

        return mentions

    def get_unmatched_stats(self) -> Dict:
        """Get statistics on unmatched names in cache."""
        unmatched = [k for k, v in self._cache.items() if v[1] == "unmatched"]
        return {
            "total_cached": len(self._cache),
            "unmatched_count": len(unmatched),
            "unmatched_names": [k.split("|")[0] for k in unmatched[:20]]
        }


def update_player_mentions(db_config: Dict, fantasyedge_db_config: Dict):
    """
    Update player_mentions table with matched player IDs.

    Args:
        db_config: Config for llm_news database (player_mentions)
        fantasyedge_db_config: Config for fantasyedge database (players)
    """
    matcher = PlayerMatcher(fantasyedge_db_config)

    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get unmatched mentions
            cur.execute("""
                SELECT id, player_name, team
                FROM player_mentions
                WHERE player_id IS NULL
                  AND match_type != 'unmatched'
            """)
            mentions = [dict(row) for row in cur.fetchall()]

            logger.info(f"Attempting to match {len(mentions)} mentions...")

            matched = 0
            for mention in mentions:
                player_id, match_type = matcher.match(
                    mention["player_name"],
                    mention["team"]
                )

                cur.execute("""
                    UPDATE player_mentions
                    SET player_id = %s, match_type = %s
                    WHERE id = %s
                """, (player_id, match_type, mention["id"]))

                if player_id:
                    matched += 1

            conn.commit()
            logger.info(f"Matched {matched}/{len(mentions)} mentions")

    return matcher.get_unmatched_stats()
