"""Configuration for AFL News Scraper."""

import os

# API endpoint for article ingestion
API_BASE_URL = os.getenv("API_BASE_URL", "http://192.168.6.75:8787")

# RSS Feeds to monitor (verified working)
RSS_FEEDS = [
    {
        "url": "https://www.theage.com.au/rss/sport/afl.xml",
        "source": "The Age",
        "priority": 1,
    },
    {
        "url": "https://www.abc.net.au/news/feed/2942460/rss.xml",
        "source": "ABC News",
        "priority": 1,
    },
    {
        "url": "https://www.smh.com.au/rss/sport/afl.xml",
        "source": "Sydney Morning Herald",
        "priority": 2,
    },
    # Note: These feeds may need validation
    # "https://www.afl.com.au/news/feed" - 404 as of Feb 2026
    # "https://www.foxsports.com.au/afl/rss" - returns empty/invalid
    # "https://www.heraldsun.com.au/sport/afl/rss" - paywall/invalid
]

# Club website scraping
CLUBS_ENABLED = os.getenv("CLUBS_ENABLED", "true").lower() == "true"
CLUB_DELAY_SECONDS = float(os.getenv("CLUB_DELAY_SECONDS", "2.0"))
CLUB_MAX_ARTICLES_PER_SITE = int(os.getenv("CLUB_MAX_ARTICLES", "20"))

# All 18 AFL club domains (same CMS stack)
CLUB_DOMAINS = [
    "afl.com.au",            # Official AFL
    "adelaidefc.com.au",     # Adelaide Crows
    "lions.com.au",          # Brisbane Lions
    "carltonfc.com.au",      # Carlton Blues
    "collingwoodfc.com.au",  # Collingwood Magpies
    "essendonfc.com.au",     # Essendon Bombers
    "fremantlefc.com.au",    # Fremantle Dockers
    "geelongcats.com.au",    # Geelong Cats
    "goldcoastfc.com.au",    # Gold Coast Suns
    "gwsgiants.com.au",      # GWS Giants
    "hawthornfc.com.au",     # Hawthorn Hawks
    "melbournefc.com.au",    # Melbourne Demons
    "nmfc.com.au",           # North Melbourne Kangaroos
    "portadelaidefc.com.au", # Port Adelaide Power
    "richmondfc.com.au",     # Richmond Tigers
    "saints.com.au",         # St Kilda Saints
    "sydneyswans.com.au",    # Sydney Swans
    "westcoasteagles.com.au",# West Coast Eagles
    "westernbulldogs.com.au",# Western Bulldogs
]

# DDG Search settings
DDG_ENABLED = os.getenv("DDG_ENABLED", "true").lower() == "true"
DDG_MAX_PLAYERS = int(os.getenv("DDG_MAX_PLAYERS", "20"))
DDG_DELAY_SECONDS = float(os.getenv("DDG_DELAY_SECONDS", "2.0"))

# Content fetching
FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "30"))
FETCH_DELAY_SECONDS = float(os.getenv("FETCH_DELAY_SECONDS", "1.0"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Database config for page cache
DB_CONFIG = {
    "host": os.getenv("LLM_NEWS_DB_HOST", "192.168.6.170"),
    "port": int(os.getenv("LLM_NEWS_DB_PORT", "5432")),
    "dbname": os.getenv("LLM_NEWS_DB_NAME", "llm_news"),
    "user": os.getenv("LLM_NEWS_DB_USER", "llm_news"),
    "password": os.getenv("LLM_NEWS_DB_PASSWORD", "llm_news_dev_2026"),
}
