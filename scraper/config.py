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

# DDG Search settings
DDG_ENABLED = os.getenv("DDG_ENABLED", "true").lower() == "true"
DDG_MAX_PLAYERS = int(os.getenv("DDG_MAX_PLAYERS", "20"))
DDG_DELAY_SECONDS = float(os.getenv("DDG_DELAY_SECONDS", "2.0"))

# Content fetching
FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "30"))
FETCH_DELAY_SECONDS = float(os.getenv("FETCH_DELAY_SECONDS", "1.0"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
