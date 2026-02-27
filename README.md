# AFL News Scraper

Standalone scraper that discovers AFL news articles via RSS feeds and DuckDuckGo search, then POSTs them to llm-news-service for processing.

## Quick Start

```bash
# Run directly
python -m scraper.main

# Or with Docker
docker build -t afl-news-scraper .
docker run --rm afl-news-scraper
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `API_BASE_URL` | `http://192.168.6.75:8787` | llm-news-service endpoint |
| `DDG_ENABLED` | `true` | Enable DuckDuckGo search |
| `DDG_MAX_PLAYERS` | `20` | Max players to search |
| `DDG_DELAY_SECONDS` | `2.0` | Delay between DDG searches |
| `FETCH_TIMEOUT` | `30` | HTTP timeout in seconds |
| `FETCH_DELAY_SECONDS` | `1.0` | Delay between fetches |
| `LOG_LEVEL` | `INFO` | Logging level |

## RSS Feeds

- AFL.com.au
- Fox Sports
- Herald Sun
- The Age
- ABC News
- SEN

## Execution Flow

1. **RSS Sweep** - Fetch all RSS feeds
2. **DDG Backfill** - Search for top players
3. **Fetch Content** - Get full article text
4. **Ingest** - POST to llm-news-service

## Deployment

### Manual Run
```bash
docker run --rm -e API_BASE_URL=http://192.168.6.75:8787 afl-news-scraper
```

### Cron (daily at 6am)
```bash
0 6 * * * docker run --rm afl-news-scraper
```
