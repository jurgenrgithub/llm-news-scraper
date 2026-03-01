-- Player Mention Views for Fantasy Analysis
-- Run on VM 170: psql -U llm_news -d llm_news -f 005_player_mention_views.sql

-- Injury alerts (worst first)
CREATE OR REPLACE VIEW v_injury_alerts AS
SELECT
    player_name,
    team,
    availability,
    fantasy_impact_score,
    severity,
    expected_return,
    summary,
    article_date,
    source_name
FROM player_mentions
WHERE signal_type = 'injury'
  AND article_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY availability ASC, fantasy_impact_score ASC;

-- Form signals (best first)
CREATE OR REPLACE VIEW v_form_signals AS
SELECT
    player_name,
    team,
    fantasy_impact_score,
    action,
    summary,
    article_date,
    source_name
FROM player_mentions
WHERE signal_type = 'form'
  AND article_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY fantasy_impact_score DESC;

-- Selection news
CREATE OR REPLACE VIEW v_selection_news AS
SELECT
    player_name,
    team,
    fantasy_impact_score,
    action,
    summary,
    article_date,
    source_name
FROM player_mentions
WHERE signal_type = 'selection'
  AND article_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY article_date DESC, fantasy_impact_score DESC;

-- Fantasy movers (biggest impact)
CREATE OR REPLACE VIEW v_fantasy_movers AS
SELECT
    player_name,
    team,
    signal_type,
    fantasy_impact_score,
    availability,
    action,
    summary,
    article_date
FROM player_mentions
WHERE article_date >= CURRENT_DATE - INTERVAL '7 days'
  AND fantasy_impact_score IS NOT NULL
ORDER BY ABS(fantasy_impact_score) DESC
LIMIT 50;

-- All recent mentions
CREATE OR REPLACE VIEW v_recent_mentions AS
SELECT
    player_name,
    team,
    signal_type,
    availability,
    fantasy_impact_score,
    action,
    confidence,
    summary,
    article_date,
    source_name,
    is_official_source
FROM player_mentions
WHERE article_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY article_date DESC, confidence DESC;
