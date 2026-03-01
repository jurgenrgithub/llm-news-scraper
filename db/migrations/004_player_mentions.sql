-- Migration: Create player_mentions table for LLM extraction output
-- Run on VM 170 (llm_news database)

-- Add extraction tracking to page_cache
ALTER TABLE page_cache ADD COLUMN IF NOT EXISTS extracted_at TIMESTAMPTZ;
ALTER TABLE page_cache ADD COLUMN IF NOT EXISTS extraction_error TEXT;

-- Create player_mentions table
CREATE TABLE IF NOT EXISTS player_mentions (
    id SERIAL PRIMARY KEY,

    -- Provenance
    article_id INTEGER REFERENCES page_cache(id) ON DELETE CASCADE,
    source_url TEXT NOT NULL,
    source_name VARCHAR(100),
    source_tier VARCHAR(20) CHECK (source_tier IN ('official', 'major', 'social', 'other')),
    is_official_source BOOLEAN DEFAULT FALSE,
    article_date DATE,

    -- Player linkage (player_id references external players table, nullable for unmatched)
    player_id INTEGER,
    player_name VARCHAR(200) NOT NULL,
    team VARCHAR(50),
    match_type VARCHAR(20) CHECK (match_type IN ('exact', 'alias', 'initials', 'unmatched')),
    match_snippet TEXT,

    -- Signal classification
    signal_type VARCHAR(20) CHECK (signal_type IN ('injury', 'selection', 'form', 'role', 'contract')),
    signal_strength DECIMAL(3,2) CHECK (signal_strength BETWEEN 0 AND 1),
    summary TEXT,
    quote TEXT,

    -- Scoring
    availability DECIMAL(3,2) CHECK (availability BETWEEN 0 AND 1),
    impact_weeks INTEGER,
    fantasy_impact_score INTEGER CHECK (fantasy_impact_score BETWEEN -100 AND 100),
    action VARCHAR(20) CHECK (action IN ('start', 'bench', 'monitor', 'no_action')),
    sentiment VARCHAR(20) CHECK (sentiment IN ('positive', 'negative', 'neutral')),
    confidence DECIMAL(3,2) CHECK (confidence BETWEEN 0 AND 1),

    -- Injury deep-dive (nullable, only for injury signals)
    injury_type VARCHAR(50),
    body_part VARCHAR(50),
    severity VARCHAR(20),
    expected_return VARCHAR(50),
    surgery_probability DECIMAL(3,2),
    playing_through BOOLEAN,
    historical_concern BOOLEAN,
    recommended_followup VARCHAR(50),

    -- Metadata
    extracted_at TIMESTAMPTZ DEFAULT NOW(),
    processing_ms INTEGER,
    model_version VARCHAR(50),

    -- Prevent duplicate mentions per article
    UNIQUE(article_id, player_name, signal_type)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_mentions_player ON player_mentions(player_id);
CREATE INDEX IF NOT EXISTS idx_mentions_article ON player_mentions(article_id);
CREATE INDEX IF NOT EXISTS idx_mentions_signal ON player_mentions(signal_type);
CREATE INDEX IF NOT EXISTS idx_mentions_date ON player_mentions(article_date);
CREATE INDEX IF NOT EXISTS idx_mentions_unmatched ON player_mentions(player_id) WHERE player_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_mentions_confidence ON player_mentions(confidence) WHERE confidence < 0.6;

-- Index on page_cache for extraction queue
CREATE INDEX IF NOT EXISTS idx_page_cache_unextracted ON page_cache(published_at DESC)
    WHERE extracted_at IS NULL AND source_type != 'injury_list';
