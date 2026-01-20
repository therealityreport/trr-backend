-- Migration: Expand show_cast.source_type constraint for GraphQL data sources
-- Related: feat/imdb-graphql-client (GraphQL persisted query fallback)
-- Date: 2026-01-15

-- Drop existing constraint
ALTER TABLE core.show_cast
DROP CONSTRAINT IF EXISTS show_cast_source_type_check;

-- Add expanded constraint with GraphQL source types
ALTER TABLE core.show_cast
ADD CONSTRAINT show_cast_source_type_check
CHECK (source_type IN (
    'fullcredits_html',                    -- HTML scraping (complete when available)
    'credits_graphql_paginated',           -- GraphQL pagination (complete, filtered for main cast)
    'credits_graphql_paginated_partial',   -- GraphQL pagination hit limits (MAX_PAGES or MAX_MEMBERS)
    'credits_api_top_billed',              -- JSON API (partial - top-billed only, renamed from credits_api_fallback)
    'credits_api_fallback',                -- Deprecated JSON API fallback (legacy - keep for existing data)
    'manual'                               -- Human entered
));

-- Update column comment to document all source types
COMMENT ON COLUMN core.show_cast.source_type IS
'Data source:
- fullcredits_html: HTML scraping (complete when available)
- credits_graphql_paginated: GraphQL pagination (complete, filtered for main cast)
- credits_graphql_paginated_partial: GraphQL pagination hit limits (MAX_PAGES/MAX_MEMBERS)
- credits_api_top_billed: JSON API (partial - top-billed only)
- credits_api_fallback: Deprecated JSON API fallback (legacy)
- manual: Human entered';

-- Migrate existing credits_api_fallback to credits_api_top_billed for clarity
-- This is a semantic rename to make the partial nature explicit
UPDATE core.show_cast
SET source_type = 'credits_api_top_billed'
WHERE source_type = 'credits_api_fallback';
