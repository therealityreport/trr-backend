-- Add per-worker platform capability declaration.
-- Safe default: known legacy platforms only (instagram, tiktok, twitter, youtube).
-- Workers with NULL are treated as legacy and restricted to this same safe set
-- in the claim query, forcing them to re-register before claiming newer platforms.

ALTER TABLE social.scrape_workers
  ADD COLUMN IF NOT EXISTS supported_platforms text[] DEFAULT NULL;

COMMENT ON COLUMN social.scrape_workers.supported_platforms
  IS 'Platforms this worker can process. NULL = legacy worker (restricted to instagram/tiktok/twitter/youtube).';
