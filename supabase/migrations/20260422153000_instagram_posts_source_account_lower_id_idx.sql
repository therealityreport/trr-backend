-- Transactional migration form is intentional: this repo's checked-in
-- Supabase migrations are consumed by reset/push workflows that expect the
-- standard transaction-wrapped idiom. The planner-backed requirement is the
-- index shape itself.

BEGIN;

CREATE INDEX IF NOT EXISTS idx_social_instagram_posts_source_account_lower_id
ON social.instagram_posts (lower(source_account), id);

COMMIT;
