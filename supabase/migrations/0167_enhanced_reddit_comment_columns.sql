-- Sprint 4: Enhanced Reddit comment metadata for deep detail scraping.
--
-- Adds extended columns to social.reddit_comments captured during
-- the detail-scrape pass (mirrors the post-level expansion in 0166).

begin;

-- ---------------------------------------------------------------------------
-- social.reddit_comments — additional metadata columns
-- ---------------------------------------------------------------------------

alter table social.reddit_comments
  add column if not exists author_flair_text text,
  add column if not exists is_submitter boolean default false,
  add column if not exists controversiality integer default 0,
  add column if not exists ups integer,
  add column if not exists downs integer default 0,
  add column if not exists gildings jsonb default '{}'::jsonb,
  add column if not exists body_html text;

comment on column social.reddit_comments.author_flair_text is 'Commenter user flair text';
comment on column social.reddit_comments.is_submitter      is 'True when the commenter is also the post author';
comment on column social.reddit_comments.controversiality  is 'Reddit controversiality flag (0 = normal, 1 = controversial)';
comment on column social.reddit_comments.ups               is 'Explicit upvote count from Reddit API';
comment on column social.reddit_comments.downs             is 'Explicit downvote count from Reddit API';
comment on column social.reddit_comments.gildings          is 'Awards / gildings metadata as JSON';
comment on column social.reddit_comments.body_html         is 'HTML-rendered version of the comment body';

commit;
