alter table if exists social.reddit_period_post_matches
  add column if not exists flair_mode text;

create index if not exists reddit_period_post_matches_flair_mode_idx
  on social.reddit_period_post_matches (flair_mode);
