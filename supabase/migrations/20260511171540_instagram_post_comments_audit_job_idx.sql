create index if not exists instagram_post_comments_audit_job_created_idx
  on social.instagram_post_comments_audit (scrape_job_id, created_at desc)
  where scrape_job_id is not null;
