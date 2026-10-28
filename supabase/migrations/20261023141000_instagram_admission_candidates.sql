-- Cast-free candidate lookup; the application retains the exact busy predicate.
create index if not exists scrape_jobs_admission_candidates_idx
on social.scrape_jobs (
  ((lower(coalesce(status,'')) not in ('completed','failed','cancelled')
    or coalesce(completed_at,heartbeat_at,started_at,claimed_at,created_at) is null
    or nullif(metadata->'dispatch'->>'lease_expires_at','') is not null)),
  (coalesce(completed_at,heartbeat_at,started_at,claimed_at,created_at)),
  id
);
