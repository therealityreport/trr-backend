create index if not exists idx_pipeline_socialblade_landing_platform_handle_norm
  on pipeline.socialblade_growth_data (
    (lower(coalesce(nullif(platform, ''), 'instagram'))),
    (ltrim(lower(coalesce(nullif(account_handle, ''), instagram_handle, '')), '@'))
  );