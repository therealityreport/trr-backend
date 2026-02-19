begin;

alter table core.networks
  add column if not exists wikidata_id text,
  add column if not exists wikipedia_url text,
  add column if not exists wikimedia_logo_file text,
  add column if not exists link_enriched_at timestamptz,
  add column if not exists link_enrichment_source text;

alter table core.watch_providers
  add column if not exists wikidata_id text,
  add column if not exists wikipedia_url text,
  add column if not exists wikimedia_logo_file text,
  add column if not exists link_enriched_at timestamptz,
  add column if not exists link_enrichment_source text;

commit;
