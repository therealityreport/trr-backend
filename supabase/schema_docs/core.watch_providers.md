# core.watch_providers

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| provider_id | integer | NO |  | NO | NEVER |
| provider_name | text | NO |  | NO | NEVER |
| display_priority | integer | YES |  | NO | NEVER |
| tmdb_logo_path | text | YES |  | NO | NEVER |
| hosted_logo_key | text | YES |  | NO | NEVER |
| hosted_logo_url | text | YES |  | NO | NEVER |
| hosted_logo_sha256 | text | YES |  | NO | NEVER |
| hosted_logo_content_type | text | YES |  | NO | NEVER |
| hosted_logo_bytes | bigint | YES |  | NO | NEVER |
| hosted_logo_etag | text | YES |  | NO | NEVER |
| hosted_logo_at | timestamp with time zone | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |
| logo_path | text | YES |  | NO | NEVER |
| tmdb_meta | jsonb | YES |  | NO | NEVER |
| tmdb_fetched_at | timestamp with time zone | YES |  | NO | NEVER |
| imdb_meta | jsonb | YES |  | NO | NEVER |
| imdb_fetched_at | timestamp with time zone | YES |  | NO | NEVER |

## Primary Key

provider_id

## Unique Constraints

(none)

## Foreign Keys

(none)

## Indexes

- tmdb_watch_providers_name_idx (non-unique): provider_name
- tmdb_watch_providers_pkey (unique): provider_id

## RLS Enabled

true

## Example Row

```json
{
  "provider_id": 0,
  "provider_name": "example",
  "display_priority": 0,
  "tmdb_logo_path": "example",
  "hosted_logo_key": "example",
  "hosted_logo_url": "example",
  "hosted_logo_sha256": "example",
  "hosted_logo_content_type": "example",
  "hosted_logo_bytes": 0,
  "hosted_logo_etag": "example",
  "hosted_logo_at": "1970-01-01T00:00:00Z",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z",
  "logo_path": "example",
  "tmdb_meta": {},
  "tmdb_fetched_at": "1970-01-01T00:00:00Z",
  "imdb_meta": {},
  "imdb_fetched_at": "1970-01-01T00:00:00Z"
}
```