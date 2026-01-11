# core.watch_providers - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_WATCH_PROVIDERS {
        INTEGER provider_id
        TEXT provider_name
        INTEGER display_priority
        TEXT tmdb_logo_path
        TEXT hosted_logo_key
        TEXT hosted_logo_url
        TEXT hosted_logo_sha256
        TEXT hosted_logo_content_type
        BIGINT hosted_logo_bytes
        TEXT hosted_logo_etag
        TIMESTAMP_WITH_TIME_ZONE hosted_logo_at
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
        TEXT logo_path
        JSONB tmdb_meta
        TIMESTAMP_WITH_TIME_ZONE tmdb_fetched_at
        JSONB imdb_meta
        TIMESTAMP_WITH_TIME_ZONE imdb_fetched_at
    }
```
