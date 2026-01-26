# core.season_source_history - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_SEASON_SOURCE_HISTORY {
        BIGINT id
        UUID season_id
        TEXT source_id
        TEXT variant
        TIMESTAMP_WITH_TIME_ZONE fetched_at
        TEXT fetch_method
        TEXT status
        TEXT error
        JSONB payload
        TEXT payload_sha256
        TIMESTAMP_WITH_TIME_ZONE created_at
    }
```
