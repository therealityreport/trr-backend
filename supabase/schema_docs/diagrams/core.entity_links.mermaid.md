# core.entity_links - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_ENTITY_LINKS {
        UUID id
        TEXT entity_type
        UUID entity_id
        UUID show_id
        INTEGER season_number
        TEXT link_group
        TEXT link_kind
        TEXT label
        TEXT url
        TEXT url_key
        TEXT status
        NUMERIC confidence
        TEXT discovered_by
        TEXT source
        JSONB metadata
        TEXT created_by
        TEXT updated_by
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
    }
```
