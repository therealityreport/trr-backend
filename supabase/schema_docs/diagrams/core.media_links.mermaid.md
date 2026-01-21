# core.media_links - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_MEDIA_LINKS {
        UUID id
        TEXT entity_type
        UUID entity_id
        UUID media_asset_id
        TEXT kind
        INTEGER position
        BOOLEAN is_primary
        JSONB context
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
    }
```
