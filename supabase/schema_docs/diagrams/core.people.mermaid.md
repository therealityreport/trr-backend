# core.people - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_PEOPLE {
        TEXT full_name
        TEXT known_for
        JSONB external_ids
        UUID id
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
    }
```
