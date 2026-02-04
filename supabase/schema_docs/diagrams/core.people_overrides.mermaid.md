# core.people_overrides - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_PEOPLE_OVERRIDES {
        UUID id
        UUID person_id
        TEXT full_name_override
        TEXT instagram_handle
        JSONB external_ids_override
        TEXT notes
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
    }
```
