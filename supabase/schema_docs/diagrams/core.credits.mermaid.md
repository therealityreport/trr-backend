# core.credits - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_CREDITS {
        UUID id
        UUID show_id
        UUID person_id
        TEXT credit_category
        TEXT role
        INTEGER billing_order
        TEXT source_type
        JSONB metadata
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
    }
```
