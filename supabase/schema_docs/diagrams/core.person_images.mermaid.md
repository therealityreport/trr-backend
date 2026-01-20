# core.person_images - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_PERSON_IMAGES {
        UUID id
        UUID person_id
        TEXT source
        TEXT url
        INTEGER width
        INTEGER height
        TEXT caption
        BOOLEAN is_primary
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
    }
```
