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
        JSONB birthday
        JSONB gender
        JSONB biography
        JSONB place_of_birth
        JSONB homepage
        JSONB profile_image_url
        JSONB alternative_names
    }
```
