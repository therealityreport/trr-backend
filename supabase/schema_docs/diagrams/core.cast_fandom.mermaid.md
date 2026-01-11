# core.cast_fandom - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_CAST_FANDOM {
        UUID id
        UUID person_id
        TEXT source
        TEXT source_url
        TEXT page_title
        BIGINT page_revision_id
        TIMESTAMP_WITH_TIME_ZONE scraped_at
        TEXT full_name
        DATE birthdate
        TEXT birthdate_display
        TEXT gender
        TEXT resides_in
        TEXT hair_color
        TEXT eye_color
        TEXT height_display
        TEXT weight_display
        TEXT_ARRAY romances
        JSONB family
        JSONB friends
        JSONB enemies
        TEXT installment
        TEXT installment_url
        TEXT main_seasons_display
        TEXT summary
        JSONB taglines
        JSONB reunion_seating
        JSONB trivia
        JSONB infobox_raw
        TEXT raw_html_sha256
    }
```
