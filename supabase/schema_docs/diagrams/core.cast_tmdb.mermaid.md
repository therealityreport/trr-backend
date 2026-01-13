# core.cast_tmdb - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_CAST_TMDB {
        UUID id
        UUID person_id
        INTEGER tmdb_id
        TEXT name
        TEXT_ARRAY also_known_as
        TEXT biography
        DATE birthday
        DATE deathday
        SMALLINT gender
        BOOLEAN adult
        TEXT homepage
        TEXT known_for_department
        TEXT place_of_birth
        NUMERIC popularity
        TEXT profile_path
        TEXT imdb_id
        TEXT freebase_mid
        TEXT freebase_id
        INTEGER tvrage_id
        TEXT wikidata_id
        TEXT facebook_id
        TEXT instagram_id
        TEXT tiktok_id
        TEXT twitter_id
        TEXT youtube_id
        TIMESTAMP_WITH_TIME_ZONE fetched_at
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
    }
```
