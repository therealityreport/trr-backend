# core.seasons - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_SEASONS {
        TEXT show_name
        TEXT name
        INTEGER season_number
        UUID show_id
        TEXT title
        TEXT overview
        DATE air_date
        DATE premiere_date
        INTEGER tmdb_series_id
        TEXT imdb_series_id
        INTEGER tmdb_season_id
        TEXT tmdb_season_object_id
        TEXT poster_path
        TEXT url_original_poster
        INTEGER external_tvdb_id
        TEXT external_wikidata_id
        JSONB external_ids
        TEXT language
        TIMESTAMP_WITH_TIME_ZONE fetched_at
        UUID id
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
    }
```
