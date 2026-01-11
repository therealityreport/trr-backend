# core.shows - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_SHOWS {
        TEXT name
        INTEGER show_total_seasons
        INTEGER show_total_episodes
        TEXT imdb_id
        INTEGER tmdb_id
        TEXT most_recent_episode
        UUID id
        TEXT description
        DATE premiere_date
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
        INTEGER most_recent_episode_season
        INTEGER most_recent_episode_number
        TEXT most_recent_episode_title
        DATE most_recent_episode_air_date
        TEXT most_recent_episode_imdb_id
        UUID primary_poster_image_id
        UUID primary_backdrop_image_id
        UUID primary_logo_image_id
        BOOLEAN needs_imdb_resolution
        TEXT_ARRAY genres
        TEXT_ARRAY keywords
        TEXT_ARRAY tags
        TEXT_ARRAY networks
        TEXT_ARRAY streaming_providers
        TEXT_ARRAY listed_on
        INTEGER tvdb_id
        INTEGER tvrage_id
        TEXT wikidata_id
        TEXT facebook_id
        TEXT instagram_id
        TEXT twitter_id
        BOOLEAN needs_tmdb_resolution
        TEXT tmdb_name
        TEXT tmdb_status
        TEXT tmdb_type
        DATE tmdb_first_air_date
        DATE tmdb_last_air_date
        NUMERIC tmdb_vote_average
        INTEGER tmdb_vote_count
        NUMERIC tmdb_popularity
        TEXT imdb_title
        TEXT imdb_content_rating
        NUMERIC imdb_rating_value
        INTEGER imdb_rating_count
        DATE imdb_date_published
        INTEGER imdb_end_year
        TIMESTAMP_WITH_TIME_ZONE tmdb_fetched_at
        TIMESTAMP_WITH_TIME_ZONE imdb_fetched_at
        JSONB tmdb_meta
        JSONB imdb_meta
        INT4_ARRAY tmdb_network_ids
        INT4_ARRAY tmdb_production_company_ids
    }
```
