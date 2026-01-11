# core.episode_appearances - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_EPISODE_APPEARANCES {
        TEXT show_name
        TEXT cast_member_name
        INT4_ARRAY seasons
        INT4_ARRAY tmdb_season_ids
        INTEGER tmdb_show_id
        TEXT imdb_show_id
        TEXT_ARRAY imdb_episode_title_ids
        INT4_ARRAY tmdb_episode_ids
        INTEGER total_episodes
        UUID show_id
        UUID person_id
        UUID id
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
    }
```
