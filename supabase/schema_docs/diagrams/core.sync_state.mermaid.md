# core.sync_state - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_SYNC_STATE {
        TEXT table_name
        UUID show_id
        TEXT status
        TIMESTAMP_WITH_TIME_ZONE last_success_at
        TEXT last_seen_most_recent_episode
        TEXT last_error
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
    }
```
