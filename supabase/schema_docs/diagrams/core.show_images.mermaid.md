# core.show_images - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_SHOW_IMAGES {
        UUID id
        UUID show_id
        TEXT source
        TEXT kind
        TEXT iso_639_1
        TEXT file_path
        INTEGER width
        INTEGER height
        NUMERIC aspect_ratio
        TIMESTAMP_WITH_TIME_ZONE fetched_at
        INTEGER tmdb_id
        TEXT url_original
        TEXT source_image_id
        TEXT url
        TEXT url_path
        TEXT caption
        INTEGER position
        JSONB metadata
        TEXT image_type
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
        TEXT fetch_method
        TEXT fetched_from_url
        TEXT hosted_bucket
        TEXT hosted_key
        TEXT hosted_url
        TEXT hosted_sha256
        TEXT hosted_content_type
        BIGINT hosted_bytes
        TEXT hosted_etag
        TIMESTAMP_WITH_TIME_ZONE hosted_at
    }
```
