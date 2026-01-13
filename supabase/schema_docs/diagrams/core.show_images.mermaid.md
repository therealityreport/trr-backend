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
        TEXT caption
        TEXT source_image_id
        TEXT image_type
        INTEGER position
        TEXT url
        TEXT url_path
        JSONB metadata
        TIMESTAMP_WITH_TIME_ZONE updated_at
        TIMESTAMP_WITH_TIME_ZONE created_at
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
