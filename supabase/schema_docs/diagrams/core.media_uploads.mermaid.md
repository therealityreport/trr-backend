# core.media_uploads - Table Structure Diagram

Auto-generated schema diagram showing columns and data types.
```mermaid
erDiagram
    CORE_MEDIA_UPLOADS {
        UUID id
        UUID uploader_user_id
        TEXT entity_type
        UUID entity_id
        TEXT kind
        TEXT original_filename
        TEXT content_type
        BIGINT expected_bytes
        TEXT caption
        TEXT alt_text
        BOOLEAN make_primary
        TEXT status
        TEXT error
        TIMESTAMP_WITH_TIME_ZONE expires_at
        TEXT s3_bucket
        TEXT s3_temp_key
        UUID media_asset_id
        UUID media_link_id
        TIMESTAMP_WITH_TIME_ZONE created_at
        TIMESTAMP_WITH_TIME_ZONE updated_at
    }
```
