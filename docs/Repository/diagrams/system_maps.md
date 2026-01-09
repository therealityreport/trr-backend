# System Architecture Maps

## Module Boundaries

```mermaid
flowchart TB
    subgraph scripts["scripts/"]
        sync["sync_*.py"]
        import["import_*.py"]
        enrich["enrich_*.py"]
        resolve["resolve_*.py"]
        backfill["backfill_*.py"]
    end

    subgraph trr["trr_backend/"]
        repos["repositories/"]
        integrations["integrations/"]
        ingestion["ingestion/"]
        media["media/"]
        utils["utils/"]
    end

    subgraph api["api/"]
        routers["routers/"]
        schemas["schemas/"]
    end

    subgraph external["External APIs"]
        tmdb["TMDb"]
        imdb["IMDb"]
        fandom["Fandom"]
    end

    scripts --> repos
    scripts --> ingestion
    scripts --> media
    api --> repos
    ingestion --> integrations
    integrations --> external
    repos --> db[(Supabase)]
    media --> s3[(S3)]
```

## TMDb Enrichment Pipeline

```mermaid
flowchart LR
    lists["IMDb/TMDb Lists"] --> resolve["resolve_tmdb_ids"]
    resolve --> backfill["backfill_tmdb_details"]
    backfill --> entities["sync_entities"]
    entities --> providers["sync_watch_providers"]
    providers --> api["API serves ShowDetail"]
```

## Data Flow Overview

```mermaid
flowchart TB
    subgraph sources["Data Sources"]
        tmdb_api["TMDb API"]
        imdb_api["IMDb API"]
        fandom_wiki["Fandom Wikis"]
    end

    subgraph pipeline["Sync Pipeline"]
        sync_shows["sync_shows"]
        sync_seasons["sync_seasons_episodes"]
        sync_people["sync_people"]
        sync_images["sync_*_images"]
    end

    subgraph storage["Storage"]
        supabase[(Supabase DB)]
        s3[(S3 CDN)]
    end

    subgraph api_layer["API Layer"]
        fastapi["FastAPI"]
        ws["WebSocket"]
    end

    sources --> pipeline
    pipeline --> supabase
    pipeline --> s3
    supabase --> api_layer
    s3 --> api_layer
```

## S3 Media Storage Layout

```mermaid
flowchart LR
    bucket["trr-media bucket"]

    subgraph images["images/"]
        logos["logos/{kind}/{id}/{sha256}.ext"]
        cast["cast_photos/{person_id}/{sha256}.ext"]
        shows["show_images/{show_id}/{sha256}.ext"]
        seasons["season_images/{season_id}/{sha256}.ext"]
    end

    bucket --> images
```

All media is content-addressed using SHA256 hashes for deduplication and immutable caching.
