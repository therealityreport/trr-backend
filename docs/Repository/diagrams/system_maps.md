# System Architecture Maps

## Module Boundaries

```mermaid
flowchart TB
    subgraph scripts["scripts/"]
        s1["ShowInfo"]
        s2["CastInfo"]
        s3["RealiteaseInfo"]
        s4["WWHLInfo"]
        s5["FinalList"]
    end

    subgraph api["api/"]
        routers["routers/"]
        realtime["realtime/"]
        auth["auth.py"]
    end

    subgraph trr["trr_backend/"]
        repos["repositories/"]
        integrations["integrations/"]
        ingestion["ingestion/"]
        media["media/"]
    end

    subgraph external["External APIs"]
        tmdb["TMDb"]
        imdb["IMDb"]
        fandom["Fandom"]
    end

    scripts --> repos
    scripts --> ingestion
    api --> repos
    ingestion --> integrations
    integrations --> external
    repos --> db[(Supabase)]
    media --> s3[(S3)]
```

## Data Flow

```mermaid
flowchart LR
    lists["IMDb/TMDb Lists"] --> resolve["resolve_tmdb_ids"]
    resolve --> backfill["backfill_tmdb_details"]
    backfill --> sync["sync_entities"]
    sync --> providers["sync_watch_providers"]
    providers --> api["API serves ShowDetail"]
```

## Key Components

- **scripts/**: Data ingestion and enrichment pipelines
- **api/**: FastAPI REST endpoints and WebSocket realtime
- **trr_backend/**: Core business logic and data access
- **integrations/**: External API clients (TMDb, IMDb, etc.)
