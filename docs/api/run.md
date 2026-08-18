# TRR Backend API - Running Guide

This document describes how to run the FastAPI-based TRR Backend API locally and deployment considerations.
For the current production-target hosting plan, use `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/deploy/render.md`.
For backend runtime ownership boundaries, use
[Backend Runtime Ownership](../../../docs/workspace/backend-runtime-ownership.md).

## Required Environment Variables

The API requires the following environment variables to be set:

### Database + Supabase JWT (Required)

| Variable | Description | Example |
|----------|-------------|---------|
| `TRR_DB_URL` | Canonical runtime Postgres URL (Supavisor session pooler preferred) | `postgresql://postgres.<project>:<password>@aws-0-...pooler.supabase.com:5432/postgres` |
| `TRR_DB_FALLBACK_URL` | Optional explicit runtime fallback URL | `postgresql://...` |
| `TRR_PREVIEW_READ_ONLY` | Isolated-preview opt-in. When `1`, every backend pool and direct psycopg2 connection preserves existing URI options, adds `-c default_transaction_read_only=on`, and must receive `SHOW transaction_read_only = on` before use; otherwise startup or the caller fails closed. PostgREST cache reloads are refused. The API also skips broker/background-task startup, and Modal forces `SOCIAL_QUEUE_ENABLED=false`. Leave `0` for normal runtimes. | `0` / `1` |
| `SUPABASE_JWT_SECRET` | Signing secret used to verify incoming Supabase access tokens | `long-random-jwt-secret` |

### Supabase API/Auth Helpers (Required only for SDK or PostgREST admin flows)

| Variable | Description | Example |
|----------|-------------|---------|
| `SUPABASE_URL` | Supabase API base URL for SDK/PostgREST calls | `https://<project>.supabase.co` |
| `SUPABASE_ANON_KEY` | Optional anon key for non-admin API flows | `eyJ...` |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key for backend-owned Supabase admin operations | `eyJ...` |

### Internal Admin Proxy (TRR-APP -> TRR-Backend)

| Variable | Description | Example |
|----------|-------------|---------|
| `TRR_INTERNAL_ADMIN_SHARED_SECRET` | Shared signing secret for TRR-APP internal admin JWTs | `long-random-shared-secret` |
| `TRR_INTERNAL_ADMIN_JWT_ISSUER` | Optional issuer override for internal admin JWT verification | `trr-app-internal` |
| `TRR_INTERNAL_ADMIN_JWT_AUDIENCE` | Optional audience override for internal admin JWT verification | `trr-backend-internal-admin` |

For internal admin routes such as `PUT /api/v1/admin/people/{person_id}/external-ids`
and `PATCH /api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed`, backend accepts:
- allowlisted user JWT (`ADMIN_EMAIL_ALLOWLIST`), or
- a signed internal admin JWT from TRR-APP with the configured issuer/audience and `scope=internal_admin`.

### Redis (Optional locally, required for multi-worker realtime)

| Variable | Description | Example |
|----------|-------------|---------|
| `REDIS_URL` | Redis connection URL for ephemeral realtime pub/sub, presence/typing, short TTL state, and cross-instance invalidation. | `redis://localhost:6379` |

If `REDIS_URL` is not set, the API uses an in-memory broker for WebSocket
pub/sub. This is fine for local development and single-instance deployments.
For multi-worker or multi-instance deployments, set `REDIS_URL` so realtime
events, presence, typing, and invalidation are shared across API processes.

Redis is not a durable queue or cache of record. Durable jobs, runs, locks,
retries, analytics outputs, persisted cache state, and migration history belong
in Postgres/Supabase. Long-running scraping, social, media, vision, and
scheduled execution belongs in Modal.

### Backend runtime behavior

| Variable | Description | Example |
|----------|-------------|---------|
| `TRR_BACKEND_WORKERS` | Number of uvicorn worker processes. Defaults to `1` for local stability. | `4` |
| `TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER` | When `1`, blocks multi-worker startup unless `REDIS_URL` is present. Local/dev launchers fall back to one worker; deployed launchers fail fast. | `1` |
| `TRR_BACKEND_RELOAD` | Enables uvicorn `--reload` in local mode. | `0` / `1` |

By default, `TRR_BACKEND_WORKERS` is `1` and `TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER=1`, so multi-worker mode is only enabled when `REDIS_URL` is configured. If `TRR_BACKEND_WORKERS > 1` is requested while `REDIS_URL` is missing, local/dev startup logs explicitly warn and fall back to a single worker. Deployed startup exits with an error so realtime state is not split across workers.

Container deployments use the same `start-api.sh` launcher in non-reload mode. The default container contract is a single `uvicorn` process per container; only raise `TRR_BACKEND_WORKERS` when you have a concrete reason and a Redis-backed realtime plane.

### CORS (Optional)

| Variable | Description | Example |
|----------|-------------|---------|
| `CORS_ALLOW_ORIGINS` | Comma-separated allowed origins | `https://trr-app.vercel.app,https://preview.example.vercel.app` |

If `CORS_ALLOW_ORIGINS` is not set, the API allows all origins but disables credentials (safer default for development).

When origins are explicitly set:
- Only listed origins are allowed
- Credentials are enabled (required for authenticated requests)
- For the Render cutover, include the exact Vercel Production and Preview origins that should send cookies or auth headers

### Better Stack logging (Optional; free tier first is fine)

| Variable | Description | Example |
|----------|-------------|---------|
| `BETTER_STACK_SOURCE_TOKEN` | Better Stack source token for HTTP log ingestion | `source-token` |
| `BETTER_STACK_INGESTING_HOST` | Better Stack ingest host | `in.logs.betterstack.com` |
| `BETTER_STACK_LOG_TIMEOUT_SECONDS` | HTTP timeout for log shipping | `2.0` |
| `BETTER_STACK_FAILURE_COOLDOWN_SECONDS` | Cooldown after a failed ship attempt | `60` |

When `BETTER_STACK_SOURCE_TOKEN` is set, the backend ships structured Python logs directly to Better Stack over HTTP. This is intended for the final `Render API + Modal jobs` steady state so app logs remain available after the legacy host and ad-hoc log sinks are retired. The default operator choice is to start with a Better Stack free source and only upgrade if actual volume or retention needs exceed the free tier.

### Object storage (Optional locally, required for media mirroring and uploads)

| Variable | Description | Example |
|----------|-------------|---------|
| `OBJECT_STORAGE_PROVIDER` | Storage backend name for operator clarity | `r2` |
| `OBJECT_STORAGE_BUCKET` | Canonical object-storage bucket name | `trr-media-prod` |
| `OBJECT_STORAGE_REGION` | Region or provider region token | `auto` |
| `OBJECT_STORAGE_ENDPOINT_URL` | S3-compatible API endpoint | `https://<accountid>.r2.cloudflarestorage.com` |
| `OBJECT_STORAGE_ACCESS_KEY_ID` | S3-compatible access key | `...` |
| `OBJECT_STORAGE_SECRET_ACCESS_KEY` | S3-compatible secret key | `...` |
| `OBJECT_STORAGE_PUBLIC_BASE_URL` | Public/custom base URL for hosted assets | `https://media.thereality.report` |
| `OBJECT_STORAGE_PREFIX` | Optional key prefix | `media` |

## Running Locally

Python 3.11+ is required.

### 1. Set up Python environment

```bash
# Create and activate virtual environment
python3.11 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure environment

```bash
# Copy the example env file
cp .env.example .env

# Edit .env with your runtime DB/auth credentials
nano .env  # or use your preferred editor
```

At minimum, set these in `.env`:
```
TRR_DB_URL=postgresql://postgres.<project>:<password>@aws-0-...pooler.supabase.com:5432/postgres
SUPABASE_JWT_SECRET=your_supabase_jwt_secret
TRR_INTERNAL_ADMIN_SHARED_SECRET=your_internal_admin_signing_secret
```

### 3. Run the development server

```bash
# Start uvicorn with hot reload
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at:
- API endpoints: `http://localhost:8000/api/v1/`
- Interactive docs: `http://localhost:8000/docs`
- OpenAPI schema: `http://localhost:8000/openapi.json`

### 4. Verify it's working

```bash
# Health check
curl http://localhost:8000/health

# List shows (requires Supabase connection)
curl http://localhost:8000/api/v1/shows
```

## API Docs UI

FastAPI ships with interactive API docs and an OpenAPI schema:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- OpenAPI schema: `http://localhost:8000/openapi.json`

## Client Access & Auth

**Auth matrix**

| Client | Auth Header | Notes |
| --- | --- | --- |
| TRR App | `Authorization: Bearer <Supabase access token>` | Use for user-scoped endpoints under `/api/v1/*`. |
| TRR App internal proxy | `Authorization: Bearer <internal admin JWT>` | Used by backend-owned admin routes such as person external-ID writes and facebank toggles; TRR-APP signs the JWT with `TRR_INTERNAL_ADMIN_SHARED_SECRET`. |

Admin allowlist
- Backend-owned admin endpoints require either an allowlisted user JWT or a valid internal admin JWT signed with `TRR_INTERNAL_ADMIN_SHARED_SECRET`.

**CORS guidance**

- TRR App: set `CORS_ALLOW_ORIGINS` in the backend env to the app domain(s) so credentials are allowed.
- Render target: keep `TRR_API_URL` pointing at the Render service URL for the public backend host while Modal remains the async executor.
- Screenalytics: server-to-server calls do not require CORS.

**Example calls**

```bash
# TRR App (Supabase JWT)
curl -H "Authorization: Bearer $SUPABASE_ACCESS_TOKEN" \
  http://localhost:8000/api/v1/shows

# Internal admin JWT (TRR-APP proxy pattern)
curl -H "Authorization: Bearer $TRR_INTERNAL_ADMIN_JWT" \
  http://localhost:8000/api/v1/admin/person/<person_id>/gallery/<link_id>/facebank-seed
```

## Running Tests

```bash
# Run all API smoke tests
python -m pytest tests/test_api_smoke.py -v
pytest tests/test_discussions_smoke.py -v
pytest tests/test_dms_smoke.py -v
pytest tests/test_ws_realtime_smoke.py -v

# Run with coverage
python -m pytest tests/ --cov=api --cov-report=term-missing
```

## Production Deployment

This guide is for local development. For the current production-target hosting plan, use `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/deploy/render.md`.

## API Endpoints

### Health Check

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Root health check |
| GET | `/health` | Health status |

### Shows

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/shows` | List all shows |
| GET | `/api/v1/shows/{show_id}` | Get show details |
| GET | `/api/v1/shows/{show_id}/seasons` | List seasons for a show |
| GET | `/api/v1/shows/{show_id}/seasons/{season_number}` | Get season details |
| GET | `/api/v1/shows/{show_id}/seasons/{season_number}/episodes` | List episodes |
| GET | `/api/v1/shows/{show_id}/cast` | List cast for a show |
| GET | `/api/v1/shows/{show_id}/seasons/{season_number}/cast` | List cast for a season |

### Surveys

| Method | Endpoint | Description | Auth |
|--------|----------|-------------|------|
| GET | `/api/v1/surveys` | List all active surveys | Public |
| GET | `/api/v1/surveys/{survey_id}` | Get survey with questions | Public |
| GET | `/api/v1/surveys/{survey_id}/results` | Get live survey results | Public |
| POST | `/api/v1/surveys/{survey_id}/submit` | Submit survey response | Optional |

**Note:** Survey submissions support both anonymous and authenticated users. When authenticated, the `user_id` is derived from the JWT token. Anonymous submissions have `user_id = NULL`.

### Discussions (Episode Threads)

Reddit-style discussion threads for episodes.

| Method | Endpoint | Description | Auth |
|--------|----------|-------------|------|
| GET | `/api/v1/episodes/{episode_id}/threads` | List threads for an episode | Public |
| POST | `/api/v1/episodes/{episode_id}/threads` | Create a new thread | Required |
| GET | `/api/v1/threads/{thread_id}` | Get thread details | Public |
| GET | `/api/v1/threads/{thread_id}/posts` | List posts in thread | Public |
| POST | `/api/v1/threads/{thread_id}/posts` | Create a post | Required |
| GET | `/api/v1/posts/{post_id}/reactions` | Get reaction counts | Public |
| POST | `/api/v1/posts/{post_id}/reactions` | Toggle reaction | Required |

**Authentication:** Write endpoints require a valid Supabase JWT in the `Authorization: Bearer <token>` header. The API validates tokens via Supabase Auth and enforces RLS policies using the user's identity.

#### Thread Types

- `episode_live` - Live discussion during episode airing
- `post_episode` - Discussion after episode airs
- `spoilers` - Spoiler discussions
- `general` - General discussion

#### Reaction Types

- `upvote`, `downvote` - Vote reactions
- `lol`, `shade`, `fire`, `heart` - Emoji reactions

#### Pagination

Posts support cursor-based pagination:

```
GET /api/v1/threads/{thread_id}/posts?cursor=2025-01-01T00:00:00Z&limit=50
```

- `cursor`: ISO timestamp to start after (from previous page's last post `created_at`)
- `limit`: Max posts to return (default: 50, max: 100)
- `parent_post_id`: Filter to replies of a specific post (omit for top-level posts)

### Direct Messages (DMs)

1:1 direct messaging between users.

| Method | Endpoint | Description | Auth |
|--------|----------|-------------|------|
| POST | `/api/v1/dms` | Create or get 1:1 conversation | Required |
| GET | `/api/v1/dms` | List user's conversations | Required |
| GET | `/api/v1/dms/{conversation_id}/messages` | List messages in conversation | Required |
| POST | `/api/v1/dms/{conversation_id}/messages` | Send a message | Required |
| POST | `/api/v1/dms/{conversation_id}/read` | Update read receipt | Required |

**Authentication:** All DM endpoints require a valid Supabase JWT. RLS policies ensure users can only access conversations they're members of.

#### Creating a Conversation

```bash
POST /api/v1/dms
Content-Type: application/json
Authorization: Bearer <token>

{"other_user_id": "uuid-of-other-user"}
```

Returns the conversation (creating it if it doesn't exist). Idempotent - calling with the same user pair always returns the same conversation.

#### Message Pagination

Messages support cursor-based pagination:

```
GET /api/v1/dms/{conversation_id}/messages?cursor=2025-01-01T00:00:00Z&limit=50
```

- `cursor`: ISO timestamp to start after
- `limit`: Max messages to return (default: 50, max: 100)
- Messages are returned oldest to newest (for chat display)

#### Read Receipts

Mark messages as read:

```bash
POST /api/v1/dms/{conversation_id}/read
Content-Type: application/json
Authorization: Bearer <token>

{"last_read_message_id": "uuid-of-last-read-message"}
```

### WebSocket Real-Time

Real-time updates via WebSocket connections.

| Endpoint | Description | Auth |
|----------|-------------|------|
| `WS /api/v1/ws/discussions/episodes/{episode_id}` | Episode discussion updates | Optional |
| `WS /api/v1/ws/dms/{conversation_id}` | DM conversation updates | Required |

#### Discussion WebSocket

Connect to receive real-time updates for episode discussions:

```
ws://localhost:8000/api/v1/ws/discussions/episodes/{episode_id}?token=<jwt>
```

- **Token optional**: Anonymous users can subscribe to read-only updates
- **Token required for**: Sending typing events

**Events received:**
- `thread_created` - New thread created
- `post_created` - New post created
- `reaction_toggled` - Reaction added/removed

**Event envelope format:**
```json
{
  "type": "post_created",
  "ts": "2025-01-01T12:00:00Z",
  "payload": {
    "post_id": "uuid",
    "thread_id": "uuid",
    "user_id": "uuid",
    "body": "Post content",
    "created_at": "2025-01-01T12:00:00Z"
  }
}
```

#### DM WebSocket

Connect to receive real-time updates for DM conversations:

```
ws://localhost:8000/api/v1/ws/dms/{conversation_id}?token=<jwt>
```

- **Token required**: Must be authenticated
- **Membership required**: Must be a member of the conversation

**Events received:**
- `dm_message_created` - New message in conversation
- `dm_read_updated` - Read receipt updated
- `typing` - User started/stopped typing
- `presence` - User came online/offline

**Client messages:**
```json
{"type": "typing_start", "payload": {}}
{"type": "typing_stop", "payload": {}}
{"type": "heartbeat", "payload": {}}
```

- Send `heartbeat` every 20 seconds to maintain presence
- Connections without heartbeat for 45 seconds are considered offline

**Event envelope format:**
```json
{
  "type": "dm_message_created",
  "ts": "2025-01-01T12:00:00Z",
  "payload": {
    "message_id": "uuid",
    "conversation_id": "uuid",
    "sender_id": "uuid",
    "body": "Hello!",
    "created_at": "2025-01-01T12:00:00Z"
  }
}
```

#### System Events

Both WebSocket endpoints send these system events:

- `subscribed` - Confirmation of successful subscription
- `error` - Error message (invalid JSON, auth required, etc.)

## Database

The API uses Supabase with the following schemas:

- `core` - Shows, seasons, episodes, cast
- `surveys` - Surveys, questions, responses
- `social` - Discussion threads, posts, reactions, DM conversations, messages, read receipts

See [docs/db/schema.md](../db/schema.md) for full schema documentation.

## API Documentation

When running locally, interactive docs are available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
