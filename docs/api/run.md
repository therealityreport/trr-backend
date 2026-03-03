# TRR Backend API - Running Guide

This document describes how to run the FastAPI-based TRR Backend API locally and deployment considerations.
For production deployment, use `docs/deploy/cloud_run.md`.

## Required Environment Variables

The API requires the following environment variables to be set:

### Supabase (Required)

| Variable | Description | Example |
|----------|-------------|---------|
| `SUPABASE_URL` | Your Supabase project URL | `https://your-project.supabase.co` |
| `SUPABASE_ANON_KEY` | Supabase anonymous/public key | `eyJhbGciOiJIUzI1NiIs...` |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase service role key (for admin operations) | `eyJhbGciOiJIUzI1NiIs...` |

### Screenalytics (Service-to-service)

| Variable | Description | Example |
|----------|-------------|---------|
| `SCREENALYTICS_SERVICE_TOKEN` | Shared service token for Screenalytics endpoints | `change-me-long-random-token` |
| `SCREENALYTICS_API_URL` | Base URL for the Screenalytics service (auto-count) | `https://screenalytics.example.com` |

If you call `/api/v1/screenalytics/*` or `/api/v1/screenalytics/v2/*`, the backend must have `SCREENALYTICS_SERVICE_TOKEN` set and clients must send it as a Bearer token. If the backend needs to call Screenalytics for auto-counting, set `SCREENALYTICS_API_URL` to the Screenalytics base URL.

### Internal Admin Proxy (TRR-APP -> TRR-Backend)

| Variable | Description | Example |
|----------|-------------|---------|
| `TRR_INTERNAL_ADMIN_SHARED_SECRET` | Shared secret required for service-role calls to facebank seed toggle admin endpoint | `long-random-shared-secret` |

For `PATCH /api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed`, backend accepts:
- allowlisted user JWT (`ADMIN_EMAIL_ALLOWLIST`), or
- `service_role` JWT **only** when header `X-TRR-Internal-Admin-Secret` matches `TRR_INTERNAL_ADMIN_SHARED_SECRET`.

### Redis (Optional)

| Variable | Description | Example |
|----------|-------------|---------|
| `REDIS_URL` | Redis connection URL for pub/sub (optional) | `redis://localhost:6379` |

If `REDIS_URL` is not set, the API uses an in-memory broker for WebSocket pub/sub. This is fine for local development and single-instance deployments. For multi-instance production deployments, set `REDIS_URL` to enable cross-instance real-time event delivery.

### Backend runtime behavior

| Variable | Description | Example |
|----------|-------------|---------|
| `TRR_BACKEND_WORKERS` | Number of uvicorn worker processes. Defaults to `1` for local stability. | `4` |
| `TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER` | When `1`, blocks multi-worker startup unless `REDIS_URL` is present, forcing `1` worker to avoid websocket broker fragmentation. | `1` |
| `TRR_BACKEND_RELOAD` | Enables uvicorn `--reload` in local mode. | `0` / `1` |

By default, `TRR_BACKEND_WORKERS` is `1` and `TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER=1`, so multi-worker mode is only enabled when `REDIS_URL` is configured. If `TRR_BACKEND_WORKERS > 1` is requested while `REDIS_URL` is missing, startup logs explicitly warn and fall back to a single worker.

### CORS (Optional)

| Variable | Description | Example |
|----------|-------------|---------|
| `CORS_ALLOW_ORIGINS` | Comma-separated allowed origins | `https://therealityreport.com,https://app.therealityreport.com` |

If `CORS_ALLOW_ORIGINS` is not set, the API allows all origins but disables credentials (safer default for development).

When origins are explicitly set:
- Only listed origins are allowed
- Credentials are enabled (required for authenticated requests)

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

# Edit .env with your Supabase credentials
nano .env  # or use your preferred editor
```

At minimum, set these in `.env`:
```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your_anon_key
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
TRR_INTERNAL_ADMIN_SHARED_SECRET=your_internal_shared_secret
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
| TRR App internal proxy (facebank toggle) | `Authorization: Bearer <SUPABASE_SERVICE_ROLE_KEY>` + `X-TRR-Internal-Admin-Secret: <TRR_INTERNAL_ADMIN_SHARED_SECRET>` | Allowed only for `PATCH /api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed`. |
| Screenalytics | `Authorization: Bearer <SCREENALYTICS_SERVICE_TOKEN>` | Use for `/api/v1/screenalytics/*` and `/api/v1/screenalytics/v2/*`. |

Admin allowlist
- Facebank seed toggle endpoint requires either allowlisted user JWT or service-role plus valid `X-TRR-Internal-Admin-Secret`.

**CORS guidance**

- TRR App: set `CORS_ALLOW_ORIGINS` in the backend env to the app domain(s) so credentials are allowed.
- Screenalytics: server-to-server calls do not require CORS.

**Example calls**

```bash
# TRR App (Supabase JWT)
curl -H "Authorization: Bearer $SUPABASE_ACCESS_TOKEN" \
  http://localhost:8000/api/v1/shows

# Screenalytics (service token)
curl -H "Authorization: Bearer $SCREENALYTICS_SERVICE_TOKEN" \
  http://localhost:8000/api/v1/screenalytics/episodes/<episode_id>/cast
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

This guide is for local development. For production Docker and Cloud Run deployment, use `docs/deploy/cloud_run.md`.

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
