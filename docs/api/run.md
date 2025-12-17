# TRR Backend API - Running Guide

This document describes how to run the FastAPI-based TRR Backend API locally and deployment considerations.

## Required Environment Variables

The API requires the following environment variables to be set:

### Supabase (Required)

| Variable | Description | Example |
|----------|-------------|---------|
| `SUPABASE_URL` | Your Supabase project URL | `https://your-project.supabase.co` |
| `SUPABASE_ANON_KEY` | Supabase anonymous/public key | `eyJhbGciOiJIUzI1NiIs...` |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase service role key (for admin operations) | `eyJhbGciOiJIUzI1NiIs...` |

### CORS (Optional)

| Variable | Description | Example |
|----------|-------------|---------|
| `CORS_ALLOW_ORIGINS` | Comma-separated allowed origins | `https://therealityreport.com,https://app.therealityreport.com` |

If `CORS_ALLOW_ORIGINS` is not set, the API allows all origins but disables credentials (safer default for development).

When origins are explicitly set:
- Only listed origins are allowed
- Credentials are enabled (required for authenticated requests)

## Running Locally

### 1. Set up Python environment

```bash
# Create and activate virtual environment
python3 -m venv .venv
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

## Running Tests

```bash
# Run all API smoke tests
python -m pytest tests/test_api_smoke.py -v
pytest tests/test_discussions_smoke.py -v
pytest tests/test_dms_smoke.py -v

# Run with coverage
python -m pytest tests/ --cov=api --cov-report=term-missing
```

## Deployment Considerations

### Where to Host

The TRR API is a **FastAPI application** that:
- Uses synchronous Supabase client calls
- May add WebSocket support for real-time features in the future
- Requires persistent process (not serverless)

**Recommended hosting options:**

| Platform | Pros | Cons |
|----------|------|------|
| **Railway** | Easy deploy, good for Python, supports WebSockets | Cost scales with usage |
| **Render** | Free tier, auto-deploy from GitHub | Cold starts on free tier |
| **Fly.io** | Global edge deployment, WebSocket support | More complex setup |
| **DigitalOcean App Platform** | Simple, predictable pricing | Less auto-scaling |
| **AWS ECS/Fargate** | Full control, scalable | Complex setup |

**Not recommended:**
- **Vercel** - Optimized for Next.js/serverless, not ideal for long-running Python processes or WebSockets
- **AWS Lambda** - Cold starts, 15-minute timeout, not ideal for potential real-time features

### Production Configuration

For production deployments:

1. **Set explicit CORS origins:**
   ```
   CORS_ALLOW_ORIGINS=https://therealityreport.com,https://app.therealityreport.com
   ```

2. **Use a process manager:**
   ```bash
   # With gunicorn + uvicorn workers
   gunicorn api.main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
   ```

3. **Add to requirements.txt for production:**
   ```
   gunicorn>=21.0.0
   ```

4. **Configure logging:**
   Set log level via environment or uvicorn args:
   ```bash
   uvicorn api.main:app --log-level info
   ```

### Docker Deployment

Example `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/ ./api/

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

Build and run:
```bash
docker build -t trr-api .
docker run -p 8000:8000 --env-file .env trr-api
```

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
