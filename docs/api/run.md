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

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/v1/shows` | List all shows |
| GET | `/api/v1/shows/{id}` | Get show details |
| GET | `/api/v1/shows/{id}/seasons` | List seasons |
| GET | `/api/v1/shows/{id}/cast` | List cast members |
| GET | `/api/v1/surveys` | List surveys |
| GET | `/api/v1/surveys/{id}` | Get survey with questions |
| GET | `/api/v1/surveys/{id}/results` | Get live aggregate results |
| POST | `/api/v1/surveys/{id}/submit` | Submit survey, get instant results |

See `/docs` endpoint for full interactive API documentation.
