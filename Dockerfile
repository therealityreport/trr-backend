FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies needed by psycopg2-binary and Pillow
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    libcairo2 \
    libffi-dev \
    libjpeg62-turbo-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency manifests first for better layer caching
COPY requirements.txt requirements.lock.txt ./
RUN test -f requirements.lock.txt && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Cloud Run/Render use PORT; keep container launches single-process by default.
ENV PORT=8080 \
    TRR_BACKEND_HOST=0.0.0.0 \
    TRR_BACKEND_RELOAD=0

EXPOSE ${PORT}

CMD ["./start-api.sh"]
