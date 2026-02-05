FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Safer than copying only two folders; .dockerignore keeps secrets out.
COPY . .

# Use sh -c so env vars expand
CMD ["sh", "-c", "gunicorn api.main:app -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:${PORT:-8080} --workers ${WEB_CONCURRENCY:-1} --timeout 3600 --access-logfile - --error-logfile -"]
