# Setting up Supabase Credentials in GitHub Codespaces

If you run sync scripts in Codespaces, store required secrets in your repo's Codespaces secrets.

## Required Secrets

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `TMDB_API_KEY`
- `IMDB_API_KEY`

Optional (media sync):
- `AWS_REGION`, `AWS_S3_BUCKET`, `AWS_CDN_BASE_URL`

## Steps

1. GitHub repo → Settings → Secrets and variables → Codespaces
2. Add the secrets listed above
3. In the Codespace, export them to `.env` or load them into your shell before running scripts

Example:
```bash
printf "SUPABASE_URL=%s\nSUPABASE_SERVICE_ROLE_KEY=%s\n" \
  "$SUPABASE_URL" "$SUPABASE_SERVICE_ROLE_KEY" >> .env
```
