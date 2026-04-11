# TikTok Bright Data `curl_cffi` / `requests` Proxy Failure

## Summary

TikTok direct web scraping is no longer the production-default path. `yt-dlp` is the active posts path, and the Bright Data proxy transport issue is intentionally off the critical path.

## Repros

### Plain `curl --proxy` succeeds

```bash
curl --proxy "$BRIGHTDATA_PROXY_URL" \
  -H 'user-agent: Mozilla/5.0' \
  'https://www.tiktok.com/@bravotv'
```

Expected result: the proxy CONNECT tunnel succeeds and TikTok returns the profile page HTML.

### Scraper transport with `requests` fails

```bash
python -m scripts.socials.tiktok.scrape \
  --username bravotv \
  --hashtags RHOBH \
  --start 2026-03-31 \
  --end 2026-04-10 \
  --scrape-mode api \
  --http-client requests \
  --proxy-url "$BRIGHTDATA_PROXY_URL" \
  --diagnostics-json /tmp/tiktok-requests-proxy.json
```

Observed result: the direct TikTok path fails through the scraper transport despite the same proxy succeeding via plain `curl`.

### Scraper transport with `curl_cffi` fails

```bash
python -m scripts.socials.tiktok.scrape \
  --username bravotv \
  --hashtags RHOBH \
  --start 2026-03-31 \
  --end 2026-04-10 \
  --scrape-mode api \
  --http-client curl_cffi \
  --proxy-url "$BRIGHTDATA_PROXY_URL" \
  --diagnostics-json /tmp/tiktok-curl-cffi-proxy.json
```

Observed result: `curl_cffi` fails the same way in this stack.

## Current Read

- Both scraper transports fail against the proxy in the same environment.
- The working plain `curl --proxy` repro suggests the unresolved issue is in the higher-level transport stack rather than the Bright Data endpoint itself.
- The current working hypothesis is a CONNECT tunneling mismatch between the Bright Data proxy and the scraper transport layers.
- This issue is parked because it does not block the production-default TikTok posts path.
