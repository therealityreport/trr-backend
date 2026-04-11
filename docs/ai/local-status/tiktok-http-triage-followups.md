# TikTok HTTP Triage Follow-Ups

Last updated: 2026-04-10

## Handoff Snapshot
```yaml
handoff:
  include: false
  state: recent
  last_updated: 2026-04-10
  current_phase: "auth-vs-no-auth triage captured"
  next_action: "obtain a residential or isp proxy trial url and run the curl_cffi plus proxy comparison"
  detail: self
```

- yt-dlp fallback datetime crash: fixed
- fallback execution after fix: working
- fallback smoke artifact: `/tmp/tiktok-triage/posts-baseline-after-ytdlp-fix.json`
- TikTok cookie availability: present
- cookie audit artifacts:
  - `/tmp/tiktok-triage/tiktok-cookie-audit-raw.json`
  - `/tmp/tiktok-triage/tiktok-cookie-audit-fresh.json`
- auth-vs-no-auth result:
  - canonical loaders returned 27 cookies, including `sessionid`, `sessionid_ss`, and `sid_tt`
  - `requests` + cookies and `curl_cffi` + cookies still ended in `retrieval_mode=ytdlp_fallback`
  - `api_fail_reason` and `api_pagination_blocked_reason` remained `non_json_response`
  - cookies changed `auth_mode` and HTML response size, but did not unblock the API path on this IP
- next step:
  - obtain a residential/ISP proxy trial URL
  - run the `curl_cffi + proxy` comparison against the same `@bravotv` target
