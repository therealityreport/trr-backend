# Show-season watcher Desktop mirror

The cloud watcher is the source of truth. This optional local command copies
only manifest revisions that pass local policy checks; it never deletes local
files and cloud collection does not depend on the Mac being online.

## Local configuration

Create a local JSON file outside the repository, with permissions appropriate
for its destination paths. The folder values are chosen by the Mac operator,
not by the API. Do not put a bearer token in this file.

```json
{
  "api_base_url": "https://api.example.invalid",
  "allowed_api_hosts": ["api.example.invalid"],
  "allowed_download_hosts": ["media.example.invalid"],
  "destinations": {
    "RHOSLC-S7": "/operator-chosen/local/media-folder"
  },
  "max_download_bytes": 104857600,
  "allowed_content_types": ["image/jpeg", "image/png", "image/webp", "video/mp4", "audio/mpeg"],
  "timeout_seconds": 30,
  "keychain_service": "trr-media-watcher-admin-token",
  "keychain_account": "optional-local-account-name"
}
```

The destination must already exist, be a real directory (not a symlink), and
have no symlinked parent. API and download hosts are exact allowlists; every
redirect is checked again against the relevant list. Use the actual TRR API and
media host names when preparing the local file.

## Authentication and manual operation

The command reads `TRR_MEDIA_WATCHER_TOKEN` first, then
`TRR_ADMIN_BEARER_TOKEN`; if neither is set it can read the configured macOS
Keychain generic-password item. The token is never written to state, config,
logs, or the LaunchAgent template.

```sh
export TRR_MEDIA_WATCHER_CONFIG="/operator-chosen/config/media-watcher.json"
export TRR_MEDIA_WATCHER_TOKEN="<short-lived internal admin bearer token>"
python -m scripts.media.sync_watched_media_to_desktop \
  --watch-id "<watch UUID>" \
  --destination-label "RHOSLC-S7"
```

The client fetches the authenticated manifest, accepts only HTTPS URLs from
the local host allowlists, enforces the configured and manifest byte limits,
requires matching MIME headers, streams into a same-directory `.partial` file,
fsyncs it, verifies SHA-256, and atomically renames it. It writes durable local
state at `.trr-media-watcher-state.json` keyed by revision ID and SHA-256.
Reruns therefore skip verified files; an offline or failed download has no
state entry and is retried next time. A conflicting existing file is preserved
and the verified revision receives a deterministic collision suffix.

## LaunchAgent example

Use [com.trr.media-watcher-sync.plist.example](com.trr.media-watcher-sync.plist.example)
only as a template. Replace each `__PLACEHOLDER__` locally with operator-owned
paths and the authorized watch/label, then inspect it for credentials before
installing it. The supplied template intentionally contains no user path,
token, or Keychain secret and is not installed by this repository.

To stop syncing, unload/remove the locally installed LaunchAgent. Do not use
this client to remove destination files; downloaded files remain user-owned.
