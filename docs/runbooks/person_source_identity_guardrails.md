# Person Source Identity Guardrails

## Purpose

Define operational policy and repeatable diagnostics for person-source identity drift
in Bravo cast scope, with strict approved-only behavior for person links.

## Policy

1. Person source links are `approved` or excluded; never `pending` from discovery/sync/backfill.
2. Fresh approvals require strict owner/topic match.
3. For `imdb` and `tmdb` only, transient `fetch_error` may reuse a previously approved URL
   for the same `person_id + link_kind + url_key` (carry-forward path).
4. Duplicate same-name person rows are not merged destructively in routine operations.
5. When duplicate same-name rows conflict on external IDs, prefer the cast-linked canonical row.
6. External ID corrections must be validated against source owner signals before upsert.

## Release Gate Checks

Run from `/Users/thomashulihan/Projects/TRR/TRR-Backend`.

1. Ensure migration parity (`entity_links_unique_active` includes `show_id`).
2. Run backfill dry-run and apply artifacts.
3. Verify post-run rollup counts:
   - `missing_imdb_with_id == 0`
   - `missing_tmdb_with_id == 0`
   - `pending_person_source_rows == 0`
4. Run duplicate identity diagnostics and review conflicts.

## Commands

### Backfill with thresholds

```bash
python scripts/shows/backfill_bravo_person_source_links.py \
  --json-summary /tmp/person_sources_dryrun_release.json \
  --warn-fetch-errors 500 \
  --fail-fetch-errors 1500 \
  --warn-pending-person-sources 0 \
  --fail-pending-person-sources 0

python scripts/shows/backfill_bravo_person_source_links.py \
  --apply \
  --json-summary /tmp/person_sources_apply_release.json \
  --warn-fetch-errors 500 \
  --fail-fetch-errors 1500 \
  --warn-pending-person-sources 0 \
  --fail-pending-person-sources 0
```

### Missing reasons diagnostics

```bash
python scripts/shows/backfill_bravo_person_source_links.py \
  --diagnose-missing-person-sources \
  --diagnose-name "Andy Cohen" \
  --diagnostics-json /tmp/person_sources_diagnostics_andy.json
```

### Duplicate same-name identity diagnostics

```bash
python scripts/shows/diagnose_duplicate_person_external_ids.py \
  --json-summary /tmp/person_duplicate_conflicts.json
```

## Remediation Workflow

1. Confirm whether conflict is transient fetch-error or owner mismatch.
2. For fetch-error on IMDb/TMDb with prior approved source, use carry-forward upsert.
3. For owner mismatch, do not approve; correct external IDs on canonical cast-linked person row.
4. Re-run targeted discovery/backfill for affected shows.
5. Re-run rollup verification and archive artifacts.

## Alert Conditions

1. `pending_person_source_rows > 0`.
2. `missing_imdb_with_id > 0` or `missing_tmdb_with_id > 0`.
3. Abnormal increase in `cleanup_fetch_errors`.
4. New conflicting duplicate identities in cast scope.

## Evidence Artifacts

Store for each release:

1. `/tmp/person_sources_dryrun_release.json`
2. `/tmp/person_sources_apply_release.json`
3. `/tmp/person_sources_diagnostics_*.json`
4. `/tmp/person_duplicate_conflicts*.json`
