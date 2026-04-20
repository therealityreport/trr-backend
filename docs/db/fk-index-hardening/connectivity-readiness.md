# FK Index Hardening — Direct-Host Connectivity Readiness

Completed: 2026-04-20

## Symptom

Initial Stage 0 Task 0.1 probe (from operator workstation at start of execution) reported direct host unreachable from some network paths. After operator confirmed `.env` credentials in place, `TRR_DB_URL` resolution via direct-host connection string succeeded.

## Network Restrictions

Not checked against a specific allowlist entry — direct host was reachable once `.env` credentials were loaded. No temporary IP allowlist entry was required for this rollout preparation. If future access fails for the Stage 4/5 operator, check Supabase Dashboard → Project Settings → Database → Network Restrictions for current workstation public IP (`curl -s https://ifconfig.me`).

## Path Test

```
$ TRR_DB_URL=... ./scripts/db/run_sql.sh -c "select inet_server_addr(), inet_server_port(), current_setting('application_name', true);"
inet_server_addr: 2600:1f18:2e13:9d1d:89c0:ebe8:2026:178b
inet_server_port: 5432
application_name: Supavisor
```

IPv6 AWS us-east-1 address; port `5432`; `application_name` default value `Supavisor` (the pooler layer is passthrough session-mode, not transaction-pool multiplexing). This is the expected shape for Supabase's direct-host connection on managed tiers — Supavisor sits in front but preserves per-session semantics so `CREATE INDEX CONCURRENTLY` works end-to-end.

## Unblock Path

**Option A (implicit):** Operator's `.env` already had a working `TRR_DB_URL` pointing at `db.vwxfvzutyufrkhfgoeaa.supabase.co:5432`. No new allowlist entry, no bastion tunnel, no in-VPC host needed.

## Concurrent-DDL Verification

Stage 0 Task 0.3 Step 2 probe executed successfully on 2026-04-20:

```
$ ./scripts/db/run_sql.sh -c "create table if not exists public._fk_index_connectivity_probe (id int);"
CREATE TABLE

$ ./scripts/db/run_sql.sh -c "create index concurrently if not exists _fk_probe_idx on public._fk_index_connectivity_probe (id);"
CREATE INDEX

$ ./scripts/db/run_sql.sh -c "drop index concurrently if exists public._fk_probe_idx;"
DROP INDEX

$ ./scripts/db/run_sql.sh -c "drop table if exists public._fk_index_connectivity_probe;"
DROP TABLE
```

**Interpretation:** `CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` both succeeded outside a transaction block. Session-mode Supavisor does not break concurrent DDL. Stage 4 / Stage 5 apply paths are viable against this `TRR_DB_URL` value.

## Post-Rollout Revert

No revert action required — no temporary IP allowlist entry was created, no bastion tunnel was started. Normal `.env`-based connection will continue to work post-rollout.
