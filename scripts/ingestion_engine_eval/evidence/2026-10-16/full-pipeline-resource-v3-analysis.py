"""Post-run attribution only; never opens a database or reruns workloads."""
import collections
import hashlib
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent
ARMS = {
    "legacy": ROOT / "representative-legacy-v3-01",
    "candidate": ROOT / "representative-candidate-595fda4b-v3-01",
}


def category(row):
    q = " ".join(row["query"].lower().replace('"', '').split())
    if "pg_stat_statements_reset" in q:
        return "observer-reset-excluded"
    if row["role"] == "eval_runtime":
        return "DBOS SDK, lease, listener and SDK session SQL"
    if q in {"begin", "commit", "rollback"} or q.startswith("set ") or "select set_config(" in q:
        return "Business transaction/session setup"
    if "information_schema" in q or "to_regclass(" in q:
        return "Business schema capability probes"
    if "pg_advisory" in q or "pg_try_advisory" in q:
        return "Business advisory/identity locks"
    if q == "select $1" or q.startswith("with legacy as"):
        return "Candidate capacity admission and fresh-session probes"
    if q.startswith("select o.*,r.account,r.engine,r.workflow_version from social.instagram_ingestion_outbox"):
        return "Coordinator pending-outbox/partition polling and error scans"
    if q.startswith("select p.*,r.account from social.instagram_ingestion_partitions") and "where execution_id=" not in q:
        return "Coordinator pending-outbox/partition polling and error scans"
    if "social.instagram_detail_request_lanes" in q or "social.ig_comment_rate_pace" in q or "social.account_auth_cooldown" in q or q.startswith("select clock_timestamp() as current_time"):
        return "Shared request pressure, pacing and auth cooldown"
    if "social.instagram_detail_run_targets" in q:
        return "Legacy durable detail targets/receipts"
    if "social.instagram_ingestion_evidence" in q or "social.instagram_ingestion_receipts" in q or "social.instagram_ingestion_comment_pages" in q:
        return "Business evidence, receipts and comment-page durability"
    if "social.instagram_ingestion_decisions" in q or "social.instagram_ingestion_dispatches" in q or "social.instagram_ingestion_partitions" in q or "social.instagram_ingestion_outbox" in q:
        return "Business dispatch, decision and partition journal"
    if "social.avatar_registry" in q or q.startswith("update social.instagram_comments set hosted_author_profile_pic_url"):
        return "Avatar registry and hosted-avatar linking"
    if "social.instagram_comments" in q or "social.instagram_post_comments_audit" in q or "social.comment_capture_health" in q:
        return "Comment models, audit and inventory"
    if "social.instagram_posts" in q or "social.instagram_account_catalog" in q or "social.instagram_post_payloads" in q or "social.social_post" in q:
        return "Post/catalog/canonical models, preservation and media inventory"
    if "social.instagram_ingestion_" in q:
        return "Business run/target ownership, request accounting and manifest state"
    if "social.scrape_workers" in q:
        return "Legacy worker registry/health"
    if "social.scrape_jobs" in q or "social.scrape_runs" in q:
        return "Legacy job/run lifecycle and progress"
    if "core." in q or "social.shared_account_sources" in q:
        return "Native core/storage context and source status"
    raise AssertionError("Unclassified normalized statement: " + q)


def analyze(name, root):
    measurement = json.loads((root / "measurement.json").read_text())
    assert measurement["valid"] and measurement["observer_error"] is None
    assert measurement["final_sessions"] == []
    assert json.loads((root / "sql-errors.json").read_text()) == []
    assert json.loads((root / "cleanup.json").read_text())["postgres_stopped"]
    phases = {}
    for phase, rows in (("completion", measurement["completion"]["queries"]), ("full", measurement["queries"])):
        counts = collections.Counter()
        classified = []
        for row in rows:
            group = category(row)
            counts[group] += row["calls"]
            classified.append({**row, "category": group})
        assert counts["observer-reset-excluded"] == 1
        phases[phase] = {"raw_calls": sum(counts.values()),
                         "execution_calls": sum(counts.values()) - 1,
                         "categories": dict(sorted(counts.items())), "queries": classified}
    completion_samples = [row for row in measurement["samples"] if row["seconds"] <= measurement["completion"]["seconds"]]
    peak_example = next(row for row in measurement["samples"] if sum(v[3] for v in row["sessions"]) == measurement["peak_sessions"])
    roles = {phase: {role: sum(q["calls"] for q in rows["queries"] if q["role"] == role)
                     for role in ("eval", "eval_runtime")}
             for phase, rows in phases.items()}
    timings = [json.loads(line) for line in (root / "dispatch-timings.jsonl").read_text().splitlines()]
    transport = [json.loads(line) for line in (root / "transport-events.jsonl").read_text().splitlines()]
    cdn = collections.Counter(row["asset"] for row in transport if row["kind"] == "cdn")
    avatar_cdn = {key: cdn[key] for key in ("owner.png", "reader.png", "replier.png")}
    summary = {
        "phases": phases, "source": json.loads((root / "sealed-inputs.json").read_text()),
        "completion_seconds": measurement["completion"]["seconds"],
        "lifecycle_seconds": measurement["full_lifecycle_seconds"],
        "peak_sessions": measurement["peak_sessions"], "peak_sample": peak_example,
        "completion_peak_sessions": max(sum(v[3] for v in row["sessions"]) for row in completion_samples),
        "role_calls_raw": roles,
        "dispatch_seconds_sum": sum(row["seconds"] for row in timings),
        "dispatches": len(timings), "avatar_cdn_requests": avatar_cdn,
        "operations": json.loads((root / "operation-counts.json").read_text()),
    }
    return summary


result = {name: analyze(name, root) for name, root in ARMS.items()}
comparison = json.loads((ARMS["candidate"] / "matched-output-comparison.json").read_text())
assert comparison["common_fields_and_asset_bytes"] == "passed"
result["semantic_comparison"] = comparison
result["deltas"] = {}
for phase in ("completion", "full"):
    old, new = (result[name]["phases"][phase]["execution_calls"] for name in ARMS)
    categories = set(result["legacy"]["phases"][phase]["categories"]) | set(result["candidate"]["phases"][phase]["categories"])
    delta = {key: result["candidate"]["phases"][phase]["categories"].get(key,0) - result["legacy"]["phases"][phase]["categories"].get(key,0) for key in sorted(categories)}
    assert sum(delta.values()) == new - old
    result["deltas"][phase] = {"calls": new-old, "percent": (new/old-1)*100,
                                "categories": delta, "legacy_per_post": old/12, "candidate_per_post": new/12}
result["interpretation"] = {
    "acceptance": "parent-owned; no threshold waiver",
    "unclassified_sql_calls": 0,
    "necessity_or_optimality_proven": False,
    "attribution_limit": "SQL categories explain observed work; they do not prove every extra call is necessary or irreducible",
    "timing": "one serial trial per arm with instrumented fake I/O; descriptive, not causal or live",
    "workload_boundary": "explicit legacy27-job driver/native per-job entrypoint vs real candidate DBOS/native bounded activities; old HTTP launch/planner/periodic sweep and new account-specific canary/HTTP wrapper excluded",
}
result["tail"] = {}
for name in ARMS:
    arm = result[name]
    groups = set(arm["phases"]["full"]["categories"]) | set(arm["phases"]["completion"]["categories"])
    counts = {k: arm["phases"]["full"]["categories"].get(k,0)-arm["phases"]["completion"]["categories"].get(k,0) for k in sorted(groups)}
    result["tail"][name] = {"seconds": arm["lifecycle_seconds"]-arm["completion_seconds"], "calls": sum(counts.values()), "categories": counts}
    arm["statement_setup_breakdown"] = {}
    for phase in ("completion", "full"):
        arm["statement_setup_breakdown"][phase] = {q["query"]: q["calls"] for q in arm["phases"][phase]["queries"] if q["category"] == "Business transaction/session setup"}
    receipts = [json.loads(p.read_text()) for p in ARMS[name].glob("pool-*.json")]
    assert receipts and all(p == receipts[0] for p in receipts)
    arm["pool_receipts"] = {"count": len(receipts), "all_identical": True, "receipt": receipts[0]}
result["operation_delta"] = {"additional_cdn_avatar_downloads": 33, "additional_head": 180,
    "additional_client_close_events_not_network": 12, "storage_writes_both": 147,
    "legacy_network_operations": 216, "candidate_network_operations": 427,
    "equation": "427 - 216 = 180 HEAD + 33 CDN - 2 Instagram = 211"}
result["interpretation"].update({
    "session_limit": "10ms sampled aggregate lower bound; no per-PID sessions; topology mapping inferred from application/role plus frozen source and process receipts",
    "cap_evidence": "all per-process resolved receipts match default1/2, other named pools1/1, close_after_return=true; no configured cap increase; not a live aggregate/Supavisor cap proof",
    "tail": "global coordinator bounded scheduling loop remains available for other outbox/partition work until lifetime; run business completed earlier, engine status/exit are distinct",
    "no_additional_runs": True})
result["analysis_sha256"] = hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
(ROOT / "representative-attribution.json").write_text(json.dumps(result,indent=2)+"\n")
for phase in ("completion", "full"):
    print(phase,result["deltas"][phase])
print("avatar-CDN",{name:result[name]["avatar_cdn_requests"] for name in ARMS})
