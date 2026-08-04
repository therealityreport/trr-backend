# graphify reference: incremental update and cluster-only

Load this only for an explicit --update or --cluster-only request. It inherits the
core local-only, pinned-install, structured-request, and output-before-manifest
rules.

## Non-negotiable update invariants

- A request supplies input_path, directed, and force through the ignored structured
  request file; no user value is substituted into shell or Python source.
- Every update materializes current AST, semantic, and extraction artifacts. It
  never reuses leftovers from a prior run.
- The merge preserves stored source/target endpoints.
- The manifest remains unchanged until both graph.json and GRAPH_REPORT.md have
  been written successfully.
- If any gate fails, retain the old graph and leave the pending state for diagnosis.

## 1. Detect and materialize current state

This detection reads the existing manifest but does not save one.

~~~bash
"$GRAPHIFY_PYTHON" - "$GRAPHIFY_REQUEST_FILE" <<'PY'
import json
import sys
from pathlib import Path

from graphify.detect import detect_incremental

request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
root = Path(request["input_path"]).resolve(strict=True)
result = detect_incremental(root)
Path("graphify-out/.graphify_incremental.json").write_text(
    json.dumps(result, ensure_ascii=False), encoding="utf-8"
)
changed = int(result.get("new_total", 0))
deleted = list(result.get("deleted_files", []))
if not changed and not deleted:
    raise SystemExit("No files changed since the last successful graph/report run.")
print(f"Incremental state: {changed} changed, {len(deleted)} deleted")
PY
~~~

Then write the current detection subset and full corpus explicitly:

~~~bash
"$GRAPHIFY_PYTHON" - <<'PY'
import json
from pathlib import Path

incremental = json.loads(Path("graphify-out/.graphify_incremental.json").read_text(encoding="utf-8"))
Path("graphify-out/.graphify_detect.json").write_text(json.dumps({
    "files": incremental.get("new_files", {}),
    "all_files": incremental.get("files", {}),
    "total_files": incremental.get("new_total", 0),
    "total_words": incremental.get("total_words", 0),
    "skipped_sensitive": incremental.get("skipped_sensitive", []),
    "mode": "incremental",
}, ensure_ascii=False), encoding="utf-8")
PY
~~~

For code-only changes, run core Part A, then always create an empty current semantic
artifact and run core Part C. Do not skip Part C.

~~~bash
"$GRAPHIFY_PYTHON" - <<'PY'
import json
from pathlib import Path

empty = {"nodes": [], "edges": [], "hyperedges": [], "input_tokens": 0, "output_tokens": 0}
Path("graphify-out/.graphify_semantic.json").write_text(json.dumps(empty), encoding="utf-8")
PY
~~~

For document, paper, image, or transcript changes, use the core cache flow. Even
when every file is cached, merge the current cached payload into
.graphify_semantic.json and run Part C. If the update has only deletions, overwrite
the AST, semantic, and extraction artifacts with the same empty payload before the
merge; do not conditionally reuse a stale extraction.

~~~bash
"$GRAPHIFY_PYTHON" - <<'PY'
import json
from pathlib import Path

empty = {"nodes": [], "edges": [], "hyperedges": [], "input_tokens": 0, "output_tokens": 0}
incremental = json.loads(Path("graphify-out/.graphify_incremental.json").read_text(encoding="utf-8"))
if int(incremental.get("new_total", 0)) == 0:
    for name in (".graphify_ast.json", ".graphify_semantic.json", ".graphify_extract.json"):
        Path("graphify-out", name).write_text(json.dumps(empty), encoding="utf-8")
    print("Deletion-only update: wrote fresh empty intermediates")
else:
    print("Not deletion-only: retained the current-run extraction artifacts")
PY
~~~

## 2. Merge and stage, but do not save, the manifest

Before merging, retain graphify-out/.graphify_old.json for later comparison. The
following code creates graphify-out/.graphify_manifest_pending.json, which is not a
manifest and must not be treated as one.

~~~bash
"$GRAPHIFY_PYTHON" - "$GRAPHIFY_REQUEST_FILE" <<'PY'
import json
import sys
from pathlib import Path

from graphify.build import build_merge
from graphify.cli import _stamped_manifest_files

request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
root = Path(request["input_path"]).resolve(strict=True)
directed = bool(request.get("directed", False))
graph_path = Path("graphify-out/graph.json")
if not graph_path.is_file():
    raise SystemExit("ERROR: incremental update requires an existing graph.json")
Path("graphify-out/.graphify_old.json").write_bytes(graph_path.read_bytes())

new_extraction = json.loads(Path("graphify-out/.graphify_extract.json").read_text(encoding="utf-8"))
incremental = json.loads(Path("graphify-out/.graphify_incremental.json").read_text(encoding="utf-8"))
deleted = list(incremental.get("deleted_files", []))

manifest_files = _stamped_manifest_files(incremental["files"], new_extraction, root)
semantic_types = {"document", "paper", "image"}
dispatched = {
    item for kind, values in incremental.get("new_files", {}).items()
    if kind in semantic_types for item in values
}
stamped = {item for values in manifest_files.values() for item in values}
scan_corpus = {item for values in incremental["files"].values() for item in values}
Path("graphify-out/.graphify_manifest_pending.json").write_text(json.dumps({
    "manifest_files": manifest_files,
    "scan_corpus": sorted(scan_corpus),
    "clear_semantic": sorted(dispatched - stamped),
}, ensure_ascii=False), encoding="utf-8")

merged_graph = build_merge(
    [new_extraction],
    graph_path=str(graph_path),
    prune_sources=deleted or None,
    root=str(root),
    directed=directed,
)
merged = {
    "nodes": [{"id": node_id, **attrs} for node_id, attrs in merged_graph.nodes(data=True)],
    "edges": [
        {**{key: value for key, value in attrs.items() if key not in {"_src", "_tgt", "source", "target"}},
         "source": attrs.get("_src", left), "target": attrs.get("_tgt", right)}
        for left, right, attrs in merged_graph.edges(data=True)
    ],
    "hyperedges": list(merged_graph.graph.get("hyperedges", [])),
    "input_tokens": new_extraction.get("input_tokens", 0),
    "output_tokens": new_extraction.get("output_tokens", 0),
}
Path("graphify-out/.graphify_extract.json").write_text(
    json.dumps(merged, ensure_ascii=False), encoding="utf-8"
)
print(f"Merged extraction: {len(merged['nodes'])} nodes, {len(merged['edges'])} edges")
PY
~~~

Run core Step 4 next. It is the only stage that writes the new graph and report and
must honor the request's directed and force values. If it fails, do not run the
next step.

## 3. Finalize only after graph and report success

This is the only update location that saves a manifest. Its file checks are a
self-checkable ordering gate.

~~~bash
"$GRAPHIFY_PYTHON" - "$GRAPHIFY_REQUEST_FILE" <<'PY'
import json
import sys
from pathlib import Path

from graphify.detect import save_manifest

request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
root = Path(request["input_path"]).resolve(strict=True)
graph_path = Path("graphify-out/graph.json")
report_path = Path("graphify-out/GRAPH_REPORT.md")
pending_path = Path("graphify-out/.graphify_manifest_pending.json")
if not graph_path.is_file() or not report_path.is_file() or not pending_path.is_file():
    raise SystemExit("ERROR: graph, report, or pending manifest missing; manifest unchanged")
pending = json.loads(pending_path.read_text(encoding="utf-8"))
save_manifest(
    pending["manifest_files"],
    root=str(root),
    scan_corpus=set(pending["scan_corpus"]),
    clear_semantic=set(pending["clear_semantic"]) or None,
)
pending_path.unlink()
print("Manifest saved after the graph and report outputs.")
PY
~~~

Only now may the parent compare .graphify_old.json with the new graph. Remove only
that known backup and the explicit current-run chunk paths after the result has been
reported; never wildcard-scan and merge arbitrary leftover chunks.

## Cluster-only

Cluster-only is an explicit local operation on a freshness-checked graph. It may
regenerate graph/report artifacts but must not install a package, select an external
backend, write a manifest, or reuse deleted intermediate files.
