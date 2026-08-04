# graphify reference: query, path, and explain

Load this only for an explicit request against an existing graph. Graph output is
navigation evidence, not source truth: validate every material conclusion against
current source files.

## Request and freshness gate

The parent stores user-controlled query text, node names, traversal mode, and
budget in the ignored structured request file. Do not paste any of those values
into a shell command or a `python -c` string. The request may contain `query`,
`mode` (`bfs` or `dfs`), `budget`, `path_start`, `path_end`, or `node_name`.

Run this read-only gate before **every** query, path, or explain operation. It
checks the graph manifest against the current input root; it never refreshes the
graph or writes a manifest. Any error, missing graph, or changed/deleted corpus
file means the graph is stale. Omit Graphify evidence and inspect current files
instead.

```bash
"$GRAPHIFY_PYTHON" - "$GRAPHIFY_REQUEST_FILE" <<'PY'
import json
import sys
from pathlib import Path

from graphify.detect import detect_incremental

request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
root = Path(request["input_path"]).resolve(strict=True)
graph_path = Path("graphify-out/graph.json")
if not graph_path.is_file():
    raise SystemExit("STALE: graph.json is missing")
try:
    incremental = detect_incremental(root)
except Exception as exc:
    raise SystemExit(f"STALE: read-only freshness check failed: {exc}")
changed = int(incremental.get("new_total", 0))
deleted = list(incremental.get("deleted_files", []))
if changed or deleted:
    raise SystemExit(f"STALE: {changed} changed and {len(deleted)} deleted file(s)")
print("FRESH: graph manifest matches the current corpus")
PY
```

## Query expansion

After the freshness gate passes, extract the graph's vocabulary in memory. Select at
most 12 tokens from the returned list; do not invent synonyms. If no graph
vocabulary matches the request, say so and stop.

```bash
"$GRAPHIFY_PYTHON" - <<'PY'
import json
import re
from pathlib import Path

data = json.loads(Path("graphify-out/graph.json").read_text(encoding="utf-8"))
vocab = set()
for node in data.get("nodes", []):
    for token in re.findall(r"[^\W\d_]+", node.get("label", "") or "", re.UNICODE):
        for part in re.findall(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+", token) or [token]:
            lowered = part.lower()
            if 3 <= len(lowered) <= 30:
                vocab.add(lowered)
print(json.dumps({"vocab": sorted(vocab)}, ensure_ascii=False))
PY
```

Show the selected tokens before traversal. The parent writes the expanded token
string back into the structured request file; it does not interpolate it into
source code.

## Query traversal

Prefer the local CLI only through a subprocess argument array:

```bash
"$GRAPHIFY_PYTHON" - "$GRAPHIFY_REQUEST_FILE" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
query = request.get("expanded_query")
mode = request.get("mode", "bfs")
budget = request.get("budget", 2000)
if not isinstance(query, str) or not query.strip():
    raise SystemExit("ERROR: no graph-vocabulary query was selected")
if mode not in {"bfs", "dfs"} or not isinstance(budget, int) or budget <= 0:
    raise SystemExit("ERROR: invalid traversal request")
args = ["graphify", "query", query, "--budget", str(budget)]
if mode == "dfs":
    args.append("--dfs")
raise SystemExit(subprocess.run(args, check=False).returncode)
PY
```

If the local CLI is unavailable, use this inline fallback. It receives all data
from JSON/argv and preserves stored directed endpoints rather than printing arrows
in traversal order.

```bash
"$GRAPHIFY_PYTHON" - "$GRAPHIFY_REQUEST_FILE" <<'PY'
import json
import sys
from pathlib import Path

import networkx as nx
from networkx.readwrite import json_graph

request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
question = request.get("expanded_query")
mode = request.get("mode", "bfs")
budget = request.get("budget", 2000)
if not isinstance(question, str) or mode not in {"bfs", "dfs"}:
    raise SystemExit("ERROR: invalid traversal request")
if not isinstance(budget, int) or budget <= 0:
    raise SystemExit("ERROR: budget must be a positive integer")

data = json.loads(Path("graphify-out/graph.json").read_text(encoding="utf-8"))
graph = json_graph.node_link_graph(data, edges="links")
terms = [term.lower() for term in question.split() if len(term) >= 3]
scored = []
for node_id, attrs in graph.nodes(data=True):
    score = sum(term in attrs.get("label", "").lower() for term in terms)
    if score:
        scored.append((score, node_id))
start_nodes = [node_id for _, node_id in sorted(scored, reverse=True)[:3]]
if not start_nodes:
    raise SystemExit("No matching nodes found for the selected graph vocabulary")

def edge_attrs(left, right):
    raw = graph[left][right]
    return next(iter(raw.values()), {}) if graph.is_multigraph() else raw

def label(node_id):
    return graph.nodes[node_id].get("label", node_id) if node_id in graph else str(node_id)

def render_edge(left, right):
    attrs = edge_attrs(left, right)
    relation = attrs.get("relation", "")
    confidence = attrs.get("confidence", "")
    if graph.is_directed():
        stored_source = attrs.get("_src", left)
        stored_target = attrs.get("_tgt", right)
        return f"EDGE {label(stored_source)} --{relation} [{confidence}]--> {label(stored_target)}"
    return f"EDGE {label(left)} --{relation} [{confidence}]-- {label(right)} (undirected traversal)"

nodes = set(start_nodes)
edges = []
if mode == "dfs":
    seen, stack = set(), [(node_id, 0) for node_id in reversed(start_nodes)]
    while stack:
        current, depth = stack.pop()
        if current in seen or depth > 6:
            continue
        seen.add(current)
        nodes.add(current)
        for neighbor in graph.neighbors(current):
            if neighbor not in seen:
                stack.append((neighbor, depth + 1))
                edges.append((current, neighbor))
else:
    frontier = set(start_nodes)
    for _ in range(3):
        next_frontier = set()
        for current in frontier:
            for neighbor in graph.neighbors(current):
                if neighbor not in nodes:
                    next_frontier.add(neighbor)
                    edges.append((current, neighbor))
        nodes.update(next_frontier)
        frontier = next_frontier

lines = [f"Traversal: {mode.upper()} | {len(nodes)} nodes"]
for node_id in sorted(nodes, key=lambda item: label(item).lower()):
    attrs = graph.nodes[node_id]
    lines.append(f"NODE {label(node_id)} [src={attrs.get('source_file', '')} loc={attrs.get('source_location', '')}]")
for left, right in edges:
    if left in nodes and right in nodes:
        lines.append(render_edge(left, right))
output = "\n".join(lines)
limit = budget * 4
print(output if len(output) <= limit else output[:limit] + "\n... (truncated at the requested budget)")
PY
```

Answer only from the returned graph data and label any conclusion that needs live
source verification.

## Path and explain

Use the same freshness gate and request-file boundary. For a local CLI call, pass
node terms as argv entries via `subprocess.run(["graphify", "path", start, end])`
or `subprocess.run(["graphify", "explain", node])`; never quote-concatenate them
into a command string.

The fallback must use `graph.is_directed()` as above. For a directed path, render
each edge using stored `_src` and `_tgt`; for an undirected graph, use `--relation--`
without an arrow. An explain result must list outgoing and incoming directed edges
separately; it must not print every neighbor as an outgoing arrow.

~~~bash
"$GRAPHIFY_PYTHON" - "$GRAPHIFY_REQUEST_FILE" <<'PY'
import json
import sys
from pathlib import Path

import networkx as nx
from networkx.readwrite import json_graph

request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
operation = request.get("operation")
data = json.loads(Path("graphify-out/graph.json").read_text(encoding="utf-8"))
graph = json_graph.node_link_graph(data, edges="links")

def choose(term):
    if not isinstance(term, str) or not term.strip():
        raise SystemExit("ERROR: missing node term")
    ranked = sorted(
        ((sum(word in attrs.get("label", "").lower() for word in term.lower().split()), node_id)
         for node_id, attrs in graph.nodes(data=True)), reverse=True
    )
    if not ranked or not ranked[0][0]:
        raise SystemExit("No matching graph node")
    return ranked[0][1]

def attrs(left, right):
    raw = graph[left][right]
    return next(iter(raw.values()), {}) if graph.is_multigraph() else raw

def label(node_id):
    return graph.nodes[node_id].get("label", node_id) if node_id in graph else str(node_id)

def render(left, right):
    edge = attrs(left, right)
    relation = edge.get("relation", "")
    if graph.is_directed():
        return f"{label(edge.get('_src', left))} --{relation}--> {label(edge.get('_tgt', right))}"
    return f"{label(left)} --{relation}-- {label(right)} (undirected)"

if operation == "path":
    start, end = choose(request.get("path_start")), choose(request.get("path_end"))
    path = nx.shortest_path(graph, start, end)
    for left, right in zip(path, path[1:]):
        print(render(left, right))
elif operation == "explain":
    node = choose(request.get("node_name"))
    if graph.is_directed():
        for other in graph.predecessors(node):
            print("incoming:", render(other, node))
        for other in graph.successors(node):
            print("outgoing:", render(node, other))
    else:
        for other in graph.neighbors(node):
            print("connection:", render(node, other))
else:
    raise SystemExit("ERROR: operation must be path or explain")
PY
~~~

## Query-note persistence is opt-in and non-evidentiary

By default, do **not** persist a question, answer, expanded tokens, outcome, or
correction. Do not invoke the package result-persistence command automatically. If the user
explicitly asks to retain a note after seeing the exact redacted content, store it
only under `graphify-out/query-notes/` with `evidence_status: "non_evidentiary"`.
That directory is excluded from the corpus, manifest, and future graph extraction.
Never treat generated Q&A or a saved outcome as source evidence or as a preferred
source for a later answer.
