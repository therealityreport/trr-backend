# graphify reference: optional exports

Load this only for an explicitly requested export flag. Verify the graph is fresh
first and use the pinned, already-installed local Graphify interpreter from the
core runbook. A missing graph, stale graph, or missing local package is a stop
condition, not an installation prompt.

## Local artifact exports

These commands operate only on the local graph and require the matching explicit
flag:

```bash
graphify export wiki
graphify export neo4j
graphify export falkordb
graphify export svg
graphify export graphml
"$GRAPHIFY_PYTHON" -m graphify.serve graphify-out/graph.json
graphify benchmark
```

Run `wiki` before the current-run cleanup only when `--wiki` was explicitly
requested. Run `benchmark` only when the detected corpus exceeds 5,000 words.
The MCP server is a local foreground process; do not register it in another app or
start it in the background without separate user authority.

## Neo4j and FalkorDB pushes

`--neo4j-push` and `--falkordb-push` create an external side effect and are not
part of the default workflow. They are prohibited by the current backend policy
unless the user separately authorizes the target and credentials.

Never put a password, token, connection string containing a password, or secret
file contents on a command line, in shell history, in a request JSON file, or in
an agent transcript. Use the package's documented host-managed secret environment
mechanism or a protected credential file that the command reads itself. If the
pinned Graphify version cannot use such a mechanism, do not push; generate the
local Cypher artifact instead.

After the host has injected the secret outside the transcript, the visible command
may contain only a non-secret URI and user value, both passed as quoted arguments:

```bash
graphify export neo4j --push "$GRAPHIFY_NEO4J_URI" --user "$GRAPHIFY_NEO4J_USER"
```

Do not add a password-bearing command option, echo an environment value, or log the resolved
environment. Report only that a host-managed secret was available or unavailable.
