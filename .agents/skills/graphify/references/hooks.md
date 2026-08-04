# graphify reference: hooks and AGENTS.md integration

Load this only when the user explicitly asks about repository configuration.

## Read-only status

The only routine hook operation permitted by this backend policy is a read-only
status inspection:

```bash
graphify hook status
```

Do not install, uninstall, append to, or replace a Git hook from this runbook. A
hook must never rebuild a graph, invoke an external backend, update a manifest, or
mutate agent configuration automatically. It may at most report that graph
freshness should be checked.

## AGENTS.md guidance

Do not run `graphify agents install` or `graphify agents uninstall`: both mutate a
repository policy file. If a user separately authorizes a policy edit, present the
exact diff for review and keep the resulting guidance read-only: check freshness,
omit stale graph evidence, and require an explicit requested update. No automatic
rebuild is allowed.
