# Comparison

## Summary

The original plan had the right target and most of the right file surfaces. The revised plan keeps the same architecture but corrects three execution blockers and adds clearer acceptance criteria.

## Score Delta

| Topic | Original | Revised | Delta | Reason |
| --- | ---: | ---: | ---: | --- |
| Goal clarity | 4.2 | 4.6 | +0.4 | Added non-goals and observable outcomes. |
| Surface awareness | 4.6 | 4.8 | +0.2 | Added `auth_resolver.py` constructor reality and `pg.fetch_one(conn=...)`. |
| Sequencing | 4.1 | 4.6 | +0.5 | Added preflight and cancellation-before-persist ordering. |
| Execution specificity | 3.4 | 4.6 | +1.2 | Fixed invalid test snippets and missing posts warmup metadata handling. |
| Verification | 4.3 | 4.7 | +0.4 | Added tests for posts warmup job-runner metadata and connection-aware cancellation. |
| Gap coverage | 3.7 | 4.5 | +0.8 | Closed pool-pressure, metadata, and markdown-fence gaps. |
| Tool usage | 3.8 | 4.3 | +0.5 | More explicit handling of unavailable Scrapling MCP and installed package validation. |
| Value | 7.8/10 | 9.3/10 | +1.5 | Added measurable post-implementation outcomes. |
| Safety | 3.4 | 4.5 | +1.1 | Prevents adding cancellation checks that can worsen DB pressure. |
| Scope | 4.1 | 4.5 | +0.4 | Keeps full pluggable Scrapling runtime implementation out of scope. |
| Organization | 4.0 | 4.6 | +0.6 | Fixes broken nested code fences and adds stop rules. |
| Bonus | 3.2 | 4.0 | +0.8 | Adds subagent work-scope split and runbook-oriented outcomes. |

## Execution Source

Use `REVISED_PLAN.md`, not the original source plan, for implementation.

