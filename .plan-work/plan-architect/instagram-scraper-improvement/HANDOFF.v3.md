# HANDOFF.v3.md

## Target Skill

Use `orchestrate-subagents`.

## Execution Mode

Serialized integration with parallel discovery allowed. Implementation should use worker subagents with disjoint write ownership:

1. Budget worker: control-plane budget module and tests.
2. Completion worker: snapshot completion/retry target model and tests.
3. Media gate worker: hosted/comment media completion gate helpers and tests.

## Shared Rules

- Start from current files only.
- Do not read saved notes, prior sessions, handoffs, memories, or stale plans.
- Do not revert unrelated dirty-tree changes.
- Keep code changes narrow.
- List changed files and validation commands in worker final messages.
- Lead integrates any overlap and runs final validation.

## Required Work

3. Create the adaptive control-plane budget module.
4. Implement snapshot completion and retry target tracking.
5. Add hosted media and comment media completion gates.

## Validation

Run focused backend tests for the touched areas plus `py_compile` for new/changed modules. Modal readiness is required only after runtime changes are ready to send to Modal.
