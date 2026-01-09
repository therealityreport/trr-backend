# Git Workflow

```mermaid
gitgraph
    commit id: "main"
    branch feature/my-feature
    commit id: "implement"
    commit id: "tests"
    checkout main
    merge feature/my-feature id: "PR merge"
    commit id: "CI passes"
```

## Branching Strategy

- `main` is the default branch and deployment target
- Feature branches: `feature/<name>`
- Bug fixes: `fix/<name>`
- Documentation: `docs/<name>`
- All changes via PR with CI checks

## Pull Request Process

1. Create feature branch from `main`
2. Implement changes with tests
3. Open PR targeting `main`
4. CI runs: tests, schema-docs-check
5. Code review and approval
6. Squash and merge

## Commit Message Format

```
<type>(<scope>): <description>

[optional body]

Co-Authored-By: Claude <noreply@anthropic.com>
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`
