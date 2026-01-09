# Git Workflow

This diagram shows our standard branching strategy.

```mermaid
flowchart TB
    main1[main branch] --> branch[Create feature branch]
    branch --> dev1[Develop & commit]
    dev1 --> dev2[Add tests]
    dev2 --> pr[Open Pull Request]
    pr --> ci[CI checks run]
    ci --> review[Code review]
    review --> merge[Merge to main]
    merge --> main2[main branch updated]

    style main1 fill:#90EE90
    style main2 fill:#90EE90
    style pr fill:#87CEEB
    style merge fill:#FFD700
```

## Branching Strategy

- **main**: Primary branch, always deployable
- **feature/\***: New features and enhancements
- **fix/\***: Bug fixes
- **docs/\***: Documentation updates
- **chore/\***: Maintenance and tooling

## Workflow

1. Create branch from `main`
2. Make changes and commit
3. Open Pull Request
4. CI checks pass
5. Code review
6. Merge to `main`
