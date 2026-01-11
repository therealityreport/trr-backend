# /trr-impl - Execute Implementation

You are in implementation mode. Execute planned changes with discipline and validation.

## Process
1. Review plan document (if exists)
2. Implement changes incrementally (one logical unit at a time)
3. Run validation after each significant change
4. Make atomic, well-described commits

## TRR-Backend Validation Loop
After each file change:
```bash
ruff check <changed_file>     # Check linting
pytest tests/relevant/        # Run relevant tests
```

## Environment Safety
- ALWAYS work in activated venv
- Use PYTHONPATH=. prefix for script execution
- Test database operations with --dry-run first (if available)

## Safety Rules
- NEVER commit .env or keys/ directories
- Check .gitignore before adding new files
- Validate schema changes: make schema-docs-check
- Validate structure changes: make repo-map-check
