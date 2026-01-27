PYTHON := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

.PHONY: doctor schema-docs schema-docs-check schema-docs-reset-check ci-local repo-map repo-map-check

# Environment diagnostic - run before pytest to verify setup
doctor:
	@$(PYTHON) scripts/dev/doctor.py

# Generate schema docs (JSON, MD) and diagrams (Mermaid) from database
schema-docs:
	@$(PYTHON) scripts/supabase/generate_schema_docs.py

# Verify schema docs and diagrams are in sync with database
# IMPORTANT: Requires fresh DB state. If local DB has drifted, run:
#   supabase start && supabase db reset --yes && make schema-docs-check
# Or use the convenience target: make schema-docs-reset-check
schema-docs-check:
	@$(PYTHON) scripts/supabase/generate_schema_docs.py
	git diff --exit-code supabase/schema_docs

# Reset database to migrations and verify schema docs are in sync
# This is a convenience target that ensures DB is fresh before checking
schema-docs-reset-check:
	@echo "Resetting database to migrations..."
	@supabase db reset --yes
	@echo "Verifying schema docs..."
	@$(MAKE) schema-docs-check

ci-local:
	@bash -c 'set -euo pipefail; \
	trap "supabase stop --no-backup" EXIT; \
	docker info >/dev/null; \
	supabase start --exclude gotrue,realtime,storage-api,imgproxy,kong,mailpit,postgrest,postgres-meta,studio,edge-runtime,logflare,vector,supavisor; \
	supabase db reset --yes; \
	"$(PYTHON)" -m pytest; \
	$(MAKE) schema-docs-check'

repo-map:
	@$(PYTHON) scripts/generate_repo_mermaid.py

repo-map-check:
	@$(PYTHON) scripts/generate_repo_mermaid.py
	@if git diff --quiet docs/Repository/generated/; then \
		echo "✅ Repository maps are up to date"; \
	else \
		echo "❌ Repository maps are out of date. Run 'make repo-map' and commit."; \
		git diff docs/Repository/generated/; \
		exit 1; \
	fi
