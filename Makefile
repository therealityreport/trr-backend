PYTHON := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

.PHONY: schema-docs schema-docs-check ci-local repo-map repo-map-check

schema-docs:
	@$(PYTHON) scripts/supabase/generate_schema_docs.py

schema-docs-check:
	@$(PYTHON) scripts/supabase/generate_schema_docs.py
	git diff --exit-code supabase/schema_docs

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
