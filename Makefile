PYTHON := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

.PHONY: schema-docs schema-docs-check ci-local

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
