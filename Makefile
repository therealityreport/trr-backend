PYTHON := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

schema-docs:
	@$(PYTHON) scripts/supabase/generate_schema_docs.py

schema-docs-check:
	@$(PYTHON) scripts/supabase/generate_schema_docs.py
	git diff --exit-code supabase/schema_docs
