schema-docs:
	@python3 scripts/supabase/generate_schema_docs.py

schema-docs-check:
	@python3 scripts/supabase/generate_schema_docs.py
	git diff --exit-code supabase/schema_docs
