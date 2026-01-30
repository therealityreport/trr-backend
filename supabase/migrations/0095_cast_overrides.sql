BEGIN;

-- ---------------------------------------------------------------------------
-- Person-level overrides
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.people_overrides (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  person_id uuid NOT NULL REFERENCES core.people (id) ON DELETE CASCADE,
  full_name_override text,
  instagram_handle text,
  external_ids_override jsonb NOT NULL DEFAULT '{}'::jsonb,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (person_id)
);

CREATE INDEX IF NOT EXISTS idx_people_overrides_person_id
  ON core.people_overrides (person_id);

DROP TRIGGER IF EXISTS people_overrides_set_updated_at ON core.people_overrides;
CREATE TRIGGER people_overrides_set_updated_at
BEFORE UPDATE ON core.people_overrides
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

GRANT SELECT ON core.people_overrides TO service_role;
GRANT INSERT, UPDATE, DELETE ON core.people_overrides TO service_role;

-- ---------------------------------------------------------------------------
-- Show-cast overrides
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.show_cast_overrides (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  show_id uuid NOT NULL REFERENCES core.shows (id) ON DELETE CASCADE,
  person_id uuid NOT NULL REFERENCES core.people (id) ON DELETE CASCADE,
  credit_category text NOT NULL DEFAULT 'Self',
  friend_of boolean,
  role_override text,
  billing_order_override integer,
  notes_override text,
  tags_override text[] NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (show_id, person_id, credit_category)
);

CREATE INDEX IF NOT EXISTS idx_show_cast_overrides_show_id
  ON core.show_cast_overrides (show_id);

CREATE INDEX IF NOT EXISTS idx_show_cast_overrides_person_id
  ON core.show_cast_overrides (person_id);

DROP TRIGGER IF EXISTS show_cast_overrides_set_updated_at ON core.show_cast_overrides;
CREATE TRIGGER show_cast_overrides_set_updated_at
BEFORE UPDATE ON core.show_cast_overrides
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

GRANT SELECT ON core.show_cast_overrides TO service_role;
GRANT INSERT, UPDATE, DELETE ON core.show_cast_overrides TO service_role;

COMMIT;
