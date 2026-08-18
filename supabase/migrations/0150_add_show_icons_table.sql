-- Show icon assets uploaded by admins for survey/icon rating usage.
CREATE TABLE IF NOT EXISTS public.show_icons (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  show_key text NOT NULL,
  filename text NOT NULL,
  s3_key text NOT NULL UNIQUE,
  hosted_url text NOT NULL,
  content_type text NOT NULL,
  size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
  created_by text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_show_icons_show_key_created_at
  ON public.show_icons(show_key, created_at DESC);

ALTER TABLE public.show_icons ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'show_icons'
      AND policyname = 'Allow public read on show_icons'
  ) THEN
    CREATE POLICY "Allow public read on show_icons"
      ON public.show_icons
      FOR SELECT
      USING (true);
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'show_icons'
      AND policyname = 'Allow service role all on show_icons'
  ) THEN
    CREATE POLICY "Allow service role all on show_icons"
      ON public.show_icons
      FOR ALL
      USING (auth.role() = 'service_role')
      WITH CHECK (auth.role() = 'service_role');
  END IF;
END $$;
