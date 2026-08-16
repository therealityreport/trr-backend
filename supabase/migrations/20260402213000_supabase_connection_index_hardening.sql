-- PREVIEW-25 reconciled 27-table empty-data baseline candidate
-- Generated read-only from Supabase pg_catalog for vwxfvzutyufrkhfgoeaa.
-- Candidate source: f2080b083fed6c4e27b17e42a10f11ff059a144b.
BEGIN;
CREATE SCHEMA IF NOT EXISTS "admin";
CREATE OR REPLACE FUNCTION admin.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
SET search_path TO admin, pg_temp
AS $function$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$function$;
CREATE SCHEMA IF NOT EXISTS "firebase_surveys";
CREATE OR REPLACE FUNCTION firebase_surveys.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
SET search_path TO firebase_surveys, pg_temp
AS $function$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$function$;
-- Required user-defined type closure
CREATE SCHEMA IF NOT EXISTS "firebase_surveys";
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='firebase_surveys' AND t.typname='question_type') THEN CREATE TYPE firebase_surveys.question_type AS ENUM ('single_choice','multi_choice','free_text','likert','numeric','ranking'); END IF; END $$;
CREATE SCHEMA IF NOT EXISTS "admin";
CREATE TABLE IF NOT EXISTS "admin"."brands_franchise_rules" (
  "franchise_key" text NOT NULL,
  "name" text NOT NULL,
  "primary_url" text NOT NULL,
  "review_allpages_url" text,
  "match_terms" text[] DEFAULT ARRAY[]::text[] NOT NULL,
  "aliases" text[] DEFAULT ARRAY[]::text[] NOT NULL,
  "community_domains" text[] DEFAULT ARRAY[]::text[] NOT NULL,
  "include_allpages_scan" boolean DEFAULT false NOT NULL,
  "source_rank" integer DEFAULT 100 NOT NULL,
  "network_terms" text[] DEFAULT ARRAY[]::text[] NOT NULL,
  "is_active" boolean DEFAULT true NOT NULL,
  "rule_version" integer DEFAULT 1 NOT NULL,
  "updated_by" text,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='brands_franchise_rules' AND con.conname='brands_franchise_rules_pkey') THEN ALTER TABLE "admin"."brands_franchise_rules" ADD CONSTRAINT "brands_franchise_rules_pkey" PRIMARY KEY (franchise_key); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS brands_franchise_rules_pkey ON admin.brands_franchise_rules USING btree (franchise_key);
CREATE INDEX IF NOT EXISTS brands_franchise_rules_active_rank_idx ON admin.brands_franchise_rules USING btree (is_active, source_rank, franchise_key);
ALTER TABLE "admin"."brands_franchise_rules" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "admin";
CREATE TABLE IF NOT EXISTS "admin"."covered_shows" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "trr_show_id" uuid NOT NULL,
  "show_name" text NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "created_by_firebase_uid" text NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='covered_shows' AND con.conname='covered_shows_pkey') THEN ALTER TABLE "admin"."covered_shows" ADD CONSTRAINT "covered_shows_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='covered_shows' AND con.conname='covered_shows_trr_show_id_key') THEN ALTER TABLE "admin"."covered_shows" ADD CONSTRAINT "covered_shows_trr_show_id_key" UNIQUE (trr_show_id); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS covered_shows_pkey ON admin.covered_shows USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS covered_shows_trr_show_id_key ON admin.covered_shows USING btree (trr_show_id);
ALTER TABLE "admin"."covered_shows" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "admin";
CREATE TABLE IF NOT EXISTS "admin"."person_cover_photos" (
  "person_id" uuid NOT NULL,
  "photo_id" uuid NOT NULL,
  "photo_url" text NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "created_by_firebase_uid" text NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='person_cover_photos' AND con.conname='person_cover_photos_pkey') THEN ALTER TABLE "admin"."person_cover_photos" ADD CONSTRAINT "person_cover_photos_pkey" PRIMARY KEY (person_id); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS person_cover_photos_pkey ON admin.person_cover_photos USING btree (person_id);
ALTER TABLE "admin"."person_cover_photos" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "admin";
CREATE TABLE IF NOT EXISTS "admin"."recent_people_views" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "firebase_uid" text NOT NULL,
  "person_id" uuid NOT NULL,
  "show_context" text,
  "view_count" integer DEFAULT 1 NOT NULL,
  "first_viewed_at" timestamp with time zone DEFAULT now() NOT NULL,
  "last_viewed_at" timestamp with time zone DEFAULT now() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='recent_people_views' AND con.conname='recent_people_views_pkey') THEN ALTER TABLE "admin"."recent_people_views" ADD CONSTRAINT "recent_people_views_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='recent_people_views' AND con.conname='recent_people_views_unique_user_person') THEN ALTER TABLE "admin"."recent_people_views" ADD CONSTRAINT "recent_people_views_unique_user_person" UNIQUE (firebase_uid, person_id); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS recent_people_views_pkey ON admin.recent_people_views USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS recent_people_views_unique_user_person ON admin.recent_people_views USING btree (firebase_uid, person_id);
CREATE INDEX IF NOT EXISTS idx_recent_people_views_user_last_viewed ON admin.recent_people_views USING btree (firebase_uid, last_viewed_at DESC);
CREATE INDEX IF NOT EXISTS admin_recent_people_views_person_id_idx ON admin.recent_people_views USING btree (person_id);
ALTER TABLE "admin"."recent_people_views" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "admin";
CREATE TABLE IF NOT EXISTS "admin"."reddit_communities" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "trr_show_id" uuid NOT NULL,
  "trr_show_name" text NOT NULL,
  "subreddit" text NOT NULL,
  "display_name" text,
  "notes" text,
  "is_active" boolean DEFAULT true NOT NULL,
  "created_by_firebase_uid" text NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "post_flairs" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "post_flairs_updated_at" timestamp with time zone,
  "analysis_flairs" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "analysis_all_flairs" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "is_show_focused" boolean DEFAULT false NOT NULL,
  "network_focus_targets" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "franchise_focus_targets" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "episode_title_patterns" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "episode_required_flairs" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "post_flair_categories" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "post_flair_assignments" jsonb DEFAULT '{}'::jsonb NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_communities' AND con.conname='reddit_communities_analysis_all_flairs_is_array') THEN ALTER TABLE "admin"."reddit_communities" ADD CONSTRAINT "reddit_communities_analysis_all_flairs_is_array" CHECK (jsonb_typeof(analysis_all_flairs) = 'array'::text); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_communities' AND con.conname='reddit_communities_analysis_flairs_is_array') THEN ALTER TABLE "admin"."reddit_communities" ADD CONSTRAINT "reddit_communities_analysis_flairs_is_array" CHECK (jsonb_typeof(analysis_flairs) = 'array'::text); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_communities' AND con.conname='reddit_communities_episode_required_flairs_is_array') THEN ALTER TABLE "admin"."reddit_communities" ADD CONSTRAINT "reddit_communities_episode_required_flairs_is_array" CHECK (jsonb_typeof(episode_required_flairs) = 'array'::text); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_communities' AND con.conname='reddit_communities_episode_title_patterns_is_array') THEN ALTER TABLE "admin"."reddit_communities" ADD CONSTRAINT "reddit_communities_episode_title_patterns_is_array" CHECK (jsonb_typeof(episode_title_patterns) = 'array'::text); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_communities' AND con.conname='reddit_communities_franchise_focus_targets_is_array') THEN ALTER TABLE "admin"."reddit_communities" ADD CONSTRAINT "reddit_communities_franchise_focus_targets_is_array" CHECK (jsonb_typeof(franchise_focus_targets) = 'array'::text); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_communities' AND con.conname='reddit_communities_network_focus_targets_is_array') THEN ALTER TABLE "admin"."reddit_communities" ADD CONSTRAINT "reddit_communities_network_focus_targets_is_array" CHECK (jsonb_typeof(network_focus_targets) = 'array'::text); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_communities' AND con.conname='reddit_communities_pkey') THEN ALTER TABLE "admin"."reddit_communities" ADD CONSTRAINT "reddit_communities_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_communities' AND con.conname='reddit_communities_post_flair_assignments_is_object') THEN ALTER TABLE "admin"."reddit_communities" ADD CONSTRAINT "reddit_communities_post_flair_assignments_is_object" CHECK (jsonb_typeof(post_flair_assignments) = 'object'::text); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_communities' AND con.conname='reddit_communities_post_flair_categories_is_object') THEN ALTER TABLE "admin"."reddit_communities" ADD CONSTRAINT "reddit_communities_post_flair_categories_is_object" CHECK (jsonb_typeof(post_flair_categories) = 'object'::text); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_communities' AND con.conname='reddit_communities_post_flairs_is_array') THEN ALTER TABLE "admin"."reddit_communities" ADD CONSTRAINT "reddit_communities_post_flairs_is_array" CHECK (jsonb_typeof(post_flairs) = 'array'::text); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS reddit_communities_pkey ON admin.reddit_communities USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_reddit_communities_unique_show_subreddit ON admin.reddit_communities USING btree (trr_show_id, lower(subreddit));
CREATE INDEX IF NOT EXISTS idx_reddit_communities_show ON admin.reddit_communities USING btree (trr_show_id);
CREATE INDEX IF NOT EXISTS idx_reddit_communities_active ON admin.reddit_communities USING btree (is_active, updated_at DESC);
ALTER TABLE "admin"."reddit_communities" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "admin";
CREATE TABLE IF NOT EXISTS "admin"."reddit_discovery_posts" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "community_id" uuid NOT NULL,
  "subreddit" text NOT NULL,
  "reddit_post_id" text NOT NULL,
  "title" text NOT NULL,
  "text" text,
  "url" text NOT NULL,
  "permalink" text,
  "author" text,
  "score" integer DEFAULT 0 NOT NULL,
  "num_comments" integer DEFAULT 0 NOT NULL,
  "posted_at" timestamp with time zone,
  "link_flair_text" text,
  "source_sorts" text[] DEFAULT '{}'::text[] NOT NULL,
  "matched_terms" text[] DEFAULT '{}'::text[] NOT NULL,
  "matched_cast_terms" text[] DEFAULT '{}'::text[] NOT NULL,
  "cross_show_terms" text[] DEFAULT '{}'::text[] NOT NULL,
  "is_show_match" boolean DEFAULT false NOT NULL,
  "passes_flair_filter" boolean DEFAULT true NOT NULL,
  "match_score" integer DEFAULT 0 NOT NULL,
  "last_discovered_at" timestamp with time zone DEFAULT now() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_discovery_posts' AND con.conname='reddit_discovery_posts_pkey') THEN ALTER TABLE "admin"."reddit_discovery_posts" ADD CONSTRAINT "reddit_discovery_posts_pkey" PRIMARY KEY (id); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS reddit_discovery_posts_pkey ON admin.reddit_discovery_posts USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_reddit_discovery_posts_unique_community_post ON admin.reddit_discovery_posts USING btree (community_id, reddit_post_id);
CREATE INDEX IF NOT EXISTS idx_reddit_discovery_posts_community_posted ON admin.reddit_discovery_posts USING btree (community_id, posted_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_reddit_discovery_posts_community_discovered ON admin.reddit_discovery_posts USING btree (community_id, last_discovered_at DESC);
ALTER TABLE "admin"."reddit_discovery_posts" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "admin";
CREATE TABLE IF NOT EXISTS "admin"."reddit_threads" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "community_id" uuid NOT NULL,
  "trr_show_id" uuid NOT NULL,
  "trr_show_name" text NOT NULL,
  "trr_season_id" uuid,
  "reddit_post_id" text NOT NULL,
  "title" text NOT NULL,
  "url" text NOT NULL,
  "permalink" text,
  "author" text,
  "score" integer DEFAULT 0 NOT NULL,
  "num_comments" integer DEFAULT 0 NOT NULL,
  "posted_at" timestamp with time zone,
  "notes" text,
  "created_by_firebase_uid" text NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "source_kind" text DEFAULT 'manual'::text NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_threads' AND con.conname='reddit_threads_pkey') THEN ALTER TABLE "admin"."reddit_threads" ADD CONSTRAINT "reddit_threads_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_threads' AND con.conname='reddit_threads_source_kind_valid') THEN ALTER TABLE "admin"."reddit_threads" ADD CONSTRAINT "reddit_threads_source_kind_valid" CHECK (source_kind = ANY (ARRAY['manual'::text, 'episode_discussion'::text])); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS reddit_threads_pkey ON admin.reddit_threads USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_reddit_threads_unique_show_post ON admin.reddit_threads USING btree (trr_show_id, reddit_post_id);
CREATE INDEX IF NOT EXISTS idx_reddit_threads_community ON admin.reddit_threads USING btree (community_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_reddit_threads_show ON admin.reddit_threads USING btree (trr_show_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_reddit_threads_season ON admin.reddit_threads USING btree (trr_season_id) WHERE (trr_season_id IS NOT NULL);
ALTER TABLE "admin"."reddit_threads" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "admin";
CREATE TABLE IF NOT EXISTS "admin"."season_cast_survey_roles" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "trr_show_id" uuid NOT NULL,
  "season_number" integer NOT NULL,
  "person_id" uuid NOT NULL,
  "role" text NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='season_cast_survey_roles' AND con.conname='season_cast_survey_roles_pkey') THEN ALTER TABLE "admin"."season_cast_survey_roles" ADD CONSTRAINT "season_cast_survey_roles_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='season_cast_survey_roles' AND con.conname='season_cast_survey_roles_role_check') THEN ALTER TABLE "admin"."season_cast_survey_roles" ADD CONSTRAINT "season_cast_survey_roles_role_check" CHECK (role = ANY (ARRAY['main'::text, 'friend_of'::text])); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='season_cast_survey_roles' AND con.conname='season_cast_survey_roles_season_number_check') THEN ALTER TABLE "admin"."season_cast_survey_roles" ADD CONSTRAINT "season_cast_survey_roles_season_number_check" CHECK (season_number > 0); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='season_cast_survey_roles' AND con.conname='season_cast_survey_roles_trr_show_id_season_number_person_i_key') THEN ALTER TABLE "admin"."season_cast_survey_roles" ADD CONSTRAINT "season_cast_survey_roles_trr_show_id_season_number_person_i_key" UNIQUE (trr_show_id, season_number, person_id); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS season_cast_survey_roles_pkey ON admin.season_cast_survey_roles USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS season_cast_survey_roles_trr_show_id_season_number_person_i_key ON admin.season_cast_survey_roles USING btree (trr_show_id, season_number, person_id);
CREATE INDEX IF NOT EXISTS idx_season_cast_survey_roles_show_season ON admin.season_cast_survey_roles USING btree (trr_show_id, season_number);
CREATE INDEX IF NOT EXISTS idx_season_cast_survey_roles_person ON admin.season_cast_survey_roles USING btree (person_id);
ALTER TABLE "admin"."season_cast_survey_roles" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "admin";
CREATE TABLE IF NOT EXISTS "admin"."show_social_posts" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "trr_show_id" uuid NOT NULL,
  "trr_season_id" uuid,
  "platform" text NOT NULL,
  "url" text NOT NULL,
  "title" text,
  "notes" text,
  "created_by_firebase_uid" text NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='show_social_posts' AND con.conname='show_social_posts_pkey') THEN ALTER TABLE "admin"."show_social_posts" ADD CONSTRAINT "show_social_posts_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='show_social_posts' AND con.conname='show_social_posts_platform_check') THEN ALTER TABLE "admin"."show_social_posts" ADD CONSTRAINT "show_social_posts_platform_check" CHECK (platform = ANY (ARRAY['reddit'::text, 'twitter'::text, 'instagram'::text, 'tiktok'::text, 'youtube'::text, 'other'::text])); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS show_social_posts_pkey ON admin.show_social_posts USING btree (id);
CREATE INDEX IF NOT EXISTS idx_show_social_posts_show ON admin.show_social_posts USING btree (trr_show_id);
CREATE INDEX IF NOT EXISTS idx_show_social_posts_season ON admin.show_social_posts USING btree (trr_season_id);
ALTER TABLE "admin"."show_social_posts" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "firebase_surveys";
CREATE TABLE IF NOT EXISTS "firebase_surveys"."answers" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "response_id" uuid NOT NULL,
  "question_id" uuid NOT NULL,
  "option_id" uuid,
  "text_value" text,
  "numeric_value" numeric,
  "json_value" jsonb,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='answers' AND con.conname='answers_pkey') THEN ALTER TABLE "firebase_surveys"."answers" ADD CONSTRAINT "answers_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='answers' AND con.conname='answers_response_question_unique') THEN ALTER TABLE "firebase_surveys"."answers" ADD CONSTRAINT "answers_response_question_unique" UNIQUE (response_id, question_id); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS answers_pkey ON firebase_surveys.answers USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS answers_response_question_unique ON firebase_surveys.answers USING btree (response_id, question_id);
CREATE INDEX IF NOT EXISTS idx_firebase_answers_response ON firebase_surveys.answers USING btree (response_id);
CREATE INDEX IF NOT EXISTS firebase_surveys_answers_option_id_idx ON firebase_surveys.answers USING btree (option_id);
CREATE INDEX IF NOT EXISTS firebase_surveys_answers_question_id_idx ON firebase_surveys.answers USING btree (question_id);
ALTER TABLE "firebase_surveys"."answers" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "firebase_surveys";
CREATE TABLE IF NOT EXISTS "firebase_surveys"."options" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "question_id" uuid NOT NULL,
  "option_key" text NOT NULL,
  "option_text" text NOT NULL,
  "display_order" integer DEFAULT 0 NOT NULL,
  "metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='options' AND con.conname='options_pkey') THEN ALTER TABLE "firebase_surveys"."options" ADD CONSTRAINT "options_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='options' AND con.conname='options_question_key_unique') THEN ALTER TABLE "firebase_surveys"."options" ADD CONSTRAINT "options_question_key_unique" UNIQUE (question_id, option_key); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS options_pkey ON firebase_surveys.options USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS options_question_key_unique ON firebase_surveys.options USING btree (question_id, option_key);
CREATE INDEX IF NOT EXISTS idx_firebase_options_question_order ON firebase_surveys.options USING btree (question_id, display_order);
CREATE SCHEMA IF NOT EXISTS "firebase_surveys";
CREATE TABLE IF NOT EXISTS "firebase_surveys"."questions" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "survey_id" uuid NOT NULL,
  "question_key" text NOT NULL,
  "question_text" text NOT NULL,
  "question_type" firebase_surveys.question_type NOT NULL,
  "display_order" integer DEFAULT 0 NOT NULL,
  "is_required" boolean DEFAULT false NOT NULL,
  "config" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='questions' AND con.conname='questions_pkey') THEN ALTER TABLE "firebase_surveys"."questions" ADD CONSTRAINT "questions_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='questions' AND con.conname='questions_survey_key_unique') THEN ALTER TABLE "firebase_surveys"."questions" ADD CONSTRAINT "questions_survey_key_unique" UNIQUE (survey_id, question_key); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS questions_pkey ON firebase_surveys.questions USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS questions_survey_key_unique ON firebase_surveys.questions USING btree (survey_id, question_key);
CREATE INDEX IF NOT EXISTS idx_firebase_questions_survey_order ON firebase_surveys.questions USING btree (survey_id, display_order);
CREATE SCHEMA IF NOT EXISTS "firebase_surveys";
CREATE TABLE IF NOT EXISTS "firebase_surveys"."responses" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "survey_run_id" uuid NOT NULL,
  "user_id" text NOT NULL,
  "submission_number" integer DEFAULT 1 NOT NULL,
  "completed_at" timestamp with time zone,
  "metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='responses' AND con.conname='responses_pkey') THEN ALTER TABLE "firebase_surveys"."responses" ADD CONSTRAINT "responses_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='responses' AND con.conname='responses_run_user_submission_unique') THEN ALTER TABLE "firebase_surveys"."responses" ADD CONSTRAINT "responses_run_user_submission_unique" UNIQUE (survey_run_id, user_id, submission_number); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS responses_pkey ON firebase_surveys.responses USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS responses_run_user_submission_unique ON firebase_surveys.responses USING btree (survey_run_id, user_id, submission_number);
CREATE INDEX IF NOT EXISTS idx_firebase_responses_run_user ON firebase_surveys.responses USING btree (survey_run_id, user_id);
CREATE INDEX IF NOT EXISTS idx_firebase_responses_user ON firebase_surveys.responses USING btree (user_id);
ALTER TABLE "firebase_surveys"."responses" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "firebase_surveys";
CREATE TABLE IF NOT EXISTS "firebase_surveys"."survey_runs" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "survey_id" uuid NOT NULL,
  "run_key" text NOT NULL,
  "title" text,
  "starts_at" timestamp with time zone NOT NULL,
  "ends_at" timestamp with time zone,
  "max_submissions_per_user" integer DEFAULT 1 NOT NULL,
  "is_active" boolean DEFAULT true NOT NULL,
  "metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='survey_runs' AND con.conname='survey_runs_pkey') THEN ALTER TABLE "firebase_surveys"."survey_runs" ADD CONSTRAINT "survey_runs_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='survey_runs' AND con.conname='survey_runs_survey_key_unique') THEN ALTER TABLE "firebase_surveys"."survey_runs" ADD CONSTRAINT "survey_runs_survey_key_unique" UNIQUE (survey_id, run_key); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS survey_runs_pkey ON firebase_surveys.survey_runs USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS survey_runs_survey_key_unique ON firebase_surveys.survey_runs USING btree (survey_id, run_key);
CREATE INDEX IF NOT EXISTS idx_firebase_survey_runs_active ON firebase_surveys.survey_runs USING btree (survey_id, is_active, starts_at, ends_at);
CREATE SCHEMA IF NOT EXISTS "firebase_surveys";
CREATE TABLE IF NOT EXISTS "firebase_surveys"."surveys" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "slug" text NOT NULL,
  "title" text NOT NULL,
  "description" text,
  "is_active" boolean DEFAULT true NOT NULL,
  "metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='surveys' AND con.conname='surveys_pkey') THEN ALTER TABLE "firebase_surveys"."surveys" ADD CONSTRAINT "surveys_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='surveys' AND con.conname='surveys_slug_unique') THEN ALTER TABLE "firebase_surveys"."surveys" ADD CONSTRAINT "surveys_slug_unique" UNIQUE (slug); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS surveys_pkey ON firebase_surveys.surveys USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS surveys_slug_unique ON firebase_surveys.surveys USING btree (slug);
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."site_typography_assignments" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "area" text NOT NULL,
  "page_key" text,
  "instance_key" text,
  "set_id" uuid NOT NULL,
  "source_path" text NOT NULL,
  "notes" text,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='site_typography_assignments' AND con.conname='site_typography_assignments_area_check') THEN ALTER TABLE "public"."site_typography_assignments" ADD CONSTRAINT "site_typography_assignments_area_check" CHECK (area = ANY (ARRAY['user-frontend'::text, 'surveys'::text, 'admin'::text])); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='site_typography_assignments' AND con.conname='site_typography_assignments_pkey') THEN ALTER TABLE "public"."site_typography_assignments" ADD CONSTRAINT "site_typography_assignments_pkey" PRIMARY KEY (id); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS site_typography_assignments_pkey ON public.site_typography_assignments USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_site_typography_assignments_scope ON public.site_typography_assignments USING btree (area, COALESCE(page_key, ''::text), COALESCE(instance_key, ''::text));
CREATE INDEX IF NOT EXISTS idx_site_typography_assignments_set_id ON public.site_typography_assignments USING btree (set_id);
ALTER TABLE "public"."site_typography_assignments" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."site_typography_sets" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "slug" text NOT NULL,
  "name" text NOT NULL,
  "area" text NOT NULL,
  "seed_source" text NOT NULL,
  "roles" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='site_typography_sets' AND con.conname='site_typography_sets_area_check') THEN ALTER TABLE "public"."site_typography_sets" ADD CONSTRAINT "site_typography_sets_area_check" CHECK (area = ANY (ARRAY['user-frontend'::text, 'surveys'::text, 'admin'::text])); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='site_typography_sets' AND con.conname='site_typography_sets_pkey') THEN ALTER TABLE "public"."site_typography_sets" ADD CONSTRAINT "site_typography_sets_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='site_typography_sets' AND con.conname='site_typography_sets_slug_key') THEN ALTER TABLE "public"."site_typography_sets" ADD CONSTRAINT "site_typography_sets_slug_key" UNIQUE (slug); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS site_typography_sets_pkey ON public.site_typography_sets USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS site_typography_sets_slug_key ON public.site_typography_sets USING btree (slug);
ALTER TABLE "public"."site_typography_sets" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."survey_cast" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "survey_id" uuid NOT NULL,
  "name" text NOT NULL,
  "slug" text NOT NULL,
  "image_url" text,
  "role" text,
  "status" text,
  "instagram" text,
  "display_order" integer DEFAULT 0 NOT NULL,
  "is_alumni" boolean DEFAULT false NOT NULL,
  "alumni_verdict_enabled" boolean DEFAULT false NOT NULL,
  "metadata" jsonb DEFAULT '{}'::jsonb
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_cast' AND con.conname='survey_cast_pkey') THEN ALTER TABLE "public"."survey_cast" ADD CONSTRAINT "survey_cast_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_cast' AND con.conname='survey_cast_status_check') THEN ALTER TABLE "public"."survey_cast" ADD CONSTRAINT "survey_cast_status_check" CHECK (status = ANY (ARRAY['main'::text, 'friend'::text, 'new'::text, 'alum'::text])); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_cast' AND con.conname='survey_cast_survey_id_slug_key') THEN ALTER TABLE "public"."survey_cast" ADD CONSTRAINT "survey_cast_survey_id_slug_key" UNIQUE (survey_id, slug); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS survey_cast_pkey ON public.survey_cast USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS survey_cast_survey_id_slug_key ON public.survey_cast USING btree (survey_id, slug);
CREATE INDEX IF NOT EXISTS idx_survey_cast_survey_id ON public.survey_cast USING btree (survey_id);
CREATE INDEX IF NOT EXISTS idx_survey_cast_display_order ON public.survey_cast USING btree (survey_id, display_order);
CREATE INDEX IF NOT EXISTS idx_survey_cast_status ON public.survey_cast USING btree (status);
CREATE INDEX IF NOT EXISTS idx_survey_cast_is_alumni ON public.survey_cast USING btree (survey_id, is_alumni) WHERE (is_alumni = true);
ALTER TABLE "public"."survey_cast" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."survey_episodes" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "survey_id" uuid NOT NULL,
  "episode_number" integer NOT NULL,
  "episode_id" text NOT NULL,
  "episode_label" text,
  "air_date" date,
  "opens_at" timestamp with time zone,
  "closes_at" timestamp with time zone,
  "is_active" boolean DEFAULT true NOT NULL,
  "is_current" boolean DEFAULT false NOT NULL,
  "firestore_synced_at" timestamp with time zone
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_episodes' AND con.conname='survey_episodes_pkey') THEN ALTER TABLE "public"."survey_episodes" ADD CONSTRAINT "survey_episodes_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_episodes' AND con.conname='survey_episodes_survey_id_episode_id_key') THEN ALTER TABLE "public"."survey_episodes" ADD CONSTRAINT "survey_episodes_survey_id_episode_id_key" UNIQUE (survey_id, episode_id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_episodes' AND con.conname='survey_episodes_survey_id_episode_number_key') THEN ALTER TABLE "public"."survey_episodes" ADD CONSTRAINT "survey_episodes_survey_id_episode_number_key" UNIQUE (survey_id, episode_number); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS survey_episodes_pkey ON public.survey_episodes USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS survey_episodes_survey_id_episode_number_key ON public.survey_episodes USING btree (survey_id, episode_number);
CREATE UNIQUE INDEX IF NOT EXISTS survey_episodes_survey_id_episode_id_key ON public.survey_episodes USING btree (survey_id, episode_id);
CREATE INDEX IF NOT EXISTS idx_survey_episodes_survey_id ON public.survey_episodes USING btree (survey_id);
CREATE INDEX IF NOT EXISTS idx_survey_episodes_is_current ON public.survey_episodes USING btree (survey_id, is_current) WHERE (is_current = true);
CREATE INDEX IF NOT EXISTS idx_survey_episodes_air_date ON public.survey_episodes USING btree (air_date);
CREATE INDEX IF NOT EXISTS idx_survey_episodes_is_active ON public.survey_episodes USING btree (survey_id, is_active) WHERE (is_active = true);
ALTER TABLE "public"."survey_episodes" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."survey_global_profile_responses" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "respondent_id" text,
  "app_user_id" text NOT NULL,
  "app_user_email" text,
  "source" text DEFAULT 'trr_app'::text NOT NULL,
  "show_id" text,
  "season_number" integer,
  "episode_number" integer,
  "age_bracket" text,
  "birthdate" date,
  "gender" text,
  "pronouns" jsonb,
  "country" text,
  "state_region" text,
  "postal_code" text,
  "household_size" integer,
  "children_in_household" text,
  "relationship_status" text,
  "education_level" text,
  "household_income_band" text,
  "view_hours_week" text,
  "view_devices_reality" jsonb,
  "view_live_tv_household" text,
  "view_platforms_household" jsonb,
  "view_platforms_subscriptions" jsonb,
  "view_reality_cowatch" text,
  "view_live_chats_social" text,
  "view_bravo_platform_primary" text,
  "view_bravo_other_sources" jsonb,
  "view_new_episode_timing" text,
  "view_binge_style" text,
  "psych_bravo_fandom_level" text,
  "psych_other_reality_categories" jsonb,
  "psych_online_engagement" jsonb,
  "psych_purchase_behavior" jsonb,
  "psych_watch_reasons" jsonb,
  "profile_email" text,
  "profile_reuse_ok" text,
  "extra" jsonb,
  "app_username" text
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_global_profile_responses' AND con.conname='survey_global_profile_responses_app_user_id_key') THEN ALTER TABLE "public"."survey_global_profile_responses" ADD CONSTRAINT "survey_global_profile_responses_app_user_id_key" UNIQUE (app_user_id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_global_profile_responses' AND con.conname='survey_global_profile_responses_pkey') THEN ALTER TABLE "public"."survey_global_profile_responses" ADD CONSTRAINT "survey_global_profile_responses_pkey" PRIMARY KEY (id); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS survey_global_profile_responses_pkey ON public.survey_global_profile_responses USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS survey_global_profile_responses_app_user_id_key ON public.survey_global_profile_responses USING btree (app_user_id);
CREATE INDEX IF NOT EXISTS idx_sgpr_created_at ON public.survey_global_profile_responses USING btree (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sgpr_app_username ON public.survey_global_profile_responses USING btree (app_username);
ALTER TABLE "public"."survey_global_profile_responses" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."survey_rhop_s10_responses" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "respondent_id" text,
  "app_user_id" text NOT NULL,
  "app_user_email" text,
  "app_username" text,
  "source" text DEFAULT 'trr_app'::text NOT NULL,
  "show_id" text,
  "season_number" integer,
  "episode_number" integer,
  "season_id" text,
  "episode_id" text,
  "ranking" jsonb,
  "completion_pct" integer,
  "completed" boolean,
  "client_schema_version" integer,
  "client_version" text,
  "extra" jsonb
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_rhop_s10_responses' AND con.conname='survey_rhop_s10_responses_app_user_id_season_id_episode_id_key') THEN ALTER TABLE "public"."survey_rhop_s10_responses" ADD CONSTRAINT "survey_rhop_s10_responses_app_user_id_season_id_episode_id_key" UNIQUE (app_user_id, season_id, episode_id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_rhop_s10_responses' AND con.conname='survey_rhop_s10_responses_pkey') THEN ALTER TABLE "public"."survey_rhop_s10_responses" ADD CONSTRAINT "survey_rhop_s10_responses_pkey" PRIMARY KEY (id); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS survey_rhop_s10_responses_pkey ON public.survey_rhop_s10_responses USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS survey_rhop_s10_responses_app_user_id_season_id_episode_id_key ON public.survey_rhop_s10_responses USING btree (app_user_id, season_id, episode_id);
CREATE INDEX IF NOT EXISTS idx_rhop_s10_app_user_id ON public.survey_rhop_s10_responses USING btree (app_user_id);
CREATE INDEX IF NOT EXISTS idx_rhop_s10_created_at ON public.survey_rhop_s10_responses USING btree (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rhop_s10_season_episode ON public.survey_rhop_s10_responses USING btree (season_id, episode_id);
ALTER TABLE "public"."survey_rhop_s10_responses" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."survey_rhoslc_s6_responses" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "respondent_id" text,
  "app_user_id" text NOT NULL,
  "app_user_email" text,
  "source" text DEFAULT 'trr_app'::text NOT NULL,
  "show_id" text,
  "season_number" integer,
  "season_id" text NOT NULL,
  "episode_number" integer,
  "episode_id" text NOT NULL,
  "ranking" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "completion_pct" integer,
  "completed" boolean,
  "client_schema_version" integer,
  "client_version" text,
  "extra" jsonb,
  "app_username" text,
  "season_rating" numeric(3,1)
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_rhoslc_s6_responses' AND con.conname='survey_rhoslc_s6_responses_app_user_id_season_id_episode_id_key') THEN ALTER TABLE "public"."survey_rhoslc_s6_responses" ADD CONSTRAINT "survey_rhoslc_s6_responses_app_user_id_season_id_episode_id_key" UNIQUE (app_user_id, season_id, episode_id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_rhoslc_s6_responses' AND con.conname='survey_rhoslc_s6_responses_pkey') THEN ALTER TABLE "public"."survey_rhoslc_s6_responses" ADD CONSTRAINT "survey_rhoslc_s6_responses_pkey" PRIMARY KEY (id); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS survey_rhoslc_s6_responses_pkey ON public.survey_rhoslc_s6_responses USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS survey_rhoslc_s6_responses_app_user_id_season_id_episode_id_key ON public.survey_rhoslc_s6_responses USING btree (app_user_id, season_id, episode_id);
CREATE INDEX IF NOT EXISTS idx_rhoslc_s6_created_at ON public.survey_rhoslc_s6_responses USING btree (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rhoslc_s6_show_episode ON public.survey_rhoslc_s6_responses USING btree (show_id, season_number, episode_number);
CREATE INDEX IF NOT EXISTS idx_rhoslc_s6_app_username ON public.survey_rhoslc_s6_responses USING btree (app_username);
ALTER TABLE "public"."survey_rhoslc_s6_responses" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."survey_show_palette_library" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "trr_show_id" uuid NOT NULL,
  "season_number" integer,
  "name" text NOT NULL,
  "colors" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "source_type" text NOT NULL,
  "source_image_url" text,
  "seed" integer NOT NULL,
  "marker_points" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "created_by_uid" text,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_show_palette_library' AND con.conname='survey_show_palette_library_pkey') THEN ALTER TABLE "public"."survey_show_palette_library" ADD CONSTRAINT "survey_show_palette_library_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_show_palette_library' AND con.conname='survey_show_palette_library_season_number_valid') THEN ALTER TABLE "public"."survey_show_palette_library" ADD CONSTRAINT "survey_show_palette_library_season_number_valid" CHECK (season_number IS NULL OR season_number > 0); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_show_palette_library' AND con.conname='survey_show_palette_library_source_type_valid') THEN ALTER TABLE "public"."survey_show_palette_library" ADD CONSTRAINT "survey_show_palette_library_source_type_valid" CHECK (source_type = ANY (ARRAY['upload'::text, 'url'::text, 'media_library'::text])); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS survey_show_palette_library_pkey ON public.survey_show_palette_library USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_survey_show_palette_library_name_scope ON public.survey_show_palette_library USING btree (trr_show_id, COALESCE(season_number, '-1'::integer), lower(name));
CREATE INDEX IF NOT EXISTS idx_survey_show_palette_library_show ON public.survey_show_palette_library USING btree (trr_show_id);
CREATE INDEX IF NOT EXISTS idx_survey_show_palette_library_show_season ON public.survey_show_palette_library USING btree (trr_show_id, season_number);
ALTER TABLE "public"."survey_show_palette_library" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."survey_show_seasons" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "show_id" uuid NOT NULL,
  "season_number" integer NOT NULL,
  "label" text NOT NULL,
  "year" text,
  "description" text,
  "colors" jsonb DEFAULT '{}'::jsonb,
  "show_icon_url" text,
  "wordmark_url" text,
  "hero_url" text,
  "cast_members" jsonb DEFAULT '[]'::jsonb,
  "notes" text[] DEFAULT '{}'::text[],
  "is_active" boolean DEFAULT true NOT NULL,
  "is_current" boolean DEFAULT false NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_show_seasons' AND con.conname='survey_show_seasons_pkey') THEN ALTER TABLE "public"."survey_show_seasons" ADD CONSTRAINT "survey_show_seasons_pkey" PRIMARY KEY (id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_show_seasons' AND con.conname='survey_show_seasons_show_id_season_number_key') THEN ALTER TABLE "public"."survey_show_seasons" ADD CONSTRAINT "survey_show_seasons_show_id_season_number_key" UNIQUE (show_id, season_number); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS survey_show_seasons_pkey ON public.survey_show_seasons USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS survey_show_seasons_show_id_season_number_key ON public.survey_show_seasons USING btree (show_id, season_number);
CREATE INDEX IF NOT EXISTS idx_survey_show_seasons_show_id ON public.survey_show_seasons USING btree (show_id);
CREATE INDEX IF NOT EXISTS idx_survey_show_seasons_is_current ON public.survey_show_seasons USING btree (show_id, is_current) WHERE (is_current = true);
CREATE INDEX IF NOT EXISTS idx_survey_show_seasons_is_active ON public.survey_show_seasons USING btree (show_id, is_active) WHERE (is_active = true);
ALTER TABLE "public"."survey_show_seasons" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."survey_shows" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "key" text NOT NULL,
  "title" text NOT NULL,
  "short_title" text,
  "network" text,
  "status" text,
  "logline" text,
  "palette" jsonb DEFAULT '{}'::jsonb,
  "icon_url" text,
  "wordmark_url" text,
  "hero_url" text,
  "tags" text[] DEFAULT '{}'::text[],
  "is_active" boolean DEFAULT true NOT NULL,
  "trr_show_id" uuid,
  "fonts" jsonb DEFAULT '{}'::jsonb NOT NULL
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_shows' AND con.conname='survey_shows_key_key') THEN ALTER TABLE "public"."survey_shows" ADD CONSTRAINT "survey_shows_key_key" UNIQUE (key); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_shows' AND con.conname='survey_shows_pkey') THEN ALTER TABLE "public"."survey_shows" ADD CONSTRAINT "survey_shows_pkey" PRIMARY KEY (id); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS survey_shows_pkey ON public.survey_shows USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS survey_shows_key_key ON public.survey_shows USING btree (key);
CREATE INDEX IF NOT EXISTS idx_survey_shows_is_active ON public.survey_shows USING btree (is_active) WHERE (is_active = true);
CREATE UNIQUE INDEX IF NOT EXISTS idx_survey_shows_trr_show_id_unique ON public.survey_shows USING btree (trr_show_id) WHERE (trr_show_id IS NOT NULL);
CREATE INDEX IF NOT EXISTS idx_survey_shows_trr_show_id ON public.survey_shows USING btree (trr_show_id);
ALTER TABLE "public"."survey_shows" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."survey_x_responses" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "respondent_id" text,
  "app_user_id" text NOT NULL,
  "app_user_email" text,
  "source" text DEFAULT 'trr_app'::text NOT NULL,
  "show_id" text,
  "season_number" integer,
  "episode_number" integer,
  "view_live_tv_household" text,
  "view_platforms_subscriptions" text[] DEFAULT '{}'::text[] NOT NULL,
  "primary_platform" text,
  "watch_frequency" text,
  "watch_mode" text,
  "view_reality_cowatch" text,
  "view_live_chats_social" text,
  "view_devices_reality" text[] DEFAULT '{}'::text[] NOT NULL,
  "extra" jsonb,
  "app_username" text
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_x_responses' AND con.conname='survey_x_responses_app_user_id_key') THEN ALTER TABLE "public"."survey_x_responses" ADD CONSTRAINT "survey_x_responses_app_user_id_key" UNIQUE (app_user_id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_x_responses' AND con.conname='survey_x_responses_pkey') THEN ALTER TABLE "public"."survey_x_responses" ADD CONSTRAINT "survey_x_responses_pkey" PRIMARY KEY (id); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS survey_x_responses_pkey ON public.survey_x_responses USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS survey_x_responses_app_user_id_key ON public.survey_x_responses USING btree (app_user_id);
CREATE INDEX IF NOT EXISTS idx_survey_x_created_at ON public.survey_x_responses USING btree (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_survey_x_app_username ON public.survey_x_responses USING btree (app_username);
ALTER TABLE "public"."survey_x_responses" ENABLE ROW LEVEL SECURITY;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE IF NOT EXISTS "public"."surveys" (
  "id" uuid DEFAULT gen_random_uuid() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "key" text NOT NULL,
  "title" text NOT NULL,
  "description" text,
  "response_table_name" text NOT NULL,
  "show_id" text,
  "season_number" integer,
  "episode_number" integer,
  "is_active" boolean DEFAULT true NOT NULL,
  "theme" jsonb DEFAULT '{}'::jsonb,
  "air_schedule" jsonb,
  "current_episode_id" uuid,
  "firestore_path" text
);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='surveys' AND con.conname='surveys_key_key') THEN ALTER TABLE "public"."surveys" ADD CONSTRAINT "surveys_key_key" UNIQUE (key); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='surveys' AND con.conname='surveys_pkey') THEN ALTER TABLE "public"."surveys" ADD CONSTRAINT "surveys_pkey" PRIMARY KEY (id); END IF; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS surveys_pkey ON public.surveys USING btree (id);
CREATE UNIQUE INDEX IF NOT EXISTS surveys_key_key ON public.surveys USING btree (key);
CREATE INDEX IF NOT EXISTS idx_surveys_is_active ON public.surveys USING btree (is_active);
CREATE INDEX IF NOT EXISTS idx_surveys_show_season ON public.surveys USING btree (show_id, season_number);
CREATE INDEX IF NOT EXISTS idx_surveys_theme ON public.surveys USING gin (theme);
CREATE INDEX IF NOT EXISTS idx_surveys_air_schedule ON public.surveys USING gin (air_schedule);
CREATE INDEX IF NOT EXISTS public_surveys_current_episode_id_idx ON public.surveys USING btree (current_episode_id) WHERE (current_episode_id IS NOT NULL);
ALTER TABLE "public"."surveys" ENABLE ROW LEVEL SECURITY;

-- Foreign keys are added last so closure tables exist before references.
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='recent_people_views' AND con.conname='recent_people_views_person_id_fkey') THEN ALTER TABLE "admin"."recent_people_views" ADD CONSTRAINT "recent_people_views_person_id_fkey" FOREIGN KEY (person_id) REFERENCES core.people(id) ON DELETE CASCADE; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_discovery_posts' AND con.conname='reddit_discovery_posts_community_id_fkey') THEN ALTER TABLE "admin"."reddit_discovery_posts" ADD CONSTRAINT "reddit_discovery_posts_community_id_fkey" FOREIGN KEY (community_id) REFERENCES admin.reddit_communities(id) ON DELETE CASCADE; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='admin' AND cc.relname='reddit_threads' AND con.conname='reddit_threads_community_id_fkey') THEN ALTER TABLE "admin"."reddit_threads" ADD CONSTRAINT "reddit_threads_community_id_fkey" FOREIGN KEY (community_id) REFERENCES admin.reddit_communities(id) ON DELETE CASCADE; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='answers' AND con.conname='answers_option_id_fkey') THEN ALTER TABLE "firebase_surveys"."answers" ADD CONSTRAINT "answers_option_id_fkey" FOREIGN KEY (option_id) REFERENCES firebase_surveys.options(id); END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='answers' AND con.conname='answers_question_id_fkey') THEN ALTER TABLE "firebase_surveys"."answers" ADD CONSTRAINT "answers_question_id_fkey" FOREIGN KEY (question_id) REFERENCES firebase_surveys.questions(id) ON DELETE RESTRICT; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='answers' AND con.conname='answers_response_id_fkey') THEN ALTER TABLE "firebase_surveys"."answers" ADD CONSTRAINT "answers_response_id_fkey" FOREIGN KEY (response_id) REFERENCES firebase_surveys.responses(id) ON DELETE CASCADE; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='options' AND con.conname='options_question_id_fkey') THEN ALTER TABLE "firebase_surveys"."options" ADD CONSTRAINT "options_question_id_fkey" FOREIGN KEY (question_id) REFERENCES firebase_surveys.questions(id) ON DELETE CASCADE; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='questions' AND con.conname='questions_survey_id_fkey') THEN ALTER TABLE "firebase_surveys"."questions" ADD CONSTRAINT "questions_survey_id_fkey" FOREIGN KEY (survey_id) REFERENCES firebase_surveys.surveys(id) ON DELETE CASCADE; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='responses' AND con.conname='responses_survey_run_id_fkey') THEN ALTER TABLE "firebase_surveys"."responses" ADD CONSTRAINT "responses_survey_run_id_fkey" FOREIGN KEY (survey_run_id) REFERENCES firebase_surveys.survey_runs(id) ON DELETE RESTRICT; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='firebase_surveys' AND cc.relname='survey_runs' AND con.conname='survey_runs_survey_id_fkey') THEN ALTER TABLE "firebase_surveys"."survey_runs" ADD CONSTRAINT "survey_runs_survey_id_fkey" FOREIGN KEY (survey_id) REFERENCES firebase_surveys.surveys(id) ON DELETE CASCADE; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='site_typography_assignments' AND con.conname='site_typography_assignments_set_id_fkey') THEN ALTER TABLE "public"."site_typography_assignments" ADD CONSTRAINT "site_typography_assignments_set_id_fkey" FOREIGN KEY (set_id) REFERENCES site_typography_sets(id) ON DELETE RESTRICT; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_cast' AND con.conname='survey_cast_survey_id_fkey') THEN ALTER TABLE "public"."survey_cast" ADD CONSTRAINT "survey_cast_survey_id_fkey" FOREIGN KEY (survey_id) REFERENCES surveys(id) ON DELETE CASCADE; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_episodes' AND con.conname='survey_episodes_survey_id_fkey') THEN ALTER TABLE "public"."survey_episodes" ADD CONSTRAINT "survey_episodes_survey_id_fkey" FOREIGN KEY (survey_id) REFERENCES surveys(id) ON DELETE CASCADE; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='survey_show_seasons' AND con.conname='survey_show_seasons_show_id_fkey') THEN ALTER TABLE "public"."survey_show_seasons" ADD CONSTRAINT "survey_show_seasons_show_id_fkey" FOREIGN KEY (show_id) REFERENCES survey_shows(id) ON DELETE CASCADE; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_class cc ON cc.oid=con.conrelid JOIN pg_namespace nn ON nn.oid=cc.relnamespace WHERE nn.nspname='public' AND cc.relname='surveys' AND con.conname='fk_surveys_current_episode') THEN ALTER TABLE "public"."surveys" ADD CONSTRAINT "fk_surveys_current_episode" FOREIGN KEY (current_episode_id) REFERENCES survey_episodes(id) ON DELETE SET NULL; END IF; END $$;
COMMIT;

-- PREVIEW-28 replay convergence: exact update helpers, triggers, and responses-only FORCE RLS.
CREATE OR REPLACE FUNCTION public.set_updated_at_timestamp()
RETURNS trigger
LANGUAGE plpgsql
SET search_path TO public, pg_temp
AS $function$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$function$;

ALTER TABLE firebase_surveys.responses ENABLE ROW LEVEL SECURITY;
ALTER TABLE firebase_surveys.responses FORCE ROW LEVEL SECURITY;

DROP TRIGGER IF EXISTS set_recent_people_views_updated_at ON admin.recent_people_views;
CREATE TRIGGER set_recent_people_views_updated_at BEFORE UPDATE ON admin.recent_people_views FOR EACH ROW EXECUTE FUNCTION admin.set_updated_at();
DROP TRIGGER IF EXISTS set_reddit_communities_updated_at ON admin.reddit_communities;
CREATE TRIGGER set_reddit_communities_updated_at BEFORE UPDATE ON admin.reddit_communities FOR EACH ROW EXECUTE FUNCTION admin.set_updated_at();
DROP TRIGGER IF EXISTS set_reddit_discovery_posts_updated_at ON admin.reddit_discovery_posts;
CREATE TRIGGER set_reddit_discovery_posts_updated_at BEFORE UPDATE ON admin.reddit_discovery_posts FOR EACH ROW EXECUTE FUNCTION admin.set_updated_at();
DROP TRIGGER IF EXISTS set_reddit_threads_updated_at ON admin.reddit_threads;
CREATE TRIGGER set_reddit_threads_updated_at BEFORE UPDATE ON admin.reddit_threads FOR EACH ROW EXECUTE FUNCTION admin.set_updated_at();
DROP TRIGGER IF EXISTS set_show_social_posts_updated_at ON admin.show_social_posts;
CREATE TRIGGER set_show_social_posts_updated_at BEFORE UPDATE ON admin.show_social_posts FOR EACH ROW EXECUTE FUNCTION admin.set_updated_at();

DROP TRIGGER IF EXISTS trg_firebase_questions_updated_at ON firebase_surveys.questions;
CREATE TRIGGER trg_firebase_questions_updated_at BEFORE UPDATE ON firebase_surveys.questions FOR EACH ROW EXECUTE FUNCTION firebase_surveys.set_updated_at();
DROP TRIGGER IF EXISTS trg_firebase_responses_updated_at ON firebase_surveys.responses;
CREATE TRIGGER trg_firebase_responses_updated_at BEFORE UPDATE ON firebase_surveys.responses FOR EACH ROW EXECUTE FUNCTION firebase_surveys.set_updated_at();
DROP TRIGGER IF EXISTS trg_firebase_survey_runs_updated_at ON firebase_surveys.survey_runs;
CREATE TRIGGER trg_firebase_survey_runs_updated_at BEFORE UPDATE ON firebase_surveys.survey_runs FOR EACH ROW EXECUTE FUNCTION firebase_surveys.set_updated_at();
DROP TRIGGER IF EXISTS trg_firebase_surveys_updated_at ON firebase_surveys.surveys;
CREATE TRIGGER trg_firebase_surveys_updated_at BEFORE UPDATE ON firebase_surveys.surveys FOR EACH ROW EXECUTE FUNCTION firebase_surveys.set_updated_at();

DROP TRIGGER IF EXISTS trg_survey_cast_updated_at ON public.survey_cast;
CREATE TRIGGER trg_survey_cast_updated_at BEFORE UPDATE ON public.survey_cast FOR EACH ROW EXECUTE FUNCTION public.set_updated_at_timestamp();
DROP TRIGGER IF EXISTS trg_survey_episodes_updated_at ON public.survey_episodes;
CREATE TRIGGER trg_survey_episodes_updated_at BEFORE UPDATE ON public.survey_episodes FOR EACH ROW EXECUTE FUNCTION public.set_updated_at_timestamp();
DROP TRIGGER IF EXISTS trg_sgpr_updated_at ON public.survey_global_profile_responses;
CREATE TRIGGER trg_sgpr_updated_at BEFORE UPDATE ON public.survey_global_profile_responses FOR EACH ROW EXECUTE FUNCTION public.set_updated_at_timestamp();
DROP TRIGGER IF EXISTS trg_rhop_s10_updated_at ON public.survey_rhop_s10_responses;
CREATE TRIGGER trg_rhop_s10_updated_at BEFORE UPDATE ON public.survey_rhop_s10_responses FOR EACH ROW EXECUTE FUNCTION public.set_updated_at_timestamp();
DROP TRIGGER IF EXISTS trg_rhoslc_s6_updated_at ON public.survey_rhoslc_s6_responses;
CREATE TRIGGER trg_rhoslc_s6_updated_at BEFORE UPDATE ON public.survey_rhoslc_s6_responses FOR EACH ROW EXECUTE FUNCTION public.set_updated_at_timestamp();
DROP TRIGGER IF EXISTS trg_survey_show_seasons_updated_at ON public.survey_show_seasons;
CREATE TRIGGER trg_survey_show_seasons_updated_at BEFORE UPDATE ON public.survey_show_seasons FOR EACH ROW EXECUTE FUNCTION public.set_updated_at_timestamp();
DROP TRIGGER IF EXISTS trg_survey_shows_updated_at ON public.survey_shows;
CREATE TRIGGER trg_survey_shows_updated_at BEFORE UPDATE ON public.survey_shows FOR EACH ROW EXECUTE FUNCTION public.set_updated_at_timestamp();
DROP TRIGGER IF EXISTS trg_survey_x_responses_updated_at ON public.survey_x_responses;
CREATE TRIGGER trg_survey_x_responses_updated_at BEFORE UPDATE ON public.survey_x_responses FOR EACH ROW EXECUTE FUNCTION public.set_updated_at_timestamp();
DROP TRIGGER IF EXISTS trg_surveys_updated_at ON public.surveys;
CREATE TRIGGER trg_surveys_updated_at BEFORE UPDATE ON public.surveys FOR EACH ROW EXECUTE FUNCTION public.set_updated_at_timestamp();

begin;

create index if not exists core_season_images_show_id_idx
  on core.season_images(show_id);

create index if not exists core_season_images_show_season_hosted_idx
  on core.season_images(show_id, season_number)
  where hosted_url is not null;

create index if not exists admin_brand_family_wikipedia_show_links_matched_show_id_idx
  on admin.brand_family_wikipedia_show_links(matched_show_id);

create index if not exists core_show_cast_role_assignments_person_id_idx
  on core.show_cast_role_assignments(person_id);

create index if not exists core_show_cast_role_assignments_season_id_idx
  on core.show_cast_role_assignments(season_id)
  where season_id is not null;

create index if not exists core_show_source_latest_source_id_idx
  on core.show_source_latest(source_id);

create index if not exists core_show_source_history_source_id_idx
  on core.show_source_history(source_id);

create index if not exists core_season_source_latest_source_id_idx
  on core.season_source_latest(source_id);

create index if not exists core_season_source_history_source_id_idx
  on core.season_source_history(source_id);

create index if not exists core_episode_source_latest_source_id_idx
  on core.episode_source_latest(source_id);

create index if not exists core_episode_source_history_source_id_idx
  on core.episode_source_history(source_id);

create index if not exists core_person_source_latest_source_id_idx
  on core.person_source_latest(source_id);

create index if not exists core_person_source_history_source_id_idx
  on core.person_source_history(source_id);

create index if not exists core_media_uploads_media_asset_id_idx
  on core.media_uploads(media_asset_id)
  where media_asset_id is not null;

create index if not exists core_media_uploads_media_link_id_idx
  on core.media_uploads(media_link_id)
  where media_link_id is not null;

create index if not exists public_surveys_current_episode_id_idx
  on public.surveys(current_episode_id)
  where current_episode_id is not null;

create index if not exists core_shows_primary_backdrop_image_id_idx
  on core.shows(primary_backdrop_image_id)
  where primary_backdrop_image_id is not null;

create index if not exists core_shows_primary_logo_image_id_idx
  on core.shows(primary_logo_image_id)
  where primary_logo_image_id is not null;

create index if not exists core_shows_primary_poster_image_id_idx
  on core.shows(primary_poster_image_id)
  where primary_poster_image_id is not null;

create index if not exists screenalytics_cast_screentime_reference_fingerprints_episode_id_idx
  on screenalytics.cast_screentime_reference_fingerprints(episode_id);

create index if not exists screenalytics_cast_screentime_reference_fingerprints_run_id_idx
  on screenalytics.cast_screentime_reference_fingerprints(run_id);

create index if not exists screenalytics_cast_screentime_reference_fingerprints_season_id_idx
  on screenalytics.cast_screentime_reference_fingerprints(season_id)
  where season_id is not null;

create index if not exists screenalytics_cast_screentime_suggestion_decisions_episode_id_idx
  on screenalytics.cast_screentime_suggestion_decisions(episode_id)
  where episode_id is not null;

create index if not exists screenalytics_cast_screentime_suggestion_decisions_person_id_idx
  on screenalytics.cast_screentime_suggestion_decisions(person_id);

create index if not exists screenalytics_cast_screentime_suggestion_decisions_season_id_idx
  on screenalytics.cast_screentime_suggestion_decisions(season_id)
  where season_id is not null;

create index if not exists screenalytics_cast_screentime_suggestion_decisions_video_asset_id_idx
  on screenalytics.cast_screentime_suggestion_decisions(video_asset_id);

create index if not exists screenalytics_cast_screentime_unknown_review_state_candidate_person_id_idx
  on screenalytics.cast_screentime_unknown_review_state(candidate_person_id)
  where candidate_person_id is not null;

create index if not exists screenalytics_cast_screentime_unknown_review_state_episode_id_idx
  on screenalytics.cast_screentime_unknown_review_state(episode_id)
  where episode_id is not null;

create index if not exists screenalytics_cast_screentime_unknown_review_state_season_id_idx
  on screenalytics.cast_screentime_unknown_review_state(season_id)
  where season_id is not null;

create index if not exists screenalytics_cast_screentime_unknown_review_state_video_asset_id_idx
  on screenalytics.cast_screentime_unknown_review_state(video_asset_id);

drop index if exists core.media_links_one_primary_uq;
drop index if exists core.show_images_source_unique;

commit;
