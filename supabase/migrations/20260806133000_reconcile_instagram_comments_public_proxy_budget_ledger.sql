-- Reconciles the published 20260629140000 predecessor to the accepted
-- production catalog shape. This migration is deliberately a forward
-- correction; it does not assert that either local file ran historically in
-- production.
--
-- It accepts only the exact published-predecessor signature or the exact
-- accepted target signature. Any other table shape is intentionally rejected.

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $reconcile_proxy_budget$
DECLARE
    target_table regclass;
    predecessor_columns boolean;
    canonical_columns boolean;
    predecessor_constraints boolean;
    canonical_constraints boolean;
    predecessor_indexes boolean;
    canonical_indexes boolean;
    predecessor_policy boolean;
    canonical_policy boolean;
    canonical_sequence boolean;
    service_acl_ok boolean;
    rls_ok boolean;
    predecessor_state boolean;
    canonical_state boolean;
BEGIN
    IF pg_catalog.to_regclass('social.instagram_comments_public_proxy_budget_ledger') IS NULL THEN
        RAISE EXCEPTION
            'A5 proxy reconciliation requires social.instagram_comments_public_proxy_budget_ledger to exist';
    END IF;

    IF pg_catalog.to_regrole('service_role') IS NULL THEN
        RAISE EXCEPTION 'A5 proxy reconciliation requires the service_role role';
    END IF;

    target_table := 'social.instagram_comments_public_proxy_budget_ledger'::regclass;

    SELECT
        count(*) = 22
        AND array_agg(
            a.attname || ':' || pg_catalog.format_type(a.atttypid, a.atttypmod)
            || ':' || a.attnotnull::text
            || ':' || a.attidentity::text
            || ':' || coalesce(md5(pg_catalog.pg_get_expr(d.adbin, d.adrelid)), '_')
            ORDER BY a.attnum
        ) = ARRAY[
            'id:bigint:true:a:_',
            'run_id:uuid:false::_',
            'job_id:uuid:false::_',
            'account_handle:text:true::_',
            'comments_load_strategy:text:true::aa43210c56e7465b2d06b95eaa3828ef',
            'proxy_state:text:true::b718f4004245f69875a29f27f6fb8a4e',
            'proxy_provider:text:false::_',
            'proxy_fingerprint:text:false::_',
            'proxy_session_mode:text:false::_',
            'http_client:text:false::_',
            'rate_scope:text:false::_',
            'request_count:integer:true::cfcd208495d565ef66e7dff9f98764da',
            'proxy_bytes_total:bigint:true::cfcd208495d565ef66e7dff9f98764da',
            'proxy_cdn_bytes_leak:bigint:true::cfcd208495d565ef66e7dff9f98764da',
            'proxy_bytes_by_host:jsonb:true::f3db357cf4f4a03883484fa3281a056c',
            'proxy_cdn_bytes_leak_by_host:jsonb:true::f3db357cf4f4a03883484fa3281a056c',
            'usd_per_gb:numeric:false::_',
            'estimated_usd:numeric:false::_',
            'budget_usd:numeric:false::_',
            'budget_exhausted:boolean:true::68934a3e9455fa72420237eb05902327',
            'metadata:jsonb:true::f3db357cf4f4a03883484fa3281a056c',
            'recorded_at:timestamp with time zone:true::75230039beb12ce952f24927f2bfa2f2'
        ]
    INTO predecessor_columns
    FROM pg_catalog.pg_attribute AS a
    LEFT JOIN pg_catalog.pg_attrdef AS d
      ON d.adrelid = a.attrelid
     AND d.adnum = a.attnum
    WHERE a.attrelid = target_table
      AND a.attnum > 0
      AND NOT a.attisdropped;

    SELECT
        count(*) = 22
        AND array_agg(
            a.attname || ':' || pg_catalog.format_type(a.atttypid, a.atttypmod)
            || ':' || a.attnotnull::text
            || ':' || a.attidentity::text
            || ':' || coalesce(md5(pg_catalog.pg_get_expr(d.adbin, d.adrelid)), '_')
            ORDER BY a.attnum
        ) = ARRAY[
            'id:bigint:true::1b16958af49f6a9e4c48e4cffe1d60dd',
            'run_id:uuid:false::_',
            'job_id:uuid:false::_',
            'account_handle:text:true::_',
            'comments_load_strategy:text:true::aa43210c56e7465b2d06b95eaa3828ef',
            'proxy_state:text:true::b718f4004245f69875a29f27f6fb8a4e',
            'proxy_provider:text:false::_',
            'proxy_fingerprint:text:false::_',
            'proxy_session_mode:text:false::_',
            'http_client:text:false::_',
            'rate_scope:text:false::_',
            'request_count:integer:true::cfcd208495d565ef66e7dff9f98764da',
            'proxy_bytes_total:bigint:true::cfcd208495d565ef66e7dff9f98764da',
            'proxy_cdn_bytes_leak:bigint:true::cfcd208495d565ef66e7dff9f98764da',
            'proxy_bytes_by_host:jsonb:true::f3db357cf4f4a03883484fa3281a056c',
            'proxy_cdn_bytes_leak_by_host:jsonb:true::f3db357cf4f4a03883484fa3281a056c',
            'usd_per_gb:numeric(12,6):false::_',
            'estimated_usd:numeric(12,6):false::_',
            'budget_usd:numeric(12,6):false::_',
            'budget_exhausted:boolean:true::68934a3e9455fa72420237eb05902327',
            'metadata:jsonb:true::f3db357cf4f4a03883484fa3281a056c',
            'recorded_at:timestamp with time zone:true::75230039beb12ce952f24927f2bfa2f2'
        ]
    INTO canonical_columns
    FROM pg_catalog.pg_attribute AS a
    LEFT JOIN pg_catalog.pg_attrdef AS d
      ON d.adrelid = a.attrelid
     AND d.adnum = a.attnum
    WHERE a.attrelid = target_table
      AND a.attnum > 0
      AND NOT a.attisdropped;

    SELECT
        count(*) = 1
        AND bool_and(
            conname = 'instagram_comments_public_proxy_budget_ledger_pkey'
            AND contype = 'p'
            AND convalidated
            AND NOT condeferrable
            AND NOT condeferred
            AND conkey = ARRAY[1]::smallint[]
            AND md5(pg_catalog.pg_get_constraintdef(oid, true)) = '4c6419b3704337bbfe50f018842a9ad3'
        )
    INTO predecessor_constraints
    FROM pg_catalog.pg_constraint
    WHERE conrelid = target_table;

    SELECT
        count(*) = 6
        AND count(*) FILTER (
            WHERE conname = 'instagram_comments_public_proxy_budg_proxy_cdn_bytes_leak_check'
              AND contype = 'c'
              AND convalidated
              AND md5(pg_catalog.pg_get_constraintdef(oid, true)) = 'c917f47911276ef4411927856f5bf063'
        ) = 1
        AND count(*) FILTER (
            WHERE conname = 'instagram_comments_public_proxy_budget__proxy_bytes_total_check'
              AND contype = 'c'
              AND convalidated
              AND md5(pg_catalog.pg_get_constraintdef(oid, true)) = '97f3b0196b5d9cfd1abe70b6c4dbe718'
        ) = 1
        AND count(*) FILTER (
            WHERE conname = 'instagram_comments_public_proxy_budget_ledg_request_count_check'
              AND contype = 'c'
              AND convalidated
              AND md5(pg_catalog.pg_get_constraintdef(oid, true)) = '0b444ed30e80a54a84b96e7aec6a36e7'
        ) = 1
        AND count(*) FILTER (
            WHERE conname = 'instagram_comments_public_proxy_budget_ledger_job_id_fkey'
              AND contype = 'f'
              AND convalidated
              AND md5(pg_catalog.pg_get_constraintdef(oid, true)) = 'a922a8d3230739199d00efcd0f42bff4'
        ) = 1
        AND count(*) FILTER (
            WHERE conname = 'instagram_comments_public_proxy_budget_ledger_pkey'
              AND contype = 'p'
              AND convalidated
              AND md5(pg_catalog.pg_get_constraintdef(oid, true)) = '4c6419b3704337bbfe50f018842a9ad3'
        ) = 1
        AND count(*) FILTER (
            WHERE conname = 'instagram_comments_public_proxy_budget_ledger_run_id_fkey'
              AND contype = 'f'
              AND convalidated
              AND md5(pg_catalog.pg_get_constraintdef(oid, true)) = '2fc6e7cc19bc2ebf42352ad2ce6a11d7'
        ) = 1
    INTO canonical_constraints
    FROM pg_catalog.pg_constraint
    WHERE conrelid = target_table;

    SELECT
        count(*) = 4
        AND count(*) FILTER (
            WHERE c.relname = 'instagram_comments_public_proxy_budget_ledger_pkey'
            AND i.indisprimary
            AND i.indisunique
            AND i.indisvalid
            AND i.indisready
            AND md5(pg_catalog.pg_get_indexdef(i.indexrelid)) = '3d157544840ec584feb47cdb1da1bbff'
        ) = 1
        AND count(*) FILTER (
            WHERE c.relname = 'ig_comments_proxy_budget_ledger_run_idx'
            AND NOT i.indisprimary
            AND NOT i.indisunique
            AND i.indisvalid
            AND i.indisready
            AND i.indkey::text = '2'
            AND i.indpred IS NULL
            AND i.indexprs IS NULL
            AND md5(pg_catalog.pg_get_indexdef(i.indexrelid)) = '06d54d9c65408de1db63f3d083dbe889'
        ) = 1
        AND count(*) FILTER (
            WHERE c.relname = 'ig_comments_proxy_budget_ledger_recorded_idx'
              AND NOT i.indisprimary
              AND NOT i.indisunique
              AND i.indisvalid
            AND i.indisready
            AND i.indkey::text = '22'
            AND i.indpred IS NULL
            AND i.indexprs IS NULL
            AND md5(pg_catalog.pg_get_indexdef(i.indexrelid)) = '8b4121bcc00298b71942747bc4572031'
        ) = 1
        AND count(*) FILTER (
            WHERE c.relname = 'ig_comments_proxy_budget_ledger_account_recorded_idx'
              AND NOT i.indisprimary
              AND NOT i.indisunique
              AND i.indisvalid
            AND i.indisready
            AND i.indkey::text = '4 22'
            AND i.indpred IS NULL
            AND i.indexprs IS NULL
            AND md5(pg_catalog.pg_get_indexdef(i.indexrelid)) = '01fda201fef66a33601f31a80d1b1199'
        ) = 1
    INTO predecessor_indexes
    FROM pg_catalog.pg_index AS i
    JOIN pg_catalog.pg_class AS c
      ON c.oid = i.indexrelid
    WHERE i.indrelid = target_table;

    SELECT
        count(*) = 4
        AND count(*) FILTER (
            WHERE c.relname = 'idx_ig_comments_public_proxy_budget_account_recorded'
              AND NOT i.indisprimary
              AND NOT i.indisunique
              AND i.indisvalid
              AND i.indisready
              AND md5(pg_catalog.pg_get_indexdef(i.indexrelid)) = 'd99439b903b4a60ffb52c058160fda14'
        ) = 1
        AND count(*) FILTER (
            WHERE c.relname = 'idx_ig_comments_public_proxy_budget_job'
              AND NOT i.indisprimary
              AND NOT i.indisunique
              AND i.indisvalid
              AND i.indisready
              AND md5(pg_catalog.pg_get_indexdef(i.indexrelid)) = 'c9e56ed5fd9f1b0bed938df7a53cb8a5'
        ) = 1
        AND count(*) FILTER (
            WHERE c.relname = 'idx_ig_comments_public_proxy_budget_run_recorded'
              AND NOT i.indisprimary
              AND NOT i.indisunique
              AND i.indisvalid
              AND i.indisready
              AND md5(pg_catalog.pg_get_indexdef(i.indexrelid)) = '9e38d18784198023baa303b2c02f5c87'
        ) = 1
        AND count(*) FILTER (
            WHERE c.relname = 'instagram_comments_public_proxy_budget_ledger_pkey'
              AND md5(pg_catalog.pg_get_indexdef(i.indexrelid)) = '3d157544840ec584feb47cdb1da1bbff'
        ) = 1
    INTO canonical_indexes
    FROM pg_catalog.pg_index AS i
    JOIN pg_catalog.pg_class AS c
      ON c.oid = i.indexrelid
    WHERE i.indrelid = target_table;

    SELECT
        count(*) = 1
        AND bool_and(
            polname = 'ig_comments_proxy_budget_ledger_service_role_all'
            AND polpermissive
            AND polcmd = '*'
            AND polroles = ARRAY[pg_catalog.to_regrole('service_role')::oid]
            AND pg_catalog.pg_get_expr(polqual, polrelid) = 'true'
            AND pg_catalog.pg_get_expr(polwithcheck, polrelid) = 'true'
        )
    INTO predecessor_policy
    FROM pg_catalog.pg_policy
    WHERE polrelid = target_table;

    SELECT
        count(*) = 1
        AND bool_and(
            polname = 'instagram_comments_public_proxy_budget_ledger_service_role_all'
            AND polpermissive
            AND polcmd = '*'
            AND polroles = ARRAY[pg_catalog.to_regrole('service_role')::oid]
            AND pg_catalog.pg_get_expr(polqual, polrelid) = 'true'
            AND pg_catalog.pg_get_expr(polwithcheck, polrelid) = 'true'
        )
    INTO canonical_policy
    FROM pg_catalog.pg_policy
    WHERE polrelid = target_table;

    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class AS sequence_relation
        JOIN pg_catalog.pg_namespace AS sequence_namespace
          ON sequence_namespace.oid = sequence_relation.relnamespace
        JOIN pg_catalog.pg_depend AS dependency
          ON dependency.classid = 'pg_class'::regclass
         AND dependency.objid = sequence_relation.oid
         AND dependency.refclassid = 'pg_class'::regclass
         AND dependency.refobjid = target_table
         AND dependency.refobjsubid = 1
         AND dependency.deptype = 'a'
        WHERE sequence_namespace.nspname = 'social'
          AND sequence_relation.relname = 'instagram_comments_public_proxy_budget_ledger_id_seq'
          AND sequence_relation.relkind = 'S'
    )
    INTO canonical_sequence;

    SELECT relrowsecurity AND NOT relforcerowsecurity
    INTO rls_ok
    FROM pg_catalog.pg_class
    WHERE oid = target_table;

    SELECT
        count(*) = 8
        AND NOT bool_or(acl.is_grantable)
        AND array_agg(acl.privilege_type ORDER BY acl.privilege_type) = ARRAY[
            'DELETE', 'INSERT', 'MAINTAIN', 'REFERENCES',
            'SELECT', 'TRIGGER', 'TRUNCATE', 'UPDATE'
        ]
    INTO service_acl_ok
    FROM pg_catalog.pg_class AS c
    CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) AS acl
    WHERE c.oid = target_table
      AND acl.grantee = pg_catalog.to_regrole('service_role')::oid;

    predecessor_state := predecessor_columns
        AND predecessor_constraints
        AND predecessor_indexes
        AND predecessor_policy
        AND NOT canonical_sequence
        AND rls_ok
        AND service_acl_ok;
    canonical_state := canonical_columns
        AND canonical_constraints
        AND canonical_indexes
        AND canonical_policy
        AND canonical_sequence
        AND rls_ok
        AND service_acl_ok;

    IF canonical_state THEN
        RETURN;
    END IF;

    IF NOT predecessor_state THEN
        RAISE EXCEPTION
            'A5 proxy reconciliation refuses an unrecognized hybrid schema for %',
            target_table;
    END IF;

    IF pg_catalog.to_regclass('social.scrape_jobs') IS NULL
       OR pg_catalog.to_regclass('social.scrape_runs') IS NULL THEN
        RAISE EXCEPTION
            'A5 proxy reconciliation requires social.scrape_jobs and social.scrape_runs';
    END IF;

    EXECUTE 'LOCK TABLE social.instagram_comments_public_proxy_budget_ledger IN SHARE ROW EXCLUSIVE MODE';

    IF EXISTS (
        SELECT 1
        FROM social.instagram_comments_public_proxy_budget_ledger
        WHERE id < 1
           OR (usd_per_gb IS NOT NULL AND (usd_per_gb = 'NaN'::numeric OR abs(usd_per_gb) >= 1000000 OR usd_per_gb <> trunc(usd_per_gb, 6)))
           OR (estimated_usd IS NOT NULL AND (estimated_usd = 'NaN'::numeric OR abs(estimated_usd) >= 1000000 OR estimated_usd <> trunc(estimated_usd, 6)))
           OR (budget_usd IS NOT NULL AND (budget_usd = 'NaN'::numeric OR abs(budget_usd) >= 1000000 OR budget_usd <> trunc(budget_usd, 6)))
           OR proxy_cdn_bytes_leak < 0
           OR proxy_bytes_total < 0
           OR request_count < 0
    ) THEN
        RAISE EXCEPTION
            'A5 proxy reconciliation found rows that cannot be converted without changing data';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM social.instagram_comments_public_proxy_budget_ledger AS ledger
        WHERE ledger.job_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM social.scrape_jobs AS job WHERE job.id = ledger.job_id)
    ) OR EXISTS (
        SELECT 1
        FROM social.instagram_comments_public_proxy_budget_ledger AS ledger
        WHERE ledger.run_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM social.scrape_runs AS run WHERE run.id = ledger.run_id)
    ) THEN
        RAISE EXCEPTION
            'A5 proxy reconciliation found rows without required foreign-key targets';
    END IF;

    EXECUTE 'ALTER TABLE social.instagram_comments_public_proxy_budget_ledger ALTER COLUMN id DROP IDENTITY';
    EXECUTE 'CREATE SEQUENCE social.instagram_comments_public_proxy_budget_ledger_id_seq';
    EXECUTE 'ALTER SEQUENCE social.instagram_comments_public_proxy_budget_ledger_id_seq OWNED BY social.instagram_comments_public_proxy_budget_ledger.id';
    EXECUTE 'ALTER TABLE social.instagram_comments_public_proxy_budget_ledger ALTER COLUMN id SET DEFAULT nextval(''social.instagram_comments_public_proxy_budget_ledger_id_seq''::regclass)';
    EXECUTE 'SELECT setval(''social.instagram_comments_public_proxy_budget_ledger_id_seq''::regclass, GREATEST(COALESCE(max(id), 1), 1), count(*) > 0) FROM social.instagram_comments_public_proxy_budget_ledger';

    EXECUTE 'ALTER TABLE social.instagram_comments_public_proxy_budget_ledger ALTER COLUMN usd_per_gb TYPE numeric(12,6) USING usd_per_gb::numeric(12,6)';
    EXECUTE 'ALTER TABLE social.instagram_comments_public_proxy_budget_ledger ALTER COLUMN estimated_usd TYPE numeric(12,6) USING estimated_usd::numeric(12,6)';
    EXECUTE 'ALTER TABLE social.instagram_comments_public_proxy_budget_ledger ALTER COLUMN budget_usd TYPE numeric(12,6) USING budget_usd::numeric(12,6)';

    EXECUTE 'ALTER TABLE social.instagram_comments_public_proxy_budget_ledger ADD CONSTRAINT instagram_comments_public_proxy_budg_proxy_cdn_bytes_leak_check CHECK (proxy_cdn_bytes_leak >= 0)';
    EXECUTE 'ALTER TABLE social.instagram_comments_public_proxy_budget_ledger ADD CONSTRAINT instagram_comments_public_proxy_budget__proxy_bytes_total_check CHECK (proxy_bytes_total >= 0)';
    EXECUTE 'ALTER TABLE social.instagram_comments_public_proxy_budget_ledger ADD CONSTRAINT instagram_comments_public_proxy_budget_ledg_request_count_check CHECK (request_count >= 0)';
    EXECUTE 'ALTER TABLE social.instagram_comments_public_proxy_budget_ledger ADD CONSTRAINT instagram_comments_public_proxy_budget_ledger_job_id_fkey FOREIGN KEY (job_id) REFERENCES social.scrape_jobs(id) ON DELETE SET NULL';
    EXECUTE 'ALTER TABLE social.instagram_comments_public_proxy_budget_ledger ADD CONSTRAINT instagram_comments_public_proxy_budget_ledger_run_id_fkey FOREIGN KEY (run_id) REFERENCES social.scrape_runs(id) ON DELETE SET NULL';

    EXECUTE 'DROP INDEX social.ig_comments_proxy_budget_ledger_run_idx';
    EXECUTE 'DROP INDEX social.ig_comments_proxy_budget_ledger_recorded_idx';
    EXECUTE 'DROP INDEX social.ig_comments_proxy_budget_ledger_account_recorded_idx';
    EXECUTE 'CREATE INDEX idx_ig_comments_public_proxy_budget_account_recorded ON social.instagram_comments_public_proxy_budget_ledger (account_handle, recorded_at DESC)';
    EXECUTE 'CREATE INDEX idx_ig_comments_public_proxy_budget_job ON social.instagram_comments_public_proxy_budget_ledger (job_id) WHERE job_id IS NOT NULL';
    EXECUTE 'CREATE INDEX idx_ig_comments_public_proxy_budget_run_recorded ON social.instagram_comments_public_proxy_budget_ledger (run_id, recorded_at DESC) WHERE run_id IS NOT NULL';

    EXECUTE 'DROP POLICY ig_comments_proxy_budget_ledger_service_role_all ON social.instagram_comments_public_proxy_budget_ledger';
    EXECUTE 'CREATE POLICY instagram_comments_public_proxy_budget_ledger_service_role_all ON social.instagram_comments_public_proxy_budget_ledger FOR ALL TO service_role USING (true) WITH CHECK (true)';
END;
$reconcile_proxy_budget$;

COMMIT;
