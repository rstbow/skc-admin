/* =============================================================================
   Scheduler glue: adds columns to admin.Jobs + creates admin.ErrorRunbooks.

   Run against: skc-admin

   Adds to admin.Jobs:
     - ExecutionMode         how the job is scheduled + run
                              SSIS_NATIVE     = old SSIS package, untouched
                              SSIS_CALLS_NODE = SSIS fires, posts to Node runner
                              NODE_NATIVE     = Node scheduler fires Node runner
     - LastErrorMessage      sticky last error (mirrors JobRuns.ErrorMessage
                              for the most recent failure so the dashboard
                              doesn't need a JOIN on every card)
     - LastErrorFingerprint  SHA of normalized error — groups repeat errors
                              and keys into admin.ErrorRunbooks
     - ConsecutiveFailures   auto-disable threshold (we pause at 5)
     - Name                  human-readable label for the jobs page

   Creates admin.ErrorRunbooks:
     - Look up "what does this error mean + how do I fix it" by matching
       a regex against a job's LastErrorFingerprint or LastErrorMessage.
     - Seeded with a couple of entries for known failure modes.

   Idempotent: safe to re-run.
   ============================================================================= */

SET NOCOUNT ON;
GO

/* ----- Columns on admin.Jobs ----- */

IF COL_LENGTH('admin.Jobs', 'ExecutionMode') IS NULL
BEGIN
    ALTER TABLE admin.Jobs
        ADD ExecutionMode NVARCHAR(20) NOT NULL
            CONSTRAINT DF_Jobs_ExecMode DEFAULT ('NODE_NATIVE');
    PRINT 'Added admin.Jobs.ExecutionMode (default NODE_NATIVE).';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.check_constraints
    WHERE name = 'CK_Jobs_ExecMode'
      AND parent_object_id = OBJECT_ID('admin.Jobs')
)
BEGIN
    ALTER TABLE admin.Jobs
        ADD CONSTRAINT CK_Jobs_ExecMode
            CHECK (ExecutionMode IN ('SSIS_NATIVE','SSIS_CALLS_NODE','NODE_NATIVE'));
    PRINT 'Added CK_Jobs_ExecMode.';
END
GO

IF COL_LENGTH('admin.Jobs', 'LastErrorMessage') IS NULL
BEGIN
    ALTER TABLE admin.Jobs ADD LastErrorMessage NVARCHAR(MAX) NULL;
    PRINT 'Added admin.Jobs.LastErrorMessage.';
END
GO

IF COL_LENGTH('admin.Jobs', 'LastErrorFingerprint') IS NULL
BEGIN
    ALTER TABLE admin.Jobs ADD LastErrorFingerprint NVARCHAR(64) NULL;
    PRINT 'Added admin.Jobs.LastErrorFingerprint.';
END
GO

IF COL_LENGTH('admin.Jobs', 'ConsecutiveFailures') IS NULL
BEGIN
    ALTER TABLE admin.Jobs
        ADD ConsecutiveFailures INT NOT NULL
            CONSTRAINT DF_Jobs_ConsecFail DEFAULT (0);
    PRINT 'Added admin.Jobs.ConsecutiveFailures.';
END
GO

IF COL_LENGTH('admin.Jobs', 'Name') IS NULL
BEGIN
    ALTER TABLE admin.Jobs ADD Name NVARCHAR(100) NULL;
    PRINT 'Added admin.Jobs.Name.';
END
GO

/* Backfill Name where blank — "<Endpoint> · <Brand>" */
UPDATE j
   SET Name = CONCAT(e.Name, ' · ',
                     ISNULL(b.BrandName, CONVERT(NVARCHAR(36), j.BrandUID)))
  FROM admin.Jobs j
  JOIN admin.Endpoints e ON e.EndpointID = j.EndpointID
  LEFT JOIN admin.Brands b ON b.BrandUID  = j.BrandUID
 WHERE j.Name IS NULL OR j.Name = '';
PRINT 'Backfilled admin.Jobs.Name for null rows.';
GO

/* ----- admin.ErrorRunbooks ----- */

IF OBJECT_ID('admin.ErrorRunbooks', 'U') IS NULL
BEGIN
    CREATE TABLE admin.ErrorRunbooks (
        RunbookID           INT IDENTITY(1,1) NOT NULL,
        MatchPattern        NVARCHAR(400)  NOT NULL,   -- SQL LIKE pattern, matched against ErrorMessage
        Title               NVARCHAR(200)  NOT NULL,
        WhatItMeans         NVARCHAR(MAX)  NOT NULL,
        HowToFix            NVARCHAR(MAX)  NOT NULL,
        Severity            NVARCHAR(20)   NOT NULL CONSTRAINT DF_Runbook_Sev DEFAULT ('WARN'),
        IsActive            BIT            NOT NULL CONSTRAINT DF_Runbook_Active DEFAULT (1),
        CreatedAt           DATETIME2(3)   NOT NULL CONSTRAINT DF_Runbook_Created DEFAULT (SYSUTCDATETIME()),
        UpdatedAt           DATETIME2(3)   NOT NULL CONSTRAINT DF_Runbook_Updated DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_ErrorRunbooks PRIMARY KEY CLUSTERED (RunbookID),
        CONSTRAINT CK_Runbook_Sev CHECK (Severity IN ('INFO','WARN','ERROR','CRITICAL'))
    );
    PRINT 'Created admin.ErrorRunbooks.';
END
GO

/* Seed a few known-failure-mode runbooks. MERGE so re-runs don't duplicate. */
MERGE admin.ErrorRunbooks AS tgt
USING (VALUES
    (N'%invalid_grant%',
     N'Amazon LWA refresh token rejected',
     N'Amazon''s OAuth server refused the brand''s refresh token. Either the token was rotated on Seller Central, the seller revoked the app, or the token expired from disuse (Amazon expires unused tokens after ~180 days).',
     N'1) Open Credentials → find the affected brand''s AMAZON_SP_API row → click "Rotate refresh token". 2) Walk the seller through the re-auth consent screen at sellercentral.amazon.com. 3) After the callback saves the new token, click "Run now" on the job to confirm recovery.',
     N'ERROR'),

    (N'%Request is throttled%',
     N'Amazon SP-API rate limit hit',
     N'Amazon returned 429. Our per-seller token bucket is topped out, usually because a backfill ran alongside a scheduled pull.',
     N'Usually safe to ignore — the runner''s built-in backoff will catch up within one cron cycle. If the same job fingerprint shows up 3+ cycles in a row, reduce cron frequency or split the job (narrower date window per run).',
     N'WARN'),

    (N'%invalid scale%',
     N'Tedious TVP encoder decimal bug',
     N'This is the historical "invalid scale" error from the mssql/tedious driver rejecting TVP columns. If you''re seeing it now, something regressed — the runner was switched to JSON+OPENJSON in migration 017 specifically to avoid TVPs.',
     N'Check that the affected runner isn''t still using sql.Table. Grep the runner file for "AmzFinancialEventsTVP" — should be zero hits. If any remain, follow the migration 017 / commit 3b5a4f4 pattern: JSON.stringify rows, pass as sql.NVarChar(sql.MAX), let OPENJSON parse server-side.',
     N'ERROR'),

    (N'%DataDbConnString%',
     N'Brand has no data DB configured',
     N'The runner tried to connect to the brand''s data DB but admin.Brands.DataDbConnString is NULL or empty.',
     N'Open Brands → find the affected brand → set DataDbConnString. The encrypted connection string format is: Server=tcp:...,1433;Database=...;User Id=...;Password=...;Encrypt=True. Hit Save; the next run should succeed.',
     N'ERROR'),

    (N'%ETIMEDOUT%',
     N'Database or upstream API timeout',
     N'A network call exceeded its timeout. Could be Azure SQL during a pool event, or Amazon/Shopify during a regional outage.',
     N'1) Check the Azure portal for DB availability alerts. 2) Check https://sellercentral-status.amazon.com (or the relevant connector status page). 3) If the runner is transient-error-aware, just retry — the next cron cycle will likely succeed. 4) If persistent >30min, escalate to the connector provider.',
     N'WARN'),

    (N'%permission%denied%',
     N'Insufficient SQL permission',
     N'The Node app''s SQL login lacks a grant that a proc or table needs. Most common after adding a new schema or proc without running GRANT EXECUTE.',
     N'1) Identify the object from the error (usually "proc X" or "table Y"). 2) Connect to the target DB as an admin in SSMS. 3) Run: GRANT EXECUTE ON <object> TO skc_app_user; (or the appropriate login). 4) Retry the job.',
     N'ERROR')
) AS src (MatchPattern, Title, WhatItMeans, HowToFix, Severity)
ON tgt.MatchPattern = src.MatchPattern

WHEN MATCHED THEN
    UPDATE SET Title       = src.Title,
               WhatItMeans = src.WhatItMeans,
               HowToFix    = src.HowToFix,
               Severity    = src.Severity,
               UpdatedAt   = SYSUTCDATETIME()

WHEN NOT MATCHED BY TARGET THEN
    INSERT (MatchPattern, Title, WhatItMeans, HowToFix, Severity)
    VALUES (src.MatchPattern, src.Title, src.WhatItMeans, src.HowToFix, src.Severity);

PRINT 'Runbook seeds upserted.';
GO

PRINT '--------------------------------------------------';
PRINT 'Migration 019 complete: scheduler columns + runbooks.';
PRINT '--------------------------------------------------';
GO
