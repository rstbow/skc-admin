/* =============================================================================
   Migration 020 — seed recurring + backfill jobs for Amazon Financial Events.

   Run against: skc-admin

   Does three things:

   1. DDL — adds admin.Jobs.Params NVARCHAR(MAX). Runners that need config
      (daysBack, window size, page size, etc.) read this JSON blob. Without
      it, every runner would hardcode its window, and onboarding would need
      code changes instead of just data.

   2. DML — inserts ONE recurring NODE_NATIVE job per active Amazon SP-API
      credential, firing every 6 hours, pulling daysBack=2. The overlap is
      deliberate — Amazon sometimes posts events retroactively (the hash-
      gated MERGE dedupes). ExecutionMode=NODE_NATIVE means the scheduler
      inside skc-admin-api owns the schedule; SSIS never touches these.

   3. DML — inserts ONE one-shot BACKFILL job per active Amazon SP-API
      credential, no cron (so the scheduler doesn't auto-fire it), pulls
      daysBack=180 when triggered. These sit paused-by-default on the
      Jobs page with a "Run now" button. The human decides when to kick
      each one off during onboarding. After a successful backfill run,
      you'd typically pause the job (IsActive=0) so it can't be fired
      again by accident.

   Idempotent via MERGE on (EndpointID, BrandUID, JobType).
   ============================================================================= */

SET NOCOUNT ON;
GO

/* ----- 1. DDL: admin.Jobs.Params ----- */

IF COL_LENGTH('admin.Jobs', 'Params') IS NULL
BEGIN
    ALTER TABLE admin.Jobs ADD Params NVARCHAR(MAX) NULL;
    PRINT 'Added admin.Jobs.Params (runner config JSON).';
END
ELSE
    PRINT 'admin.Jobs.Params already exists.';
GO

/* ----- 2. DML: recurring + backfill job per active Amazon credential ----- */

DECLARE @EndpointID INT = (
    SELECT e.EndpointID
    FROM admin.Endpoints e
    JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
    WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_FINANCIAL_EVENTS'
);

IF @EndpointID IS NULL
BEGIN
    RAISERROR('AMZ_FINANCIAL_EVENTS endpoint not found — cannot seed jobs. Check migration 008.', 16, 1);
    RETURN;
END

PRINT 'Using EndpointID=' + CAST(@EndpointID AS NVARCHAR(10)) + ' for AMZ_FINANCIAL_EVENTS.';

/* Recurring: every 6h, daysBack=2. */
MERGE admin.Jobs AS tgt
USING (
    SELECT
        bc.BrandUID,
        @EndpointID                                         AS EndpointID,
        CONCAT('AMZ_FINANCIAL_EVENTS · ', b.BrandName)      AS Name,
        '0 */6 * * *'                                       AS CronExpression,
        'America/Chicago'                                   AS TimezoneIANA,
        'NODE_NATIVE'                                       AS ExecutionMode,
        'INGEST'                                            AS JobType,
        CONCAT('amz-fin-events:', CONVERT(NVARCHAR(36), bc.BrandUID)) AS ConcurrencyKey,
        N'{"daysBack":2}'                                   AS Params,
        CAST(1 AS BIT)                                      AS IsActive,
        50                                                  AS Priority
    FROM admin.BrandCredentials bc
    JOIN admin.Connectors  c ON c.ConnectorID = bc.ConnectorID
    JOIN admin.Brands      b ON b.BrandUID    = bc.BrandUID
    WHERE c.Name = 'AMAZON_SP_API'
      AND bc.IsActive = 1
      AND b.IsActive = 1
) AS src
ON tgt.EndpointID = src.EndpointID
   AND tgt.BrandUID  = src.BrandUID
   AND tgt.JobType   = src.JobType
   AND tgt.ExecutionMode = src.ExecutionMode

WHEN MATCHED THEN
    UPDATE SET Name           = src.Name,
               CronExpression = src.CronExpression,
               TimezoneIANA   = src.TimezoneIANA,
               ConcurrencyKey = src.ConcurrencyKey,
               Params         = src.Params,
               UpdatedAt      = SYSUTCDATETIME()

WHEN NOT MATCHED BY TARGET THEN
    INSERT (Name, EndpointID, BrandUID, CronExpression, TimezoneIANA,
            ExecutionMode, JobType, ConcurrencyKey, Params, IsActive, Priority)
    VALUES (src.Name, src.EndpointID, src.BrandUID, src.CronExpression,
            src.TimezoneIANA, src.ExecutionMode, src.JobType,
            src.ConcurrencyKey, src.Params, src.IsActive, src.Priority);

PRINT 'Recurring AMZ_FINANCIAL_EVENTS jobs upserted.';
GO

DECLARE @EndpointID INT = (
    SELECT e.EndpointID
    FROM admin.Endpoints e
    JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
    WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_FINANCIAL_EVENTS'
);

/* Backfill: one-shot, no cron, daysBack=180. IsActive=0 so the scheduler
   ignores it — user hits "Run now" per brand when ready. */
MERGE admin.Jobs AS tgt
USING (
    SELECT
        bc.BrandUID,
        @EndpointID                                         AS EndpointID,
        CONCAT('AMZ_FINANCIAL_EVENTS · ', b.BrandName, ' · backfill 180d') AS Name,
        CAST(NULL AS NVARCHAR(50))                          AS CronExpression,
        'America/Chicago'                                   AS TimezoneIANA,
        'NODE_NATIVE'                                       AS ExecutionMode,
        'BACKFILL'                                          AS JobType,
        CONCAT('amz-fin-events-backfill:', CONVERT(NVARCHAR(36), bc.BrandUID)) AS ConcurrencyKey,
        N'{"daysBack":180}'                                 AS Params,
        CAST(0 AS BIT)                                      AS IsActive,
        30                                                  AS Priority
    FROM admin.BrandCredentials bc
    JOIN admin.Connectors  c ON c.ConnectorID = bc.ConnectorID
    JOIN admin.Brands      b ON b.BrandUID    = bc.BrandUID
    WHERE c.Name = 'AMAZON_SP_API'
      AND bc.IsActive = 1
      AND b.IsActive = 1
) AS src
ON tgt.EndpointID = src.EndpointID
   AND tgt.BrandUID  = src.BrandUID
   AND tgt.JobType   = src.JobType
   AND tgt.ExecutionMode = src.ExecutionMode

WHEN MATCHED THEN
    UPDATE SET Name           = src.Name,
               TimezoneIANA   = src.TimezoneIANA,
               ConcurrencyKey = src.ConcurrencyKey,
               Params         = src.Params,
               UpdatedAt      = SYSUTCDATETIME()

WHEN NOT MATCHED BY TARGET THEN
    INSERT (Name, EndpointID, BrandUID, CronExpression, TimezoneIANA,
            ExecutionMode, JobType, ConcurrencyKey, Params, IsActive, Priority)
    VALUES (src.Name, src.EndpointID, src.BrandUID, src.CronExpression,
            src.TimezoneIANA, src.ExecutionMode, src.JobType,
            src.ConcurrencyKey, src.Params, src.IsActive, src.Priority);

PRINT 'BACKFILL AMZ_FINANCIAL_EVENTS jobs upserted (IsActive=0, no cron — kick off manually).';
GO

/* ----- 3. Report what we just set up ----- */

SELECT
    j.JobID,
    j.Name,
    j.ExecutionMode,
    j.JobType,
    j.CronExpression,
    j.IsActive,
    j.Params,
    b.BrandName,
    e.Name AS EndpointName
FROM admin.Jobs j
JOIN admin.Endpoints  e ON e.EndpointID  = j.EndpointID
LEFT JOIN admin.Brands b ON b.BrandUID    = j.BrandUID
WHERE e.Name = 'AMZ_FINANCIAL_EVENTS'
ORDER BY b.BrandName, j.JobType;

PRINT '--------------------------------------------------';
PRINT 'Migration 020 complete. Recurring + BACKFILL jobs seeded.';
PRINT 'Scheduler reloads jobs on next app boot (or via /api/jobs PATCH).';
PRINT '--------------------------------------------------';
GO
