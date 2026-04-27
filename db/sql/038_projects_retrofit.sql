/* ============================================================================
   File:    2026-04-27-01-038-projects-retrofit-migration.sql.txt
   Purpose: Backfill 7 of 12 existing Amazon SP-API INGEST Jobs into the
            "Amazon Daily" Project (created by 037).
   Author:  admin (Nator), reviewed and amended by sql-specialist (Chip)
   Reviewed: 2026-04-27 against snapshot 2026-04-26 17:12 UTC + post-037 state.
   Database: skc-admin
   Idempotent: safe to re-run.

   USAGE:
     PASS 1 (dry-run preview):
       1. Open this file, copy ALL of it, paste into SSMS connected to skc-admin.
       2. F5. Should finish in under 1 second.
       3. Inspect output. Expected:
            Candidate jobs: 12
            (cluster summary table, 3 rows)
            (cron divergence warning + 3-row table)
            Backfill plan: 7 will be backfilled, 5 skipped
            (skipped jobs table, 5 rows)
            [DRY-RUN] Would insert 3 ProjectEndpoint rows
            [DRY-RUN] Would insert up to 4 ProjectBrand rows
            [DRY-RUN] Would update 7 admin.Jobs rows
            === summary === No changes written. Re-run with @DryRun = 0 to apply.

     PASS 2 (apply):
       4. Find the line below: DECLARE @DryRun BIT = 1;
       5. Change the 1 to a 0.
       6. F5 again.
       7. Expected output:
            (same discovery + cluster summary as Pass 1)
            Backfill plan: 7 will be backfilled, 5 skipped
            ProjectEndpoints inserted: 3
            ProjectBrands inserted:    4 (out of 4 distinct in candidates)
            Jobs backfilled: 7
            === summary === Retrofit applied. Verify with the post-run queries below.

     VERIFY: run 2026-04-27-01-038-projects-retrofit-verify.sql.txt next.

   CHANGES FROM NATOR'S DRAFT (Chip review, 2026-04-27):
     - Dry-run preview now correctly counts predicted backfills + skipped.
       (Nator's draft computed via @ToBackfill which depends on
       admin.ProjectEndpoints — only populated in non-dry-run, so dry-run
       always reported 0 backfill / all skipped — misleading.)
     - NULL-cron tiebreak: NULL sorts last instead of first.
       (Moot for current data; defensive for future scope expansion.)
     - Soft PRINT warning if any non-Amazon INGEST jobs exist.
       (Currently 0; future-proofing per Nator's Q6.)

   None of these changes alter scope or end-state. Same 3 PE rows, 4 PB rows,
   7 Jobs backfilled.
============================================================================ */

USE [skc-admin];
GO

SET NOCOUNT ON;
GO

DECLARE @DryRun BIT = 1;          -- *** flip to 0 to actually write ***
DECLARE @ProjectName NVARCHAR(100) = 'Amazon Daily';
DECLARE @ConnectorName NVARCHAR(50) = 'AMAZON_SP_API';

DECLARE @ProjectID INT;
SELECT @ProjectID = ProjectID FROM admin.Projects WHERE Name = @ProjectName;
IF @ProjectID IS NULL
BEGIN
    RAISERROR('Project "%s" not found. Run 037_projects.sql first.', 16, 1, @ProjectName);
    RETURN;
END

PRINT '=== 038_projects_retrofit ===';
PRINT 'DryRun:       ' + CASE WHEN @DryRun = 1 THEN 'YES (no writes)' ELSE 'NO (applying)' END;
PRINT 'Target:       ' + @ProjectName + ' (ProjectID=' + CAST(@ProjectID AS VARCHAR(10)) + ')';
PRINT 'Connector:    ' + @ConnectorName;
PRINT '';

/* ---------- Discovery: candidate jobs + cluster cron picks ---------- */

DECLARE @Candidates TABLE (
    JobID                  INT NOT NULL PRIMARY KEY,
    EndpointID             INT NOT NULL,
    EndpointName           NVARCHAR(100) NULL,
    JobType                NVARCHAR(20) NOT NULL,
    BrandUID               UNIQUEIDENTIFIER NOT NULL,
    JobCron                NVARCHAR(50) NULL,
    JobTimezone            NVARCHAR(50) NULL,
    JobIsActive            BIT NOT NULL,
    JobPriority            INT NOT NULL
);

INSERT INTO @Candidates (JobID, EndpointID, EndpointName, JobType, BrandUID, JobCron, JobTimezone, JobIsActive, JobPriority)
SELECT j.JobID, j.EndpointID, e.Name, j.JobType, j.BrandUID,
       j.CronExpression, j.TimezoneIANA, j.IsActive, j.Priority
FROM admin.Jobs j
JOIN admin.Endpoints  e ON e.EndpointID  = j.EndpointID
JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
WHERE c.Name = @ConnectorName
  AND j.JobType = 'INGEST'
  AND j.ManagedByProjectID IS NULL;

DECLARE @CandidateCount INT = (SELECT COUNT(*) FROM @Candidates);
PRINT 'Candidate jobs (Amazon SP-API, INGEST, unmanaged): ' + CAST(@CandidateCount AS VARCHAR(10));

/* CHANGED FROM DRAFT: soft warning for any non-Amazon INGEST jobs (per Q6) */
DECLARE @NonAmazonCount INT = (
    SELECT COUNT(*)
    FROM admin.Jobs j
    JOIN admin.Endpoints  e ON e.EndpointID  = j.EndpointID
    JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
    WHERE c.Name <> @ConnectorName
      AND j.JobType = 'INGEST'
      AND j.ManagedByProjectID IS NULL
);
IF @NonAmazonCount > 0
    PRINT 'NOTE: ' + CAST(@NonAmazonCount AS VARCHAR(10)) + ' non-Amazon INGEST jobs exist (unmanaged). 038 does not touch them.';

IF @CandidateCount = 0
BEGIN
    PRINT '(nothing to retrofit. Already done, or no matching jobs.)';
    RETURN;
END

/* Cluster cron picking (mode + tiebreak)
   CHANGED FROM DRAFT: NULL JobCron now sorts LAST in the lex tiebreak. */
DECLARE @ClusterCrons TABLE (
    EndpointID    INT NOT NULL,
    JobType       NVARCHAR(20) NOT NULL,
    EndpointName  NVARCHAR(100) NULL,
    ChosenCron    NVARCHAR(50) NULL,
    ChosenTZ      NVARCHAR(50) NULL,
    JobCount      INT NOT NULL,
    DistinctCrons INT NOT NULL,
    PRIMARY KEY (EndpointID, JobType)
);

;WITH CronCounts AS (
    SELECT EndpointID, JobType, JobCron, JobTimezone,
           COUNT(*)                          AS Cnt,
           ROW_NUMBER() OVER (
               PARTITION BY EndpointID, JobType
               ORDER BY COUNT(*) DESC,
                        CASE WHEN MIN(JobCron) IS NULL THEN 1 ELSE 0 END,
                        MIN(JobCron) ASC,
                        MIN(JobTimezone) ASC
           )                                 AS Rnk
    FROM @Candidates
    GROUP BY EndpointID, JobType, JobCron, JobTimezone
), ClusterStats AS (
    SELECT EndpointID, JobType,
           COUNT(*)                          AS JobCount,
           COUNT(DISTINCT ISNULL(JobCron,'__NULL__')) AS DistinctCrons
    FROM @Candidates
    GROUP BY EndpointID, JobType
)
INSERT INTO @ClusterCrons (EndpointID, JobType, EndpointName, ChosenCron, ChosenTZ, JobCount, DistinctCrons)
SELECT cc.EndpointID, cc.JobType,
       (SELECT TOP 1 EndpointName FROM @Candidates WHERE EndpointID = cc.EndpointID),
       cc.JobCron, cc.JobTimezone,
       cs.JobCount, cs.DistinctCrons
FROM CronCounts cc
JOIN ClusterStats cs ON cs.EndpointID = cc.EndpointID AND cs.JobType = cc.JobType
WHERE cc.Rnk = 1;

PRINT '';
PRINT '--- Cluster summary (one row per Endpoint x JobType) ---';
SELECT EndpointName, JobType, JobCount,
       DistinctCrons,
       ChosenCron, ChosenTZ
FROM @ClusterCrons
ORDER BY EndpointName, JobType;

IF EXISTS (SELECT 1 FROM @ClusterCrons WHERE DistinctCrons > 1)
BEGIN
    PRINT '';
    PRINT '!! Cron divergence in some clusters. Jobs whose cron differs from the chosen cron stay unmanaged. !!';
    SELECT EndpointName, JobType, ChosenCron, DistinctCrons
    FROM @ClusterCrons WHERE DistinctCrons > 1
    ORDER BY EndpointName;
END

/* ---------- Step 1: ProjectEndpoint inserts ---------- */
IF @DryRun = 0
BEGIN
    INSERT INTO admin.ProjectEndpoints
        (ProjectID, EndpointID, JobType, CronExpression, TimezoneIANA, Priority, IsActive, SortOrder)
    SELECT @ProjectID, cc.EndpointID, cc.JobType,
           cc.ChosenCron,
           ISNULL(cc.ChosenTZ, 'America/Chicago'),
           50, 1, 100
    FROM @ClusterCrons cc
    WHERE NOT EXISTS (
        SELECT 1 FROM admin.ProjectEndpoints pe
        WHERE pe.ProjectID = @ProjectID
          AND pe.EndpointID = cc.EndpointID
          AND pe.JobType    = cc.JobType
    );

    PRINT '';
    PRINT 'ProjectEndpoints inserted: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
END
ELSE
BEGIN
    /* PRINT cannot inline a subquery (Msg 1046). Pre-compute into a variable. */
    DECLARE @PEPlannedCount INT = (SELECT COUNT(*) FROM @ClusterCrons);
    PRINT '';
    PRINT '[DRY-RUN] Would insert ' + CAST(@PEPlannedCount AS VARCHAR(10)) + ' ProjectEndpoint rows.';
END

/* ---------- Step 2: ProjectBrand inserts ---------- */
DECLARE @DistinctBrandCount INT = (SELECT COUNT(DISTINCT BrandUID) FROM @Candidates);

IF @DryRun = 0
BEGIN
    INSERT INTO admin.ProjectBrands (ProjectID, BrandUID, IsActive)
    SELECT DISTINCT @ProjectID, c.BrandUID, 1
    FROM @Candidates c
    WHERE NOT EXISTS (
        SELECT 1 FROM admin.ProjectBrands pb
        WHERE pb.ProjectID = @ProjectID AND pb.BrandUID = c.BrandUID
    );

    PRINT 'ProjectBrands inserted:    ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' (out of ' + CAST(@DistinctBrandCount AS VARCHAR(10)) + ' distinct in candidates)';
END
ELSE
BEGIN
    PRINT '[DRY-RUN] Would insert up to ' + CAST(@DistinctBrandCount AS VARCHAR(10)) + ' ProjectBrand rows (distinct brands across candidates).';
END

/* ---------- Step 3: backfill admin.Jobs.ManagedByProject* ----------
   CHANGED FROM DRAFT: split into "predicted" (works in dry-run) and "applied"
   (only in non-dry-run, joins to real ProjectEndpoints). Both use identical
   cron-match logic so they stay in sync. */

DECLARE @PredictedMatching TABLE (JobID INT PRIMARY KEY);
INSERT INTO @PredictedMatching (JobID)
SELECT c.JobID
FROM @Candidates c
JOIN @ClusterCrons cc
  ON cc.EndpointID = c.EndpointID AND cc.JobType = c.JobType
 AND ISNULL(c.JobCron,'') = ISNULL(cc.ChosenCron,'');

DECLARE @BackfillCount INT = (SELECT COUNT(*) FROM @PredictedMatching);
DECLARE @SkippedCount  INT = (SELECT COUNT(*) FROM @Candidates) - @BackfillCount;

PRINT '';
PRINT '--- Backfill plan ---';
PRINT 'Jobs that match cluster cron (will be backfilled): ' + CAST(@BackfillCount AS VARCHAR(10));
PRINT 'Jobs with divergent cron (stay unmanaged):         ' + CAST(@SkippedCount AS VARCHAR(10));

IF @SkippedCount > 0
BEGIN
    PRINT '';
    PRINT '--- Skipped jobs (cron divergence) ---';
    SELECT c.JobID, c.EndpointName, c.JobType, c.BrandUID,
           c.JobCron AS JobActualCron, cc.ChosenCron AS ClusterCron
    FROM @Candidates c
    JOIN @ClusterCrons cc ON cc.EndpointID = c.EndpointID AND cc.JobType = c.JobType
    WHERE NOT EXISTS (SELECT 1 FROM @PredictedMatching pm WHERE pm.JobID = c.JobID)
    ORDER BY c.EndpointName, c.JobID;
END

IF @DryRun = 0 AND @BackfillCount > 0
BEGIN
    DECLARE @ToBackfill TABLE (
        JobID                       INT PRIMARY KEY,
        ProjectEndpointID           INT NOT NULL
    );

    INSERT INTO @ToBackfill (JobID, ProjectEndpointID)
    SELECT c.JobID, pe.ProjectEndpointID
    FROM @Candidates c
    JOIN @ClusterCrons cc
      ON cc.EndpointID = c.EndpointID AND cc.JobType = c.JobType
     AND ISNULL(c.JobCron,'') = ISNULL(cc.ChosenCron,'')
    JOIN admin.ProjectEndpoints pe
      ON pe.ProjectID  = @ProjectID
     AND pe.EndpointID = c.EndpointID
     AND pe.JobType    = c.JobType;

    UPDATE j
    SET    ManagedByProjectID         = @ProjectID,
           ManagedByProjectEndpointID = tb.ProjectEndpointID,
           UpdatedAt                  = SYSUTCDATETIME()
    FROM   admin.Jobs j
    JOIN   @ToBackfill tb ON tb.JobID = j.JobID
    WHERE  j.ManagedByProjectID IS NULL;

    PRINT '';
    PRINT 'Jobs backfilled: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
END
ELSE IF @DryRun = 1
BEGIN
    PRINT '';
    PRINT '[DRY-RUN] Would update ' + CAST(@BackfillCount AS VARCHAR(10)) + ' admin.Jobs rows with ManagedByProject* values.';
END

/* ---------- Final summary ---------- */
PRINT '';
PRINT '=== summary ===';
IF @DryRun = 1
    PRINT 'No changes written. Re-run with @DryRun = 0 to apply.';
ELSE
    PRINT 'Retrofit applied. Verify with the post-run queries.';
GO
