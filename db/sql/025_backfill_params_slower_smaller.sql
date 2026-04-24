/* =============================================================================
   Migration 025 — retune BACKFILL job Params for quota safety.

   Run against: skc-admin   (DML only — no schema changes)

   What changes:
     Recurring (INGEST) jobs keep daysBack=2, no other Params. They run
     fast because they rarely exceed a handful of pages.

     BACKFILL jobs now carry:
       {"daysBack": 180, "chunkDays": 15, "pageDelayMs": 3000}

     Why:
       - chunkDays 15 (was 30): splits 180 days into 12 chunks, not 6.
         Each chunk only ~100 pages max even for ZenToes-volume sellers,
         which stays well under Amazon's 30-request burst ceiling so we
         don't have to rely on retry backoff to recover.
       - pageDelayMs 3000 (runner default 2100): ~0.33 rps, below the
         0.5 rps sustained quota with a comfortable safety margin. The
         2100 default stays fine for the quick daily recurring pulls.

   Running cost per ZenToes-style backfill (~600 pages total):
     12 chunks × ~50 pages × 3s  =  ~30 min end-to-end
   Slower than before, but shouldn't 429 mid-run — better steady
   progress than repeated fail-and-retry cycles.

   Idempotent via UPDATE on AMZ_FINANCIAL_EVENTS BACKFILL rows only.
   ============================================================================= */

SET NOCOUNT ON;
GO

DECLARE @EndpointID INT = (
    SELECT e.EndpointID
      FROM admin.Endpoints e
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
     WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_FINANCIAL_EVENTS'
);

IF @EndpointID IS NULL
BEGIN
    RAISERROR('AMZ_FINANCIAL_EVENTS endpoint not found.', 16, 1);
    RETURN;
END

UPDATE admin.Jobs
   SET Params    = N'{"daysBack":180,"chunkDays":15,"pageDelayMs":3000}',
       UpdatedAt = SYSUTCDATETIME()
 WHERE EndpointID    = @EndpointID
   AND JobType       = 'BACKFILL'
   AND ExecutionMode = 'NODE_NATIVE';

SELECT JobID, Name, JobType, Params
  FROM admin.Jobs
 WHERE EndpointID    = @EndpointID
   AND JobType       = 'BACKFILL'
 ORDER BY Name;

PRINT 'AMZ_FINANCIAL_EVENTS BACKFILL jobs retuned to chunkDays=15, pageDelayMs=3000.';
GO
