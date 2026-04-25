/* =============================================================================
   028 — drop AMZ_FINANCIAL_EVENTS chunkDays from 15 to 5.

   Run against: skc-admin   (DML only)

   180 / 5 = 36 chunks per backfill (was 12 with chunkDays=15). Each chunk
   covers a smaller date window so:
     - per-chunk page count is lower → 429 risk drops further
     - progress bar moves more frequently on /jobs.html
     - if a single chunk fails, only 5 days of work needs to retry
       (hash-gated MERGE makes that retry safe)
     - total wall time roughly the same — page delay is the bottleneck,
       not chunk overhead

   Applies to both INGEST + BACKFILL via JSON_MODIFY so the key is added if
   missing, updated if present. INGEST jobs effectively no-op (daysBack=2
   still collapses to 1 chunk regardless of chunkDays).

   Idempotent — re-running just sets the same value.
   ============================================================================= */

USE [skc-admin];
GO

UPDATE admin.Jobs
   SET Params    = JSON_MODIFY(ISNULL(Params, N'{}'), '$.chunkDays', 5),
       UpdatedAt = SYSUTCDATETIME()
 WHERE EndpointID IN (
    SELECT e.EndpointID
      FROM admin.Endpoints e
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
     WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_FINANCIAL_EVENTS'
 );

SELECT JobID, Name, JobType, Params
  FROM admin.Jobs
 WHERE EndpointID IN (
    SELECT e.EndpointID
      FROM admin.Endpoints e
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
     WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_FINANCIAL_EVENTS'
 )
 ORDER BY JobType, Name;

PRINT '028 complete: AMZ_FINANCIAL_EVENTS chunkDays now 5.';
GO
