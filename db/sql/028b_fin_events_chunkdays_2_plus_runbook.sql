/* =============================================================================
   028b — drop AMZ_FINANCIAL_EVENTS chunkDays from 5 to 2 + add timeout runbook.

   Run against: skc-admin   (DML only)

   ZenToes 180-day backfill at chunkDays=5 hit a 30s SQL request timeout
   after 36 minutes of work — the table had grown enough that late-stage
   MERGE batches couldn't finish in time. Two compounding fixes:

     - Code (already pushed): brandPool requestTimeout 30s → 120s,
       MERGE BATCH_SIZE 1000 → 500
     - Params (this file): chunkDays 5 → 2 for finer-grained recovery

   180/2 = 90 chunks. Each chunk is small enough that even on the
   highest-volume brand the MERGE finishes well under the bumped timeout.

   Idempotent.
   ============================================================================= */

USE [skc-admin];
GO

UPDATE admin.Jobs
   SET Params    = JSON_MODIFY(ISNULL(Params, N'{}'), '$.chunkDays', 2),
       UpdatedAt = SYSUTCDATETIME()
 WHERE EndpointID IN (
    SELECT e.EndpointID
      FROM admin.Endpoints e
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
     WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_FINANCIAL_EVENTS'
 );


/* ----- Runbook: tedious request timeout ----- */

MERGE admin.ErrorRunbooks AS tgt
USING (VALUES
    (N'%Request failed to complete in%',
     N'SQL Server: tedious request timeout',
     N'A SQL request didn''t finish before the driver''s requestTimeout. Most often this is a MERGE proc on a high-volume brand timing out late in a backfill — index pages grow, lock contention rises, and a per-call timeout that was fine early becomes tight.',
     N'1) For BACKFILL jobs, drop chunkDays in admin.Jobs.Params (e.g. 5 → 2). Smaller chunks = smaller MERGE input = faster per-call. 2) Drop BATCH_SIZE in the runner (lib/amazonFinancialEventsRunner.js) — 500 is safer than 1000 for big tables. 3) If still hitting it, bump brandPool requestTimeout in lib/brandDb.js (currently 120000 / 2 min). 4) Re-run from the Jobs page — hash-gated MERGE makes resumed backfills no-ops on already-ingested chunks, so you only re-pay for what failed.',
     N'WARN')
) AS src (MatchPattern, Title, WhatItMeans, HowToFix, Severity)
ON tgt.MatchPattern = src.MatchPattern
WHEN MATCHED THEN
    UPDATE SET Title = src.Title, WhatItMeans = src.WhatItMeans,
               HowToFix = src.HowToFix, Severity = src.Severity,
               UpdatedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (MatchPattern, Title, WhatItMeans, HowToFix, Severity)
    VALUES (src.MatchPattern, src.Title, src.WhatItMeans, src.HowToFix, src.Severity);

PRINT '028b applied: chunkDays=2 + timeout runbook seeded.';
GO
