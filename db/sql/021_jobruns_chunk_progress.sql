/* =============================================================================
   Migration 021 — add chunk-progress columns to admin.JobRuns.

   Run against: skc-admin

   Why:
     Long-window jobs (180-day backfills, historical order pulls, etc.)
     now execute in N smaller chunks instead of one shot. We want the UI
     to render progress while a chunked run is in flight — "chunk 3/6 ·
     27K rows so far" — rather than a black-box RUNNING badge that could
     sit there silently for an hour.

     Runners set ChunksTotal at the start of the run (as soon as windows
     are computed) and bump ChunksCompleted after each chunk finishes.
     RowsIngested is also updated incrementally, so the UI sees progress
     even within a single very large chunk's MERGE.

     Runners that don't chunk (short single-window pulls) simply leave
     both columns NULL, and the UI falls back to the previous behavior.

   Idempotent: safe to re-run.
   ============================================================================= */

SET NOCOUNT ON;
GO

IF COL_LENGTH('admin.JobRuns', 'ChunksTotal') IS NULL
BEGIN
    ALTER TABLE admin.JobRuns ADD ChunksTotal INT NULL;
    PRINT 'Added admin.JobRuns.ChunksTotal.';
END
ELSE
    PRINT 'admin.JobRuns.ChunksTotal already exists.';
GO

IF COL_LENGTH('admin.JobRuns', 'ChunksCompleted') IS NULL
BEGIN
    ALTER TABLE admin.JobRuns ADD ChunksCompleted INT NULL;
    PRINT 'Added admin.JobRuns.ChunksCompleted.';
END
ELSE
    PRINT 'admin.JobRuns.ChunksCompleted already exists.';
GO

PRINT '--------------------------------------------------';
PRINT 'Migration 021 complete: chunk-progress columns added.';
PRINT '--------------------------------------------------';
GO
