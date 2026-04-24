/* =============================================================================
   Hotfix for migration 017 — restore EXECUTE grant on the recreated proc.

   Run against: vs-ims-staging

   Why:
     Migration 017 used DROP + CREATE to swap the TVP-based proc for the
     JSON-based one. DROP wipes permissions; CREATE gives back a fresh
     object with no grants on it. The old proc had GRANT EXECUTE TO
     skc_app_user — the runner's login — so the runner started failing
     with "EXECUTE permission was denied" the first time it tried to
     use the rebuilt proc.

     Lesson: future proc refactors should use CREATE OR ALTER PROCEDURE,
     which preserves grants. See C:\Users\rstbo\Projects\skc-admin\db\sql\
     for when the pattern is appropriate (not idempotent across signature
     changes, so occasionally DROP+CREATE is unavoidable — in which case
     add a re-GRANT at the tail of the migration).

   Idempotent: safe to re-run.
   ============================================================================= */

SET NOCOUNT ON;
GO

IF OBJECT_ID('raw.usp_merge_amz_financial_events', 'P') IS NULL
BEGIN
    RAISERROR('raw.usp_merge_amz_financial_events does not exist — run migration 017 first.', 16, 1);
    RETURN;
END

GRANT EXECUTE ON raw.usp_merge_amz_financial_events TO skc_app_user;
PRINT 'Granted EXECUTE on raw.usp_merge_amz_financial_events to skc_app_user.';
GO
