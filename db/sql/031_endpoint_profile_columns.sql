/* =============================================================================
   031 — admin.Endpoints becomes a profile/template for jobs.

   Run against: skc-admin

   Phase 1 of the endpoint-as-profile architecture (see
   skc-roadmap/~039-endpoint-profile-architecture.md). Adds Default*
   columns so brand jobs can inherit cron + params + execution mode
   from the endpoint instead of operators setting them per-row.

   Phase 2 (UI rebuild) and Phase 3 (auto-create on new brand) are
   tracked separately in the roadmap. This migration is additive —
   current admin.Jobs rows keep their explicit values, no behavior
   change until the UI starts honoring defaults.

   Idempotent.
   ============================================================================= */

SET NOCOUNT ON;
GO

IF COL_LENGTH('admin.Endpoints', 'DefaultCronExpression') IS NULL
BEGIN
    ALTER TABLE admin.Endpoints ADD DefaultCronExpression NVARCHAR(50) NULL;
    PRINT 'Added admin.Endpoints.DefaultCronExpression';
END
GO

IF COL_LENGTH('admin.Endpoints', 'DefaultTimezoneIANA') IS NULL
BEGIN
    ALTER TABLE admin.Endpoints
        ADD DefaultTimezoneIANA NVARCHAR(50) NOT NULL
            CONSTRAINT DF_Endpoints_DefaultTZ DEFAULT (N'America/Chicago');
    PRINT 'Added admin.Endpoints.DefaultTimezoneIANA';
END
GO

IF COL_LENGTH('admin.Endpoints', 'DefaultParams') IS NULL
BEGIN
    ALTER TABLE admin.Endpoints ADD DefaultParams NVARCHAR(MAX) NULL;
    PRINT 'Added admin.Endpoints.DefaultParams';
END
GO

IF COL_LENGTH('admin.Endpoints', 'DefaultExecutionMode') IS NULL
BEGIN
    ALTER TABLE admin.Endpoints
        ADD DefaultExecutionMode NVARCHAR(20) NOT NULL
            CONSTRAINT DF_Endpoints_DefaultExecMode DEFAULT (N'NODE_NATIVE');
    PRINT 'Added admin.Endpoints.DefaultExecutionMode';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.check_constraints
    WHERE name = 'CK_Endpoints_DefaultExecMode'
      AND parent_object_id = OBJECT_ID('admin.Endpoints')
)
BEGIN
    ALTER TABLE admin.Endpoints
        ADD CONSTRAINT CK_Endpoints_DefaultExecMode
            CHECK (DefaultExecutionMode IN (N'SSIS_NATIVE', N'SSIS_CALLS_NODE', N'NODE_NATIVE'));
    PRINT 'Added CK_Endpoints_DefaultExecMode';
END
GO

IF COL_LENGTH('admin.Endpoints', 'DefaultJobType') IS NULL
BEGIN
    ALTER TABLE admin.Endpoints
        ADD DefaultJobType NVARCHAR(20) NOT NULL
            CONSTRAINT DF_Endpoints_DefaultJobType DEFAULT (N'INGEST');
    PRINT 'Added admin.Endpoints.DefaultJobType';
END
GO

IF COL_LENGTH('admin.Endpoints', 'DefaultIsActive') IS NULL
BEGIN
    ALTER TABLE admin.Endpoints
        ADD DefaultIsActive BIT NOT NULL
            CONSTRAINT DF_Endpoints_DefaultIsActive DEFAULT (1);
    PRINT 'Added admin.Endpoints.DefaultIsActive';
END
GO

IF COL_LENGTH('admin.Endpoints', 'AutoCreateOnNewBrand') IS NULL
BEGIN
    ALTER TABLE admin.Endpoints
        ADD AutoCreateOnNewBrand BIT NOT NULL
            CONSTRAINT DF_Endpoints_AutoCreate DEFAULT (0);
    PRINT 'Added admin.Endpoints.AutoCreateOnNewBrand (default 0 = opt-in)';
END
GO


PRINT '--------------------------------------------------';
PRINT '031 DDL complete. Run 031_seed_endpoint_defaults.sql next';
PRINT 'in SSMS to backfill Default* columns from current Jobs patterns.';
PRINT '--------------------------------------------------';
GO
