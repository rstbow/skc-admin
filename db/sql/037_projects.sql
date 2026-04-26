/* =============================================================================
   037_projects.sql
   SKU Compass Admin — Projects/Folders for jobs.html grouping
   Database:    skc-admin
   Adds:        admin.Projects, admin.ProjectEndpoints, admin.ProjectBrands
   Modifies:    admin.Jobs (+ ManagedByProjectID, ManagedByProjectEndpointID)
   Author:      admin (Nator), reviewed and amended by sql-specialist (Chip)
   Reviewed:    2026-04-26 v3 (20:10 UTC) against snapshot 2026-04-26 17:12 UTC
   Applied:     2026-04-26 (executed by Randy in SSMS via sql-to-run/ workflow)
   Idempotent:  safe to re-run.

   Provenance:  this file is the canonical repo copy of the migration that ran
                via team-ops/projects/skucompass/sql-to-run/2026-04-26-01-037-
                projects-schema.md (now in sql-to-run/done/). Review threads:
                  - inbox-sql/done/2026-04-26-02-projects-folder-system-schema.md
                  - inbox-admin/done/2026-04-26-04-confirm-037-projects-changes.md
                  - inbox-admin/done/2026-04-26-05-cascade-paths-pushback.md

   Net changes from Nator's draft (v3, after self-review and Nator's response):
     - ProjectBrands.BrandUID now has FK to admin.Brands(BrandUID)
     - IX_Jobs_ManagedByProject widened from 2 cols to 3 (added BrandUID) so
       the materialization-sync join seeks instead of doing a key lookup
     - Added ISJSON CHECK on ProjectEndpoints.Params
     - Split the FK_Jobs_ManagedByProject* IF NOT EXISTS check into two
       independent batches (idempotency fix — partial-failure retry)
     - Added rollback block at end (commented)

   Changes considered and rejected (kept here for the historical record):
     - ON DELETE SET NULL on Jobs.ManagedByProject* FKs — would cause SQL
       error 1785 (multiple cascade paths Projects -> Jobs). Stayed with
       Nator's plain FKs; Node sync engine handles cleanup.
     - WITH (ONLINE = ON) on Jobs index — not supported on lower Azure SQL
       tiers; zero benefit on a 16-row table. Defer.
     - GRANT block for skc_app_user — Nator pushed back correctly:
       skc_app_user is app2's login on vs-ims-staging, not a principal in
       skc-admin. App2 doesn't read admin.* directly. Convention doesn't
       apply here.

   No transactions: each batch is independently idempotent (IF NOT EXISTS
   guards everything). DDL across multiple GO batches can't be wrapped in a
   single tran anyway. Partial failure is safe to retry — re-running
   creates whatever's missing.
   ============================================================================= */

USE [skc-admin];
GO

SET NOCOUNT ON;
GO

/* =============================================================================
   admin.Projects — top-level folder grouping endpoints + brands
   ============================================================================= */
IF OBJECT_ID('admin.Projects', 'U') IS NULL
BEGIN
    CREATE TABLE admin.Projects (
        ProjectID           INT IDENTITY(1,1) NOT NULL,
        ProjectUID          UNIQUEIDENTIFIER  NOT NULL CONSTRAINT DF_Projects_UID DEFAULT (NEWID()),
        Name                NVARCHAR(100)     NOT NULL,
        DisplayName         NVARCHAR(150)     NULL,
        ConnectorScope      NVARCHAR(50)      NULL,
        Description         NVARCHAR(MAX)     NULL,
        SortOrder           INT               NOT NULL CONSTRAINT DF_Projects_Sort    DEFAULT (100),
        IsActive            BIT               NOT NULL CONSTRAINT DF_Projects_Active  DEFAULT (1),
        CreatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_Projects_Created DEFAULT (SYSUTCDATETIME()),
        UpdatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_Projects_Updated DEFAULT (SYSUTCDATETIME()),
        CreatedBy           INT               NULL,
        UpdatedBy           INT               NULL,
        CONSTRAINT PK_Projects        PRIMARY KEY CLUSTERED (ProjectID),
        CONSTRAINT UQ_Projects_UID    UNIQUE NONCLUSTERED (ProjectUID),
        CONSTRAINT UQ_Projects_Name   UNIQUE NONCLUSTERED (Name)
    );
END
GO

/* =============================================================================
   admin.ProjectEndpoints — endpoints (with cron + params) belonging to a project
   ============================================================================= */
IF OBJECT_ID('admin.ProjectEndpoints', 'U') IS NULL
BEGIN
    CREATE TABLE admin.ProjectEndpoints (
        ProjectEndpointID   INT IDENTITY(1,1) NOT NULL,
        ProjectID           INT               NOT NULL,
        EndpointID          INT               NOT NULL,
        JobType             NVARCHAR(20)      NOT NULL CONSTRAINT DF_PrjEp_Type    DEFAULT ('INGEST'),
        CronExpression      NVARCHAR(50)      NULL,
        TimezoneIANA        NVARCHAR(50)      NOT NULL CONSTRAINT DF_PrjEp_TZ      DEFAULT ('America/Chicago'),
        Params              NVARCHAR(MAX)     NULL,
        Priority            INT               NOT NULL CONSTRAINT DF_PrjEp_Prio    DEFAULT (50),
        IsActive            BIT               NOT NULL CONSTRAINT DF_PrjEp_Active  DEFAULT (1),
        SortOrder           INT               NOT NULL CONSTRAINT DF_PrjEp_Sort    DEFAULT (100),
        CreatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_PrjEp_Created DEFAULT (SYSUTCDATETIME()),
        UpdatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_PrjEp_Updated DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_ProjectEndpoints                PRIMARY KEY CLUSTERED (ProjectEndpointID),
        CONSTRAINT FK_PrjEp_Project                   FOREIGN KEY (ProjectID)  REFERENCES admin.Projects (ProjectID)   ON DELETE CASCADE,
        CONSTRAINT FK_PrjEp_Endpoint                  FOREIGN KEY (EndpointID) REFERENCES admin.Endpoints (EndpointID),
        CONSTRAINT UQ_PrjEp_Project_Endpoint_Type     UNIQUE NONCLUSTERED (ProjectID, EndpointID, JobType),
        CONSTRAINT CK_PrjEp_JobType                   CHECK (JobType IN ('INGEST','ROLLUP','MIGRATE','BACKFILL')),
        CONSTRAINT CK_PrjEp_Params_IsJson             CHECK (Params IS NULL OR ISJSON(Params) = 1)
    );

    CREATE INDEX IX_PrjEp_Endpoint ON admin.ProjectEndpoints (EndpointID);
END
GO

/* =============================================================================
   admin.ProjectBrands — brand memberships in a project
   CHANGED FROM DRAFT: added FK to admin.Brands(BrandUID)
   ============================================================================= */
IF OBJECT_ID('admin.ProjectBrands', 'U') IS NULL
BEGIN
    CREATE TABLE admin.ProjectBrands (
        ProjectBrandID      INT IDENTITY(1,1) NOT NULL,
        ProjectID           INT               NOT NULL,
        BrandUID            UNIQUEIDENTIFIER  NOT NULL,
        IsActive            BIT               NOT NULL CONSTRAINT DF_PrjBr_Active DEFAULT (1),
        JoinedAt            DATETIME2(3)      NOT NULL CONSTRAINT DF_PrjBr_Joined DEFAULT (SYSUTCDATETIME()),
        CreatedBy           INT               NULL,
        CONSTRAINT PK_ProjectBrands           PRIMARY KEY CLUSTERED (ProjectBrandID),
        CONSTRAINT FK_PrjBr_Project           FOREIGN KEY (ProjectID) REFERENCES admin.Projects (ProjectID) ON DELETE CASCADE,
        CONSTRAINT FK_PrjBr_Brand             FOREIGN KEY (BrandUID)  REFERENCES admin.Brands   (BrandUID),
        CONSTRAINT UQ_PrjBr_Project_Brand     UNIQUE NONCLUSTERED (ProjectID, BrandUID)
    );

    CREATE INDEX IX_PrjBr_Brand ON admin.ProjectBrands (BrandUID);
END
GO

/* =============================================================================
   admin.Jobs — add management columns. Existing rows: NULL = unmanaged (legacy/manual).
   ============================================================================= */
IF COL_LENGTH('admin.Jobs', 'ManagedByProjectID') IS NULL
BEGIN
    ALTER TABLE admin.Jobs ADD
        ManagedByProjectID         INT NULL,
        ManagedByProjectEndpointID INT NULL;
END
GO

/* SELF-REVIEW REVERSION: back to Nator's plain FKs (no cascade action).
   Earlier draft of this artifact (v1) added ON DELETE SET NULL — that creates
   multiple cascade paths from Projects -> Jobs (direct + via ProjectEndpoints
   CASCADE), which SQL Server forbids (error 1785). Node sync engine does the
   cleanup, as Nator originally designed.

   ALSO FIXED: split the IF NOT EXISTS into two independent checks so a
   partial-failure retry creates whichever FK is missing. */
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_Jobs_ManagedByProject')
BEGIN
    ALTER TABLE admin.Jobs ADD
        CONSTRAINT FK_Jobs_ManagedByProject
            FOREIGN KEY (ManagedByProjectID) REFERENCES admin.Projects (ProjectID);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_Jobs_ManagedByProjectEndpoint')
BEGIN
    ALTER TABLE admin.Jobs ADD
        CONSTRAINT FK_Jobs_ManagedByProjectEndpoint
            FOREIGN KEY (ManagedByProjectEndpointID) REFERENCES admin.ProjectEndpoints (ProjectEndpointID);
END
GO

/* CHANGED FROM DRAFT: widened to include BrandUID so the materialization sync
   join (test query #4) seeks instead of doing a key lookup. Replaces the
   narrower draft index.

   SELF-REVIEW REVERSION: dropped WITH (ONLINE = ON) — not supported on lower
   Azure SQL tiers (Basic / Standard DTU); zero benefit on a 16-row table.
   Add back when the table is large enough to matter and tier is confirmed. */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Jobs_ManagedByProject' AND object_id = OBJECT_ID('admin.Jobs'))
BEGIN
    CREATE INDEX IX_Jobs_ManagedByProject
        ON admin.Jobs (ManagedByProjectID, ManagedByProjectEndpointID, BrandUID)
        WHERE ManagedByProjectID IS NOT NULL;
END
GO

/* =============================================================================
   GRANTS: intentionally none.

   v1 of this artifact added a GRANT block for skc_app_user. Nator (admin-app)
   correctly pushed back: skc_app_user is app2's login on vs-ims-staging, not a
   principal in skc-admin. App2 never reads admin.* directly — it goes through
   HTTP. The "GRANT to skc_app_user on every new table" rule applies to
   curated.* views in vs-ims-staging, not to admin.* in skc-admin.

   The admin schema is accessed by skc_admin_app (admin-side login) which
   already has database-level rights. No GRANT statements needed here.
   ============================================================================= */

/* =============================================================================
   Seed initial Projects (idempotent on Name)
   ============================================================================= */
IF NOT EXISTS (SELECT 1 FROM admin.Projects WHERE Name = 'Amazon Daily')
    INSERT admin.Projects (Name, DisplayName, ConnectorScope, Description, SortOrder)
    VALUES ('Amazon Daily', 'Amazon — Daily ingest', 'AMAZON_SP_API',
            'Listings, rank, financial events, orders, returns on standard cadence.', 10);

IF NOT EXISTS (SELECT 1 FROM admin.Projects WHERE Name = 'Amazon Onboarding')
    INSERT admin.Projects (Name, DisplayName, ConnectorScope, Description, SortOrder)
    VALUES ('Amazon Onboarding', 'Amazon — Onboarding kickoff', 'AMAZON_SP_API',
            'First-load endpoints fired when a brand connects Amazon. Mirrors the amazon-onboarding bundle.', 20);

IF NOT EXISTS (SELECT 1 FROM admin.Projects WHERE Name = 'Walmart Daily')
    INSERT admin.Projects (Name, DisplayName, ConnectorScope, Description, SortOrder)
    VALUES ('Walmart Daily', 'Walmart — Daily ingest', 'WALMART_MP',
            'Placeholder. Endpoints + brands attach once Walmart runners ship.', 30);
GO

PRINT '037_projects.sql applied successfully.';
GO

/* =============================================================================
   ROLLBACK (commented — uncomment + run only if you need to undo)
   =============================================================================
   -- Drop in reverse dependency order.
   IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Jobs_ManagedByProject' AND object_id = OBJECT_ID('admin.Jobs'))
       DROP INDEX IX_Jobs_ManagedByProject ON admin.Jobs;

   IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_Jobs_ManagedByProjectEndpoint')
       ALTER TABLE admin.Jobs DROP CONSTRAINT FK_Jobs_ManagedByProjectEndpoint;
   IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_Jobs_ManagedByProject')
       ALTER TABLE admin.Jobs DROP CONSTRAINT FK_Jobs_ManagedByProject;

   IF COL_LENGTH('admin.Jobs','ManagedByProjectEndpointID') IS NOT NULL
       ALTER TABLE admin.Jobs DROP COLUMN ManagedByProjectEndpointID;
   IF COL_LENGTH('admin.Jobs','ManagedByProjectID') IS NOT NULL
       ALTER TABLE admin.Jobs DROP COLUMN ManagedByProjectID;

   IF OBJECT_ID('admin.ProjectBrands','U')    IS NOT NULL DROP TABLE admin.ProjectBrands;
   IF OBJECT_ID('admin.ProjectEndpoints','U') IS NOT NULL DROP TABLE admin.ProjectEndpoints;
   IF OBJECT_ID('admin.Projects','U')         IS NOT NULL DROP TABLE admin.Projects;
   ============================================================================= */
