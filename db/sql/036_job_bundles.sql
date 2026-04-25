/* =============================================================================
   036 — Job Bundles: named, persistent recipes for provisioning + firing
   groups of jobs.

   Run against: skc-admin

   Why this exists
   ----------------
   The Phase 3 onboarding hook (POST /api/runner/onboard-brand-jobs) hardcodes:
     - which endpoints to attach for a new Amazon brand
     - which endpoints to fire immediately vs. delayed
     - the 5-minute delay between listings and rank
     - the BACKFILL row for AMZ_FINANCIAL_EVENTS

   That works for one connector but doesn't scale. As soon as we add
   Shopify / QBO / Walmart onboarding flows we'd be cloning the same
   logic with different constants. And app2 can't trigger anything
   *but* "the onboarding flow" — there's no way to say "run this
   bundle of jobs" by name.

   This migration introduces:
     admin.JobBundles      — one row per named recipe
     admin.JobBundleSteps  — ordered steps within a bundle

   Each step references an Endpoint and an Action:
     PROVISION    — INSERT the admin.Jobs row from endpoint defaults; don't fire
     FIRE_NOW     — provision (idempotent) + immediately runNow
     FIRE_DELAYED — provision + runNow after DelayMinutes
     PROVISION_BACKFILL — provision a paused BACKFILL job (e.g., 180-day pull)

   Bundles are addressable by app2 via:
     POST /api/bundles/{name}/run  (X-Service-Token)
       body: { brandUID, credentialID? }

   Idempotent.
   ============================================================================= */

USE [skc-admin];
GO

SET NOCOUNT ON;
GO


/* ---------- 1. Tables ---------- */

IF OBJECT_ID('admin.JobBundles', 'U') IS NULL
BEGIN
    CREATE TABLE admin.JobBundles (
        BundleID       INT IDENTITY(1,1) NOT NULL,
        BundleUID      UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_JobBundles_UID DEFAULT (NEWID()),
        Name           NVARCHAR(100)    NOT NULL,
        DisplayName    NVARCHAR(200)    NULL,
        Description    NVARCHAR(MAX)    NULL,
        ConnectorScope NVARCHAR(50)     NULL,    -- e.g. 'AMAZON_SP_API'; NULL = cross-connector
        IsActive       BIT              NOT NULL CONSTRAINT DF_JobBundles_Active DEFAULT (1),
        CreatedAt      DATETIME2(3)     NOT NULL CONSTRAINT DF_JobBundles_Created DEFAULT (SYSUTCDATETIME()),
        UpdatedAt      DATETIME2(3)     NOT NULL CONSTRAINT DF_JobBundles_Updated DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_JobBundles PRIMARY KEY CLUSTERED (BundleID),
        CONSTRAINT UQ_JobBundles_Name UNIQUE (Name),
        CONSTRAINT UQ_JobBundles_UID  UNIQUE (BundleUID)
    );
    PRINT 'Created admin.JobBundles.';
END
ELSE
    PRINT 'admin.JobBundles already exists — skipping.';
GO

IF OBJECT_ID('admin.JobBundleSteps', 'U') IS NULL
BEGIN
    CREATE TABLE admin.JobBundleSteps (
        StepID         INT IDENTITY(1,1) NOT NULL,
        BundleID       INT              NOT NULL,
        StepOrder      INT              NOT NULL,
        EndpointID     INT              NOT NULL,
        Action         NVARCHAR(30)     NOT NULL,
        DelayMinutes   INT              NULL,
        ParamsOverride NVARCHAR(MAX)    NULL,    -- JSON merged on top of endpoint DefaultParams
        JobType        NVARCHAR(20)     NULL,    -- override endpoint DefaultJobType (e.g., BACKFILL)
        IsActiveOverride BIT            NULL,    -- override endpoint DefaultIsActive (NULL = use default)
        Notes          NVARCHAR(MAX)    NULL,
        CreatedAt      DATETIME2(3)     NOT NULL CONSTRAINT DF_JobBundleSteps_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_JobBundleSteps PRIMARY KEY CLUSTERED (StepID),
        CONSTRAINT FK_JobBundleSteps_Bundle FOREIGN KEY (BundleID)
            REFERENCES admin.JobBundles(BundleID) ON DELETE CASCADE,
        CONSTRAINT FK_JobBundleSteps_Endpoint FOREIGN KEY (EndpointID)
            REFERENCES admin.Endpoints(EndpointID),
        CONSTRAINT CK_JobBundleSteps_Action CHECK (Action IN
            (N'PROVISION', N'FIRE_NOW', N'FIRE_DELAYED', N'PROVISION_BACKFILL')),
        CONSTRAINT UQ_JobBundleSteps_Order UNIQUE (BundleID, StepOrder)
    );
    CREATE INDEX IX_JobBundleSteps_Bundle ON admin.JobBundleSteps (BundleID, StepOrder);
    PRINT 'Created admin.JobBundleSteps.';
END
ELSE
    PRINT 'admin.JobBundleSteps already exists — skipping.';
GO


/* ---------- 2. Seed: amazon-onboarding ----------
   Mirrors the hardcoded logic in routes/runner.js's onboard-brand-jobs hook.
   After this migration + the runner refactor, that hook just calls the
   bundle engine with bundleName='amazon-onboarding'.
*/

DECLARE @BundleID INT;

MERGE admin.JobBundles AS tgt
USING (SELECT
    N'amazon-onboarding'                                  AS Name,
    N'Amazon — New Brand Onboarding'                      AS DisplayName,
    N'Provisions every AMAZON_SP_API endpoint that has AutoCreateOnNewBrand=1, fires the recurring loads immediately, schedules rank snapshot 5 minutes later, and creates a paused 180-day Financial Events backfill row for the user to fire on demand. Called by app2 right after a brand saves Amazon credentials.'
                                                          AS Description,
    N'AMAZON_SP_API'                                      AS ConnectorScope
) AS src
ON tgt.Name = src.Name
WHEN MATCHED THEN
    UPDATE SET DisplayName    = src.DisplayName,
               Description    = src.Description,
               ConnectorScope = src.ConnectorScope,
               IsActive       = 1,
               UpdatedAt      = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (Name, DisplayName, Description, ConnectorScope)
    VALUES (src.Name, src.DisplayName, src.Description, src.ConnectorScope);

SELECT @BundleID = BundleID FROM admin.JobBundles WHERE Name = N'amazon-onboarding';
PRINT 'amazon-onboarding bundle ID = ' + CAST(@BundleID AS NVARCHAR(10));

/* Replace steps wholesale — keeps the seed deterministic. */
DELETE FROM admin.JobBundleSteps WHERE BundleID = @BundleID;

DECLARE @ConnID INT = (SELECT ConnectorID FROM admin.Connectors WHERE Name = N'AMAZON_SP_API');

DECLARE @epListings    INT = (SELECT EndpointID FROM admin.Endpoints WHERE ConnectorID = @ConnID AND Name = N'AMZ_LISTINGS_READ');
DECLARE @epFinEvents   INT = (SELECT EndpointID FROM admin.Endpoints WHERE ConnectorID = @ConnID AND Name = N'AMZ_FINANCIAL_EVENTS');
DECLARE @epRank        INT = (SELECT EndpointID FROM admin.Endpoints WHERE ConnectorID = @ConnID AND Name = N'AMZ_LISTING_RANK_SNAPSHOT');
DECLARE @epOrders      INT = (SELECT EndpointID FROM admin.Endpoints WHERE ConnectorID = @ConnID AND Name = N'AMZ_ORDERS');
DECLARE @epReturns     INT = (SELECT EndpointID FROM admin.Endpoints WHERE ConnectorID = @ConnID AND Name = N'AMZ_RETURNS');

/* Step 1: provision + fire AMZ_LISTINGS_READ now (drives ASIN catalog). */
IF @epListings IS NOT NULL
INSERT INTO admin.JobBundleSteps (BundleID, StepOrder, EndpointID, Action, DelayMinutes, Notes)
VALUES (@BundleID, 10, @epListings, N'FIRE_NOW', NULL,
    N'Listings sweep. ASINs land in raw.amz_listings — rank step needs this first.');

/* Step 2: provision + fire AMZ_FINANCIAL_EVENTS now (last 2 days, fast). */
IF @epFinEvents IS NOT NULL
INSERT INTO admin.JobBundleSteps (BundleID, StepOrder, EndpointID, Action, DelayMinutes, Notes)
VALUES (@BundleID, 20, @epFinEvents, N'FIRE_NOW', NULL,
    N'Recurring 2-day financial events pull. Cheap, runs in seconds.');

/* Step 3: provision + fire AMZ_ORDERS now (CREATED mode, last 2 days). */
IF @epOrders IS NOT NULL
INSERT INTO admin.JobBundleSteps (BundleID, StepOrder, EndpointID, Action, DelayMinutes, Notes)
VALUES (@BundleID, 30, @epOrders, N'FIRE_NOW', NULL,
    N'Recent sales pull (CREATED mode). Gets the last 2 days of orders + items.');

/* Step 4: provision AMZ_RETURNS but don't fire — daily 5am cron handles it. */
IF @epReturns IS NOT NULL
INSERT INTO admin.JobBundleSteps (BundleID, StepOrder, EndpointID, Action, DelayMinutes, Notes)
VALUES (@BundleID, 40, @epReturns, N'PROVISION', NULL,
    N'Returns trickle in for ~30 days; daily cron picks them up — no immediate fire needed.');

/* Step 5: fire AMZ_LISTING_RANK_SNAPSHOT after 5min delay — needs ASINs from listings. */
IF @epRank IS NOT NULL
INSERT INTO admin.JobBundleSteps (BundleID, StepOrder, EndpointID, Action, DelayMinutes, Notes)
VALUES (@BundleID, 50, @epRank, N'FIRE_DELAYED', 5,
    N'Catalog Items rank pull. 5min delay so listings runner finishes populating ASINs first.');

/* Step 6: provision a paused BACKFILL row for AMZ_FINANCIAL_EVENTS. */
IF @epFinEvents IS NOT NULL
INSERT INTO admin.JobBundleSteps (BundleID, StepOrder, EndpointID, Action, DelayMinutes, JobType, IsActiveOverride, ParamsOverride, Notes)
VALUES (@BundleID, 60, @epFinEvents, N'PROVISION_BACKFILL', NULL, N'BACKFILL', 0,
    N'{"daysBack":180,"chunkDays":2,"pageDelayMs":3000}',
    N'Paused 180-day backfill row — user fires manually for historical reconciliation.');

DECLARE @StepCount INT = (SELECT COUNT(*) FROM admin.JobBundleSteps WHERE BundleID = @BundleID);
PRINT 'Seeded amazon-onboarding with ' + CAST(@StepCount AS NVARCHAR(10)) + ' steps.';
GO


/* ---------- 3. Verification SELECT ---------- */

SELECT
    b.Name           AS Bundle,
    b.DisplayName,
    b.ConnectorScope,
    s.StepOrder,
    e.Name           AS Endpoint,
    s.Action,
    s.DelayMinutes,
    s.JobType        AS JobTypeOverride,
    s.IsActiveOverride,
    s.ParamsOverride
FROM admin.JobBundles b
LEFT JOIN admin.JobBundleSteps s ON s.BundleID = b.BundleID
LEFT JOIN admin.Endpoints e      ON e.EndpointID = s.EndpointID
WHERE b.Name = N'amazon-onboarding'
ORDER BY s.StepOrder;

PRINT '036 complete: JobBundles schema + amazon-onboarding seeded.';
GO
