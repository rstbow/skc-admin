/* =============================================================================
   035 — Seed AMZ_ORDERS + AMZ_RETURNS endpoint profiles for auto-onboarding.

   Run against: skc-admin

   AMZ_ORDERS (new) — Orders API direct (REST_GET on /orders/v0/orders).
     Replaces the legacy report-based AMZ_ORDERS_BY_LAST_UPDATE seeded in
     008. The runner supports two pull modes via params.mode:
       CREATED (default) → CreatedAfter window for recent sales
       UPDATED           → LastUpdatedAfter window for status flips
     The merge proc is LastUpdate-aware so an out-of-order CREATED pull
     can never overwrite fresher UPDATED data.

   AMZ_RETURNS (existing in 008) — set defaults + AutoCreateOnNewBrand=1
     so the existing onboarding hook auto-provisions a daily 30-day pull
     for every new Amazon brand.

   Deactivates the legacy AMZ_ORDERS_BY_LAST_UPDATE row (no schedule, no
   auto-create) — it stays in the table for history but won't be picked up.

   Idempotent.
   ============================================================================= */

USE [skc-admin];
GO

SET NOCOUNT ON;
GO

DECLARE @ConnID INT = (SELECT ConnectorID FROM admin.Connectors WHERE Name = 'AMAZON_SP_API');
IF @ConnID IS NULL
BEGIN
    RAISERROR(N'AMAZON_SP_API connector not found. Run 002_seed_connectors.sql first.', 16, 1);
    RETURN;
END

/* ---------- 1. AMZ_ORDERS — new Orders API endpoint ---------- */

MERGE admin.Endpoints AS tgt
USING (
    SELECT @ConnID AS ConnectorID,
           N'AMZ_ORDERS' AS Name,
           N'Orders + Order Items (Orders API)' AS DisplayName,
           N'Sync getOrders + getOrderItems pulls. Two modes via params.mode: CREATED (default — recent sales by buyer PurchaseDate) or UPDATED (catch status flips on existing orders by LastUpdateDate). Backed by lib/amazonOrdersRunner.js. The merge proc raw.usp_merge_amz_orders is LastUpdate-aware — an out-of-order CREATED pull never overwrites fresher UPDATED data.'
               AS Description,
           N'REST_GET' AS EndpointType,
           N'GET'      AS HttpMethod,
           N'/orders/v0/orders' AS Path,
           N'NEXT_TOKEN' AS PaginationStrategy,
           CAST(NULL AS INT) AS PollIntervalSec,
           CAST(NULL AS INT) AS PollMaxAttempts,
           N'raw' AS TargetSchema,
           N'amz_orders' AS TargetTable,
           N'_BrandUID,AmazonOrderID' AS NaturalKeyColumns,
           5 AS RateLimitWeight,
           N'{"mode":"CREATED","daysBack":2,"chunkDays":2}' AS ParamsTemplate,
           N'Recurring CREATED-mode pull every 4 hours catches new sales. UPDATED-mode runs less often (daily) to refresh status changes. Order items are fetched per-order in CREATED mode only (item data is fixed at purchase time).'
               AS Notes
) AS src
ON tgt.ConnectorID = src.ConnectorID AND tgt.Name = src.Name
WHEN MATCHED THEN
    UPDATE SET DisplayName        = src.DisplayName,
               Description        = src.Description,
               EndpointType       = src.EndpointType,
               HttpMethod         = src.HttpMethod,
               Path               = src.Path,
               PaginationStrategy = src.PaginationStrategy,
               PollIntervalSec    = src.PollIntervalSec,
               PollMaxAttempts    = src.PollMaxAttempts,
               TargetSchema       = src.TargetSchema,
               TargetTable        = src.TargetTable,
               NaturalKeyColumns  = src.NaturalKeyColumns,
               RateLimitWeight    = src.RateLimitWeight,
               ParamsTemplate     = src.ParamsTemplate,
               Notes              = src.Notes,
               UpdatedAt          = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ConnectorID, Name, DisplayName, Description, EndpointType, HttpMethod, Path,
            PaginationStrategy, PollIntervalSec, PollMaxAttempts,
            TargetSchema, TargetTable, NaturalKeyColumns, RateLimitWeight,
            ParamsTemplate, Notes)
    VALUES (src.ConnectorID, src.Name, src.DisplayName, src.Description, src.EndpointType, src.HttpMethod, src.Path,
            src.PaginationStrategy, src.PollIntervalSec, src.PollMaxAttempts,
            src.TargetSchema, src.TargetTable, src.NaturalKeyColumns, src.RateLimitWeight,
            src.ParamsTemplate, src.Notes);
GO


/* ---------- 2. Set Default* + AutoCreateOnNewBrand for AMZ_ORDERS ---------- */

UPDATE e
   SET DefaultCronExpression = '0 */4 * * *',     -- every 4 hours
       DefaultTimezoneIANA   = 'America/Chicago',
       DefaultParams         = N'{"mode":"CREATED","daysBack":2,"chunkDays":2,"pageDelayMs":1000,"itemDelayMs":2100}',
       DefaultExecutionMode  = 'NODE_NATIVE',
       DefaultJobType        = 'INGEST',
       DefaultIsActive       = 1,
       AutoCreateOnNewBrand  = 1,
       UpdatedAt             = SYSUTCDATETIME()
  FROM admin.Endpoints e
  JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
 WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_ORDERS';
GO


/* ---------- 3. AMZ_RETURNS — set defaults on existing endpoint ---------- */

UPDATE e
   SET DefaultCronExpression = '0 5 * * *',       -- 5am Chicago daily
       DefaultTimezoneIANA   = 'America/Chicago',
       DefaultParams         = N'{"daysBack":30}',
       DefaultExecutionMode  = 'NODE_NATIVE',
       DefaultJobType        = 'INGEST',
       DefaultIsActive       = 1,
       AutoCreateOnNewBrand  = 1,
       UpdatedAt             = SYSUTCDATETIME()
  FROM admin.Endpoints e
  JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
 WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_RETURNS';
GO


/* ---------- 4. Deactivate legacy AMZ_ORDERS_BY_LAST_UPDATE ----------
   Kept in the table for history (admin.Jobs may FK it if anyone wired
   it up); make sure new brands don't auto-create against the old report
   path. */

UPDATE e
   SET AutoCreateOnNewBrand  = 0,
       DefaultIsActive       = 0,
       Notes                 = COALESCE(e.Notes, N'') + N' [DEPRECATED 2026-04-25 — superseded by AMZ_ORDERS (Orders API direct).]',
       UpdatedAt             = SYSUTCDATETIME()
  FROM admin.Endpoints e
  JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
 WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_ORDERS_BY_LAST_UPDATE';
GO


/* Show the result */
SELECT e.Name, e.DefaultCronExpression, e.DefaultParams, e.AutoCreateOnNewBrand, e.DefaultIsActive
  FROM admin.Endpoints e
  JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
 WHERE c.Name = 'AMAZON_SP_API'
   AND e.Name IN ('AMZ_ORDERS','AMZ_RETURNS','AMZ_ORDERS_BY_LAST_UPDATE',
                  'AMZ_FINANCIAL_EVENTS','AMZ_LISTINGS_READ','AMZ_LISTING_RANK_SNAPSHOT')
 ORDER BY e.Name;

PRINT '035 complete: AMZ_ORDERS + AMZ_RETURNS auto-onboard ready.';
GO
