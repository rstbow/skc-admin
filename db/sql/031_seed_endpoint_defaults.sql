/* =============================================================================
   031 seed — backfill admin.Endpoints.Default* from current Jobs patterns.

   Run against: skc-admin   (DML — user runs in SSMS)

   Companion to 031_endpoint_profile_columns.sql. Sets sensible defaults
   on every Amazon ingestion endpoint, so future brand auto-onboarding
   has a real template to clone from.

   Defaults derived from the patterns Migration 020 + 026 + 028b set up
   for the existing 4 brands. If a brand has been overriding these via
   admin.Jobs.Params, that's preserved — only the endpoint defaults
   change.

   AutoCreateOnNewBrand is set to 1 for the recurring ingestion
   endpoints we want app2's brand-onboarding flow to provision
   automatically. BACKFILL-style endpoints (none yet at this layer)
   would stay 0 since they're manual triggers.

   Idempotent.
   ============================================================================= */

USE [skc-admin];
GO

SET NOCOUNT ON;
GO

UPDATE e
   SET DefaultCronExpression = '0 */6 * * *',
       DefaultTimezoneIANA   = 'America/Chicago',
       DefaultParams         = N'{"daysBack":2,"chunkDays":2,"pageDelayMs":2100}',
       DefaultExecutionMode  = 'NODE_NATIVE',
       DefaultJobType        = 'INGEST',
       DefaultIsActive       = 1,
       AutoCreateOnNewBrand  = 1,
       UpdatedAt             = SYSUTCDATETIME()
  FROM admin.Endpoints e
  JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
 WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_FINANCIAL_EVENTS';

UPDATE e
   SET DefaultCronExpression = '0 3 * * *',
       DefaultTimezoneIANA   = 'America/Chicago',
       DefaultParams         = N'{"marketplaceId":"ATVPDKIKX0DER"}',
       DefaultExecutionMode  = 'NODE_NATIVE',
       DefaultJobType        = 'INGEST',
       DefaultIsActive       = 1,
       AutoCreateOnNewBrand  = 1,
       UpdatedAt             = SYSUTCDATETIME()
  FROM admin.Endpoints e
  JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
 WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_LISTINGS_READ';

UPDATE e
   SET DefaultCronExpression = '0 4 * * *',
       DefaultTimezoneIANA   = 'America/Chicago',
       DefaultParams         = N'{"marketplaceId":"ATVPDKIKX0DER"}',
       DefaultExecutionMode  = 'NODE_NATIVE',
       DefaultJobType        = 'INGEST',
       DefaultIsActive       = 1,
       AutoCreateOnNewBrand  = 1,
       UpdatedAt             = SYSUTCDATETIME()
  FROM admin.Endpoints e
  JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
 WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_LISTING_RANK_SNAPSHOT';

-- Show the result
SELECT e.Name, e.DefaultCronExpression, e.DefaultParams, e.AutoCreateOnNewBrand
  FROM admin.Endpoints e
  JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
 WHERE c.Name = 'AMAZON_SP_API'
   AND e.Name IN ('AMZ_FINANCIAL_EVENTS','AMZ_LISTINGS_READ','AMZ_LISTING_RANK_SNAPSHOT')
 ORDER BY e.Name;

PRINT '031 seed complete: Amazon ingestion endpoints have defaults + auto-create flag.';
GO
