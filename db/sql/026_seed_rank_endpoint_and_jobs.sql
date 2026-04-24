/* =============================================================================
   026 seed — DML for rank tracking. RUN IN SSMS against skc-admin.

   Adds:
     - admin.Endpoints row 'AMZ_LISTING_RANK_SNAPSHOT' under AMAZON_SP_API
     - One NODE_NATIVE recurring job per active Amazon credential
       (cron '0 4 * * *' America/Chicago — 4am, an hour after the
        listings job so rank has fresh raw.amz_listings to read from)

   Also grants SELECT on the curated rank views to skc_app_user, since
   Claude can't do that via CREATE-only grants.

   Idempotent (MERGE + IF NOT EXISTS).
   ============================================================================= */

USE [skc-admin];
GO

SET NOCOUNT ON;
GO

/* ---------- 1. Register the endpoint ---------- */

DECLARE @ConnectorID INT = (SELECT ConnectorID FROM admin.Connectors WHERE Name = 'AMAZON_SP_API');
IF @ConnectorID IS NULL
BEGIN
    RAISERROR('AMAZON_SP_API connector not found. Check admin.Connectors.', 16, 1);
    RETURN;
END

IF NOT EXISTS (SELECT 1 FROM admin.Endpoints WHERE Name = 'AMZ_LISTING_RANK_SNAPSHOT')
BEGIN
    INSERT INTO admin.Endpoints (
        ConnectorID, Name, DisplayName, Description, EndpointType,
        TargetSchema, TargetTable, NaturalKeyColumns, ParamsTemplate, Notes
    ) VALUES (
        @ConnectorID,
        'AMZ_LISTING_RANK_SNAPSHOT',
        'Amazon Catalog Items — rank snapshot',
        N'Daily per-ASIN pull from Catalog Items API. Stores one row per '
          + N'(brand × SKU × category × day) in raw.amz_listing_rank. Used '
          + N'by the app2 Listing Ledger for rank history charts and delta '
          + N'chips. See lib/amazonListingRankRunner.js.',
        'REST_GET',
        'raw',
        'amz_listing_rank',
        'BrandUID,SKU,CategoryID,SnapshotDate',
        N'{"marketplaceId":"ATVPDKIKX0DER"}',
        N'Reads ASINs from raw.amz_listings (populated by AMZ_LISTINGS_READ) '
          + N'— run that one first to seed ASINs for new brands. Rate limit: '
          + N'Catalog Items is 2 rps, runner stays serial at 500ms.'
    );
    PRINT 'Registered endpoint AMZ_LISTING_RANK_SNAPSHOT.';
END
ELSE
    PRINT 'Endpoint AMZ_LISTING_RANK_SNAPSHOT already exists, skipping.';
GO


/* ---------- 2. Seed a daily job per active Amazon credential ---------- */

DECLARE @EndpointID INT = (
    SELECT e.EndpointID
      FROM admin.Endpoints e
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
     WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_LISTING_RANK_SNAPSHOT'
);

MERGE admin.Jobs AS tgt
USING (
    SELECT bc.BrandUID,
           @EndpointID                                                       AS EndpointID,
           CONCAT('AMZ_LISTING_RANK · ', b.BrandName)                        AS Name,
           '0 4 * * *'                                                       AS CronExpression,
           'America/Chicago'                                                 AS TimezoneIANA,
           'NODE_NATIVE'                                                     AS ExecutionMode,
           'INGEST'                                                          AS JobType,
           CONCAT('amz-rank:', CONVERT(NVARCHAR(36), bc.BrandUID))           AS ConcurrencyKey,
           N'{"marketplaceId":"ATVPDKIKX0DER"}'                              AS Params,
           CAST(1 AS BIT)                                                    AS IsActive,
           40                                                                AS Priority
      FROM admin.BrandCredentials bc
      JOIN admin.Connectors  c ON c.ConnectorID = bc.ConnectorID
      JOIN admin.Brands      b ON b.BrandUID    = bc.BrandUID
     WHERE c.Name = 'AMAZON_SP_API' AND bc.IsActive = 1 AND b.IsActive = 1
) AS src
ON tgt.EndpointID = src.EndpointID AND tgt.BrandUID = src.BrandUID
   AND tgt.JobType = src.JobType AND tgt.ExecutionMode = src.ExecutionMode
WHEN MATCHED THEN
    UPDATE SET Name = src.Name, CronExpression = src.CronExpression,
               TimezoneIANA = src.TimezoneIANA, ConcurrencyKey = src.ConcurrencyKey,
               Params = src.Params, UpdatedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (Name, EndpointID, BrandUID, CronExpression, TimezoneIANA,
            ExecutionMode, JobType, ConcurrencyKey, Params, IsActive, Priority)
    VALUES (src.Name, src.EndpointID, src.BrandUID, src.CronExpression,
            src.TimezoneIANA, src.ExecutionMode, src.JobType,
            src.ConcurrencyKey, src.Params, src.IsActive, src.Priority);

PRINT 'AMZ_LISTING_RANK_SNAPSHOT daily jobs upserted (4am Chicago).';
GO


/* ---------- 3. Grants on curated rank views (vs-ims-staging) ---------- */
/* Claude's DDL role can create views but not grant SELECT to skc_app_user.
   Run this in vs-ims-staging. */

/*
USE [vs-ims-staging];
GO
GRANT SELECT ON curated.amz_listing_rank       TO skc_app_user;
GRANT SELECT ON curated.amz_listing_rank_delta TO skc_app_user;
GRANT EXECUTE ON raw.usp_merge_amz_listing_rank TO skc_app_user;
PRINT 'Rank view SELECTs + proc EXECUTE granted to skc_app_user.';
GO
*/

PRINT '--------------------------------------------------';
PRINT '026 seed complete: endpoint + jobs ready.';
PRINT 'Don''t forget the GRANT block (uncomment, run in vs-ims-staging).';
PRINT '--------------------------------------------------';
GO
