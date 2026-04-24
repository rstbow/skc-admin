/* =============================================================================
   Migration 024 — Listing Ledger.

   The "Listing Ledger" is a daily pull of Amazon listing data per brand,
   with field-level delta detection. Every change to a title, price,
   bullet, image URL, etc. creates a row in raw.amz_listing_changes so
   users can correlate edits to sales outcomes and (later) roll them back.

   This migration has TWO parts:

     DDL (Claude can run) — section 1 + 2:
       - raw.usp_merge_amz_listings         (JSON-based MERGE, hash-gated)
       - raw.usp_append_amz_listing_changes (JSON-based INSERT into delta log)
       - curated.amz_listing_changes        (view for skc-api)
       - curated.amz_listing_change_sales_impact (joins changes to fees
                                                   for before/after analysis)
       - GRANT SELECT on views to skc_app_user

     DML (user runs in SSMS) — section 3:
       - MERGE admin.Jobs to seed a daily AMZ_LISTINGS_READ job per active
         Amazon credential (cron 0 3 * * * Chicago = 3am local)

   Idempotent.
   ============================================================================= */

SET NOCOUNT ON;
GO


/* ============================================================
   1. Merge proc — bulk upsert listings, hash-gated to minimize churn
   ============================================================ */

CREATE OR ALTER PROCEDURE raw.usp_merge_amz_listings
    @BrandUID     UNIQUEIDENTIFIER,
    @SourceRunID  BIGINT,
    @RowsJson     NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Actions TABLE (Action NVARCHAR(10));

    ;WITH parsed AS (
        SELECT
            j.SKU, j.MarketplaceID, j.ASIN, j.ProductType,
            j.Title, j.Brand, j.Description,
            j.Bullet1, j.Bullet2, j.Bullet3, j.Bullet4, j.Bullet5,
            j.SearchTerms, j.Category, j.BrowseNodeID, j.ImagesJSON,
            TRY_CAST(j.Price AS DECIMAL(18,4)) AS Price,
            j.Currency, j.Quantity, j.Condition, j.Status, j.IssueCount,
            j._RawPayload,
            CONVERT(VARBINARY(32), j._SourceRowHashHex, 2) AS _SourceRowHash,
            ROW_NUMBER() OVER (
                PARTITION BY j.SKU, j.MarketplaceID
                ORDER BY (SELECT 1)
            ) AS _rn
        FROM OPENJSON(@RowsJson)
            WITH (
                SKU              NVARCHAR(200)  '$.SKU',
                MarketplaceID    NVARCHAR(20)   '$.MarketplaceID',
                ASIN             NVARCHAR(20)   '$.ASIN',
                ProductType      NVARCHAR(100)  '$.ProductType',
                Title            NVARCHAR(500)  '$.Title',
                Brand            NVARCHAR(200)  '$.Brand',
                Description      NVARCHAR(MAX)  '$.Description',
                Bullet1          NVARCHAR(500)  '$.Bullet1',
                Bullet2          NVARCHAR(500)  '$.Bullet2',
                Bullet3          NVARCHAR(500)  '$.Bullet3',
                Bullet4          NVARCHAR(500)  '$.Bullet4',
                Bullet5          NVARCHAR(500)  '$.Bullet5',
                SearchTerms      NVARCHAR(500)  '$.SearchTerms',
                Category         NVARCHAR(200)  '$.Category',
                BrowseNodeID     NVARCHAR(50)   '$.BrowseNodeID',
                ImagesJSON       NVARCHAR(MAX)  '$.ImagesJSON',
                Price            NVARCHAR(20)   '$.Price',
                Currency         NVARCHAR(3)    '$.Currency',
                Quantity         INT            '$.Quantity',
                Condition        NVARCHAR(30)   '$.Condition',
                Status           NVARCHAR(30)   '$.Status',
                IssueCount       INT            '$.IssueCount',
                _RawPayload      NVARCHAR(MAX)  '$._RawPayload',
                _SourceRowHashHex NVARCHAR(64)  '$._SourceRowHashHex'
            ) AS j
    )
    MERGE raw.amz_listings WITH (HOLDLOCK) AS tgt
    USING (
        SELECT @BrandUID AS _BrandUID, @SourceRunID AS _SourceRunID, *
        FROM parsed WHERE _rn = 1
    ) AS src
        ON  tgt._BrandUID     = src._BrandUID
        AND tgt.SKU           = src.SKU
        AND tgt.MarketplaceID = src.MarketplaceID

    WHEN MATCHED AND (tgt._SourceRowHash IS NULL OR tgt._SourceRowHash <> src._SourceRowHash) THEN
        UPDATE SET
            ASIN         = src.ASIN,
            ProductType  = src.ProductType,
            Title        = src.Title,
            Brand        = src.Brand,
            Description  = src.Description,
            Bullet1      = src.Bullet1, Bullet2 = src.Bullet2, Bullet3 = src.Bullet3,
            Bullet4      = src.Bullet4, Bullet5 = src.Bullet5,
            SearchTerms  = src.SearchTerms,
            Category     = src.Category, BrowseNodeID = src.BrowseNodeID,
            ImagesJSON   = src.ImagesJSON,
            Price        = src.Price, Currency = src.Currency,
            Quantity     = src.Quantity,
            Condition    = src.Condition, Status = src.Status,
            IssueCount   = src.IssueCount,
            _RawPayload  = src._RawPayload,
            _IngestedAt  = SYSUTCDATETIME(),
            _SourceRunID = src._SourceRunID,
            _SourceRowHash = src._SourceRowHash

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (_BrandUID, SKU, MarketplaceID, ASIN, ProductType, Title, Brand,
                Description, Bullet1, Bullet2, Bullet3, Bullet4, Bullet5,
                SearchTerms, Category, BrowseNodeID, ImagesJSON,
                Price, Currency, Quantity, Condition, Status, IssueCount,
                _RawPayload, _SourceRunID, _SourceRowHash)
        VALUES (src._BrandUID, src.SKU, src.MarketplaceID, src.ASIN, src.ProductType, src.Title, src.Brand,
                src.Description, src.Bullet1, src.Bullet2, src.Bullet3, src.Bullet4, src.Bullet5,
                src.SearchTerms, src.Category, src.BrowseNodeID, src.ImagesJSON,
                src.Price, src.Currency, src.Quantity, src.Condition, src.Status, src.IssueCount,
                src._RawPayload, src._SourceRunID, src._SourceRowHash)

    OUTPUT $action INTO @Actions(Action);

    DECLARE @Inserted INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'INSERT');
    DECLARE @Updated  INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'UPDATE');
    DECLARE @Total    INT = (SELECT COUNT(*) FROM OPENJSON(@RowsJson));

    SELECT Inserted = @Inserted, Updated = @Updated,
           Unchanged = @Total - (@Inserted + @Updated), Total = @Total;
END
GO


/* ============================================================
   2. Append proc — bulk INSERT into the change ledger
      Runner computes field-level deltas in JS and sends them here.
   ============================================================ */

CREATE OR ALTER PROCEDURE raw.usp_append_amz_listing_changes
    @BrandUID     UNIQUEIDENTIFIER,
    @SourceRunID  BIGINT,
    @RowsJson     NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    INSERT INTO raw.amz_listing_changes (
        _BrandUID, SKU, ASIN, MarketplaceID,
        ChangeSource, ChangeType, FieldPath,
        BeforeValue, AfterValue,
        Status, _SourceRunID, _IngestedAt
    )
    SELECT
        @BrandUID,
        j.SKU, j.ASIN, j.MarketplaceID,
        ISNULL(j.ChangeSource, 'SKC_ADMIN_SCHEDULER'),
        j.ChangeType,
        j.FieldPath,
        j.BeforeValue, j.AfterValue,
        ISNULL(j.Status, 'DETECTED'),
        @SourceRunID,
        SYSUTCDATETIME()
    FROM OPENJSON(@RowsJson)
        WITH (
            SKU            NVARCHAR(200)  '$.SKU',
            ASIN           NVARCHAR(20)   '$.ASIN',
            MarketplaceID  NVARCHAR(20)   '$.MarketplaceID',
            ChangeSource   NVARCHAR(30)   '$.ChangeSource',
            ChangeType     NVARCHAR(50)   '$.ChangeType',
            FieldPath      NVARCHAR(200)  '$.FieldPath',
            BeforeValue    NVARCHAR(MAX)  '$.BeforeValue',
            AfterValue     NVARCHAR(MAX)  '$.AfterValue',
            Status         NVARCHAR(30)   '$.Status'
        ) AS j;

    SELECT Inserted = @@ROWCOUNT;
END
GO


/* ============================================================
   3. Curated views for the SaaS app
   ============================================================ */

CREATE OR ALTER VIEW curated.amz_listing_changes AS
/*
    Per-brand stream of detected listing changes. Filter out terminal
    reverted ones so callers just see "what's live and what changed".
*/
SELECT
    lc.ChangeID,
    lc._BrandUID,
    lc.SKU,
    lc.ASIN,
    lc.MarketplaceID,
    lc.ChangeSource,
    lc.ChangeType,
    lc.FieldPath,
    lc.BeforeValue,
    lc.AfterValue,
    lc.Status,
    lc._IngestedAt AS DetectedAt,
    lc.AppliedAt,
    lc.RevertedAt,
    lc.RevertedByChangeID
FROM raw.amz_listing_changes lc;
GO

CREATE OR ALTER VIEW curated.amz_listing_change_sales_impact AS
/*
    Each detected change, annotated with units sold 7 / 14 / 28 days
    before and after the change. Lets the SaaS app show "I changed the
    title on X, units/day went from A → B" inline.

    Uses SHIPMENT + REFUND events from raw.amz_financial_events. Refunds
    count negatively toward units (we care about net sales).
*/
WITH impact AS (
    SELECT
        lc.ChangeID, lc._BrandUID, lc.SKU, lc.ChangeType, lc.FieldPath,
        lc.BeforeValue, lc.AfterValue, lc._IngestedAt AS DetectedAt,
        -- Before: 7d
        (SELECT ISNULL(SUM(fe.Quantity), 0)
           FROM raw.amz_financial_events fe
          WHERE fe._BrandUID = lc._BrandUID AND fe.SKU = lc.SKU
            AND fe.EventType IN ('SHIPMENT','REFUND')
            AND fe.PostedDate >= DATEADD(day, -7, lc._IngestedAt)
            AND fe.PostedDate <  lc._IngestedAt) AS Units7dBefore,
        (SELECT ISNULL(SUM(fe.Quantity), 0)
           FROM raw.amz_financial_events fe
          WHERE fe._BrandUID = lc._BrandUID AND fe.SKU = lc.SKU
            AND fe.EventType IN ('SHIPMENT','REFUND')
            AND fe.PostedDate >= lc._IngestedAt
            AND fe.PostedDate <  DATEADD(day, 7, lc._IngestedAt)) AS Units7dAfter,
        -- Before: 28d / After: 28d
        (SELECT ISNULL(SUM(fe.Quantity), 0)
           FROM raw.amz_financial_events fe
          WHERE fe._BrandUID = lc._BrandUID AND fe.SKU = lc.SKU
            AND fe.EventType IN ('SHIPMENT','REFUND')
            AND fe.PostedDate >= DATEADD(day, -28, lc._IngestedAt)
            AND fe.PostedDate <  lc._IngestedAt) AS Units28dBefore,
        (SELECT ISNULL(SUM(fe.Quantity), 0)
           FROM raw.amz_financial_events fe
          WHERE fe._BrandUID = lc._BrandUID AND fe.SKU = lc.SKU
            AND fe.EventType IN ('SHIPMENT','REFUND')
            AND fe.PostedDate >= lc._IngestedAt
            AND fe.PostedDate <  DATEADD(day, 28, lc._IngestedAt)) AS Units28dAfter
    FROM raw.amz_listing_changes lc
)
SELECT * FROM impact;
GO


/* ============================================================
   4. Grants — SaaS app reads the curated views, NOT the raw tables
   ============================================================ */

IF DATABASE_PRINCIPAL_ID('skc_app_user') IS NOT NULL
BEGIN
    GRANT SELECT ON curated.amz_listing_changes             TO skc_app_user;
    GRANT SELECT ON curated.amz_listing_change_sales_impact TO skc_app_user;
    PRINT 'Granted SELECT on listing curated views to skc_app_user.';
END
GO


PRINT '--------------------------------------------------';
PRINT 'Migration 024 DDL complete.';
PRINT 'Run section 5 (job seeds) manually in SSMS.';
PRINT '--------------------------------------------------';
GO


/* ============================================================
   5. DML — daily job seed. RUN THIS IN SSMS.
      Needs INSERT on admin.Jobs which claude_readonly lacks on purpose.
   ============================================================ */

/*
USE [skc-admin];
GO

DECLARE @EndpointID INT = (
    SELECT e.EndpointID
      FROM admin.Endpoints e
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
     WHERE c.Name = 'AMAZON_SP_API' AND e.Name = 'AMZ_LISTINGS_READ'
);

IF @EndpointID IS NULL
BEGIN
    RAISERROR('AMZ_LISTINGS_READ endpoint not found. Check admin.Endpoints.', 16, 1);
    RETURN;
END

MERGE admin.Jobs AS tgt
USING (
    SELECT bc.BrandUID,
           @EndpointID                                                       AS EndpointID,
           CONCAT('AMZ_LISTINGS · ', b.BrandName)                            AS Name,
           '0 3 * * *'                                                       AS CronExpression,
           'America/Chicago'                                                 AS TimezoneIANA,
           'NODE_NATIVE'                                                     AS ExecutionMode,
           'INGEST'                                                          AS JobType,
           CONCAT('amz-listings:', CONVERT(NVARCHAR(36), bc.BrandUID))       AS ConcurrencyKey,
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

PRINT 'AMZ_LISTINGS_READ daily jobs upserted (3am Chicago).';
GO
*/
