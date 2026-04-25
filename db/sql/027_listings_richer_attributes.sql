/* =============================================================================
   027 — extend raw.amz_listings with sale price + ensure curated view
   exposes all enriched fields. Bullets/brand/etc. columns already exist;
   the gap is sale-price + curated view + grants.

   Run against: vs-ims-staging

   Adds:
     raw.amz_listings.SalePrice          DECIMAL(18,4)   — current sale
                                                          price when an
                                                          active offer
                                                          schedule is live
     raw.amz_listings.SalePriceStart     DATETIMEOFFSET  — sale window start
     raw.amz_listings.SalePriceEnd       DATETIMEOFFSET  — sale window end

   And:
     - CREATE OR ALTER raw.usp_merge_amz_listings   to accept new fields
     - CREATE OR ALTER curated.amz_listings         to expose them
     - GRANT SELECT on curated.amz_listings         to skc_app_user

   Idempotent.
   ============================================================================= */

SET NOCOUNT ON;
GO

IF COL_LENGTH('raw.amz_listings', 'SalePrice') IS NULL
BEGIN
    ALTER TABLE raw.amz_listings ADD SalePrice DECIMAL(18,4) NULL;
    PRINT 'Added raw.amz_listings.SalePrice';
END
GO

IF COL_LENGTH('raw.amz_listings', 'SalePriceStart') IS NULL
BEGIN
    ALTER TABLE raw.amz_listings ADD SalePriceStart DATETIMEOFFSET NULL;
    PRINT 'Added raw.amz_listings.SalePriceStart';
END
GO

IF COL_LENGTH('raw.amz_listings', 'SalePriceEnd') IS NULL
BEGIN
    ALTER TABLE raw.amz_listings ADD SalePriceEnd DATETIMEOFFSET NULL;
    PRINT 'Added raw.amz_listings.SalePriceEnd';
END
GO


/* ---------- merge proc accepts new fields ---------- */

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
            TRY_CAST(j.Price AS DECIMAL(18,4))     AS Price,
            j.Currency, j.Quantity, j.Condition, j.Status, j.IssueCount,
            j.SalesRank, j.SalesRankCategory,
            TRY_CAST(j.SalePrice AS DECIMAL(18,4)) AS SalePrice,
            TRY_CAST(j.SalePriceStart AS DATETIMEOFFSET) AS SalePriceStart,
            TRY_CAST(j.SalePriceEnd   AS DATETIMEOFFSET) AS SalePriceEnd,
            j._RawPayload,
            CONVERT(VARBINARY(32), j._SourceRowHashHex, 2) AS _SourceRowHash,
            ROW_NUMBER() OVER (
                PARTITION BY j.SKU, j.MarketplaceID ORDER BY (SELECT 1)
            ) AS _rn
        FROM OPENJSON(@RowsJson)
            WITH (
                SKU              NVARCHAR(200)   '$.SKU',
                MarketplaceID    NVARCHAR(20)    '$.MarketplaceID',
                ASIN             NVARCHAR(20)    '$.ASIN',
                ProductType      NVARCHAR(100)   '$.ProductType',
                Title            NVARCHAR(500)   '$.Title',
                Brand            NVARCHAR(200)   '$.Brand',
                Description      NVARCHAR(MAX)   '$.Description',
                Bullet1          NVARCHAR(500)   '$.Bullet1',
                Bullet2          NVARCHAR(500)   '$.Bullet2',
                Bullet3          NVARCHAR(500)   '$.Bullet3',
                Bullet4          NVARCHAR(500)   '$.Bullet4',
                Bullet5          NVARCHAR(500)   '$.Bullet5',
                SearchTerms      NVARCHAR(500)   '$.SearchTerms',
                Category         NVARCHAR(200)   '$.Category',
                BrowseNodeID     NVARCHAR(50)    '$.BrowseNodeID',
                ImagesJSON       NVARCHAR(MAX)   '$.ImagesJSON',
                Price            NVARCHAR(20)    '$.Price',
                Currency         NVARCHAR(3)     '$.Currency',
                Quantity         INT             '$.Quantity',
                Condition        NVARCHAR(30)    '$.Condition',
                Status           NVARCHAR(30)    '$.Status',
                IssueCount       INT             '$.IssueCount',
                SalesRank        INT             '$.SalesRank',
                SalesRankCategory NVARCHAR(200)  '$.SalesRankCategory',
                SalePrice        NVARCHAR(20)    '$.SalePrice',
                SalePriceStart   NVARCHAR(40)    '$.SalePriceStart',
                SalePriceEnd     NVARCHAR(40)    '$.SalePriceEnd',
                _RawPayload      NVARCHAR(MAX)   '$._RawPayload',
                _SourceRowHashHex NVARCHAR(64)   '$._SourceRowHashHex'
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

    WHEN MATCHED AND (
        tgt._SourceRowHash IS NULL
        OR tgt._SourceRowHash <> src._SourceRowHash
        OR ISNULL(tgt.SalesRank, -1)             <> ISNULL(src.SalesRank, -1)
        OR ISNULL(tgt.SalesRankCategory, N'')    <> ISNULL(src.SalesRankCategory, N'')
        OR ISNULL(tgt.SalePrice, -1)             <> ISNULL(src.SalePrice, -1)
        OR ISNULL(tgt.SalePriceStart, '1900-01-01 +00:00') <> ISNULL(src.SalePriceStart, '1900-01-01 +00:00')
        OR ISNULL(tgt.SalePriceEnd,   '1900-01-01 +00:00') <> ISNULL(src.SalePriceEnd,   '1900-01-01 +00:00')
    ) THEN
        UPDATE SET
            ASIN              = ISNULL(src.ASIN, tgt.ASIN),
            ProductType       = ISNULL(src.ProductType, tgt.ProductType),
            Title             = ISNULL(src.Title, tgt.Title),
            Brand             = ISNULL(src.Brand, tgt.Brand),
            Description       = ISNULL(src.Description, tgt.Description),
            Bullet1           = ISNULL(src.Bullet1, tgt.Bullet1),
            Bullet2           = ISNULL(src.Bullet2, tgt.Bullet2),
            Bullet3           = ISNULL(src.Bullet3, tgt.Bullet3),
            Bullet4           = ISNULL(src.Bullet4, tgt.Bullet4),
            Bullet5           = ISNULL(src.Bullet5, tgt.Bullet5),
            SearchTerms       = ISNULL(src.SearchTerms, tgt.SearchTerms),
            Category          = ISNULL(src.Category, tgt.Category),
            BrowseNodeID      = ISNULL(src.BrowseNodeID, tgt.BrowseNodeID),
            ImagesJSON        = ISNULL(src.ImagesJSON, tgt.ImagesJSON),
            Price             = ISNULL(src.Price, tgt.Price),
            Currency          = ISNULL(src.Currency, tgt.Currency),
            Quantity          = ISNULL(src.Quantity, tgt.Quantity),
            Condition         = ISNULL(src.Condition, tgt.Condition),
            Status            = ISNULL(src.Status, tgt.Status),
            IssueCount        = ISNULL(src.IssueCount, tgt.IssueCount),
            SalesRank         = src.SalesRank,
            SalesRankCategory = src.SalesRankCategory,
            SalePrice         = src.SalePrice,
            SalePriceStart    = src.SalePriceStart,
            SalePriceEnd      = src.SalePriceEnd,
            _RawPayload       = ISNULL(src._RawPayload, tgt._RawPayload),
            _IngestedAt       = SYSUTCDATETIME(),
            _SourceRunID      = src._SourceRunID,
            _SourceRowHash    = src._SourceRowHash

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (_BrandUID, SKU, MarketplaceID, ASIN, ProductType, Title, Brand,
                Description, Bullet1, Bullet2, Bullet3, Bullet4, Bullet5,
                SearchTerms, Category, BrowseNodeID, ImagesJSON,
                Price, Currency, Quantity, Condition, Status, IssueCount,
                SalesRank, SalesRankCategory,
                SalePrice, SalePriceStart, SalePriceEnd,
                _RawPayload, _SourceRunID, _SourceRowHash)
        VALUES (src._BrandUID, src.SKU, src.MarketplaceID, src.ASIN, src.ProductType, src.Title, src.Brand,
                src.Description, src.Bullet1, src.Bullet2, src.Bullet3, src.Bullet4, src.Bullet5,
                src.SearchTerms, src.Category, src.BrowseNodeID, src.ImagesJSON,
                src.Price, src.Currency, src.Quantity, src.Condition, src.Status, src.IssueCount,
                src.SalesRank, src.SalesRankCategory,
                src.SalePrice, src.SalePriceStart, src.SalePriceEnd,
                src._RawPayload, src._SourceRunID, src._SourceRowHash)

    OUTPUT $action INTO @Actions(Action);

    DECLARE @Inserted INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'INSERT');
    DECLARE @Updated  INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'UPDATE');
    DECLARE @Total    INT = (SELECT COUNT(*) FROM OPENJSON(@RowsJson));

    SELECT Inserted = @Inserted, Updated = @Updated,
           Unchanged = @Total - (@Inserted + @Updated), Total = @Total;
END
GO


/* ---------- curated view ---------- */

CREATE OR ALTER VIEW curated.amz_listings AS
/*
    Per-brand current snapshot of each Amazon listing. Refreshed daily:
      - flat-file report fills Title / Description / Price / Quantity / Status
        at 3am Chicago via AMZ_LISTINGS_READ
      - Catalog Items API fills Brand / Bullets / Images / SearchTerms /
        Category / SalesRank / SalePrice* at 4am Chicago via
        AMZ_LISTING_RANK_SNAPSHOT (same call that snapshots rank)
*/
SELECT
    _BrandUID,
    SKU,
    MarketplaceID,
    ASIN,
    Title,
    Brand,
    ProductType,
    Category,
    BrowseNodeID,
    Description,
    Bullet1, Bullet2, Bullet3, Bullet4, Bullet5,
    SearchTerms,
    ImagesJSON,
    Price,
    Currency,
    SalePrice,
    SalePriceStart,
    SalePriceEnd,
    Quantity,
    Condition,
    Status,
    IssueCount,
    SalesRank,
    SalesRankCategory,
    _IngestedAt AS LastUpdated
FROM raw.amz_listings;
GO


/* ---------- Grant attempt — no-op if Claude lacks GRANT OPTION ---------- */

IF DATABASE_PRINCIPAL_ID('skc_app_user') IS NOT NULL
BEGIN
    BEGIN TRY
        GRANT SELECT ON curated.amz_listings TO skc_app_user;
        PRINT 'Granted SELECT on curated.amz_listings to skc_app_user.';
    END TRY
    BEGIN CATCH
        PRINT 'GRANT failed (no GRANT OPTION) — user must run manually: ' + ERROR_MESSAGE();
    END CATCH
END
GO

PRINT '027 applied: SalePrice trio + curated view updated.';
GO
