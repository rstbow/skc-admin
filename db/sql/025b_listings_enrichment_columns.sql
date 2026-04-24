/* =============================================================================
   025b — add enrichment columns to raw.amz_listings + update merge proc.

   Run against: vs-ims-staging

   Adds two columns populated by the Listings Items API enrichment:
     - SalesRank          INT  — current Best Sellers Rank at the category
     - SalesRankCategory  NVARCHAR(200)  — human-readable category name

   SalesRank is intentionally NOT included in the delta hash (see
   TRACKED_FIELDS in amazonListingsRunner.js). It moves hourly; tracking
   it as a delta would make raw.amz_listing_changes unreadable. Instead
   we surface current-only via raw.amz_listings / curated.amz_listings,
   and a future daily-rank-snapshot table can track its history cheaply
   if someone wants the curve.

   Also updates raw.usp_merge_amz_listings to accept these new columns
   from the JSON payload. CREATE OR ALTER preserves the EXECUTE grant.

   Idempotent.
   ============================================================================= */

SET NOCOUNT ON;
GO

IF COL_LENGTH('raw.amz_listings', 'SalesRank') IS NULL
BEGIN
    ALTER TABLE raw.amz_listings ADD SalesRank INT NULL;
    PRINT 'Added raw.amz_listings.SalesRank';
END
GO

IF COL_LENGTH('raw.amz_listings', 'SalesRankCategory') IS NULL
BEGIN
    ALTER TABLE raw.amz_listings ADD SalesRankCategory NVARCHAR(200) NULL;
    PRINT 'Added raw.amz_listings.SalesRankCategory';
END
GO


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
            j.SalesRank, j.SalesRankCategory,
            j._RawPayload,
            CONVERT(VARBINARY(32), j._SourceRowHashHex, 2) AS _SourceRowHash,
            ROW_NUMBER() OVER (
                PARTITION BY j.SKU, j.MarketplaceID ORDER BY (SELECT 1)
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
                SalesRank        INT            '$.SalesRank',
                SalesRankCategory NVARCHAR(200) '$.SalesRankCategory',
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

    -- One WHEN MATCHED branch (SQL Server only allows one per MERGE
    -- UPDATE). Trigger if EITHER the tracked-fields hash changed OR the
    -- sales rank shifted. Either way we update all fields — cheap, and
    -- keeps current-state consistent. The scheduler's in-memory diff
    -- (see TRACKED_FIELDS in amazonListingsRunner.js) decides which
    -- ledger change rows to emit, so rank-only days are a silent UPDATE
    -- here with no corresponding raw.amz_listing_changes row.
    WHEN MATCHED AND (
        tgt._SourceRowHash IS NULL
        OR tgt._SourceRowHash <> src._SourceRowHash
        OR ISNULL(tgt.SalesRank, -1)             <> ISNULL(src.SalesRank, -1)
        OR ISNULL(tgt.SalesRankCategory, N'')    <> ISNULL(src.SalesRankCategory, N'')
    ) THEN
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
            SalesRank    = src.SalesRank,
            SalesRankCategory = src.SalesRankCategory,
            _RawPayload  = src._RawPayload,
            _IngestedAt  = SYSUTCDATETIME(),
            _SourceRunID = src._SourceRunID,
            _SourceRowHash = src._SourceRowHash

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (_BrandUID, SKU, MarketplaceID, ASIN, ProductType, Title, Brand,
                Description, Bullet1, Bullet2, Bullet3, Bullet4, Bullet5,
                SearchTerms, Category, BrowseNodeID, ImagesJSON,
                Price, Currency, Quantity, Condition, Status, IssueCount,
                SalesRank, SalesRankCategory,
                _RawPayload, _SourceRunID, _SourceRowHash)
        VALUES (src._BrandUID, src.SKU, src.MarketplaceID, src.ASIN, src.ProductType, src.Title, src.Brand,
                src.Description, src.Bullet1, src.Bullet2, src.Bullet3, src.Bullet4, src.Bullet5,
                src.SearchTerms, src.Category, src.BrowseNodeID, src.ImagesJSON,
                src.Price, src.Currency, src.Quantity, src.Condition, src.Status, src.IssueCount,
                src.SalesRank, src.SalesRankCategory,
                src._RawPayload, src._SourceRunID, src._SourceRowHash)

    OUTPUT $action INTO @Actions(Action);

    DECLARE @Inserted INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'INSERT');
    DECLARE @Updated  INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'UPDATE');
    DECLARE @Total    INT = (SELECT COUNT(*) FROM OPENJSON(@RowsJson));

    SELECT Inserted = @Inserted, Updated = @Updated,
           Unchanged = @Total - (@Inserted + @Updated), Total = @Total;
END
GO

PRINT '025b applied: SalesRank + SalesRankCategory + merge proc updated.';
GO
