/* =============================================================================
   026 — Amazon listing rank tracking (daily snapshot + delta views).

   Run against: vs-ims-staging

   Feeds app2's rank-tracking feature (see
   tasks/~037-amazon-rank-tracking.md). Each day the rank runner pulls
   SP-API Catalog Items → salesRanks per ASIN, writes one row per
   (brand × SKU × category × day).

   Tables:
     raw.amz_listing_rank                — daily snapshot, append-style
                                            with unique key so same-day
                                            re-runs UPDATE not duplicate.

   Proc:
     raw.usp_merge_amz_listing_rank      — JSON-based bulk MERGE. Hash
                                            gated so unchanged rank
                                            doesn't rewrite the row.

   Views (for app2):
     curated.amz_listing_rank            — flat feed
     curated.amz_listing_rank_delta      — current + 1d/7d/28d lookback

   Separate seed file (026_seed_endpoint_and_jobs.sql) for the
   admin.Endpoints + admin.Jobs rows — those need INSERT on admin which
   is DML-only, user runs in SSMS.

   Idempotent. CREATE OR ALTER where applicable.
   ============================================================================= */

SET NOCOUNT ON;
GO

/* ---------- 1. raw.amz_listing_rank table ---------- */

IF OBJECT_ID('raw.amz_listing_rank', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_listing_rank (
        RankID            BIGINT IDENTITY(1,1) NOT NULL,
        _BrandUID         UNIQUEIDENTIFIER      NOT NULL,
        SKU               NVARCHAR(200)         NOT NULL,
        ASIN              NVARCHAR(20)          NOT NULL,
        MarketplaceID     NVARCHAR(20)          NOT NULL,
        SnapshotDate      DATE                  NOT NULL,
        CategoryType      NVARCHAR(30)          NOT NULL, -- 'PRIMARY' | 'CLASSIFICATION'
        CategoryTitle     NVARCHAR(200)         NULL,
        CategoryID        NVARCHAR(100)         NOT NULL, -- classificationId or displayGroup slug; NOT NULL so it's a real key component
        [Rank]            INT                   NULL,     -- can be null (deranked)
        SourceLink        NVARCHAR(500)         NULL,
        _IngestedAt       DATETIME2(3)          NOT NULL CONSTRAINT DF_rank_Ingested DEFAULT SYSUTCDATETIME(),
        _SourceRunID      BIGINT                NULL,
        _SourceRowHash    VARBINARY(32)         NULL,
        CONSTRAINT PK_amz_listing_rank PRIMARY KEY CLUSTERED
            (_BrandUID, SKU, CategoryID, SnapshotDate),
        CONSTRAINT CK_amz_listing_rank_CategoryType
            CHECK (CategoryType IN ('PRIMARY','CLASSIFICATION'))
    );

    CREATE INDEX IX_rank_brand_sku_date
        ON raw.amz_listing_rank (_BrandUID, SKU, SnapshotDate DESC)
        INCLUDE (CategoryID, CategoryTitle, CategoryType, [Rank]);

    PRINT 'Created raw.amz_listing_rank + indexes.';
END
ELSE
    PRINT 'raw.amz_listing_rank already exists.';
GO


/* ---------- 2. Bulk MERGE proc ---------- */

CREATE OR ALTER PROCEDURE raw.usp_merge_amz_listing_rank
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
            j.SKU, j.ASIN, j.MarketplaceID,
            TRY_CAST(j.SnapshotDate AS DATE)     AS SnapshotDate,
            j.CategoryType, j.CategoryTitle, j.CategoryID,
            TRY_CAST(j.[Rank] AS INT)            AS [Rank],
            j.SourceLink,
            CONVERT(VARBINARY(32), j._SourceRowHashHex, 2) AS _SourceRowHash,
            ROW_NUMBER() OVER (
                PARTITION BY j.SKU, j.CategoryID, j.SnapshotDate
                ORDER BY (SELECT 1)
            ) AS _rn
        FROM OPENJSON(@RowsJson)
            WITH (
                SKU              NVARCHAR(200)  '$.SKU',
                ASIN             NVARCHAR(20)   '$.ASIN',
                MarketplaceID    NVARCHAR(20)   '$.MarketplaceID',
                SnapshotDate     NVARCHAR(10)   '$.SnapshotDate',       -- 'YYYY-MM-DD'
                CategoryType     NVARCHAR(30)   '$.CategoryType',
                CategoryTitle    NVARCHAR(200)  '$.CategoryTitle',
                CategoryID       NVARCHAR(100)  '$.CategoryID',
                [Rank]           NVARCHAR(20)   '$.Rank',
                SourceLink       NVARCHAR(500)  '$.SourceLink',
                _SourceRowHashHex NVARCHAR(64)  '$._SourceRowHashHex'
            ) AS j
    )
    MERGE raw.amz_listing_rank WITH (HOLDLOCK) AS tgt
    USING (
        SELECT @BrandUID AS _BrandUID, @SourceRunID AS _SourceRunID, *
        FROM parsed WHERE _rn = 1
    ) AS src
        ON  tgt._BrandUID    = src._BrandUID
        AND tgt.SKU          = src.SKU
        AND tgt.CategoryID   = src.CategoryID
        AND tgt.SnapshotDate = src.SnapshotDate

    WHEN MATCHED AND (tgt._SourceRowHash IS NULL OR tgt._SourceRowHash <> src._SourceRowHash) THEN
        UPDATE SET
            ASIN           = src.ASIN,
            MarketplaceID  = src.MarketplaceID,
            CategoryType   = src.CategoryType,
            CategoryTitle  = src.CategoryTitle,
            [Rank]         = src.[Rank],
            SourceLink     = src.SourceLink,
            _IngestedAt    = SYSUTCDATETIME(),
            _SourceRunID   = src._SourceRunID,
            _SourceRowHash = src._SourceRowHash

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (_BrandUID, SKU, ASIN, MarketplaceID, SnapshotDate,
                CategoryType, CategoryTitle, CategoryID, [Rank],
                SourceLink, _SourceRunID, _SourceRowHash)
        VALUES (src._BrandUID, src.SKU, src.ASIN, src.MarketplaceID, src.SnapshotDate,
                src.CategoryType, src.CategoryTitle, src.CategoryID, src.[Rank],
                src.SourceLink, src._SourceRunID, src._SourceRowHash)

    OUTPUT $action INTO @Actions(Action);

    DECLARE @Inserted INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'INSERT');
    DECLARE @Updated  INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'UPDATE');
    DECLARE @Total    INT = (SELECT COUNT(*) FROM OPENJSON(@RowsJson));

    SELECT Inserted = @Inserted, Updated = @Updated,
           Unchanged = @Total - (@Inserted + @Updated), Total = @Total;
END
GO


/* ---------- 3. Curated views for skc-api ---------- */

CREATE OR ALTER VIEW curated.amz_listing_rank AS
/*
    Flat per-brand feed of rank snapshots. One row per (SKU, category,
    day). CategoryType 'PRIMARY' = top-level BSR (e.g. "Home & Kitchen").
    CategoryType 'CLASSIFICATION' = narrow sub-cat (e.g. "Kids' Cooking
    Aprons") — more granular, usually lower numbers.
*/
SELECT
    _BrandUID,
    SKU,
    ASIN,
    MarketplaceID,
    SnapshotDate,
    CategoryType,
    CategoryTitle,
    CategoryID,
    [Rank],
    SourceLink,
    _IngestedAt
FROM raw.amz_listing_rank;
GO


CREATE OR ALTER VIEW curated.amz_listing_rank_delta AS
/*
    For each (brand, SKU, category), the latest rank snapshot + what
    rank was 1/7/28 days before. Drives the "rank 1427 ↓+35" chip in
    app2's Listing Ledger.

    app2 filters on CategoryType if they want primary-only vs all-
    categories. Currently returns both; caller decides.

    If the correlated subquery becomes expensive at scale (>10 brands
    × 500 SKUs × 5 categories × 90 days = ~2M rows), we'll swap to a
    materialized rollup written at ingest time.
*/
WITH latest AS (
    SELECT r.*,
           ROW_NUMBER() OVER (
               PARTITION BY _BrandUID, SKU, CategoryID
               ORDER BY SnapshotDate DESC
           ) AS rn
    FROM raw.amz_listing_rank r
)
SELECT
    l._BrandUID, l.SKU, l.ASIN, l.MarketplaceID,
    l.CategoryType, l.CategoryTitle, l.CategoryID,
    l.[Rank]          AS CurrentRank,
    l.SnapshotDate    AS CurrentDate,
    (SELECT TOP 1 [Rank] FROM raw.amz_listing_rank
      WHERE _BrandUID = l._BrandUID AND SKU = l.SKU AND CategoryID = l.CategoryID
        AND SnapshotDate <= DATEADD(DAY, -1,  l.SnapshotDate)
      ORDER BY SnapshotDate DESC) AS Rank1d,
    (SELECT TOP 1 [Rank] FROM raw.amz_listing_rank
      WHERE _BrandUID = l._BrandUID AND SKU = l.SKU AND CategoryID = l.CategoryID
        AND SnapshotDate <= DATEADD(DAY, -7,  l.SnapshotDate)
      ORDER BY SnapshotDate DESC) AS Rank7d,
    (SELECT TOP 1 [Rank] FROM raw.amz_listing_rank
      WHERE _BrandUID = l._BrandUID AND SKU = l.SKU AND CategoryID = l.CategoryID
        AND SnapshotDate <= DATEADD(DAY, -28, l.SnapshotDate)
      ORDER BY SnapshotDate DESC) AS Rank28d,
    l.SourceLink
FROM latest l
WHERE l.rn = 1;
GO


/* ---------- 4. Grant attempts (will be no-ops if Claude lacks GRANT OPTION) ---------- */

IF DATABASE_PRINCIPAL_ID('skc_app_user') IS NOT NULL
BEGIN
    BEGIN TRY
        GRANT SELECT  ON curated.amz_listing_rank       TO skc_app_user;
        GRANT SELECT  ON curated.amz_listing_rank_delta TO skc_app_user;
        GRANT EXECUTE ON raw.usp_merge_amz_listing_rank TO skc_app_user;
        PRINT 'Granted rank view SELECTs + proc EXECUTE to skc_app_user.';
    END TRY
    BEGIN CATCH
        PRINT 'GRANT failed (Claude lacks GRANT OPTION) — user must run GRANT block manually: ' + ERROR_MESSAGE();
    END CATCH
END
GO

PRINT '--------------------------------------------------';
PRINT '026 complete: rank tracking tables + views ready.';
PRINT '--------------------------------------------------';
GO
