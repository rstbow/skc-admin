/* =============================================================================
   033 — Amazon FBA Returns ingestion (proc + curated views).

   Run against: vs-ims-staging

   Source: GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA (flat-file report).
   Daily pull of last 30 days. Returns trickle in for ~30 days after
   the original sale, so the rolling window catches everything.

   raw.amz_returns table already exists from earlier scaffolding work
   (column names: OrderID, ReturnDate DATE, FulfillmentCenter,
   DetailedDisposition, Reason). This migration just adds the proc +
   views + grants — keeping the column names the existing table uses.

   Idempotent.
   ============================================================================= */

SET NOCOUNT ON;
GO

/* Add helpful indexes if missing (existing table is heap on PK only) */

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_amz_returns_brand_returndate'
      AND object_id = OBJECT_ID('raw.amz_returns')
)
BEGIN
    CREATE INDEX IX_amz_returns_brand_returndate
        ON raw.amz_returns (_BrandUID, ReturnDate DESC)
        INCLUDE (SKU, ASIN, Quantity, DetailedDisposition, Reason);
    PRINT 'Created IX_amz_returns_brand_returndate.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_amz_returns_brand_sku'
      AND object_id = OBJECT_ID('raw.amz_returns')
)
BEGIN
    CREATE INDEX IX_amz_returns_brand_sku
        ON raw.amz_returns (_BrandUID, SKU, ReturnDate DESC);
    PRINT 'Created IX_amz_returns_brand_sku.';
END
GO


/* ---------- Bulk MERGE proc ---------- */

CREATE OR ALTER PROCEDURE raw.usp_merge_amz_returns
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
            j.OrderID, j.SKU, j.ASIN, j.FNSKU, j.ProductName,
            TRY_CAST(j.ReturnDate AS DATE)         AS ReturnDate,
            TRY_CAST(j.Quantity AS INT)            AS Quantity,
            j.FulfillmentCenter, j.DetailedDisposition, j.Reason,
            j.Status, j.LicensePlateNumber, j.CustomerComments,
            j._RawPayload,
            CONVERT(VARBINARY(32), j._SourceRowHashHex, 2) AS _SourceRowHash,
            ROW_NUMBER() OVER (
                PARTITION BY j.OrderID, j.SKU, j.ReturnDate
                ORDER BY (SELECT 1)
            ) AS _rn
        FROM OPENJSON(@RowsJson)
            WITH (
                OrderID             NVARCHAR(50)   '$.OrderID',
                SKU                 NVARCHAR(200)  '$.SKU',
                ASIN                NVARCHAR(20)   '$.ASIN',
                FNSKU               NVARCHAR(50)   '$.FNSKU',
                ProductName         NVARCHAR(500)  '$.ProductName',
                ReturnDate          NVARCHAR(20)   '$.ReturnDate',
                Quantity            NVARCHAR(20)   '$.Quantity',
                FulfillmentCenter   NVARCHAR(10)   '$.FulfillmentCenter',
                DetailedDisposition NVARCHAR(100)  '$.DetailedDisposition',
                Reason              NVARCHAR(200)  '$.Reason',
                Status              NVARCHAR(50)   '$.Status',
                LicensePlateNumber  NVARCHAR(100)  '$.LicensePlateNumber',
                CustomerComments    NVARCHAR(MAX)  '$.CustomerComments',
                _RawPayload         NVARCHAR(MAX)  '$._RawPayload',
                _SourceRowHashHex   NVARCHAR(64)   '$._SourceRowHashHex'
            ) AS j
    )
    MERGE raw.amz_returns WITH (HOLDLOCK) AS tgt
    USING (
        SELECT @BrandUID AS _BrandUID, @SourceRunID AS _SourceRunID, *
        FROM parsed WHERE _rn = 1
    ) AS src
        ON  tgt._BrandUID  = src._BrandUID
        AND tgt.OrderID    = src.OrderID
        AND tgt.SKU        = src.SKU
        AND tgt.ReturnDate = src.ReturnDate

    WHEN MATCHED AND (tgt._SourceRowHash IS NULL OR tgt._SourceRowHash <> src._SourceRowHash) THEN
        UPDATE SET
            ASIN                = src.ASIN,
            FNSKU               = src.FNSKU,
            ProductName         = src.ProductName,
            Quantity            = src.Quantity,
            FulfillmentCenter   = src.FulfillmentCenter,
            DetailedDisposition = src.DetailedDisposition,
            Reason              = src.Reason,
            Status              = src.Status,
            LicensePlateNumber  = src.LicensePlateNumber,
            CustomerComments    = src.CustomerComments,
            _RawPayload         = src._RawPayload,
            _IngestedAt         = SYSUTCDATETIME(),
            _SourceRunID        = src._SourceRunID,
            _SourceRowHash      = src._SourceRowHash

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (_BrandUID, OrderID, SKU, ASIN, FNSKU, ProductName,
                ReturnDate, Quantity, FulfillmentCenter, DetailedDisposition,
                Reason, Status, LicensePlateNumber, CustomerComments,
                _RawPayload, _SourceRunID, _SourceRowHash, _IngestedAt)
        VALUES (src._BrandUID, src.OrderID, src.SKU, src.ASIN, src.FNSKU, src.ProductName,
                src.ReturnDate, src.Quantity, src.FulfillmentCenter, src.DetailedDisposition,
                src.Reason, src.Status, src.LicensePlateNumber, src.CustomerComments,
                src._RawPayload, src._SourceRunID, src._SourceRowHash, SYSUTCDATETIME())

    OUTPUT $action INTO @Actions(Action);

    DECLARE @Inserted INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'INSERT');
    DECLARE @Updated  INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'UPDATE');
    DECLARE @Total    INT = (SELECT COUNT(*) FROM OPENJSON(@RowsJson));

    SELECT Inserted = @Inserted, Updated = @Updated,
           Unchanged = @Total - (@Inserted + @Updated), Total = @Total;
END
GO


/* ---------- Curated views ---------- */

CREATE OR ALTER VIEW curated.amz_returns AS
SELECT
    _BrandUID, OrderID, SKU, ASIN, FNSKU, ProductName,
    ReturnDate, Quantity,
    FulfillmentCenter, DetailedDisposition, Reason, Status,
    LicensePlateNumber, CustomerComments,
    _IngestedAt
FROM raw.amz_returns;
GO

CREATE OR ALTER VIEW curated.amz_returns_summary AS
/*
    Per-SKU rollups. Pair with curated.amz_fees on (BrandUID, SKU)
    to compute return-rate = ReturnedUnits / UnitsSold.
*/
SELECT
    _BrandUID, SKU, ASIN,
    COUNT(*) AS ReturnEvents,
    SUM(ISNULL(Quantity, 1)) AS ReturnedUnits,
    SUM(CASE WHEN ReturnDate >= DATEADD(day, -30,  CAST(SYSUTCDATETIME() AS date))
             THEN ISNULL(Quantity, 1) ELSE 0 END) AS ReturnedUnits30d,
    SUM(CASE WHEN ReturnDate >= DATEADD(day, -90,  CAST(SYSUTCDATETIME() AS date))
             THEN ISNULL(Quantity, 1) ELSE 0 END) AS ReturnedUnits90d,
    SUM(CASE WHEN ReturnDate >= DATEADD(day, -365, CAST(SYSUTCDATETIME() AS date))
             THEN ISNULL(Quantity, 1) ELSE 0 END) AS ReturnedUnits365d,
    (SELECT TOP 1 r2.Reason
     FROM raw.amz_returns r2
     WHERE r2._BrandUID = r._BrandUID AND r2.SKU = r.SKU
       AND r2.ReturnDate >= DATEADD(day, -90, CAST(SYSUTCDATETIME() AS date))
       AND r2.Reason IS NOT NULL
     GROUP BY r2.Reason
     ORDER BY COUNT(*) DESC) AS TopReason90d,
    MAX(ReturnDate) AS LastReturnAt
FROM raw.amz_returns r
GROUP BY _BrandUID, SKU, ASIN;
GO


/* ---------- Grants (best-effort) ---------- */

IF DATABASE_PRINCIPAL_ID('skc_app_user') IS NOT NULL
BEGIN
    BEGIN TRY
        GRANT SELECT  ON curated.amz_returns         TO skc_app_user;
        GRANT SELECT  ON curated.amz_returns_summary TO skc_app_user;
        GRANT EXECUTE ON raw.usp_merge_amz_returns   TO skc_app_user;
        PRINT 'Granted SELECT on amz_returns views + EXECUTE on proc to skc_app_user.';
    END TRY
    BEGIN CATCH
        PRINT 'GRANT failed (no GRANT OPTION) — user must run manually.';
    END CATCH
END
GO

PRINT '033 complete: AMZ FBA returns DDL ready.';
GO
