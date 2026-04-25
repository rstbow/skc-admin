/* =============================================================================
   034 — Amazon Orders + Order Items ingestion (proc + curated views).

   Run against: vs-ims-staging

   Two-table model (tables already exist from earlier scaffolding):
     raw.amz_orders        — order header (one row per AmazonOrderID)
     raw.amz_order_items   — line items   (one row per OrderID + OrderItemID)

   Two pull strategies the runner supports:
     1. Recent sales — CreatedAfter window. Drives the daily/weekly
        incremental — captures every new order placed in the window.
     2. Historical updates — LastUpdatedAfter window. Catches status
        flips (Pending → Shipped, Shipped → Canceled), partial-ship
        events. Run less frequently or as backfill.

   Merge logic on UPDATE: only replace tgt if src.LastUpdatedDate is
   newer (or equal). Prevents an out-of-order CreatedAfter pull from
   overwriting fresher LastUpdate-driven data.

   Existing column names (kept as-is):
     amz_orders:       MerchantOrderID, LastUpdatedDate, OrderTotal, Currency
     amz_order_items:  SKU, ProductName, Quantity, ItemPrice, ItemTax,
                       ShippingPrice, ShippingTax, ItemPromotionDiscount,
                       ShipPromotionDiscount, GiftWrapPrice, GiftWrapTax

   Idempotent.
   ============================================================================= */

SET NOCOUNT ON;
GO

/* ---------- 1. Add helpful indexes if missing ---------- */

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_amz_orders_brand_purchasedate'
      AND object_id = OBJECT_ID('raw.amz_orders')
)
BEGIN
    CREATE INDEX IX_amz_orders_brand_purchasedate
        ON raw.amz_orders (_BrandUID, PurchaseDate DESC)
        INCLUDE (OrderStatus, FulfillmentChannel, OrderTotal, NumberOfItemsShipped, MarketplaceID);
    PRINT 'Created IX_amz_orders_brand_purchasedate.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_amz_orders_brand_lastupdated'
      AND object_id = OBJECT_ID('raw.amz_orders')
)
BEGIN
    CREATE INDEX IX_amz_orders_brand_lastupdated
        ON raw.amz_orders (_BrandUID, LastUpdatedDate DESC);
    PRINT 'Created IX_amz_orders_brand_lastupdated.';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_amz_order_items_brand_sku'
      AND object_id = OBJECT_ID('raw.amz_order_items')
)
BEGIN
    CREATE INDEX IX_amz_order_items_brand_sku
        ON raw.amz_order_items (_BrandUID, SKU)
        INCLUDE (AmazonOrderID, Quantity, QuantityShipped, ItemPrice);
    PRINT 'Created IX_amz_order_items_brand_sku.';
END
GO


/* ---------- 2. Merge proc for orders (header) — LastUpdate-aware ---------- */

CREATE OR ALTER PROCEDURE raw.usp_merge_amz_orders
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
            j.AmazonOrderID, j.MerchantOrderID,
            TRY_CAST(j.PurchaseDate AS DATETIMEOFFSET)    AS PurchaseDate,
            TRY_CAST(j.LastUpdatedDate AS DATETIMEOFFSET) AS LastUpdatedDate,
            j.OrderStatus, j.FulfillmentChannel, j.SalesChannel, j.OrderChannel, j.ShipServiceLevel,
            TRY_CAST(j.OrderTotal AS DECIMAL(18,4))   AS OrderTotal,
            j.Currency,
            TRY_CAST(j.NumberOfItemsShipped AS INT)   AS NumberOfItemsShipped,
            TRY_CAST(j.NumberOfItemsUnshipped AS INT) AS NumberOfItemsUnshipped,
            j.MarketplaceID, j.MarketplaceName,
            j.PaymentMethod, j.BuyerEmail,
            j.ShipCity, j.ShipState, j.ShipPostalCode, j.ShipCountryCode,
            CASE WHEN j.IsBusinessOrder    = N'true' THEN CAST(1 AS BIT)
                 WHEN j.IsBusinessOrder    = N'false' THEN CAST(0 AS BIT) ELSE NULL END AS IsBusinessOrder,
            CASE WHEN j.IsPrime            = N'true' THEN CAST(1 AS BIT)
                 WHEN j.IsPrime            = N'false' THEN CAST(0 AS BIT) ELSE NULL END AS IsPrime,
            CASE WHEN j.IsReplacementOrder = N'true' THEN CAST(1 AS BIT)
                 WHEN j.IsReplacementOrder = N'false' THEN CAST(0 AS BIT) ELSE NULL END AS IsReplacementOrder,
            CASE WHEN j.IsSnS              = N'true' THEN CAST(1 AS BIT)
                 WHEN j.IsSnS              = N'false' THEN CAST(0 AS BIT) ELSE NULL END AS IsSnS,
            j._RawPayload,
            CONVERT(VARBINARY(32), j._SourceRowHashHex, 2) AS _SourceRowHash,
            ROW_NUMBER() OVER (
                PARTITION BY j.AmazonOrderID
                -- Newest LastUpdatedDate wins inside the same batch
                ORDER BY TRY_CAST(j.LastUpdatedDate AS DATETIMEOFFSET) DESC
            ) AS _rn
        FROM OPENJSON(@RowsJson)
            WITH (
                AmazonOrderID          NVARCHAR(50)   '$.AmazonOrderID',
                MerchantOrderID        NVARCHAR(50)   '$.MerchantOrderID',
                PurchaseDate           NVARCHAR(40)   '$.PurchaseDate',
                LastUpdatedDate        NVARCHAR(40)   '$.LastUpdatedDate',
                OrderStatus            NVARCHAR(30)   '$.OrderStatus',
                FulfillmentChannel     NVARCHAR(10)   '$.FulfillmentChannel',
                SalesChannel           NVARCHAR(50)   '$.SalesChannel',
                OrderChannel           NVARCHAR(50)   '$.OrderChannel',
                ShipServiceLevel       NVARCHAR(50)   '$.ShipServiceLevel',
                OrderTotal             NVARCHAR(20)   '$.OrderTotal',
                Currency               NVARCHAR(3)    '$.Currency',
                NumberOfItemsShipped   NVARCHAR(20)   '$.NumberOfItemsShipped',
                NumberOfItemsUnshipped NVARCHAR(20)   '$.NumberOfItemsUnshipped',
                MarketplaceID          NVARCHAR(20)   '$.MarketplaceID',
                MarketplaceName        NVARCHAR(50)   '$.MarketplaceName',
                PaymentMethod          NVARCHAR(30)   '$.PaymentMethod',
                BuyerEmail             NVARCHAR(320)  '$.BuyerEmail',
                ShipCity               NVARCHAR(100)  '$.ShipCity',
                ShipState              NVARCHAR(50)   '$.ShipState',
                ShipPostalCode         NVARCHAR(20)   '$.ShipPostalCode',
                ShipCountryCode        NVARCHAR(2)    '$.ShipCountryCode',
                IsBusinessOrder        NVARCHAR(10)   '$.IsBusinessOrder',
                IsPrime                NVARCHAR(10)   '$.IsPrime',
                IsReplacementOrder     NVARCHAR(10)   '$.IsReplacementOrder',
                IsSnS                  NVARCHAR(10)   '$.IsSnS',
                _RawPayload            NVARCHAR(MAX)  '$._RawPayload',
                _SourceRowHashHex      NVARCHAR(64)   '$._SourceRowHashHex'
            ) AS j
    )
    MERGE raw.amz_orders WITH (HOLDLOCK) AS tgt
    USING (
        SELECT @BrandUID AS _BrandUID, @SourceRunID AS _SourceRunID, *
        FROM parsed WHERE _rn = 1
    ) AS src
        ON  tgt._BrandUID     = src._BrandUID
        AND tgt.AmazonOrderID = src.AmazonOrderID

    -- Only update when source is genuinely newer (LastUpdatedDate guard).
    -- Prevents an out-of-order CreatedAfter pull (which contains older
    -- LastUpdatedDate values) from overwriting fresher LastUpdate-driven
    -- data the previous run already wrote.
    WHEN MATCHED AND (
        tgt._SourceRowHash IS NULL
        OR tgt._SourceRowHash <> src._SourceRowHash
    ) AND ISNULL(src.LastUpdatedDate, src.PurchaseDate)
        >= ISNULL(tgt.LastUpdatedDate, tgt.PurchaseDate) THEN
        UPDATE SET
            MerchantOrderID        = ISNULL(src.MerchantOrderID, tgt.MerchantOrderID),
            PurchaseDate           = src.PurchaseDate,
            LastUpdatedDate        = src.LastUpdatedDate,
            OrderStatus            = src.OrderStatus,
            FulfillmentChannel     = src.FulfillmentChannel,
            SalesChannel           = src.SalesChannel,
            OrderChannel           = src.OrderChannel,
            ShipServiceLevel       = src.ShipServiceLevel,
            OrderTotal             = src.OrderTotal,
            Currency               = src.Currency,
            NumberOfItemsShipped   = src.NumberOfItemsShipped,
            NumberOfItemsUnshipped = src.NumberOfItemsUnshipped,
            MarketplaceID          = ISNULL(src.MarketplaceID, tgt.MarketplaceID),
            MarketplaceName        = ISNULL(src.MarketplaceName, tgt.MarketplaceName),
            PaymentMethod          = ISNULL(src.PaymentMethod, tgt.PaymentMethod),
            BuyerEmail             = ISNULL(src.BuyerEmail, tgt.BuyerEmail),
            ShipCity               = ISNULL(src.ShipCity, tgt.ShipCity),
            ShipState              = ISNULL(src.ShipState, tgt.ShipState),
            ShipPostalCode         = ISNULL(src.ShipPostalCode, tgt.ShipPostalCode),
            ShipCountryCode        = ISNULL(src.ShipCountryCode, tgt.ShipCountryCode),
            IsBusinessOrder        = src.IsBusinessOrder,
            IsPrime                = src.IsPrime,
            IsReplacementOrder     = src.IsReplacementOrder,
            IsSnS                  = src.IsSnS,
            _RawPayload            = src._RawPayload,
            _IngestedAt            = SYSUTCDATETIME(),
            _SourceRunID           = src._SourceRunID,
            _SourceRowHash         = src._SourceRowHash

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (_BrandUID, AmazonOrderID, MerchantOrderID,
                MarketplaceID, MarketplaceName,
                PurchaseDate, LastUpdatedDate, OrderStatus,
                FulfillmentChannel, SalesChannel, OrderChannel, ShipServiceLevel,
                Currency, OrderTotal,
                NumberOfItemsShipped, NumberOfItemsUnshipped,
                PaymentMethod, BuyerEmail,
                ShipCity, ShipState, ShipPostalCode, ShipCountryCode,
                IsBusinessOrder, IsReplacementOrder, IsPrime, IsSnS,
                _RawPayload, _SourceRunID, _SourceRowHash)
        VALUES (src._BrandUID, src.AmazonOrderID, src.MerchantOrderID,
                src.MarketplaceID, src.MarketplaceName,
                src.PurchaseDate, src.LastUpdatedDate, src.OrderStatus,
                src.FulfillmentChannel, src.SalesChannel, src.OrderChannel, src.ShipServiceLevel,
                src.Currency, src.OrderTotal,
                src.NumberOfItemsShipped, src.NumberOfItemsUnshipped,
                src.PaymentMethod, src.BuyerEmail,
                src.ShipCity, src.ShipState, src.ShipPostalCode, src.ShipCountryCode,
                src.IsBusinessOrder, src.IsReplacementOrder, src.IsPrime, src.IsSnS,
                src._RawPayload, src._SourceRunID, src._SourceRowHash)

    OUTPUT $action INTO @Actions(Action);

    DECLARE @Inserted INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'INSERT');
    DECLARE @Updated  INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'UPDATE');
    DECLARE @Total    INT = (SELECT COUNT(*) FROM OPENJSON(@RowsJson));

    SELECT Inserted = @Inserted, Updated = @Updated,
           Unchanged = @Total - (@Inserted + @Updated), Total = @Total;
END
GO


/* ---------- 3. Merge proc for order_items ---------- */

CREATE OR ALTER PROCEDURE raw.usp_merge_amz_order_items
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
            j.AmazonOrderID, j.OrderItemID, j.ASIN, j.SKU, j.ProductName,
            TRY_CAST(j.Quantity        AS INT) AS Quantity,
            TRY_CAST(j.QuantityShipped AS INT) AS QuantityShipped,
            j.ConditionID, j.Currency,
            TRY_CAST(j.ItemPrice              AS DECIMAL(18,4)) AS ItemPrice,
            TRY_CAST(j.ItemTax                AS DECIMAL(18,4)) AS ItemTax,
            TRY_CAST(j.ShippingPrice          AS DECIMAL(18,4)) AS ShippingPrice,
            TRY_CAST(j.ShippingTax            AS DECIMAL(18,4)) AS ShippingTax,
            TRY_CAST(j.GiftWrapPrice          AS DECIMAL(18,4)) AS GiftWrapPrice,
            TRY_CAST(j.GiftWrapTax            AS DECIMAL(18,4)) AS GiftWrapTax,
            TRY_CAST(j.ItemPromotionDiscount  AS DECIMAL(18,4)) AS ItemPromotionDiscount,
            TRY_CAST(j.ShipPromotionDiscount  AS DECIMAL(18,4)) AS ShipPromotionDiscount,
            j.PromotionIDs, j._RawPayload,
            CONVERT(VARBINARY(32), j._SourceRowHashHex, 2) AS _SourceRowHash,
            ROW_NUMBER() OVER (
                PARTITION BY j.AmazonOrderID, j.OrderItemID ORDER BY (SELECT 1)
            ) AS _rn
        FROM OPENJSON(@RowsJson)
            WITH (
                AmazonOrderID         NVARCHAR(50)   '$.AmazonOrderID',
                OrderItemID           NVARCHAR(50)   '$.OrderItemID',
                ASIN                  NVARCHAR(20)   '$.ASIN',
                SKU                   NVARCHAR(200)  '$.SKU',
                ProductName           NVARCHAR(500)  '$.ProductName',
                Quantity              NVARCHAR(20)   '$.Quantity',
                QuantityShipped       NVARCHAR(20)   '$.QuantityShipped',
                ConditionID           NVARCHAR(30)   '$.ConditionID',
                Currency              NVARCHAR(3)    '$.Currency',
                ItemPrice             NVARCHAR(20)   '$.ItemPrice',
                ItemTax               NVARCHAR(20)   '$.ItemTax',
                ShippingPrice         NVARCHAR(20)   '$.ShippingPrice',
                ShippingTax           NVARCHAR(20)   '$.ShippingTax',
                GiftWrapPrice         NVARCHAR(20)   '$.GiftWrapPrice',
                GiftWrapTax           NVARCHAR(20)   '$.GiftWrapTax',
                ItemPromotionDiscount NVARCHAR(20)   '$.ItemPromotionDiscount',
                ShipPromotionDiscount NVARCHAR(20)   '$.ShipPromotionDiscount',
                PromotionIDs          NVARCHAR(500)  '$.PromotionIDs',
                _RawPayload           NVARCHAR(MAX)  '$._RawPayload',
                _SourceRowHashHex     NVARCHAR(64)   '$._SourceRowHashHex'
            ) AS j
    )
    MERGE raw.amz_order_items WITH (HOLDLOCK) AS tgt
    USING (
        SELECT @BrandUID AS _BrandUID, @SourceRunID AS _SourceRunID, *
        FROM parsed WHERE _rn = 1
    ) AS src
        ON  tgt._BrandUID     = src._BrandUID
        AND tgt.AmazonOrderID = src.AmazonOrderID
        AND tgt.OrderItemID   = src.OrderItemID

    WHEN MATCHED AND (tgt._SourceRowHash IS NULL OR tgt._SourceRowHash <> src._SourceRowHash) THEN
        UPDATE SET
            ASIN                  = src.ASIN,
            SKU                   = src.SKU,
            ProductName           = src.ProductName,
            Quantity              = src.Quantity,
            QuantityShipped       = src.QuantityShipped,
            ConditionID           = src.ConditionID,
            Currency              = src.Currency,
            ItemPrice             = src.ItemPrice,
            ItemTax               = src.ItemTax,
            ShippingPrice         = src.ShippingPrice,
            ShippingTax           = src.ShippingTax,
            GiftWrapPrice         = src.GiftWrapPrice,
            GiftWrapTax           = src.GiftWrapTax,
            ItemPromotionDiscount = src.ItemPromotionDiscount,
            ShipPromotionDiscount = src.ShipPromotionDiscount,
            PromotionIDs          = src.PromotionIDs,
            _RawPayload           = src._RawPayload,
            _IngestedAt           = SYSUTCDATETIME(),
            _SourceRunID          = src._SourceRunID,
            _SourceRowHash        = src._SourceRowHash

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (_BrandUID, AmazonOrderID, OrderItemID, SKU, ASIN, ProductName,
                Quantity, QuantityShipped, Currency,
                ItemPrice, ItemTax, ShippingPrice, ShippingTax,
                GiftWrapPrice, GiftWrapTax,
                ItemPromotionDiscount, ShipPromotionDiscount,
                PromotionIDs, ConditionID,
                _RawPayload, _SourceRunID, _SourceRowHash)
        VALUES (src._BrandUID, src.AmazonOrderID, src.OrderItemID, src.SKU, src.ASIN, src.ProductName,
                src.Quantity, src.QuantityShipped, src.Currency,
                src.ItemPrice, src.ItemTax, src.ShippingPrice, src.ShippingTax,
                src.GiftWrapPrice, src.GiftWrapTax,
                src.ItemPromotionDiscount, src.ShipPromotionDiscount,
                src.PromotionIDs, src.ConditionID,
                src._RawPayload, src._SourceRunID, src._SourceRowHash)

    OUTPUT $action INTO @Actions(Action);

    DECLARE @Inserted INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'INSERT');
    DECLARE @Updated  INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'UPDATE');
    DECLARE @Total    INT = (SELECT COUNT(*) FROM OPENJSON(@RowsJson));

    SELECT Inserted = @Inserted, Updated = @Updated,
           Unchanged = @Total - (@Inserted + @Updated), Total = @Total;
END
GO


/* ---------- 4. Curated views ---------- */

CREATE OR ALTER VIEW curated.amz_orders AS
SELECT
    _BrandUID, AmazonOrderID, MerchantOrderID,
    PurchaseDate, LastUpdatedDate, OrderStatus,
    FulfillmentChannel, SalesChannel, OrderChannel, ShipServiceLevel,
    OrderTotal, Currency,
    NumberOfItemsShipped, NumberOfItemsUnshipped,
    MarketplaceID, MarketplaceName,
    PaymentMethod,
    ShipCity, ShipState, ShipPostalCode, ShipCountryCode,
    IsBusinessOrder, IsPrime, IsReplacementOrder, IsSnS,
    _IngestedAt
FROM raw.amz_orders;
GO

CREATE OR ALTER VIEW curated.amz_order_items AS
/*
    SKU-level order detail joined to the order header so consumers
    get PurchaseDate / status / fulfillment in a single view.
*/
SELECT
    oi._BrandUID,
    oi.AmazonOrderID, oi.OrderItemID,
    oi.ASIN, oi.SKU, oi.ProductName,
    oi.Quantity, oi.QuantityShipped,
    oi.Currency,
    oi.ItemPrice, oi.ItemTax,
    oi.ShippingPrice, oi.ShippingTax,
    oi.GiftWrapPrice, oi.GiftWrapTax,
    oi.ItemPromotionDiscount, oi.ShipPromotionDiscount,
    oi.PromotionIDs, oi.ConditionID,
    oi._IngestedAt,
    o.PurchaseDate, o.LastUpdatedDate, o.OrderStatus,
    o.FulfillmentChannel, o.SalesChannel, o.MarketplaceID
FROM raw.amz_order_items oi
LEFT JOIN raw.amz_orders o
       ON o._BrandUID = oi._BrandUID AND o.AmazonOrderID = oi.AmazonOrderID;
GO

CREATE OR ALTER VIEW curated.amz_sales_daily AS
/*
    Daily SKU-level sales rollup using buyer's PurchaseDate (NOT
    settlement). Excludes Canceled / Pending. This is the "what did I
    sell today" answer for app2.

    For settlement-side accounting, use curated.amz_fees instead.
*/
SELECT
    o._BrandUID,
    CAST(o.PurchaseDate AS date) AS SalesDay,
    o.MarketplaceID,
    oi.SKU,
    oi.ASIN,
    COUNT(DISTINCT o.AmazonOrderID)            AS Orders,
    SUM(ISNULL(oi.Quantity, 0))                AS UnitsOrdered,
    SUM(ISNULL(oi.QuantityShipped, 0))         AS UnitsShipped,
    SUM(CASE WHEN oi.Currency = N'USD'
             THEN ISNULL(oi.ItemPrice, 0) ELSE 0 END) AS GrossUsd,
    oi.Currency
FROM raw.amz_orders o
JOIN raw.amz_order_items oi
  ON oi._BrandUID = o._BrandUID AND oi.AmazonOrderID = o.AmazonOrderID
WHERE o.OrderStatus NOT IN (N'Canceled', N'Pending')
GROUP BY o._BrandUID, CAST(o.PurchaseDate AS date),
         o.MarketplaceID, oi.SKU, oi.ASIN, oi.Currency;
GO


/* ---------- 5. Grants (best-effort) ---------- */

IF DATABASE_PRINCIPAL_ID('skc_app_user') IS NOT NULL
BEGIN
    BEGIN TRY
        GRANT SELECT  ON curated.amz_orders             TO skc_app_user;
        GRANT SELECT  ON curated.amz_order_items        TO skc_app_user;
        GRANT SELECT  ON curated.amz_sales_daily        TO skc_app_user;
        GRANT EXECUTE ON raw.usp_merge_amz_orders       TO skc_app_user;
        GRANT EXECUTE ON raw.usp_merge_amz_order_items  TO skc_app_user;
        PRINT 'Granted SELECT on amz_orders + items views + EXECUTE on procs to skc_app_user.';
    END TRY
    BEGIN CATCH
        PRINT 'GRANT failed (no GRANT OPTION) — user must run manually.';
    END CATCH
END
GO

PRINT '034 complete: Amazon orders + order_items DDL ready.';
GO
