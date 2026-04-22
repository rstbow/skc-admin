/* =============================================================================
   Phase 3 — Amazon data tables for the shared brand data database.

   Run against: vs-ims-staging   (the shared brand data DB)
   NOT against: skc-admin

   Why here:
     - skc-admin holds "how the system works" (connectors, endpoints, runs, creds)
     - the shared brand data DB holds "business data" (orders, fees, listings, ...)
     - When brands get per-brand DBs later, we re-run this against each one.
       The _BrandUID leading-PK design makes that migration a no-op at query time.

   Conventions in every table:
     * _BrandUID          first column + leading in clustered PK
     * _IngestedAt        UTC timestamp of when we wrote this row
     * _SourceRunID       FK-style to admin.JobRuns (cross-DB; no enforced FK)
     * _SourceRowHash     SHA2_256 over normalized source row — powers
                          hash-gated MERGE (only update when data changed)
     * _RawPayload        original JSON/TSV line, preserves fidelity even if
                          schema evolves before we parse new fields
     * NO deletes         Amazon source data is append-only; readers never
                          block writers once RCSI is enabled (below)

   ONE-TIME: enable Read Committed Snapshot Isolation so readers get a
   consistent snapshot without being blocked by writer MERGEs. This is
   the single most important knob for high-write + high-read workloads.

   Idempotent: safe to re-run.
   ============================================================================= */

SET NOCOUNT ON;
GO

/* =========================================================================
   0. RCSI — flip once per database
   ========================================================================= */
DECLARE @dbName SYSNAME = DB_NAME();
DECLARE @rcsi BIT = (SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = @dbName);
IF @rcsi = 0
BEGIN
    DECLARE @sql NVARCHAR(200) = N'ALTER DATABASE [' + @dbName + N'] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK AFTER 10 SECONDS;';
    EXEC sp_executesql @sql;
    PRINT 'Enabled READ_COMMITTED_SNAPSHOT on ' + @dbName + '.';
END
ELSE
    PRINT 'READ_COMMITTED_SNAPSHOT already ON on ' + @dbName + '.';
GO

/* =========================================================================
   1. raw schema
   ========================================================================= */
IF SCHEMA_ID('raw') IS NULL
    EXEC('CREATE SCHEMA raw AUTHORIZATION dbo;');
GO
PRINT 'Schema raw ready.';
GO

/* =========================================================================
   2. raw.amz_financial_events — the pilot
   ========================================================================= */
IF OBJECT_ID('raw.amz_financial_events', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_financial_events (
        _BrandUID         UNIQUEIDENTIFIER NOT NULL,
        EventType         NVARCHAR(50)     NOT NULL,   -- SHIPMENT / REFUND / SERVICE_FEE / ADJUSTMENT
        ExternalID        NVARCHAR(200)    NOT NULL,   -- composed from Amazon's event identifiers
        PostedDate        DATETIMEOFFSET   NULL,
        MarketplaceName   NVARCHAR(50)     NULL,
        AmazonOrderID     NVARCHAR(50)     NULL,
        ShipmentID        NVARCHAR(50)     NULL,
        AdjustmentID      NVARCHAR(50)     NULL,
        SKU               NVARCHAR(200)    NULL,
        Quantity          INT              NULL,
        Currency          NVARCHAR(3)      NULL,
        Principal         DECIMAL(18,4)    NULL,
        Tax               DECIMAL(18,4)    NULL,
        Shipping          DECIMAL(18,4)    NULL,
        PromotionDiscount DECIMAL(18,4)    NULL,
        Commission        DECIMAL(18,4)    NULL,
        FBAFee            DECIMAL(18,4)    NULL,
        OtherFees         DECIMAL(18,4)    NULL,
        ServiceFeeType    NVARCHAR(100)    NULL,        -- e.g. FBAStorageFee, AWDStorageFee, LTSF, SubscriptionFee
        _RawPayload       NVARCHAR(MAX)    NULL,
        _IngestedAt       DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_fin_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID      BIGINT           NOT NULL,
        _SourceRowHash    VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_financial_events PRIMARY KEY CLUSTERED (_BrandUID, EventType, ExternalID)
    );
    CREATE INDEX IX_amz_fin_events_posted
        ON raw.amz_financial_events (_BrandUID, PostedDate DESC)
        INCLUDE (Commission, FBAFee, Principal, ServiceFeeType);
    PRINT 'Created raw.amz_financial_events';
END
GO

/* =========================================================================
   3. raw.amz_orders — order header (parent of amz_order_items)
   ========================================================================= */
IF OBJECT_ID('raw.amz_orders', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_orders (
        _BrandUID             UNIQUEIDENTIFIER NOT NULL,
        AmazonOrderID         NVARCHAR(50)     NOT NULL,
        MerchantOrderID       NVARCHAR(50)     NULL,
        MarketplaceID         NVARCHAR(20)     NULL,
        MarketplaceName       NVARCHAR(50)     NULL,
        PurchaseDate          DATETIMEOFFSET   NULL,
        LastUpdatedDate       DATETIMEOFFSET   NULL,
        OrderStatus           NVARCHAR(30)     NULL,
        FulfillmentChannel    NVARCHAR(10)     NULL,  -- AFN / MFN
        SalesChannel          NVARCHAR(50)     NULL,
        OrderChannel          NVARCHAR(50)     NULL,
        ShipServiceLevel      NVARCHAR(50)     NULL,
        Currency              NVARCHAR(3)      NULL,
        OrderTotal            DECIMAL(18,4)    NULL,
        NumberOfItemsShipped  INT              NULL,
        NumberOfItemsUnshipped INT             NULL,
        PaymentMethod         NVARCHAR(30)     NULL,
        BuyerEmail            NVARCHAR(320)    NULL,
        ShipCity              NVARCHAR(100)    NULL,
        ShipState             NVARCHAR(50)     NULL,
        ShipPostalCode        NVARCHAR(20)     NULL,
        ShipCountryCode       NVARCHAR(2)      NULL,
        IsBusinessOrder       BIT              NULL,
        IsReplacementOrder    BIT              NULL,
        IsPrime               BIT              NULL,
        IsSnS                 BIT              NULL,
        _RawPayload           NVARCHAR(MAX)    NULL,
        _IngestedAt           DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_orders_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID          BIGINT           NOT NULL,
        _SourceRowHash        VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_orders PRIMARY KEY CLUSTERED (_BrandUID, AmazonOrderID)
    );
    CREATE INDEX IX_amz_orders_purchase ON raw.amz_orders (_BrandUID, PurchaseDate DESC);
    CREATE INDEX IX_amz_orders_updated  ON raw.amz_orders (_BrandUID, LastUpdatedDate DESC);
    PRINT 'Created raw.amz_orders';
END
GO

/* =========================================================================
   4. raw.amz_order_items — line items child table
   ========================================================================= */
IF OBJECT_ID('raw.amz_order_items', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_order_items (
        _BrandUID             UNIQUEIDENTIFIER NOT NULL,
        AmazonOrderID         NVARCHAR(50)     NOT NULL,
        OrderItemID           NVARCHAR(50)     NOT NULL,
        SKU                   NVARCHAR(200)    NULL,
        ASIN                  NVARCHAR(20)     NULL,
        ProductName           NVARCHAR(500)    NULL,
        Quantity              INT              NULL,
        QuantityShipped       INT              NULL,
        Currency              NVARCHAR(3)      NULL,
        ItemPrice             DECIMAL(18,4)    NULL,
        ItemTax               DECIMAL(18,4)    NULL,
        ShippingPrice         DECIMAL(18,4)    NULL,
        ShippingTax           DECIMAL(18,4)    NULL,
        GiftWrapPrice         DECIMAL(18,4)    NULL,
        GiftWrapTax           DECIMAL(18,4)    NULL,
        ItemPromotionDiscount DECIMAL(18,4)    NULL,
        ShipPromotionDiscount DECIMAL(18,4)    NULL,
        PromotionIDs          NVARCHAR(500)    NULL,
        ConditionID           NVARCHAR(30)     NULL,
        _RawPayload           NVARCHAR(MAX)    NULL,
        _IngestedAt           DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_order_items_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID          BIGINT           NOT NULL,
        _SourceRowHash        VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_order_items PRIMARY KEY CLUSTERED (_BrandUID, AmazonOrderID, OrderItemID)
    );
    CREATE INDEX IX_amz_order_items_sku ON raw.amz_order_items (_BrandUID, SKU);
    CREATE INDEX IX_amz_order_items_asin ON raw.amz_order_items (_BrandUID, ASIN) WHERE ASIN IS NOT NULL;
    PRINT 'Created raw.amz_order_items';
END
GO

/* =========================================================================
   5. raw.amz_fba_inventory — MYI snapshot
   ========================================================================= */
IF OBJECT_ID('raw.amz_fba_inventory', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_fba_inventory (
        _BrandUID             UNIQUEIDENTIFIER NOT NULL,
        _SnapshotDate         DATE             NOT NULL,
        SKU                   NVARCHAR(200)    NOT NULL,
        FNSKU                 NVARCHAR(50)     NULL,
        ASIN                  NVARCHAR(20)     NULL,
        ProductName           NVARCHAR(500)    NULL,
        Condition             NVARCHAR(30)     NULL,
        YourPrice             DECIMAL(18,4)    NULL,
        MfnFulfillableQty     INT              NULL,
        AfnListingExists      BIT              NULL,
        AfnWarehouseQty       INT              NULL,
        AfnFulfillableQty     INT              NULL,
        AfnUnsellableQty      INT              NULL,
        AfnReservedQty        INT              NULL,
        AfnTotalQty           INT              NULL,
        AfnInboundWorkingQty  INT              NULL,
        AfnInboundShippedQty  INT              NULL,
        AfnInboundReceivingQty INT             NULL,
        AfnResearchingQty     INT              NULL,
        PerUnitVolume         DECIMAL(18,4)    NULL,
        _RawPayload           NVARCHAR(MAX)    NULL,
        _IngestedAt           DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_fba_inv_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID          BIGINT           NOT NULL,
        _SourceRowHash        VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_fba_inventory PRIMARY KEY CLUSTERED (_BrandUID, _SnapshotDate, SKU)
    );
    CREATE INDEX IX_amz_fba_inv_sku ON raw.amz_fba_inventory (_BrandUID, SKU, _SnapshotDate DESC);
    PRINT 'Created raw.amz_fba_inventory';
END
GO

/* =========================================================================
   6. raw.amz_restock_recommendations
   ========================================================================= */
IF OBJECT_ID('raw.amz_restock_recommendations', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_restock_recommendations (
        _BrandUID                   UNIQUEIDENTIFIER NOT NULL,
        _SnapshotDate               DATE             NOT NULL,
        Country                     NVARCHAR(2)      NOT NULL,
        MerchantSKU                 NVARCHAR(200)    NOT NULL,
        FNSKU                       NVARCHAR(50)     NULL,
        ASIN                        NVARCHAR(20)     NULL,
        ProductName                 NVARCHAR(500)    NULL,
        Supplier                    NVARCHAR(200)    NULL,
        Currency                    NVARCHAR(3)      NULL,
        Price                       DECIMAL(18,4)    NULL,
        SalesLast30Days             DECIMAL(18,4)    NULL,
        UnitsSoldLast30Days         INT              NULL,
        TotalUnits                  INT              NULL,
        Inbound                     INT              NULL,
        Available                   INT              NULL,
        FulfillableUnits            INT              NULL,
        TotalDaysOfSupply           DECIMAL(9,2)     NULL,
        DaysOfSupplyAtAFN           DECIMAL(9,2)     NULL,
        Alert                       NVARCHAR(200)    NULL,
        RecommendedReplenishmentQty INT              NULL,
        RecommendedShipDate         DATE             NULL,
        RecommendedAction           NVARCHAR(100)    NULL,
        _RawPayload                 NVARCHAR(MAX)    NULL,
        _IngestedAt                 DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_restock_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID                BIGINT           NOT NULL,
        _SourceRowHash              VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_restock_recs PRIMARY KEY CLUSTERED (_BrandUID, _SnapshotDate, Country, MerchantSKU)
    );
    CREATE INDEX IX_amz_restock_action
        ON raw.amz_restock_recommendations (_BrandUID, _SnapshotDate, RecommendedAction)
        INCLUDE (MerchantSKU, RecommendedReplenishmentQty, TotalDaysOfSupply)
        WHERE RecommendedReplenishmentQty > 0;
    PRINT 'Created raw.amz_restock_recommendations';
END
GO

/* =========================================================================
   7. raw.amz_listings — full per-SKU listing snapshot
   ========================================================================= */
IF OBJECT_ID('raw.amz_listings', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_listings (
        _BrandUID         UNIQUEIDENTIFIER NOT NULL,
        SKU               NVARCHAR(200)    NOT NULL,
        MarketplaceID     NVARCHAR(20)     NOT NULL,
        ASIN              NVARCHAR(20)     NULL,
        ProductType       NVARCHAR(100)    NULL,
        Title             NVARCHAR(500)    NULL,
        Brand             NVARCHAR(200)    NULL,
        Description       NVARCHAR(MAX)    NULL,
        Bullet1           NVARCHAR(500)    NULL,
        Bullet2           NVARCHAR(500)    NULL,
        Bullet3           NVARCHAR(500)    NULL,
        Bullet4           NVARCHAR(500)    NULL,
        Bullet5           NVARCHAR(500)    NULL,
        SearchTerms       NVARCHAR(500)    NULL,   -- Amazon backend keywords
        Category          NVARCHAR(200)    NULL,
        BrowseNodeID      NVARCHAR(50)     NULL,
        ImagesJSON        NVARCHAR(MAX)    NULL,   -- JSON array of image URLs
        Price             DECIMAL(18,4)    NULL,
        Currency          NVARCHAR(3)      NULL,
        Quantity          INT              NULL,
        Condition         NVARCHAR(30)     NULL,
        Status            NVARCHAR(30)     NULL,   -- ACTIVE / INACTIVE / INCOMPLETE / DISCOVERABLE
        IssueCount        INT              NULL,
        _RawPayload       NVARCHAR(MAX)    NULL,   -- full Listings Items API response
        _IngestedAt       DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_listings_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID      BIGINT           NOT NULL,
        _SourceRowHash    VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_listings PRIMARY KEY CLUSTERED (_BrandUID, SKU, MarketplaceID)
    );
    CREATE INDEX IX_amz_listings_asin ON raw.amz_listings (_BrandUID, ASIN) WHERE ASIN IS NOT NULL;
    CREATE INDEX IX_amz_listings_status ON raw.amz_listings (_BrandUID, Status);
    PRINT 'Created raw.amz_listings';
END
GO

/* =========================================================================
   8. raw.amz_listing_changes — write-path workflow + rollback history
   ========================================================================= */
IF OBJECT_ID('raw.amz_listing_changes', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_listing_changes (
        ChangeID            BIGINT IDENTITY(1,1) NOT NULL,
        _BrandUID           UNIQUEIDENTIFIER NOT NULL,
        SKU                 NVARCHAR(200)    NOT NULL,
        ASIN                NVARCHAR(20)     NULL,
        MarketplaceID       NVARCHAR(20)     NULL,
        ChangeSource        NVARCHAR(30)     NOT NULL,  -- PULLED_FROM_AMAZON / PROPOSED_BY_USER / PROPOSED_BY_AI
        ChangeType          NVARCHAR(50)     NOT NULL,  -- TITLE / BULLET_1..5 / DESCRIPTION / SEARCH_TERMS / PRICE / CATEGORY / IMAGES / ...
        FieldPath           NVARCHAR(200)    NULL,      -- JSON Patch path, e.g. '/attributes/item_name'
        BeforeValue         NVARCHAR(MAX)    NULL,
        AfterValue          NVARCHAR(MAX)    NULL,
        Status              NVARCHAR(30)     NOT NULL CONSTRAINT DF_amz_lc_status DEFAULT ('OBSERVED'),
                                                          -- OBSERVED (Amazon-side drift)
                                                          -- PROPOSED (awaiting approval)
                                                          -- APPROVED (ready to apply)
                                                          -- APPLIED (PATCH sent + accepted)
                                                          -- REVERTED (rolled back via PATCH of BeforeValue)
                                                          -- REJECTED / FAILED
        AmazonSubmissionID  NVARCHAR(100)    NULL,
        AmazonResponseJSON  NVARCHAR(MAX)    NULL,
        AISuggestionID      BIGINT           NULL,
        AIRationale         NVARCHAR(MAX)    NULL,
        AIConfidence        DECIMAL(5,2)     NULL,
        ProposedBy          INT              NULL,
        ProposedAt          DATETIME2(3)     NULL,
        ApprovedBy          INT              NULL,
        ApprovedAt          DATETIME2(3)     NULL,
        AppliedAt           DATETIME2(3)     NULL,
        RevertedAt          DATETIME2(3)     NULL,
        RevertedByChangeID  BIGINT           NULL,  -- the APPLIED row whose BeforeValue we pushed back
        _RawPayload         NVARCHAR(MAX)    NULL,
        _IngestedAt         DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_lc_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID        BIGINT           NULL,  -- null for user/AI proposals
        CONSTRAINT PK_amz_listing_changes PRIMARY KEY CLUSTERED (ChangeID),
        CONSTRAINT CK_amz_lc_Status CHECK (Status IN ('OBSERVED','PROPOSED','APPROVED','APPLIED','REVERTED','REJECTED','FAILED'))
    );
    CREATE INDEX IX_amz_lc_brand_sku       ON raw.amz_listing_changes (_BrandUID, SKU, _IngestedAt DESC);
    CREATE INDEX IX_amz_lc_status          ON raw.amz_listing_changes (_BrandUID, Status, _IngestedAt DESC);
    CREATE INDEX IX_amz_lc_applied_for_rb  ON raw.amz_listing_changes (_BrandUID, SKU, AppliedAt DESC)
        INCLUDE (ChangeID, ChangeType, BeforeValue) WHERE Status = 'APPLIED';
    PRINT 'Created raw.amz_listing_changes';
END
GO

/* =========================================================================
   9. raw.amz_listings_catalog — lightweight full-catalog report dump
   ========================================================================= */
IF OBJECT_ID('raw.amz_listings_catalog', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_listings_catalog (
        _BrandUID         UNIQUEIDENTIFIER NOT NULL,
        SKU               NVARCHAR(200)    NOT NULL,
        ASIN              NVARCHAR(20)     NULL,
        Title             NVARCHAR(500)    NULL,
        Price             DECIMAL(18,4)    NULL,
        Currency          NVARCHAR(3)      NULL,
        Quantity          INT              NULL,
        OpenDate          DATETIMEOFFSET   NULL,
        FulfillmentChannel NVARCHAR(10)    NULL,
        Status            NVARCHAR(30)     NULL,
        _RawPayload       NVARCHAR(MAX)    NULL,
        _IngestedAt       DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_lcat_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID      BIGINT           NOT NULL,
        _SourceRowHash    VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_listings_catalog PRIMARY KEY CLUSTERED (_BrandUID, SKU)
    );
    PRINT 'Created raw.amz_listings_catalog';
END
GO

/* =========================================================================
   10. raw.amz_settlement_v2 — per-settlement ledger rows
   ========================================================================= */
IF OBJECT_ID('raw.amz_settlement_v2', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_settlement_v2 (
        SettlementLineID    BIGINT IDENTITY(1,1) NOT NULL,
        _BrandUID           UNIQUEIDENTIFIER NOT NULL,
        SettlementID        NVARCHAR(50)     NOT NULL,
        SettlementStartDate DATETIMEOFFSET   NULL,
        SettlementEndDate   DATETIMEOFFSET   NULL,
        DepositDate         DATETIMEOFFSET   NULL,
        TotalAmount         DECIMAL(18,4)    NULL,
        Currency            NVARCHAR(3)      NULL,
        TransactionType     NVARCHAR(50)     NULL,
        OrderID             NVARCHAR(50)     NULL,
        AdjustmentID        NVARCHAR(50)     NULL,
        ShipmentID          NVARCHAR(50)     NULL,
        MarketplaceName     NVARCHAR(50)     NULL,
        AmountType          NVARCHAR(50)     NULL,
        AmountDescription   NVARCHAR(200)    NULL,
        Amount              DECIMAL(18,4)    NULL,
        PostedDate          DATE             NULL,
        PostedDateTime      DATETIMEOFFSET   NULL,
        SKU                 NVARCHAR(200)    NULL,
        QuantityPurchased   INT              NULL,
        PromotionID         NVARCHAR(500)    NULL,
        _RawPayload         NVARCHAR(MAX)    NULL,
        _IngestedAt         DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_settle_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID        BIGINT           NOT NULL,
        _SourceRowHash      VARBINARY(32)    NOT NULL,
        CONSTRAINT PK_amz_settlement_v2 PRIMARY KEY CLUSTERED (SettlementLineID),
        CONSTRAINT UQ_amz_settle_natural UNIQUE (_BrandUID, SettlementID, _SourceRowHash)
    );
    CREATE INDEX IX_amz_settle_deposit ON raw.amz_settlement_v2 (_BrandUID, DepositDate);
    PRINT 'Created raw.amz_settlement_v2';
END
GO

/* =========================================================================
   11. raw.amz_fba_reimbursements — money Amazon paid back for lost/damaged
   ========================================================================= */
IF OBJECT_ID('raw.amz_fba_reimbursements', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_fba_reimbursements (
        _BrandUID              UNIQUEIDENTIFIER NOT NULL,
        ReimbursementID        NVARCHAR(50)     NOT NULL,
        ApprovalDate           DATETIMEOFFSET   NULL,
        CaseID                 NVARCHAR(50)     NULL,
        AmazonOrderID          NVARCHAR(50)     NULL,
        Reason                 NVARCHAR(200)    NULL,
        SKU                    NVARCHAR(200)    NULL,
        FNSKU                  NVARCHAR(50)     NULL,
        ASIN                   NVARCHAR(20)     NULL,
        ProductName            NVARCHAR(500)    NULL,
        QuantityReimbursed     INT              NULL,
        AmountPerUnit          DECIMAL(18,4)    NULL,
        AmountTotal            DECIMAL(18,4)    NULL,
        Currency               NVARCHAR(3)      NULL,
        OriginalReimbursementID NVARCHAR(50)    NULL,
        OriginalReimbursementType NVARCHAR(50)  NULL,
        _RawPayload            NVARCHAR(MAX)    NULL,
        _IngestedAt            DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_reimb_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID           BIGINT           NOT NULL,
        _SourceRowHash         VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_fba_reimbursements PRIMARY KEY CLUSTERED (_BrandUID, ReimbursementID)
    );
    CREATE INDEX IX_amz_reimb_sku      ON raw.amz_fba_reimbursements (_BrandUID, SKU, ApprovalDate DESC);
    CREATE INDEX IX_amz_reimb_reason   ON raw.amz_fba_reimbursements (_BrandUID, Reason);
    PRINT 'Created raw.amz_fba_reimbursements';
END
GO

/* =========================================================================
   12. raw.amz_ledger_detail — lost / found / damaged etc.
   ========================================================================= */
IF OBJECT_ID('raw.amz_ledger_detail', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_ledger_detail (
        _BrandUID         UNIQUEIDENTIFIER NOT NULL,
        TransactionDate   DATE             NOT NULL,
        SKU               NVARCHAR(200)    NOT NULL,
        FulfillmentCenter NVARCHAR(10)     NOT NULL,
        TransactionType   NVARCHAR(50)     NOT NULL,   -- Receipts / CustomerOrders / CustomerReturns / Lost / Found / Damaged / ...
        ReferenceID       NVARCHAR(100)    NOT NULL,   -- distinguishes multiple events on same day+sku+fc
        FNSKU             NVARCHAR(50)     NULL,
        ASIN              NVARCHAR(20)     NULL,
        Title             NVARCHAR(500)    NULL,
        Disposition       NVARCHAR(50)     NULL,
        Reason            NVARCHAR(200)    NULL,
        Quantity          INT              NULL,
        _RawPayload       NVARCHAR(MAX)    NULL,
        _IngestedAt       DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_ledger_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID      BIGINT           NOT NULL,
        _SourceRowHash    VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_ledger_detail PRIMARY KEY CLUSTERED (_BrandUID, TransactionDate, SKU, FulfillmentCenter, TransactionType, ReferenceID)
    );
    CREATE INDEX IX_amz_ledger_lost
        ON raw.amz_ledger_detail (_BrandUID, TransactionType, TransactionDate DESC)
        INCLUDE (SKU, Quantity, FulfillmentCenter)
        WHERE TransactionType IN ('Lost','Damaged','Found');
    PRINT 'Created raw.amz_ledger_detail';
END
GO

/* =========================================================================
   13. raw.amz_storage_fees_monthly — per-SKU monthly storage fees
   ========================================================================= */
IF OBJECT_ID('raw.amz_storage_fees_monthly', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_storage_fees_monthly (
        _BrandUID              UNIQUEIDENTIFIER NOT NULL,
        AsinOrSKU              NVARCHAR(200)    NOT NULL,
        MonthOfCharge          NVARCHAR(20)     NOT NULL,   -- 'YYYY-MM' or Amazon's format
        FNSKU                  NVARCHAR(50)     NULL,
        ASIN                   NVARCHAR(20)     NULL,
        ProductName            NVARCHAR(500)    NULL,
        Currency               NVARCHAR(3)      NULL,
        FulfillmentCenter      NVARCHAR(10)     NULL,
        CountryCode            NVARCHAR(2)      NULL,
        ItemVolume             DECIMAL(18,6)    NULL,
        VolumeUnitMeasurement  NVARCHAR(20)     NULL,
        ProductSizeTier        NVARCHAR(50)     NULL,
        AverageQuantityOnHand  DECIMAL(18,4)    NULL,
        AverageQuantityPendingRemoval DECIMAL(18,4) NULL,
        EstimatedTotalItemVolume DECIMAL(18,6)  NULL,
        MonthlyStorageFeeRate  DECIMAL(18,4)    NULL,
        EstimatedMonthlyStorageFee DECIMAL(18,4) NULL,
        DangerousGoodsStorageType NVARCHAR(50)  NULL,
        EligibleForInventoryDiscount BIT        NULL,
        QualifiesForInventoryDiscount BIT       NULL,
        _RawPayload            NVARCHAR(MAX)    NULL,
        _IngestedAt            DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_stfm_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID           BIGINT           NOT NULL,
        _SourceRowHash         VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_storage_fees_monthly PRIMARY KEY CLUSTERED (_BrandUID, AsinOrSKU, MonthOfCharge)
    );
    CREATE INDEX IX_amz_stfm_month ON raw.amz_storage_fees_monthly (_BrandUID, MonthOfCharge);
    PRINT 'Created raw.amz_storage_fees_monthly';
END
GO

/* =========================================================================
   14. raw.amz_ltsf_charges — long-term storage fees (365+ days)
   ========================================================================= */
IF OBJECT_ID('raw.amz_ltsf_charges', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_ltsf_charges (
        _BrandUID              UNIQUEIDENTIFIER NOT NULL,
        AsinOrSKU              NVARCHAR(200)    NOT NULL,
        SnapshotDate           DATE             NOT NULL,
        FNSKU                  NVARCHAR(50)     NULL,
        ASIN                   NVARCHAR(20)     NULL,
        ProductName            NVARCHAR(500)    NULL,
        Condition              NVARCHAR(30)     NULL,
        QuantityCharged12Month INT              NULL,
        PerUnitVolume          DECIMAL(18,6)    NULL,
        Currency               NVARCHAR(3)      NULL,
        AmountCharged          DECIMAL(18,4)    NULL,
        SurchargeAgeTier       NVARCHAR(50)     NULL,   -- e.g. "181-270", "271-365", "365+"
        _RawPayload            NVARCHAR(MAX)    NULL,
        _IngestedAt            DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_ltsf_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID           BIGINT           NOT NULL,
        _SourceRowHash         VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_ltsf_charges PRIMARY KEY CLUSTERED (_BrandUID, SnapshotDate, AsinOrSKU)
    );
    PRINT 'Created raw.amz_ltsf_charges';
END
GO

/* =========================================================================
   15. raw.amz_fba_inventory_aged — aging buckets
   ========================================================================= */
IF OBJECT_ID('raw.amz_fba_inventory_aged', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_fba_inventory_aged (
        _BrandUID          UNIQUEIDENTIFIER NOT NULL,
        _SnapshotDate      DATE             NOT NULL,
        SKU                NVARCHAR(200)    NOT NULL,
        Country            NVARCHAR(2)      NOT NULL,
        FNSKU              NVARCHAR(50)     NULL,
        ASIN               NVARCHAR(20)     NULL,
        ProductName        NVARCHAR(500)    NULL,
        Condition          NVARCHAR(30)     NULL,
        AvailableQuantity  INT              NULL,
        Age0To90Days       INT              NULL,
        Age91To180Days     INT              NULL,
        Age181To270Days    INT              NULL,
        Age271To365Days    INT              NULL,
        Age365PlusDays     INT              NULL,
        Currency           NVARCHAR(3)      NULL,
        EstimatedLTSF      DECIMAL(18,4)    NULL,
        _RawPayload        NVARCHAR(MAX)    NULL,
        _IngestedAt        DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_aged_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID       BIGINT           NOT NULL,
        _SourceRowHash     VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_fba_inventory_aged PRIMARY KEY CLUSTERED (_BrandUID, _SnapshotDate, SKU, Country)
    );
    PRINT 'Created raw.amz_fba_inventory_aged';
END
GO

/* =========================================================================
   16. raw.amz_sns_forecast — 8-week SnS demand per SKU
   ========================================================================= */
IF OBJECT_ID('raw.amz_sns_forecast', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_sns_forecast (
        _BrandUID         UNIQUEIDENTIFIER NOT NULL,
        _SnapshotDate     DATE             NOT NULL,
        SKU               NVARCHAR(200)    NOT NULL,
        ForecastWeek      DATE             NOT NULL,   -- Monday of the forecast week
        FNSKU             NVARCHAR(50)     NULL,
        ASIN              NVARCHAR(20)     NULL,
        ProductName       NVARCHAR(500)    NULL,
        Country           NVARCHAR(2)      NULL,
        ActiveSubscriptions INT            NULL,
        ScheduledDeliveries INT            NULL,
        ForecastedUnits   DECIMAL(18,4)    NULL,
        _RawPayload       NVARCHAR(MAX)    NULL,
        _IngestedAt       DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_sns_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID      BIGINT           NOT NULL,
        _SourceRowHash    VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_sns_forecast PRIMARY KEY CLUSTERED (_BrandUID, _SnapshotDate, SKU, ForecastWeek)
    );
    CREATE INDEX IX_amz_sns_forecast_sku ON raw.amz_sns_forecast (_BrandUID, SKU, ForecastWeek);
    PRINT 'Created raw.amz_sns_forecast';
END
GO

/* =========================================================================
   17. raw.amz_awd_inventory — AWD current inventory
   ========================================================================= */
IF OBJECT_ID('raw.amz_awd_inventory', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_awd_inventory (
        _BrandUID         UNIQUEIDENTIFIER NOT NULL,
        _SnapshotDate     DATE             NOT NULL,
        SKU               NVARCHAR(200)    NOT NULL,
        ProductName       NVARCHAR(500)    NULL,
        TotalOnHand       INT              NULL,
        AvailableDistributable INT         NULL,
        Reserved          INT              NULL,
        Inbound           INT              NULL,
        MeasurementValue  DECIMAL(18,6)    NULL,
        MeasurementUnit   NVARCHAR(20)     NULL,
        _RawPayload       NVARCHAR(MAX)    NULL,
        _IngestedAt       DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_awd_inv_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID      BIGINT           NOT NULL,
        _SourceRowHash    VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_awd_inventory PRIMARY KEY CLUSTERED (_BrandUID, _SnapshotDate, SKU)
    );
    PRINT 'Created raw.amz_awd_inventory';
END
GO

/* =========================================================================
   18. raw.amz_awd_shipments — AWD inbound shipments
   ========================================================================= */
IF OBJECT_ID('raw.amz_awd_shipments', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_awd_shipments (
        _BrandUID         UNIQUEIDENTIFIER NOT NULL,
        ShipmentID        NVARCHAR(100)    NOT NULL,
        ShipmentStatus    NVARCHAR(50)     NULL,
        CreatedAtAmazon   DATETIMEOFFSET   NULL,
        ExpectedArrivalAt DATETIMEOFFSET   NULL,
        ReceivedAt        DATETIMEOFFSET   NULL,
        OriginCountry     NVARCHAR(2)      NULL,
        DestinationFC     NVARCHAR(10)     NULL,
        ItemCount         INT              NULL,
        CaseCount         INT              NULL,
        PalletCount       INT              NULL,
        _RawPayload       NVARCHAR(MAX)    NULL,
        _IngestedAt       DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_awd_ship_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID      BIGINT           NOT NULL,
        _SourceRowHash    VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_awd_shipments PRIMARY KEY CLUSTERED (_BrandUID, ShipmentID)
    );
    PRINT 'Created raw.amz_awd_shipments';
END
GO

/* =========================================================================
   19. raw.amz_returns — customer returns
   ========================================================================= */
IF OBJECT_ID('raw.amz_returns', 'U') IS NULL
BEGIN
    CREATE TABLE raw.amz_returns (
        _BrandUID         UNIQUEIDENTIFIER NOT NULL,
        OrderID           NVARCHAR(50)     NOT NULL,
        SKU               NVARCHAR(200)    NOT NULL,
        ReturnDate        DATE             NOT NULL,
        FNSKU             NVARCHAR(50)     NULL,
        ASIN              NVARCHAR(20)     NULL,
        ProductName       NVARCHAR(500)    NULL,
        Quantity          INT              NULL,
        FulfillmentCenter NVARCHAR(10)     NULL,
        DetailedDisposition NVARCHAR(100)  NULL,
        Reason            NVARCHAR(200)    NULL,
        Status            NVARCHAR(50)     NULL,
        LicensePlateNumber NVARCHAR(100)   NULL,
        CustomerComments  NVARCHAR(MAX)    NULL,
        _RawPayload       NVARCHAR(MAX)    NULL,
        _IngestedAt       DATETIME2(3)     NOT NULL CONSTRAINT DF_amz_ret_ing DEFAULT (SYSUTCDATETIME()),
        _SourceRunID      BIGINT           NOT NULL,
        _SourceRowHash    VARBINARY(32)    NULL,
        CONSTRAINT PK_amz_returns PRIMARY KEY CLUSTERED (_BrandUID, OrderID, SKU, ReturnDate)
    );
    CREATE INDEX IX_amz_returns_reason ON raw.amz_returns (_BrandUID, Reason, ReturnDate DESC);
    PRINT 'Created raw.amz_returns';
END
GO

PRINT '-----------------------------------------------------------';
PRINT 'Phase 3 Amazon data tables ready in ' + DB_NAME() + '.';
PRINT 'Count check:';
SELECT TableName = s.name + '.' + t.name,
       Rows = SUM(p.rows)
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0,1)
WHERE s.name = 'raw' AND t.name LIKE 'amz_%'
GROUP BY s.name, t.name
ORDER BY s.name, t.name;
PRINT '-----------------------------------------------------------';
GO
