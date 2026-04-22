/* =============================================================================
   Seed Amazon SP-API endpoints into admin.Endpoints.
   Run against: skc-admin

   Priorities (user-confirmed 2026-04-22):
     - SnS: Forecast report (not active-subscribers)
     - Listings: PATCH with confirmations + rollbacks
     - Storage fees: per-SKU granularity

   Idempotent: upserts by (ConnectorID, Name).
   ============================================================================= */

SET NOCOUNT ON;
GO

DECLARE @ConnID INT = (SELECT ConnectorID FROM admin.Connectors WHERE Name = 'AMAZON_SP_API');
IF @ConnID IS NULL
BEGIN
    RAISERROR(N'AMAZON_SP_API connector not found. Run 002_seed_connectors.sql first.', 16, 1);
    RETURN;
END

/* Staging list — one row per endpoint we want registered. */
DECLARE @endpoints TABLE (
    Name                NVARCHAR(100),
    DisplayName         NVARCHAR(200),
    Description         NVARCHAR(MAX),
    EndpointType        NVARCHAR(30),
    HttpMethod          NVARCHAR(10),
    Path                NVARCHAR(500),
    PaginationStrategy  NVARCHAR(30),
    PollIntervalSec     INT,
    PollMaxAttempts     INT,
    TargetSchema        NVARCHAR(50),
    TargetTable         NVARCHAR(128),
    NaturalKeyColumns   NVARCHAR(500),
    RateLimitWeight     INT,
    ParamsTemplate      NVARCHAR(MAX),
    Notes               NVARCHAR(MAX)
);

INSERT INTO @endpoints VALUES

/* -------- 1. Financial Events (already piloted) -------- */
('AMZ_FINANCIAL_EVENTS', 'Financial Events (fees, refunds, adjustments)',
 N'Real-time ledger of every charge Amazon applies — commission, FBA fees, service fees (AWD, storage, LTSF), adjustments. Synchronous API, pagination via NextToken.',
 'REST_GET', 'GET', '/finances/v0/financialEvents', 'NEXT_TOKEN', NULL, NULL,
 'raw', 'amz_financial_events', '_BrandUID,EventType,ExternalID', 1,
 N'{"PostedAfter":"{{since_iso}}","MaxResultsPerPage":100}',
 N'Pilot endpoint. Event-level fee detail. Pull every 2 hours with 4-hour overlap.'),

/* -------- 2. Orders by last update -------- */
('AMZ_ORDERS_BY_LAST_UPDATE', 'Orders by Last Updated Date',
 N'Incremental orders + order items. Catches status changes, shipments, cancellations.',
 'REPORT', 'POST', 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL', NULL, 30, 60,
 'raw', 'amz_orders', '_BrandUID,AmazonOrderID,SKU', 5,
 N'{"reportType":"GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL","marketplaceIds":"{{marketplaces}}","dataStartTime":"{{since_iso}}"}',
 N'Bread-and-butter ingestion. Pull hourly with 2-hour overlap. Parent table raw.amz_orders; line items unwound into raw.amz_order_items.'),

/* -------- 3. FBA MYI Inventory snapshot -------- */
('AMZ_FBA_MYI_INVENTORY', 'FBA Inventory Snapshot (MYI)',
 N'Rich per-SKU FBA inventory view — fulfillable, reserved, inbound (working/shipped/receiving), unsellable. Snapshot-in-time with _SnapshotDate added on ingest.',
 'REPORT', 'POST', 'GET_FBA_MYI_ALL_INVENTORY_DATA', NULL, 30, 60,
 'raw', 'amz_fba_inventory', '_BrandUID,_SnapshotDate,SKU', 5,
 N'{"reportType":"GET_FBA_MYI_ALL_INVENTORY_DATA","marketplaceIds":"{{marketplaces}}"}',
 N'Daily snapshot (4-hourly later). _SnapshotDate set on ingest.'),

/* -------- 4. Restock Recommendations -------- */
('AMZ_RESTOCK_RECOMMENDATIONS', 'Restock Inventory Recommendations',
 N'Amazon''s own reorder quantities + days of supply + "ship now" alerts per SKU.',
 'REPORT', 'POST', 'GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT', NULL, 30, 60,
 'raw', 'amz_restock_recommendations', '_BrandUID,_SnapshotDate,Country,MerchantSKU', 5,
 N'{"reportType":"GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT","marketplaceIds":"{{marketplaces}}"}',
 N'Daily at 5am PT — Amazon refreshes overnight.'),

/* -------- 5a. Listings — read -------- */
('AMZ_LISTINGS_READ', 'Listings — Read (title, bullets, description, keywords, pricing)',
 N'Full listing snapshot per SKU: title, bullets, description, search terms, category, images, offer (price/qty), issues.',
 'REST_GET', 'GET', '/listings/2021-08-01/items/{sellerId}/{sku}?marketplaceIds={{marketplaces}}&includedData=summaries,attributes,issues,offers,fulfillmentAvailability,procurement,relationships',
 'NONE', NULL, NULL,
 'raw', 'amz_listings', '_BrandUID,SKU,MarketplaceID', 5,
 N'{"sellerId":"{{seller_id}}","marketplaceIds":"{{marketplaces}}"}',
 N'Daily full sweep + on-demand refresh. Also writes diffs to raw.amz_listing_changes for history/rollback.'),

/* -------- 5b. Listings — catalog sweep for bulk ingestion -------- */
('AMZ_LISTINGS_CATALOG_SWEEP', 'Listings — Full Catalog Sweep (report)',
 N'Lightweight full-catalog dump. Faster than iterating every SKU via the per-SKU API. Use for initial onboarding and weekly reconciliation.',
 'REPORT', 'POST', 'GET_MERCHANT_LISTINGS_ALL_DATA', NULL, 30, 60,
 'raw', 'amz_listings_catalog', '_BrandUID,SKU', 5,
 N'{"reportType":"GET_MERCHANT_LISTINGS_ALL_DATA","marketplaceIds":"{{marketplaces}}"}',
 N'Use for catalog onboarding. Per-SKU detail comes from AMZ_LISTINGS_READ.'),

/* -------- 5c. Listings — write (PATCH with confirmations + rollbacks) -------- */
('AMZ_LISTINGS_WRITE_PATCH', 'Listings — PATCH (write path with rollback)',
 N'Applies a JSON Patch to a single listing on Amazon. Every write is preceded by a PROPOSED row in raw.amz_listing_changes so admin/AI can confirm before commit, and the row''s BeforeValue enables one-click rollback.',
 'REST_POST', 'PATCH', '/listings/2021-08-01/items/{sellerId}/{sku}?marketplaceIds={{marketplaces}}',
 'NONE', NULL, NULL,
 'raw', 'amz_listing_changes', '_BrandUID,ChangeID', 10,
 N'{"sellerId":"{{seller_id}}","marketplaceIds":"{{marketplaces}}","productType":"{{product_type}}","patches":[]}',
 N'Write path. Every PATCH is logged to raw.amz_listing_changes (status workflow: PROPOSED → APPROVED → APPLIED; rollback via APPLIED → REVERTED by PATCHing the BeforeValue back). AI suggestions propose changes for admin review.'),

/* -------- 6. Settlement Reports (financial reconciliation) -------- */
('AMZ_SETTLEMENT_V2', 'Settlement Reports (V2)',
 N'Per-settlement complete ledger: every sale, refund, fee, adjustment that hit a payout. Generated by Amazon every ~2 weeks.',
 'REPORT', 'GET', 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE', NULL, 30, 60,
 'raw', 'amz_settlement_v2', '_BrandUID,SettlementID,_SourceRowHash', 5,
 N'{"reportTypes":"GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE","processingStatuses":"DONE"}',
 N'Amazon generates these automatically; we LIST new ones and download. No createReport call.'),

/* -------- 7. FBA Reimbursements -------- */
('AMZ_FBA_REIMBURSEMENTS', 'FBA Reimbursements (money back for lost/damaged)',
 N'Every reimbursement Amazon has paid for lost, damaged, destroyed, or over-charged inventory. Includes reason codes and dollar amounts.',
 'REPORT', 'POST', 'GET_FBA_REIMBURSEMENTS_DATA', NULL, 30, 60,
 'raw', 'amz_fba_reimbursements', '_BrandUID,ReimbursementID', 5,
 N'{"reportType":"GET_FBA_REIMBURSEMENTS_DATA","marketplaceIds":"{{marketplaces}}"}',
 N'Weekly. Many sellers leave real money on the table by not chasing under-reimbursements — this feeds that workflow.'),

/* -------- 8. Ledger Detail (lost / found / damaged) -------- */
('AMZ_LEDGER_DETAIL', 'Inventory Ledger — Detail (lost/found/damaged)',
 N'Transaction-level inventory movements per SKU per FC: Receipts, CustomerOrders, CustomerReturns, Vendor Returns, Lost, Found, Damaged, Transferred. Canonical source for lost-inventory tracking.',
 'REPORT', 'POST', 'GET_LEDGER_DETAIL_VIEW_DATA', NULL, 30, 60,
 'raw', 'amz_ledger_detail', '_BrandUID,TransactionDate,SKU,FulfillmentCenter,TransactionType,ReferenceID', 5,
 N'{"reportType":"GET_LEDGER_DETAIL_VIEW_DATA","marketplaceIds":"{{marketplaces}}","reportOptions":{"eventType":"Adjustments"}}',
 N'Weekly. Pair with AMZ_FBA_REIMBURSEMENTS to see "inventory lost" vs "money recovered".'),

/* -------- 9a. Storage Fee Charges (per-SKU monthly) -------- */
('AMZ_STORAGE_FEES_MONTHLY', 'FBA Storage Fees — Monthly (per SKU)',
 N'Monthly FBA storage fees broken down per SKU: volume, weight tier, storage category. User wants per-SKU granularity.',
 'REPORT', 'POST', 'GET_FBA_STORAGE_FEE_CHARGES_DATA', NULL, 30, 60,
 'raw', 'amz_storage_fees_monthly', '_BrandUID,AsinOrSKU,MonthOfCharge', 5,
 N'{"reportType":"GET_FBA_STORAGE_FEE_CHARGES_DATA","marketplaceIds":"{{marketplaces}}"}',
 N'Monthly (charges hit around the 7th-15th). Per-SKU granular detail of storage costs.'),

/* -------- 9b. Long-Term Storage Fees -------- */
('AMZ_LTSF_CHARGES', 'FBA Long-Term Storage Fees (365+ days)',
 N'Long-term storage fees per SKU for inventory aged over 365 days.',
 'REPORT', 'POST', 'GET_FBA_FULFILLMENT_LONGTERM_STORAGE_FEE_CHARGES_DATA', NULL, 30, 60,
 'raw', 'amz_ltsf_charges', '_BrandUID,AsinOrSKU,SnapshotDate', 5,
 N'{"reportType":"GET_FBA_FULFILLMENT_LONGTERM_STORAGE_FEE_CHARGES_DATA","marketplaceIds":"{{marketplaces}}"}',
 N'Monthly. Join with AMZ_FBA_INVENTORY_AGED to forecast next month''s LTSF hit.'),

/* -------- 10. FBA Inventory Aged (for LTSF forecasting) -------- */
('AMZ_FBA_INVENTORY_AGED', 'FBA Inventory — Aged Buckets',
 N'Inventory aging buckets: 0-90, 91-180, 181-270, 271-365, 365+ days. Predicts LTSF exposure.',
 'REPORT', 'POST', 'GET_FBA_INVENTORY_AGED_DATA', NULL, 30, 60,
 'raw', 'amz_fba_inventory_aged', '_BrandUID,_SnapshotDate,SKU,Country', 5,
 N'{"reportType":"GET_FBA_INVENTORY_AGED_DATA","marketplaceIds":"{{marketplaces}}"}',
 N'Weekly.'),

/* -------- 11. Subscribe & Save Forecast -------- */
('AMZ_SNS_FORECAST', 'Subscribe & Save — 8-Week Demand Forecast',
 N'Amazon''s 8-week forecast of SnS demand per SKU. Critical for planning inventory for recurring demand.',
 'REPORT', 'POST', 'GET_FBA_SNS_FORECAST_REPORT', NULL, 30, 60,
 'raw', 'amz_sns_forecast', '_BrandUID,_SnapshotDate,SKU,ForecastWeek', 5,
 N'{"reportType":"GET_FBA_SNS_FORECAST_REPORT","marketplaceIds":"{{marketplaces}}"}',
 N'Weekly. User-confirmed priority: forecast report (not active-subscribers — Amazon doesn''t expose that).'),

/* -------- 12a. AWD Inventory -------- */
('AMZ_AWD_INVENTORY', 'AWD Inventory (Amazon Warehousing & Distribution)',
 N'Current inventory at Amazon''s bulk warehousing service (AWD) — separate from FBA.',
 'REST_GET', 'GET', '/awd/2024-05-09/inventory', 'NEXT_TOKEN', NULL, NULL,
 'raw', 'amz_awd_inventory', '_BrandUID,_SnapshotDate,SKU', 2,
 N'{}',
 N'Daily. AWD storage fees are in Financial Events ServiceFeeEventList.'),

/* -------- 12b. AWD Inbound Shipments -------- */
('AMZ_AWD_SHIPMENTS', 'AWD Inbound Shipments',
 N'Shipments sent into AWD with status tracking.',
 'REST_GET', 'GET', '/awd/2024-05-09/inboundShipments', 'NEXT_TOKEN', NULL, NULL,
 'raw', 'amz_awd_shipments', '_BrandUID,ShipmentID', 2,
 N'{}',
 N'Daily.'),

/* -------- 13. Returns -------- */
('AMZ_RETURNS', 'FBA Returns',
 N'Customer returns with disposition (sellable/unsellable), reason, status, tracking.',
 'REPORT', 'POST', 'GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA', NULL, 30, 60,
 'raw', 'amz_returns', '_BrandUID,OrderID,SKU,ReturnDate', 5,
 N'{"reportType":"GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA","marketplaceIds":"{{marketplaces}}"}',
 N'Daily.');

/* Merge into admin.Endpoints */
MERGE admin.Endpoints AS tgt
USING (
    SELECT @ConnID AS ConnectorID, * FROM @endpoints
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

PRINT '--------------------------------------------------';
PRINT '16 Amazon endpoints registered/updated.';
PRINT 'Check: SELECT Name, DisplayName, EndpointType, TargetTable FROM admin.Endpoints';
PRINT '       WHERE ConnectorID = (SELECT ConnectorID FROM admin.Connectors WHERE Name = ''AMAZON_SP_API'')';
PRINT '       ORDER BY Name;';
PRINT '--------------------------------------------------';
GO
