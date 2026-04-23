/* =============================================================================
   Register two more Amazon endpoints used by the Pricing Calculator:
     - AMZ_PRICING_GET              sync API, per-SKU current price
     - AMZ_PRODUCT_FEES_ESTIMATE    sync API, what-if fees at a hypothetical price

   Live calculator — no raw.* table needed (not persisted).

   Run against: skc-admin.
   Idempotent (MERGE on ConnectorID + Name).
   ============================================================================= */

SET NOCOUNT ON;
GO

DECLARE @ConnID INT = (SELECT ConnectorID FROM admin.Connectors WHERE Name = 'AMAZON_SP_API');
IF @ConnID IS NULL
BEGIN
    RAISERROR(N'AMAZON_SP_API connector not found. Run 002_seed_connectors.sql first.', 16, 1);
    RETURN;
END

DECLARE @endpoints TABLE (
    Name                NVARCHAR(100),
    DisplayName         NVARCHAR(200),
    Description         NVARCHAR(MAX),
    EndpointType        NVARCHAR(30),
    HttpMethod          NVARCHAR(10),
    Path                NVARCHAR(500),
    PaginationStrategy  NVARCHAR(30),
    RateLimitWeight     INT,
    Notes               NVARCHAR(MAX)
);

INSERT INTO @endpoints VALUES
    ('AMZ_PRICING_GET', 'Pricing — Current Price by SKU',
     N'Returns the seller''s current listing price + shipping for a SKU (or up to 20 SKUs) in a given marketplace. Sync API, no pagination. Used as baseline for the pricing calculator.',
     'REST_GET', 'GET', '/products/pricing/v0/price',
     'NONE', 1,
     N'Rate: 10 rps burst / 0.5 rps sustained. Per-marketplace. Live-query only.'),

    ('AMZ_PRODUCT_FEES_ESTIMATE', 'Product Fees — Estimate at a Price',
     N'POST a hypothetical listing price (+ optional shipping/coupon/points), Amazon returns every fee they would charge at that price — referral fee, FBA fulfillment, variable closing, per-item, etc. Used for what-if pricing scenarios.',
     'REST_POST', 'POST', '/products/fees/v0/listings/{SellerSKU}/feesEstimate',
     'NONE', 1,
     N'Rate: 10 rps burst / 0.5 rps sustained. One SKU per call. Live calculator, not persisted.');

MERGE admin.Endpoints AS tgt
USING (SELECT @ConnID AS ConnectorID, * FROM @endpoints) AS src
    ON tgt.ConnectorID = src.ConnectorID AND tgt.Name = src.Name
WHEN MATCHED THEN
    UPDATE SET DisplayName        = src.DisplayName,
               Description        = src.Description,
               EndpointType       = src.EndpointType,
               HttpMethod         = src.HttpMethod,
               Path               = src.Path,
               PaginationStrategy = src.PaginationStrategy,
               RateLimitWeight    = src.RateLimitWeight,
               Notes              = src.Notes,
               UpdatedAt          = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ConnectorID, Name, DisplayName, Description, EndpointType, HttpMethod, Path,
            PaginationStrategy, TargetSchema, TargetTable, RateLimitWeight, Notes)
    VALUES (src.ConnectorID, src.Name, src.DisplayName, src.Description, src.EndpointType, src.HttpMethod, src.Path,
            src.PaginationStrategy, 'raw', '__NOT_PERSISTED__', src.RateLimitWeight, src.Notes);
GO

PRINT 'Registered AMZ_PRICING_GET + AMZ_PRODUCT_FEES_ESTIMATE endpoints.';
GO
