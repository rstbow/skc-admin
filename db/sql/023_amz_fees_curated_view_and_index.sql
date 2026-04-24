/* =============================================================================
   Migration 023 — curated view + covering index for raw.amz_financial_events.

   Run against: vs-ims-staging

   Why:
     skc-api (the SaaS app) wants to surface Amazon fees / net-settled
     amounts to customers on a per-brand basis. Two things make that
     fast + clean:

     1. A NONCLUSTERED COVERING INDEX on (_BrandUID, PostedDate DESC).
        Today the clustered PK is (_BrandUID, EventType, ExternalID),
        which is great for MERGE but bad for "last 30 days of fees for
        brand X" queries — SQL would seek into the brand range then
        scan EVERY event type for it. The new index makes that query a
        pure index-seek + covered read, no key lookups.

     2. A curated view curated.amz_fees that exposes:
          - The raw columns callers care about
          - A computed AmzNetAmount = sum of the 7 money fields
            (what Amazon actually net-settled, before COG)
          - _IngestedAt for cache-freshness checks

        Keeps skc-api from re-implementing the net-amount math in N
        places and lets us rename/restructure raw.amz_financial_events
        in the future without breaking callers.

   Idempotent: re-runnable.
   ============================================================================= */

SET NOCOUNT ON;
GO

/* ----- 1. Covering index ----- */

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_amz_fin_events_brand_posted'
      AND object_id = OBJECT_ID('raw.amz_financial_events')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_amz_fin_events_brand_posted
        ON raw.amz_financial_events (_BrandUID, PostedDate DESC)
        INCLUDE (
            EventType, SKU, AmazonOrderID, Quantity, Currency,
            Principal, Tax, Shipping, PromotionDiscount,
            Commission, FBAFee, OtherFees, ServiceFeeType, _IngestedAt
        );
    PRINT 'Created IX_amz_fin_events_brand_posted (covering).';
END
ELSE
    PRINT 'IX_amz_fin_events_brand_posted already exists.';
GO

/* ----- 2. curated schema (if missing) ----- */

IF SCHEMA_ID('curated') IS NULL
    EXEC('CREATE SCHEMA curated AUTHORIZATION dbo');
GO

/* ----- 3. Curated view ----- */

CREATE OR ALTER VIEW curated.amz_fees AS
/*
    Per-brand, per-event view of Amazon SP-API financial events.
    Source: raw.amz_financial_events (ingested every ~6h by skc-admin-api
    scheduler; 180-day backfill available per-brand on demand).

    Net-amount math:
      AmzNetAmount = Principal + Tax + Shipping + PromotionDiscount
                   + Commission + FBAFee + OtherFees
    Commission / FBAFee / OtherFees are already negative in the raw data
    (Amazon's perspective — money flowing out), so a simple SUM gives
    "what Amazon settled to the seller" before any COG deduction.

    For net-profit, callers join against the brand's COG source
    (tbl_PPA_IMS_SKU on the brand data DB):
      NetProfit = AmzNetAmount - (Quantity * COG)
*/
SELECT
    _BrandUID,
    EventType,
    ExternalID,
    PostedDate,
    MarketplaceName,
    AmazonOrderID,
    ShipmentID,
    AdjustmentID,
    SKU,
    Quantity,
    Currency,
    Principal,
    Tax,
    Shipping,
    PromotionDiscount,
    Commission,
    FBAFee,
    OtherFees,
    ServiceFeeType,
    CAST(
        ISNULL(Principal, 0)
      + ISNULL(Tax, 0)
      + ISNULL(Shipping, 0)
      + ISNULL(PromotionDiscount, 0)
      + ISNULL(Commission, 0)
      + ISNULL(FBAFee, 0)
      + ISNULL(OtherFees, 0)
      AS DECIMAL(18,4)
    ) AS AmzNetAmount,
    _IngestedAt
FROM raw.amz_financial_events;
GO

PRINT 'Created/updated curated.amz_fees view.';
GO

/* ----- 4. Grant SELECT to the SaaS app login ----- */

/*
   skc-api connects as (typically) skc_app_user. Grant SELECT on the
   view so the app can read it. Grant directly on the view, NOT the raw
   table — keeps the schema change surface narrow and lets us tighten
   base-table access later.
*/
IF DATABASE_PRINCIPAL_ID('skc_app_user') IS NOT NULL
BEGIN
    GRANT SELECT ON curated.amz_fees TO skc_app_user;
    PRINT 'Granted SELECT on curated.amz_fees to skc_app_user.';
END
ELSE
    PRINT 'WARNING: skc_app_user not present — SaaS app will fail to query until the login is created + granted.';
GO

PRINT '--------------------------------------------------';
PRINT 'Migration 023 complete.';
PRINT '--------------------------------------------------';
GO
