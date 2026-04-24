/* =============================================================================
   025c — curated.amz_listings view so the SaaS app can read current listing
   state including SalesRank.

   Run against: vs-ims-staging

   Why: the skc-api team needs current title / brand / bullets / images /
   rank for each SKU to render detail pages. Direct SELECT on raw.amz_listings
   requires an explicit grant; a curated view keeps the access surface
   narrow and stable.

   Idempotent (CREATE OR ALTER).
   ============================================================================= */

SET NOCOUNT ON;
GO

CREATE OR ALTER VIEW curated.amz_listings AS
/*
    Per-brand current snapshot of each Amazon listing. Source of truth
    for "what does this SKU look like on Amazon right now". Refreshed
    daily (3am Chicago) by the Listing Ledger scheduler, which pulls
    GET_MERCHANT_LISTINGS_ALL_DATA + enriches per-SKU via Listings
    Items API.

    For the delta feed (what changed), use curated.amz_listing_changes.
    For the sales-impact analysis, use curated.amz_listing_change_sales_impact.
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
    Quantity,
    Condition,
    Status,
    IssueCount,
    SalesRank,
    SalesRankCategory,
    _IngestedAt AS LastUpdated
FROM raw.amz_listings;
GO

IF DATABASE_PRINCIPAL_ID('skc_app_user') IS NOT NULL
BEGIN
    GRANT SELECT ON curated.amz_listings TO skc_app_user;
    PRINT 'Granted SELECT on curated.amz_listings to skc_app_user.';
END
GO

PRINT '025c applied: curated.amz_listings view ready.';
GO
