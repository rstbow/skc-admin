/* =============================================================================
   030 — purge false-positive "value cleared" rows from raw.amz_listing_changes.

   Run against: vs-ims-staging   (DML — user runs in SSMS)

   Background:
     The listings runner's TRACKED_FIELDS list previously included fields
     the GET_MERCHANT_LISTINGS_ALL_DATA flat-file report doesn't carry
     (bullets, brand, category, browse_node, product_type, images,
     search_terms, currency). Each daily run saw cur=populated (from the
     rank runner's catalog enrichment) vs fresh=null (the report doesn't
     have these) and emitted a "value cleared" change row. Real table
     state stayed correct because the merge proc preserves existing
     values via ISNULL(src.X, tgt.X), but the ledger filled with
     contradictory rows that misrepresent listing changes.

     Code fix shipped — TRACKED_FIELDS no longer includes those fields.
     This migration cleans up the historical noise so app2's Listing
     Ledger renders accurately.

   Strategy:
     Self-consistency check. Delete rows where:
       - ChangeType is in the catalog-only set
       - AfterValue is NULL (claimed value was cleared)
       - BUT the corresponding column in raw.amz_listings is currently
         non-null for the same brand+SKU+marketplace
     If the column is currently populated, the "deletion" change row
     can't have been real — it was the false positive we're fixing.

     Real deletions (where the field genuinely got cleared) won't match
     because their corresponding raw.amz_listings field would also be
     NULL or the field wouldn't have existed in the first place.

   Idempotent (re-running just deletes zero more rows).

   Reports counts before/after for sanity.
   ============================================================================= */

SET NOCOUNT ON;
GO

DECLARE @before INT = (SELECT COUNT(*) FROM raw.amz_listing_changes);

-- One DELETE per affected ChangeType so each clause's existence-check is
-- on a single column. The OR pattern in a single DELETE would force
-- SQL Server to evaluate every column for every row.

DELETE lc
  FROM raw.amz_listing_changes lc
  JOIN raw.amz_listings l
    ON l._BrandUID = lc._BrandUID AND l.SKU = lc.SKU AND l.MarketplaceID = lc.MarketplaceID
 WHERE lc.AfterValue IS NULL
   AND ((lc.ChangeType = 'BULLET_CHANGED'        AND (l.Bullet1 IS NOT NULL OR l.Bullet2 IS NOT NULL OR l.Bullet3 IS NOT NULL OR l.Bullet4 IS NOT NULL OR l.Bullet5 IS NOT NULL))
     OR (lc.ChangeType = 'BRAND_CHANGED'         AND l.Brand        IS NOT NULL)
     OR (lc.ChangeType = 'CATEGORY_CHANGED'      AND l.Category     IS NOT NULL)
     OR (lc.ChangeType = 'BROWSE_NODE_CHANGED'   AND l.BrowseNodeID IS NOT NULL)
     OR (lc.ChangeType = 'PRODUCT_TYPE_CHANGED'  AND l.ProductType  IS NOT NULL)
     OR (lc.ChangeType = 'SEARCH_TERMS_CHANGED'  AND l.SearchTerms  IS NOT NULL)
     OR (lc.ChangeType = 'IMAGES_CHANGED'        AND l.ImagesJSON   IS NOT NULL AND l.ImagesJSON <> N'[]')
     OR (lc.ChangeType = 'CURRENCY_CHANGED'      AND l.Currency     IS NOT NULL));

DECLARE @after INT = (SELECT COUNT(*) FROM raw.amz_listing_changes);
PRINT 'raw.amz_listing_changes rows: ' + CAST(@before AS NVARCHAR(20)) + ' → ' + CAST(@after AS NVARCHAR(20))
    + '  (deleted ' + CAST(@before - @after AS NVARCHAR(20)) + ')';
GO
