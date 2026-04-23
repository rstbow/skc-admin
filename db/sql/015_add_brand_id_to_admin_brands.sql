/* =============================================================================
   Add BrandID (int) to admin.Brands.

   Why: data-DB tables like tbl_PPA_IMS_SKU typically identify brands via an
   integer Brand_ID column (SKU Compass convention), while skc-admin and the
   SP-API world identify brands by BrandUID (uniqueidentifier). We need a
   local mapping so queries against shared data-DB tables can filter to the
   right brand without round-tripping to skc-auth-dev on every call.

   Nullable: brands that haven't been mapped yet fail gracefully with a
   diagnostic message in the UI ("no BrandID set — set it on the Brands page
   or run the sync SQL").

   Run against: skc-admin.
   Idempotent.
   ============================================================================= */

SET NOCOUNT ON;
GO

IF COL_LENGTH('admin.Brands', 'BrandID') IS NULL
BEGIN
    ALTER TABLE admin.Brands ADD BrandID INT NULL;
    PRINT 'Added admin.Brands.BrandID (int, nullable).';
END
ELSE
    PRINT 'admin.Brands.BrandID already exists.';
GO

PRINT '';
PRINT 'To populate: look up each brand''s integer Brand_ID from your data DB';
PRINT '(e.g. tbl_PPA_IMS_SKU or a brand master), then:';
PRINT '';
PRINT '  USE [skc-admin];';
PRINT '  UPDATE admin.Brands SET BrandID = <int> WHERE BrandName = ''Zentoes'';';
PRINT '  -- repeat per brand';
PRINT '';
PRINT 'Helpful diagnostic (run against vs-ims-staging):';
PRINT '  SELECT DISTINCT Brand_ID FROM dbo.tbl_PPA_IMS_SKU ORDER BY Brand_ID;';
GO
