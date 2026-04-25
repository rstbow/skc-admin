/* =============================================================================
   029 — admin.Brands.Category column for hide/legacy filtering.

   Run against: skc-admin

   Adds a categorization layer separate from IsActive. IsActive stays
   the binary "should the scheduler treat this brand?" flag — leave a
   brand IsActive=1 but mark Category='Legacy' to keep its data
   pipelines running while hiding it from the default brands list.

   Categories:
     - Active   (default for new brands)
     - Legacy   (still alive but de-prioritized — hidden by default)
     - Hidden   (cosmetic only — should never appear in default UI)
     - Test     (sandbox / dev brands)

   Idempotent.
   ============================================================================= */

SET NOCOUNT ON;
GO

IF COL_LENGTH('admin.Brands', 'Category') IS NULL
BEGIN
    ALTER TABLE admin.Brands
        ADD Category NVARCHAR(20) NOT NULL
            CONSTRAINT DF_Brands_Category DEFAULT (N'Active');
    PRINT 'Added admin.Brands.Category (default Active).';
END
ELSE
    PRINT 'admin.Brands.Category already exists.';
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.check_constraints
    WHERE name = 'CK_Brands_Category'
      AND parent_object_id = OBJECT_ID('admin.Brands')
)
BEGIN
    ALTER TABLE admin.Brands
        ADD CONSTRAINT CK_Brands_Category
            CHECK (Category IN (N'Active', N'Legacy', N'Hidden', N'Test'));
    PRINT 'Added CK_Brands_Category.';
END
GO

PRINT '029 complete: admin.Brands.Category column live.';
GO
