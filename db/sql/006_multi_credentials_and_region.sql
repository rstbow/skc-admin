/* =============================================================================
   Allow multiple credentials per (brand × connector).

   Use cases:
     - Amazon seller operating in multiple regions (NA + EU + FE) needs
       separate refresh tokens per region (they're distinct OAuth grants).
     - A brand with two Amazon seller accounts (sub-brand, legacy account)
       wants each tracked separately.

   Changes:
     1. Drop the existing UNIQUE (BrandUID, ConnectorID) constraint
     2. Add Region column (NA / EU / FE / US / UK / global / …)
     3. Add filtered unique indexes so duplicates are still prevented when
        AccountIdentifier is set, and a single "default" (null identifier)
        row per (brand, connector) is still enforced.

   Runtime semantics after this:
     - A credential is keyed (BrandUID, ConnectorID, AccountIdentifier, Region).
     - Brands can have many rows for the same connector.
     - UI lets you add, edit, or delete each row independently.

   Idempotent: safe to re-run.
   ============================================================================= */

SET NOCOUNT ON;
GO

/* 1. Drop the old unique constraint if present */
IF EXISTS (
    SELECT 1 FROM sys.key_constraints
    WHERE name = 'UQ_Creds_BrandConnector'
      AND parent_object_id = OBJECT_ID('admin.BrandCredentials')
)
BEGIN
    ALTER TABLE admin.BrandCredentials DROP CONSTRAINT UQ_Creds_BrandConnector;
    PRINT 'Dropped UQ_Creds_BrandConnector.';
END
GO

/* 2. Add Region column */
IF COL_LENGTH('admin.BrandCredentials', 'Region') IS NULL
BEGIN
    ALTER TABLE admin.BrandCredentials ADD Region NVARCHAR(10) NULL;
    PRINT 'Added admin.BrandCredentials.Region.';
END
GO

/* 3. Add filtered unique indexes to prevent accidental duplicates */

-- One row with NULL AccountIdentifier per (brand, connector, region-or-null)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_Creds_DefaultRowPerBrandConnector' AND object_id = OBJECT_ID('admin.BrandCredentials'))
BEGIN
    CREATE UNIQUE INDEX UX_Creds_DefaultRowPerBrandConnector
        ON admin.BrandCredentials (BrandUID, ConnectorID)
        WHERE AccountIdentifier IS NULL AND Region IS NULL;
    PRINT 'Added UX_Creds_DefaultRowPerBrandConnector (one NULL-identifier cred per brand+connector when Region also NULL).';
END
GO

-- Unique by AccountIdentifier when provided
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_Creds_ByAccountIdentifier' AND object_id = OBJECT_ID('admin.BrandCredentials'))
BEGIN
    CREATE UNIQUE INDEX UX_Creds_ByAccountIdentifier
        ON admin.BrandCredentials (BrandUID, ConnectorID, AccountIdentifier)
        WHERE AccountIdentifier IS NOT NULL;
    PRINT 'Added UX_Creds_ByAccountIdentifier (unique per (brand, connector, AccountIdentifier)).';
END
GO

-- Unique by Region when provided (so same brand can have NA + EU rows without AccountIdentifier)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_Creds_ByRegion' AND object_id = OBJECT_ID('admin.BrandCredentials'))
BEGIN
    CREATE UNIQUE INDEX UX_Creds_ByRegion
        ON admin.BrandCredentials (BrandUID, ConnectorID, Region)
        WHERE Region IS NOT NULL AND AccountIdentifier IS NULL;
    PRINT 'Added UX_Creds_ByRegion (one cred per brand+connector+region when no AccountIdentifier).';
END
GO

-- Lookup index for runners: find all creds for a given brand+connector fast
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Creds_BrandConnector' AND object_id = OBJECT_ID('admin.BrandCredentials'))
BEGIN
    CREATE INDEX IX_Creds_BrandConnector
        ON admin.BrandCredentials (BrandUID, ConnectorID, IsActive);
    PRINT 'Added IX_Creds_BrandConnector lookup index.';
END
GO

PRINT 'admin.BrandCredentials now supports multiple credentials per (brand, connector).';
GO
