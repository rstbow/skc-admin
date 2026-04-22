/* =============================================================================
   Add CredentialScope to admin.Connectors.

   Context:
   Connectors vary in where credentials live.
     - APP_AND_BRAND: app-level client_id/secret + per-brand refresh tokens.
       e.g. Amazon SP-API (LWA), Walmart, TikTok, Shopify PUBLIC apps.
     - BRAND_ONLY: every credential is per-brand — no shared app identity.
       e.g. Shopify CUSTOM apps (shpat_ token), Extensiv (per-3PL OAuth client).
     - APP_ONLY: reserved for future — all creds at app level, brand only
       identified by account identifier (rare).

   The UI uses this to show/hide the app-level credential section on the
   connector edit form. Runners use it to know where to look up creds at
   pull time.

   Backfill values for the existing 5 seeded connectors are applied.

   Idempotent: safe to re-run.
   ============================================================================= */

SET NOCOUNT ON;
GO

IF COL_LENGTH('admin.Connectors', 'CredentialScope') IS NULL
BEGIN
    ALTER TABLE admin.Connectors
        ADD CredentialScope NVARCHAR(20) NOT NULL
            CONSTRAINT DF_Connectors_CredScope DEFAULT ('APP_AND_BRAND');
    PRINT 'Added admin.Connectors.CredentialScope';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.check_constraints
    WHERE name = 'CK_Connectors_CredScope'
      AND parent_object_id = OBJECT_ID('admin.Connectors')
)
BEGIN
    ALTER TABLE admin.Connectors
        ADD CONSTRAINT CK_Connectors_CredScope
            CHECK (CredentialScope IN ('APP_AND_BRAND','BRAND_ONLY','APP_ONLY'));
    PRINT 'Added CK_Connectors_CredScope';
END
GO

/* Backfill existing seeds with correct scope. */
UPDATE admin.Connectors SET CredentialScope = 'APP_AND_BRAND'
WHERE Name IN ('AMAZON_SP_API','WALMART_MP','TIKTOK_SHOP')
  AND CredentialScope <> 'APP_AND_BRAND';

UPDATE admin.Connectors SET CredentialScope = 'BRAND_ONLY'
WHERE Name IN ('SHOPIFY','EXTENSIV')
  AND CredentialScope <> 'BRAND_ONLY';

PRINT 'CredentialScope backfilled.';
PRINT '  AMAZON_SP_API / WALMART_MP / TIKTOK_SHOP  -> APP_AND_BRAND';
PRINT '  SHOPIFY (custom apps) / EXTENSIV           -> BRAND_ONLY';
GO
