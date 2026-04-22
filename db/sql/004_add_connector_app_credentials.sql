/* =============================================================================
   Add app-level credential columns to admin.Connectors.

   Rationale:
   OAuth connectors (Amazon SP-API LWA, Walmart, TikTok, Extensiv, Shopify public
   apps) have credentials that identify YOUR application, not a specific seller.
   These are shared across all brand installs and must live at the connector
   level, not per brand.

   - AppClientID:        public-ish identifier (e.g. LWA client_id starting
                         with "amzn1.application-oa2-client.")
   - AppClientSecret_Enc: sensitive; app-layer AES-256-GCM encrypted
                         (same scheme as admin.BrandCredentials.*_Enc)

   Shopify CUSTOM apps don't need these — each store gives you a shpat_ token
   directly, which still lives in admin.BrandCredentials.ApiKey_Enc.

   Idempotent: safe to re-run.
   ============================================================================= */

SET NOCOUNT ON;
GO

IF COL_LENGTH('admin.Connectors', 'AppClientID') IS NULL
BEGIN
    ALTER TABLE admin.Connectors
        ADD AppClientID NVARCHAR(200) NULL;
    PRINT 'Added admin.Connectors.AppClientID';
END
GO

IF COL_LENGTH('admin.Connectors', 'AppClientSecret_Enc') IS NULL
BEGIN
    ALTER TABLE admin.Connectors
        ADD AppClientSecret_Enc NVARCHAR(MAX) NULL;
    PRINT 'Added admin.Connectors.AppClientSecret_Enc';
END
GO

PRINT 'admin.Connectors schema updated with app-level credentials.';
GO
