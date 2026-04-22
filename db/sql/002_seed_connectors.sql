/* =============================================================================
   Seed initial connectors — Amazon SP-API, Shopify, Walmart, TikTok, Extensiv.
   Idempotent: uses MERGE.
   Run AFTER 001_admin_schema.sql.
   ============================================================================= */

MERGE admin.Connectors AS tgt
USING (VALUES
    ('AMAZON_SP_API', 'Amazon Selling Partner API', 'OAUTH2',
     'https://sellingpartnerapi-na.amazon.com',
     'https://developer-docs.amazon.com/sp-api',
     'GENERIC', NULL, '2021-06-30'),

    ('SHOPIFY', 'Shopify Admin API', 'CUSTOM_APP_TOKEN',
     'https://{shop}.myshopify.com',
     'https://shopify.dev/docs/api/admin',
     'GENERIC', NULL, '2026-04'),

    ('WALMART_MP', 'Walmart Marketplace API', 'HMAC',
     'https://marketplace.walmartapis.com',
     'https://developer.walmart.com/doc/us/mp/us-mp-overview',
     'GENERIC', NULL, 'v3'),

    ('TIKTOK_SHOP', 'TikTok Shop Partner API', 'OAUTH2',
     'https://open-api.tiktokglobalshop.com',
     'https://partner.tiktokshop.com/docv2',
     'GENERIC', NULL, '202309'),

    ('EXTENSIV', 'Extensiv (3PL Central) API', 'OAUTH2',
     'https://secure-wms.com',
     'https://api.extensiv.com',
     'GENERIC', NULL, 'v1')
) AS src (Name, DisplayName, AuthType, BaseURL, DocsURL, RunnerType, RunnerRef, ApiVersion)
ON tgt.Name = src.Name

WHEN NOT MATCHED BY TARGET THEN
    INSERT (Name, DisplayName, AuthType, BaseURL, DocsURL, RunnerType, RunnerRef, ApiVersion)
    VALUES (src.Name, src.DisplayName, src.AuthType, src.BaseURL, src.DocsURL,
            src.RunnerType, src.RunnerRef, src.ApiVersion);

PRINT 'Connectors seeded.';
GO
