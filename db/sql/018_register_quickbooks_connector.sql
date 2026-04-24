/* =============================================================================
   Register QUICKBOOKS_ONLINE as a connector (queued, inactive).

   Run against: skc-admin

   Why:
     User goal is to pull YTD transactions from QuickBooks Online, match
     them to QBO Accounts (the chart of accounts = categories), and later
     grant SKU Compass users per-card / per-category access.

     This migration just reserves the row in admin.Connectors so the UI
     surface and future endpoint registrations have something to hang off.
     Endpoints, credentials, and the runner are all follow-up work —
     see tasks/06-register-quickbooks-endpoints.txt and
     tasks/05-build-quickbooks-skill.txt.

   Shape:
     - Name          QUICKBOOKS_ONLINE
     - AuthType      OAUTH2  (Intuit OAuth 2.0, access 1h / refresh 100d)
     - CredentialScope  APP_AND_BRAND
         Single Intuit app (AppClientID/Secret on the connector row),
         per-brand realmId + refresh token live on admin.BrandCredentials.
     - BaseURL       https://quickbooks.api.intuit.com   (prod)
                     Sandbox uses https://sandbox-quickbooks.api.intuit.com
     - ApiVersion    v3
     - IsActive      0   (queued — flip to 1 once endpoints + runner exist)

   Idempotent: safe to re-run.
   ============================================================================= */

SET NOCOUNT ON;
GO

MERGE admin.Connectors AS tgt
USING (VALUES
    ('QUICKBOOKS_ONLINE', 'QuickBooks Online API', 'OAUTH2',
     'https://quickbooks.api.intuit.com',
     'https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/account',
     'NODE', NULL, 'v3', 'APP_AND_BRAND',
     N'QUEUED. Intuit OAuth 2.0: single app (AppClientID/Secret on connector), '
       + N'per-brand realmId + refresh token on admin.BrandCredentials. '
       + N'Access token 1h, refresh 100d — runner must rotate on every call. '
       + N'Rate limit 500 req/min per realmId. Sandbox base URL: '
       + N'https://sandbox-quickbooks.api.intuit.com. '
       + N'Endpoints to register: QBO_ACCOUNTS, QBO_PURCHASES, QBO_BILLS, '
       + N'QBO_JOURNAL_ENTRIES, QBO_TRANSACTION_LIST_REPORT. '
       + N'See tasks/05-build-quickbooks-skill.txt and '
       + N'tasks/06-register-quickbooks-endpoints.txt.')
) AS src (Name, DisplayName, AuthType, BaseURL, DocsURL, RunnerType, RunnerRef,
          ApiVersion, CredentialScope, Notes)
ON tgt.Name = src.Name

WHEN MATCHED THEN
    UPDATE SET DisplayName     = src.DisplayName,
               AuthType        = src.AuthType,
               BaseURL         = src.BaseURL,
               DocsURL         = src.DocsURL,
               RunnerType      = src.RunnerType,
               ApiVersion      = src.ApiVersion,
               CredentialScope = src.CredentialScope,
               Notes           = src.Notes,
               UpdatedAt       = SYSUTCDATETIME()

WHEN NOT MATCHED BY TARGET THEN
    INSERT (Name, DisplayName, AuthType, BaseURL, DocsURL, RunnerType, RunnerRef,
            ApiVersion, CredentialScope, IsActive, Notes)
    VALUES (src.Name, src.DisplayName, src.AuthType, src.BaseURL, src.DocsURL,
            src.RunnerType, src.RunnerRef, src.ApiVersion, src.CredentialScope,
            0,   -- queued, not live yet
            src.Notes);

PRINT 'QUICKBOOKS_ONLINE connector registered (IsActive=0 — queued).';
GO
