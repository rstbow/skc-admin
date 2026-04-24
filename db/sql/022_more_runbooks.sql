/* =============================================================================
   Migration 022 — seed more runbooks for observed failure modes.

   Run against: skc-admin (DML-only — no schema changes)

   Adds runbook entries for:
     - SP-API 400 (catch-all for InvalidInput / parameter validation)
     - SP-API 401 (token expired/revoked — subtly different from invalid_grant)
     - SP-API 403 (scope denial / app not authorized for the operation)

   Runner commit d6481ca + later improvements include the Amazon error
   body in e.message, so these LIKE patterns will match actual content
   like "SP-API call failed: 400 — InvalidInput: ..." and surface the
   right runbook card.

   MERGE so re-running upserts instead of duplicating.
   ============================================================================= */

SET NOCOUNT ON;
GO

MERGE admin.ErrorRunbooks AS tgt
USING (VALUES
    (N'%SP-API call failed: 400%',
     N'Amazon SP-API 400 — invalid request',
     N'Amazon rejected the request as malformed. Most common triggers for the Finances API:'
       + CHAR(10) + N'  • MarketplaceId on the credential doesn''t match the seller''s actual region'
       + CHAR(10) + N'  • PostedAfter/PostedBefore outside the allowed range (must be within last 730 days, and PostedAfter must be at least 2 minutes in the past)'
       + CHAR(10) + N'  • Missing required header (rare if using the shared callSpApi wrapper)'
       + CHAR(10) + N'The Amazon error body is captured in ErrorMessage after the em-dash — check the code/message there for the specific complaint.',
     N'1) Look at the error text — if it includes "InvalidInput" with a field name, fix that field on the credential row. '
       + N'2) If it says "InvalidParameterValue" on PostedAfter, ensure daysBack >= 1 (can''t request events from the future or from this exact moment). '
       + N'3) If the message mentions MarketplaceId, open Credentials, confirm the Region + MarketplaceIDs match Seller Central. Tessa''s Kitchen, ZenToes, etc. are all ATVPDKIKX0DER (US / Amazon.com). '
       + N'4) After a credential fix, click Run now — no redeploy needed.',
     N'ERROR'),

    (N'%SP-API call failed: 401%',
     N'Amazon SP-API 401 — token unauthorized',
     N'Our access token (minted from the refresh token) was rejected. Usually the refresh token itself is still valid, but the minted access token has expired or been revoked. Can also happen if the LWA client ID/secret on the connector got rotated and we''re still using the old pair.',
     N'1) Try the job again — callSpApi mints a fresh access token per call, so a one-off 401 usually self-heals on retry. '
       + N'2) If it persists, check admin.Connectors for AMAZON_SP_API — verify AppClientID + AppClientSecret match what''s live at developer.amazonservices.com. '
       + N'3) If the app-level creds are correct, the brand''s refresh token is likely the real problem — jump to the "Amazon LWA refresh token rejected" runbook.',
     N'WARN'),

    (N'%SP-API call failed: 403%',
     N'Amazon SP-API 403 — scope / role denial',
     N'Amazon authenticated the token but refused the specific operation. This is a SCOPE problem, not an auth problem. Finances requires the "Finance and Accounting" role to be granted on the seller''s authorization. If this is the first time hitting Finances for this brand, the role probably wasn''t included when they consented.',
     N'1) Have the seller sign in to Seller Central → Apps & Services → Manage Your Apps → find our app → "Edit app permissions". '
       + N'2) Ensure "Finance and Accounting" (and any other role the failing endpoint needs) is checked. '
       + N'3) If roles were changed, the refresh token is still valid but the token needs to be re-minted with the new scope — usually automatic on next call. If not, rotate the refresh token via the Credentials page.',
     N'ERROR')
) AS src (MatchPattern, Title, WhatItMeans, HowToFix, Severity)
ON tgt.MatchPattern = src.MatchPattern

WHEN MATCHED THEN
    UPDATE SET Title       = src.Title,
               WhatItMeans = src.WhatItMeans,
               HowToFix    = src.HowToFix,
               Severity    = src.Severity,
               UpdatedAt   = SYSUTCDATETIME()

WHEN NOT MATCHED BY TARGET THEN
    INSERT (MatchPattern, Title, WhatItMeans, HowToFix, Severity)
    VALUES (src.MatchPattern, src.Title, src.WhatItMeans, src.HowToFix, src.Severity);

PRINT 'Migration 022 complete — SP-API 400/401/403 runbooks seeded.';
GO
