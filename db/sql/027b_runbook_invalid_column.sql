/* =============================================================================
   027b — runbook entry for "Invalid column name" errors. DML only, run
   in SSMS against skc-admin.

   Hit this today: runner referenced bc.SellerID but the actual column is
   bc.AccountIdentifier. Without a runbook, "No runbook match" leaves the
   user guessing. With it, the drawer points at the likely fix.
   ============================================================================= */

USE [skc-admin];
GO

MERGE admin.ErrorRunbooks AS tgt
USING (VALUES
    (N'%Invalid column name%',
     N'SQL Server: invalid column name',
     N'Runner referenced a column that doesn''t exist on the table. Most often this is a schema-vs-code drift: someone renamed the column on the table without updating the runner''s SELECT or vice versa. Other common causes: typo, copying field names from one runner to another that has a different schema (e.g. admin.BrandCredentials uses AccountIdentifier; some runners alias it to SellerID).',
     N'1) Open the runner file referenced in the error log. 2) Find the offending column reference. 3) Verify the actual column name in the target table by querying sys.columns or running scripts/inspect-*.js. 4) Either update the runner to use the correct column name, or alias in the SELECT (e.g. AccountIdentifier AS SellerID) to keep downstream code working. 5) Commit + redeploy. 6) Retry the job — this is purely a code fix, no DB migration needed.',
     N'ERROR')
) AS src (MatchPattern, Title, WhatItMeans, HowToFix, Severity)
ON tgt.MatchPattern = src.MatchPattern

WHEN MATCHED THEN
    UPDATE SET Title = src.Title, WhatItMeans = src.WhatItMeans,
               HowToFix = src.HowToFix, Severity = src.Severity,
               UpdatedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (MatchPattern, Title, WhatItMeans, HowToFix, Severity)
    VALUES (src.MatchPattern, src.Title, src.WhatItMeans, src.HowToFix, src.Severity);

PRINT '027b applied: Invalid column name runbook seeded.';
GO
