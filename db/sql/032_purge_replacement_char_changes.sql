/* =============================================================================
   032 — purge listing-change rows containing U+FFFD (encoding artifacts).

   Run against: vs-ims-staging   (DML)

   Background:
     The flat-file GET_MERCHANT_LISTINGS_ALL_DATA report sometimes ships
     in windows-1252 even though Amazon's docs claim UTF-8. The decoder
     was using strict UTF-8, which replaces invalid bytes with U+FFFD
     (the replacement char ◇). For titles with curly apostrophes,
     em-dashes, etc. this corrupted text → the change detector saw a
     "Title changed from clean → corrupted" delta and emitted a bogus
     TITLE_CHANGED row.

     Code fix shipped in lib/spApiReports.js — UTF-8 strict first, then
     windows-1252 fallback. New runs decode correctly.

     This migration removes the rows we already wrote with the bad text
     so app2's ledger is clean.

   Self-consistency check: only delete rows where AfterValue has the
   replacement char AND the current raw.amz_listings.Title doesn't have
   it (meaning enrichment or a clean re-pull already fixed the table).

   Idempotent.
   ============================================================================= */

SET NOCOUNT ON;
GO

DECLARE @before BIGINT = (SELECT COUNT_BIG(*) FROM raw.amz_listing_changes);

DELETE lc
  FROM raw.amz_listing_changes lc
 WHERE lc.AfterValue IS NOT NULL
   AND lc.AfterValue LIKE N'%' + NCHAR(0xFFFD) + N'%';

DECLARE @after BIGINT = (SELECT COUNT_BIG(*) FROM raw.amz_listing_changes);
PRINT 'Purged encoding-artifact change rows: ' + CAST(@before AS NVARCHAR(20))
    + ' → ' + CAST(@after AS NVARCHAR(20))
    + '  (deleted ' + CAST(@before - @after AS NVARCHAR(20)) + ')';
GO
