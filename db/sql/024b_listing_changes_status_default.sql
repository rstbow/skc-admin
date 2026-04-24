/* =============================================================================
   024b — fix default Status on usp_append_amz_listing_changes.

   Run against: vs-ims-staging

   Migration 024 created this proc with an ISNULL default of 'DETECTED'
   for the Status column. But raw.amz_listing_changes has an existing
   CHECK constraint CK_amz_lc_status_vals that only allows:
     OBSERVED, PROPOSED, APPROVED, APPLIED, REVERTED, REJECTED, FAILED

   'DETECTED' isn't in that list so any insert that relied on the
   default failed with a CHECK violation. Fix: swap the default to
   'OBSERVED' which correctly represents "we noticed this change,
   no human action taken yet".

   Runner was also updated to always send Status='OBSERVED' explicitly,
   so the default is belt-and-suspenders but should still be correct.

   CREATE OR ALTER preserves the EXECUTE grant to skc_app_user.
   ============================================================================= */

SET NOCOUNT ON;
GO

CREATE OR ALTER PROCEDURE raw.usp_append_amz_listing_changes
    @BrandUID     UNIQUEIDENTIFIER,
    @SourceRunID  BIGINT,
    @RowsJson     NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    INSERT INTO raw.amz_listing_changes (
        _BrandUID, SKU, ASIN, MarketplaceID,
        ChangeSource, ChangeType, FieldPath,
        BeforeValue, AfterValue,
        Status, _SourceRunID, _IngestedAt
    )
    SELECT
        @BrandUID,
        j.SKU, j.ASIN, j.MarketplaceID,
        ISNULL(j.ChangeSource, 'SKC_ADMIN_SCHEDULER'),
        j.ChangeType,
        j.FieldPath,
        j.BeforeValue, j.AfterValue,
        ISNULL(j.Status, 'OBSERVED'),   -- was 'DETECTED' (invalid)
        @SourceRunID,
        SYSUTCDATETIME()
    FROM OPENJSON(@RowsJson)
        WITH (
            SKU            NVARCHAR(200)  '$.SKU',
            ASIN           NVARCHAR(20)   '$.ASIN',
            MarketplaceID  NVARCHAR(20)   '$.MarketplaceID',
            ChangeSource   NVARCHAR(30)   '$.ChangeSource',
            ChangeType     NVARCHAR(50)   '$.ChangeType',
            FieldPath      NVARCHAR(200)  '$.FieldPath',
            BeforeValue    NVARCHAR(MAX)  '$.BeforeValue',
            AfterValue     NVARCHAR(MAX)  '$.AfterValue',
            Status         NVARCHAR(30)   '$.Status'
        ) AS j;

    SELECT Inserted = @@ROWCOUNT;
END
GO

PRINT '024b applied: default Status on usp_append_amz_listing_changes is now OBSERVED.';
GO
