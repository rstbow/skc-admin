/* =============================================================================
   REBUILD: drops and recreates raw.amz_listing_changes from scratch.

   Run against: vs-ims-staging

   Why rebuild (not alter): earlier migration attempts left a malformed version
   of this table in place (missing Status and _IngestedAt columns based on the
   errors observed). Simpler and safer to drop + recreate since the table has
   no ingested data yet.

   SAFETY GUARDRAIL: the DROP is blocked if any rows exist. If this script
   fails the guard, something DID load data — stop and investigate before
   forcing the rebuild.
   ============================================================================= */

SET NOCOUNT ON;
GO

/* Schema insurance */
IF SCHEMA_ID('raw') IS NULL EXEC('CREATE SCHEMA raw AUTHORIZATION dbo;');
GO

/* Guard: refuse to drop if table has any rows */
IF OBJECT_ID('raw.amz_listing_changes', 'U') IS NOT NULL
BEGIN
    DECLARE @cnt BIGINT;
    SELECT @cnt = COUNT_BIG(*) FROM raw.amz_listing_changes;
    IF @cnt > 0
    BEGIN
        RAISERROR(N'raw.amz_listing_changes has data (%I64d rows) — refusing to drop. Investigate before continuing.', 16, 1, @cnt);
        RETURN;
    END
    DROP TABLE raw.amz_listing_changes;
    PRINT 'Dropped existing empty raw.amz_listing_changes.';
END
ELSE
    PRINT 'raw.amz_listing_changes did not exist yet; will create fresh.';
GO

/* Create table (Status bracketed, no inline comments in declaration list) */
CREATE TABLE raw.amz_listing_changes (
    ChangeID            BIGINT IDENTITY(1,1) NOT NULL,
    _BrandUID           UNIQUEIDENTIFIER     NOT NULL,
    SKU                 NVARCHAR(200)        NOT NULL,
    ASIN                NVARCHAR(20)         NULL,
    MarketplaceID       NVARCHAR(20)         NULL,
    ChangeSource        NVARCHAR(30)         NOT NULL,
    ChangeType          NVARCHAR(50)         NOT NULL,
    FieldPath           NVARCHAR(200)        NULL,
    BeforeValue         NVARCHAR(MAX)        NULL,
    AfterValue          NVARCHAR(MAX)        NULL,
    [Status]            NVARCHAR(30)         NOT NULL,
    AmazonSubmissionID  NVARCHAR(100)        NULL,
    AmazonResponseJSON  NVARCHAR(MAX)        NULL,
    AISuggestionID      BIGINT               NULL,
    AIRationale         NVARCHAR(MAX)        NULL,
    AIConfidence        DECIMAL(5,2)         NULL,
    ProposedBy          INT                  NULL,
    ProposedAt          DATETIME2(3)         NULL,
    ApprovedBy          INT                  NULL,
    ApprovedAt          DATETIME2(3)         NULL,
    AppliedAt           DATETIME2(3)         NULL,
    RevertedAt          DATETIME2(3)         NULL,
    RevertedByChangeID  BIGINT               NULL,
    _RawPayload         NVARCHAR(MAX)        NULL,
    _IngestedAt         DATETIME2(3)         NOT NULL,
    _SourceRunID        BIGINT               NULL,
    CONSTRAINT PK_amz_listing_changes PRIMARY KEY CLUSTERED (ChangeID)
);
GO

/* Defaults + checks as separate statements (cleanest parse path) */
ALTER TABLE raw.amz_listing_changes
    ADD CONSTRAINT DF_amz_lc_status   DEFAULT ('OBSERVED')       FOR [Status];
GO
ALTER TABLE raw.amz_listing_changes
    ADD CONSTRAINT DF_amz_lc_ingested DEFAULT (SYSUTCDATETIME()) FOR _IngestedAt;
GO
ALTER TABLE raw.amz_listing_changes
    ADD CONSTRAINT CK_amz_lc_status_vals
    CHECK ([Status] IN ('OBSERVED','PROPOSED','APPROVED','APPLIED','REVERTED','REJECTED','FAILED'));
GO

/* Indexes */
CREATE INDEX IX_amz_lc_brand_sku
    ON raw.amz_listing_changes (_BrandUID, SKU, _IngestedAt DESC);
GO
CREATE INDEX IX_amz_lc_status
    ON raw.amz_listing_changes (_BrandUID, [Status], _IngestedAt DESC);
GO
CREATE INDEX IX_amz_lc_applied_for_rb
    ON raw.amz_listing_changes (_BrandUID, SKU, AppliedAt DESC)
    INCLUDE (ChangeID, ChangeType, BeforeValue)
    WHERE [Status] = 'APPLIED';
GO

PRINT '-----------------------------------------------------------';
PRINT 'raw.amz_listing_changes rebuilt with clean structure.';
PRINT 'Verify with:';
PRINT '  SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE';
PRINT '  FROM INFORMATION_SCHEMA.COLUMNS';
PRINT '  WHERE TABLE_SCHEMA = ''raw'' AND TABLE_NAME = ''amz_listing_changes''';
PRINT '  ORDER BY ORDINAL_POSITION;';
PRINT '  -- Expected: 26 columns';
PRINT '-----------------------------------------------------------';
GO
