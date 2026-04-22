/* =============================================================================
   HOTFIX: creates raw.amz_listing_changes if it failed to create in 009.

   Run against: vs-ims-staging (the brand data DB)

   Background: the original DDL in 009 hit a parser error on the Status column.
   This version uses bracketed [Status] everywhere and separates the DEFAULT +
   CHECK constraints for cleaner parsing.

   Idempotent: safe to re-run. If the table already exists (e.g. 009 succeeded
   on a later re-run), this script is a no-op.
   ============================================================================= */

SET NOCOUNT ON;
GO

IF SCHEMA_ID('raw') IS NULL
    EXEC('CREATE SCHEMA raw AUTHORIZATION dbo;');
GO

IF OBJECT_ID('raw.amz_listing_changes', 'U') IS NOT NULL
BEGIN
    PRINT 'raw.amz_listing_changes already exists — nothing to do.';
    RETURN;
END
GO

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
    [Status]            NVARCHAR(30)         NOT NULL CONSTRAINT DF_amz_lc_status DEFAULT ('OBSERVED'),
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
    _IngestedAt         DATETIME2(3)         NOT NULL CONSTRAINT DF_amz_lc_ing DEFAULT (SYSUTCDATETIME()),
    _SourceRunID        BIGINT               NULL,
    CONSTRAINT PK_amz_listing_changes PRIMARY KEY CLUSTERED (ChangeID)
);
GO

ALTER TABLE raw.amz_listing_changes
    ADD CONSTRAINT CK_amz_lc_status_vals
    CHECK ([Status] IN ('OBSERVED','PROPOSED','APPROVED','APPLIED','REVERTED','REJECTED','FAILED'));
GO

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

PRINT 'Created raw.amz_listing_changes (hotfix).';
GO
