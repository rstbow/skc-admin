/* =============================================================================
   Refactor raw.AmzFinancialEventsTVP to carry decimals as NVARCHAR.

   Run against: vs-ims-staging

   Why: the tedious driver's Decimal encoder computes value scale from the
   JS binary-float representation, which is noisy (0.29 becomes
   0.28999999999999998 → scale 17, fails Decimal(18,4)). Even passing
   strings doesn't help because the driver re-parses them back to numbers
   internally before scale check.

   Fix: TVP transport uses NVARCHAR(20); the MERGE proc CASTs to
   DECIMAL(18,4) on the server side. Target table raw.amz_financial_events
   is UNCHANGED — still typed DECIMAL(18,4). Only the transport changes.

   Drops + recreates TVP and proc in one go. Safe — neither holds data.

   Idempotent: safe to re-run.
   ============================================================================= */

SET NOCOUNT ON;
GO

IF OBJECT_ID('raw.usp_merge_amz_financial_events', 'P') IS NOT NULL
BEGIN
    DROP PROCEDURE raw.usp_merge_amz_financial_events;
    PRINT 'Dropped old raw.usp_merge_amz_financial_events.';
END
GO

IF TYPE_ID('raw.AmzFinancialEventsTVP') IS NOT NULL
BEGIN
    DROP TYPE raw.AmzFinancialEventsTVP;
    PRINT 'Dropped old raw.AmzFinancialEventsTVP.';
END
GO

/* ----- TVP type: decimals as strings ----- */
CREATE TYPE raw.AmzFinancialEventsTVP AS TABLE (
    EventType         NVARCHAR(50)   NOT NULL,
    ExternalID        NVARCHAR(200)  NOT NULL,
    PostedDate        DATETIMEOFFSET NULL,
    MarketplaceName   NVARCHAR(50)   NULL,
    AmazonOrderID     NVARCHAR(50)   NULL,
    ShipmentID        NVARCHAR(50)   NULL,
    AdjustmentID      NVARCHAR(50)   NULL,
    SKU               NVARCHAR(200)  NULL,
    Quantity          INT            NULL,
    Currency          NVARCHAR(3)    NULL,
    -- Decimals carried as NVARCHAR — CAST on the server side so the
    -- tedious driver never has to compute scale from JS floats.
    Principal         NVARCHAR(20)   NULL,
    Tax               NVARCHAR(20)   NULL,
    Shipping          NVARCHAR(20)   NULL,
    PromotionDiscount NVARCHAR(20)   NULL,
    Commission        NVARCHAR(20)   NULL,
    FBAFee            NVARCHAR(20)   NULL,
    OtherFees         NVARCHAR(20)   NULL,
    ServiceFeeType    NVARCHAR(100)  NULL,
    _RawPayload       NVARCHAR(MAX)  NULL,
    _SourceRowHash    VARBINARY(32)  NOT NULL,
    PRIMARY KEY (EventType, ExternalID)
);
GO
PRINT 'Created TYPE raw.AmzFinancialEventsTVP (strings for decimals).';
GO

/* ----- MERGE proc: CAST the NVARCHAR decimals to DECIMAL(18,4) on insert/update ----- */
CREATE PROCEDURE raw.usp_merge_amz_financial_events
    @BrandUID      UNIQUEIDENTIFIER,
    @SourceRunID   BIGINT,
    @Rows          raw.AmzFinancialEventsTVP READONLY
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Actions TABLE (Action NVARCHAR(10));

    MERGE raw.amz_financial_events WITH (HOLDLOCK) AS tgt
    USING (
        SELECT @BrandUID AS _BrandUID, @SourceRunID AS _SourceRunID,
               EventType, ExternalID, PostedDate, MarketplaceName,
               AmazonOrderID, ShipmentID, AdjustmentID, SKU, Quantity,
               Currency,
               TRY_CAST(Principal         AS DECIMAL(18,4)) AS Principal,
               TRY_CAST(Tax               AS DECIMAL(18,4)) AS Tax,
               TRY_CAST(Shipping          AS DECIMAL(18,4)) AS Shipping,
               TRY_CAST(PromotionDiscount AS DECIMAL(18,4)) AS PromotionDiscount,
               TRY_CAST(Commission        AS DECIMAL(18,4)) AS Commission,
               TRY_CAST(FBAFee            AS DECIMAL(18,4)) AS FBAFee,
               TRY_CAST(OtherFees         AS DECIMAL(18,4)) AS OtherFees,
               ServiceFeeType, _RawPayload, _SourceRowHash
        FROM @Rows
    ) AS src
        ON  tgt._BrandUID  = src._BrandUID
        AND tgt.EventType  = src.EventType
        AND tgt.ExternalID = src.ExternalID

    WHEN MATCHED AND (tgt._SourceRowHash IS NULL OR tgt._SourceRowHash <> src._SourceRowHash) THEN
        UPDATE SET
            PostedDate        = src.PostedDate,
            MarketplaceName   = src.MarketplaceName,
            AmazonOrderID     = src.AmazonOrderID,
            ShipmentID        = src.ShipmentID,
            AdjustmentID      = src.AdjustmentID,
            SKU               = src.SKU,
            Quantity          = src.Quantity,
            Currency          = src.Currency,
            Principal         = src.Principal,
            Tax               = src.Tax,
            Shipping          = src.Shipping,
            PromotionDiscount = src.PromotionDiscount,
            Commission        = src.Commission,
            FBAFee            = src.FBAFee,
            OtherFees         = src.OtherFees,
            ServiceFeeType    = src.ServiceFeeType,
            _RawPayload       = src._RawPayload,
            _IngestedAt       = SYSUTCDATETIME(),
            _SourceRunID      = src._SourceRunID,
            _SourceRowHash    = src._SourceRowHash

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (_BrandUID, EventType, ExternalID, PostedDate, MarketplaceName,
                AmazonOrderID, ShipmentID, AdjustmentID, SKU, Quantity,
                Currency, Principal, Tax, Shipping, PromotionDiscount,
                Commission, FBAFee, OtherFees, ServiceFeeType,
                _RawPayload, _SourceRunID, _SourceRowHash)
        VALUES (src._BrandUID, src.EventType, src.ExternalID, src.PostedDate, src.MarketplaceName,
                src.AmazonOrderID, src.ShipmentID, src.AdjustmentID, src.SKU, src.Quantity,
                src.Currency, src.Principal, src.Tax, src.Shipping, src.PromotionDiscount,
                src.Commission, src.FBAFee, src.OtherFees, src.ServiceFeeType,
                src._RawPayload, src._SourceRunID, src._SourceRowHash)

    OUTPUT $action INTO @Actions(Action);

    DECLARE @Inserted  INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'INSERT');
    DECLARE @Updated   INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'UPDATE');
    DECLARE @Total     INT = (SELECT COUNT(*) FROM @Rows);
    DECLARE @Unchanged INT = @Total - (@Inserted + @Updated);

    SELECT
        Inserted  = @Inserted,
        Updated   = @Updated,
        Unchanged = @Unchanged,
        Total     = @Total;
END
GO
PRINT 'Created PROCEDURE raw.usp_merge_amz_financial_events with CAST-based decimals.';
GO

PRINT '--------------------------------------------------';
PRINT 'Decimal-as-string TVP transport live in ' + DB_NAME() + '.';
PRINT 'The Node runner will pass NVARCHAR values; server CASTs to DECIMAL(18,4).';
PRINT '--------------------------------------------------';
GO
