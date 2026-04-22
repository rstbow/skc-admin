/* =============================================================================
   Phase 3 runner infrastructure: TVP type + MERGE proc for amz_financial_events.

   Run against: vs-ims-staging

   Shape of the write path:
     Node runner builds a TVP batch of ~1000 flattened events →
     exec raw.usp_merge_amz_financial_events (@BrandUID, @SourceRunID, @Rows) →
     MERGE WITH HOLDLOCK, hash-gated WHEN MATCHED, no DELETEs.

   Idempotent re-runs:
     - Same data in → zero writes (hash match skips the UPDATE branch).
     - Revised amounts in → single targeted UPDATE per changed row.
     - New events → INSERT.
     - Amazon never "unlists" events, so we never DELETE.

   Returns inserted/updated counts via OUTPUT capture so the runner can
   populate admin.JobRuns accurately.
   ============================================================================= */

SET NOCOUNT ON;
GO

/* -----------------------------------------------------------------------
   1. Table-valued parameter type matching raw.amz_financial_events
   ----------------------------------------------------------------------- */
IF TYPE_ID('raw.AmzFinancialEventsTVP') IS NOT NULL
    DROP TYPE raw.AmzFinancialEventsTVP;
GO

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
    Principal         DECIMAL(18,4)  NULL,
    Tax               DECIMAL(18,4)  NULL,
    Shipping          DECIMAL(18,4)  NULL,
    PromotionDiscount DECIMAL(18,4)  NULL,
    Commission        DECIMAL(18,4)  NULL,
    FBAFee            DECIMAL(18,4)  NULL,
    OtherFees         DECIMAL(18,4)  NULL,
    ServiceFeeType    NVARCHAR(100)  NULL,
    _RawPayload       NVARCHAR(MAX)  NULL,
    _SourceRowHash    VARBINARY(32)  NOT NULL,
    PRIMARY KEY (EventType, ExternalID)
);
GO
PRINT 'Created TYPE raw.AmzFinancialEventsTVP.';
GO

/* -----------------------------------------------------------------------
   2. MERGE procedure
   ----------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE raw.usp_merge_amz_financial_events
    @BrandUID      UNIQUEIDENTIFIER,
    @SourceRunID   BIGINT,
    @Rows          raw.AmzFinancialEventsTVP READONLY
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    /* Track action per row so runner can report inserted/updated/unchanged */
    DECLARE @Actions TABLE (Action NVARCHAR(10));

    MERGE raw.amz_financial_events WITH (HOLDLOCK) AS tgt
    USING (
        SELECT @BrandUID AS _BrandUID, @SourceRunID AS _SourceRunID, *
        FROM @Rows
    ) AS src
        ON  tgt._BrandUID  = src._BrandUID
        AND tgt.EventType  = src.EventType
        AND tgt.ExternalID = src.ExternalID

    /* Only overwrite when the source row actually changed.
       Protects against log/trigger/CDC churn on re-pulls. */
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

    /* No DELETE — Amazon financial events are append-only. */

    OUTPUT $action INTO @Actions(Action);

    /* Return counts: inserted / updated / unchanged (unchanged = source rows
       whose hash matched target, so they fell through MERGE without action). */
    DECLARE @Inserted INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'INSERT');
    DECLARE @Updated  INT = (SELECT COUNT(*) FROM @Actions WHERE Action = 'UPDATE');
    DECLARE @Total    INT = (SELECT COUNT(*) FROM @Rows);
    DECLARE @Unchanged INT = @Total - (@Inserted + @Updated);

    SELECT
        Inserted  = @Inserted,
        Updated   = @Updated,
        Unchanged = @Unchanged,
        Total     = @Total;
END
GO
PRINT 'Created PROCEDURE raw.usp_merge_amz_financial_events.';
GO

PRINT '--------------------------------------------------';
PRINT 'Phase 3 runner infrastructure ready in ' + DB_NAME() + '.';
PRINT 'Test:';
PRINT '  DECLARE @T raw.AmzFinancialEventsTVP;';
PRINT '  -- populate @T then:';
PRINT '  EXEC raw.usp_merge_amz_financial_events';
PRINT '       @BrandUID    = ''00000000-0000-0000-0000-000000000000'',';
PRINT '       @SourceRunID = 1,';
PRINT '       @Rows        = @T;';
PRINT '--------------------------------------------------';
GO
