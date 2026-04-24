/* =============================================================================
   017c — fix MERGE to tolerate duplicate source rows in the same batch.

   Run against: vs-ims-staging

   Why:
     Observed PK violation:
       Violation of PRIMARY KEY constraint 'PK_amz_financial_events'.
       Cannot insert duplicate key in object 'raw.amz_financial_events'.

     Root cause: when the SAME natural key (EventType, ExternalID) appears
     twice in a single @RowsJson batch — which happens when SP-API paging
     overlaps, or a retry re-fetches a page — MERGE processes both source
     rows as NOT MATCHED and tries to INSERT twice. The second INSERT hits
     the PK. MERGE does NOT dedupe its source; that's on us.

     Fix: wrap the OPENJSON projection so every (EventType, ExternalID)
     surfaces exactly once via ROW_NUMBER() ... PARTITION BY EventType,
     ExternalID. If duplicates share the same _SourceRowHash, either row
     works equivalently; ORDER BY (SELECT 1) is deterministic-enough for
     our purposes.

     Also switched DROP + CREATE → CREATE OR ALTER so grants (skc_app_user
     EXECUTE) are preserved. Lesson from 017/017b.

   Idempotent: CREATE OR ALTER by definition.
   ============================================================================= */

SET NOCOUNT ON;
GO

CREATE OR ALTER PROCEDURE raw.usp_merge_amz_financial_events
    @BrandUID     UNIQUEIDENTIFIER,
    @SourceRunID  BIGINT,
    @RowsJson     NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Actions TABLE (Action NVARCHAR(10));

    ;WITH parsed AS (
        SELECT
            j.EventType, j.ExternalID, j.PostedDate, j.MarketplaceName,
            j.AmazonOrderID, j.ShipmentID, j.AdjustmentID, j.SKU, j.Quantity,
            j.Currency,
            TRY_CAST(j.Principal         AS DECIMAL(18,4)) AS Principal,
            TRY_CAST(j.Tax               AS DECIMAL(18,4)) AS Tax,
            TRY_CAST(j.Shipping          AS DECIMAL(18,4)) AS Shipping,
            TRY_CAST(j.PromotionDiscount AS DECIMAL(18,4)) AS PromotionDiscount,
            TRY_CAST(j.Commission        AS DECIMAL(18,4)) AS Commission,
            TRY_CAST(j.FBAFee            AS DECIMAL(18,4)) AS FBAFee,
            TRY_CAST(j.OtherFees         AS DECIMAL(18,4)) AS OtherFees,
            j.ServiceFeeType, j._RawPayload,
            CONVERT(VARBINARY(32), j._SourceRowHashHex, 2) AS _SourceRowHash,
            ROW_NUMBER() OVER (
                PARTITION BY j.EventType, j.ExternalID
                ORDER BY (SELECT 1)
            ) AS _rn
        FROM OPENJSON(@RowsJson)
            WITH (
                EventType         NVARCHAR(50)   '$.EventType',
                ExternalID        NVARCHAR(200)  '$.ExternalID',
                PostedDate        DATETIMEOFFSET '$.PostedDate',
                MarketplaceName   NVARCHAR(50)   '$.MarketplaceName',
                AmazonOrderID     NVARCHAR(50)   '$.AmazonOrderID',
                ShipmentID        NVARCHAR(50)   '$.ShipmentID',
                AdjustmentID      NVARCHAR(50)   '$.AdjustmentID',
                SKU               NVARCHAR(200)  '$.SKU',
                Quantity          INT            '$.Quantity',
                Currency          NVARCHAR(3)    '$.Currency',
                Principal         NVARCHAR(20)   '$.Principal',
                Tax               NVARCHAR(20)   '$.Tax',
                Shipping          NVARCHAR(20)   '$.Shipping',
                PromotionDiscount NVARCHAR(20)   '$.PromotionDiscount',
                Commission        NVARCHAR(20)   '$.Commission',
                FBAFee            NVARCHAR(20)   '$.FBAFee',
                OtherFees         NVARCHAR(20)   '$.OtherFees',
                ServiceFeeType    NVARCHAR(100)  '$.ServiceFeeType',
                _RawPayload       NVARCHAR(MAX)  '$._RawPayload',
                _SourceRowHashHex NVARCHAR(64)   '$._SourceRowHashHex'
            ) AS j
    )
    MERGE raw.amz_financial_events WITH (HOLDLOCK) AS tgt
    USING (
        SELECT @BrandUID AS _BrandUID, @SourceRunID AS _SourceRunID,
               EventType, ExternalID, PostedDate, MarketplaceName,
               AmazonOrderID, ShipmentID, AdjustmentID, SKU, Quantity,
               Currency, Principal, Tax, Shipping, PromotionDiscount,
               Commission, FBAFee, OtherFees, ServiceFeeType,
               _RawPayload, _SourceRowHash
        FROM parsed
        WHERE _rn = 1   -- dedupe duplicate natural keys from pagination overlap
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
    DECLARE @TotalRows INT = (SELECT COUNT(*) FROM OPENJSON(@RowsJson));
    DECLARE @Unchanged INT = @TotalRows - (@Inserted + @Updated);

    SELECT
        Inserted  = @Inserted,
        Updated   = @Updated,
        Unchanged = @Unchanged,
        Total     = @TotalRows;
END
GO

PRINT 'Updated raw.usp_merge_amz_financial_events with PARTITION BY dedupe.';
GO
