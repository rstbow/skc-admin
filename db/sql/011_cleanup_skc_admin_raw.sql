/* =============================================================================
   CLEANUP: removes raw.* tables mistakenly created in skc-admin.

   Run against: skc-admin  (NOT the brand data DB)

   Context: 009_phase3_brand_data_tables.sql targets the brand data DB
   (vs-ims-staging). If it was accidentally run against skc-admin, it created
   a raw schema + 17 empty tables there. This script tears them back out so
   skc-admin stays clean (admin.* only).

   Non-destructive:
     - Every DROP is guarded by an existence check
     - RCSI setting on skc-admin is LEFT ENABLED (it's a net positive setting)
     - No data loss — tables are empty

   Idempotent: safe to run multiple times.
   ============================================================================= */

SET NOCOUNT ON;
GO

DECLARE @dropped INT = 0;

IF OBJECT_ID('raw.amz_awd_shipments', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_awd_shipments;           SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_awd_inventory', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_awd_inventory;           SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_sns_forecast', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_sns_forecast;            SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_fba_inventory_aged', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_fba_inventory_aged;      SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_ltsf_charges', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_ltsf_charges;            SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_storage_fees_monthly', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_storage_fees_monthly;    SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_ledger_detail', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_ledger_detail;           SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_fba_reimbursements', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_fba_reimbursements;      SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_settlement_v2', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_settlement_v2;           SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_listings_catalog', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_listings_catalog;        SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_listing_changes', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_listing_changes;         SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_listings', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_listings;                SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_restock_recommendations', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_restock_recommendations; SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_fba_inventory', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_fba_inventory;           SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_order_items', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_order_items;             SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_orders', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_orders;                  SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_financial_events', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_financial_events;        SET @dropped = @dropped + 1; END;
IF OBJECT_ID('raw.amz_returns', 'U') IS NOT NULL
BEGIN DROP TABLE raw.amz_returns;                 SET @dropped = @dropped + 1; END;

PRINT 'Dropped ' + CAST(@dropped AS VARCHAR) + ' raw.amz_* tables.';
GO

/* Drop the raw schema if (and only if) empty now */
IF SCHEMA_ID('raw') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID('raw'))
BEGIN
    EXEC('DROP SCHEMA raw;');
    PRINT 'Dropped empty raw schema.';
END
ELSE IF SCHEMA_ID('raw') IS NOT NULL
    PRINT 'raw schema still has other objects — left in place.';
ELSE
    PRINT 'raw schema already absent.';
GO

PRINT 'skc-admin cleanup complete. admin.* is untouched; raw.* is gone.';
GO
