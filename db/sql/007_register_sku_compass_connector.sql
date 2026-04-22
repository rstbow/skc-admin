/* =============================================================================
   Register SKU Compass as an internal connector in the registry.

   Why:
     SKU Compass already hosts COG (and lead times, safety stock, etc.) in
     each brand's data DB (`tbl_PPA_IMS_SKU`). Modeling SKU Compass as a
     connector with endpoints lets every internal data need flow through
     the same registry that external APIs (Amazon, Shopify, …) use.
     Future joins like "orders × COG" become "two connector endpoints"
     rather than a hand-crafted one-off query.

   Shape:
     - Connector: SKU_COMPASS   (CredentialScope=BRAND_ONLY, no app creds)
         The "credential" for a brand is its DataDbConnString already
         stored on admin.Brands — no admin.BrandCredentials row needed.
     - Endpoint:  SKC_COG         returns (brand_uid, sku, cog) rows
         Type=SQL_QUERY, target=curated.skc_sku_master.

   This migration also relaxes three CHECK constraints so the new
   type values are valid:
     - admin.Connectors.AuthType        adds 'SQL_CONNECTION','NONE'
     - admin.Connectors.RunnerType      adds 'INTERNAL_SQL'
     - admin.Endpoints.EndpointType     adds 'SQL_QUERY'

   Idempotent: safe to re-run.
   ============================================================================= */

SET NOCOUNT ON;
GO

/* 1. Relax AuthType constraint */
IF EXISTS (
    SELECT 1 FROM sys.check_constraints
    WHERE name = 'CK_Connectors_AuthType'
      AND parent_object_id = OBJECT_ID('admin.Connectors')
)
BEGIN
    ALTER TABLE admin.Connectors DROP CONSTRAINT CK_Connectors_AuthType;
    PRINT 'Dropped old CK_Connectors_AuthType.';
END
GO
ALTER TABLE admin.Connectors
    ADD CONSTRAINT CK_Connectors_AuthType
    CHECK (AuthType IN ('OAUTH2','API_KEY','CUSTOM_APP_TOKEN','BASIC','AWS_SIGV4','HMAC','SQL_CONNECTION','NONE'));
PRINT 'Added expanded CK_Connectors_AuthType.';
GO

/* 2. Relax RunnerType constraint */
IF EXISTS (
    SELECT 1 FROM sys.check_constraints
    WHERE name = 'CK_Connectors_RunnerType'
      AND parent_object_id = OBJECT_ID('admin.Connectors')
)
BEGIN
    ALTER TABLE admin.Connectors DROP CONSTRAINT CK_Connectors_RunnerType;
    PRINT 'Dropped old CK_Connectors_RunnerType.';
END
GO
ALTER TABLE admin.Connectors
    ADD CONSTRAINT CK_Connectors_RunnerType
    CHECK (RunnerType IN ('GENERIC','CUSTOM','SSIS','NODE','DURABLE_FN','INTERNAL_SQL'));
PRINT 'Added expanded CK_Connectors_RunnerType.';
GO

/* 3. Relax Endpoints.EndpointType constraint */
IF EXISTS (
    SELECT 1 FROM sys.check_constraints
    WHERE name = 'CK_Endpoints_Type'
      AND parent_object_id = OBJECT_ID('admin.Endpoints')
)
BEGIN
    ALTER TABLE admin.Endpoints DROP CONSTRAINT CK_Endpoints_Type;
    PRINT 'Dropped old CK_Endpoints_Type.';
END
GO
ALTER TABLE admin.Endpoints
    ADD CONSTRAINT CK_Endpoints_Type
    CHECK (EndpointType IN ('REPORT','REST_GET','REST_POST','GRAPHQL','GRAPHQL_BULK','WEBHOOK','SQL_QUERY'));
PRINT 'Added expanded CK_Endpoints_Type.';
GO

/* 4. Seed / upsert SKU_COMPASS connector */
MERGE admin.Connectors AS tgt
USING (VALUES
    ('SKU_COMPASS', 'SKU Compass (internal)', 'SQL_CONNECTION',
     'sql://brand-db',
     'https://skucompass.com',
     'INTERNAL_SQL', NULL, NULL, 'BRAND_ONLY',
     N'Internal data source. Connection per brand uses admin.Brands.DataDbConnString. No admin.BrandCredentials row needed.')
) AS src (Name, DisplayName, AuthType, BaseURL, DocsURL, RunnerType, RunnerRef, ApiVersion, CredentialScope, Notes)
ON tgt.Name = src.Name
WHEN MATCHED THEN
    UPDATE SET DisplayName = src.DisplayName,
               AuthType    = src.AuthType,
               BaseURL     = src.BaseURL,
               DocsURL     = src.DocsURL,
               RunnerType  = src.RunnerType,
               CredentialScope = src.CredentialScope,
               Notes       = src.Notes,
               UpdatedAt   = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (Name, DisplayName, AuthType, BaseURL, DocsURL, RunnerType, RunnerRef, ApiVersion, CredentialScope, Notes)
    VALUES (src.Name, src.DisplayName, src.AuthType, src.BaseURL, src.DocsURL,
            src.RunnerType, src.RunnerRef, src.ApiVersion, src.CredentialScope, src.Notes);
PRINT 'SKU_COMPASS connector registered.';
GO

/* 5. Seed / upsert SKC_COG endpoint */
DECLARE @skcConnectorID INT = (SELECT ConnectorID FROM admin.Connectors WHERE Name = 'SKU_COMPASS');

IF NOT EXISTS (SELECT 1 FROM admin.Endpoints WHERE ConnectorID = @skcConnectorID AND Name = 'SKC_COG')
BEGIN
    INSERT INTO admin.Endpoints (
        ConnectorID, Name, DisplayName, Description, EndpointType,
        TargetSchema, TargetTable, NaturalKeyColumns,
        ParamsTemplate, Notes
    ) VALUES (
        @skcConnectorID,
        'SKC_COG',
        'Cost of Goods — per brand × SKU',
        N'Returns (brand_uid, sku, cog) rows from tbl_PPA_IMS_SKU on the brand''s data DB. '
          + N'COG column is auto-discovered (tries COG, Cost, UnitCost, COGS, Item_COG, Unit_Cost).',
        'SQL_QUERY',
        'curated',
        'skc_sku_master',
        'brand_uid,sku',
        N'{"table":"tbl_PPA_IMS_SKU","cogCandidates":["COG","Cost","UnitCost","COGS","Item_COG","Unit_Cost"]}',
        N'This is a live query endpoint, not a scheduled pull. The Phase 3 runner can also materialize it into curated.skc_sku_master if consumers want cached data.'
    );
    PRINT 'SKC_COG endpoint registered.';
END
ELSE
    PRINT 'SKC_COG endpoint already exists, skipping.';
GO

/* Optional: stub additional SKU Compass endpoints for future use */
DECLARE @skcConnectorID INT = (SELECT ConnectorID FROM admin.Connectors WHERE Name = 'SKU_COMPASS');

IF NOT EXISTS (SELECT 1 FROM admin.Endpoints WHERE ConnectorID = @skcConnectorID AND Name = 'SKC_SKU_MASTER')
BEGIN
    INSERT INTO admin.Endpoints (
        ConnectorID, Name, DisplayName, Description, EndpointType,
        TargetSchema, TargetTable, NaturalKeyColumns,
        ParamsTemplate, IsActive, Notes
    ) VALUES (
        @skcConnectorID,
        'SKC_SKU_MASTER',
        'SKU Master (full tbl_PPA_IMS_SKU)',
        N'Full SKU master row per SKU — lead times, safety stock, supplier, dimensions, COG, images, etc.',
        'SQL_QUERY',
        'curated',
        'skc_sku_master_full',
        'brand_uid,sku',
        N'{"table":"tbl_PPA_IMS_SKU"}',
        0,  -- inactive until a runner consumes it
        N'Reserved for future use. Activate when a consumer needs full SKU metadata rather than just COG.'
    );
    PRINT 'SKC_SKU_MASTER endpoint registered (inactive).';
END
GO

PRINT '--------------------------------------------------';
PRINT 'SKU Compass connector + SKC_COG endpoint are live.';
PRINT '--------------------------------------------------';
GO
