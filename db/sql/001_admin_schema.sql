/* =============================================================================
   SKU Compass Admin — Control Plane Schema
   Database: skc-admin (on vs-ims.database.windows.net)
   Phase: 1 (control plane only; workers come in Phase 3+)

   Run this entire script in SSMS against the skc-admin database.
   Idempotent: safe to re-run.
   ============================================================================= */

SET NOCOUNT ON;
GO

/* =============================================================================
   Schema
   ============================================================================= */
IF SCHEMA_ID('admin') IS NULL EXEC('CREATE SCHEMA [admin] AUTHORIZATION dbo;');
GO

/* =============================================================================
   admin.Users — admin tool login accounts (separate from SKC_Users in skc-auth-dev)
   ============================================================================= */
IF OBJECT_ID('admin.Users', 'U') IS NULL
BEGIN
    CREATE TABLE admin.Users (
        UserID          INT IDENTITY(1,1) NOT NULL,
        UserUID         UNIQUEIDENTIFIER  NOT NULL CONSTRAINT DF_Users_UID DEFAULT (NEWID()),
        Email           NVARCHAR(320)     NOT NULL,
        DisplayName     NVARCHAR(100)     NULL,
        PasswordHash    NVARCHAR(200)     NOT NULL,  -- bcrypt hash
        IsActive        BIT               NOT NULL CONSTRAINT DF_Users_IsActive DEFAULT (1),
        IsSuperAdmin    BIT               NOT NULL CONSTRAINT DF_Users_IsSuper DEFAULT (0),
        LastLoginAt     DATETIME2(3)      NULL,
        CreatedAt       DATETIME2(3)      NOT NULL CONSTRAINT DF_Users_Created DEFAULT (SYSUTCDATETIME()),
        UpdatedAt       DATETIME2(3)      NOT NULL CONSTRAINT DF_Users_Updated DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_Users PRIMARY KEY CLUSTERED (UserID),
        CONSTRAINT UQ_Users_UID UNIQUE NONCLUSTERED (UserUID),
        CONSTRAINT UQ_Users_Email UNIQUE NONCLUSTERED (Email)
    );
END
GO

/* =============================================================================
   admin.Connectors — data source families (Amazon, Shopify, Walmart, TikTok, ...)
   ============================================================================= */
IF OBJECT_ID('admin.Connectors', 'U') IS NULL
BEGIN
    CREATE TABLE admin.Connectors (
        ConnectorID         INT IDENTITY(1,1) NOT NULL,
        ConnectorUID        UNIQUEIDENTIFIER  NOT NULL CONSTRAINT DF_Connectors_UID DEFAULT (NEWID()),
        Name                NVARCHAR(50)      NOT NULL,   -- e.g. 'AMAZON_SP_API', 'SHOPIFY'
        DisplayName         NVARCHAR(100)     NOT NULL,   -- e.g. 'Amazon Selling Partner API'
        AuthType            NVARCHAR(30)      NOT NULL,   -- 'OAUTH2','API_KEY','CUSTOM_APP_TOKEN','BASIC','AWS_SIGV4'
        BaseURL             NVARCHAR(500)     NOT NULL,
        DocsURL             NVARCHAR(500)     NULL,
        DefaultRateLimitRPM INT               NULL,
        RunnerType          NVARCHAR(30)      NOT NULL CONSTRAINT DF_Connectors_Runner DEFAULT ('GENERIC'),
        RunnerRef           NVARCHAR(200)     NULL,       -- e.g. 'AMZ_Report_Runner.dtsx' or 'node:amazon-reports'
        ApiVersion          NVARCHAR(20)      NULL,       -- e.g. '2026-04' for Shopify, '2021-06-30' for SP-API Reports
        IsActive            BIT               NOT NULL CONSTRAINT DF_Connectors_Active DEFAULT (1),
        Notes               NVARCHAR(MAX)     NULL,
        CreatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_Connectors_Created DEFAULT (SYSUTCDATETIME()),
        UpdatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_Connectors_Updated DEFAULT (SYSUTCDATETIME()),
        CreatedBy           INT               NULL,
        UpdatedBy           INT               NULL,
        CONSTRAINT PK_Connectors PRIMARY KEY CLUSTERED (ConnectorID),
        CONSTRAINT UQ_Connectors_UID UNIQUE NONCLUSTERED (ConnectorUID),
        CONSTRAINT UQ_Connectors_Name UNIQUE NONCLUSTERED (Name),
        CONSTRAINT CK_Connectors_RunnerType CHECK (RunnerType IN ('GENERIC','CUSTOM','SSIS','NODE','DURABLE_FN')),
        CONSTRAINT CK_Connectors_AuthType CHECK (AuthType IN ('OAUTH2','API_KEY','CUSTOM_APP_TOKEN','BASIC','AWS_SIGV4','HMAC'))
    );
END
GO

/* =============================================================================
   admin.Endpoints — a single report/API call within a connector.
   This is the "registry row" that defines what gets pulled.
   ============================================================================= */
IF OBJECT_ID('admin.Endpoints', 'U') IS NULL
BEGIN
    CREATE TABLE admin.Endpoints (
        EndpointID          INT IDENTITY(1,1) NOT NULL,
        EndpointUID         UNIQUEIDENTIFIER  NOT NULL CONSTRAINT DF_Endpoints_UID DEFAULT (NEWID()),
        ConnectorID         INT               NOT NULL,
        Name                NVARCHAR(100)     NOT NULL,   -- e.g. 'AMZ_FBA_INVENTORY_BY_COUNTRY', 'SHOP_ORDERS_BULK'
        DisplayName         NVARCHAR(200)     NOT NULL,
        Description         NVARCHAR(MAX)     NULL,
        EndpointType        NVARCHAR(30)      NOT NULL,   -- 'REPORT','REST_GET','REST_POST','GRAPHQL','GRAPHQL_BULK','WEBHOOK'
        HttpMethod          NVARCHAR(10)      NULL,
        Path                NVARCHAR(500)     NULL,       -- relative path or report type code
        ParamsTemplate      NVARCHAR(MAX)     NULL,       -- JSON template with {brand.*} placeholders
        PaginationStrategy  NVARCHAR(30)      NULL,       -- 'NONE','CURSOR','OFFSET','NEXT_TOKEN','LINK_HEADER','GRAPHQL_CURSOR'
        PollIntervalSec     INT               NULL,       -- for async reports
        PollMaxAttempts     INT               NULL,
        TargetSchema        NVARCHAR(50)      NOT NULL CONSTRAINT DF_Endpoints_TSchema DEFAULT ('raw'),
        TargetTable         NVARCHAR(128)     NOT NULL,   -- in the brand's data DB
        NaturalKeyColumns   NVARCHAR(500)     NULL,       -- comma-separated, for MERGE
        TransformProc       NVARCHAR(200)     NULL,       -- optional stored proc to call post-insert
        RateLimitWeight     INT               NOT NULL CONSTRAINT DF_Endpoints_RLW DEFAULT (1),
        Version             INT               NOT NULL CONSTRAINT DF_Endpoints_Ver DEFAULT (1),
        IsActive            BIT               NOT NULL CONSTRAINT DF_Endpoints_Active DEFAULT (1),
        Notes               NVARCHAR(MAX)     NULL,
        CreatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_Endpoints_Created DEFAULT (SYSUTCDATETIME()),
        UpdatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_Endpoints_Updated DEFAULT (SYSUTCDATETIME()),
        CreatedBy           INT               NULL,
        UpdatedBy           INT               NULL,
        CONSTRAINT PK_Endpoints PRIMARY KEY CLUSTERED (EndpointID),
        CONSTRAINT UQ_Endpoints_UID UNIQUE NONCLUSTERED (EndpointUID),
        CONSTRAINT UQ_Endpoints_ConnectorName UNIQUE NONCLUSTERED (ConnectorID, Name, Version),
        CONSTRAINT FK_Endpoints_Connector FOREIGN KEY (ConnectorID) REFERENCES admin.Connectors (ConnectorID),
        CONSTRAINT CK_Endpoints_Type CHECK (EndpointType IN ('REPORT','REST_GET','REST_POST','GRAPHQL','GRAPHQL_BULK','WEBHOOK'))
    );
END
GO

/* =============================================================================
   admin.Brands — known brands (references skc-auth-dev.SKC_Brands.BrandUID)
   Kept lean here — the authoritative brand record stays in skc-auth-dev.
   ============================================================================= */
IF OBJECT_ID('admin.Brands', 'U') IS NULL
BEGIN
    CREATE TABLE admin.Brands (
        BrandUID            UNIQUEIDENTIFIER  NOT NULL,   -- matches skc-auth-dev.SKC_Brands.BrandUID
        BrandName           NVARCHAR(200)     NOT NULL,
        BrandSlug           NVARCHAR(100)     NOT NULL,
        DataDbConnString    NVARCHAR(MAX)     NULL,       -- cached snapshot; source of truth is skc-auth-dev.SKC_Brands
        IsActive            BIT               NOT NULL CONSTRAINT DF_Brands_Active DEFAULT (1),
        SyncedAt            DATETIME2(3)      NULL,       -- last time we synced from skc-auth-dev
        CreatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_Brands_Created DEFAULT (SYSUTCDATETIME()),
        UpdatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_Brands_Updated DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_Brands PRIMARY KEY CLUSTERED (BrandUID),
        CONSTRAINT UQ_Brands_Slug UNIQUE NONCLUSTERED (BrandSlug)
    );
END
GO

/* =============================================================================
   admin.BrandCredentials — per-brand auth material.
   Phase 1: plain NVARCHAR columns with app-side encryption (bcrypt/Node crypto).
   Phase 2+: migrate to SQL Always Encrypted OR Azure Key Vault references.
   ============================================================================= */
IF OBJECT_ID('admin.BrandCredentials', 'U') IS NULL
BEGIN
    CREATE TABLE admin.BrandCredentials (
        CredentialID            INT IDENTITY(1,1) NOT NULL,
        BrandUID                UNIQUEIDENTIFIER  NOT NULL,
        ConnectorID             INT               NOT NULL,
        AccountIdentifier       NVARCHAR(200)     NULL,   -- e.g. Amazon seller ID, Shopify shop domain
        MarketplaceIDs          NVARCHAR(500)     NULL,   -- comma-separated, JSON, or NULL
        RefreshToken_Enc        NVARCHAR(MAX)     NULL,   -- encrypted (app-layer, Node crypto AES-256-GCM)
        AccessToken_Enc         NVARCHAR(MAX)     NULL,   -- encrypted short-lived
        AccessTokenExpiresAt    DATETIME2(3)      NULL,
        ApiKey_Enc              NVARCHAR(MAX)     NULL,   -- for API_KEY / CUSTOM_APP_TOKEN auth
        AppSecret_Enc           NVARCHAR(MAX)     NULL,   -- for webhook HMAC verification
        ExtraConfig             NVARCHAR(MAX)     NULL,   -- JSON for connector-specific extras
        IsActive                BIT               NOT NULL CONSTRAINT DF_Creds_Active DEFAULT (1),
        LastAuthedAt            DATETIME2(3)      NULL,
        LastAuthError           NVARCHAR(MAX)     NULL,
        CreatedAt               DATETIME2(3)      NOT NULL CONSTRAINT DF_Creds_Created DEFAULT (SYSUTCDATETIME()),
        UpdatedAt               DATETIME2(3)      NOT NULL CONSTRAINT DF_Creds_Updated DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_BrandCredentials PRIMARY KEY CLUSTERED (CredentialID),
        CONSTRAINT UQ_Creds_BrandConnector UNIQUE NONCLUSTERED (BrandUID, ConnectorID),
        CONSTRAINT FK_Creds_Connector FOREIGN KEY (ConnectorID) REFERENCES admin.Connectors (ConnectorID)
    );
END
GO

/* =============================================================================
   admin.Jobs — the scheduled unit. endpoint × brand × schedule.
   Phase 1: we write rows via UI but NO scheduler runs them yet.
   ============================================================================= */
IF OBJECT_ID('admin.Jobs', 'U') IS NULL
BEGIN
    CREATE TABLE admin.Jobs (
        JobID               INT IDENTITY(1,1) NOT NULL,
        JobUID              UNIQUEIDENTIFIER  NOT NULL CONSTRAINT DF_Jobs_UID DEFAULT (NEWID()),
        EndpointID          INT               NOT NULL,
        BrandUID            UNIQUEIDENTIFIER  NOT NULL,
        JobType             NVARCHAR(20)      NOT NULL CONSTRAINT DF_Jobs_Type DEFAULT ('INGEST'),
        CronExpression      NVARCHAR(50)      NULL,   -- e.g. '0 */4 * * *'
        TimezoneIANA        NVARCHAR(50)      NOT NULL CONSTRAINT DF_Jobs_TZ DEFAULT ('America/Chicago'),
        NextRunAt           DATETIME2(3)      NULL,
        LastRunAt           DATETIME2(3)      NULL,
        LastRunStatus       NVARCHAR(20)      NULL,
        IsActive            BIT               NOT NULL CONSTRAINT DF_Jobs_Active DEFAULT (1),
        Priority            INT               NOT NULL CONSTRAINT DF_Jobs_Prio DEFAULT (50),
        ConcurrencyKey      NVARCHAR(100)     NULL,   -- prevents same brand+endpoint double-run
        CreatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_Jobs_Created DEFAULT (SYSUTCDATETIME()),
        UpdatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_Jobs_Updated DEFAULT (SYSUTCDATETIME()),
        CreatedBy           INT               NULL,
        UpdatedBy           INT               NULL,
        CONSTRAINT PK_Jobs PRIMARY KEY CLUSTERED (JobID),
        CONSTRAINT UQ_Jobs_UID UNIQUE NONCLUSTERED (JobUID),
        CONSTRAINT FK_Jobs_Endpoint FOREIGN KEY (EndpointID) REFERENCES admin.Endpoints (EndpointID),
        CONSTRAINT CK_Jobs_Type CHECK (JobType IN ('INGEST','ROLLUP','MIGRATE','BACKFILL'))
    );

    CREATE INDEX IX_Jobs_NextRun ON admin.Jobs (NextRunAt, IsActive) WHERE IsActive = 1;
    CREATE INDEX IX_Jobs_BrandEndpoint ON admin.Jobs (BrandUID, EndpointID);
END
GO

/* =============================================================================
   admin.JobRuns — run history. Append-only.
   ============================================================================= */
IF OBJECT_ID('admin.JobRuns', 'U') IS NULL
BEGIN
    CREATE TABLE admin.JobRuns (
        RunID               BIGINT IDENTITY(1,1) NOT NULL,
        RunUID              UNIQUEIDENTIFIER  NOT NULL CONSTRAINT DF_Runs_UID DEFAULT (NEWID()),
        JobID               INT               NOT NULL,
        StartedAt           DATETIME2(3)      NOT NULL,
        EndedAt             DATETIME2(3)      NULL,
        DurationMs          AS DATEDIFF_BIG(MILLISECOND, StartedAt, EndedAt) PERSISTED,
        Status              NVARCHAR(20)      NOT NULL,
        RowsIngested        INT               NULL,
        BytesProcessed      BIGINT            NULL,
        WorkerHost          NVARCHAR(100)     NULL,
        WorkerType          NVARCHAR(30)      NULL,   -- 'SSIS','NODE','DURABLE_FN','MANUAL'
        ErrorMessage        NVARCHAR(MAX)     NULL,
        ErrorFingerprint    NVARCHAR(64)      NULL,   -- SHA of normalized error for grouping
        TriggeredBy         NVARCHAR(30)      NOT NULL,
        CONSTRAINT PK_JobRuns PRIMARY KEY CLUSTERED (RunID),
        CONSTRAINT UQ_JobRuns_UID UNIQUE NONCLUSTERED (RunUID),
        CONSTRAINT FK_JobRuns_Job FOREIGN KEY (JobID) REFERENCES admin.Jobs (JobID),
        CONSTRAINT CK_JobRuns_Status CHECK (Status IN ('RUNNING','SUCCESS','FAILED','PARTIAL','CANCELED')),
        CONSTRAINT CK_JobRuns_Trigger CHECK (TriggeredBy IN ('SCHEDULE','MANUAL','RETRY','WIZARD_TEST','WEBHOOK'))
    );

    CREATE INDEX IX_JobRuns_JobStart ON admin.JobRuns (JobID, StartedAt DESC);
    CREATE INDEX IX_JobRuns_StatusStart ON admin.JobRuns (Status, StartedAt DESC);
END
GO

/* =============================================================================
   admin.ReportRequests — async operation state (SP-API reports, Shopify bulk ops, etc.)
   ============================================================================= */
IF OBJECT_ID('admin.ReportRequests', 'U') IS NULL
BEGIN
    CREATE TABLE admin.ReportRequests (
        RequestID                BIGINT IDENTITY(1,1) NOT NULL,
        RunID                    BIGINT            NOT NULL,
        ExternalReportID         NVARCHAR(200)     NOT NULL,   -- Amazon reportId or Shopify bulkOperation.id
        Status                   NVARCHAR(30)      NOT NULL,
        ReportDocumentID         NVARCHAR(200)     NULL,
        DocumentURL              NVARCHAR(1000)    NULL,       -- short-lived presigned URL
        DocumentURLFetchedAt     DATETIME2(3)      NULL,
        DownloadedAt             DATETIME2(3)      NULL,
        ParsedAt                 DATETIME2(3)      NULL,
        RowsIngested             INT               NULL,
        PollCount                INT               NOT NULL CONSTRAINT DF_ReqPoll DEFAULT (0),
        LastPolledAt             DATETIME2(3)      NULL,
        ErrorMessage             NVARCHAR(MAX)     NULL,
        CONSTRAINT PK_ReportRequests PRIMARY KEY CLUSTERED (RequestID),
        CONSTRAINT FK_Req_Run FOREIGN KEY (RunID) REFERENCES admin.JobRuns (RunID)
    );

    CREATE INDEX IX_ReportRequests_External ON admin.ReportRequests (ExternalReportID);
    CREATE INDEX IX_ReportRequests_Status ON admin.ReportRequests (Status, LastPolledAt);
END
GO

/* =============================================================================
   admin.RateLimitState — per-brand × connector throttle state.
   ============================================================================= */
IF OBJECT_ID('admin.RateLimitState', 'U') IS NULL
BEGIN
    CREATE TABLE admin.RateLimitState (
        StateID             INT IDENTITY(1,1) NOT NULL,
        BrandUID            UNIQUEIDENTIFIER  NOT NULL,
        ConnectorID         INT               NOT NULL,
        BucketName          NVARCHAR(100)     NOT NULL,   -- 'rest','graphql','reports.createReport',etc.
        TokensRemaining     DECIMAL(10,4)     NOT NULL,
        TokensPerSecond     DECIMAL(10,4)     NOT NULL,
        BurstCapacity       DECIMAL(10,4)     NOT NULL,
        LastRefillAt        DATETIME2(3)      NOT NULL,
        UpdatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_RLS_Updated DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_RateLimitState PRIMARY KEY CLUSTERED (StateID),
        CONSTRAINT UQ_RLS_Bucket UNIQUE NONCLUSTERED (BrandUID, ConnectorID, BucketName),
        CONSTRAINT FK_RLS_Connector FOREIGN KEY (ConnectorID) REFERENCES admin.Connectors (ConnectorID)
    );
END
GO

/* =============================================================================
   admin.RollupDefs — cross-client fan-in aggregations (per-client DB → admin analytics).
   Deferred to Phase 6 but reserved now.
   ============================================================================= */
IF OBJECT_ID('admin.RollupDefs', 'U') IS NULL
BEGIN
    CREATE TABLE admin.RollupDefs (
        RollupID            INT IDENTITY(1,1) NOT NULL,
        Name                NVARCHAR(100)     NOT NULL,
        SourceQuery         NVARCHAR(MAX)     NOT NULL,   -- SQL to run against each brand DB
        TargetSchema        NVARCHAR(50)      NOT NULL CONSTRAINT DF_Rollup_TSchema DEFAULT ('analytics'),
        TargetTable         NVARCHAR(128)     NOT NULL,
        WriteMode           NVARCHAR(20)      NOT NULL,   -- 'APPEND','REPLACE','MERGE'
        CronExpression      NVARCHAR(50)      NOT NULL,
        IsActive            BIT               NOT NULL CONSTRAINT DF_Rollup_Active DEFAULT (1),
        CreatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_Rollup_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_RollupDefs PRIMARY KEY CLUSTERED (RollupID),
        CONSTRAINT UQ_Rollup_Name UNIQUE NONCLUSTERED (Name),
        CONSTRAINT CK_Rollup_Mode CHECK (WriteMode IN ('APPEND','REPLACE','MERGE'))
    );
END
GO

/* =============================================================================
   admin.MigrationBatches — one-time moves from existing staging DBs to per-brand DBs.
   Deferred to Phase 6 but reserved now.
   ============================================================================= */
IF OBJECT_ID('admin.MigrationBatches', 'U') IS NULL
BEGIN
    CREATE TABLE admin.MigrationBatches (
        BatchID             INT IDENTITY(1,1) NOT NULL,
        BrandUID            UNIQUEIDENTIFIER  NOT NULL,
        SourceDB            NVARCHAR(200)     NOT NULL,   -- e.g. 'vs-ims-staging'
        TargetDB            NVARCHAR(200)     NOT NULL,
        Phase               NVARCHAR(30)      NOT NULL,   -- 'PLANNED','PROVISIONED','COPYING','VERIFIED','CUTOVER','RETIRED','FAILED'
        TablesIncluded      NVARCHAR(MAX)     NULL,       -- JSON array
        RowsCopied          BIGINT            NULL,
        StartedAt           DATETIME2(3)      NULL,
        CutoverAt           DATETIME2(3)      NULL,
        RetiredAt           DATETIME2(3)      NULL,
        Notes               NVARCHAR(MAX)     NULL,
        CONSTRAINT PK_MigrationBatches PRIMARY KEY CLUSTERED (BatchID),
        CONSTRAINT CK_Migration_Phase CHECK (Phase IN ('PLANNED','PROVISIONED','COPYING','VERIFIED','CUTOVER','RETIRED','FAILED'))
    );
END
GO

/* =============================================================================
   admin.AuditLog — track admin actions for change history.
   ============================================================================= */
IF OBJECT_ID('admin.AuditLog', 'U') IS NULL
BEGIN
    CREATE TABLE admin.AuditLog (
        AuditID             BIGINT IDENTITY(1,1) NOT NULL,
        UserID              INT               NULL,   -- NULL for system-generated events
        Action              NVARCHAR(100)     NOT NULL,   -- 'CONNECTOR_CREATE','ENDPOINT_UPDATE',etc.
        EntityType          NVARCHAR(50)      NULL,
        EntityID            NVARCHAR(100)     NULL,
        DetailsJSON         NVARCHAR(MAX)     NULL,
        IpAddress           NVARCHAR(50)      NULL,
        UserAgent           NVARCHAR(500)     NULL,
        CreatedAt           DATETIME2(3)      NOT NULL CONSTRAINT DF_Audit_Created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_AuditLog PRIMARY KEY CLUSTERED (AuditID)
    );

    CREATE INDEX IX_Audit_User ON admin.AuditLog (UserID, CreatedAt DESC);
    CREATE INDEX IX_Audit_Entity ON admin.AuditLog (EntityType, EntityID, CreatedAt DESC);
END
GO

PRINT 'admin.* schema created successfully.';
GO
