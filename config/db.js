/**
 * Admin DB connection pool (skc-admin on vs-ims.database.windows.net).
 * Single long-lived pool — do not create per-request.
 */
const sql = require('mssql');

let _pool = null;

async function getPool() {
  if (_pool && _pool.connected) return _pool;

  const config = {
    server: process.env.ADMIN_DB_SERVER,
    database: process.env.ADMIN_DB_DATABASE,
    user: process.env.ADMIN_DB_USER,
    password: process.env.ADMIN_DB_PASSWORD,
    options: {
      encrypt: (process.env.ADMIN_DB_ENCRYPT ?? 'true') === 'true',
      trustServerCertificate: (process.env.ADMIN_DB_TRUST_CERT ?? 'false') === 'true',
      enableArithAbort: true,
    },
    pool: {
      max: 10,
      min: 0,
      idleTimeoutMillis: 30000,
    },
    requestTimeout: 30000,
    connectionTimeout: 30000,
  };

  _pool = new sql.ConnectionPool(config);
  await _pool.connect();
  _pool.on('error', (err) => {
    console.error('[admin-db] pool error', err);
  });
  return _pool;
}

/**
 * Staging DB pool (vs-ims-staging). Separate login from the admin pool.
 *
 * The admin pool uses `skc_admin_app` (a login that only exists in the
 * skc-admin database). vs-ims-staging uses `skc_app_user` — a different
 * login with SELECT/EXECUTE on raw.* and dbo.tbl_PPA_*. Pools can't
 * share the login across DBs.
 *
 * Env vars:
 *   STAGING_DB_SERVER   — defaults to ADMIN_DB_SERVER (same server)
 *   STAGING_DB_DATABASE — defaults to 'vs-ims-staging'
 *   STAGING_DB_USER     — REQUIRED for this pool to work (typically 'skc_app_user')
 *   STAGING_DB_PASSWORD — REQUIRED
 *
 * If the STAGING_DB_USER env var isn't set, getStagingPool throws with
 * a clear message. Set them in Azure App Service Configuration.
 */
let _stagingPool = null;

async function getStagingPool() {
  if (_stagingPool && _stagingPool.connected) return _stagingPool;

  if (!process.env.STAGING_DB_USER || !process.env.STAGING_DB_PASSWORD) {
    throw new Error(
      'Staging DB pool not configured — set STAGING_DB_USER and STAGING_DB_PASSWORD ' +
      'env vars on the App Service. Use the skc_app_user login that already has ' +
      'SELECT on raw.amz_financial_events / raw.amz_listings / dbo.tbl_PPA_L_Brand.'
    );
  }

  const config = {
    server:   process.env.STAGING_DB_SERVER   || process.env.ADMIN_DB_SERVER,
    database: process.env.STAGING_DB_DATABASE || 'vs-ims-staging',
    user:     process.env.STAGING_DB_USER,
    password: process.env.STAGING_DB_PASSWORD,
    options: {
      encrypt: (process.env.STAGING_DB_ENCRYPT ?? process.env.ADMIN_DB_ENCRYPT ?? 'true') === 'true',
      trustServerCertificate: (process.env.STAGING_DB_TRUST_CERT ?? process.env.ADMIN_DB_TRUST_CERT ?? 'false') === 'true',
      enableArithAbort: true,
    },
    pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },
    requestTimeout: 30000,
    connectionTimeout: 30000,
  };

  _stagingPool = new sql.ConnectionPool(config);
  await _stagingPool.connect();
  _stagingPool.on('error', (err) => console.error('[staging-db] pool error', err));
  return _stagingPool;
}

/**
 * AIR_Bots DB pool (vs-ims.AIR_Bots) — the multi-tenant data plane for
 * AIR Bots. Schema lives at AIR_Bots.air.* (Tenants, TenantBrands,
 * AgentRecipes, Agents, AgentRuns, AgentRunLog) per Chip's blessed
 * design. RLS is enforced via `air.fn_TenantPredicate` + Security
 * Policy on 5 tables (NOT AgentRecipes).
 *
 * Env vars (all required):
 *   AIR_BOTS_DB_SERVER   — defaults to ADMIN_DB_SERVER (same Azure SQL server)
 *   AIR_BOTS_DB_DATABASE — defaults to 'AIR_Bots'
 *   AIR_BOTS_DB_USER     — REQUIRED (the runner login Chip provisions in
 *                          his pending GRANT INSERT bundle; until that
 *                          bundle ships, can be SA / db_owner)
 *   AIR_BOTS_DB_PASSWORD — REQUIRED
 *
 * RLS interaction:
 *   - Routes/runner running as db_owner bypass RLS via the predicate's
 *     IS_ROLEMEMBER(N'db_owner') = 1 clause (added in artifact 08).
 *   - For v0.2 multi-tenant, the runner sets sp_set_session_context
 *     @key='tenant_uid' before each agent run to enforce per-tenant
 *     visibility under non-db_owner login.
 */
let _airBotsPool = null;

async function getAirBotsPool() {
  if (_airBotsPool && _airBotsPool.connected) return _airBotsPool;

  if (!process.env.AIR_BOTS_DB_USER || !process.env.AIR_BOTS_DB_PASSWORD) {
    throw new Error(
      'AIR_Bots DB pool not configured — set AIR_BOTS_DB_USER and AIR_BOTS_DB_PASSWORD ' +
      'env vars on the App Service. Use a login with db_datareader + db_datawriter ' +
      'on AIR_Bots (or db_owner for v0.1 with the RLS bypass clause active).'
    );
  }

  const config = {
    server:   process.env.AIR_BOTS_DB_SERVER   || process.env.ADMIN_DB_SERVER,
    database: process.env.AIR_BOTS_DB_DATABASE || 'AIR_Bots',
    user:     process.env.AIR_BOTS_DB_USER,
    password: process.env.AIR_BOTS_DB_PASSWORD,
    options: {
      encrypt: (process.env.AIR_BOTS_DB_ENCRYPT ?? process.env.ADMIN_DB_ENCRYPT ?? 'true') === 'true',
      trustServerCertificate: (process.env.AIR_BOTS_DB_TRUST_CERT ?? process.env.ADMIN_DB_TRUST_CERT ?? 'false') === 'true',
      enableArithAbort: true,
    },
    pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },
    requestTimeout: 30000,
    connectionTimeout: 30000,
  };

  _airBotsPool = new sql.ConnectionPool(config);
  await _airBotsPool.connect();
  _airBotsPool.on('error', (err) => console.error('[air-bots-db] pool error', err));
  return _airBotsPool;
}

/**
 * Project DB pool (vs-ims-project) — separate database on the SAME
 * server as vs-ims-staging. Used by recipes that need to read tables
 * living in vs-ims-project (e.g. tbl_PPA_SP_API_Report_Runs for the
 * adaptive-source-window recipe).
 *
 * Azure SQL Database doesn't support cross-database 4-part references
 * (Msg "Reference to database... is not supported in this version of
 * SQL Server"). So we keep a dedicated pool per database.
 *
 * Env vars (all optional — default to the STAGING_DB_* values since
 * skc_app_user typically has access to both DBs):
 *   PROJECT_DB_SERVER   — defaults to STAGING_DB_SERVER -> ADMIN_DB_SERVER
 *   PROJECT_DB_DATABASE — defaults to 'vs-ims-project'
 *   PROJECT_DB_USER     — defaults to STAGING_DB_USER
 *   PROJECT_DB_PASSWORD — defaults to STAGING_DB_PASSWORD
 *
 * If neither PROJECT_DB_USER nor STAGING_DB_USER is set, throws with a
 * clear message.
 */
let _projectPool = null;

async function getProjectPool() {
  if (_projectPool && _projectPool.connected) return _projectPool;

  const user     = process.env.PROJECT_DB_USER     || process.env.STAGING_DB_USER;
  const password = process.env.PROJECT_DB_PASSWORD || process.env.STAGING_DB_PASSWORD;
  if (!user || !password) {
    throw new Error(
      'Project DB pool not configured — set PROJECT_DB_USER + PROJECT_DB_PASSWORD ' +
      '(or fall back via STAGING_DB_USER + STAGING_DB_PASSWORD) on the App Service.'
    );
  }

  const config = {
    server:   process.env.PROJECT_DB_SERVER   || process.env.STAGING_DB_SERVER || process.env.ADMIN_DB_SERVER,
    database: process.env.PROJECT_DB_DATABASE || 'vs-ims-project',
    user,
    password,
    options: {
      encrypt: (process.env.PROJECT_DB_ENCRYPT ?? process.env.STAGING_DB_ENCRYPT ?? process.env.ADMIN_DB_ENCRYPT ?? 'true') === 'true',
      trustServerCertificate: (process.env.PROJECT_DB_TRUST_CERT ?? process.env.STAGING_DB_TRUST_CERT ?? process.env.ADMIN_DB_TRUST_CERT ?? 'false') === 'true',
      enableArithAbort: true,
    },
    pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },
    requestTimeout: 30000,
    connectionTimeout: 30000,
  };

  _projectPool = new sql.ConnectionPool(config);
  await _projectPool.connect();
  _projectPool.on('error', (err) => console.error('[project-db] pool error', err));
  return _projectPool;
}

module.exports = { sql, getPool, getStagingPool, getAirBotsPool, getProjectPool };
