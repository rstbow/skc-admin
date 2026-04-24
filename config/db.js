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

module.exports = { sql, getPool, getStagingPool };
