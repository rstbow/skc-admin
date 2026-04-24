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
 * Staging DB pool (vs-ims-staging). Same login as admin pool, different DB.
 * Used for features that need to read legacy PPA tables (e.g. brand import,
 * SKU source-of-truth lookups) without going through brandDb's per-brand
 * connection string.
 *
 * Same pattern: one long-lived pool, lazy-initialized.
 */
let _stagingPool = null;

async function getStagingPool() {
  if (_stagingPool && _stagingPool.connected) return _stagingPool;

  const config = {
    server: process.env.ADMIN_DB_SERVER,
    database: process.env.STAGING_DB_DATABASE || 'vs-ims-staging',
    user: process.env.ADMIN_DB_USER,
    password: process.env.ADMIN_DB_PASSWORD,
    options: {
      encrypt: (process.env.ADMIN_DB_ENCRYPT ?? 'true') === 'true',
      trustServerCertificate: (process.env.ADMIN_DB_TRUST_CERT ?? 'false') === 'true',
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
