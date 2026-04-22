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

module.exports = { sql, getPool };
