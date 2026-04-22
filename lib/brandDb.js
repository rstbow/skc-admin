/**
 * Per-brand data DB access.
 *
 * Each row in admin.Brands has an optional DataDbConnString (JSON) with the
 * connection info for that brand's data database (same pattern skc-api uses).
 *
 * Shape expected:
 *   {
 *     "server":   "vs-ims.database.windows.net",
 *     "database": "vs-ims-staging",
 *     "user":     "...",
 *     "password": "...",
 *     "options":  { "encrypt": true, "trustServerCertificate": false }
 *   }
 *
 * Connection pools are cached in-process by BrandUID.
 */
const sql = require('mssql');
const { getPool: getAdminPool } = require('../config/db');

const POOL_CACHE = new Map(); // BrandUID (lowercased) -> ConnectionPool

async function getBrandPool(brandUID) {
  const key = String(brandUID).toLowerCase();
  const cached = POOL_CACHE.get(key);
  if (cached && cached.connected) return cached;

  const adminPool = await getAdminPool();
  const r = await adminPool.request()
    .input('uid', sql.UniqueIdentifier, brandUID)
    .query('SELECT BrandName, DataDbConnString FROM admin.Brands WHERE BrandUID = @uid AND IsActive = 1');
  if (!r.recordset.length) throw new Error('Brand not found or inactive');

  const connStr = r.recordset[0].DataDbConnString;
  if (!connStr) {
    const err = new Error('Brand has no DataDbConnString configured. Add it on the Brands page.');
    err.code = 'NO_DATA_DB';
    throw err;
  }

  let cfg;
  try { cfg = JSON.parse(connStr); }
  catch (_) {
    const err = new Error('Brand DataDbConnString is not valid JSON.');
    err.code = 'BAD_DATA_DB_JSON';
    throw err;
  }

  cfg.options = cfg.options || {};
  if (cfg.options.encrypt === undefined) cfg.options.encrypt = true;
  if (cfg.options.trustServerCertificate === undefined) cfg.options.trustServerCertificate = false;
  cfg.pool = cfg.pool || {};
  cfg.pool.max = cfg.pool.max || 5;
  cfg.pool.min = cfg.pool.min || 0;
  cfg.pool.idleTimeoutMillis = cfg.pool.idleTimeoutMillis || 30000;
  cfg.requestTimeout = cfg.requestTimeout || 30000;
  cfg.connectionTimeout = cfg.connectionTimeout || 30000;

  const pool = new sql.ConnectionPool(cfg);
  await pool.connect();
  pool.on('error', (err) => console.error('[brand-db:' + key + '] pool error', err.message));
  POOL_CACHE.set(key, pool);
  return pool;
}

/**
 * Fetch COG for a set of SKUs from the brand's data DB.
 * Currently looks at tbl_PPA_IMS_SKU (the canonical SKU master in the
 * SKU Compass world). Column name guess: COG — falls back to common
 * alternatives if that column doesn't exist.
 *
 * Returns { cogBySku: {sku: number}, cogColumn: string|null, unavailableReason?: string }
 */
async function fetchCogBySku(brandUID, skus) {
  if (!skus || !skus.length) return { cogBySku: {}, cogColumn: null };
  const unique = Array.from(new Set(skus.filter(Boolean)));
  if (!unique.length) return { cogBySku: {}, cogColumn: null };

  let pool;
  try { pool = await getBrandPool(brandUID); }
  catch (e) {
    return { cogBySku: {}, cogColumn: null, unavailableReason: e.message, code: e.code };
  }

  // Discover the right COG column in tbl_PPA_IMS_SKU (without knowing the
  // exact schema, we try reasonable candidates in preference order).
  const candidateCols = ['COG', 'Cost', 'UnitCost', 'COGS', 'Item_COG', 'Unit_Cost'];
  let cogColumn = null;
  try {
    const colRes = await pool.request().query(`
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = 'tbl_PPA_IMS_SKU'
    `);
    const cols = new Set(colRes.recordset.map((r) => r.COLUMN_NAME));
    cogColumn = candidateCols.find((c) => cols.has(c)) || null;
    if (!cogColumn) {
      return {
        cogBySku: {},
        cogColumn: null,
        unavailableReason: 'No recognizable COG column on tbl_PPA_IMS_SKU (tried: ' + candidateCols.join(', ') + ').',
      };
    }
  } catch (e) {
    return {
      cogBySku: {},
      cogColumn: null,
      unavailableReason: 'Could not inspect tbl_PPA_IMS_SKU schema: ' + e.message,
    };
  }

  // Build parameterized IN clause (mssql doesn't natively accept arrays)
  const request = pool.request();
  const params = unique.map((s, i) => {
    const p = 'sku' + i;
    request.input(p, sql.NVarChar(200), s);
    return '@' + p;
  }).join(',');

  try {
    const r = await request.query(`
      SELECT SKU, [${cogColumn}] AS COG
      FROM dbo.tbl_PPA_IMS_SKU
      WHERE SKU IN (${params})
    `);
    const cogBySku = {};
    for (const row of r.recordset) {
      if (row.COG != null) cogBySku[row.SKU] = Number(row.COG);
    }
    return { cogBySku, cogColumn };
  } catch (e) {
    return {
      cogBySku: {},
      cogColumn,
      unavailableReason: 'COG query failed: ' + e.message,
    };
  }
}

module.exports = { getBrandPool, fetchCogBySku };
