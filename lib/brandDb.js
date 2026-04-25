/**
 * Per-brand data DB access.
 *
 * Each row in admin.Brands has an optional DataDbConnString (JSON) with the
 * connection info for that brand's data database (same pattern skc-api uses).
 *
 * In the current shared-data-DB world, ALL brands have the same
 * DataDbConnString pointing at vs-ims-staging. Data tables there carry a
 * brand identifier column (BrandUID or Brand_ID). We auto-discover the
 * column and filter accordingly — so every query stays brand-scoped even
 * though the physical database is shared.
 *
 * Connection pools are cached in-process by (BrandUID, serverKey).
 */
const sql = require('mssql');
const { getPool: getAdminPool } = require('../config/db');

const POOL_CACHE = new Map();         // BrandUID lowercased -> ConnectionPool
const BRAND_ID_CACHE = new Map();     // BrandUID lowercased -> int

async function getBrandPool(brandUID) {
  const key = String(brandUID).toLowerCase();
  const cached = POOL_CACHE.get(key);
  if (cached && cached.connected) return cached;

  const adminPool = await getAdminPool();
  const r = await adminPool.request()
    .input('uid', sql.UniqueIdentifier, brandUID)
    .query('SELECT BrandName, DataDbConnString, BrandID FROM admin.Brands WHERE BrandUID = @uid AND IsActive = 1');
  if (!r.recordset.length) throw new Error('Brand not found or inactive');

  if (r.recordset[0].BrandID != null) {
    BRAND_ID_CACHE.set(key, Number(r.recordset[0].BrandID));
  }

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
  // 30s default was too tight for late-stage MERGE batches in big backfills
  // (ZenToes hit 30s on a 5-day chunk after 36 minutes of work). Bump to 2
  // min — well under the scheduler's 1h concurrency lock, but generous
  // enough that lock-contended or large-input MERGE calls finish.
  cfg.requestTimeout = cfg.requestTimeout || 120000;
  cfg.connectionTimeout = cfg.connectionTimeout || 30000;

  const pool = new sql.ConnectionPool(cfg);
  await pool.connect();
  pool.on('error', (err) => console.error('[brand-db:' + key + '] pool error', err.message));
  POOL_CACHE.set(key, pool);
  return pool;
}

/**
 * Look up the integer BrandID for a brand, cached after the first call.
 */
async function getBrandIntID(brandUID) {
  const key = String(brandUID).toLowerCase();
  if (BRAND_ID_CACHE.has(key)) return BRAND_ID_CACHE.get(key);

  const adminPool = await getAdminPool();
  const r = await adminPool.request()
    .input('uid', sql.UniqueIdentifier, brandUID)
    .query('SELECT BrandID FROM admin.Brands WHERE BrandUID = @uid AND IsActive = 1');
  const id = r.recordset[0]?.BrandID;
  if (id != null) {
    BRAND_ID_CACHE.set(key, Number(id));
    return Number(id);
  }
  return null;
}

/**
 * Discover the brand-identifying column on a data-DB table.
 * Returns { column, kind: 'uuid' | 'int' } or null if none found.
 *
 * Preference order: Brand_ID-type FIRST (integer/decimal) because in the
 * SKU Compass schema Brand_UID is sparsely populated on tbl_PPA_IMS_SKU
 * while Brand_ID is populated on every row. UUID column is a fallback.
 */
async function discoverBrandColumn(pool, tableName) {
  const r = await pool.request()
    .input('t', sql.NVarChar(128), tableName)
    .query(`
      SELECT COLUMN_NAME, DATA_TYPE
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = @t
        AND UPPER(COLUMN_NAME) IN (
          'BRANDUID','BRAND_UID',
          'BRANDID','BRAND_ID'
        )
    `);
  if (!r.recordset.length) return null;

  // Prefer integer/decimal Brand_ID (populated consistently in the shared
  // data DB). Fall back to uuid Brand_UID (sometimes NULL on legacy rows).
  const numericTypes = ['int','bigint','smallint','tinyint','decimal','numeric'];
  const intCol = r.recordset.find((x) =>
    numericTypes.includes(x.DATA_TYPE.toLowerCase())
    && /^BRAND[_]?ID$/i.test(x.COLUMN_NAME)
  );
  if (intCol) return { column: intCol.COLUMN_NAME, kind: 'int' };

  const uuidCol = r.recordset.find((x) =>
    x.DATA_TYPE.toLowerCase() === 'uniqueidentifier'
    && /^BRAND[_]?UID$/i.test(x.COLUMN_NAME)
  );
  if (uuidCol) return { column: uuidCol.COLUMN_NAME, kind: 'uuid' };

  return null;
}

/**
 * Build a parameterized WHERE clause fragment that scopes a query to a brand.
 * Returns { fragment, bind } — bind is a function that adds the right
 * parameter to a mssql Request.
 */
async function buildBrandScope(pool, brandUID, tableName) {
  const col = await discoverBrandColumn(pool, tableName);
  if (!col) {
    // No brand column detected — can't safely filter. Let caller decide.
    return { fragment: null, bind: null, reason: 'no-brand-column-on-' + tableName };
  }
  if (col.kind === 'uuid') {
    return {
      fragment: `[${col.column}] = @brand_scope_uid`,
      bind: (request) => request.input('brand_scope_uid', sql.UniqueIdentifier, brandUID),
      column: col.column,
      kind: col.kind,
    };
  }
  // int
  const intID = await getBrandIntID(brandUID);
  if (intID == null) {
    return {
      fragment: null,
      bind: null,
      reason: 'brand-id-int-not-set-on-admin.Brands',
      column: col.column,
      kind: col.kind,
    };
  }
  return {
    fragment: `[${col.column}] = @brand_scope_int`,
    bind: (request) => request.input('brand_scope_int', sql.Int, intID),
    column: col.column,
    kind: col.kind,
  };
}

/* =========================================================================
   COG queries (brand-scoped)
   ========================================================================= */

/**
 * Fetch COG for a set of SKUs from the brand's data DB.
 * Returns { cogBySku, cogColumn, brandScopeColumn?, unavailableReason? }.
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

  const candidateCols = ['COG', 'Cost', 'UnitCost', 'COGS', 'Item_COG', 'Unit_Cost'];
  let cogColumn = null;
  let availableCols = new Set();
  try {
    const colRes = await pool.request().query(`
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = 'tbl_PPA_IMS_SKU'
    `);
    availableCols = new Set(colRes.recordset.map((r) => r.COLUMN_NAME));
    cogColumn = candidateCols.find((c) => availableCols.has(c)) || null;
    if (!cogColumn) {
      return {
        cogBySku: {}, cogColumn: null,
        unavailableReason: 'No recognizable COG column on tbl_PPA_IMS_SKU (tried: ' + candidateCols.join(', ') + ').',
      };
    }
  } catch (e) {
    return { cogBySku: {}, cogColumn: null, unavailableReason: 'Schema inspect failed: ' + e.message };
  }

  const scope = await buildBrandScope(pool, brandUID, 'tbl_PPA_IMS_SKU');
  if (!scope.fragment) {
    return {
      cogBySku: {}, cogColumn,
      unavailableReason: 'Cannot scope to brand — ' + (scope.reason || 'no brand column')
        + (scope.reason === 'brand-id-int-not-set-on-admin.Brands'
           ? '. Set admin.Brands.BrandID for this brand.'
           : ''),
    };
  }

  const request = pool.request();
  scope.bind(request);
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
        AND ${scope.fragment}
    `);
    const cogBySku = {};
    for (const row of r.recordset) {
      if (row.COG != null) cogBySku[row.SKU] = Number(row.COG);
    }
    return { cogBySku, cogColumn, brandScopeColumn: scope.column };
  } catch (e) {
    return { cogBySku: {}, cogColumn, unavailableReason: 'COG query failed: ' + e.message };
  }
}

/**
 * Fetch ALL COG rows for a brand (no SKU filter). Used by the COG browser UI.
 * Brand-scoped via the auto-discovered Brand column.
 */
async function fetchAllCog(brandUID, { limit = 5000, search = null } = {}) {
  let pool;
  try { pool = await getBrandPool(brandUID); }
  catch (e) {
    return { rows: [], cogColumn: null, total: 0, unavailableReason: e.message, code: e.code };
  }

  const candidateCols = ['COG', 'Cost', 'UnitCost', 'COGS', 'Item_COG', 'Unit_Cost'];
  let cogColumn = null;
  let availableCols = new Set();
  try {
    const colRes = await pool.request().query(`
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = 'tbl_PPA_IMS_SKU'
    `);
    availableCols = new Set(colRes.recordset.map((r) => r.COLUMN_NAME));
    cogColumn = candidateCols.find((c) => availableCols.has(c)) || null;
    if (!cogColumn) {
      return { rows: [], cogColumn: null, total: 0,
               unavailableReason: 'No recognizable COG column on tbl_PPA_IMS_SKU.' };
    }
  } catch (e) {
    return { rows: [], cogColumn: null, total: 0, unavailableReason: 'Schema inspect failed: ' + e.message };
  }

  const scope = await buildBrandScope(pool, brandUID, 'tbl_PPA_IMS_SKU');
  if (!scope.fragment) {
    return {
      rows: [], cogColumn, total: 0,
      unavailableReason: 'Cannot scope to brand — ' + (scope.reason || 'no brand column')
        + (scope.reason === 'brand-id-int-not-set-on-admin.Brands'
           ? '. Set admin.Brands.BrandID for this brand (see 015 migration output for SQL).'
           : ''),
    };
  }

  const ctxCandidates = ['ASIN', 'ProductName', 'Product_Name', 'Title', 'Brand', 'Supplier',
                         'LeadTime', 'Lead_Time', 'SafetyStock', 'Safety_Stock'];
  const ctxCols = ctxCandidates.filter((c) => availableCols.has(c));

  const selectCols = ['SKU', `[${cogColumn}] AS COG`].concat(ctxCols.map((c) => `[${c}] AS ${c}`));
  const request = pool.request();
  scope.bind(request);

  let where = 'WHERE SKU IS NOT NULL AND ' + scope.fragment;
  if (search) {
    request.input('search', sql.NVarChar(200), '%' + search + '%');
    where += ' AND (SKU LIKE @search';
    if (ctxCols.includes('ASIN')) where += ' OR ASIN LIKE @search';
    if (ctxCols.includes('ProductName')) where += ' OR ProductName LIKE @search';
    if (ctxCols.includes('Product_Name')) where += ' OR Product_Name LIKE @search';
    if (ctxCols.includes('Title')) where += ' OR Title LIKE @search';
    where += ')';
  }
  request.input('lim', sql.Int, Math.max(1, Math.min(limit, 20000)));

  try {
    const r = await request.query(`
      SELECT TOP (@lim) ${selectCols.join(', ')}
      FROM dbo.tbl_PPA_IMS_SKU
      ${where}
      ORDER BY SKU
    `);

    // Count only this brand's rows
    const countReq = pool.request();
    scope.bind(countReq);
    const countRes = await countReq.query(`
      SELECT COUNT(*) AS cnt
      FROM dbo.tbl_PPA_IMS_SKU
      WHERE SKU IS NOT NULL AND ${scope.fragment}
    `);
    const total = countRes.recordset[0]?.cnt ?? r.recordset.length;

    return {
      rows: r.recordset.map((row) => ({
        sku: row.SKU,
        cog: row.COG == null ? null : Number(row.COG),
        asin: row.ASIN || null,
        productName: row.ProductName || row.Product_Name || row.Title || null,
        brand: row.Brand || null,
        supplier: row.Supplier || null,
        leadTime: row.LeadTime || row.Lead_Time || null,
        safetyStock: row.SafetyStock || row.Safety_Stock || null,
      })),
      cogColumn,
      brandScopeColumn: scope.column,
      total,
      ctxCols,
    };
  } catch (e) {
    return { rows: [], cogColumn, total: 0, unavailableReason: 'COG query failed: ' + e.message };
  }
}

module.exports = { getBrandPool, getBrandIntID, fetchCogBySku, fetchAllCog };
