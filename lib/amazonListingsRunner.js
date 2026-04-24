/**
 * Listing Ledger runner.
 *
 * Daily pull of Amazon listing data per brand, with field-level delta
 * detection. Every changed title / price / quantity / status / image /
 * description creates a row in raw.amz_listing_changes so users can
 * correlate edits to sales outcomes and (eventually) roll them back.
 *
 * Pipeline:
 *   1. Submit GET_MERCHANT_LISTINGS_ALL_DATA report to SP-API
 *   2. Poll until DONE (typically 5-15 minutes)
 *   3. Download + parse the tab-separated report body
 *   4. Normalize rows into our schema
 *   5. Load current state from raw.amz_listings for this brand
 *   6. Diff: for each SKU compute hash; if changed, emit field-level
 *      delta rows and upsert the new snapshot
 *   7. MERGE upserts via raw.usp_merge_amz_listings (hash-gated)
 *   8. Bulk-INSERT deltas via raw.usp_append_amz_listing_changes
 *
 * Tracked fields (present in the report):
 *   Title, Description, Price, Quantity, Status, FulfillmentChannel,
 *   MainImageURL, ASIN, ProductIDType, Condition, OpenDate
 *
 * Not in this report (would require per-SKU listings-items API later):
 *   Bullet points, full HTML description, multiple images,
 *   search terms, enhanced brand content.
 *
 * Runner contract (same shape as other runners):
 *   runAmazonListings({ credentialID, triggeredBy, userID, params })
 *     → { runID, ok, eventsProcessed, rowsInserted, rowsUpdated,
 *         changesDetected, addedListings, removedListings, reportId, ... }
 */
const crypto = require('crypto');
const { sql, getPool } = require('../config/db');
const { getBrandPool } = require('./brandDb');
const { runReport, parseTsv } = require('./spApiReports');

const BATCH_SIZE = 500;
const REPORT_TYPE = 'GET_MERCHANT_LISTINGS_ALL_DATA';

/* =========================================================================
   Public entry
   ========================================================================= */
async function runAmazonListings({
  credentialID,
  triggeredBy = 'MANUAL',
  userID = null,
  params = {},
} = {}) {
  if (!credentialID) throw new Error('credentialID is required');

  const ctx = await loadCredentialContext(credentialID);
  if (!ctx) throw new Error('Credential not found');
  if (ctx.connector.Name !== 'AMAZON_SP_API') {
    throw new Error('Listings runner only supports AMAZON_SP_API credentials');
  }

  const endpointID = await resolveEndpointID('AMZ_LISTINGS_READ');
  const jobID = await ensureJob(endpointID, ctx.brand.BrandUID);
  const runID = await startJobRun(jobID, triggeredBy);

  try {
    const brandPool = await getBrandPool(ctx.brand.BrandUID);
    const marketplaceId = params.marketplaceId || 'ATVPDKIKX0DER';
    const startedAt = Date.now();

    /* 1. Submit + poll + download the listings report from SP-API. */
    console.log('[runner/listings] submitting', REPORT_TYPE, 'for brand',
      ctx.brand.BrandName, '/', marketplaceId);
    const report = await runReport(ctx, {
      reportType: REPORT_TYPE,
      marketplaceIds: [marketplaceId],
      onProgress: ({ status, attempts, elapsedMs }) => {
        console.log('[runner/listings] report poll', {
          status, attempts, elapsedS: Math.round(elapsedMs / 1000),
        });
      },
    });

    /* 2. Parse the tab-separated report body. */
    const raw = parseTsv(report.text);
    console.log('[runner/listings] report parsed rows:', raw.length,
      ' (reportId=' + report.reportId + ')',
      ' elapsedS=' + Math.round((Date.now() - startedAt) / 1000));

    if (!raw.length) {
      // No listings for this seller + marketplace. Not an error — just
      // nothing to diff. Could mean a new seller account with no live SKUs.
      await completeJobRun(runID, {
        status: 'SUCCESS',
        rowsIngested: 0,
        errorMessage: null,
        workerType: 'NODE',
      });
      await bumpLastAuthed(credentialID);
      return {
        runID, ok: true, brand: ctx.brand, source: 'SP_API_REPORT',
        marketplaceId, reportId: report.reportId,
        eventsProcessed: 0, rowsInserted: 0, rowsUpdated: 0, rowsUnchanged: 0,
        changesDetected: 0, addedListings: 0, removedListings: 0,
      };
    }

    const fresh = raw.map((r) => normalizeReportRow(r, marketplaceId));

    /* 2. Pull our current known-state for this brand + marketplace. */
    const current = await readCurrentListings(brandPool, ctx.brand.BrandUID, marketplaceId);
    console.log('[runner/listings] current raw.amz_listings rows:', current.size);

    /* 3. Diff in memory. */
    const { upserts, changes, stats } = diffListings(current, fresh);
    console.log('[runner/listings] diff result', stats);

    /* 4. MERGE upserts (only rows whose hash changed — no wasted writes). */
    let mergeResult = { Inserted: 0, Updated: 0, Unchanged: 0, Total: 0 };
    for (let i = 0; i < upserts.length; i += BATCH_SIZE) {
      const slice = upserts.slice(i, i + BATCH_SIZE);
      const r = await mergeListings(brandPool, ctx.brand.BrandUID, runID, slice);
      mergeResult.Inserted += r.Inserted;
      mergeResult.Updated  += r.Updated;
      mergeResult.Unchanged += r.Unchanged;
      mergeResult.Total    += r.Total;
    }

    /* 5. Append detected field-level changes. */
    let changesInserted = 0;
    for (let i = 0; i < changes.length; i += BATCH_SIZE) {
      const slice = changes.slice(i, i + BATCH_SIZE);
      const r = await appendChanges(brandPool, ctx.brand.BrandUID, runID, slice);
      changesInserted += r.Inserted;
    }

    await completeJobRun(runID, {
      status: 'SUCCESS',
      rowsIngested: fresh.length,
      errorMessage: null,
      workerType: 'NODE',
    });
    await bumpLastAuthed(credentialID);

    return {
      runID,
      ok: true,
      brand: ctx.brand,
      source: 'SP_API_REPORT',
      reportType: REPORT_TYPE,
      reportId: report.reportId,
      marketplaceId,
      eventsProcessed: fresh.length,
      rowsInserted: mergeResult.Inserted,
      rowsUpdated:  mergeResult.Updated,
      rowsUnchanged: mergeResult.Unchanged,
      changesDetected: stats.changesDetected,
      addedListings: stats.added,
      removedListings: stats.removed,
    };
  } catch (e) {
    console.error('[runner/amz-listings]', e);
    await completeJobRun(runID, {
      status: 'FAILED',
      rowsIngested: null,
      errorMessage: (e.message || 'Unknown error').slice(0, 4000),
      workerType: 'NODE',
    });
    throw e;
  }
}

/* =========================================================================
   Report row normalizer — maps GET_MERCHANT_LISTINGS_ALL_DATA TSV columns
   to our canonical listing shape. Amazon's report uses kebab-case header
   names; some are sparse depending on seller/marketplace config.
   ========================================================================= */
function normalizeReportRow(r, marketplaceId) {
  const imageUrl = r['image-url'] || null;
  const imagesArr = imageUrl ? [imageUrl] : [];
  const priceStr = r['price'];
  const price = priceStr && priceStr.length ? Number(priceStr) : null;
  const qtyStr = r['quantity'];
  const qty = qtyStr && qtyStr.length ? parseInt(qtyStr, 10) : null;

  return {
    SKU:           r['seller-sku'] || null,
    MarketplaceID: marketplaceId,
    ASIN:          r['asin1'] || null,
    ProductType:   null,                                  // not in this report
    Title:         r['item-name'] || null,
    Brand:         null,                                  // not in this report
    Description:   r['item-description'] || null,
    Bullet1: null, Bullet2: null, Bullet3: null, Bullet4: null, Bullet5: null,
    SearchTerms:   null,
    Category:      null,
    BrowseNodeID:  null,
    ImagesJSON:    JSON.stringify(imagesArr),
    Price:         Number.isFinite(price) ? price : null,
    Currency:      null,                                  // report doesn't label currency directly
    Quantity:      Number.isFinite(qty) ? qty : null,
    Condition:     r['item-condition'] || null,
    Status:        r['status'] || null,
    IssueCount:    null,
    _RawPayload:   JSON.stringify(r),                     // keep original row for debug
  };
}

async function readCurrentListings(brandPool, brandUID, marketplaceId) {
  const r = await brandPool.request()
    .input('uid', sql.UniqueIdentifier, brandUID)
    .input('mp',  sql.NVarChar(20), marketplaceId)
    .query(`
      SELECT SKU, MarketplaceID, ASIN, ProductType, Title, Brand, Description,
             Bullet1, Bullet2, Bullet3, Bullet4, Bullet5,
             SearchTerms, Category, BrowseNodeID, ImagesJSON,
             Price, Currency, Quantity, Condition, Status, IssueCount,
             _SourceRowHash
      FROM raw.amz_listings
      WHERE _BrandUID = @uid AND MarketplaceID = @mp;
    `);
  const map = new Map();
  for (const row of r.recordset) {
    map.set(row.SKU + '|' + row.MarketplaceID, row);
  }
  return map;
}

/* =========================================================================
   Diff engine — the core of the Ledger
   ========================================================================= */

// Which fields carry a delta worth tracking. Paths are serialized so the
// ledger uses a stable schema for FieldPath.
const TRACKED_FIELDS = [
  { key: 'Title',        path: 'summary.itemName' },
  { key: 'Brand',        path: 'summary.brandName' },
  { key: 'Description',  path: 'description' },
  { key: 'Bullet1',      path: 'bullet_point[0]' },
  { key: 'Bullet2',      path: 'bullet_point[1]' },
  { key: 'Bullet3',      path: 'bullet_point[2]' },
  { key: 'Bullet4',      path: 'bullet_point[3]' },
  { key: 'Bullet5',      path: 'bullet_point[4]' },
  { key: 'SearchTerms',  path: 'search_terms' },
  { key: 'Price',        path: 'offer.price' },
  { key: 'Currency',     path: 'offer.currency' },
  { key: 'ImagesJSON',   path: 'images' },
  { key: 'Category',     path: 'summary.browseClassification' },
  { key: 'BrowseNodeID', path: 'summary.browseNodeID' },
  { key: 'ASIN',         path: 'summary.asin' },
  { key: 'ProductType',  path: 'productType' },
];

function computeRowHash(row) {
  const canonical = TRACKED_FIELDS.map((f) => f.key + '=' + (row[f.key] ?? '')).join('\n');
  return crypto.createHash('sha256').update(canonical).digest();
}

function normalizeForCompare(v) {
  if (v == null) return '';
  return String(v);
}

function diffListings(currentMap, freshRows) {
  const upserts = [];
  const changes = [];
  const stats   = { added: 0, removed: 0, changed: 0, unchanged: 0, changesDetected: 0 };
  const seenKeys = new Set();

  for (const fresh of freshRows) {
    const key = fresh.SKU + '|' + fresh.MarketplaceID;
    seenKeys.add(key);

    const hash = computeRowHash(fresh);
    fresh._SourceRowHashHex = hash.toString('hex');

    const cur = currentMap.get(key);

    if (!cur) {
      // NEW listing
      stats.added++;
      upserts.push(fresh);
      changes.push({
        SKU: fresh.SKU, ASIN: fresh.ASIN, MarketplaceID: fresh.MarketplaceID,
        ChangeSource: 'SKC_ADMIN_SCHEDULER',
        ChangeType:   'LISTING_ADDED',
        FieldPath:    null,
        BeforeValue:  null,
        AfterValue:   fresh.Title || fresh.SKU,
        Status:       'DETECTED',
      });
      stats.changesDetected++;
      continue;
    }

    const curHash = cur._SourceRowHash ? Buffer.from(cur._SourceRowHash).toString('hex') : null;
    if (curHash === fresh._SourceRowHashHex) {
      stats.unchanged++;
      continue;
    }

    // Something changed — upsert + emit field-level change rows
    stats.changed++;
    upserts.push(fresh);

    for (const f of TRACKED_FIELDS) {
      const before = normalizeForCompare(cur[f.key]);
      const after  = normalizeForCompare(fresh[f.key]);
      if (before === after) continue;
      changes.push({
        SKU: fresh.SKU, ASIN: fresh.ASIN, MarketplaceID: fresh.MarketplaceID,
        ChangeSource: 'SKC_ADMIN_SCHEDULER',
        ChangeType:   fieldChangeType(f.key),
        FieldPath:    f.path,
        BeforeValue:  before || null,
        AfterValue:   after  || null,
        Status:       'DETECTED',
      });
      stats.changesDetected++;
    }
  }

  // Detect REMOVED listings (were in our store, missing from today's snapshot)
  for (const [key, cur] of currentMap) {
    if (seenKeys.has(key)) continue;
    stats.removed++;
    changes.push({
      SKU: cur.SKU, ASIN: cur.ASIN, MarketplaceID: cur.MarketplaceID,
      ChangeSource: 'SKC_ADMIN_SCHEDULER',
      ChangeType:   'LISTING_REMOVED',
      FieldPath:    null,
      BeforeValue:  cur.Title || cur.SKU,
      AfterValue:   null,
      Status:       'DETECTED',
    });
    stats.changesDetected++;
  }

  return { upserts, changes, stats };
}

function fieldChangeType(fieldKey) {
  // Map internal column names to stable change-type enum values consumed
  // by the UI / runbooks. Keep UPPER_SNAKE_CASE for SQL-friendliness.
  const map = {
    Title:        'TITLE_CHANGED',
    Brand:        'BRAND_CHANGED',
    Description:  'DESCRIPTION_CHANGED',
    Bullet1:      'BULLET_CHANGED',
    Bullet2:      'BULLET_CHANGED',
    Bullet3:      'BULLET_CHANGED',
    Bullet4:      'BULLET_CHANGED',
    Bullet5:      'BULLET_CHANGED',
    SearchTerms:  'SEARCH_TERMS_CHANGED',
    Price:        'PRICE_CHANGED',
    Currency:     'CURRENCY_CHANGED',
    ImagesJSON:   'IMAGES_CHANGED',
    Category:     'CATEGORY_CHANGED',
    BrowseNodeID: 'BROWSE_NODE_CHANGED',
    ASIN:         'ASIN_CHANGED',
    ProductType:  'PRODUCT_TYPE_CHANGED',
  };
  return map[fieldKey] || 'FIELD_CHANGED';
}

/* =========================================================================
   Proc callers — talk to the 024 procs
   ========================================================================= */
async function mergeListings(brandPool, brandUID, runID, rows) {
  if (!rows.length) return { Inserted: 0, Updated: 0, Unchanged: 0, Total: 0 };

  const rowsJson = JSON.stringify(rows.map((r) => ({
    SKU: r.SKU, MarketplaceID: r.MarketplaceID, ASIN: r.ASIN,
    ProductType: r.ProductType, Title: r.Title, Brand: r.Brand,
    Description: r.Description,
    Bullet1: r.Bullet1, Bullet2: r.Bullet2, Bullet3: r.Bullet3,
    Bullet4: r.Bullet4, Bullet5: r.Bullet5,
    SearchTerms: r.SearchTerms, Category: r.Category, BrowseNodeID: r.BrowseNodeID,
    ImagesJSON: r.ImagesJSON,
    Price: r.Price != null ? String(r.Price) : null,
    Currency: r.Currency, Quantity: r.Quantity,
    Condition: r.Condition, Status: r.Status, IssueCount: r.IssueCount,
    _RawPayload: r._RawPayload,
    _SourceRowHashHex: r._SourceRowHashHex,
  })));

  const result = await brandPool.request()
    .input('BrandUID',    sql.UniqueIdentifier, brandUID)
    .input('SourceRunID', sql.BigInt, runID)
    .input('RowsJson',    sql.NVarChar(sql.MAX), rowsJson)
    .execute('raw.usp_merge_amz_listings');

  return result.recordset[0] || { Inserted: 0, Updated: 0, Unchanged: 0, Total: rows.length };
}

async function appendChanges(brandPool, brandUID, runID, rows) {
  if (!rows.length) return { Inserted: 0 };
  const rowsJson = JSON.stringify(rows);
  const result = await brandPool.request()
    .input('BrandUID',    sql.UniqueIdentifier, brandUID)
    .input('SourceRunID', sql.BigInt, runID)
    .input('RowsJson',    sql.NVarChar(sql.MAX), rowsJson)
    .execute('raw.usp_append_amz_listing_changes');
  return result.recordset[0] || { Inserted: 0 };
}

/* =========================================================================
   JobRun bookkeeping — duplicates minimal bits of the financial-events
   runner so the listings runner doesn't depend on it. If we grow more
   runners these helpers belong in lib/jobRunHelpers.js.
   ========================================================================= */
async function loadCredentialContext(credentialID) {
  const pool = await getPool();
  const r = await pool.request()
    .input('id', sql.Int, credentialID)
    .query(`
      -- Full SP-API auth context — we mint an LWA access token per
      -- report submission via amazonApi.getAccessToken.
      SELECT bc.CredentialID, bc.BrandUID, bc.ConnectorID,
             bc.Region, bc.MarketplaceIDs,
             bc.RefreshToken_Enc,
             c.Name AS ConnectorName, c.DisplayName AS ConnectorDisplay,
             c.AuthType, c.BaseURL, c.AppClientID, c.AppClientSecret_Enc, c.ApiVersion,
             b.BrandName, b.DataDbConnString
      FROM admin.BrandCredentials bc
      JOIN admin.Connectors c ON c.ConnectorID = bc.ConnectorID
      JOIN admin.Brands     b ON b.BrandUID    = bc.BrandUID
      WHERE bc.CredentialID = @id;
    `);
  if (!r.recordset.length) return null;
  const row = r.recordset[0];
  return {
    credentialID: row.CredentialID,
    connector: {
      ConnectorID: row.ConnectorID, Name: row.ConnectorName, DisplayName: row.ConnectorDisplay,
      AuthType: row.AuthType, BaseURL: row.BaseURL,
      AppClientID: row.AppClientID,
      AppClientSecret_Enc: row.AppClientSecret_Enc,
      ApiVersion: row.ApiVersion,
    },
    cred: {
      Region: row.Region,
      MarketplaceIDs: row.MarketplaceIDs,
      RefreshToken_Enc: row.RefreshToken_Enc,
    },
    brand: { BrandUID: row.BrandUID, BrandName: row.BrandName, DataDbConnString: row.DataDbConnString },
  };
}

async function resolveEndpointID(endpointName) {
  const pool = await getPool();
  const r = await pool.request()
    .input('n', sql.NVarChar(100), endpointName)
    .query(`SELECT EndpointID FROM admin.Endpoints WHERE Name = @n`);
  if (!r.recordset.length) throw new Error('Endpoint ' + endpointName + ' not found');
  return r.recordset[0].EndpointID;
}

async function ensureJob(endpointID, brandUID) {
  const pool = await getPool();
  const existing = await pool.request()
    .input('eid', sql.Int, endpointID)
    .input('uid', sql.UniqueIdentifier, brandUID)
    .query(`SELECT TOP 1 JobID FROM admin.Jobs WHERE EndpointID = @eid AND BrandUID = @uid`);
  if (existing.recordset.length) return existing.recordset[0].JobID;

  const ins = await pool.request()
    .input('eid', sql.Int, endpointID)
    .input('uid', sql.UniqueIdentifier, brandUID)
    .query(`
      INSERT INTO admin.Jobs (EndpointID, BrandUID, JobType, IsActive, Priority, ConcurrencyKey, ExecutionMode)
      OUTPUT INSERTED.JobID
      VALUES (@eid, @uid, 'INGEST', 1, 40,
              'AMZ_LISTINGS:' + CAST(@uid AS NVARCHAR(50)), 'NODE_NATIVE');
    `);
  return ins.recordset[0].JobID;
}

async function startJobRun(jobID, triggeredBy) {
  const pool = await getPool();
  const r = await pool.request()
    .input('jid', sql.Int, jobID)
    .input('tb',  sql.NVarChar(30), triggeredBy)
    .query(`
      INSERT INTO admin.JobRuns (JobID, StartedAt, Status, TriggeredBy, WorkerType, WorkerHost)
      OUTPUT INSERTED.RunID
      VALUES (@jid, SYSUTCDATETIME(), 'RUNNING', @tb, 'NODE', HOST_NAME());
    `);
  return r.recordset[0].RunID;
}

async function completeJobRun(runID, { status, rowsIngested, errorMessage, workerType }) {
  const pool = await getPool();
  let fingerprint = null;
  if (errorMessage) {
    fingerprint = crypto.createHash('sha256')
      .update(errorMessage.replace(/[0-9a-f-]{36}/gi, '<uuid>').replace(/\d+/g, '<n>'))
      .digest('hex').substring(0, 64);
  }
  await pool.request()
    .input('rid',  sql.BigInt, runID)
    .input('s',    sql.NVarChar(20), status)
    .input('ri',   sql.Int, rowsIngested)
    .input('em',   sql.NVarChar(sql.MAX), errorMessage)
    .input('ef',   sql.NVarChar(64), fingerprint)
    .input('wt',   sql.NVarChar(30), workerType)
    .query(`
      UPDATE admin.JobRuns
         SET EndedAt          = SYSUTCDATETIME(),
             Status            = @s,
             RowsIngested      = @ri,
             ErrorMessage      = @em,
             ErrorFingerprint  = @ef,
             WorkerType        = @wt
       WHERE RunID = @rid;
    `);
}

async function bumpLastAuthed(credentialID) {
  const pool = await getPool();
  await pool.request()
    .input('id', sql.Int, credentialID)
    .query(`
      UPDATE admin.BrandCredentials
         SET LastAuthedAt  = SYSUTCDATETIME(),
             LastAuthError = NULL,
             UpdatedAt     = SYSUTCDATETIME()
       WHERE CredentialID = @id;
    `);
}

module.exports = { runAmazonListings };
