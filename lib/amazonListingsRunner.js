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
const { callSpApi } = require('./amazonApi');
const { runReport, parseTsv } = require('./spApiReports');

const BATCH_SIZE = 500;
const REPORT_TYPE = 'GET_MERCHANT_LISTINGS_ALL_DATA';

// Listings Items API rate limit: 5 rps sustained, 10 burst. Running
// 4 concurrent requests with a 250ms stagger stays comfortably under.
const ENRICH_CONCURRENCY  = 4;
const ENRICH_INTER_MS     = 250;

// Amazon's numeric condition codes used in the flat-file report.
// Published at https://sellercentral.amazon.com/gp/help/external/200202070
const CONDITION_CODE_MAP = {
  '1':  'New',
  '11': 'New',
  '2':  'Used-LikeNew',
  '3':  'Used-VeryGood',
  '4':  'Used-Good',
  '5':  'Used-Acceptable',
  '6':  'Refurbished',
  '7':  'Collectible-LikeNew',
  '8':  'Collectible-VeryGood',
  '9':  'Collectible-Good',
  '10': 'Collectible-Acceptable',
};

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

    /* 2. Enrich each row via Listings Items API to pick up fields the
          flat-file report doesn't carry (bullets, images, search terms,
          brand, category, sales rank). Happens here so the diff in
          step 3 includes these fields as tracked deltas. */
    if (!ctx.cred.SellerID) {
      console.warn('[runner/listings] no SellerID on credential — skipping Listings Items enrichment');
    } else {
      await enrichAll(ctx, fresh, marketplaceId);
    }

    /* 3. Pull our current known-state for this brand + marketplace. */
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
    Condition:     decodeConditionCode(r['item-condition']),
    Status:        r['status'] || null,
    IssueCount:    null,
    _RawPayload:   JSON.stringify(r),                     // keep original row for debug
  };
}

function decodeConditionCode(code) {
  if (code == null || code === '') return null;
  const s = String(code).trim();
  return CONDITION_CODE_MAP[s] || s;  // unmapped = keep original so we can discover new codes
}

/* =========================================================================
   Listings Items API enrichment — fills in fields the flat-file report
   doesn't carry. One API call per SKU; rate-limited to 4 concurrent with
   a 250ms stagger (well under Amazon's 5 rps / 10 burst quota).

   On per-SKU failure, we log + keep the row with report-only fields. The
   enrichment is best-effort, not required for the run to succeed.
   ========================================================================= */
async function enrichAll(ctx, rows, marketplaceId) {
  const startedAt = Date.now();
  let done = 0, failed = 0;

  // Concurrency control: a rolling worker pool so we don't flood the API.
  const queue = rows.slice();
  const workers = Array.from({ length: ENRICH_CONCURRENCY }, async () => {
    while (queue.length) {
      const row = queue.shift();
      if (!row || !row.SKU) continue;
      try {
        await enrichOne(ctx, row, marketplaceId);
        done++;
      } catch (e) {
        failed++;
        if (failed <= 5) {
          console.warn('[runner/listings] enrich failed sku=' + row.SKU + ': ' + e.message);
        }
      }
      await sleep(ENRICH_INTER_MS);
    }
  });
  await Promise.all(workers);
  console.log('[runner/listings] enrichment done',
    { total: rows.length, enriched: done, failed, elapsedS: Math.round((Date.now() - startedAt) / 1000) });
}

async function enrichOne(ctx, row, marketplaceId) {
  const path = '/listings/2021-08-01/items/' +
    encodeURIComponent(ctx.cred.SellerID) + '/' + encodeURIComponent(row.SKU) +
    '?marketplaceIds=' + encodeURIComponent(marketplaceId) +
    '&includedData=summaries,attributes';
  const res = await callSpApi(ctx, path);
  applyListingsItemsToRow(row, res);
}

function applyListingsItemsToRow(row, apiRes) {
  const summaries = Array.isArray(apiRes && apiRes.summaries) ? apiRes.summaries : [];
  const summary = summaries[0] || {};
  const attributes = (apiRes && apiRes.attributes) || {};

  if (summary.asin) row.ASIN = summary.asin;
  if (summary.itemName)  row.Title = summary.itemName;
  if (summary.brandName) row.Brand = summary.brandName;
  if (summary.productType) row.ProductType = summary.productType;
  if (summary.itemClassification) row.Category = summary.itemClassification;
  if (summary.websiteDisplayGroupName && !row.Category) row.Category = summary.websiteDisplayGroupName;

  // Images: summary.mainImage + otherImages
  const imgs = [];
  if (summary.mainImage && summary.mainImage.link) imgs.push(summary.mainImage.link);
  if (Array.isArray(summary.otherImages)) {
    for (const img of summary.otherImages) {
      if (img && img.link) imgs.push(img.link);
    }
  }
  if (imgs.length) row.ImagesJSON = JSON.stringify(imgs);

  // Sales rank: summary.salesRanks[0] — pick the top one
  if (Array.isArray(summary.salesRanks) && summary.salesRanks.length) {
    const top = summary.salesRanks[0];
    if (top && typeof top.rank === 'number') {
      row.SalesRank = top.rank;
      row.SalesRankCategory = top.title || top.classificationRanks?.[0]?.title || null;
    }
  }

  // Bullets from attributes (shape: { bullet_point: [{ value, language_tag? }] })
  const bullets = pickAttr(attributes, 'bullet_point');
  if (Array.isArray(bullets)) {
    for (let i = 0; i < Math.min(5, bullets.length); i++) {
      const v = bullets[i] && (bullets[i].value || bullets[i]);
      if (v) row['Bullet' + (i + 1)] = String(v).slice(0, 500);
    }
  }

  // Description — attributes.product_description[0].value, typically long HTML
  const desc = firstAttrValue(attributes, 'product_description');
  if (desc && (!row.Description || desc.length > row.Description.length)) {
    row.Description = desc;
  }

  // Search terms — attributes.generic_keyword[0].value
  const search = firstAttrValue(attributes, 'generic_keyword');
  if (search) row.SearchTerms = search;

  // Condition — prefer the report's decoded code, but fall back if empty
  const cond = firstAttrValue(attributes, 'condition_type');
  if (cond && !row.Condition) row.Condition = cond;
}

function pickAttr(attributes, key) {
  if (!attributes || typeof attributes !== 'object') return null;
  const v = attributes[key];
  return Array.isArray(v) ? v : null;
}

function firstAttrValue(attributes, key) {
  const arr = pickAttr(attributes, key);
  if (!arr || !arr.length) return null;
  const first = arr[0];
  if (first && typeof first === 'object' && 'value' in first) return first.value;
  return typeof first === 'string' ? first : null;
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function readCurrentListings(brandPool, brandUID, marketplaceId) {
  const r = await brandPool.request()
    .input('uid', sql.UniqueIdentifier, brandUID)
    .input('mp',  sql.NVarChar(20), marketplaceId)
    .query(`
      SELECT SKU, MarketplaceID, ASIN, ProductType, Title, Brand, Description,
             Bullet1, Bullet2, Bullet3, Bullet4, Bullet5,
             SearchTerms, Category, BrowseNodeID, ImagesJSON,
             Price, Currency, Quantity, Condition, Status, IssueCount,
             SalesRank, SalesRankCategory,
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
        Status:       'OBSERVED',
      });
      stats.changesDetected++;
      continue;
    }

    const curHash = cur._SourceRowHash ? Buffer.from(cur._SourceRowHash).toString('hex') : null;
    if (curHash === fresh._SourceRowHashHex) {
      // Tracked-fields hash matches. Rank moves constantly, so it's not
      // in TRACKED_FIELDS. But we DO want the current-state table to
      // keep rank fresh — push to upserts without emitting change rows.
      const rankDiffers =
        (cur.SalesRank ?? null)       !== (fresh.SalesRank ?? null) ||
        (cur.SalesRankCategory || '') !== (fresh.SalesRankCategory || '');
      if (rankDiffers) upserts.push(fresh);
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
        Status:       'OBSERVED',
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
      Status:       'OBSERVED',
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
    SalesRank: r.SalesRank ?? null,
    SalesRankCategory: r.SalesRankCategory || null,
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
      -- report submission via amazonApi.getAccessToken. SellerID
      -- needed for per-SKU Listings Items API enrichment.
      SELECT bc.CredentialID, bc.BrandUID, bc.ConnectorID,
             bc.Region, bc.MarketplaceIDs, bc.AccountIdentifier AS SellerID,
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
      SellerID: row.SellerID,
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
