/**
 * Amazon listing rank snapshot runner.
 *
 * Daily per-ASIN pull of Amazon Catalog Items → salesRanks, stored as
 * one row per (brand × SKU × category × snapshot-day) in
 * raw.amz_listing_rank.
 *
 * Driven by app2's rank-tracking feature (tasks/~037-amazon-rank-tracking.md).
 *
 * Pipeline:
 *   1. Read active SKUs + ASINs from raw.amz_listings for the brand
 *      (deduped by ASIN — many SKUs share an ASIN via variations)
 *   2. For each unique ASIN, call
 *        GET /catalog/2022-04-01/items/{asin}
 *            ?marketplaceIds=...&includedData=salesRanks
 *   3. Parse salesRanks[marketplaceId match]:
 *        - displayGroupRanks → CategoryType='PRIMARY'
 *        - classificationRanks → CategoryType='CLASSIFICATION'
 *   4. Fan back out: one row per (SKU, category) — multiple SKUs
 *      sharing an ASIN each get their own row with the same rank data
 *   5. Bulk MERGE via raw.usp_merge_amz_listing_rank
 *
 * Rate limit: Catalog Items API is 2 rps burst + 2 rps sustained per
 * application. Running serial with 500ms between calls = exactly 2 rps;
 * callSpApi's 429 backoff catches occasional bursts.
 *
 * At ZenToes scale (~742 SKUs, ~400 unique ASINs), ~3.5 min per run.
 * Well under the scheduler's 1h concurrency window.
 */
const crypto = require('crypto');
const { sql, getPool } = require('../config/db');
const { getBrandPool } = require('./brandDb');
const { callSpApi } = require('./amazonApi');

const BATCH_SIZE = 500;
const CATALOG_API_DELAY_MS = 500;  // 2 rps

async function runAmazonListingRank({
  credentialID,
  triggeredBy = 'MANUAL',
  userID = null,
  params = {},
} = {}) {
  if (!credentialID) throw new Error('credentialID is required');

  const ctx = await loadCredentialContext(credentialID);
  if (!ctx) throw new Error('Credential not found');
  if (ctx.connector.Name !== 'AMAZON_SP_API') {
    throw new Error('Rank runner only supports AMAZON_SP_API credentials');
  }

  const endpointID = await resolveEndpointID('AMZ_LISTING_RANK_SNAPSHOT');
  const jobID = await ensureJob(endpointID, ctx.brand.BrandUID);
  const runID = await startJobRun(jobID, triggeredBy);

  try {
    const brandPool = await getBrandPool(ctx.brand.BrandUID);
    const marketplaceId = params.marketplaceId || 'ATVPDKIKX0DER';
    const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

    /* 1. Enumerate SKUs + ASINs we know about for this brand */
    const skusByAsin = await readActiveSkusGroupedByAsin(brandPool, ctx.brand.BrandUID, marketplaceId);
    const asins = Array.from(skusByAsin.keys());
    console.log('[runner/rank] brand=' + ctx.brand.BrandName + ' ASINs=' + asins.length +
      ' (covering ' + Array.from(skusByAsin.values()).reduce((n, skus) => n + skus.length, 0) + ' SKUs)');

    if (!asins.length) {
      await completeJobRun(runID, {
        status: 'SUCCESS', rowsIngested: 0, errorMessage: null, workerType: 'NODE',
      });
      return {
        runID, ok: true, brand: ctx.brand, marketplaceId,
        asinsQueried: 0, rowsInserted: 0, rowsUpdated: 0, rowsUnchanged: 0,
        message: 'No active SKUs with ASIN yet. Run the listings job first to populate raw.amz_listings.',
      };
    }

    /* 2. Serial fetch with 500ms stagger (2 rps). Each call's catalog
          attributes go into a parallel accumulator that we MERGE into
          raw.amz_listings after the loop, so one Catalog Items call
          serves both rank + listings enrichment. */
    const rows = [];
    const listingsRows = [];
    let asinsQueried = 0;
    let asinsFailed  = 0;
    const startedAt = Date.now();
    for (let i = 0; i < asins.length; i++) {
      const asin = asins[i];
      try {
        const rankRows = await fetchAndFlattenRanks(ctx, asin, marketplaceId, skusByAsin.get(asin), today, listingsRows);
        rows.push(...rankRows);
        asinsQueried++;
      } catch (e) {
        asinsFailed++;
        if (asinsFailed <= 5) console.warn('[runner/rank] asin=' + asin + ' failed: ' + e.message);
      }
      if (i < asins.length - 1) await sleep(CATALOG_API_DELAY_MS);
    }
    console.log('[runner/rank] fetch done',
      { asinsQueried, asinsFailed,
        rankRowsEmitted: rows.length,
        listingRowsEnriched: listingsRows.length,
        elapsedS: Math.round((Date.now() - startedAt) / 1000) });

    /* 3. Bulk MERGE rank rows */
    let merge = { Inserted: 0, Updated: 0, Unchanged: 0, Total: 0 };
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const slice = rows.slice(i, i + BATCH_SIZE);
      const r = await mergeRankRows(brandPool, ctx.brand.BrandUID, runID, slice);
      merge.Inserted += r.Inserted;
      merge.Updated  += r.Updated;
      merge.Unchanged += r.Unchanged;
      merge.Total    += r.Total;
    }

    /* 4. Bulk MERGE listings enrichment via raw.usp_merge_amz_listings.
          This populates Brand / Bullets / SearchTerms / Images / Category /
          SalePrice trio that the flat-file report doesn't carry. */
    let listingsMerge = { Inserted: 0, Updated: 0, Unchanged: 0 };
    if (listingsRows.length) {
      // Hash on the catalog-derived fields so subsequent same-day runs
      // skip rewriting unchanged content.
      for (const r of listingsRows) {
        const canonical = [r.Title, r.Brand, r.Bullet1, r.Bullet2, r.Bullet3, r.Bullet4, r.Bullet5,
                           r.SearchTerms, r.Category, r.ImagesJSON, r.Price, r.SalePrice,
                           r.SalePriceStart, r.SalePriceEnd].map((v) => v ?? '').join('\n');
        r._SourceRowHashHex = crypto.createHash('sha256').update(canonical).digest('hex');
      }
      for (let i = 0; i < listingsRows.length; i += BATCH_SIZE) {
        const slice = listingsRows.slice(i, i + BATCH_SIZE);
        const r = await mergeListingsEnrichment(brandPool, ctx.brand.BrandUID, runID, slice);
        listingsMerge.Inserted += r.Inserted;
        listingsMerge.Updated  += r.Updated;
        listingsMerge.Unchanged += r.Unchanged;
      }
      console.log('[runner/rank] listings enrichment merged', listingsMerge);
    }

    await completeJobRun(runID, {
      status: 'SUCCESS', rowsIngested: rows.length, errorMessage: null, workerType: 'NODE',
    });
    await bumpLastAuthed(credentialID);

    return {
      runID, ok: true, brand: ctx.brand, marketplaceId,
      asinsQueried, asinsFailed,
      rankRowsInserted:    merge.Inserted,
      rankRowsUpdated:     merge.Updated,
      rankRowsUnchanged:   merge.Unchanged,
      listingsEnriched:    listingsMerge.Inserted + listingsMerge.Updated,
      listingsUnchanged:   listingsMerge.Unchanged,
      eventsProcessed:     rows.length,
      snapshotDate:        today,
    };
  } catch (e) {
    console.error('[runner/rank]', e);
    await completeJobRun(runID, {
      status: 'FAILED', rowsIngested: null,
      errorMessage: (e.message || 'Unknown error').slice(0, 4000),
      workerType: 'NODE',
    });
    throw e;
  }
}

/* =========================================================================
   Source of SKUs/ASINs — only pull rank for listings we already ingested
   via the listings runner. Dedupe by ASIN so shared-ASIN variation SKUs
   cost one API call not N.
   ========================================================================= */
async function readActiveSkusGroupedByAsin(brandPool, brandUID, marketplaceId) {
  const r = await brandPool.request()
    .input('uid', sql.UniqueIdentifier, brandUID)
    .input('mp',  sql.NVarChar(20), marketplaceId)
    .query(`
      SELECT SKU, ASIN
      FROM raw.amz_listings
      WHERE _BrandUID = @uid
        AND MarketplaceID = @mp
        AND ASIN IS NOT NULL AND LEN(ASIN) > 0;
    `);
  const map = new Map();
  for (const row of r.recordset) {
    if (!map.has(row.ASIN)) map.set(row.ASIN, []);
    map.get(row.ASIN).push(row.SKU);
  }
  return map;
}

/* =========================================================================
   Catalog Items API — pull ranks for one ASIN, fan out to rows per SKU.
   ========================================================================= */
async function fetchAndFlattenRanks(ctx, asin, marketplaceId, skus, snapshotDate, listingsRowsAccumulator) {
  // Pull everything we need from one call — rank, attributes, images,
  // classifications, productTypes. Same API call we were already
  // making, just with a wider includedData set. This lets the rank
  // runner double as the catalog-enrichment runner per the
  // 2026-04-24-listings-richer-attributes handoff.
  const path = '/catalog/2022-04-01/items/' + encodeURIComponent(asin) +
    '?marketplaceIds=' + encodeURIComponent(marketplaceId) +
    '&includedData=salesRanks,summaries,attributes,images,classifications,productTypes';
  const res = await callSpApi(ctx, path);

  /* ---- Listings catalog enrichment (separate accumulator; written
          in batch via usp_merge_amz_listings after the per-ASIN loop) ---- */
  const enrichedFields = extractCatalogFields(res, marketplaceId);
  if (enrichedFields && listingsRowsAccumulator) {
    for (const sku of skus) {
      listingsRowsAccumulator.push({
        ...enrichedFields,
        SKU: sku,
        MarketplaceID: marketplaceId,
        ASIN: asin,
      });
    }
  }

  /* ---- Rank rows (existing behavior) ---- */
  const salesRanksBlock = (res && Array.isArray(res.salesRanks)) ? res.salesRanks : [];
  const marketMatch = salesRanksBlock.find((b) => b && b.marketplaceId === marketplaceId);
  if (!marketMatch) return [];

  const rows = [];
  // PRIMARY — displayGroupRanks
  for (const g of (marketMatch.displayGroupRanks || [])) {
    if (typeof g.rank !== 'number' && g.rank != null) continue;
    const categoryID = g.websiteDisplayGroup || g.title || 'unknown';
    for (const sku of skus) {
      rows.push(buildRankRow({
        sku, asin, marketplaceId, snapshotDate,
        type: 'PRIMARY',
        title: g.title,
        categoryID,
        rank: g.rank ?? null,
        link: g.link,
      }));
    }
  }
  // CLASSIFICATION — classificationRanks
  for (const c of (marketMatch.classificationRanks || [])) {
    if (typeof c.rank !== 'number' && c.rank != null) continue;
    const categoryID = c.classificationId || c.title || 'unknown';
    for (const sku of skus) {
      rows.push(buildRankRow({
        sku, asin, marketplaceId, snapshotDate,
        type: 'CLASSIFICATION',
        title: c.title,
        categoryID,
        rank: c.rank ?? null,
        link: c.link,
      }));
    }
  }
  return rows;
}

/**
 * Pull the catalog attributes the skc-api Listings UI needs from a
 * Catalog Items response. Returns a partial row object (without SKU
 * since shared ASINs fan out to multiple SKUs) — caller fills SKU +
 * MarketplaceID + ASIN.
 *
 * Field mapping follows skc-handoffs/from-api/2026-04-24-listings-richer-attributes.md.
 *
 * Sale-price precedence: per the handoff, surface the **retail** offer
 * (no quantity_discount_type set) — that's what regular buyers see.
 * Business-tier discount offers are ignored.
 */
function extractCatalogFields(res, marketplaceId) {
  if (!res || typeof res !== 'object') return null;

  const fields = {
    Brand: null, ProductType: null,
    Bullet1: null, Bullet2: null, Bullet3: null, Bullet4: null, Bullet5: null,
    SearchTerms: null, Category: null, BrowseNodeID: null,
    ImagesJSON: null,
    Price: null, Currency: null,
    SalePrice: null, SalePriceStart: null, SalePriceEnd: null,
    Title: null,
  };

  /* summaries[0] — high-level fields per marketplace */
  const summaries = Array.isArray(res.summaries) ? res.summaries : [];
  const summary = summaries.find((s) => s && s.marketplaceId === marketplaceId) || summaries[0] || {};
  // Brand precedence: summary.brand → summary.brandName → attributes.brand[0].value.
  // Catalog Items uses `brand` in summaries; some ASINs only have
  // brand under attributes.brand (older listings, reseller listings,
  // FBM listings without brand registry attribution at the summary
  // level). Try the summary first, fall back to the attribute path.
  if (summary.brand)            fields.Brand        = summary.brand;
  else if (summary.brandName)   fields.Brand        = summary.brandName;
  else {
    const attrBrand = firstAttrValue(attrs, 'brand');
    if (attrBrand) fields.Brand = String(attrBrand).slice(0, 200);
  }
  if (summary.itemName)         fields.Title        = summary.itemName;
  if (summary.itemClassification) fields.Category   = summary.itemClassification;
  if (summary.websiteDisplayGroupName && !fields.Category) fields.Category = summary.websiteDisplayGroupName;

  /* productTypes[0].productType */
  const pts = Array.isArray(res.productTypes) ? res.productTypes : [];
  const ptMatch = pts.find((p) => p && p.marketplaceId === marketplaceId) || pts[0];
  if (ptMatch && ptMatch.productType) fields.ProductType = ptMatch.productType;

  /* classifications[0].classifications[0].displayName */
  const cls = Array.isArray(res.classifications) ? res.classifications : [];
  const clsMatch = cls.find((c) => c && c.marketplaceId === marketplaceId) || cls[0];
  if (clsMatch && Array.isArray(clsMatch.classifications) && clsMatch.classifications[0]) {
    const inner = clsMatch.classifications[0];
    if (inner.displayName && !fields.Category) fields.Category = inner.displayName;
    if (inner.classificationId)                fields.BrowseNodeID = String(inner.classificationId).slice(0, 50);
  }

  /* images[0].images[].link → JSON array, MAIN first, up to 8 total */
  const imgsAll = Array.isArray(res.images) ? res.images : [];
  const imgsMatch = imgsAll.find((g) => g && g.marketplaceId === marketplaceId) || imgsAll[0];
  if (imgsMatch && Array.isArray(imgsMatch.images) && imgsMatch.images.length) {
    const links = [];
    // MAIN variant first
    for (const img of imgsMatch.images) {
      if (img && img.variant === 'MAIN' && img.link) { links.push(img.link); break; }
    }
    // Then up to 7 others (skip dup MAIN if it gets picked up)
    for (const img of imgsMatch.images) {
      if (links.length >= 8) break;
      if (img && img.link && !links.includes(img.link)) links.push(img.link);
    }
    if (links.length) fields.ImagesJSON = JSON.stringify(links);
  }

  /* attributes — bullets, search terms, list price, purchasable_offer */
  const attrs = (res && res.attributes) || {};
  const bullets = pickAttr(attrs, 'bullet_point');
  if (Array.isArray(bullets)) {
    for (let i = 0; i < Math.min(5, bullets.length); i++) {
      const v = bullets[i] && (bullets[i].value || bullets[i]);
      if (v) fields['Bullet' + (i + 1)] = String(v).slice(0, 500);
    }
  }
  const search = firstAttrValue(attrs, 'generic_keyword');
  if (search) fields.SearchTerms = String(search).slice(0, 500);

  // list_price — { value: { Amount, CurrencyCode } } or { value, currency } depending on schema
  const listPrice = Array.isArray(attrs.list_price) ? attrs.list_price[0] : null;
  if (listPrice) {
    const amount = (listPrice.value && (listPrice.value.Amount ?? listPrice.value.amount)) ?? listPrice.value;
    const currency = (listPrice.value && (listPrice.value.CurrencyCode ?? listPrice.value.currency)) || listPrice.currency || null;
    if (Number.isFinite(Number(amount))) fields.Price = Number(amount);
    if (currency) fields.Currency = currency;
  }

  // purchasable_offer is NOT in Catalog Items API responses — it's
  // seller-specific data that lives in Listings Items API. Sale price
  // extraction is owned by the listings runner (applySalePriceFromOffers
  // in lib/amazonListingsRunner.js, called via Listings Items API with
  // includedData=offers). Don't try to read it here.

  return fields;
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

function buildRankRow({ sku, asin, marketplaceId, snapshotDate, type, title, categoryID, rank, link }) {
  const row = {
    SKU: sku,
    ASIN: asin,
    MarketplaceID: marketplaceId,
    SnapshotDate: snapshotDate,
    CategoryType: type,
    CategoryTitle: title || null,
    CategoryID: String(categoryID).slice(0, 100),
    Rank: rank,
    SourceLink: link || null,
  };
  // Hash the rank-bearing fields so identical re-pulls on the same day
  // are skipped by the hash-gated MERGE.
  const canonical = [type, categoryID, rank ?? ''].join('|');
  row._SourceRowHashHex = crypto.createHash('sha256').update(canonical).digest('hex');
  return row;
}

/* =========================================================================
   Proc call
   ========================================================================= */
async function mergeRankRows(brandPool, brandUID, runID, rows) {
  if (!rows.length) return { Inserted: 0, Updated: 0, Unchanged: 0, Total: 0 };
  const rowsJson = JSON.stringify(rows.map((r) => ({
    SKU: r.SKU, ASIN: r.ASIN, MarketplaceID: r.MarketplaceID,
    SnapshotDate: r.SnapshotDate,
    CategoryType: r.CategoryType, CategoryTitle: r.CategoryTitle, CategoryID: r.CategoryID,
    Rank: r.Rank != null ? String(r.Rank) : null,
    SourceLink: r.SourceLink,
    _SourceRowHashHex: r._SourceRowHashHex,
  })));
  const result = await brandPool.request()
    .input('BrandUID',    sql.UniqueIdentifier, brandUID)
    .input('SourceRunID', sql.BigInt, runID)
    .input('RowsJson',    sql.NVarChar(sql.MAX), rowsJson)
    .execute('raw.usp_merge_amz_listing_rank');
  return result.recordset[0] || { Inserted: 0, Updated: 0, Unchanged: 0, Total: rows.length };
}

/**
 * Calls raw.usp_merge_amz_listings to upsert the catalog-enriched
 * fields (bullets, images, brand, sale price, etc.) into the same
 * raw.amz_listings table the listings runner writes to. Hash-gated by
 * the proc — unchanged rows are silent UPDATEs that don't touch tracked
 * fields the listings runner owns (Title/Price/Quantity/Status from
 * the flat-file report).
 */
async function mergeListingsEnrichment(brandPool, brandUID, runID, rows) {
  if (!rows.length) return { Inserted: 0, Updated: 0, Unchanged: 0 };
  const rowsJson = JSON.stringify(rows.map((r) => ({
    SKU: r.SKU, MarketplaceID: r.MarketplaceID, ASIN: r.ASIN,
    ProductType: r.ProductType, Title: r.Title, Brand: r.Brand,
    Description: null,
    Bullet1: r.Bullet1, Bullet2: r.Bullet2, Bullet3: r.Bullet3,
    Bullet4: r.Bullet4, Bullet5: r.Bullet5,
    SearchTerms: r.SearchTerms, Category: r.Category, BrowseNodeID: r.BrowseNodeID,
    ImagesJSON: r.ImagesJSON,
    Price: r.Price != null ? String(r.Price) : null,
    Currency: r.Currency,
    Quantity: null, Condition: null, Status: null, IssueCount: null,
    SalesRank: null, SalesRankCategory: null,    // rank goes into raw.amz_listing_rank, not here
    SalePrice: r.SalePrice != null ? String(r.SalePrice) : null,
    SalePriceStart: r.SalePriceStart || null,
    SalePriceEnd:   r.SalePriceEnd   || null,
    _RawPayload: null,
    _SourceRowHashHex: r._SourceRowHashHex,
  })));
  const result = await brandPool.request()
    .input('BrandUID',    sql.UniqueIdentifier, brandUID)
    .input('SourceRunID', sql.BigInt, runID)
    .input('RowsJson',    sql.NVarChar(sql.MAX), rowsJson)
    .execute('raw.usp_merge_amz_listings');
  return result.recordset[0] || { Inserted: 0, Updated: 0, Unchanged: 0, Total: rows.length };
}

/* =========================================================================
   JobRun helpers + credential loader — duplicated from the fin-events /
   listings runners. When we grow a fourth or fifth runner, extract to
   lib/jobRunHelpers.js.
   ========================================================================= */
async function loadCredentialContext(credentialID) {
  const pool = await getPool();
  const r = await pool.request()
    .input('id', sql.Int, credentialID)
    .query(`
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
      AppClientID: row.AppClientID, AppClientSecret_Enc: row.AppClientSecret_Enc,
      ApiVersion: row.ApiVersion,
    },
    cred: {
      Region: row.Region, MarketplaceIDs: row.MarketplaceIDs, SellerID: row.SellerID,
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
  if (!r.recordset.length) throw new Error('Endpoint ' + endpointName + ' not found — run 026 seed.');
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
              'AMZ_RANK:' + CAST(@uid AS NVARCHAR(50)), 'NODE_NATIVE');
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
         SET EndedAt = SYSUTCDATETIME(), Status = @s, RowsIngested = @ri,
             ErrorMessage = @em, ErrorFingerprint = @ef, WorkerType = @wt
       WHERE RunID = @rid;
    `);
}

async function bumpLastAuthed(credentialID) {
  const pool = await getPool();
  await pool.request()
    .input('id', sql.Int, credentialID)
    .query(`
      UPDATE admin.BrandCredentials
         SET LastAuthedAt = SYSUTCDATETIME(), LastAuthError = NULL, UpdatedAt = SYSUTCDATETIME()
       WHERE CredentialID = @id;
    `);
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

module.exports = { runAmazonListingRank };
