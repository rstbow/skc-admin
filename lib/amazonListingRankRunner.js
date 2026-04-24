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

    /* 2. Serial fetch with 500ms stagger (2 rps). */
    const rows = [];
    let asinsQueried = 0;
    let asinsFailed  = 0;
    const startedAt = Date.now();
    for (let i = 0; i < asins.length; i++) {
      const asin = asins[i];
      try {
        const rankRows = await fetchAndFlattenRanks(ctx, asin, marketplaceId, skusByAsin.get(asin), today);
        rows.push(...rankRows);
        asinsQueried++;
      } catch (e) {
        asinsFailed++;
        if (asinsFailed <= 5) console.warn('[runner/rank] asin=' + asin + ' failed: ' + e.message);
      }
      if (i < asins.length - 1) await sleep(CATALOG_API_DELAY_MS);
    }
    console.log('[runner/rank] fetch done',
      { asinsQueried, asinsFailed, rankRowsEmitted: rows.length,
        elapsedS: Math.round((Date.now() - startedAt) / 1000) });

    /* 3. Bulk MERGE */
    let merge = { Inserted: 0, Updated: 0, Unchanged: 0, Total: 0 };
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const slice = rows.slice(i, i + BATCH_SIZE);
      const r = await mergeRankRows(brandPool, ctx.brand.BrandUID, runID, slice);
      merge.Inserted += r.Inserted;
      merge.Updated  += r.Updated;
      merge.Unchanged += r.Unchanged;
      merge.Total    += r.Total;
    }

    await completeJobRun(runID, {
      status: 'SUCCESS', rowsIngested: rows.length, errorMessage: null, workerType: 'NODE',
    });
    await bumpLastAuthed(credentialID);

    return {
      runID, ok: true, brand: ctx.brand, marketplaceId,
      asinsQueried, asinsFailed,
      rowsInserted:  merge.Inserted,
      rowsUpdated:   merge.Updated,
      rowsUnchanged: merge.Unchanged,
      eventsProcessed: rows.length,
      snapshotDate: today,
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
async function fetchAndFlattenRanks(ctx, asin, marketplaceId, skus, snapshotDate) {
  const path = '/catalog/2022-04-01/items/' + encodeURIComponent(asin) +
    '?marketplaceIds=' + encodeURIComponent(marketplaceId) +
    '&includedData=salesRanks,summaries';
  const res = await callSpApi(ctx, path);

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
             bc.Region, bc.MarketplaceIDs, bc.SellerID,
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
