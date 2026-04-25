/**
 * Amazon Financial Events runner — Phase 3 pilot.
 *
 * End-to-end flow:
 *   1. Insert admin.JobRuns row (Status=RUNNING)
 *   2. Load credential + connector
 *   3. Paginate GET /finances/v0/financialEvents
 *   4. Flatten events into raw.amz_financial_events row shape + compute hashes
 *   5. Batch-MERGE via raw.usp_merge_amz_financial_events
 *   6. Update admin.JobRuns with final counts + status
 *   7. Bump admin.BrandCredentials.LastAuthedAt on success
 *
 * Idempotent: re-running the same window = zero writes (hash match skips
 * the UPDATE branch in the MERGE proc).
 */
const crypto = require('crypto');
const { sql, getPool } = require('../config/db');
const { getBrandPool } = require('./brandDb');
const { callSpApi, paginateSpApi } = require('./amazonApi');
const { computeWindows } = require('./windowChunks');

// Per-MERGE batch size.
//   1000 → 30s timeouts late in ZenToes 180d backfill
//   500  → 120s timeouts in same job (hot lock + many rows)
//   250  → current. Halves the surface for any single MERGE to lock.
// If we still flake at 250, drop chunkDays before halving again — at
// some point the per-call HTTP overhead dominates.
const BATCH_SIZE = 250;
// Per-window defaults. Generous because a 30-day chunk at a high-volume
// seller (~1000 events/day) = 30k events = ~300 pages.
const DEFAULT_PAGE_LIMIT  = 1000;
const DEFAULT_TIME_LIMIT  = 600_000;   // 10 min per window
const DEFAULT_CHUNK_DAYS  = 30;

/* =========================================================================
   Public entry point
   ========================================================================= */
async function runAmazonFinancialEvents({
  credentialID,
  daysBack,
  postedAfter,
  postedBefore,
  chunkDays,       // window size per chunk — default 30
  pageLimit,       // max pages per window — default 1000
  timeLimitMs,     // max wall clock per window — default 10min
  pageDelayMs,     // ms between SP-API pages — default 2100 (recurring)
                   // backfill jobs pass 3000+ via Params for extra quota slack
  triggeredBy = 'MANUAL',
  userID = null,
}) {
  if (!credentialID) throw new Error('credentialID is required');

  const ctx = await loadCredentialContext(credentialID);
  if (!ctx) throw new Error('Credential not found');
  if (ctx.connector.Name !== 'AMAZON_SP_API') {
    throw new Error('Runner only supports AMAZON_SP_API credentials');
  }

  const endpointID = await resolveEndpointID('AMZ_FINANCIAL_EVENTS');
  const jobID     = await ensureJob(endpointID, ctx.brand.BrandUID);
  const runID     = await startJobRun(jobID, triggeredBy);

  try {
    /* Compute the window set. One-shot runs (daysBack=2, the recurring
       default) collapse to a single window. BACKFILL runs (daysBack=180)
       slice into ~6 chunks. The chunker clamps the range so we never
       query the future. */
    const windows = computeWindows({
      postedAfter, postedBefore, daysBack,
      chunkDays: chunkDays || DEFAULT_CHUNK_DAYS,
      // Amazon Finances rejects PostedBefore within ~2 min of now.
      // Stay 5 min behind for safety. Next recurring cycle's daysBack=2
      // overlap catches any gap (hash-MERGE makes overlap free).
      endBufferMs: 5 * 60 * 1000,
    });

    console.log('[runner/fin-events] start', {
      credentialID, brandUID: ctx.brand.BrandUID, brandName: ctx.brand.BrandName,
      region: ctx.cred.Region, marketplaces: ctx.cred.MarketplaceIDs,
      chunks: windows.length,
      range: windows.length
        ? { first: windows[0].after, last: windows[windows.length-1].before }
        : null,
    });

    // Publish chunks-total to admin.JobRuns so the UI shows "0/N" instantly
    // rather than waiting for the first chunk to complete.
    await updateJobRunProgress(runID, {
      chunksTotal: windows.length,
      chunksCompleted: 0,
      rowsIngested: 0,
    });

    const brandUID = ctx.brand.BrandUID;
    const brandPool = await getBrandPool(brandUID);

    const totals = {
      events: 0, inserted: 0, updated: 0, unchanged: 0, pages: 0,
      rawEventCounts: {},
      truncatedChunks: 0,
    };

    for (let i = 0; i < windows.length; i++) {
      const w = windows[i];
      const chunkStart = Date.now();
      console.log(`[runner/fin-events] chunk ${i+1}/${windows.length}`, w);

      const chunkResult = await pullMergeWindow(ctx, brandPool, runID, w, {
        pageLimit:   pageLimit   || DEFAULT_PAGE_LIMIT,
        timeLimitMs: timeLimitMs || DEFAULT_TIME_LIMIT,
        pageDelayMs: Number.isFinite(pageDelayMs) ? pageDelayMs : 2100,
      });

      totals.events     += chunkResult.eventsProcessed;
      totals.inserted   += chunkResult.rowsInserted;
      totals.updated    += chunkResult.rowsUpdated;
      totals.unchanged  += chunkResult.rowsUnchanged;
      totals.pages      += chunkResult.pages;
      if (chunkResult.truncated) totals.truncatedChunks++;
      for (const [k, n] of Object.entries(chunkResult.rawEventCounts || {})) {
        totals.rawEventCounts[k] = (totals.rawEventCounts[k] || 0) + n;
      }

      // Update progress so the Jobs page can render "chunk 3/6 · 27K rows".
      await updateJobRunProgress(runID, {
        chunksCompleted: i + 1,
        chunksTotal: windows.length,
        rowsIngested: totals.events,
      });

      console.log(`[runner/fin-events] chunk ${i+1}/${windows.length} done`, {
        events: chunkResult.eventsProcessed,
        pages: chunkResult.pages,
        truncated: chunkResult.truncated || false,
        elapsedMs: Date.now() - chunkStart,
      });
    }

    await completeJobRun(runID, {
      status: 'SUCCESS',
      rowsIngested: totals.events,
      errorMessage: null,
      workerType: 'NODE',
    });
    await bumpLastAuthed(credentialID);

    return {
      runID,
      ok: true,
      brand: ctx.brand,
      chunks: windows.length,
      truncatedChunks: totals.truncatedChunks,
      truncated: totals.truncatedChunks > 0,
      truncatedReason: totals.truncatedChunks > 0
        ? totals.truncatedChunks + ' chunk(s) hit per-window cap — consider lowering chunkDays'
        : null,
      window: windows.length
        ? { postedAfter: windows[0].after, postedBefore: windows[windows.length-1].before }
        : null,
      pages: totals.pages,
      eventsProcessed: totals.events,
      rowsInserted: totals.inserted,
      rowsUpdated: totals.updated,
      rowsUnchanged: totals.unchanged,
      rawEventCounts: totals.rawEventCounts,
    };
  } catch (e) {
    console.error('[runner/amz-financial-events]', e);
    await completeJobRun(runID, {
      status: 'FAILED',
      rowsIngested: null,
      errorMessage: (e.message || 'Unknown error').slice(0, 4000),
      workerType: 'NODE',
    });
    throw e;
  }
}

/**
 * Pull + flatten + dedupe + MERGE one window. Isolates the per-window
 * work so the orchestrator can loop cleanly. Returns aggregate counts;
 * writes its rows to raw.amz_financial_events via execMergeBatch. Does
 * NOT touch admin.JobRuns — bookkeeping stays with the orchestrator.
 */
async function pullMergeWindow(ctx, brandPool, runID, win, opts) {
  const { after, before } = win;
  const allEvents = {};

  const buildPath = (nextToken) => {
    if (nextToken) return '/finances/v0/financialEvents?NextToken=' + encodeURIComponent(nextToken);
    let qs = 'PostedAfter=' + encodeURIComponent(after) + '&MaxResultsPerPage=100';
    if (before) qs += '&PostedBefore=' + encodeURIComponent(before);
    return '/finances/v0/financialEvents?' + qs;
  };

  const { pages, hitCap, capReason } = await paginateSpApi(ctx, buildPath, (payload) => {
    const events = payload.FinancialEvents || {};
    for (const key of Object.keys(events)) {
      if (key.endsWith('EventList') && Array.isArray(events[key])) {
        if (!allEvents[key]) allEvents[key] = [];
        allEvents[key].push(...events[key]);
      }
    }
  }, {
    maxPages: opts.pageLimit, maxElapsedMs: opts.timeLimitMs,
    // SP-API Finances: 0.5 rps sustained, burst 30. 2100ms = ~0.47 rps
    // (below sustained), 3000ms = ~0.33 rps (wide safety margin for
    // backfills). Callers can tune via Params.pageDelayMs. 429 backoff
    // in callSpApi still rescues bursts past the quota either way.
    pageDelayMs: opts.pageDelayMs != null ? opts.pageDelayMs : 2100,
  });

  const rawEventCounts = {};
  for (const k of Object.keys(allEvents)) rawEventCounts[k] = allEvents[k].length;

  const rawRows = flattenAll(allEvents);
  // Per-chunk dedupe (belt-and-suspenders alongside the proc's ROW_NUMBER).
  const seen = new Map();
  for (const r of rawRows) seen.set(r.EventType + '|' + r.ExternalID, r);
  const rows = Array.from(seen.values());

  let rowsInserted = 0, rowsUpdated = 0, rowsUnchanged = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const slice = rows.slice(i, i + BATCH_SIZE);
    const r = await execMergeBatch(brandPool, ctx.brand.BrandUID, runID, slice);
    rowsInserted  += r.Inserted;
    rowsUpdated   += r.Updated;
    rowsUnchanged += r.Unchanged;
  }

  return {
    eventsProcessed: rows.length,
    rowsInserted, rowsUpdated, rowsUnchanged,
    pages,
    truncated: hitCap,
    truncatedReason: capReason,
    rawEventCounts,
  };
}

/* =========================================================================
   DB helpers
   ========================================================================= */

async function loadCredentialContext(credentialID) {
  const pool = await getPool();
  const r = await pool.request()
    .input('id', sql.Int, credentialID)
    .query(`
      SELECT bc.CredentialID, bc.BrandUID, bc.ConnectorID, bc.AccountIdentifier,
             bc.MarketplaceIDs, bc.Region, bc.RefreshToken_Enc, bc.AccessToken_Enc,
             bc.AccessTokenExpiresAt, bc.ApiKey_Enc, bc.AppSecret_Enc, bc.ExtraConfig,
             c.ConnectorUID, c.Name AS ConnectorName, c.DisplayName AS ConnectorDisplay,
             c.AuthType, c.BaseURL, c.AppClientID, c.AppClientSecret_Enc, c.ApiVersion,
             c.CredentialScope,
             b.BrandName, b.BrandSlug
      FROM admin.BrandCredentials bc
      JOIN admin.Connectors c ON c.ConnectorID = bc.ConnectorID
      JOIN admin.Brands     b ON b.BrandUID    = bc.BrandUID
      WHERE bc.CredentialID = @id
    `);
  if (!r.recordset.length) return null;
  const row = r.recordset[0];
  return {
    credentialID: row.CredentialID,
    brand: { BrandUID: row.BrandUID, BrandName: row.BrandName, BrandSlug: row.BrandSlug },
    connector: {
      ConnectorUID: row.ConnectorUID,
      Name: row.ConnectorName,
      DisplayName: row.ConnectorDisplay,
      AuthType: row.AuthType,
      BaseURL: row.BaseURL,
      AppClientID: row.AppClientID,
      AppClientSecret_Enc: row.AppClientSecret_Enc,
      ApiVersion: row.ApiVersion,
      CredentialScope: row.CredentialScope,
    },
    cred: {
      CredentialID: row.CredentialID,
      BrandUID: row.BrandUID,
      AccountIdentifier: row.AccountIdentifier,
      MarketplaceIDs: row.MarketplaceIDs,
      Region: row.Region,
      RefreshToken_Enc: row.RefreshToken_Enc,
      AccessToken_Enc: row.AccessToken_Enc,
      AccessTokenExpiresAt: row.AccessTokenExpiresAt,
      ApiKey_Enc: row.ApiKey_Enc,
      AppSecret_Enc: row.AppSecret_Enc,
      ExtraConfig: row.ExtraConfig,
    },
  };
}

async function resolveEndpointID(endpointName) {
  const pool = await getPool();
  const r = await pool.request()
    .input('n', sql.NVarChar(100), endpointName)
    .query(`
      SELECT e.EndpointID
      FROM admin.Endpoints e
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
      WHERE c.Name = 'AMAZON_SP_API' AND e.Name = @n
    `);
  if (!r.recordset.length) {
    throw new Error('Endpoint ' + endpointName + ' not registered. Run db/sql/008_seed_amazon_endpoints.sql.');
  }
  return r.recordset[0].EndpointID;
}

/**
 * Ensure an admin.Jobs row exists for (endpointID, brandUID). If not, create a
 * minimal one. This lets JobRuns FK to Jobs while still allowing ad-hoc manual
 * triggers.
 */
async function ensureJob(endpointID, brandUID) {
  const pool = await getPool();
  const r = await pool.request()
    .input('eid', sql.Int, endpointID)
    .input('buid', sql.UniqueIdentifier, brandUID)
    .query(`
      SELECT TOP 1 JobID FROM admin.Jobs
      WHERE EndpointID = @eid AND BrandUID = @buid
      ORDER BY JobID ASC
    `);
  if (r.recordset.length) return r.recordset[0].JobID;

  const ins = await pool.request()
    .input('eid', sql.Int, endpointID)
    .input('buid', sql.UniqueIdentifier, brandUID)
    .query(`
      INSERT INTO admin.Jobs (EndpointID, BrandUID, JobType, IsActive, Priority, ConcurrencyKey)
      OUTPUT INSERTED.JobID
      VALUES (@eid, @buid, 'INGEST', 1, 50, 'AMZ_FIN_EVENTS:' + CAST(@buid AS NVARCHAR(50)));
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

/**
 * Update chunk progress on an in-flight JobRun so the UI can render
 * "chunk 3/6 · 27K rows so far". Safe to call repeatedly — only writes
 * if the run is still RUNNING (avoids overwriting a terminal state if
 * something else already marked it FAILED).
 */
async function updateJobRunProgress(runID, { chunksTotal, chunksCompleted, rowsIngested }) {
  const pool = await getPool();
  await pool.request()
    .input('rid', sql.BigInt, runID)
    .input('ct',  sql.Int, chunksTotal ?? null)
    .input('cc',  sql.Int, chunksCompleted ?? null)
    .input('ri',  sql.Int, rowsIngested ?? null)
    .query(`
      UPDATE admin.JobRuns
         SET ChunksTotal     = COALESCE(@ct, ChunksTotal),
             ChunksCompleted = COALESCE(@cc, ChunksCompleted),
             RowsIngested    = COALESCE(@ri, RowsIngested)
       WHERE RunID = @rid AND Status = 'RUNNING';
    `);
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
      SET EndedAt       = SYSUTCDATETIME(),
          Status        = @s,
          RowsIngested  = @ri,
          ErrorMessage  = @em,
          ErrorFingerprint = @ef,
          WorkerType    = @wt
      WHERE RunID = @rid;
    `);
}

async function bumpLastAuthed(credentialID) {
  const pool = await getPool();
  await pool.request()
    .input('id', sql.Int, credentialID)
    .query(`
      UPDATE admin.BrandCredentials
      SET LastAuthedAt = SYSUTCDATETIME(),
          LastAuthError = NULL,
          UpdatedAt    = SYSUTCDATETIME()
      WHERE CredentialID = @id;
    `);
}

/**
 * Execute the MERGE proc against the brand data DB. Returns {Inserted, Updated, Unchanged, Total}.
 */
async function execMergeBatch(brandPool, brandUID, runID, rows) {
  if (!rows.length) return { Inserted: 0, Updated: 0, Unchanged: 0, Total: 0 };

  // JSON + OPENJSON path. We abandoned TVPs entirely — the tedious driver's
  // TVP encoder was rejecting NVARCHAR-typed decimal columns with a spurious
  // "invalid scale" even though no Decimal type was involved. A direct-proc
  // test (scripts/test-merge-proc-direct.js) reproduced the same error
  // bypassing the runner, confirming it's driver-side. Migration 017
  // replaces the proc signature with @RowsJson NVARCHAR(MAX) + OPENJSON, so
  // we serialize to JSON here, parse server-side — no TVP, no driver type
  // negotiation, just a single string parameter.
  const jsonRows = rows.map((r) => ({
    EventType:         r.EventType,
    ExternalID:        r.ExternalID,
    PostedDate:        r.PostedDate ? new Date(r.PostedDate).toISOString() : null,
    MarketplaceName:   r.MarketplaceName,
    AmazonOrderID:     r.AmazonOrderID,
    ShipmentID:        r.ShipmentID,
    AdjustmentID:      r.AdjustmentID,
    SKU:               r.SKU,
    Quantity:          r.Quantity,
    Currency:          r.Currency,
    Principal:         decimalForSql(r.Principal),
    Tax:               decimalForSql(r.Tax),
    Shipping:          decimalForSql(r.Shipping),
    PromotionDiscount: decimalForSql(r.PromotionDiscount),
    Commission:        decimalForSql(r.Commission),
    FBAFee:            decimalForSql(r.FBAFee),
    OtherFees:         decimalForSql(r.OtherFees),
    ServiceFeeType:    r.ServiceFeeType,
    _RawPayload:       r._RawPayload,
    // Hash travels as hex string; the proc does CONVERT(VARBINARY(32), ..., 2).
    _SourceRowHashHex: r._SourceRowHash
      ? (Buffer.isBuffer(r._SourceRowHash) ? r._SourceRowHash.toString('hex') : String(r._SourceRowHash))
      : null,
  }));

  const rowsJson = JSON.stringify(jsonRows);

  // Diagnostic: sample the first row so logs show exactly what's being sent.
  if (rows.length) {
    const s = rows[0];
    console.log('[runner/merge] batch size=' + rows.length + '  (JSON mode, ' + rowsJson.length + ' chars)  first row sample:', {
      EventType:  s.EventType,
      Principal:  decimalForSql(s.Principal),
      Tax:        decimalForSql(s.Tax),
      Commission: decimalForSql(s.Commission),
      FBAFee:     decimalForSql(s.FBAFee),
      OtherFees:  decimalForSql(s.OtherFees),
    });
  }

  try {
    const result = await brandPool.request()
      .input('BrandUID',    sql.UniqueIdentifier, brandUID)
      .input('SourceRunID', sql.BigInt,           runID)
      .input('RowsJson',    sql.NVarChar(sql.MAX), rowsJson)
      .execute('raw.usp_merge_amz_financial_events');

    const row = result.recordset && result.recordset[0];
    return row || { Inserted: 0, Updated: 0, Unchanged: 0, Total: rows.length };
  } catch (e) {
    // Capture everything the mssql/tedious driver gives us and surface it
    // up — the default e.message is often just a short phrase with no
    // context. The richer fields pinpoint which column / line / state.
    const detail = {
      message:         e.message,
      code:            e.code,
      number:          e.number,
      state:           e.state,
      class:           e.class,
      lineNumber:      e.lineNumber,
      procName:        e.procName,
      serverName:      e.serverName,
      infoMessage:     e.info && e.info.message,
      originalMessage: e.originalError && e.originalError.message,
      precedingErrors: Array.isArray(e.precedingErrors)
        ? e.precedingErrors.map((p) => ({ n: p.number, m: p.message, ln: p.lineNumber }))
        : undefined,
    };
    console.error('[runner/merge] SQL error detail:', JSON.stringify(detail, null, 2));
    const wrapped = new Error(e.message + ' | ' + (detail.infoMessage || detail.originalMessage || '(no detail)'));
    wrapped.sqlDetail = detail;
    throw wrapped;
  }
}

/* =========================================================================
   Event flattening
   ========================================================================= */

const valOf = (amt) => (amt && typeof amt.CurrencyAmount === 'number') ? amt.CurrencyAmount : null;
const curOf = (amt) => amt && amt.CurrencyCode ? amt.CurrencyCode : null;

function stableHash(payload) {
  const canonical = JSON.stringify(payload, Object.keys(payload).sort());
  return crypto.createHash('sha256').update(canonical).digest();
}

function stableHashID(eventType, payload) {
  const full = stableHash(payload);
  return eventType + '|' + full.toString('hex').substring(0, 32);
}

function flattenAll(allEvents) {
  const out = [];
  for (const ev of allEvents.ShipmentEventList || [])   out.push(...flattenShipmentEvent(ev, 'SHIPMENT'));
  for (const ev of allEvents.RefundEventList   || [])   out.push(...flattenShipmentEvent(ev, 'REFUND'));
  for (const ev of allEvents.ServiceFeeEventList || []) out.push(...flattenServiceFeeEvent(ev));
  for (const ev of allEvents.AdjustmentEventList || []) out.push(...flattenAdjustmentEvent(ev));
  return out;
}

/** ShipmentEvent & RefundEvent share shape — handle both. */
function flattenShipmentEvent(ev, kind) {
  const items = ev.ShipmentItemList || ev.ShipmentItemAdjustmentList || [];
  const currency = curOf(ev.ShipmentFeeList?.[0]?.FeeAmount)
                || curOf((items[0]?.ItemChargeList || items[0]?.ItemChargeAdjustmentList || [])[0]?.ChargeAmount);
  if (!items.length) return [];

  return items.map((item, idx) => {
    let principal = 0, tax = 0, shipping = 0, promo = 0;
    let commission = 0, fba = 0, other = 0;
    let cur = currency;

    const charges = item.ItemChargeList || item.ItemChargeAdjustmentList || [];
    for (const c of charges) {
      const a = valOf(c.ChargeAmount) || 0;
      cur = cur || curOf(c.ChargeAmount);
      switch (c.ChargeType) {
        case 'Principal':         principal += a; break;
        case 'Shipping':
        case 'ShippingCharge':    shipping  += a; break;
        case 'Tax':
        case 'ShippingTax':
        case 'GiftWrapTax':       tax       += a; break;
        case 'GiftWrap':          principal += a; break;
        case 'Discount':
        case 'ShippingDiscount':
        case 'PromotionAmount':   promo     += a; break;
      }
    }

    const fees = item.ItemFeeList || item.ItemFeeAdjustmentList || [];
    for (const f of fees) {
      const a = valOf(f.FeeAmount) || 0;
      cur = cur || curOf(f.FeeAmount);
      if (f.FeeType === 'Commission')         commission += a;
      else if (/^FBA/.test(f.FeeType || ''))  fba        += a;
      else                                    other      += a;
    }

    const itemId = item.OrderItemId || item.OrderAdjustmentItemId || idx.toString();
    const externalIDSeed = {
      kind, orderId: ev.AmazonOrderId, itemId,
      posted: ev.PostedDate, charges, fees,
    };
    const hash = stableHash(externalIDSeed);
    const externalID = kind + '|' + (ev.AmazonOrderId || 'no-order') + '|' + itemId + '|' + hash.toString('hex').substring(0, 12);

    return {
      EventType:         kind,
      ExternalID:        externalID.substring(0, 200),
      PostedDate:        ev.PostedDate || null,
      MarketplaceName:   ev.MarketplaceName || null,
      AmazonOrderID:     ev.AmazonOrderId || null,
      ShipmentID:        ev.ShipmentId || null,
      AdjustmentID:      null,
      SKU:               item.SellerSKU || null,
      Quantity:          item.QuantityShipped ?? null,
      Currency:          cur || null,
      Principal:         round2(principal),
      Tax:               round2(tax),
      Shipping:          round2(shipping),
      PromotionDiscount: round2(promo),
      Commission:        round2(commission),
      FBAFee:            round2(fba),
      OtherFees:         round2(other),
      ServiceFeeType:    null,
      _RawPayload:       JSON.stringify({ ev: {
        AmazonOrderId: ev.AmazonOrderId, PostedDate: ev.PostedDate, MarketplaceName: ev.MarketplaceName,
      }, item }),
      _SourceRowHash:    hash,
    };
  });
}

function flattenServiceFeeEvent(ev) {
  const rows = [];
  const fees = ev.FeeList || [];
  if (!fees.length) return rows;
  fees.forEach((f, idx) => {
    const amount = valOf(f.FeeAmount) || 0;
    const payloadSeed = {
      kind: 'SERVICE_FEE', posted: ev.PostedDate,
      sku: ev.SellerSKU, fnSku: ev.FnSKU, asin: ev.ASIN,
      feeType: f.FeeType, feeDescription: f.FeeDescription,
      amount, orderId: ev.AmazonOrderId, idx,
    };
    const hash = stableHash(payloadSeed);
    const externalID = ('SVC|' + (ev.AmazonOrderId || ev.SellerSKU || 'std') + '|'
      + (f.FeeType || 'fee') + '|' + hash.toString('hex').substring(0, 12)).substring(0, 200);

    rows.push({
      EventType:         'SERVICE_FEE',
      ExternalID:        externalID,
      PostedDate:        ev.PostedDate || null,
      MarketplaceName:   ev.MarketplaceName || null,
      AmazonOrderID:     ev.AmazonOrderId || null,
      ShipmentID:        null,
      AdjustmentID:      null,
      SKU:               ev.SellerSKU || null,
      Quantity:          null,
      Currency:          curOf(f.FeeAmount),
      Principal:         null,
      Tax:               null,
      Shipping:          null,
      PromotionDiscount: null,
      Commission:        null,
      FBAFee:            null,
      OtherFees:         round2(amount),
      ServiceFeeType:    f.FeeType || f.FeeDescription || null,
      _RawPayload:       JSON.stringify(payloadSeed),
      _SourceRowHash:    hash,
    });
  });
  return rows;
}

function flattenAdjustmentEvent(ev) {
  const amount = valOf(ev.AdjustmentAmount) || 0;
  const payloadSeed = {
    kind: 'ADJUSTMENT',
    type: ev.AdjustmentType,
    posted: ev.PostedDate,
    amount,
    items: ev.AdjustmentItemList || null,
  };
  const hash = stableHash(payloadSeed);
  const externalID = ('ADJ|' + (ev.AdjustmentType || 'gen') + '|'
    + hash.toString('hex').substring(0, 16)).substring(0, 200);

  return [{
    EventType:         'ADJUSTMENT',
    ExternalID:        externalID,
    PostedDate:        ev.PostedDate || null,
    MarketplaceName:   null,
    AmazonOrderID:     null,
    ShipmentID:        null,
    AdjustmentID:      ev.AdjustmentType || null,
    SKU:               null,
    Quantity:          null,
    Currency:          curOf(ev.AdjustmentAmount),
    Principal:         null,
    Tax:               null,
    Shipping:          null,
    PromotionDiscount: null,
    Commission:        null,
    FBAFee:            null,
    OtherFees:         round2(amount),
    ServiceFeeType:    null,
    _RawPayload:       JSON.stringify(payloadSeed),
    _SourceRowHash:    hash,
  }];
}

function round2(n) {
  if (n == null) return null;
  return Math.round((n + Number.EPSILON) * 100) / 100;
}

/**
 * Coerce a JS number into a string safe for SQL Decimal(18,4).
 *
 * Why a string (not a number): JavaScript's binary64 representation for
 * values like 0.1+0.2 produces 0.30000000000000004, and round-tripping
 * through parseFloat(toFixed(4)) silently drops back into the same
 * float-noise representation. The mssql driver's toString conversion
 * can still emit 17-decimal-place values, which fail the Decimal(18,4)
 * scale check ("invalid scale").
 *
 * Returning a string — "0.2900" — bypasses JS float entirely. SQL Server
 * parses the string directly as DECIMAL(18,4). Guaranteed to have scale
 * <= 4 because toFixed(4) always produces exactly 4 decimal places.
 *
 * mssql accepts strings for Decimal-typed TVP columns and numeric params.
 */
function decimalForSql(n) {
  if (n == null) return null;
  if (typeof n !== 'number') n = Number(n);
  if (!Number.isFinite(n)) return null;
  return n.toFixed(4);  // STRING, not parseFloat'd back to number
}

module.exports = { runAmazonFinancialEvents };
