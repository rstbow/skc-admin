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

const BATCH_SIZE = 1000;
const PAGE_CAP   = 60;   // 60 pages × 100 events = 6k event ceiling per run
const TIME_CAP   = 60_000;

/* =========================================================================
   Public entry point
   ========================================================================= */
async function runAmazonFinancialEvents({
  credentialID,
  daysBack,
  postedAfter,
  postedBefore,
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
    /* Resolve time window */
    let after, before;
    if (postedAfter) {
      after = new Date(postedAfter).toISOString();
    } else {
      const d = Math.max(1, Math.min(parseInt(daysBack, 10) || 1, 90));
      after = new Date(Date.now() - d * 86_400_000).toISOString();
    }
    if (postedBefore) before = new Date(postedBefore).toISOString();

    /* Pull pages */
    const allEvents = {};
    const buildPath = (nextToken) => {
      if (nextToken) return '/finances/v0/financialEvents?NextToken=' + encodeURIComponent(nextToken);
      let qs = 'PostedAfter=' + encodeURIComponent(after) + '&MaxResultsPerPage=100';
      if (before) qs += '&PostedBefore=' + encodeURIComponent(before);
      return '/finances/v0/financialEvents?' + qs;
    };

    const { pages, hitCap, capReason, elapsedMs } = await paginateSpApi(ctx, buildPath, (payload) => {
      const events = payload.FinancialEvents || {};
      for (const key of Object.keys(events)) {
        if (key.endsWith('EventList') && Array.isArray(events[key])) {
          if (!allEvents[key]) allEvents[key] = [];
          allEvents[key].push(...events[key]);
        }
      }
    }, { maxPages: PAGE_CAP, maxElapsedMs: TIME_CAP, pageDelayMs: 300 });

    /* Flatten event lists into TVP rows */
    const rows = flattenAll(allEvents);
    const brandUID = ctx.brand.BrandUID;

    /* MERGE in batches */
    const pool = await getBrandPool(brandUID);
    let totalInserted = 0, totalUpdated = 0, totalUnchanged = 0;
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const slice = rows.slice(i, i + BATCH_SIZE);
      const result = await execMergeBatch(pool, brandUID, runID, slice);
      totalInserted  += result.Inserted;
      totalUpdated   += result.Updated;
      totalUnchanged += result.Unchanged;
    }

    /* Finalize JobRun */
    await completeJobRun(runID, {
      status: 'SUCCESS',
      rowsIngested: rows.length,
      errorMessage: null,
      workerType: 'NODE',
    });
    await bumpLastAuthed(credentialID);

    return {
      runID,
      ok: true,
      brand: ctx.brand,
      window: { postedAfter: after, postedBefore: before || null },
      pages,
      truncated: hitCap,
      truncatedReason: capReason,
      elapsedMs,
      eventsProcessed: rows.length,
      rowsInserted: totalInserted,
      rowsUpdated: totalUpdated,
      rowsUnchanged: totalUnchanged,
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

  const table = new sql.Table('raw.AmzFinancialEventsTVP');
  table.columns.add('EventType',         sql.NVarChar(50),   { nullable: false });
  table.columns.add('ExternalID',        sql.NVarChar(200),  { nullable: false });
  table.columns.add('PostedDate',        sql.DateTimeOffset, { nullable: true  });
  table.columns.add('MarketplaceName',   sql.NVarChar(50),   { nullable: true  });
  table.columns.add('AmazonOrderID',     sql.NVarChar(50),   { nullable: true  });
  table.columns.add('ShipmentID',        sql.NVarChar(50),   { nullable: true  });
  table.columns.add('AdjustmentID',      sql.NVarChar(50),   { nullable: true  });
  table.columns.add('SKU',               sql.NVarChar(200),  { nullable: true  });
  table.columns.add('Quantity',          sql.Int,            { nullable: true  });
  table.columns.add('Currency',          sql.NVarChar(3),    { nullable: true  });
  table.columns.add('Principal',         sql.Decimal(18, 4), { nullable: true  });
  table.columns.add('Tax',               sql.Decimal(18, 4), { nullable: true  });
  table.columns.add('Shipping',          sql.Decimal(18, 4), { nullable: true  });
  table.columns.add('PromotionDiscount', sql.Decimal(18, 4), { nullable: true  });
  table.columns.add('Commission',        sql.Decimal(18, 4), { nullable: true  });
  table.columns.add('FBAFee',            sql.Decimal(18, 4), { nullable: true  });
  table.columns.add('OtherFees',         sql.Decimal(18, 4), { nullable: true  });
  table.columns.add('ServiceFeeType',    sql.NVarChar(100),  { nullable: true  });
  table.columns.add('_RawPayload',       sql.NVarChar(sql.MAX), { nullable: true  });
  table.columns.add('_SourceRowHash',    sql.VarBinary(32),  { nullable: false });

  for (const r of rows) {
    table.rows.add(
      r.EventType, r.ExternalID, r.PostedDate, r.MarketplaceName,
      r.AmazonOrderID, r.ShipmentID, r.AdjustmentID, r.SKU, r.Quantity,
      r.Currency,
      decimalForSql(r.Principal), decimalForSql(r.Tax),
      decimalForSql(r.Shipping), decimalForSql(r.PromotionDiscount),
      decimalForSql(r.Commission), decimalForSql(r.FBAFee),
      decimalForSql(r.OtherFees),
      r.ServiceFeeType,
      r._RawPayload, r._SourceRowHash
    );
  }

  // Diagnostic: sample the first row's decimal values so logs show what's
  // actually being sent to the driver.
  if (rows.length) {
    const s = rows[0];
    console.log('[runner/merge] batch size=' + rows.length + '  first row sample:', {
      EventType: s.EventType,
      Principal:   s.Principal   + ' (' + typeof s.Principal + ')',
      Tax:         s.Tax         + ' (' + typeof s.Tax + ')',
      Commission:  s.Commission  + ' (' + typeof s.Commission + ')',
      FBAFee:      s.FBAFee      + ' (' + typeof s.FBAFee + ')',
      OtherFees:   s.OtherFees   + ' (' + typeof s.OtherFees + ')',
      afterCoerce: {
        Principal:  decimalForSql(s.Principal)  + ' (' + typeof decimalForSql(s.Principal) + ')',
        Commission: decimalForSql(s.Commission) + ' (' + typeof decimalForSql(s.Commission) + ')',
      },
    });
  }

  try {
    const result = await brandPool.request()
      .input('BrandUID',    sql.UniqueIdentifier, brandUID)
      .input('SourceRunID', sql.BigInt,           runID)
      .input('Rows',        table)
      .execute('raw.usp_merge_amz_financial_events');

    const row = result.recordset && result.recordset[0];
    return row || { Inserted: 0, Updated: 0, Unchanged: 0, Total: rows.length };
  } catch (e) {
    // Capture everything the mssql/tedious driver gives us and surface it
    // up — the default e.message is often just "invalid scale" with no
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
