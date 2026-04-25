/**
 * Amazon FBA Returns runner.
 *
 * Pulls GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA, a flat-file Reports API
 * export of customer returns processed at FBA fulfillment centers. Returns
 * trickle in for ~30 days after the original sale, so the runner pulls a
 * rolling window of the last N days each time (default 30) and lets the
 * MERGE proc dedupe.
 *
 * Report columns (TSV, lower-kebab-case):
 *   return-date, order-id, sku, asin, fnsku, product-name, quantity,
 *   fulfillment-center-id, detailed-disposition, reason, status,
 *   license-plate-number, customer-comments
 *
 * Mapped to raw.amz_returns columns (existing schema):
 *   ReturnDate, OrderID, SKU, ASIN, FNSKU, ProductName, Quantity,
 *   FulfillmentCenter, DetailedDisposition, Reason, Status,
 *   LicensePlateNumber, CustomerComments
 *
 * Idempotent: hash-MERGE skips unchanged rows.
 */
const crypto = require('crypto');
const { sql, getPool } = require('../config/db');
const { getBrandPool } = require('./brandDb');
const { runReport, parseTsv } = require('./spApiReports');

const BATCH_SIZE = 500;

const DEFAULT_DAYS_BACK = 30;          // rolling window
const DEFAULT_POLL_TIMEOUT_MS = 30 * 60_000; // 30 min

/* =========================================================================
   Public entry point
   ========================================================================= */
async function runAmazonReturns({
  credentialID,
  triggeredBy = 'MANUAL',
  userID = null,
  params = {},
}) {
  if (!credentialID) throw new Error('credentialID is required');

  const ctx = await loadCredentialContext(credentialID);
  if (!ctx) throw new Error('Credential not found');
  if (ctx.connector.Name !== 'AMAZON_SP_API') {
    throw new Error('Runner only supports AMAZON_SP_API credentials');
  }

  const daysBack = Math.max(1, parseInt(params.daysBack, 10) || DEFAULT_DAYS_BACK);
  const marketplaceIds = String(ctx.cred.MarketplaceIDs || '')
    .split(',').map((s) => s.trim()).filter(Boolean);
  if (!marketplaceIds.length) {
    throw new Error('Credential has no MarketplaceIDs configured.');
  }

  const endpointID = await resolveEndpointID('AMZ_RETURNS');
  const jobID      = await ensureJob(endpointID, ctx.brand.BrandUID);
  const runID      = await startJobRun(jobID, triggeredBy);

  try {
    const dataEndTime   = new Date();
    const dataStartTime = new Date(dataEndTime.getTime() - daysBack * 86_400_000);

    console.log('[runner/amz-returns] start', {
      credentialID, brandUID: ctx.brand.BrandUID, brandName: ctx.brand.BrandName,
      daysBack, marketplaceIds,
      window: { start: dataStartTime.toISOString(), end: dataEndTime.toISOString() },
    });

    const reportResult = await runReport(ctx, {
      reportType: 'GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA',
      marketplaceIds,
      dataStartTime: dataStartTime.toISOString(),
      dataEndTime:   dataEndTime.toISOString(),
      pollTimeoutMs: DEFAULT_POLL_TIMEOUT_MS,
      onProgress: (p) => {
        if (p.status === 'SUBMITTED' || p.status === 'DONE' || p.attempts === 1 || p.attempts % 5 === 0) {
          console.log('[runner/amz-returns] poll', p);
        }
      },
    });

    const rawRows = parseTsv(reportResult.text);
    console.log('[runner/amz-returns] parsed ' + rawRows.length + ' rows from report ' + reportResult.reportId);

    // Update progress so the UI shows row count before MERGE finishes.
    await updateJobRunProgress(runID, {
      chunksTotal: 1, chunksCompleted: 0, rowsIngested: rawRows.length,
    });

    const rows = rawRows
      .map(toReturnRow)
      .filter((r) => r && r.OrderID && r.ReturnDate);

    const brandPool = await getBrandPool(ctx.brand.BrandUID);

    let inserted = 0, updated = 0, unchanged = 0;
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const slice = rows.slice(i, i + BATCH_SIZE);
      const r = await execMergeBatch(brandPool, ctx.brand.BrandUID, runID, slice,
        'raw.usp_merge_amz_returns');
      inserted  += r.Inserted;
      updated   += r.Updated;
      unchanged += r.Unchanged;
    }

    await updateJobRunProgress(runID, {
      chunksCompleted: 1, chunksTotal: 1, rowsIngested: rows.length,
    });

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
      windowDays: daysBack,
      reportId: reportResult.reportId,
      rowsParsed: rawRows.length,
      returnsProcessed: rows.length,
      eventsProcessed: rows.length,
      rowsInserted: inserted,
      rowsUpdated: updated,
      rowsUnchanged: unchanged,
    };
  } catch (e) {
    console.error('[runner/amz-returns]', e);
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
   Row shaping
   ========================================================================= */

function toReturnRow(r) {
  // FBA returns report uses lowercase-kebab keys.
  const orderID = (r['order-id'] || '').trim();
  const returnDate = parseDate(r['return-date']);
  if (!orderID || !returnDate) return null;

  const hashSeed = {
    OrderID: orderID, SKU: r.sku, ReturnDate: returnDate,
    ASIN: r.asin, FNSKU: r.fnsku, ProductName: r['product-name'],
    Quantity: r.quantity,
    FulfillmentCenter: r['fulfillment-center-id'],
    DetailedDisposition: r['detailed-disposition'],
    Reason: r.reason, Status: r.status,
    LicensePlateNumber: r['license-plate-number'],
    CustomerComments: r['customer-comments'],
  };
  const hash = stableHash(hashSeed);

  return {
    OrderID:             orderID,
    SKU:                 (r.sku || '').trim() || null,
    ASIN:                (r.asin || '').trim() || null,
    FNSKU:               (r.fnsku || '').trim() || null,
    ProductName:         r['product-name'] || null,
    ReturnDate:          returnDate,
    Quantity:            intOrNull(r.quantity),
    FulfillmentCenter:   (r['fulfillment-center-id'] || '').trim() || null,
    DetailedDisposition: (r['detailed-disposition'] || '').trim() || null,
    Reason:              (r.reason || '').trim() || null,
    Status:              (r.status || '').trim() || null,
    LicensePlateNumber:  (r['license-plate-number'] || '').trim() || null,
    CustomerComments:    r['customer-comments'] || null,
    _RawPayload:         JSON.stringify(r),
    _SourceRowHashHex:   hash.toString('hex'),
  };
}

/* =========================================================================
   DB helpers — mirror ordersRunner / financialEventsRunner
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
      ConnectorUID: row.ConnectorUID, Name: row.ConnectorName,
      DisplayName: row.ConnectorDisplay, AuthType: row.AuthType,
      BaseURL: row.BaseURL, AppClientID: row.AppClientID,
      AppClientSecret_Enc: row.AppClientSecret_Enc,
      ApiVersion: row.ApiVersion, CredentialScope: row.CredentialScope,
    },
    cred: {
      CredentialID: row.CredentialID, BrandUID: row.BrandUID,
      AccountIdentifier: row.AccountIdentifier,
      MarketplaceIDs: row.MarketplaceIDs, Region: row.Region,
      RefreshToken_Enc: row.RefreshToken_Enc,
      AccessToken_Enc: row.AccessToken_Enc,
      AccessTokenExpiresAt: row.AccessTokenExpiresAt,
      ApiKey_Enc: row.ApiKey_Enc, AppSecret_Enc: row.AppSecret_Enc,
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
    throw new Error('Endpoint ' + endpointName + ' not registered. Run db/sql/035_seed_orders_returns_endpoints.sql to set defaults.');
  }
  return r.recordset[0].EndpointID;
}

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
      VALUES (@eid, @buid, 'INGEST', 1, 50, 'AMZ_RETURNS:' + CAST(@buid AS NVARCHAR(50)));
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
    .input('rid', sql.BigInt, runID)
    .input('s',   sql.NVarChar(20), status)
    .input('ri',  sql.Int, rowsIngested)
    .input('em',  sql.NVarChar(sql.MAX), errorMessage)
    .input('ef',  sql.NVarChar(64), fingerprint)
    .input('wt',  sql.NVarChar(30), workerType)
    .query(`
      UPDATE admin.JobRuns
      SET EndedAt          = SYSUTCDATETIME(),
          Status           = @s,
          RowsIngested     = @ri,
          ErrorMessage     = @em,
          ErrorFingerprint = @ef,
          WorkerType       = @wt
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

async function execMergeBatch(brandPool, brandUID, runID, rows, procName) {
  if (!rows.length) return { Inserted: 0, Updated: 0, Unchanged: 0, Total: 0 };
  const rowsJson = JSON.stringify(rows);

  console.log('[runner/amz-returns/merge] proc=' + procName + ' batch size=' + rows.length +
    ' (' + rowsJson.length + ' chars)');

  try {
    const result = await brandPool.request()
      .input('BrandUID',    sql.UniqueIdentifier, brandUID)
      .input('SourceRunID', sql.BigInt,           runID)
      .input('RowsJson',    sql.NVarChar(sql.MAX), rowsJson)
      .execute(procName);
    const row = result.recordset && result.recordset[0];
    return row || { Inserted: 0, Updated: 0, Unchanged: 0, Total: rows.length };
  } catch (e) {
    const detail = {
      message: e.message, code: e.code, number: e.number, state: e.state,
      class: e.class, lineNumber: e.lineNumber, procName: e.procName,
      serverName: e.serverName,
      infoMessage: e.info && e.info.message,
      originalMessage: e.originalError && e.originalError.message,
    };
    console.error('[runner/amz-returns/merge] SQL error:', JSON.stringify(detail, null, 2));
    const wrapped = new Error(e.message + ' | ' + (detail.infoMessage || detail.originalMessage || '(no detail)'));
    wrapped.sqlDetail = detail;
    throw wrapped;
  }
}

/* =========================================================================
   Utilities
   ========================================================================= */

function stableHash(payload) {
  const canonical = JSON.stringify(payload, Object.keys(payload).sort());
  return crypto.createHash('sha256').update(canonical).digest();
}

function intOrNull(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.round(n) : null;
}

/**
 * Parse a return-date cell. The FBA returns report typically emits ISO
 * timestamps but historically some markets shipped dates without the
 * time component. Normalize to YYYY-MM-DD because raw.amz_returns.ReturnDate
 * is a DATE column (no time).
 */
function parseDate(raw) {
  if (!raw) return null;
  const s = String(raw).trim();
  if (!s) return null;
  // Strip time portion if present, then validate.
  const datePart = s.length >= 10 ? s.slice(0, 10) : s;
  const m = datePart.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) {
    // Try Date.parse fallback
    const d = new Date(s);
    if (Number.isFinite(d.getTime())) {
      return d.toISOString().slice(0, 10);
    }
    return null;
  }
  return datePart;
}

module.exports = { runAmazonReturns };
