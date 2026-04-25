/**
 * SP-API Reports helper (Reports API v2021-06-30).
 *
 * Reports are Amazon's async bulk-export mechanism. Lifecycle:
 *   1. POST /reports/2021-06-30/reports                   → reportId
 *   2. GET  /reports/2021-06-30/reports/{reportId}        → poll until DONE
 *   3. GET  /reports/2021-06-30/documents/{documentId}    → download URL
 *   4. Fetch URL (may be gzip'd) → raw TSV / CSV / XML
 *
 * This wrapper hides the poll + decompress dance. Callers hand in a
 * reportType + marketplaceIds and get the parsed content back.
 *
 * Rate limits worth knowing:
 *   - createReport:      0.0167 rps (~1/min) — don't spam
 *   - getReport:         2 rps
 *   - getReportDocument: 0.0167 rps (~1/min)
 *
 * Used by: amazonListingsRunner (GET_MERCHANT_LISTINGS_ALL_DATA).
 * Will be used by: Shopify bulk ops migrations, QBO transaction list,
 * any endpoint that exports large tabular data async.
 */
const zlib = require('zlib');
const { promisify } = require('util');
const gunzip = promisify(zlib.gunzip);
const { callSpApi } = require('./amazonApi');

const REPORTS_BASE = '/reports/2021-06-30';
const DEFAULT_POLL_INTERVAL_MS = 30_000;   // 30s between polls
const DEFAULT_POLL_TIMEOUT_MS  = 45 * 60_000; // 45 min max per report
const DEFAULT_POLL_INITIAL_WAIT_MS = 15_000;  // small initial wait before first poll

/**
 * Submit → poll → download → return { text, reportId, reportDocumentId }.
 * `text` is the raw report body (TSV / CSV / XML — Amazon's choice per
 * reportType). Decompressed already if gzip'd.
 *
 * @param {object} ctx credential context for callSpApi
 * @param {object} options
 *   options.reportType       — e.g. 'GET_MERCHANT_LISTINGS_ALL_DATA'
 *   options.marketplaceIds   — array of strings, e.g. ['ATVPDKIKX0DER']
 *   options.dataStartTime    — optional ISO string
 *   options.dataEndTime      — optional ISO string
 *   options.pollIntervalMs   — override polling cadence
 *   options.pollTimeoutMs    — override total time budget
 *   options.onProgress       — optional callback({ status, attempts, elapsedMs })
 */
async function runReport(ctx, {
  reportType,
  marketplaceIds,
  dataStartTime,
  dataEndTime,
  pollIntervalMs = DEFAULT_POLL_INTERVAL_MS,
  pollTimeoutMs = DEFAULT_POLL_TIMEOUT_MS,
  onProgress,
}) {
  if (!reportType) throw new Error('reportType is required');
  if (!Array.isArray(marketplaceIds) || !marketplaceIds.length) {
    throw new Error('marketplaceIds must be a non-empty array');
  }

  /* 1. Submit */
  const submitBody = { reportType, marketplaceIds };
  if (dataStartTime) submitBody.dataStartTime = dataStartTime;
  if (dataEndTime)   submitBody.dataEndTime   = dataEndTime;
  const submitRes = await callSpApi(ctx, REPORTS_BASE + '/reports', {
    method: 'POST', body: submitBody,
  });
  const reportId = submitRes.reportId;
  if (!reportId) throw new Error('createReport returned no reportId: ' + JSON.stringify(submitRes));
  if (onProgress) onProgress({ status: 'SUBMITTED', reportId, attempts: 0, elapsedMs: 0 });

  /* 2. Poll */
  const start = Date.now();
  let attempts = 0;
  await sleep(DEFAULT_POLL_INITIAL_WAIT_MS);
  let reportDocumentId = null;
  while (true) {
    attempts++;
    const poll = await callSpApi(ctx, REPORTS_BASE + '/reports/' + encodeURIComponent(reportId));
    const status = poll.processingStatus;
    const elapsedMs = Date.now() - start;
    if (onProgress) onProgress({ status, reportId, attempts, elapsedMs });

    if (status === 'DONE') {
      reportDocumentId = poll.reportDocumentId;
      break;
    }
    if (status === 'CANCELLED') {
      throw new Error('Report was cancelled (reportId=' + reportId + ')');
    }
    if (status === 'FATAL') {
      const errDocId = poll.reportDocumentId;
      let detail = '';
      if (errDocId) {
        try {
          const errBody = await downloadReportDocument(ctx, errDocId);
          detail = ' — ' + errBody.text.slice(0, 300);
        } catch (_) { /* ignore */ }
      }
      throw new Error('Report failed with FATAL status (reportId=' + reportId + ')' + detail);
    }
    // IN_QUEUE / IN_PROGRESS — keep polling
    if (elapsedMs > pollTimeoutMs) {
      throw new Error('Report poll timeout after ' + Math.round(elapsedMs / 1000) +
        's waiting for reportId=' + reportId);
    }
    await sleep(pollIntervalMs);
  }

  /* 3. Download the signed-URL document */
  const doc = await downloadReportDocument(ctx, reportDocumentId);
  return { text: doc.text, reportId, reportDocumentId };
}

async function downloadReportDocument(ctx, reportDocumentId) {
  const meta = await callSpApi(ctx, REPORTS_BASE + '/documents/' + encodeURIComponent(reportDocumentId));
  if (!meta.url) throw new Error('getReportDocument returned no url');

  // Fetch the signed S3 URL directly — no SP-API auth on this call.
  const res = await fetch(meta.url);
  if (!res.ok) throw new Error('Report document fetch failed: HTTP ' + res.status);
  const buf = Buffer.from(await res.arrayBuffer());

  // GZIP is the only compression Amazon uses today. Older reports used
  // AES-256-CBC envelope encryption; the v2021-06-30 API dropped that.
  let bodyBuf = buf;
  if ((meta.compressionAlgorithm || '').toUpperCase() === 'GZIP') {
    bodyBuf = await gunzip(buf);
  }

  // Encoding handling: Amazon's docs say UTF-8 but in practice the older
  // flat-file reports (GET_MERCHANT_LISTINGS_ALL_DATA, settlement reports)
  // ship in windows-1252 — typical symptom is the curly apostrophe ' (U+2019)
  // arriving as the lone byte 0x92, which is invalid UTF-8 and gets replaced
  // with U+FFFD (the replacement-char ◇). Try UTF-8 strict first; if any
  // byte is invalid, decode as windows-1252 instead.
  let text;
  try {
    text = new TextDecoder('utf-8', { fatal: true }).decode(bodyBuf);
  } catch (_) {
    // Strict UTF-8 failed → fall back to windows-1252 (preserves curly
    // apostrophe, em-dash, smart-quotes, € etc.)
    text = new TextDecoder('windows-1252').decode(bodyBuf);
  }
  return { text, compressionAlgorithm: meta.compressionAlgorithm || null, url: meta.url };
}

/**
 * Parse a tab-separated report body into an array of objects keyed by the
 * first-row headers. Amazon's reports rarely contain quoted tabs, so no
 * CSV-style quoting logic. Handles BOM + CRLF.
 */
function parseTsv(text) {
  // Strip UTF-8 BOM if present
  if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);
  const lines = text.split(/\r?\n/).filter((l) => l.length > 0);
  if (!lines.length) return [];
  const headers = lines[0].split('\t');
  return lines.slice(1).map((line) => {
    const cells = line.split('\t');
    const obj = {};
    headers.forEach((h, i) => { obj[h] = cells[i] !== undefined ? cells[i] : null; });
    return obj;
  });
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

module.exports = { runReport, downloadReportDocument, parseTsv };
