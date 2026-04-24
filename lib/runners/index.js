/**
 * Runner registry. Maps admin.Endpoints.Name → async runner fn.
 *
 * Every NODE_NATIVE / SSIS_CALLS_NODE job looks up its endpoint name
 * here to find the code that actually does the work. Adding a new
 * connector endpoint = add one row in admin.Endpoints + one line here.
 *
 * Runner contract:
 *   runner({ credentialID, brandUID, jobID, runID, triggeredBy, params, userID })
 *     → Promise<{
 *         runID,            // same runID passed in, for convenience
 *         status,           // 'SUCCESS' | 'PARTIAL' | 'FAILED'
 *         rowsIngested,     // int or null
 *         message?,         // optional human-readable summary
 *       }>
 *
 * Runners MAY continue to write their own admin.JobRuns row (as
 * amazonFinancialEventsRunner does today) — the scheduler will detect
 * this and skip writing a duplicate. This keeps the migration path
 * gentle: existing runners don't need to change.
 */
const { runAmazonFinancialEvents } = require('../amazonFinancialEventsRunner');
const { runAmazonListings }        = require('../amazonListingsRunner');

/* ---------- Adapter for amazonFinancialEventsRunner ----------
   Its native signature is { credentialID, daysBack?, postedAfter?, postedBefore? }.
   Scheduler calls with { credentialID, params, ... } — params carries the
   scheduling-specific fields. We unwrap here so the runner itself stays
   decoupled from the scheduler's ambient context.
*/
async function amzFinancialEventsAdapter(ctx) {
  const p = ctx.params || {};
  const result = await runAmazonFinancialEvents({
    credentialID: ctx.credentialID,
    daysBack:     p.daysBack,
    postedAfter:  p.postedAfter,
    postedBefore: p.postedBefore,
    chunkDays:    p.chunkDays,      // Params can override (default 30; backfill recs 15)
    pageLimit:    p.pageLimit,      // Params can override (default 1000)
    timeLimitMs:  p.timeLimitMs,    // Params can override (default 600000)
    pageDelayMs:  p.pageDelayMs,    // Params can override (default 2100; backfill recs 3000)
    triggeredBy:  ctx.triggeredBy || 'SCHEDULE',
    userID:       ctx.userID || null,
  });
  return {
    runID:        result.runID || ctx.runID,
    status:       result.ok === false ? 'FAILED' : 'SUCCESS',
    rowsIngested: result.eventsProcessed ?? result.rowsIngested ?? null,
    message:      result.truncated
      ? ('Truncated: ' + result.truncatedReason)
      : null,
    // Signal: this runner wrote its own JobRuns row. Scheduler should skip
    // writing a duplicate.
    wroteOwnJobRun: true,
  };
}

/* ---------- Registry ---------- */
/* Adapter for the Listings Ledger runner — passes Params through so
   marketplaceId can be overridden per-job. */
async function amzListingsAdapter(ctx) {
  const result = await runAmazonListings({
    credentialID: ctx.credentialID,
    triggeredBy:  ctx.triggeredBy || 'SCHEDULE',
    userID:       ctx.userID || null,
    params:       ctx.params || {},
  });
  return {
    runID:          result.runID || ctx.runID,
    status:         result.ok === false ? 'FAILED' : 'SUCCESS',
    rowsIngested:   result.eventsProcessed ?? null,
    message:        result.changesDetected
      ? (result.changesDetected + ' changes detected (' +
         result.addedListings + ' added, ' + result.removedListings + ' removed)')
      : null,
    wroteOwnJobRun: true,
  };
}

const registry = {
  AMZ_FINANCIAL_EVENTS: amzFinancialEventsAdapter,
  AMZ_LISTINGS_READ:    amzListingsAdapter,
  // New endpoints go here, one per line:
  //   AMZ_ORDERS:              require('../amazonOrdersRunner').run,
  //   SHOPIFY_ORDERS:          require('../shopifyOrdersRunner').run,
  //   QBO_TRANSACTION_LIST:    require('../quickbooksRunner').runTransactionList,
};

function getRunner(endpointName) {
  return registry[endpointName] || null;
}

function listRegistered() {
  return Object.keys(registry).sort();
}

module.exports = { getRunner, listRegistered };
