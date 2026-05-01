/**
 * AIR Bots recipe: amazon.source-window-adaptive
 *
 * v0.1 (OBSERVATIONAL). The full-form recipe will:
 *   1. Read rolling-7d-avg destination row count per active brand
 *   2. Compare to last-N-day actual count
 *   3. Detect gap (anomaly threshold breach)
 *   4. Recommend (or, post-Chip-bless, perform) elevation:
 *        Report1DataStart 2 -> 5 (or higher)
 *   5. Persist decision + audit trail to admin.SourceWindowConfig +
 *      admin.SourceWindowAuditLog
 *
 * v0.1 ships only step 0 + 4-recommendation, NO writes outside air.*:
 *   - Reads tbl_PPA_SP_API_Report_Runs (staging-side source-of-truth for
 *     current Report1DataStart per brand)
 *   - Reports current state per brand
 *   - Flags any brand at non-default (potential elevated state from a
 *     prior manual intervention)
 *   - Emits aggregate metadata
 *   - Logs per-brand observations to air.AgentRunLog
 *
 * Once Chip blesses inbox-sql/2026-04-29-03-adaptive-source-window-design.md
 * (still OPEN), the recipe gains the SourceWindowConfig read + the
 * gap-detection + the elevation-recommendation logic. v0.2 adds the
 * auto-write capability gated by air.AgentRules / capability grants.
 *
 * Refs:
 * - inbox-admin/2026-04-30-02-amazon-sales-pool-rebuild-via-admin-app.md (Randy P1)
 * - inbox-sql/2026-04-29-03-adaptive-source-window-design.md (Chip schema review)
 * - strategy/2026-04-30-architecture-and-go-to-market.md
 */

const { register } = require('../airAgentRecipes');
const { sql, getStagingPool } = require('../../config/db');

const RECIPE_NAME    = 'amazon.source-window-adaptive';
const RECIPE_VERSION = '0.1.0';
const DEFAULT_DAYS_BACK = 2;

/**
 * Handler. ctx is provided by airAgentRunner.
 */
async function handler(ctx) {
  const { params, log, emit } = ctx;
  const defaultDaysBack = (params && Number.isInteger(params.defaultDaysBack)) ? params.defaultDaysBack : DEFAULT_DAYS_BACK;

  log('INFO', 'amazon.source-window-adaptive starting', { defaultDaysBack, params });

  // 1. Pull current per-brand source-window state from the source-runs
  //    table. This is the table the SP-API SSIS package reads to
  //    determine its window when pulling reports. The table lives in
  //    `vs-ims-project` (NOT vs-ims-staging) per Chip's adaptive-source-
  //    window design filing (inbox-sql/2026-04-29-03). We connect via
  //    the staging pool and use a cross-DB 4-part name; works as long as
  //    the staging login (skc_app_user) has SELECT on vs-ims-project.
  let stagingPool;
  try {
    stagingPool = await getStagingPool();
  } catch (e) {
    // Distinguish "env vars not set" (no AT-symbol in message) from
    // "auth failed" / "connection refused" / etc. so the run row's
    // summary tells us which.
    const isUnconfigured = /not configured/i.test(e.message);
    log('ERROR', isUnconfigured ? 'staging pool not configured' : 'staging pool connect failed', { error: e.message });
    return {
      status:           'ERROR',
      summary:          isUnconfigured
                          ? 'Staging DB pool not configured (set STAGING_DB_USER/STAGING_DB_PASSWORD env vars)'
                          : 'Staging DB pool connect failed: ' + e.message,
      metadata:         { defaultDaysBack, brandsObserved: 0 },
      errorMessage:     e.message,
      errorFingerprint: isUnconfigured ? 'staging-pool-unconfigured' : 'staging-pool-connect-failed',
    };
  }

  let stateRows;
  try {
    const r = await stagingPool.request().query(`
      SELECT TOP 200
             brand_id,
             Report1Active,
             Report1DataStart
        FROM [vs-ims-project].dbo.tbl_PPA_SP_API_Report_Runs
       WHERE Report1Active = 1
       ORDER BY brand_id
    `);
    stateRows = r.recordset;
  } catch (e) {
    log('ERROR', 'failed to read tbl_PPA_SP_API_Report_Runs', { error: e.message });
    return {
      status:           'ERROR',
      summary:          'Failed to read source-runs state: ' + e.message,
      metadata:         { defaultDaysBack },
      errorMessage:     e.message,
      errorFingerprint: 'source-runs-query-failed',
    };
  }

  if (!stateRows.length) {
    log('WARN', 'no active brands found in tbl_PPA_SP_API_Report_Runs');
    return {
      status:   'WARN',
      summary:  'No active brands found in tbl_PPA_SP_API_Report_Runs',
      metadata: { defaultDaysBack, brandsObserved: 0 },
    };
  }

  // 2. Bucket each brand's current state vs the configured default
  let atDefault   = 0;
  let aboveDefault = 0;
  let belowDefault = 0;
  const elevated  = [];
  const tightened = [];

  for (const row of stateRows) {
    const dsb = row.Report1DataStart;
    if (dsb === defaultDaysBack) {
      atDefault++;
    } else if (dsb > defaultDaysBack) {
      aboveDefault++;
      elevated.push({ brandID: row.brand_id, currentDaysBack: dsb });
    } else {
      belowDefault++;
      tightened.push({ brandID: row.brand_id, currentDaysBack: dsb });
    }
  }

  // 3. Per-brand log lines for any non-default state (operator visibility)
  for (const e of elevated) {
    log('INFO', 'brand at elevated source window', e);
  }
  for (const t of tightened) {
    log('WARN', 'brand at sub-default source window (likely manual override)', t);
  }

  // 4. Aggregate metadata for the run row
  const metadata = {
    recipeVersion:        RECIPE_VERSION,
    defaultDaysBack,
    brandsObserved:       stateRows.length,
    atDefault,
    aboveDefault,
    belowDefault,
    sampleElevated:       elevated.slice(0, 10),
    sampleTightened:      tightened.slice(0, 10),
    nextActions:          [
      'v0.1 is observational only — no writes to source-runs.',
      'v0.2 (after Chip blesses inbox-sql/2026-04-29-03) adds gap detection + write capability.',
      'Today: review elevated brands; if any are stuck elevated past their hold window, flag for manual revert.',
    ],
  };
  emit('observation_summary', {
    atDefault, aboveDefault, belowDefault, defaultDaysBack,
  });

  log('INFO', 'amazon.source-window-adaptive complete', metadata);

  // Status decision: OK in steady state (no anomalies); WARN if any brands
  // are tightened below default (suggests an operator hand-tuned).
  const status = belowDefault > 0 ? 'WARN' : 'OK';
  const summary = `${stateRows.length} active brands observed: ${atDefault} at default(${defaultDaysBack}d), ${aboveDefault} elevated, ${belowDefault} tightened.`;

  return { status, summary, metadata };
}

/* ---------- registration ---------- */

register({
  name:        RECIPE_NAME,
  version:     RECIPE_VERSION,
  description: 'Observe per-brand Report1DataStart settings on the SP-API source-runs table; flag drift from default. v0.1 is read-only; v0.2 adds gap-detection + auto-elevation per Chip\'s SourceWindowConfig design.',
  defaultCron: '0 */4 * * *',     // every 4 hours
  defaultParams: { defaultDaysBack: DEFAULT_DAYS_BACK },
  paramsShape: {
    type: 'object',
    properties: {
      defaultDaysBack: {
        type:        'integer',
        minimum:     1,
        maximum:     30,
        default:     DEFAULT_DAYS_BACK,
        description: 'The "normal" Report1DataStart value to compare against. Brands at this value are at-default; higher = elevated, lower = tightened.',
      },
    },
  },
  handler,
});

module.exports = { handler };
