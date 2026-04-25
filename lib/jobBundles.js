/**
 * Job bundle engine.
 *
 * A bundle is a named recipe of admin.Endpoints + actions, persisted in
 * admin.JobBundles + admin.JobBundleSteps. Bundles can be triggered by
 * name from app2 (POST /api/bundles/{name}/run) so the admin side owns
 * the orchestration logic and app2 just says "do the onboarding thing
 * for this brand."
 *
 * Step actions:
 *   PROVISION          — INSERT admin.Jobs row from endpoint defaults; don't fire
 *   FIRE_NOW           — provision + scheduler.runNow() immediately (async)
 *   FIRE_DELAYED       — provision + setTimeout(runNow, DelayMinutes * 60000)
 *   PROVISION_BACKFILL — provision a separate JobType='BACKFILL' row, paused,
 *                        with ParamsOverride applied. User fires it manually.
 *
 * All provisioning is idempotent on (EndpointID, BrandUID, JobType) — running
 * the same bundle twice for the same brand is a no-op for already-attached jobs.
 */
const { sql, getPool } = require('../config/db');
const scheduler = require('./scheduler');

/**
 * List active bundles (for UI + GET /api/bundles).
 */
async function listBundles() {
  const pool = await getPool();
  const r = await pool.request().query(`
    SELECT b.BundleID, b.BundleUID, b.Name, b.DisplayName, b.Description,
           b.ConnectorScope, b.IsActive, b.CreatedAt, b.UpdatedAt,
           (SELECT COUNT(*) FROM admin.JobBundleSteps s WHERE s.BundleID = b.BundleID) AS StepCount
    FROM admin.JobBundles b
    WHERE b.IsActive = 1
    ORDER BY b.ConnectorScope, b.Name;
  `);
  return r.recordset;
}

/**
 * Fetch one bundle + its steps. Returns null if not found.
 */
async function getBundle(name) {
  const pool = await getPool();
  const r = await pool.request()
    .input('n', sql.NVarChar(100), name)
    .query(`
      SELECT b.BundleID, b.BundleUID, b.Name, b.DisplayName, b.Description,
             b.ConnectorScope, b.IsActive, b.CreatedAt, b.UpdatedAt
      FROM admin.JobBundles b
      WHERE b.Name = @n;
    `);
  if (!r.recordset.length) return null;
  const bundle = r.recordset[0];

  const stepsR = await pool.request()
    .input('bid', sql.Int, bundle.BundleID)
    .query(`
      SELECT s.StepID, s.StepOrder, s.EndpointID, s.Action, s.DelayMinutes,
             s.ParamsOverride, s.JobType, s.IsActiveOverride, s.Notes,
             e.Name AS EndpointName, e.DisplayName AS EndpointDisplay,
             e.DefaultJobType, e.DefaultIsActive,
             c.Name AS ConnectorName
      FROM admin.JobBundleSteps s
      JOIN admin.Endpoints  e ON e.EndpointID  = s.EndpointID
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
      WHERE s.BundleID = @bid
      ORDER BY s.StepOrder;
    `);
  bundle.steps = stepsR.recordset;
  return bundle;
}

/**
 * Run a bundle for one brand. The big workhorse.
 *
 * @param {string} bundleName
 * @param {object} opts
 *   opts.brandUID       — required
 *   opts.credentialID?  — optional; if omitted, looked up from BrandCredentials
 *                         using bundle.ConnectorScope. If multiple credentials
 *                         exist, the first IsActive=1 wins.
 *   opts.triggeredBy?   — 'MANUAL' | 'SCHEDULE' | 'ONBOARD' (default 'ONBOARD')
 *   opts.dryRun?        — if true, returns the plan without writing anything
 *
 * @returns {object}
 *   {
 *     bundleName, bundleID, brandUID, brandName, connector,
 *     provisioned: [{ endpoint, jobID, jobType, action, status }],
 *     fired:       [{ endpoint, jobID, status }],
 *     skipped:     [{ endpoint, reason, jobID? }],
 *     warnings:    [string],
 *   }
 */
async function runBundle(bundleName, opts = {}) {
  const { brandUID, dryRun = false, triggeredBy = 'ONBOARD' } = opts;
  let { credentialID } = opts;

  if (!brandUID) throw makeErr(400, 'brandUID is required');
  if (!bundleName) throw makeErr(400, 'bundleName is required');

  const pool = await getPool();

  // 1. Load the bundle + steps.
  const bundle = await getBundle(bundleName);
  if (!bundle) throw makeErr(404, 'Bundle not found: ' + bundleName);
  if (!bundle.IsActive) throw makeErr(400, 'Bundle is inactive: ' + bundleName);
  if (!bundle.steps.length) throw makeErr(400, 'Bundle has no steps: ' + bundleName);

  // 2. Verify the brand exists + active.
  const brandR = await pool.request()
    .input('uid', sql.UniqueIdentifier, brandUID)
    .query(`SELECT BrandUID, BrandName FROM admin.Brands WHERE BrandUID = @uid AND IsActive = 1`);
  if (!brandR.recordset.length) {
    throw makeErr(404, 'Brand not found or inactive — add to admin.Brands first');
  }
  const brand = brandR.recordset[0];

  // 3. If bundle has a ConnectorScope, verify (and resolve) credential.
  let connectorName = bundle.ConnectorScope;
  if (connectorName && !credentialID) {
    const credR = await pool.request()
      .input('uid', sql.UniqueIdentifier, brandUID)
      .input('cn',  sql.NVarChar(50), connectorName)
      .query(`
        SELECT TOP 1 bc.CredentialID
        FROM admin.BrandCredentials bc
        JOIN admin.Connectors c ON c.ConnectorID = bc.ConnectorID
        WHERE bc.BrandUID = @uid AND c.Name = @cn AND bc.IsActive = 1;
      `);
    if (!credR.recordset.length) {
      throw makeErr(400,
        'No active ' + connectorName + ' credential for this brand — save credentials first, then run the bundle.');
    }
    credentialID = credR.recordset[0].CredentialID;
  }

  if (dryRun) {
    return {
      bundleName, bundleID: bundle.BundleID,
      brandUID, brandName: brand.BrandName,
      connector: connectorName, credentialID,
      dryRun: true,
      plan: bundle.steps.map((s) => ({
        order: s.StepOrder, endpoint: s.EndpointName,
        action: s.Action, delayMinutes: s.DelayMinutes,
        paramsOverride: s.ParamsOverride, jobType: s.JobType,
      })),
    };
  }

  const provisioned = [];
  const fired = [];
  const skipped = [];
  const warnings = [];

  // 4. Walk steps in order.
  for (const step of bundle.steps) {
    try {
      const result = await executeStep(pool, bundle, step, brand, brandUID);
      if (result.skipped) {
        skipped.push(result);
      } else {
        provisioned.push(result);

        // Fire if action says so.
        if (step.Action === 'FIRE_NOW') {
          fireAsync(result.jobID, result.endpoint, brand.BrandName, triggeredBy, 0);
          fired.push({ endpoint: result.endpoint, jobID: result.jobID, status: 'firing-async' });
        } else if (step.Action === 'FIRE_DELAYED') {
          const delayMs = (step.DelayMinutes || 0) * 60_000;
          fireAsync(result.jobID, result.endpoint, brand.BrandName, triggeredBy, delayMs);
          fired.push({
            endpoint: result.endpoint, jobID: result.jobID,
            status: 'queued (' + (step.DelayMinutes || 0) + 'min delay)',
          });
        }
      }
    } catch (e) {
      warnings.push('Step ' + step.StepOrder + ' (' + step.EndpointName + ' / ' + step.Action + ') failed: ' + e.message);
    }
  }

  // 5. Reload scheduler so cron picks up new rows immediately.
  if (provisioned.length) {
    try { await scheduler.reload(); }
    catch (e) { warnings.push('scheduler.reload failed: ' + e.message); }
  }

  return {
    bundleName, bundleID: bundle.BundleID,
    brandUID, brandName: brand.BrandName,
    connector: connectorName, credentialID,
    provisioned, fired, skipped, warnings,
  };
}

/**
 * Execute one bundle step against the DB. Returns either:
 *   { skipped: true, endpoint, reason, jobID? }
 * or
 *   { endpoint, jobID, jobType, action, status: 'created' | 'existing' }
 */
async function executeStep(pool, bundle, step, brand, brandUID) {
  // Resolve the JobType:
  //   PROVISION_BACKFILL → 'BACKFILL' (or step.JobType override if explicit)
  //   anything else      → step.JobType ?? endpoint.DefaultJobType ?? 'INGEST'
  const jobType = step.Action === 'PROVISION_BACKFILL'
    ? (step.JobType || 'BACKFILL')
    : (step.JobType || step.DefaultJobType || 'INGEST');

  // Idempotency: same (Endpoint, Brand, JobType) → skip.
  const dup = await pool.request()
    .input('eid', sql.Int, step.EndpointID)
    .input('uid', sql.UniqueIdentifier, brandUID)
    .input('jt',  sql.NVarChar(20), jobType)
    .query(`SELECT TOP 1 JobID FROM admin.Jobs
            WHERE EndpointID = @eid AND BrandUID = @uid AND JobType = @jt`);
  if (dup.recordset.length) {
    return {
      skipped: true,
      endpoint: step.EndpointName,
      reason: 'already attached',
      jobID: dup.recordset[0].JobID,
    };
  }

  // Resolve IsActive:
  //   PROVISION_BACKFILL → 0 (paused) unless step explicitly overrides
  //   anything else      → step.IsActiveOverride ?? endpoint.DefaultIsActive
  const isActive = step.IsActiveOverride != null
    ? (step.IsActiveOverride ? 1 : 0)
    : (step.Action === 'PROVISION_BACKFILL' ? 0 : (step.DefaultIsActive ? 1 : 0));

  // Cron is null for BACKFILL jobs (no schedule); else use endpoint default.
  // We don't carry a cron-override on steps yet — could add if needed.
  const cron = step.Action === 'PROVISION_BACKFILL'
    ? null
    : null;  // INSERT below will pull endpoint default via subquery

  const ck = step.EndpointName.toLowerCase().replace(/_/g, '-')
           + (step.Action === 'PROVISION_BACKFILL' ? '-backfill' : '')
           + ':' + brandUID;

  const jobName = step.EndpointName + ' · ' + brand.BrandName
    + (step.Action === 'PROVISION_BACKFILL' ? ' · backfill' : '');

  const ins = await pool.request()
    .input('name', sql.NVarChar(100), jobName)
    .input('eid',  sql.Int, step.EndpointID)
    .input('uid',  sql.UniqueIdentifier, brandUID)
    .input('jt',   sql.NVarChar(20), jobType)
    .input('par',  sql.NVarChar(sql.MAX), step.ParamsOverride || null)
    .input('cron', sql.NVarChar(50), cron)
    .input('act',  sql.Bit, isActive)
    .input('ck',   sql.NVarChar(100), ck)
    .input('isBackfill', sql.Bit, step.Action === 'PROVISION_BACKFILL' ? 1 : 0)
    .query(`
      INSERT INTO admin.Jobs
        (Name, EndpointID, BrandUID, CronExpression, TimezoneIANA,
         ExecutionMode, JobType, Params, IsActive, ConcurrencyKey, Priority)
      OUTPUT INSERTED.JobID
      SELECT
        @name,
        @eid,
        @uid,
        CASE WHEN @isBackfill = 1 THEN NULL ELSE e.DefaultCronExpression END,
        ISNULL(e.DefaultTimezoneIANA, 'America/Chicago'),
        ISNULL(e.DefaultExecutionMode, 'NODE_NATIVE'),
        @jt,
        @par,
        @act,
        @ck,
        CASE WHEN @isBackfill = 1 THEN 30 ELSE 50 END
      FROM admin.Endpoints e
      WHERE e.EndpointID = @eid;
    `);

  return {
    endpoint: step.EndpointName,
    jobID: ins.recordset[0].JobID,
    jobType,
    action: step.Action,
    status: 'created',
  };
}

/**
 * Fire-and-forget: kick off scheduler.runNow on a separate tick so the
 * bundle runner doesn't block the HTTP response. Errors are logged but
 * not surfaced — the caller can poll JobRuns to see what happened.
 */
function fireAsync(jobID, endpoint, brandName, triggeredBy, delayMs) {
  const action = () => {
    scheduler.runNow(jobID, { triggeredBy })
      .then(() => console.log('[bundle] fired ' + endpoint + ' for ' + brandName + ' (jobID=' + jobID + ')'))
      .catch((e) => console.error('[bundle] fire ' + endpoint + ' for ' + brandName + ' failed: ' + e.message));
  };
  if (delayMs > 0) setTimeout(action, delayMs);
  else setImmediate(action);
}

function makeErr(status, message) {
  const err = new Error(message);
  err.status = status;
  return err;
}

module.exports = { listBundles, getBundle, runBundle };
