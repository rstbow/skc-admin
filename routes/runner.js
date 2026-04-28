/**
 * Phase 3 runner endpoints.
 *
 * Two auth paths:
 *   1. JWT (admin-UI "Run Now" button) — standard requireAuth
 *   2. Service token (SSIS Agent calling on schedule) — X-Service-Token header
 *      matched against env RUNNER_SERVICE_TOKEN
 *
 * Only one of the two needs to succeed.
 */
const express = require('express');
const { sql, getPool } = require('../config/db');
const { verify } = require('../config/jwt');
const { runAmazonFinancialEvents } = require('../lib/amazonFinancialEventsRunner');
const { runBundle } = require('../lib/jobBundles');
const { syncProject } = require('../lib/projectSync');
const scheduler = require('../lib/scheduler');

const router = express.Router();

/* ---------- Auth: JWT OR service token ---------- */
function requireAuthOrServiceToken(req, res, next) {
  // Service token path — for SSIS
  const svc = process.env.RUNNER_SERVICE_TOKEN;
  const provided = req.get('X-Service-Token');
  if (svc && provided && provided === svc) {
    req.user = { userID: null, email: 'service-account', isSuperAdmin: false, isServiceToken: true };
    return next();
  }

  // JWT path — for UI
  const header = req.get('Authorization') || '';
  const match = header.match(/^Bearer\s+(.+)$/i);
  if (!match) {
    return res.status(401).json({ error: 'Missing Authorization bearer or X-Service-Token header' });
  }
  try {
    const payload = verify(match[1]);
    req.user = payload;
    next();
  } catch (e) {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}

router.use(requireAuthOrServiceToken);

/* ---------- POST /api/runner/amazon/financial-events ----------
   Body: { credentialID, daysBack?, postedAfter?, postedBefore?, triggeredBy? }
   Persists to raw.amz_financial_events via MERGE + writes admin.JobRuns.
*/
router.post('/amazon/financial-events', async (req, res) => {
  try {
    const { credentialID, daysBack, postedAfter, postedBefore, triggeredBy } = req.body || {};
    if (!credentialID) return res.status(400).json({ error: 'credentialID is required' });

    const allowedTriggers = new Set(['SCHEDULE','MANUAL','RETRY','WIZARD_TEST']);
    const trigger = allowedTriggers.has(triggeredBy) ? triggeredBy
                  : (req.user.isServiceToken ? 'SCHEDULE' : 'MANUAL');

    const result = await runAmazonFinancialEvents({
      credentialID: parseInt(credentialID, 10),
      daysBack,
      postedAfter,
      postedBefore,
      triggeredBy: trigger,
      userID: req.user.userID,
    });

    res.json(result);
  } catch (e) {
    console.error('[runner/amazon/financial-events]', e);
    res.status(500).json({
      error: e.message || 'Runner failed',
      sqlDetail: e.sqlDetail || null,
      runID: null,
    });
  }
});

/* ---------- POST /api/runner/amazon/financial-events/bulk ----------
   Runs the runner for every active Amazon credential. Used by SSIS Agent
   to fan out across all brands in a single scheduled job.

   Body: { daysBack?, postedAfter?, postedBefore? }
   Returns: { results: [ {brand, ok, runID, ...}, ... ] }
*/
router.post('/amazon/financial-events/bulk', async (req, res) => {
  try {
    const { daysBack, postedAfter, postedBefore } = req.body || {};
    const pool = await getPool();
    const credsRes = await pool.request().query(`
      SELECT bc.CredentialID, b.BrandName
      FROM admin.BrandCredentials bc
      JOIN admin.Connectors c ON c.ConnectorID = bc.ConnectorID
      JOIN admin.Brands     b ON b.BrandUID    = bc.BrandUID
      WHERE c.Name = 'AMAZON_SP_API' AND bc.IsActive = 1 AND b.IsActive = 1
      ORDER BY b.BrandName
    `);

    const results = [];
    for (const row of credsRes.recordset) {
      try {
        const result = await runAmazonFinancialEvents({
          credentialID: row.CredentialID,
          daysBack, postedAfter, postedBefore,
          triggeredBy: req.user.isServiceToken ? 'SCHEDULE' : 'MANUAL',
          userID: req.user.userID,
        });
        results.push({ ok: true, brand: row.BrandName, credentialID: row.CredentialID, ...result });
      } catch (e) {
        console.error('[runner/bulk] brand failed', row.BrandName, e.message);
        results.push({ ok: false, brand: row.BrandName, credentialID: row.CredentialID, error: e.message });
      }
    }

    const okCount = results.filter((r) => r.ok).length;
    const failCount = results.length - okCount;
    res.json({ total: results.length, succeeded: okCount, failed: failCount, results });
  } catch (e) {
    console.error('[runner/bulk]', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- GET /api/runner/runs?brandUID=&endpointName=&limit= ----------
   Lightweight run history for the UI. Same shape as /api/runs but scoped
   per-brand + per-endpoint for drill-down.
*/
router.get('/runs', async (req, res) => {
  try {
    const pool = await getPool();
    const request = pool.request();
    const filters = [];
    if (req.query.brandUID) {
      request.input('buid', sql.UniqueIdentifier, req.query.brandUID);
      filters.push('j.BrandUID = @buid');
    }
    if (req.query.endpointName) {
      request.input('en', sql.NVarChar(100), req.query.endpointName);
      filters.push('e.Name = @en');
    }
    const where = filters.length ? 'WHERE ' + filters.join(' AND ') : '';
    const limit = Math.min(parseInt(req.query.limit, 10) || 20, 200);
    request.input('lim', sql.Int, limit);
    const r = await request.query(`
      SELECT TOP (@lim)
             jr.RunID, jr.StartedAt, jr.EndedAt, jr.DurationMs, jr.Status,
             jr.RowsIngested, jr.TriggeredBy, jr.WorkerType, jr.ErrorMessage,
             e.Name AS EndpointName, c.Name AS ConnectorName, j.BrandUID
      FROM admin.JobRuns jr
      JOIN admin.Jobs j       ON j.JobID       = jr.JobID
      JOIN admin.Endpoints e  ON e.EndpointID  = j.EndpointID
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
      ${where}
      ORDER BY jr.StartedAt DESC
    `);
    res.json({ runs: r.recordset });
  } catch (e) {
    console.error('[runner/runs]', e);
    res.status(500).json({ error: e.message });
  }
});

/* ============================================================
   Brand-onboarding hook for app2.

   POST /api/runner/onboard-brand-jobs
     Headers: X-Service-Token: <RUNNER_SERVICE_TOKEN>
     Body:    { brandUID, connector, fireInitial?, includeBackfill? }
     Returns: { created[], skipped[], initialFires[] }

   Backwards-compatible wrapper around the new bundle engine. The actual
   recipe (which endpoints to provision, fire order, delays, the BACKFILL
   row) lives in admin.JobBundles + admin.JobBundleSteps — see migration
   036_job_bundles.sql.

   The connector value selects the bundle name:
     AMAZON_SP_API → 'amazon-onboarding'
     SHOPIFY       → 'shopify-onboarding'   (when added)
     WALMART       → 'walmart-onboarding'   (when added)
     QBO           → 'quickbooks-onboarding' (when added)

   Response shape preserved so app2 doesn't need to change. New
   `bundleName` field added so callers can see which recipe ran.
   ============================================================ */

const CONNECTOR_TO_BUNDLE = {
  AMAZON_SP_API: 'amazon-onboarding',
  SHOPIFY:       'shopify-onboarding',
  WALMART:       'walmart-onboarding',
  QBO:           'quickbooks-onboarding',
};

/* CONNECTOR_TO_PROJECT — Folder System v2 integration.
   After the onboarding bundle creates kickoff Jobs, the brand is auto-added
   to the matching standing Project so the recurring Jobs become project-
   managed (sync engine claims the orphan Jobs the bundle just created).
   Bundle handles the kickoff burst (FIRE_NOW / FIRE_DELAYED / PROVISION_BACKFILL);
   Project takes over for the steady-state daily cadence. */
const CONNECTOR_TO_PROJECT = {
  AMAZON_SP_API: 'Amazon Daily',
  WALMART:       'Walmart Daily',
  // SHOPIFY / QBO: add when their daily projects exist.
};

/**
 * Add a brand to a Project's ProjectBrands (idempotent), then trigger a
 * sync so any orphan Jobs from a freshly-run bundle get claimed.
 *
 * Returns a small report shape for inclusion in the onboarding response.
 * Failures are caught and surfaced as warnings — don't fail the whole
 * onboard call if the project bit doesn't apply or a project is missing.
 */
async function addBrandToConnectorProject(brandUID, connector, triggeredBy) {
  const projectName = CONNECTOR_TO_PROJECT[connector];
  if (!projectName) {
    return { added: false, reason: 'no-project-mapping-for-connector', connector };
  }

  try {
    const pool = await getPool();
    const r = await pool.request()
      .input('n', sql.NVarChar(100), projectName)
      .query('SELECT ProjectID FROM admin.Projects WHERE Name = @n AND IsActive = 1');
    if (!r.recordset.length) {
      return { added: false, reason: 'project-not-found-or-inactive', projectName };
    }
    const projectID = r.recordset[0].ProjectID;

    // Idempotent membership insert.
    const ins = await pool.request()
      .input('pid', sql.Int,              projectID)
      .input('b',   sql.UniqueIdentifier, brandUID)
      .query(`
        IF NOT EXISTS (SELECT 1 FROM admin.ProjectBrands WHERE ProjectID = @pid AND BrandUID = @b)
        BEGIN
          INSERT INTO admin.ProjectBrands (ProjectID, BrandUID, IsActive)
          VALUES (@pid, @b, 1);
          SELECT 1 AS inserted;
        END
        ELSE
        BEGIN
          SELECT 0 AS inserted;
        END
      `);
    const wasNew = ins.recordset && ins.recordset[0] && ins.recordset[0].inserted === 1;

    // Sync — claim-orphan logic in projectSync will adopt the bundle-created
    // Jobs into the project on this pass.
    const sync = await syncProject({ projectID, triggeredBy: triggeredBy || 'onboard-hook' });

    return {
      added:        wasNew,
      alreadyMember: !wasNew,
      projectName,
      projectID,
      sync: {
        added:    sync.added.length,
        claimed:  sync.claimed?.length || 0,
        updated:  sync.updated.length,
        orphaned: sync.orphaned.length,
      },
    };
  } catch (e) {
    return { added: false, reason: 'project-add-failed', error: e.message, projectName };
  }
}

router.post('/onboard-brand-jobs', async (req, res) => {
  try {
    const { brandUID, connector, fireInitial = true, includeBackfill = true } = req.body || {};
    if (!brandUID)  return res.status(400).json({ error: 'brandUID is required' });
    if (!connector) return res.status(400).json({ error: 'connector is required (e.g. AMAZON_SP_API)' });

    const bundleName = CONNECTOR_TO_BUNDLE[connector];
    if (!bundleName) {
      return res.status(400).json({
        error: 'No onboarding bundle defined for connector ' + connector +
               '. Add a row in admin.JobBundles + JobBundleSteps and map it here.',
      });
    }

    const result = await runBundle(bundleName, {
      brandUID,
      triggeredBy: req.user.isServiceToken ? 'ONBOARD' : 'MANUAL',
    });

    // Map bundle result back to the legacy onboard-brand-jobs shape so
    // app2 doesn't have to change. fireInitial=false means strip the
    // "fired" output (still provisions). includeBackfill=false means
    // strip PROVISION_BACKFILL steps' provisioned entries.
    const created = result.provisioned.filter((p) => {
      if (!includeBackfill && p.action === 'PROVISION_BACKFILL') return false;
      return true;
    }).map((p) => ({
      endpoint: p.endpoint,
      jobID: p.jobID,
      jobType: p.jobType,
      ...(p.action === 'PROVISION_BACKFILL'
        ? { note: 'paused — user fires manually for historical pull' }
        : {}),
    }));

    const initialFires = fireInitial ? result.fired : [];

    // Folder System v2: also add the brand to the matching standing
    // Project. Sync engine's claim-orphan logic will adopt the Jobs the
    // bundle just created, making them project-managed. Best-effort —
    // never fail the onboard call if the project step has issues.
    const projectMembership = await addBrandToConnectorProject(
      brandUID, connector, req.user.isServiceToken ? 'ONBOARD' : 'MANUAL'
    );

    return res.status(201).json({
      brandUID,
      brandName:    result.brandName,
      connector,
      bundleName,
      created,
      skipped:      result.skipped,
      initialFires,
      projectMembership,
      warnings:     result.warnings && result.warnings.length ? result.warnings : undefined,
      instructions: 'Poll /api/jobs/:jobID for status. Listings runner takes 5-15 min to populate raw.amz_listings; rank runner kicks off 5 min after.',
    });
  } catch (e) {
    const status = e.status && e.status >= 400 && e.status < 600 ? e.status : 500;
    if (status === 500) console.error('[runner/onboard]', e);
    res.status(status).json({ error: e.message });
  }
});

/* ============================================================
   POST /api/runner/fire-job — fire one job by name for app2.

     Headers: X-Service-Token  OR  Authorization: Bearer
     Body:    { brandUID, endpointName, params?, triggeredBy? }
     Returns: { jobID, runID, status, brand, endpoint }

   Lets app2 trigger a single endpoint pull without knowing the JobID.
   We resolve (brand, endpoint) → JobID, then call scheduler.runNow.

   If the job doesn't exist yet, returns 404 with a hint to call
   /onboard-brand-jobs first (which provisions everything via bundle).
   Optionally we could auto-provision here, but keeping it strict
   prevents typos from silently spawning jobs.
   ============================================================ */

router.post('/fire-job', async (req, res) => {
  try {
    const { brandUID, endpointName, jobType, triggeredBy } = req.body || {};
    if (!brandUID)     return res.status(400).json({ error: 'brandUID is required' });
    if (!endpointName) return res.status(400).json({ error: 'endpointName is required (e.g. AMZ_ORDERS)' });

    const wantedType = jobType || 'INGEST';

    const pool = await getPool();
    const r = await pool.request()
      .input('uid', sql.UniqueIdentifier, brandUID)
      .input('en',  sql.NVarChar(100), endpointName)
      .input('jt',  sql.NVarChar(20), wantedType)
      .query(`
        SELECT TOP 1 j.JobID, j.IsActive, j.JobType, b.BrandName, e.Name AS EndpointName
        FROM admin.Jobs j
        JOIN admin.Endpoints e ON e.EndpointID = j.EndpointID
        JOIN admin.Brands    b ON b.BrandUID   = j.BrandUID
        WHERE j.BrandUID = @uid
          AND e.Name = @en
          AND j.JobType = @jt
        ORDER BY j.JobID ASC;
      `);
    if (!r.recordset.length) {
      return res.status(404).json({
        error: 'No ' + wantedType + ' job found for brand + endpoint. Call POST /api/runner/onboard-brand-jobs to provision, or check the endpoint name.',
      });
    }
    const job = r.recordset[0];

    const trigger = triggeredBy
      || (req.user.isServiceToken ? 'SCHEDULE' : 'MANUAL');

    // scheduler.runNow returns { runID } — we resolve it sync so we can
    // give app2 a runID to poll. The actual runner runs async.
    const runResult = await scheduler.runNow(job.JobID, {
      triggeredBy: trigger,
      userID: req.user.userID || null,
    });

    return res.status(202).json({
      jobID: job.JobID,
      runID: (runResult && runResult.runID) || null,
      status: 'firing-async',
      brand: job.BrandName,
      endpoint: job.EndpointName,
      jobType: job.JobType,
      hint: 'Poll GET /api/runs?brandUID=...&endpointName=... for status.',
    });
  } catch (e) {
    console.error('[runner/fire-job]', e);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
