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

   Called by app2 right after a brand saves credentials for a connector.
   Iterates admin.Endpoints WHERE AutoCreateOnNewBrand=1 + matching
   connector, INSERTs admin.Jobs rows from each endpoint's defaults
   (idempotent — no double-create), reloads the scheduler, and
   optionally fires the initial loads so customers see data within
   ~10 minutes of connecting.

   Special-case for AMZ_FINANCIAL_EVENTS: also creates a paused
   BACKFILL row (daysBack=180) so the user can manually fire it later
   for historical data.

   Initial fires (async, fire-and-forget — don't block the HTTP response):
     - AMZ_LISTINGS_READ           → fires NOW (~5-15 min report wait)
     - AMZ_FINANCIAL_EVENTS INGEST → fires NOW (last 2 days, fast)
     - AMZ_LISTING_RANK_SNAPSHOT   → fires 5 min later (needs ASINs)

   App2 polls /api/jobs/:jobID every 30s to track progress.
   ============================================================ */

router.post('/onboard-brand-jobs', async (req, res) => {
  try {
    const { brandUID, connector, fireInitial = true, includeBackfill = true } = req.body || {};
    if (!brandUID) return res.status(400).json({ error: 'brandUID is required' });
    if (!connector) return res.status(400).json({ error: 'connector is required (e.g. AMAZON_SP_API)' });

    const pool = await getPool();

    // 1. Verify brand exists + active
    const brandR = await pool.request()
      .input('uid', sql.UniqueIdentifier, brandUID)
      .query(`SELECT BrandUID, BrandName FROM admin.Brands WHERE BrandUID = @uid AND IsActive = 1`);
    if (!brandR.recordset.length) {
      return res.status(404).json({ error: 'Brand not found or inactive — add the brand to admin.Brands first' });
    }
    const brand = brandR.recordset[0];

    // 2. Verify there's an active credential for this brand+connector
    const credR = await pool.request()
      .input('uid', sql.UniqueIdentifier, brandUID)
      .input('cn',  sql.NVarChar(50), connector)
      .query(`
        SELECT TOP 1 bc.CredentialID
        FROM admin.BrandCredentials bc
        JOIN admin.Connectors c ON c.ConnectorID = bc.ConnectorID
        WHERE bc.BrandUID = @uid AND c.Name = @cn AND bc.IsActive = 1;
      `);
    if (!credR.recordset.length) {
      return res.status(400).json({
        error: 'No active credential for this brand + connector — save credentials first, then call this hook.',
      });
    }

    // 3. Find all auto-create endpoints for this connector
    const epR = await pool.request()
      .input('cn', sql.NVarChar(50), connector)
      .query(`
        SELECT e.EndpointID, e.Name,
               e.DefaultCronExpression, e.DefaultTimezoneIANA, e.DefaultParams,
               e.DefaultExecutionMode, e.DefaultJobType, e.DefaultIsActive
        FROM admin.Endpoints e
        JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
        WHERE c.Name = @cn
          AND e.AutoCreateOnNewBrand = 1
          AND e.IsActive = 1
        ORDER BY e.Name;
      `);
    if (!epR.recordset.length) {
      return res.status(200).json({
        brandUID, brandName: brand.BrandName, connector,
        created: [], skipped: [], initialFires: [],
        warning: 'No endpoints have AutoCreateOnNewBrand=1 for this connector. Set the flag in admin.Endpoints to enable auto-onboarding.',
      });
    }

    const created = [];
    const skipped = [];

    for (const ep of epR.recordset) {
      // Idempotency check — same (endpoint, brand, jobType) → skip
      const dup = await pool.request()
        .input('eid', sql.Int, ep.EndpointID)
        .input('uid', sql.UniqueIdentifier, brandUID)
        .input('jt',  sql.NVarChar(20), ep.DefaultJobType || 'INGEST')
        .query(`SELECT TOP 1 JobID FROM admin.Jobs
                WHERE EndpointID = @eid AND BrandUID = @uid AND JobType = @jt`);
      if (dup.recordset.length) {
        skipped.push({ endpoint: ep.Name, reason: 'already attached', jobID: dup.recordset[0].JobID });
        continue;
      }

      const ck = ep.Name.toLowerCase().replace(/_/g, '-') + ':' + brandUID;
      const ins = await pool.request()
        .input('name', sql.NVarChar(100), ep.Name + ' · ' + brand.BrandName)
        .input('eid',  sql.Int, ep.EndpointID)
        .input('uid',  sql.UniqueIdentifier, brandUID)
        .input('cron', sql.NVarChar(50), ep.DefaultCronExpression || null)
        .input('tz',   sql.NVarChar(50), ep.DefaultTimezoneIANA || 'America/Chicago')
        .input('mode', sql.NVarChar(20), ep.DefaultExecutionMode || 'NODE_NATIVE')
        .input('jt',   sql.NVarChar(20), ep.DefaultJobType || 'INGEST')
        .input('act',  sql.Bit, ep.DefaultIsActive ? 1 : 0)
        .input('ck',   sql.NVarChar(100), ck)
        .query(`
          INSERT INTO admin.Jobs (Name, EndpointID, BrandUID, CronExpression, TimezoneIANA,
                                  ExecutionMode, JobType, Params, IsActive, ConcurrencyKey, Priority)
          OUTPUT INSERTED.JobID
          VALUES (@name, @eid, @uid, @cron, @tz, @mode, @jt, NULL, @act, @ck, 50);
        `);
      created.push({
        endpoint:    ep.Name,
        jobID:       ins.recordset[0].JobID,
        jobType:     ep.DefaultJobType || 'INGEST',
      });
    }

    // 4. Backfill row for AMZ_FINANCIAL_EVENTS — paused, daysBack=180
    if (includeBackfill) {
      const finEp = epR.recordset.find((e) => e.Name === 'AMZ_FINANCIAL_EVENTS');
      if (finEp) {
        const bfDup = await pool.request()
          .input('eid', sql.Int, finEp.EndpointID)
          .input('uid', sql.UniqueIdentifier, brandUID)
          .query(`SELECT TOP 1 JobID FROM admin.Jobs
                  WHERE EndpointID = @eid AND BrandUID = @uid AND JobType = 'BACKFILL'`);
        if (!bfDup.recordset.length) {
          const bfIns = await pool.request()
            .input('name', sql.NVarChar(100), 'AMZ_FINANCIAL_EVENTS · ' + brand.BrandName + ' · backfill 180d')
            .input('eid',  sql.Int, finEp.EndpointID)
            .input('uid',  sql.UniqueIdentifier, brandUID)
            .input('par',  sql.NVarChar(sql.MAX), N(JSON.stringify({ daysBack: 180, chunkDays: 2, pageDelayMs: 3000 })))
            .input('ck',   sql.NVarChar(100), 'amz-fin-events-backfill:' + brandUID)
            .query(`
              INSERT INTO admin.Jobs (Name, EndpointID, BrandUID, CronExpression, TimezoneIANA,
                                      ExecutionMode, JobType, Params, IsActive, ConcurrencyKey, Priority)
              OUTPUT INSERTED.JobID
              VALUES (@name, @eid, @uid, NULL, 'America/Chicago', 'NODE_NATIVE', 'BACKFILL', @par, 0, @ck, 30);
            `);
          created.push({
            endpoint: 'AMZ_FINANCIAL_EVENTS', jobID: bfIns.recordset[0].JobID, jobType: 'BACKFILL',
            note: 'paused — user fires manually for 180-day historical pull',
          });
        } else {
          skipped.push({ endpoint: 'AMZ_FINANCIAL_EVENTS', reason: 'backfill already attached', jobID: bfDup.recordset[0].JobID });
        }
      }
    }

    // 5. Reload scheduler so cron picks up new rows immediately
    await scheduler.reload().catch((e) => console.error('[onboard] scheduler.reload failed', e.message));

    // 6. Initial fires (async, fire-and-forget — don't block HTTP response)
    const initialFires = [];
    if (fireInitial) {
      const listings = created.find((c) => c.endpoint === 'AMZ_LISTINGS_READ');
      if (listings) {
        scheduler.runNow(listings.jobID, { triggeredBy: 'MANUAL' })
          .then(() => console.log('[onboard] initial AMZ_LISTINGS_READ fired for ' + brand.BrandName))
          .catch((e) => console.error('[onboard] initial listings failed:', e.message));
        initialFires.push({ endpoint: 'AMZ_LISTINGS_READ', jobID: listings.jobID, status: 'firing-async' });
      }

      const fin = created.find((c) => c.endpoint === 'AMZ_FINANCIAL_EVENTS' && c.jobType === 'INGEST');
      if (fin) {
        scheduler.runNow(fin.jobID, { triggeredBy: 'MANUAL' })
          .then(() => console.log('[onboard] initial AMZ_FINANCIAL_EVENTS fired for ' + brand.BrandName))
          .catch((e) => console.error('[onboard] initial fin events failed:', e.message));
        initialFires.push({ endpoint: 'AMZ_FINANCIAL_EVENTS', jobID: fin.jobID, status: 'firing-async' });
      }

      const rank = created.find((c) => c.endpoint === 'AMZ_LISTING_RANK_SNAPSHOT');
      if (rank) {
        // Delay 5 min so the listings runner has time to populate raw.amz_listings
        // — the rank runner reads ASINs from there.
        setTimeout(() => {
          scheduler.runNow(rank.jobID, { triggeredBy: 'MANUAL' })
            .then(() => console.log('[onboard] delayed AMZ_LISTING_RANK_SNAPSHOT fired for ' + brand.BrandName))
            .catch((e) => console.error('[onboard] delayed rank fire failed:', e.message));
        }, 5 * 60 * 1000);
        initialFires.push({ endpoint: 'AMZ_LISTING_RANK_SNAPSHOT', jobID: rank.jobID, status: 'queued (5min delay)' });
      }
    }

    res.status(201).json({
      brandUID,
      brandName:    brand.BrandName,
      connector,
      created,
      skipped,
      initialFires,
      instructions: 'Poll /api/jobs/:jobID for status. Listings runner takes 5-15 min to populate raw.amz_listings; rank runner kicks off 5 min after.',
    });
  } catch (e) {
    console.error('[runner/onboard]', e);
    res.status(500).json({ error: e.message });
  }
});

// Helper to ensure NVARCHAR-prefix on JSON params
function N(s) { return s; }

module.exports = router;
