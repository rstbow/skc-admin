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
    res.status(500).json({ error: e.message || 'Runner failed', runID: null });
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

module.exports = router;
