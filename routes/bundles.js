/**
 * Job Bundles REST API.
 *
 * Bundles are named recipes of (endpoint, action, delay) — admin owns
 * the orchestration logic, app2 calls a bundle by name to provision +
 * fire jobs for a brand.
 *
 * Auth: same dual-mode pattern as routes/runner.js — JWT (admin UI) OR
 * X-Service-Token (app2 / SSIS Agent).
 *
 * Endpoints:
 *   GET    /api/bundles                  → list all active bundles
 *   GET    /api/bundles/:name            → bundle + steps
 *   POST   /api/bundles/:name/run        → execute for a brand
 *       body: { brandUID, credentialID?, dryRun?, triggeredBy? }
 *
 * Idempotent on re-run — provisioning skips already-attached jobs.
 */
const express = require('express');
const { verify } = require('../config/jwt');
const { listBundles, getBundle, runBundle } = require('../lib/jobBundles');

const router = express.Router();

/* ---------- Auth: JWT OR service token ---------- */
function requireAuthOrServiceToken(req, res, next) {
  const svc = process.env.RUNNER_SERVICE_TOKEN;
  const provided = req.get('X-Service-Token');
  if (svc && provided && provided === svc) {
    req.user = { userID: null, email: 'service-account', isSuperAdmin: false, isServiceToken: true };
    return next();
  }
  const header = req.get('Authorization') || '';
  const match = header.match(/^Bearer\s+(.+)$/i);
  if (!match) {
    return res.status(401).json({ error: 'Missing Authorization bearer or X-Service-Token header' });
  }
  try {
    req.user = verify(match[1]);
    next();
  } catch (_) {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}

router.use(requireAuthOrServiceToken);

/* ---------- GET /api/bundles ---------- */
router.get('/', async (_req, res) => {
  try {
    const bundles = await listBundles();
    res.json({ bundles });
  } catch (e) {
    console.error('[bundles/list]', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- GET /api/bundles/:name ---------- */
router.get('/:name', async (req, res) => {
  try {
    const bundle = await getBundle(req.params.name);
    if (!bundle) return res.status(404).json({ error: 'Bundle not found: ' + req.params.name });
    res.json(bundle);
  } catch (e) {
    console.error('[bundles/get]', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- POST /api/bundles/:name/run ---------- */
router.post('/:name/run', async (req, res) => {
  try {
    const { brandUID, credentialID, dryRun, triggeredBy } = req.body || {};
    if (!brandUID) return res.status(400).json({ error: 'brandUID is required' });

    const result = await runBundle(req.params.name, {
      brandUID,
      credentialID: credentialID ? parseInt(credentialID, 10) : undefined,
      dryRun: !!dryRun,
      triggeredBy: triggeredBy
        || (req.user.isServiceToken ? 'ONBOARD' : 'MANUAL'),
    });

    // 200 for dry-run (no state change), 201 for actual provision.
    res.status(dryRun ? 200 : 201).json(result);
  } catch (e) {
    const status = e.status && e.status >= 400 && e.status < 600 ? e.status : 500;
    if (status === 500) console.error('[bundles/run]', e);
    res.status(status).json({ error: e.message });
  }
});

module.exports = router;
