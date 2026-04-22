const express = require('express');
const { sql, getPool } = require('../config/db');
const { requireAuth } = require('../middleware/auth');

const router = express.Router();
router.use(requireAuth);

/* ---------- GET /api/runs — filterable run history ---------- */
router.get('/', async (req, res) => {
  try {
    const pool = await getPool();
    const request = pool.request();
    const filters = [];

    if (req.query.status) {
      request.input('status', sql.NVarChar(20), req.query.status);
      filters.push('jr.Status = @status');
    }
    if (req.query.jobUID) {
      request.input('jobUID', sql.UniqueIdentifier, req.query.jobUID);
      filters.push('j.JobUID = @jobUID');
    }
    if (req.query.brandUID) {
      request.input('brandUID', sql.UniqueIdentifier, req.query.brandUID);
      filters.push('j.BrandUID = @brandUID');
    }
    if (req.query.endpointUID) {
      request.input('endpointUID', sql.UniqueIdentifier, req.query.endpointUID);
      filters.push('e.EndpointUID = @endpointUID');
    }
    if (req.query.sinceHours) {
      request.input('sinceHours', sql.Int, parseInt(req.query.sinceHours, 10));
      filters.push('jr.StartedAt >= DATEADD(HOUR, -@sinceHours, SYSUTCDATETIME())');
    }

    const where = filters.length ? 'WHERE ' + filters.join(' AND ') : '';
    const limit = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    request.input('limit', sql.Int, limit);

    const r = await request.query(`
      SELECT TOP (@limit)
             jr.RunID, jr.RunUID, jr.StartedAt, jr.EndedAt, jr.DurationMs,
             jr.Status, jr.RowsIngested, jr.BytesProcessed,
             jr.WorkerHost, jr.WorkerType, jr.TriggeredBy,
             jr.ErrorMessage, jr.ErrorFingerprint,
             j.JobUID, j.BrandUID,
             e.EndpointUID, e.Name AS EndpointName, e.DisplayName AS EndpointDisplay,
             c.ConnectorUID, c.Name AS ConnectorName, c.DisplayName AS ConnectorDisplay
      FROM admin.JobRuns jr
      JOIN admin.Jobs j       ON j.JobID = jr.JobID
      JOIN admin.Endpoints e  ON e.EndpointID = j.EndpointID
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
      ${where}
      ORDER BY jr.StartedAt DESC
    `);

    res.json({ runs: r.recordset });
  } catch (e) {
    console.error('[runs/list]', e);
    res.status(500).json({ error: 'Failed to load runs' });
  }
});

/* ---------- GET /api/runs/stats — dashboard stats ---------- */
router.get('/stats', async (_req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request().query(`
      SELECT
        (SELECT COUNT(*) FROM admin.Connectors WHERE IsActive = 1) AS ActiveConnectors,
        (SELECT COUNT(*) FROM admin.Endpoints  WHERE IsActive = 1) AS ActiveEndpoints,
        (SELECT COUNT(*) FROM admin.Brands     WHERE IsActive = 1) AS ActiveBrands,
        (SELECT COUNT(*) FROM admin.Jobs       WHERE IsActive = 1) AS ActiveJobs,
        (SELECT COUNT(*) FROM admin.JobRuns
          WHERE StartedAt >= DATEADD(HOUR, -24, SYSUTCDATETIME())) AS RunsLast24h,
        (SELECT COUNT(*) FROM admin.JobRuns
          WHERE StartedAt >= DATEADD(HOUR, -24, SYSUTCDATETIME()) AND Status = 'FAILED') AS FailsLast24h
    `);
    res.json({ stats: r.recordset[0] });
  } catch (e) {
    console.error('[runs/stats]', e);
    res.status(500).json({ error: 'Failed to load stats' });
  }
});

module.exports = router;
