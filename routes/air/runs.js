/**
 * AIR Bots — /api/air/runs
 *
 * SCAFFOLDING: 2026-04-30 evening. Schema awaiting Chip's review.
 *
 * Read-only history API:
 *   GET /api/air/runs                — list runs across the current tenant's agents
 *   GET /api/air/runs?agentUID=...     — filter to one agent's runs
 *   GET /api/air/runs?status=ERROR   — filter by status
 *   GET /api/air/runs/:runUID        — get one run + full append-only log tail
 *
 * Tenant-scoped via middleware. RLS in DB is the second line of defense.
 */

const express = require('express');
const router  = express.Router();
const { sql, getAirBotsPool: getPool } = require('../../config/db');
const { requireAuth }  = require('../../middleware/auth');
const { resolveTenantContext, requireTenant } = require('../../middleware/tenantContext');

router.use(requireAuth);
router.use(resolveTenantContext);
router.use(requireTenant);

/* ---------- LIST ---------- */
router.get('/', async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(500, parseInt(req.query.limit) || 100));
    const status = req.query.status ? String(req.query.status).toUpperCase() : null;
    const agentUID = req.query.agentUID || null;

    const pool = await getPool();
    const reqDb = pool.request()
      .input('t',     sql.UniqueIdentifier, req.tenantUID)
      .input('limit', sql.Int,              limit);

    let where = 'r.Tenant_UID = @t';
    if (status) {
      reqDb.input('status', sql.NVarChar(20), status);
      where += ' AND r.Status = @status';
    }
    if (agentUID) {
      reqDb.input('b', sql.UniqueIdentifier, agentUID);
      where += ' AND r.Agent_UID = @b';
    }

    const r = await reqDb.query(`
      SELECT TOP (@limit) r.Run_UID, r.Tenant_UID, r.Agent_UID,
             r.StartedAt, r.EndedAt, r.DurationMs, r.Status,
             JSON_VALUE(r.Output, '$.summary') AS Summary,
             r.Output, r.TriggeredBy,
             r.ErrorMessage, r.ErrorFingerprint,
             a.DisplayName  AS AgentName,
             rec.RecipeKey  AS RecipeName,
             rec.DisplayName AS RecipeDisplayName
      FROM air.AgentRuns r
      INNER JOIN air.Agents        a   ON a.Agent_UID    = r.Agent_UID
      INNER JOIN air.AgentRecipes  rec ON rec.Recipe_UID = a.Recipe_UID
      WHERE ${where}
      ORDER BY r.StartedAt DESC
    `);
    res.json({ runs: r.recordset });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------- GET ONE + LOG TAIL ---------- */
router.get('/:runUID', async (req, res) => {
  try {
    const pool = await getPool();

    const headerR = await pool.request()
      .input('t', sql.UniqueIdentifier, req.tenantUID)
      .input('r', sql.UniqueIdentifier, req.params.runUID)
      .query(`
        SELECT r.Run_UID, r.Tenant_UID, r.Agent_UID,
               r.StartedAt, r.EndedAt, r.DurationMs, r.Status,
               JSON_VALUE(r.Output, '$.summary') AS Summary,
               r.Output, r.TriggeredBy, r.WorkerHost, r.WorkerType,
               r.ErrorMessage, r.ErrorFingerprint,
               a.DisplayName  AS AgentName,
               rec.RecipeKey  AS RecipeName,
               rec.DisplayName AS RecipeDisplayName
        FROM air.AgentRuns r
        INNER JOIN air.Agents        a   ON a.Agent_UID      = r.Agent_UID
        INNER JOIN air.AgentRecipes  rec ON rec.Recipe_UID = a.Recipe_UID
        WHERE r.Tenant_UID = @t AND r.Run_UID = @r
      `);
    if (!headerR.recordset.length) {
      return res.status(404).json({ error: 'Run not found in current tenant' });
    }

    const logR = await pool.request()
      .input('t', sql.UniqueIdentifier, req.tenantUID)
      .input('r', sql.UniqueIdentifier, req.params.runUID)
      .query(`
        SELECT LogID, Run_UID, [Level], LogTime, Message, Detail
        FROM air.AgentRunLog
        WHERE Tenant_UID = @t AND Run_UID = @r
        ORDER BY LogTime, LogID
      `);

    res.json({
      run: headerR.recordset[0],
      log: logR.recordset,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
