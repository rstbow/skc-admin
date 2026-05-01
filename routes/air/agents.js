/**
 * AIR Bots — /api/air/agents
 *
 * SCAFFOLDING: 2026-04-30 evening. Schema awaiting Chip's review.
 *
 * Routes:
 *   GET    /api/air/agents                — list agents for current tenant
 *   GET    /api/air/agents/:agentUID        — get one agent + recipe metadata
 *   POST   /api/air/agents                — create agent (subscribe tenant to recipe)
 *   PATCH  /api/air/agents/:agentUID        — update agent overrides
 *   DELETE /api/air/agents/:agentUID        — soft-delete (subscription removal)
 *   POST   /api/air/agents/:agentUID/run    — fire recipe handler immediately (manual trigger)
 *
 * All routes are tenant-scoped via middleware/tenantContext.js.
 * The Tenant_UID is taken from req.tenantUID, NEVER from request body —
 * preventing tenant-spoof at the app layer (RLS in DB is the second line).
 */

const express = require('express');
const router  = express.Router();
const { sql, getPool } = require('../../config/db');
const { requireAuth }  = require('../../middleware/auth');
const { resolveTenantContext, requireTenant } = require('../../middleware/tenantContext');
const { syncTenant } = require('../../lib/airAgentSync');
const recipes = require('../../lib/airAgentRecipes');

router.use(requireAuth);
router.use(resolveTenantContext);
router.use(requireTenant);

/* ---------- LIST ---------- */
router.get('/', async (req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request()
      .input('t', sql.UniqueIdentifier, req.tenantUID)
      .query(`
        SELECT a.Agent_UID, a.Tenant_UID, a.Recipe_UID, a.Name,
               a.CronExpression, a.TimezoneIANA, a.Params, a.Priority,
               a.IsActive, a.CreatedAt, a.UpdatedAt,
               r.Name AS RecipeName, r.Version AS RecipeVersion,
               r.Description AS RecipeDescription
        FROM air.Agents a
        INNER JOIN air.AgentRecipes r ON r.Recipe_UID = a.Recipe_UID
        WHERE a.Tenant_UID = @t
        ORDER BY a.Name
      `);
    res.json({ agents: r.recordset });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------- GET ONE ---------- */
router.get('/:agentUID', async (req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request()
      .input('t', sql.UniqueIdentifier, req.tenantUID)
      .input('b', sql.UniqueIdentifier, req.params.agentUID)
      .query(`
        SELECT a.Agent_UID, a.Tenant_UID, a.Recipe_UID, a.Name,
               a.CronExpression, a.TimezoneIANA, a.Params, a.Priority,
               a.IsActive, a.CreatedAt, a.UpdatedAt,
               r.Name AS RecipeName, r.Version AS RecipeVersion,
               r.Description AS RecipeDescription,
               r.DefaultCron, r.DefaultTimezone, r.DefaultParams, r.DefaultPriority
        FROM air.Agents a
        INNER JOIN air.AgentRecipes r ON r.Recipe_UID = a.Recipe_UID
        WHERE a.Tenant_UID = @t AND a.Agent_UID = @b
      `);
    if (!r.recordset.length) {
      return res.status(404).json({ error: 'Agent not found in current tenant' });
    }
    const agent = r.recordset[0];
    agent.handlerRegistered = !!recipes.get(agent.RecipeName);
    res.json({ agent });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------- CREATE ---------- */
router.post('/', async (req, res) => {
  try {
    const { recipeName, name, cronExpression, timezoneIANA, params, priority, isActive } = req.body || {};
    if (!recipeName) {
      return res.status(400).json({ error: 'recipeName is required' });
    }
    if (!recipes.get(recipeName)) {
      return res.status(400).json({ error: 'recipeName has no registered handler', recipeName });
    }

    const pool = await getPool();

    // Resolve Recipe_UID from Name
    const rR = await pool.request()
      .input('n', sql.NVarChar(150), recipeName)
      .query('SELECT Recipe_UID, Name FROM air.AgentRecipes WHERE Name = @n AND IsActive = 1');
    if (!rR.recordset.length) {
      return res.status(400).json({ error: 'Recipe not found in air.AgentRecipes', recipeName });
    }
    const recipeUID = rR.recordset[0].Recipe_UID;

    // Insert subscription + agent in one transaction; sync afterwards
    const tx = pool.transaction();
    await tx.begin();
    let agentUID;
    try {
      // Subscription row
      await tx.request()
        .input('t', sql.UniqueIdentifier, req.tenantUID)
        .input('r', sql.UniqueIdentifier, recipeUID)
        .query(`
          MERGE air.TenantRecipeSubscriptions AS tgt
          USING (SELECT @t AS Tenant_UID, @r AS Recipe_UID) AS src
            ON tgt.Tenant_UID = src.Tenant_UID AND tgt.Recipe_UID = src.Recipe_UID
          WHEN MATCHED THEN UPDATE SET IsActive = 1, UpdatedAt = SYSUTCDATETIME()
          WHEN NOT MATCHED THEN
            INSERT (Tenant_UID, Recipe_UID, IsActive)
            VALUES (@t, @r, 1);
        `);

      // Agent row (sync would create it too, but creating here lets us return the AgentUID immediately)
      const ins = await tx.request()
        .input('t',         sql.UniqueIdentifier,  req.tenantUID)
        .input('r',         sql.UniqueIdentifier,  recipeUID)
        .input('name',      sql.NVarChar(150),     name || ('[' + req.tenantUID.slice(0,8) + '] ' + recipeName))
        .input('cron',      sql.NVarChar(50),      cronExpression || null)
        .input('tz',        sql.NVarChar(50),      timezoneIANA || null)
        .input('params',    sql.NVarChar(sql.MAX), params ? JSON.stringify(params) : null)
        .input('priority',  sql.Int,               priority != null ? priority : null)
        .input('isActive',  sql.Bit,               isActive === false ? 0 : 1)
        .query(`
          INSERT INTO air.Agents (
            Tenant_UID, Recipe_UID, Name,
            CronExpression, TimezoneIANA, Params, Priority, IsActive
          )
          OUTPUT INSERTED.Agent_UID
          VALUES (@t, @r, @name, @cron, @tz, @params, @priority, @isActive)
        `);
      agentUID = ins.recordset[0].Agent_UID;
      await tx.commit();
    } catch (txErr) {
      try { await tx.rollback(); } catch (_) {}
      throw txErr;
    }

    // Reconcile (catches drift from defaults if any)
    const syncResult = await syncTenant({
      tenantUID: req.tenantUID,
      triggeredBy: 'POST /api/air/agents',
    });

    res.status(201).json({ agentUID, syncResult });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------- PATCH ---------- */
router.patch('/:agentUID', async (req, res) => {
  try {
    const { name, cronExpression, timezoneIANA, params, priority, isActive } = req.body || {};
    const pool = await getPool();

    // Confirm agent belongs to current tenant
    const ownR = await pool.request()
      .input('t', sql.UniqueIdentifier, req.tenantUID)
      .input('b', sql.UniqueIdentifier, req.params.agentUID)
      .query('SELECT 1 FROM air.Agents WHERE Tenant_UID = @t AND Agent_UID = @b');
    if (!ownR.recordset.length) {
      return res.status(404).json({ error: 'Agent not found in current tenant' });
    }

    const updates = [];
    const reqDb = pool.request().input('b', sql.UniqueIdentifier, req.params.agentUID);
    if (name !== undefined)            { updates.push('Name = @name');                       reqDb.input('name',     sql.NVarChar(150),     name); }
    if (cronExpression !== undefined)  { updates.push('CronExpression = @cron');              reqDb.input('cron',     sql.NVarChar(50),      cronExpression); }
    if (timezoneIANA !== undefined)    { updates.push('TimezoneIANA = @tz');                  reqDb.input('tz',       sql.NVarChar(50),      timezoneIANA); }
    if (params !== undefined)          { updates.push('Params = @params');                    reqDb.input('params',   sql.NVarChar(sql.MAX), params == null ? null : JSON.stringify(params)); }
    if (priority !== undefined)        { updates.push('Priority = @priority');                reqDb.input('priority', sql.Int,               priority); }
    if (isActive !== undefined)        { updates.push('IsActive = @isActive');                reqDb.input('isActive', sql.Bit,               isActive ? 1 : 0); }
    if (!updates.length) return res.json({ updated: false });
    updates.push('UpdatedAt = SYSUTCDATETIME()');

    await reqDb.query(`UPDATE air.Agents SET ${updates.join(', ')} WHERE Agent_UID = @b`);
    const syncResult = await syncTenant({
      tenantUID: req.tenantUID,
      triggeredBy: 'PATCH /api/air/agents/:agentUID',
    });
    res.json({ updated: true, syncResult });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------- DELETE (soft) ---------- */
router.delete('/:agentUID', async (req, res) => {
  try {
    const pool = await getPool();

    // Confirm ownership and resolve Recipe_UID for subscription teardown
    const ownR = await pool.request()
      .input('t', sql.UniqueIdentifier, req.tenantUID)
      .input('b', sql.UniqueIdentifier, req.params.agentUID)
      .query('SELECT Recipe_UID FROM air.Agents WHERE Tenant_UID = @t AND Agent_UID = @b');
    if (!ownR.recordset.length) {
      return res.status(404).json({ error: 'Agent not found in current tenant' });
    }
    const recipeUID = ownR.recordset[0].Recipe_UID;

    const tx = pool.transaction();
    await tx.begin();
    try {
      // Mark subscription inactive — sync will then orphan the agent row
      await tx.request()
        .input('t', sql.UniqueIdentifier, req.tenantUID)
        .input('r', sql.UniqueIdentifier, recipeUID)
        .query(`
          UPDATE air.TenantRecipeSubscriptions
          SET IsActive = 0, UpdatedAt = SYSUTCDATETIME()
          WHERE Tenant_UID = @t AND Recipe_UID = @r
        `);
      await tx.commit();
    } catch (txErr) {
      try { await tx.rollback(); } catch (_) {}
      throw txErr;
    }

    const syncResult = await syncTenant({
      tenantUID: req.tenantUID,
      triggeredBy: 'DELETE /api/air/agents/:agentUID',
    });
    res.json({ deleted: true, syncResult });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------- MANUAL RUN ---------- */
router.post('/:agentUID/run', async (_req, res) => {
  // The fire-now trigger lives in lib/airAgentRunner.js (forthcoming).
  // Stub returns 501 so the API surface is wired but the executor is
  // intentionally not yet attached.
  res.status(501).json({
    error: 'Not implemented',
    detail: 'airAgentRunner.runOnce() is the next slice — coming once Chip\'s schema is finalized.',
  });
});

module.exports = router;
