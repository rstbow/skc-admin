/**
 * AIR Bots — agent sync engine.
 *
 * SCAFFOLDING: 2026-04-30 evening. Schema (`air.*` tables) is awaiting
 * Chip's review of `inbox-sql/2026-04-30-05-air-schema-v01-design.md`.
 * SQL queries below are written against the PROPOSED column shape; any
 * column-name diffs after Chip's pass become small edits, not redesign.
 *
 * Conceptual mirror of `lib/projectSync.js`:
 *   projectSync materializes Jobs from (ProjectEndpoints × ProjectBrands).
 *   airAgentSync materializes Agents from (Tenants × AgentRecipes-they-subscribe-to).
 *
 * Multi-tenant invariant (the whole reason this file exists):
 *   I0. Every SQL query touching air.* tables filters by Tenant_UID.
 *       App-layer tenant guard PLUS the database-side RLS predicate
 *       (defined in the schema filing) gives belt-and-braces isolation.
 *
 * Runtime invariants (mirror projectSync.js shape):
 *   I1. For every Tenant×Recipe subscription that's IsActive on both
 *       sides, exactly one air.Agents row exists.
 *   I2. Agent.IsActive = (Tenant.IsActive AND Subscription.IsActive AND
 *       Recipe.IsActive). Agent.CronExpression / .Params reflect the
 *       merged (recipe defaults overlaid with agent-row overrides).
 *   I3. Agents whose subscription was removed are soft-deleted
 *       (IsActive=0) — air.AgentRuns FK preserves history.
 *
 * Public API:
 *   syncTenant({ tenantUID, dryRun?, triggeredBy? })       → SyncResult
 *   syncAllActiveTenants({ dryRun?, triggeredBy? })        → SyncResult[]
 *
 * SyncResult shape:
 *   {
 *     tenantUID, tenantName,
 *     added:    [ { agentUID, recipeName } ],
 *     updated:  [ { agentUID, fields: ['CronExpression', 'IsActive', ...] } ],
 *     orphaned: [ { agentUID, reason: 'subscription-removed' | 'recipe-removed' | 'tenant-removed' } ],
 *     unchanged: number,
 *     dryRun: bool,
 *     triggeredBy,
 *   }
 *
 * Called by routes/air/agents.js after every Agent/subscription mutation.
 * Single transaction per tenant for atomicity. Idempotent.
 */

const { sql, getAirBotsPool: getPool } = require('../config/db');
const recipes = require('./airAgentRecipes');

/* ---------- Internal helpers ---------- */

function paramsEqual(a, b) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return String(a) === String(b);
}

function nullableStringEqual(a, b) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return String(a) === String(b);
}

/**
 * Merge recipe defaults with per-agent overrides.
 * Agent-row fields win when non-null; otherwise recipe defaults apply.
 */
function mergeConfig(recipeRow, agentRow) {
  return {
    cronExpression: (agentRow && agentRow.CronExpression) || recipeRow.DefaultCron,
    timezoneIANA:   (agentRow && agentRow.TimezoneIANA)   || recipeRow.DefaultTimezone || 'America/Chicago',
    params:         (agentRow && agentRow.Params)         || recipeRow.DefaultParams,
    priority:       (agentRow && agentRow.Priority != null) ? agentRow.Priority : (recipeRow.DefaultPriority != null ? recipeRow.DefaultPriority : 50),
  };
}

/* ---------- Core sync ---------- */

/**
 * Reconcile materialization for one tenant.
 *
 * @param {object} opts
 * @param {string} opts.tenantUID  — UUID of the tenant to sync
 * @param {boolean} [opts.dryRun=false]
 * @param {string} [opts.triggeredBy]
 * @returns {Promise<SyncResult>}
 */
async function syncTenant({ tenantUID, dryRun = false, triggeredBy = 'sync' }) {
  if (!tenantUID || typeof tenantUID !== 'string') {
    throw new Error('syncTenant: tenantUID is required (UUID string)');
  }

  const pool = await getPool();

  // Load tenant header
  const headerR = await pool.request()
    .input('t', sql.UniqueIdentifier, tenantUID)
    .query(`
      SELECT Tenant_UID, Name, IsActive
      FROM air.Tenants
      WHERE Tenant_UID = @t
    `);
  if (!headerR.recordset.length) {
    throw new Error('syncTenant: tenant not found, Tenant_UID=' + tenantUID);
  }
  const tenant = headerR.recordset[0];

  // Load tenant's recipe subscriptions
  // SCHEMA-PROPOSED: air.TenantRecipeSubscriptions(Tenant_UID, Recipe_UID, IsActive)
  // — flagged as open question Q4 in the schema filing. If Chip prefers a
  // different shape (e.g., subscriptions implicit via Agent rows), this
  // join shifts but the algorithm doesn't.
  const subR = await pool.request()
    .input('t', sql.UniqueIdentifier, tenantUID)
    .query(`
      SELECT s.Recipe_UID, s.IsActive AS SubscriptionActive,
             r.Name, r.Version, r.IsActive AS RecipeActive,
             r.DefaultCron, r.DefaultTimezone, r.DefaultParams, r.DefaultPriority
      FROM air.TenantRecipeSubscriptions s
      INNER JOIN air.AgentRecipes r ON r.Recipe_UID = s.Recipe_UID
      WHERE s.Tenant_UID = @t
    `);
  const subscriptions = subR.recordset;

  // Load existing Agents for this tenant
  const aR = await pool.request()
    .input('t', sql.UniqueIdentifier, tenantUID)
    .query(`
      SELECT Agent_UID, Tenant_UID, Recipe_UID,
             CronExpression, TimezoneIANA, Params, Priority, IsActive,
             Name
      FROM air.Agents
      WHERE Tenant_UID = @t
    `);
  const existingAgents = aR.recordset;

  // Index existing agents by Recipe_UID
  const agentsByRecipeUID = new Map();
  for (const a of existingAgents) {
    agentsByRecipeUID.set(String(a.Recipe_UID).toLowerCase(), a);
  }

  const result = {
    tenantUID,
    tenantName: tenant.Name,
    added: [],
    updated: [],
    orphaned: [],
    unchanged: 0,
    dryRun,
    triggeredBy,
  };

  const tx = dryRun ? null : pool.transaction();
  if (tx) await tx.begin();

  try {
    /* ---------- Step 1: ensure each subscription has an Agent row ---------- */
    const desiredRecipeUIDs = new Set();
    for (const sub of subscriptions) {
      const recipeUIDLower = String(sub.Recipe_UID).toLowerCase();
      desiredRecipeUIDs.add(recipeUIDLower);

      const cfg = mergeConfig(sub, agentsByRecipeUID.get(recipeUIDLower));
      const desiredActive = !!(tenant.IsActive && sub.SubscriptionActive && sub.RecipeActive);

      const existing = agentsByRecipeUID.get(recipeUIDLower);

      if (!existing) {
        // INSERT — recipe-handler gate: refuse to materialize if Name has
        // no registered handler. This keeps DB rows in lockstep with code.
        if (!recipes.get(sub.Name)) {
          // Don't add. Surface via result so the caller can flag operator.
          result.orphaned.push({
            agentUID: null,
            reason: 'recipe-handler-not-registered',
            recipeName: sub.Name,
          });
          continue;
        }

        const agentName = '[' + tenant.Name + '] ' + sub.Name;
        if (!dryRun) {
          const ins = await tx.request()
            .input('tenantUID',  sql.UniqueIdentifier,  tenantUID)
            .input('recipeUID',  sql.UniqueIdentifier,  sub.Recipe_UID)
            .input('name',       sql.NVarChar(150),     agentName)
            .input('cron',       sql.NVarChar(50),      cfg.cronExpression)
            .input('tz',         sql.NVarChar(50),      cfg.timezoneIANA)
            .input('params',     sql.NVarChar(sql.MAX), cfg.params)
            .input('priority',   sql.Int,               cfg.priority)
            .input('isActive',   sql.Bit,               desiredActive ? 1 : 0)
            .query(`
              INSERT INTO air.Agents (
                Tenant_UID, Recipe_UID, Name,
                CronExpression, TimezoneIANA, Params, Priority, IsActive
              )
              OUTPUT INSERTED.Agent_UID
              VALUES (
                @tenantUID, @recipeUID, @name,
                @cron, @tz, @params, @priority, @isActive
              )
            `);
          result.added.push({
            agentUID:     ins.recordset[0].Agent_UID,
            recipeName: sub.Name,
          });
        } else {
          result.added.push({ agentUID: null, recipeName: sub.Name });
        }
        continue;
      }

      // UPDATE if drifted
      const drift = [];
      if (!nullableStringEqual(existing.CronExpression, cfg.cronExpression)) drift.push('CronExpression');
      if (!nullableStringEqual(existing.TimezoneIANA,   cfg.timezoneIANA))   drift.push('TimezoneIANA');
      if (!paramsEqual(existing.Params, cfg.params))                          drift.push('Params');
      if ((existing.Priority || 0) !== cfg.priority)                          drift.push('Priority');
      if (!!existing.IsActive !== desiredActive)                              drift.push('IsActive');

      if (!drift.length) {
        result.unchanged++;
        continue;
      }

      if (!dryRun) {
        await tx.request()
          .input('agentUID',   sql.UniqueIdentifier,  existing.Agent_UID)
          .input('cron',     sql.NVarChar(50),      cfg.cronExpression)
          .input('tz',       sql.NVarChar(50),      cfg.timezoneIANA)
          .input('params',   sql.NVarChar(sql.MAX), cfg.params)
          .input('priority', sql.Int,               cfg.priority)
          .input('isActive', sql.Bit,               desiredActive ? 1 : 0)
          .query(`
            UPDATE air.Agents
            SET CronExpression = @cron,
                TimezoneIANA   = @tz,
                Params         = @params,
                Priority       = @priority,
                IsActive       = @isActive,
                UpdatedAt      = SYSUTCDATETIME()
            WHERE Agent_UID = @agentUID
          `);
      }
      result.updated.push({ agentUID: existing.Agent_UID, fields: drift });
    }

    /* ---------- Step 2: orphan Agents whose subscription was removed ---------- */
    for (const a of existingAgents) {
      const recipeUIDLower = String(a.Recipe_UID).toLowerCase();
      if (desiredRecipeUIDs.has(recipeUIDLower)) continue;

      if (!dryRun) {
        await tx.request()
          .input('agentUID', sql.UniqueIdentifier, a.Agent_UID)
          .query(`
            UPDATE air.Agents
            SET IsActive  = 0,
                UpdatedAt = SYSUTCDATETIME()
            WHERE Agent_UID = @agentUID
          `);
      }
      result.orphaned.push({ agentUID: a.Agent_UID, reason: 'subscription-removed' });
    }

    if (tx) await tx.commit();
  } catch (e) {
    if (tx) {
      try { await tx.rollback(); } catch (_) { /* swallow */ }
    }
    throw e;
  }

  return result;
}

/**
 * Reconcile every active tenant. Periodic sweeper / boot.
 */
async function syncAllActiveTenants({ dryRun = false, triggeredBy = 'sync-all' } = {}) {
  const pool = await getPool();
  const r = await pool.request().query(`
    SELECT Tenant_UID FROM air.Tenants WHERE IsActive = 1 ORDER BY Name
  `);
  const results = [];
  for (const row of r.recordset) {
    try {
      results.push(await syncTenant({ tenantUID: row.Tenant_UID, dryRun, triggeredBy }));
    } catch (e) {
      results.push({ tenantUID: row.Tenant_UID, error: e.message, dryRun, triggeredBy });
    }
  }
  return results;
}

module.exports = { syncTenant, syncAllActiveTenants };
