/**
 * AIR Bots — agent runner / executor.
 *
 * The executor for a single Agent run. Picks up an Agent row, opens an
 * AgentRuns row in 'RUNNING' state, dispatches to the registered recipe
 * handler with a run context (ctx), and closes the run with the
 * handler's RecipeRunResult.
 *
 * Public API:
 *   runOnce({ agentUID, triggeredBy?, tenantUIDOverride?, params?, dryRun? }) -> RunResult
 *   runDueAgents({ tenantUID?, limit?, triggeredBy? }) -> RunResult[]
 *
 * RunResult shape (also persisted as the air.AgentRuns row):
 *   {
 *     runUID, agentUID, tenantUID, recipeName, recipeVersion (from registry),
 *     status,                 // 'RUNNING' | 'OK' | 'WARN' | 'ERROR'
 *     summary,                // one-line outcome text
 *     metadata,               // arbitrary structured object from the recipe
 *     startedAt, endedAt, durationMs,
 *     errorMessage,
 *     errorFingerprint,
 *     logCount,               // count of air.AgentRunLog rows written this run
 *   }
 *
 * Tenant context handling:
 *   Before any air.* read/write inside the run, we set
 *   sp_set_session_context @key='tenant_uid' so RLS opens for this
 *   tenant. The executor itself runs as the runner login (db_owner
 *   bypass available via the predicate added in artifact 08; we still
 *   set the context explicitly for consistency).
 *
 * Recipe handler contract:
 *   Each recipe.handler(ctx) is async and returns a RecipeRunResult:
 *     { status: 'OK'|'WARN'|'ERROR', summary: string, metadata?: object }
 *   ctx provides:
 *     - tenantUID, agentUID, runUID
 *     - params (merged: recipe.defaultParams + agent.Params overrides)
 *     - getPool() -> mssql ConnectionPool (with tenant context already set)
 *     - log(level, message, detail?) -> writes to air.AgentRunLog
 *     - emit(key, value) -> appends to run metadata for the final write
 *     - shouldAbort() -> true if the run was canceled externally (future)
 */

const { sql, getAirBotsPool: getPool } = require('../config/db');
const recipes          = require('./airAgentRecipes');

/* ---------- tenant context binding ---------- */

async function setSessionTenant(reqOrPool, tenantUID) {
  // mssql request-or-pool both expose .request()
  const r = reqOrPool.request ? reqOrPool.request() : reqOrPool;
  await r
    .input('uid', sql.UniqueIdentifier, tenantUID)
    .query("EXEC sys.sp_set_session_context @key = N'tenant_uid', @value = @uid, @read_only = 1");
}

/* ---------- run-row lifecycle ---------- */

async function openRun({ pool, tenantUID, agentUID, triggeredBy }) {
  const r = await pool.request()
    .input('t',  sql.UniqueIdentifier, tenantUID)
    .input('a',  sql.UniqueIdentifier, agentUID)
    .input('tb', sql.NVarChar(30),     triggeredBy || 'MANUAL')
    .input('wt', sql.NVarChar(30),     'NODE')
    .input('wh', sql.NVarChar(100),    require('os').hostname().slice(0, 100))
    .query(`
      INSERT INTO air.AgentRuns (
        Tenant_UID, Agent_UID, StartedAt, Status, TriggeredBy, WorkerType, WorkerHost
      )
      OUTPUT INSERTED.Run_UID, INSERTED.RunID, INSERTED.StartedAt
      VALUES (@t, @a, SYSUTCDATETIME(), 'RUNNING', @tb, @wt, @wh)
    `);
  return r.recordset[0];
}

async function closeRun({ pool, tenantUID, runUID, status, summary, metadata, errorMessage, errorFingerprint }) {
  // air.AgentRuns has no Summary column in v0.1; embed summary inside the
  // Output JSON so it's preserved + queryable via JSON_VALUE(Output, '$.summary').
  // If a future schema add introduces a Summary column, swap to that and
  // drop the embed step here.
  const outputObj = {};
  if (summary)  outputObj.summary  = summary;
  if (metadata && Object.keys(metadata).length) outputObj.metadata = metadata;
  const outputJson = Object.keys(outputObj).length ? JSON.stringify(outputObj) : null;

  await pool.request()
    .input('t',    sql.UniqueIdentifier,  tenantUID)
    .input('r',    sql.UniqueIdentifier,  runUID)
    .input('s',    sql.NVarChar(20),      status)
    .input('out',  sql.NVarChar(sql.MAX), outputJson)
    .input('em',   sql.NVarChar(sql.MAX), errorMessage || null)
    .input('ef',   sql.NVarChar(64),      errorFingerprint || null)
    .query(`
      UPDATE air.AgentRuns
      SET EndedAt          = SYSUTCDATETIME(),
          Status           = @s,
          Output           = @out,
          ErrorMessage     = @em,
          ErrorFingerprint = @ef
      WHERE Tenant_UID = @t AND Run_UID = @r
    `);
}

/* ---------- per-run log appender ---------- */

function makeLogger({ pool, tenantUID, runUID }) {
  let count = 0;
  // sequential queue so log lines land in order even under concurrency
  let chain = Promise.resolve();
  return {
    log(level, message, detail) {
      const lv = String(level || 'INFO').toUpperCase();
      if (!['DEBUG', 'INFO', 'WARN', 'ERROR'].includes(lv)) {
        // Coerce unknown levels to INFO rather than throwing
      }
      count++;
      const safeLevel = ['DEBUG', 'INFO', 'WARN', 'ERROR'].includes(lv) ? lv : 'INFO';
      chain = chain.then(() => pool.request()
        .input('t',   sql.UniqueIdentifier,  tenantUID)
        .input('r',   sql.UniqueIdentifier,  runUID)
        .input('lv',  sql.NVarChar(10),      safeLevel)
        .input('msg', sql.NVarChar(sql.MAX), String(message || '').slice(0, 8000))
        .input('d',   sql.NVarChar(sql.MAX), detail ? JSON.stringify(detail) : null)
        .query(`
          INSERT INTO air.AgentRunLog (Tenant_UID, Run_UID, LogTime, Level, Message, Detail)
          VALUES (@t, @r, SYSUTCDATETIME(), @lv, @msg, @d)
        `).catch(e => {
          // Logger failures should not crash the runner; surface to console only
          // eslint-disable-next-line no-console
          console.error('[airAgentRunner] log write failed:', e.message);
        }));
    },
    flush() { return chain; },
    count() { return count; },
  };
}

/* ---------- core: run one agent ---------- */

async function runOnce({ agentUID, triggeredBy = 'MANUAL', params: paramsOverride, dryRun = false }) {
  if (!agentUID) throw new Error('runOnce: agentUID required');

  const pool = await getPool();

  // 1. Load the agent row (no tenant-context yet — do an explicit Tenant_UID lookup
  //    so the runner can subsequently set context). Run as db_owner; bypass clause
  //    in fn_TenantPredicate covers us.
  // Column truth: agents have DisplayName (not Name); recipes have RecipeKey
  // (the slug used by the JS recipe registry) and DisplayName. AgentRecipes
  // has no Version column in v0.1 schema; recipe.version comes from the
  // in-memory registry entry instead.
  const aR = await pool.request()
    .input('a', sql.UniqueIdentifier, agentUID)
    .query(`
      SELECT a.Agent_UID, a.Tenant_UID, a.Recipe_UID,
             a.DisplayName AS AgentName,
             a.Params AS AgentParams, a.IsActive,
             r.RecipeKey AS RecipeName,
             r.DisplayName AS RecipeDisplayName,
             r.DefaultCron, r.ParamsSchema
      FROM air.Agents a
      INNER JOIN air.AgentRecipes r ON r.Recipe_UID = a.Recipe_UID
      WHERE a.Agent_UID = @a
    `);
  if (!aR.recordset.length) {
    throw new Error('runOnce: agent not found, Agent_UID=' + agentUID);
  }
  const agent = aR.recordset[0];

  if (!agent.IsActive) {
    throw new Error('runOnce: agent is inactive, Agent_UID=' + agentUID);
  }

  // 2. Resolve recipe handler from the in-memory registry
  const recipe = recipes.get(agent.RecipeName);
  if (!recipe) {
    // No handler — record an ERROR run anyway so it's visible
    const opened = await openRun({
      pool, tenantUID: agent.Tenant_UID, agentUID, triggeredBy,
    });
    await closeRun({
      pool,
      tenantUID:        agent.Tenant_UID,
      runUID:           opened.Run_UID,
      status:           'ERROR',
      summary:          'No registered handler for recipe ' + agent.RecipeName,
      metadata:         { recipeName: agent.RecipeName, registered: recipes.list().map(r => r.name) },
      errorMessage:     'recipe-handler-not-registered',
      errorFingerprint: 'recipe-handler-not-registered:' + agent.RecipeName,
    });
    return {
      runUID:        opened.Run_UID,
      agentUID,
      tenantUID:     agent.Tenant_UID,
      recipeName:    agent.RecipeName,
      recipeVersion: null, // no handler registered, no registry entry to read version from
      status:        'ERROR',
      summary:       'No registered handler for recipe ' + agent.RecipeName,
      metadata:      null,
      startedAt:     opened.StartedAt,
      endedAt:       new Date(),
      durationMs:    null,
      errorMessage:  'recipe-handler-not-registered',
      logCount:      0,
    };
  }

  // 3. Set session context to this tenant (RLS gate)
  await setSessionTenant(pool, agent.Tenant_UID);

  // 4. Open the run row
  const opened = await openRun({
    pool, tenantUID: agent.Tenant_UID, agentUID, triggeredBy,
  });

  // 5. Build merged params (recipe defaults + agent overrides + per-run override)
  const mergedParams = mergeParams(recipe.defaultParams, agent.DefaultParams, agent.AgentParams, paramsOverride);

  // 6. Build ctx + logger
  const logger = makeLogger({ pool, tenantUID: agent.Tenant_UID, runUID: opened.Run_UID });
  const emittedMetadata = {};
  const ctx = {
    tenantUID: agent.Tenant_UID,
    agentUID,
    runUID:    opened.Run_UID,
    params:    mergedParams,
    getPool:   () => pool,
    log:       (level, msg, detail) => logger.log(level, msg, detail),
    emit:      (key, value) => { emittedMetadata[key] = value; },
    shouldAbort: () => false, // future: read a cancellation table
  };

  // 7. Run the handler with timeout protection (default 5 min)
  const startTs = Date.now();
  let result;
  try {
    if (dryRun) {
      logger.log('INFO', 'dry-run mode; recipe handler skipped');
      result = { status: 'OK', summary: 'dry-run', metadata: { dryRun: true } };
    } else {
      logger.log('INFO', 'recipe handler start', {
        recipe:  agent.RecipeName,
        version: recipe.version,
        params:  mergedParams,
      });
      result = await Promise.race([
        Promise.resolve(recipe.handler(ctx)),
        new Promise((_, rej) => setTimeout(() => rej(new Error('recipe-timeout')), 5 * 60 * 1000)),
      ]);
    }
    logger.log('INFO', 'recipe handler complete', {
      status:  result && result.status,
      summary: result && result.summary,
    });
  } catch (e) {
    logger.log('ERROR', 'recipe handler threw', { message: e.message, stack: e.stack ? e.stack.slice(0, 1000) : null });
    result = {
      status:           'ERROR',
      summary:          'Handler threw: ' + e.message,
      metadata:         null,
      errorMessage:     e.message,
      errorFingerprint: fingerprint(e),
    };
  }

  // Wait for any pending log writes to land before closing the run
  await logger.flush();

  // 8. Close the run row
  const finalMetadata = Object.assign({}, result.metadata || null, emittedMetadata);
  const finalStatus   = result.status || 'OK';
  const finalSummary  = (result.summary || '').slice(0, 4000);

  await closeRun({
    pool,
    tenantUID:        agent.Tenant_UID,
    runUID:           opened.Run_UID,
    status:           normalizeStatus(finalStatus),
    summary:          finalSummary,
    metadata:         Object.keys(finalMetadata).length ? finalMetadata : null,
    errorMessage:     result.errorMessage || null,
    errorFingerprint: result.errorFingerprint || null,
  });

  // 9. Update Agents row's LastRunAt + LastRunStatus + ConsecutiveFailures
  await pool.request()
    .input('a',  sql.UniqueIdentifier, agentUID)
    .input('s',  sql.NVarChar(20),     normalizeStatus(finalStatus))
    .query(`
      UPDATE air.Agents
      SET LastRunAt           = SYSUTCDATETIME(),
          LastRunStatus       = @s,
          ConsecutiveFailures = CASE WHEN @s IN ('SUCCESS','PARTIAL') THEN 0 ELSE ConsecutiveFailures + 1 END,
          UpdatedAt           = SYSUTCDATETIME()
      WHERE Agent_UID = @a
    `);

  return {
    runUID:        opened.Run_UID,
    agentUID,
    tenantUID:     agent.Tenant_UID,
    recipeName:    agent.RecipeName,
    recipeVersion: recipe.version,
    status:        normalizeStatus(finalStatus),
    summary:       finalSummary,
    metadata:      finalMetadata,
    startedAt:     opened.StartedAt,
    endedAt:       new Date(),
    durationMs:    Date.now() - startTs,
    errorMessage:  result.errorMessage || null,
    logCount:      logger.count(),
  };
}

/* ---------- run all due agents (scheduler entry point — minimal v0.1) ---------- */

async function runDueAgents({ tenantUID, limit = 10, triggeredBy = 'SCHEDULE' } = {}) {
  const pool = await getPool();
  const reqDb = pool.request().input('limit', sql.Int, limit);
  let where = "IsActive = 1 AND NextRunAt IS NOT NULL AND NextRunAt <= SYSUTCDATETIME()";
  if (tenantUID) {
    reqDb.input('t', sql.UniqueIdentifier, tenantUID);
    where += ' AND Tenant_UID = @t';
  }
  const r = await reqDb.query(`
    SELECT TOP (@limit) Agent_UID
    FROM air.Agents
    WHERE ${where}
    ORDER BY Priority DESC, NextRunAt
  `);

  const results = [];
  for (const row of r.recordset) {
    try {
      results.push(await runOnce({ agentUID: row.Agent_UID, triggeredBy }));
    } catch (e) {
      results.push({ agentUID: row.Agent_UID, error: e.message });
    }
  }
  return results;
}

/* ---------- helpers ---------- */

function mergeParams(...layers) {
  // Each layer can be an object OR a JSON string OR null. Later layers win on
  // conflicting keys. NULL/undefined layers are ignored.
  const out = {};
  for (const layer of layers) {
    if (layer == null) continue;
    let obj = layer;
    if (typeof layer === 'string') {
      try { obj = JSON.parse(layer); } catch (_) { continue; }
    }
    if (obj && typeof obj === 'object') Object.assign(out, obj);
  }
  return out;
}

/**
 * Normalize a recipe's reported status to the air.AgentRuns CK constraint:
 * 'RUNNING' | 'SUCCESS' | 'FAILED' | 'PARTIAL' | 'CANCELED'
 */
function normalizeStatus(s) {
  const upper = String(s || '').toUpperCase();
  if (upper === 'OK')    return 'SUCCESS';
  if (upper === 'WARN')  return 'PARTIAL';
  if (upper === 'ERROR') return 'FAILED';
  if (['SUCCESS', 'FAILED', 'PARTIAL', 'RUNNING', 'CANCELED'].includes(upper)) return upper;
  return 'PARTIAL'; // unknown -> mark as PARTIAL so it's investigated
}

function fingerprint(e) {
  // Stable short hash of the error message + first stack line for grouping.
  const head = String(e && e.message || 'unknown').slice(0, 120);
  const line = String(e && e.stack || '').split('\n').find(l => l.trim().startsWith('at')) || '';
  const combo = head + '|' + line.trim().slice(0, 200);
  let h = 0;
  for (let i = 0; i < combo.length; i++) h = ((h << 5) - h + combo.charCodeAt(i)) | 0;
  return Math.abs(h).toString(16).slice(0, 16);
}

module.exports = { runOnce, runDueAgents };
