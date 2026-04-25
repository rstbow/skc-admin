/**
 * Jobs API — the UI for the Node-native scheduler.
 *
 *   GET    /api/jobs                         list every job, with computed next-run
 *   GET    /api/jobs/:jobID                  detail (job + recent runs + matched runbook)
 *   GET    /api/jobs/:jobID/runs?limit=20    run history for one job
 *   POST   /api/jobs                         create a new scheduled job
 *   PATCH  /api/jobs/:jobID                  update cron, name, IsActive, ExecutionMode
 *   POST   /api/jobs/:jobID/run              "Run now" — returns { runID } immediately
 *   POST   /api/jobs/:jobID/pause            flip IsActive=0, reset ConsecutiveFailures
 *   POST   /api/jobs/:jobID/resume           flip IsActive=1, reset ConsecutiveFailures
 *   GET    /api/jobs/scheduler/status        worker host + loaded job count
 *   GET    /api/jobs/runbooks                list all runbooks
 */
const express = require('express');
const { sql, getPool } = require('../config/db');
const { requireAuth } = require('../middleware/auth');
const scheduler = require('../lib/scheduler');
const { listRegistered } = require('../lib/runners');

const router = express.Router();
router.use(requireAuth);

/* ---------- LIST ---------- */
router.get('/', async (_req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request().query(`
      SELECT j.JobID, j.JobUID, j.Name, j.CronExpression, j.TimezoneIANA,
             j.IsActive, j.ExecutionMode, j.Priority,
             j.LastRunAt, j.LastRunStatus,
             j.LastErrorMessage, j.LastErrorFingerprint,
             j.ConsecutiveFailures, j.NextRunAt, j.JobType,
             j.BrandUID, b.BrandName,
             e.EndpointID, e.Name AS EndpointName, e.DisplayName AS EndpointDisplay,
             c.ConnectorID, c.Name AS ConnectorName, c.DisplayName AS ConnectorDisplay,
             (SELECT TOP 1 jr2.DurationMs
                FROM admin.JobRuns jr2
               WHERE jr2.JobID = j.JobID AND jr2.Status = 'SUCCESS'
               ORDER BY jr2.StartedAt DESC) AS LastSuccessDurationMs,
             (SELECT COUNT(*) FROM admin.JobRuns jr3
               WHERE jr3.JobID = j.JobID AND jr3.Status = 'RUNNING'
                 AND jr3.StartedAt > DATEADD(HOUR, -1, SYSUTCDATETIME())) AS InFlight,
             -- surface in-flight chunk progress for the UI
             (SELECT TOP 1 jr4.ChunksTotal     FROM admin.JobRuns jr4
               WHERE jr4.JobID = j.JobID AND jr4.Status = 'RUNNING'
               ORDER BY jr4.StartedAt DESC) AS InFlightChunksTotal,
             (SELECT TOP 1 jr4.ChunksCompleted FROM admin.JobRuns jr4
               WHERE jr4.JobID = j.JobID AND jr4.Status = 'RUNNING'
               ORDER BY jr4.StartedAt DESC) AS InFlightChunksCompleted,
             (SELECT TOP 1 jr4.RowsIngested    FROM admin.JobRuns jr4
               WHERE jr4.JobID = j.JobID AND jr4.Status = 'RUNNING'
               ORDER BY jr4.StartedAt DESC) AS InFlightRowsSoFar
      FROM admin.Jobs j
      JOIN admin.Endpoints  e ON e.EndpointID  = j.EndpointID
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
      LEFT JOIN admin.Brands b ON b.BrandUID    = j.BrandUID
      ORDER BY j.IsActive DESC, j.Name ASC
    `);
    res.json({ jobs: r.recordset });
  } catch (e) {
    console.error('[jobs/list]', e);
    res.status(500).json({ error: e.message || 'Failed to load jobs' });
  }
});

/* ---------- DETAIL ---------- */
router.get('/:jobID(\\d+)', async (req, res) => {
  try {
    const jobID = parseInt(req.params.jobID, 10);
    const pool = await getPool();

    const job = await pool.request()
      .input('jid', sql.Int, jobID)
      .query(`
        SELECT j.*,
               b.BrandName,
               e.Name AS EndpointName, e.DisplayName AS EndpointDisplay,
               c.Name AS ConnectorName, c.DisplayName AS ConnectorDisplay
        FROM admin.Jobs j
        JOIN admin.Endpoints  e ON e.EndpointID  = j.EndpointID
        JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
        LEFT JOIN admin.Brands b ON b.BrandUID    = j.BrandUID
        WHERE j.JobID = @jid
      `);
    if (!job.recordset.length) return res.status(404).json({ error: 'Not found' });
    const row = job.recordset[0];

    const runs = await pool.request()
      .input('jid', sql.Int, jobID)
      .query(`
        SELECT TOP 20 RunID, StartedAt, EndedAt, DurationMs, Status,
               RowsIngested, ChunksTotal, ChunksCompleted,
               TriggeredBy, WorkerHost, ErrorMessage, ErrorFingerprint
        FROM admin.JobRuns
        WHERE JobID = @jid
        ORDER BY StartedAt DESC
      `);

    let runbook = null;
    if (row.LastErrorMessage) {
      const rb = await pool.request()
        .input('em', sql.NVarChar(sql.MAX), row.LastErrorMessage)
        .query(`
          SELECT TOP 1 RunbookID, Title, WhatItMeans, HowToFix, Severity, MatchPattern
          FROM admin.ErrorRunbooks
          WHERE IsActive = 1 AND @em LIKE MatchPattern
          ORDER BY LEN(MatchPattern) DESC
        `);
      runbook = rb.recordset[0] || null;
    }

    res.json({ job: row, runs: runs.recordset, runbook });
  } catch (e) {
    console.error('[jobs/detail]', e);
    res.status(500).json({ error: e.message || 'Failed to load job' });
  }
});

/* ---------- RUN HISTORY for one job ---------- */
router.get('/:jobID(\\d+)/runs', async (req, res) => {
  try {
    const jobID = parseInt(req.params.jobID, 10);
    const limit = Math.min(parseInt(req.query.limit, 10) || 50, 500);
    const pool = await getPool();
    const r = await pool.request()
      .input('jid', sql.Int, jobID)
      .input('lim', sql.Int, limit)
      .query(`
        SELECT TOP (@lim) RunID, StartedAt, EndedAt, DurationMs, Status,
               RowsIngested, TriggeredBy, WorkerHost, ErrorMessage, ErrorFingerprint
        FROM admin.JobRuns
        WHERE JobID = @jid
        ORDER BY StartedAt DESC
      `);
    res.json({ runs: r.recordset });
  } catch (e) {
    console.error('[jobs/runs]', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- CREATE ---------- */
router.post('/', async (req, res) => {
  try {
    const { name, endpointID, brandUID, cronExpression, timezoneIANA,
            executionMode, priority, jobType, concurrencyKey } = req.body || {};
    if (!endpointID || !brandUID) {
      return res.status(400).json({ error: 'endpointID and brandUID are required' });
    }
    const pool = await getPool();
    const r = await pool.request()
      .input('name', sql.NVarChar(100), name || null)
      .input('eid',  sql.Int, parseInt(endpointID, 10))
      .input('buid', sql.UniqueIdentifier, brandUID)
      .input('cron', sql.NVarChar(50), cronExpression || null)
      .input('tz',   sql.NVarChar(50), timezoneIANA || 'America/Chicago')
      .input('em',   sql.NVarChar(20), executionMode || 'NODE_NATIVE')
      .input('prio', sql.Int, priority || 50)
      .input('jt',   sql.NVarChar(20), jobType || 'INGEST')
      .input('ck',   sql.NVarChar(100), concurrencyKey || null)
      .input('uid',  sql.Int, req.user.userID)
      .query(`
        INSERT INTO admin.Jobs
          (Name, EndpointID, BrandUID, CronExpression, TimezoneIANA,
           ExecutionMode, Priority, JobType, ConcurrencyKey, CreatedBy, UpdatedBy)
        OUTPUT INSERTED.JobID
        VALUES (@name, @eid, @buid, @cron, @tz, @em, @prio, @jt, @ck, @uid, @uid)
      `);
    const jobID = r.recordset[0].JobID;
    await scheduler.reload(jobID).catch((e) =>
      console.error('[jobs/create] reload failed:', e.message));
    res.status(201).json({ jobID });
  } catch (e) {
    console.error('[jobs/create]', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- UPDATE ---------- */
router.patch('/:jobID(\\d+)', async (req, res) => {
  try {
    const jobID = parseInt(req.params.jobID, 10);
    const fields = {};
    const allowed = ['Name', 'CronExpression', 'TimezoneIANA', 'IsActive',
                     'ExecutionMode', 'Priority', 'ConcurrencyKey'];
    for (const key of allowed) {
      const camel = key.charAt(0).toLowerCase() + key.slice(1);
      if (req.body[camel] !== undefined) fields[key] = req.body[camel];
    }
    if (!Object.keys(fields).length) {
      return res.status(400).json({ error: 'No updatable fields provided' });
    }

    const pool = await getPool();
    const request = pool.request().input('jid', sql.Int, jobID);
    const sets = [];
    let i = 0;
    for (const [col, val] of Object.entries(fields)) {
      const p = 'p' + (i++);
      // type inference — good enough for these columns
      if (col === 'IsActive')             request.input(p, sql.Bit, val ? 1 : 0);
      else if (col === 'Priority')        request.input(p, sql.Int, parseInt(val, 10));
      else                                request.input(p, sql.NVarChar(200), val);
      sets.push(`${col} = @${p}`);
    }
    sets.push(`UpdatedAt = SYSUTCDATETIME()`);

    await request.query(`UPDATE admin.Jobs SET ${sets.join(', ')} WHERE JobID = @jid`);
    await scheduler.reload(jobID).catch((e) =>
      console.error('[jobs/update] reload failed:', e.message));
    res.json({ jobID, updated: Object.keys(fields) });
  } catch (e) {
    console.error('[jobs/update]', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- RUN NOW ---------- */
router.post('/:jobID(\\d+)/run', async (req, res) => {
  try {
    const jobID = parseInt(req.params.jobID, 10);
    // Fire and return — the UI polls the job detail endpoint to watch progress.
    const fireP = scheduler.runNow(jobID, {
      triggeredBy: 'MANUAL',
      userID: req.user.userID,
    });

    // We want a runID back before responding, so await the pre-insert but
    // not the full run. The scheduler returns after the run completes
    // today, so for now we DO await — acceptable for our current runners
    // (a few seconds to a minute). Swap to returning just runID + polling
    // if runners start exceeding HTTP timeouts.
    const result = await fireP;
    res.json(result);
  } catch (e) {
    console.error('[jobs/run]', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- PAUSE / RESUME ---------- */
router.post('/:jobID(\\d+)/pause', async (req, res) => {
  try {
    const jobID = parseInt(req.params.jobID, 10);
    const pool = await getPool();
    await pool.request()
      .input('jid', sql.Int, jobID)
      .query(`
        UPDATE admin.Jobs
           SET IsActive = 0, UpdatedAt = SYSUTCDATETIME()
         WHERE JobID = @jid
      `);
    await scheduler.reload(jobID).catch(() => {});
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.post('/:jobID(\\d+)/resume', async (req, res) => {
  try {
    const jobID = parseInt(req.params.jobID, 10);
    const pool = await getPool();
    await pool.request()
      .input('jid', sql.Int, jobID)
      .query(`
        UPDATE admin.Jobs
           SET IsActive = 1,
               ConsecutiveFailures = 0,
               LastErrorMessage = NULL,
               LastErrorFingerprint = NULL,
               UpdatedAt = SYSUTCDATETIME()
         WHERE JobID = @jid
      `);
    await scheduler.reload(jobID).catch(() => {});
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------- SCHEDULER STATUS ---------- */
router.get('/scheduler/status', (_req, res) => {
  res.json({
    scheduler: scheduler.status(),
    registeredRunners: listRegistered(),
  });
});

/* ============================================================
   Endpoint-first views — Phase 2 of the endpoint-as-profile work.
   The Jobs page now renders endpoints as the top-level entity, with
   their attached brand jobs nested. Defaults editor + auto-create
   flag live on the endpoint, brand jobs inherit + override.
   ============================================================ */

/* GET /api/jobs/by-endpoint — endpoints with their attached brand jobs */
router.get('/by-endpoint', async (_req, res) => {
  try {
    const pool = await getPool();
    const epRes = await pool.request().query(`
      SELECT e.EndpointID, e.EndpointUID, e.Name AS EndpointName, e.DisplayName AS EndpointDisplay,
             e.IsActive AS EndpointIsActive,
             e.DefaultCronExpression, e.DefaultTimezoneIANA, e.DefaultParams,
             e.DefaultExecutionMode, e.DefaultJobType, e.DefaultIsActive,
             e.AutoCreateOnNewBrand,
             c.Name AS ConnectorName, c.DisplayName AS ConnectorDisplay,
             c.ConnectorUID
      FROM admin.Endpoints e
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
      ORDER BY c.DisplayName, e.IsActive DESC, e.Name;
    `);

    const jobsRes = await pool.request().query(`
      SELECT j.JobID, j.JobUID, j.Name AS JobName, j.EndpointID,
             j.CronExpression, j.TimezoneIANA, j.Params,
             j.IsActive, j.ExecutionMode, j.JobType, j.Priority,
             j.LastRunAt, j.LastRunStatus, j.NextRunAt,
             j.LastErrorMessage, j.ConsecutiveFailures,
             j.BrandUID, b.BrandName,
             (SELECT COUNT(*) FROM admin.JobRuns jr3
               WHERE jr3.JobID = j.JobID AND jr3.Status = 'RUNNING'
                 AND jr3.StartedAt > DATEADD(HOUR, -1, SYSUTCDATETIME())) AS InFlight,
             (SELECT TOP 1 jr2.DurationMs FROM admin.JobRuns jr2
               WHERE jr2.JobID = j.JobID AND jr2.Status = 'SUCCESS'
               ORDER BY jr2.StartedAt DESC) AS LastSuccessDurationMs
      FROM admin.Jobs j
      LEFT JOIN admin.Brands b ON b.BrandUID = j.BrandUID
      ORDER BY b.BrandName;
    `);

    // Group jobs by endpoint
    const byEndpoint = new Map();
    for (const ep of epRes.recordset) byEndpoint.set(ep.EndpointID, { endpoint: ep, jobs: [] });
    for (const j of jobsRes.recordset) {
      const slot = byEndpoint.get(j.EndpointID);
      if (slot) slot.jobs.push(j);
    }

    res.json({ groups: Array.from(byEndpoint.values()) });
  } catch (e) {
    console.error('[jobs/by-endpoint]', e);
    res.status(500).json({ error: e.message });
  }
});

/* POST /api/endpoints/:endpointID/attach-brand
   Body: { brandUID, overrideParams? }
   Creates an admin.Jobs row from the endpoint's defaults. Caller can
   override cron/params via the row later via PATCH /api/jobs/:id. */
router.post('/endpoints/:endpointID(\\d+)/attach-brand', async (req, res) => {
  try {
    const endpointID = parseInt(req.params.endpointID, 10);
    const { brandUID, overrideParams } = req.body || {};
    if (!brandUID) return res.status(400).json({ error: 'brandUID is required' });

    const pool = await getPool();
    const epR = await pool.request()
      .input('eid', sql.Int, endpointID)
      .query(`
        SELECT e.EndpointID, e.Name AS EndpointName,
               e.DefaultCronExpression, e.DefaultTimezoneIANA, e.DefaultParams,
               e.DefaultExecutionMode, e.DefaultJobType, e.DefaultIsActive
        FROM admin.Endpoints e WHERE e.EndpointID = @eid;
      `);
    if (!epR.recordset.length) return res.status(404).json({ error: 'Endpoint not found' });
    const ep = epR.recordset[0];

    // Dedupe: don't double-create
    const existing = await pool.request()
      .input('eid', sql.Int, endpointID)
      .input('uid', sql.UniqueIdentifier, brandUID)
      .query(`SELECT TOP 1 JobID FROM admin.Jobs WHERE EndpointID = @eid AND BrandUID = @uid AND JobType = @jt`);
    // Add @jt parameter for query
    const dedupeR = await pool.request()
      .input('eid', sql.Int, endpointID)
      .input('uid', sql.UniqueIdentifier, brandUID)
      .input('jt',  sql.NVarChar(20), ep.DefaultJobType || 'INGEST')
      .query(`SELECT TOP 1 JobID FROM admin.Jobs WHERE EndpointID = @eid AND BrandUID = @uid AND JobType = @jt`);
    if (dedupeR.recordset.length) {
      return res.status(409).json({ error: 'Brand already attached to this endpoint',
                                    existingJobID: dedupeR.recordset[0].JobID });
    }

    const brandR = await pool.request()
      .input('uid', sql.UniqueIdentifier, brandUID)
      .query(`SELECT BrandName FROM admin.Brands WHERE BrandUID = @uid`);
    const brandName = brandR.recordset[0]?.BrandName || brandUID;

    const params = overrideParams !== undefined
      ? (overrideParams && JSON.stringify(overrideParams) !== '{}' ? JSON.stringify(overrideParams) : null)
      : null; // null = inherit from endpoint default

    const ins = await pool.request()
      .input('name', sql.NVarChar(100), ep.EndpointName + ' · ' + brandName)
      .input('eid',  sql.Int, endpointID)
      .input('uid',  sql.UniqueIdentifier, brandUID)
      .input('cron', sql.NVarChar(50), ep.DefaultCronExpression || null)
      .input('tz',   sql.NVarChar(50), ep.DefaultTimezoneIANA || 'America/Chicago')
      .input('mode', sql.NVarChar(20), ep.DefaultExecutionMode || 'NODE_NATIVE')
      .input('jt',   sql.NVarChar(20), ep.DefaultJobType || 'INGEST')
      .input('par',  sql.NVarChar(sql.MAX), params)
      .input('act',  sql.Bit, ep.DefaultIsActive ? 1 : 0)
      .input('ck',   sql.NVarChar(100), ep.EndpointName.toLowerCase().replace(/_/g,'-')
                                          + ':' + brandUID)
      .query(`
        INSERT INTO admin.Jobs (Name, EndpointID, BrandUID, CronExpression, TimezoneIANA,
                                ExecutionMode, JobType, Params, IsActive, ConcurrencyKey, Priority)
        OUTPUT INSERTED.JobID
        VALUES (@name, @eid, @uid, @cron, @tz, @mode, @jt, @par, @act, @ck, 50);
      `);
    const jobID = ins.recordset[0].JobID;
    await scheduler.reload(jobID).catch((e) =>
      console.error('[attach-brand] reload failed:', e.message));

    res.status(201).json({ jobID, brandName, endpointName: ep.EndpointName });
  } catch (e) {
    console.error('[attach-brand]', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- RUNBOOKS ---------- */
router.get('/runbooks', async (_req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request().query(`
      SELECT RunbookID, MatchPattern, Title, WhatItMeans, HowToFix, Severity, IsActive
      FROM admin.ErrorRunbooks
      ORDER BY Severity DESC, Title ASC
    `);
    res.json({ runbooks: r.recordset });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
