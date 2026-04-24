/**
 * Node-native scheduler. Reads admin.Jobs on boot, schedules every
 * ExecutionMode='NODE_NATIVE' + IsActive=1 + CronExpression job with
 * node-cron, and fires the matching runner from lib/runners/index.js.
 *
 * Contract with the rest of the app:
 *   start()             — wire schedules, must be called once on server boot
 *   reload(jobID?)      — re-read admin.Jobs (or just one) and (re)schedule
 *   runNow(jobID, opts) — fire a job immediately, returns { runID }
 *   status()            — snapshot for /internal/scheduler/status
 *
 * Concurrency: a job will NOT run if admin.JobRuns already has a RUNNING
 * row for the same JobID less than 1 hour old. This prevents a long-running
 * job from stacking up if its cron fires mid-run.
 *
 * Auto-disable: after 5 consecutive failures, admin.Jobs.IsActive is set
 * to 0 and the schedule is torn down. Requires manual re-enable via the UI.
 *
 * NOTE: App Service single-instance is assumed. When the app scales out,
 * we'll need distributed locking (see the note in scheduler.txt plan).
 */
const cron = require('node-cron');
const cronParser = require('cron-parser');
const os = require('os');
const crypto = require('crypto');
const { sql, getPool } = require('../config/db');
const { getRunner } = require('./runners');

const FAILURE_DISABLE_THRESHOLD = 5;
const WORKER_HOST = os.hostname();

const scheduled = new Map(); // jobID → { task, cronExpr, timezone, job }
let started = false;

/* ---------- Boot ---------- */

async function start() {
  if (started) return;
  started = true;
  console.log('[scheduler] starting on', WORKER_HOST);
  await reload();
  console.log('[scheduler] ready, ' + scheduled.size + ' NODE_NATIVE jobs scheduled');
}

async function reload(jobID) {
  const pool = await getPool();
  const request = pool.request();
  let where = `j.ExecutionMode = 'NODE_NATIVE' AND j.IsActive = 1 AND j.CronExpression IS NOT NULL`;
  if (jobID != null) {
    request.input('jid', sql.Int, jobID);
    where += ' AND j.JobID = @jid';
  }
  const r = await request.query(`
    SELECT j.JobID, j.Name, j.CronExpression, j.TimezoneIANA,
           j.BrandUID, j.EndpointID, j.ConcurrencyKey,
           e.Name AS EndpointName,
           c.Name AS ConnectorName
    FROM admin.Jobs j
    JOIN admin.Endpoints  e ON e.EndpointID  = j.EndpointID
    JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
    WHERE ${where}
  `);

  // If we're reloading a single job, tear down its old schedule regardless.
  if (jobID != null) teardown(jobID);

  for (const row of r.recordset) {
    scheduleOne(row);
  }

  // If we did a full reload, also tear down anything that's no longer active.
  if (jobID == null) {
    const activeIDs = new Set(r.recordset.map((x) => x.JobID));
    for (const id of Array.from(scheduled.keys())) {
      if (!activeIDs.has(id)) teardown(id);
    }
  }
}

function teardown(jobID) {
  const entry = scheduled.get(jobID);
  if (!entry) return;
  try { entry.task.stop(); } catch (_) { /* ignore */ }
  scheduled.delete(jobID);
  console.log('[scheduler] torn down job', jobID);
}

function scheduleOne(job) {
  const runner = getRunner(job.EndpointName);
  if (!runner) {
    console.warn('[scheduler] no runner registered for endpoint "' + job.EndpointName +
      '" — job ' + job.JobID + ' will not run until a runner is added to lib/runners/index.js');
    return;
  }
  const expr = job.CronExpression;
  if (!cron.validate(expr)) {
    console.error('[scheduler] job ' + job.JobID + ' has invalid cron "' + expr + '"');
    return;
  }

  const tz = job.TimezoneIANA || 'America/Chicago';
  const task = cron.schedule(expr, () => {
    fire(job.JobID, { triggeredBy: 'SCHEDULE' }).catch((e) => {
      console.error('[scheduler] fire(' + job.JobID + ') threw:', e.message);
    });
  }, { timezone: tz });

  scheduled.set(job.JobID, { task, cronExpr: expr, timezone: tz, job });
  console.log('[scheduler] scheduled job ' + job.JobID + ' "' + job.Name + '" @ ' + expr + ' (' + tz + ')');

  // Publish NextRunAt so the UI can show "next: 3h 20m" without re-parsing
  // cron client-side. Best-effort — a parser failure shouldn't block
  // scheduling.
  writeNextRunAt(job.JobID, expr, tz).catch((e) => {
    console.warn('[scheduler] failed to write NextRunAt for job ' + job.JobID + ':', e.message);
  });
}

/**
 * Compute the next fire time from a cron expression + IANA tz, and
 * UPDATE admin.Jobs.NextRunAt. Returns the Date or null.
 */
async function writeNextRunAt(jobID, expr, tz) {
  const next = computeNextRun(expr, tz);
  const pool = await getPool();
  await pool.request()
    .input('jid', sql.Int, jobID)
    .input('nxt', sql.DateTime2, next)
    .query(`UPDATE admin.Jobs SET NextRunAt = @nxt WHERE JobID = @jid`);
  return next;
}

function computeNextRun(expr, tz) {
  try {
    const it = cronParser.parseExpression(expr, { tz: tz || 'America/Chicago' });
    return it.next().toDate();
  } catch (e) {
    return null;
  }
}

/* ---------- Fire a job ---------- */

async function runNow(jobID, { triggeredBy = 'MANUAL', userID = null } = {}) {
  return fire(jobID, { triggeredBy, userID });
}

async function fire(jobID, { triggeredBy, userID } = {}) {
  const pool = await getPool();

  // Load the job row — includes the params/credential hints the runner needs.
  const j = await pool.request()
    .input('jid', sql.Int, jobID)
    .query(`
      SELECT j.JobID, j.Name, j.BrandUID, j.EndpointID, j.ConcurrencyKey,
             j.IsActive, j.Params,
             e.Name AS EndpointName,
             c.Name AS ConnectorName,
             -- Latest active credential for this (brand, connector) — runners
             -- generally need this; if a runner doesn't, it ignores credentialID.
             (SELECT TOP 1 bc.CredentialID
                FROM admin.BrandCredentials bc
               WHERE bc.BrandUID = j.BrandUID AND bc.ConnectorID = c.ConnectorID
                 AND bc.IsActive = 1
               ORDER BY bc.UpdatedAt DESC) AS CredentialID
      FROM admin.Jobs j
      JOIN admin.Endpoints  e ON e.EndpointID  = j.EndpointID
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
      WHERE j.JobID = @jid
    `);
  if (!j.recordset.length) throw new Error('Job ' + jobID + ' not found');
  const job = j.recordset[0];

  // Cron MUST respect IsActive=0 — paused means paused. But MANUAL triggers
  // (Run-now button, retry, explicit API call) are allowed to fire paused
  // jobs. This is how BACKFILL jobs work: IsActive=0 by default so cron
  // ignores them, user hits "Run now" during onboarding to kick off.
  if (!job.IsActive && triggeredBy === 'SCHEDULE') {
    throw new Error('Job ' + jobID + ' is paused (IsActive=0) — re-enable before scheduling');
  }

  // Concurrency gate — is there a RUNNING row < 1h old?
  const lock = await pool.request()
    .input('jid', sql.Int, jobID)
    .query(`
      SELECT TOP 1 RunID, StartedAt
      FROM admin.JobRuns
      WHERE JobID = @jid AND Status = 'RUNNING'
        AND StartedAt > DATEADD(HOUR, -1, SYSUTCDATETIME())
      ORDER BY StartedAt DESC
    `);
  if (lock.recordset.length) {
    throw new Error('Job ' + jobID + ' already has a run in flight (RunID ' +
      lock.recordset[0].RunID + ') — skipping');
  }

  // Create a RUNNING row so the UI sees the job in flight immediately.
  const started = new Date();
  const runIns = await pool.request()
    .input('jid', sql.Int, jobID)
    .input('started', sql.DateTime2, started)
    .input('tb', sql.NVarChar(30), triggeredBy || 'SCHEDULE')
    .input('wh', sql.NVarChar(100), WORKER_HOST)
    .query(`
      INSERT INTO admin.JobRuns (JobID, StartedAt, Status, TriggeredBy, WorkerType, WorkerHost)
      OUTPUT INSERTED.RunID
      VALUES (@jid, @started, 'RUNNING', @tb, 'NODE', @wh);
    `);
  const runID = runIns.recordset[0].RunID;
  console.log('[scheduler] fire job ' + jobID + ' "' + job.Name + '" → runID ' + runID);

  // Call the runner
  const runner = getRunner(job.EndpointName);
  if (!runner) {
    await finalizeRun(runID, {
      status: 'FAILED',
      rowsIngested: null,
      errorMessage: 'No runner registered for endpoint "' + job.EndpointName + '"',
    });
    await bumpJobOnFailure(jobID, 'No runner registered for endpoint "' + job.EndpointName + '"');
    throw new Error('No runner registered for endpoint "' + job.EndpointName + '"');
  }

  // Parse job's Params JSON (runner config). If the blob is malformed we
  // log but don't fail the run — runners fall back to their defaults.
  let params = {};
  if (job.Params) {
    try { params = JSON.parse(job.Params); }
    catch (e) {
      console.warn('[scheduler] job ' + jobID + ' has malformed Params JSON, ignoring: ' + e.message);
    }
  }

  try {
    const result = await runner({
      credentialID: job.CredentialID,
      brandUID:     job.BrandUID,
      jobID,
      runID,
      triggeredBy,
      userID,
      params,
    });

    // Runners that wrote their own JobRuns row signal wroteOwnJobRun=true.
    // In that case our pre-inserted RUNNING row is orphaned — mark it as
    // a no-op terminal state so we don't leave it hanging.
    if (result && result.wroteOwnJobRun) {
      await reconcileOwnJobRun(runID, result);
    } else {
      await finalizeRun(runID, {
        status: result && result.status ? result.status : 'SUCCESS',
        rowsIngested: result && result.rowsIngested != null ? result.rowsIngested : null,
        errorMessage: null,
      });
    }

    await bumpJobOnSuccess(jobID, result);
    // After a successful fire, advance NextRunAt so the UI shows the
    // upcoming slot, not the one we just consumed.
    const entry = scheduled.get(jobID);
    if (entry) {
      writeNextRunAt(jobID, entry.cronExpr, entry.timezone).catch(() => {});
    }
    return { runID, ok: true, result };

  } catch (e) {
    const msg = (e.message || 'Unknown runner error').slice(0, 4000);
    console.error('[scheduler] job ' + jobID + ' runID ' + runID + ' failed:', msg);

    // If the runner wrote its own JobRuns row AND threw, our pre-inserted
    // RUNNING row needs to be marked FAILED too so the UI shows the error.
    await finalizeRun(runID, {
      status: 'FAILED',
      rowsIngested: null,
      errorMessage: msg,
    }).catch((_) => { /* the row may already be terminal */ });

    await bumpJobOnFailure(jobID, msg);
    // Even on failure, advance NextRunAt to the upcoming fire so the UI
    // doesn't show a stale "next" that's already passed.
    const entry = scheduled.get(jobID);
    if (entry) {
      writeNextRunAt(jobID, entry.cronExpr, entry.timezone).catch(() => {});
    }
    return { runID, ok: false, error: msg };
  }
}

/* ---------- JobRuns + Jobs bookkeeping ---------- */

async function finalizeRun(runID, { status, rowsIngested, errorMessage }) {
  const pool = await getPool();
  const fp = errorMessage ? fingerprintError(errorMessage) : null;
  await pool.request()
    .input('rid', sql.BigInt, runID)
    .input('s',   sql.NVarChar(20), status)
    .input('ri',  sql.Int, rowsIngested)
    .input('em',  sql.NVarChar(sql.MAX), errorMessage)
    .input('ef',  sql.NVarChar(64), fp)
    .query(`
      UPDATE admin.JobRuns
         SET EndedAt          = SYSUTCDATETIME(),
             Status            = @s,
             RowsIngested      = @ri,
             ErrorMessage      = @em,
             ErrorFingerprint  = @ef
       WHERE RunID = @rid AND Status = 'RUNNING'
    `);
}

async function reconcileOwnJobRun(placeholderRunID, result) {
  // The runner wrote its own JobRuns row; our pre-inserted RUNNING row is
  // orphaned. Mark it as CANCELED with a note so it doesn't skew counts.
  const pool = await getPool();
  await pool.request()
    .input('rid', sql.BigInt, placeholderRunID)
    .input('em',  sql.NVarChar(sql.MAX), 'Superseded by runner-written RunID ' + (result.runID || '?'))
    .query(`
      UPDATE admin.JobRuns
         SET EndedAt      = SYSUTCDATETIME(),
             Status        = 'CANCELED',
             ErrorMessage  = @em
       WHERE RunID = @rid AND Status = 'RUNNING'
    `);
}

async function bumpJobOnSuccess(jobID, _result) {
  const pool = await getPool();
  await pool.request()
    .input('jid', sql.Int, jobID)
    .query(`
      UPDATE admin.Jobs
         SET LastRunAt             = SYSUTCDATETIME(),
             LastRunStatus         = 'SUCCESS',
             LastErrorMessage      = NULL,
             LastErrorFingerprint  = NULL,
             ConsecutiveFailures   = 0,
             UpdatedAt             = SYSUTCDATETIME()
       WHERE JobID = @jid
    `);
}

async function bumpJobOnFailure(jobID, errorMessage) {
  const pool = await getPool();
  const fp = fingerprintError(errorMessage);
  const r = await pool.request()
    .input('jid', sql.Int, jobID)
    .input('em',  sql.NVarChar(sql.MAX), errorMessage)
    .input('ef',  sql.NVarChar(64), fp)
    .query(`
      UPDATE admin.Jobs
         SET LastRunAt             = SYSUTCDATETIME(),
             LastRunStatus         = 'FAILED',
             LastErrorMessage      = @em,
             LastErrorFingerprint  = @ef,
             ConsecutiveFailures   = ConsecutiveFailures + 1,
             UpdatedAt             = SYSUTCDATETIME()
       OUTPUT INSERTED.ConsecutiveFailures, INSERTED.IsActive
       WHERE JobID = @jid
    `);

  const consec = r.recordset[0]?.ConsecutiveFailures || 0;
  if (consec >= FAILURE_DISABLE_THRESHOLD) {
    console.warn('[scheduler] job ' + jobID + ' hit ' + consec +
      ' consecutive failures — auto-pausing');
    const pool2 = await getPool();
    await pool2.request()
      .input('jid', sql.Int, jobID)
      .query(`UPDATE admin.Jobs SET IsActive = 0, UpdatedAt = SYSUTCDATETIME() WHERE JobID = @jid`);
    teardown(jobID);
  }
}

/* ---------- Error fingerprinting ----------
   Strip volatile substrings (UUIDs, numbers, timestamps) so the same
   class of error hashes to the same fingerprint across runs. Fingerprint
   keys into admin.ErrorRunbooks via LIKE matches on MatchPattern.
*/
function fingerprintError(msg) {
  if (!msg) return null;
  const norm = String(msg)
    .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, '{uuid}')
    .replace(/\b\d{4}-\d{2}-\d{2}T[\d:.Z+-]+\b/g, '{timestamp}')
    .replace(/\b\d{6,}\b/g, '{num}')
    .toLowerCase()
    .slice(0, 500);
  return crypto.createHash('sha256').update(norm).digest('hex');
}

/* ---------- Status snapshot ---------- */

function status() {
  return {
    workerHost: WORKER_HOST,
    started,
    scheduledCount: scheduled.size,
    jobs: Array.from(scheduled.values()).map((e) => ({
      jobID:    e.job.JobID,
      name:     e.job.Name,
      cron:     e.cronExpr,
      timezone: e.timezone,
      endpoint: e.job.EndpointName,
    })),
  };
}

module.exports = {
  start,
  reload,
  runNow,
  status,
  fingerprintError, // exported for test + routes
};
