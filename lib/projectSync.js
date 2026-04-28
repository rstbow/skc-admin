/**
 * Project sync engine — Folder System v2.
 *
 * Maintains the materialization invariants between admin.Projects /
 * ProjectEndpoints / ProjectBrands and admin.Jobs. "Project wins" — the
 * Project's settings are authoritative; managed Jobs are derived state.
 *
 * Invariants enforced:
 *   I1. For every (active PE × active PB) pair belonging to the project,
 *       exactly one Job row exists with:
 *         ManagedByProjectID         = pe.ProjectID
 *         ManagedByProjectEndpointID = pe.ProjectEndpointID
 *         EndpointID                 = pe.EndpointID
 *         BrandUID                   = pb.BrandUID
 *         JobType                    = pe.JobType
 *
 *   I2. Each managed Job's CronExpression / TimezoneIANA / Params / Priority
 *       match its PE exactly. Job.IsActive = (pe.IsActive AND pb.IsActive).
 *
 *   I3. Managed Jobs whose PE × PB membership no longer exists (rows
 *       deleted, not just deactivated) are *soft-deleted*:
 *         IsActive = 0
 *         ManagedByProjectID = NULL
 *         ManagedByProjectEndpointID = NULL
 *       Run history (admin.JobRuns) preserved. The Job becomes "unmanaged
 *       + paused" — operator can manually re-activate it from the
 *       brand-tree view if useful.
 *
 * Soft-delete chosen over hard-delete because admin.JobRuns has a plain
 * FK to admin.Jobs(JobID); hard-delete would FK-violate against any job
 * with run history. Append-only audit trail is preserved.
 *
 * Public API:
 *   syncProject({ projectID, dryRun?, triggeredBy? }) → SyncResult
 *   syncAllActiveProjects({ dryRun?, triggeredBy? })   → SyncResult[]
 *
 * SyncResult shape:
 *   {
 *     projectID, projectName,
 *     added:    [ { jobID, projectEndpointID, brandUID, endpointName } ],
 *     updated:  [ { jobID, fields: ['CronExpression', 'IsActive', ...] } ],
 *     orphaned: [ { jobID, reason: 'pe-removed' | 'pb-removed' | 'both' } ],
 *     unchanged: number,
 *     dryRun: bool,
 *   }
 *
 * Called by routes/projects.js after every project-mutating request.
 * Single transaction per project for atomicity. Idempotent.
 */
const { sql, getPool } = require('../config/db');

/* ---------- Internal helpers ---------- */

function paramsEqual(a, b) {
  // Both NULL ⇒ equal. Otherwise byte-equal string compare on the JSON text.
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return String(a) === String(b);
}

function nullableStringEqual(a, b) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return String(a) === String(b);
}

/* ---------- Core sync ---------- */

/**
 * Reconcile materialization for one project.
 *
 * @param {object} opts
 * @param {number} opts.projectID
 * @param {boolean} [opts.dryRun=false]   — when true, no writes; returns plan only
 * @param {string} [opts.triggeredBy]     — for logging only
 * @returns {Promise<SyncResult>}
 */
async function syncProject({ projectID, dryRun = false, triggeredBy = 'sync' }) {
  if (!projectID || !Number.isInteger(projectID)) {
    throw new Error('syncProject: projectID is required and must be an integer');
  }

  const pool = await getPool();

  // Load project header (existence + name for the result payload)
  const headerR = await pool.request()
    .input('p', sql.Int, projectID)
    .query('SELECT ProjectID, Name, IsActive FROM admin.Projects WHERE ProjectID = @p');
  if (!headerR.recordset.length) {
    throw new Error('syncProject: project not found, ProjectID=' + projectID);
  }
  const project = headerR.recordset[0];

  // Load active ProjectEndpoints
  const peR = await pool.request()
    .input('p', sql.Int, projectID)
    .query(`
      SELECT ProjectEndpointID, ProjectID, EndpointID, JobType,
             CronExpression, TimezoneIANA, Params, Priority, IsActive
      FROM admin.ProjectEndpoints
      WHERE ProjectID = @p
    `);
  const peRows = peR.recordset;
  const activePEs = peRows.filter(pe => pe.IsActive);

  // Load active ProjectBrands
  const pbR = await pool.request()
    .input('p', sql.Int, projectID)
    .query(`
      SELECT ProjectBrandID, ProjectID, BrandUID, IsActive
      FROM admin.ProjectBrands
      WHERE ProjectID = @p
    `);
  const pbRows = pbR.recordset;
  const activePBs = pbRows.filter(pb => pb.IsActive);

  // Load existing managed Jobs for this project
  const jR = await pool.request()
    .input('p', sql.Int, projectID)
    .query(`
      SELECT j.JobID, j.ManagedByProjectID, j.ManagedByProjectEndpointID,
             j.EndpointID, j.JobType, j.BrandUID,
             j.CronExpression, j.TimezoneIANA, j.Params, j.Priority, j.IsActive,
             j.Name
      FROM admin.Jobs j
      WHERE j.ManagedByProjectID = @p
    `);
  const managedJobs = jR.recordset;

  // Index existing managed jobs by (ProjectEndpointID, BrandUID lowercase)
  const jobsByKey = new Map();
  for (const j of managedJobs) {
    const key = j.ManagedByProjectEndpointID + '|' + String(j.BrandUID).toLowerCase();
    jobsByKey.set(key, j);
  }

  // Load CLAIM-ELIGIBLE orphan Jobs — unmanaged Jobs that share
  // (EndpointID, JobType, BrandUID) with one of this project's PE × PB
  // tuples. These are typically Jobs created by JobBundle runs (e.g. the
  // amazon-onboarding bundle) that should now become project-managed.
  // Without this step, calling syncProject() right after runBundle()
  // would create DUPLICATE Jobs for the same brand × endpoint.
  const orphansByKey = new Map();
  if (peRows.length && pbRows.length) {
    const peEndpointIDs = [...new Set(peRows.map(pe => pe.EndpointID))];
    const pbBrandUIDs   = [...new Set(pbRows.map(pb => String(pb.BrandUID)))];
    const epPlace = peEndpointIDs.map((_, i) => '@oe' + i).join(',');
    const brPlace = pbBrandUIDs.map((_, i)  => '@ob' + i).join(',');
    const oReq = pool.request();
    peEndpointIDs.forEach((id, i) => oReq.input('oe' + i, sql.Int,              id));
    pbBrandUIDs.forEach((id, i)   => oReq.input('ob' + i, sql.UniqueIdentifier, id));
    const oR = await oReq.query(`
      SELECT JobID, EndpointID, JobType, BrandUID,
             CronExpression, TimezoneIANA, Params, Priority, IsActive, Name
      FROM admin.Jobs
      WHERE ManagedByProjectID IS NULL
        AND EndpointID IN (${epPlace})
        AND BrandUID   IN (${brPlace})
    `);
    for (const j of oR.recordset) {
      // Key by (EndpointID, JobType, BrandUID) — matches PE × PB lookup shape.
      const key = j.EndpointID + '|' + j.JobType + '|' + String(j.BrandUID).toLowerCase();
      // If multiple unmanaged Jobs collide on the same key, prefer the most
      // recently-touched one (highest JobID under IDENTITY ordering).
      const prior = orphansByKey.get(key);
      if (!prior || j.JobID > prior.JobID) orphansByKey.set(key, j);
    }
  }

  // Build the desired set: cartesian product of all PE rows × all PB rows
  // (note: PE/PB rows are included regardless of their IsActive — the IsActive
  // flag propagates to the materialized Job via I2, but the *materialization*
  // itself is keyed on row existence).
  const desired = [];
  for (const pe of peRows) {
    for (const pb of pbRows) {
      desired.push({ pe, pb });
    }
  }

  const result = {
    projectID,
    projectName: project.Name,
    added: [],
    updated: [],
    claimed: [],          // unmanaged Jobs adopted into the project (orphan claim)
    orphaned: [],
    unchanged: 0,
    dryRun,
    triggeredBy,
  };

  // Endpoint-name lookup for the result payload (best-effort)
  const endpointIDs = [...new Set(peRows.map(pe => pe.EndpointID))];
  const endpointNameByID = new Map();
  if (endpointIDs.length) {
    const placeholders = endpointIDs.map((_, i) => '@e' + i).join(',');
    const eReq = pool.request();
    endpointIDs.forEach((id, i) => eReq.input('e' + i, sql.Int, id));
    const eR = await eReq.query(`SELECT EndpointID, Name FROM admin.Endpoints WHERE EndpointID IN (${placeholders})`);
    for (const row of eR.recordset) endpointNameByID.set(row.EndpointID, row.Name);
  }

  // Open a transaction for the writes (skipped on dry-run)
  const tx = dryRun ? null : pool.transaction();
  if (tx) await tx.begin();

  try {
    /* ---------- Step 1: ensure each desired (PE × PB) has a Job ---------- */
    for (const { pe, pb } of desired) {
      const key = pe.ProjectEndpointID + '|' + String(pb.BrandUID).toLowerCase();
      const existing = jobsByKey.get(key);
      const desiredActive = !!(pe.IsActive && pb.IsActive);
      const desiredCron     = pe.CronExpression;
      const desiredTZ       = pe.TimezoneIANA || 'America/Chicago';
      const desiredParams   = pe.Params;
      const desiredPriority = pe.Priority != null ? pe.Priority : 50;
      const desiredJobType  = pe.JobType;

      if (!existing) {
        /* CLAIM-ORPHAN: before inserting a fresh Job, see if there's an
           unmanaged Job for this exact (EndpointID, JobType, BrandUID) that
           we can adopt. Bundle-created Jobs land here when the onboarding
           hook runs runBundle() and then adds the brand to the project. */
        const orphanKey = pe.EndpointID + '|' + desiredJobType + '|' + String(pb.BrandUID).toLowerCase();
        const orphan = orphansByKey.get(orphanKey);
        if (orphan) {
          orphansByKey.delete(orphanKey); // single claim per orphan
          if (!dryRun) {
            await tx.request()
              .input('jobID',    sql.Int,               orphan.JobID)
              .input('cron',     sql.NVarChar(50),      desiredCron)
              .input('tz',       sql.NVarChar(50),      desiredTZ)
              .input('params',   sql.NVarChar(sql.MAX), desiredParams)
              .input('priority', sql.Int,               desiredPriority)
              .input('isActive', sql.Bit,               desiredActive ? 1 : 0)
              .input('jobType',  sql.NVarChar(20),      desiredJobType)
              .input('mpID',     sql.Int,               pe.ProjectID)
              .input('mpepID',   sql.Int,               pe.ProjectEndpointID)
              .query(`
                UPDATE admin.Jobs
                SET ManagedByProjectID         = @mpID,
                    ManagedByProjectEndpointID = @mpepID,
                    CronExpression             = @cron,
                    TimezoneIANA               = @tz,
                    Params                     = @params,
                    Priority                   = @priority,
                    IsActive                   = @isActive,
                    JobType                    = @jobType,
                    UpdatedAt                  = SYSUTCDATETIME()
                WHERE JobID = @jobID
              `);
          }
          result.claimed.push({
            jobID:             orphan.JobID,
            projectEndpointID: pe.ProjectEndpointID,
            brandUID:          pb.BrandUID,
            endpointName:      endpointNameByID.get(pe.EndpointID) || ('endpoint-' + pe.EndpointID),
          });
          continue;
        }

        /* INSERT new managed Job */
        const epName = endpointNameByID.get(pe.EndpointID) || ('endpoint-' + pe.EndpointID);
        const jobName = '[' + project.Name + '] ' + epName;
        if (!dryRun) {
          const ins = await tx.request()
            .input('endpointID',   sql.Int,              pe.EndpointID)
            .input('brandUID',     sql.UniqueIdentifier, pb.BrandUID)
            .input('jobType',      sql.NVarChar(20),     desiredJobType)
            .input('name',         sql.NVarChar(150),    jobName)
            .input('cron',         sql.NVarChar(50),     desiredCron)
            .input('tz',           sql.NVarChar(50),     desiredTZ)
            .input('params',       sql.NVarChar(sql.MAX), desiredParams)
            .input('priority',     sql.Int,              desiredPriority)
            .input('isActive',     sql.Bit,              desiredActive ? 1 : 0)
            .input('mpID',         sql.Int,              pe.ProjectID)
            .input('mpepID',       sql.Int,              pe.ProjectEndpointID)
            .query(`
              INSERT INTO admin.Jobs (
                EndpointID, BrandUID, JobType, Name, CronExpression, TimezoneIANA,
                Params, Priority, IsActive,
                ManagedByProjectID, ManagedByProjectEndpointID
              )
              OUTPUT INSERTED.JobID
              VALUES (
                @endpointID, @brandUID, @jobType, @name, @cron, @tz,
                @params, @priority, @isActive,
                @mpID, @mpepID
              )
            `);
          result.added.push({
            jobID:             ins.recordset[0].JobID,
            projectEndpointID: pe.ProjectEndpointID,
            brandUID:          pb.BrandUID,
            endpointName:      epName,
          });
        } else {
          result.added.push({
            jobID:             null,
            projectEndpointID: pe.ProjectEndpointID,
            brandUID:          pb.BrandUID,
            endpointName:      epName,
          });
        }
        continue;
      }

      /* UPDATE existing managed Job if any field drifted */
      const drift = [];
      if (!nullableStringEqual(existing.CronExpression, desiredCron)) drift.push('CronExpression');
      if (!nullableStringEqual(existing.TimezoneIANA,   desiredTZ))   drift.push('TimezoneIANA');
      if (!paramsEqual(existing.Params, desiredParams))               drift.push('Params');
      if ((existing.Priority || 0) !== desiredPriority)               drift.push('Priority');
      if (!!existing.IsActive !== desiredActive)                       drift.push('IsActive');
      // JobType drift shouldn't happen (the key includes it implicitly), but guard.
      if (existing.JobType !== desiredJobType)                         drift.push('JobType');

      if (!drift.length) {
        result.unchanged++;
        continue;
      }

      if (!dryRun) {
        await tx.request()
          .input('jobID',    sql.Int,               existing.JobID)
          .input('cron',     sql.NVarChar(50),      desiredCron)
          .input('tz',       sql.NVarChar(50),      desiredTZ)
          .input('params',   sql.NVarChar(sql.MAX), desiredParams)
          .input('priority', sql.Int,               desiredPriority)
          .input('isActive', sql.Bit,               desiredActive ? 1 : 0)
          .input('jobType',  sql.NVarChar(20),      desiredJobType)
          .query(`
            UPDATE admin.Jobs
            SET CronExpression = @cron,
                TimezoneIANA   = @tz,
                Params         = @params,
                Priority       = @priority,
                IsActive       = @isActive,
                JobType        = @jobType,
                UpdatedAt      = SYSUTCDATETIME()
            WHERE JobID = @jobID
          `);
      }
      result.updated.push({ jobID: existing.JobID, fields: drift });
    }

    /* ---------- Step 2: orphan Jobs whose PE × PB no longer exists ---------- */
    const desiredKeys = new Set(
      desired.map(({ pe, pb }) => pe.ProjectEndpointID + '|' + String(pb.BrandUID).toLowerCase())
    );
    for (const j of managedJobs) {
      const key = j.ManagedByProjectEndpointID + '|' + String(j.BrandUID).toLowerCase();
      if (desiredKeys.has(key)) continue;

      // Determine why it's orphaned (best-effort label for observability)
      const peStillExists = peRows.some(pe => pe.ProjectEndpointID === j.ManagedByProjectEndpointID);
      const pbStillExists = pbRows.some(pb => String(pb.BrandUID).toLowerCase() === String(j.BrandUID).toLowerCase());
      const reason =
          (!peStillExists && !pbStillExists) ? 'both'
        : (!peStillExists)                   ? 'pe-removed'
        : (!pbStillExists)                   ? 'pb-removed'
        :                                      'unknown';

      if (!dryRun) {
        await tx.request()
          .input('jobID', sql.Int, j.JobID)
          .query(`
            UPDATE admin.Jobs
            SET IsActive                   = 0,
                ManagedByProjectID         = NULL,
                ManagedByProjectEndpointID = NULL,
                UpdatedAt                  = SYSUTCDATETIME()
            WHERE JobID = @jobID
          `);
      }
      result.orphaned.push({ jobID: j.JobID, reason });
    }

    if (tx) await tx.commit();
  } catch (e) {
    if (tx) {
      try { await tx.rollback(); } catch (_) { /* swallow rollback failure */ }
    }
    throw e;
  }

  return result;
}

/**
 * Reconcile every active project. Used by a periodic sweeper / boot.
 */
async function syncAllActiveProjects({ dryRun = false, triggeredBy = 'sync-all' } = {}) {
  const pool = await getPool();
  const r = await pool.request().query(`
    SELECT ProjectID FROM admin.Projects WHERE IsActive = 1 ORDER BY SortOrder, Name
  `);
  const results = [];
  for (const row of r.recordset) {
    try {
      results.push(await syncProject({ projectID: row.ProjectID, dryRun, triggeredBy }));
    } catch (e) {
      results.push({ projectID: row.ProjectID, error: e.message, dryRun, triggeredBy });
    }
  }
  return results;
}

module.exports = { syncProject, syncAllActiveProjects };
