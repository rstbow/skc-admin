/**
 * Projects API — Folder System v1 (read) + v2 (CRUD + membership + sync).
 *
 * A Project is a standing folder that groups Endpoints + Brands. The
 * materialization rule guarantees one admin.Jobs row per
 * (ProjectEndpoint × ProjectBrand) tuple. "Project wins" — edits on the
 * Project cascade into all member Jobs at sync time.
 *
 * Distinct from admin.JobBundles (one-shot recipes). Both work alongside.
 *
 * Endpoints:
 *   GET    /api/projects                                          list active Projects with counts
 *   GET    /api/projects/:projectUID                              detail (Project + endpoints + brands + materialized jobs)
 *   POST   /api/projects                                          create a new Project
 *   PATCH  /api/projects/:projectUID                              update Project header (Name, IsActive, etc.)
 *   DELETE /api/projects/:projectUID                              delete Project (FK-cascades PE/PB; sync orphans Jobs)
 *
 *   POST   /api/projects/:projectUID/endpoints                    add an Endpoint to the Project
 *   PATCH  /api/projects/:projectUID/endpoints/:projectEndpointID update PE (cron, tz, params, priority, IsActive)
 *   DELETE /api/projects/:projectUID/endpoints/:projectEndpointID remove Endpoint from Project
 *
 *   POST   /api/projects/:projectUID/brands                       add a Brand to the Project
 *   DELETE /api/projects/:projectUID/brands/:projectBrandID       remove Brand from Project
 *
 *   POST   /api/projects/:projectUID/sync                         force a sync pass (debug / manual reconcile)
 *
 * Mutating routes call syncProject() after the change so admin.Jobs
 * materialization stays consistent ("Project wins" cascade).
 *
 * Auth: JWT only. Projects are admin-internal — app2 doesn't read them.
 *
 * Backed by migrations 037_projects.sql (schema) + 038_projects_retrofit.sql
 * (initial backfill of existing Jobs into "Amazon Daily").
 */
const express = require('express');
const { sql, getPool } = require('../config/db');
const { requireAuth } = require('../middleware/auth');
const { syncProject } = require('../lib/projectSync');

const router = express.Router();
router.use(requireAuth);

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/* ---------- Helpers ---------- */

async function getProjectIDByUID(uid) {
  if (!UUID_RE.test(uid)) return { error: { status: 400, message: 'Invalid projectUID format' } };
  const pool = await getPool();
  const r = await pool.request()
    .input('u', sql.UniqueIdentifier, uid)
    .query('SELECT ProjectID, Name FROM admin.Projects WHERE ProjectUID = @u');
  if (!r.recordset.length) return { error: { status: 404, message: 'Project not found' } };
  return { projectID: r.recordset[0].ProjectID, projectName: r.recordset[0].Name };
}

function asInt(v, fallback = null) {
  if (v == null || v === '') return fallback;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : fallback;
}

/* ---------- LIST ----------
   Returns active Projects with aggregate counts. Used by the folder-tree
   left rail in /jobs.html. */
router.get('/', async (_req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request().query(`
      SELECT
        p.ProjectID,
        p.ProjectUID,
        p.Name,
        p.DisplayName,
        p.ConnectorScope,
        p.Description,
        p.SortOrder,
        p.IsActive,
        p.CreatedAt,
        p.UpdatedAt,
        (SELECT COUNT(*) FROM admin.ProjectEndpoints pe
          WHERE pe.ProjectID = p.ProjectID AND pe.IsActive = 1)         AS EndpointCount,
        (SELECT COUNT(*) FROM admin.ProjectBrands   pb
          WHERE pb.ProjectID = p.ProjectID AND pb.IsActive = 1)         AS BrandCount,
        (SELECT COUNT(*) FROM admin.Jobs j
          WHERE j.ManagedByProjectID = p.ProjectID)                     AS ManagedJobCount,
        (SELECT COUNT(*) FROM admin.Jobs j
          WHERE j.ManagedByProjectID = p.ProjectID AND j.IsActive = 1)  AS ActiveManagedJobCount
      FROM admin.Projects p
      WHERE p.IsActive = 1
      ORDER BY p.SortOrder ASC, p.Name ASC
    `);
    res.json({ projects: r.recordset });
  } catch (e) {
    console.error('[projects/list]', e);
    res.status(500).json({ error: e.message || 'Failed to load projects' });
  }
});

/* ---------- DETAIL ----------
   Full picture for one Project: header + member endpoints + member brands +
   materialized jobs (with last-run summary).

   Lookup is by UID (UNIQUEIDENTIFIER) — matches the public-identifier pattern
   used elsewhere in this API. */
router.get('/:projectUID', async (req, res) => {
  try {
    const projectUID = req.params.projectUID;

    // Reject obviously bad UIDs early — keeps SQL Server from a cryptic
    // "Conversion failed" error if someone hits /api/projects/garbage.
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(projectUID)) {
      return res.status(400).json({ error: 'Invalid projectUID format' });
    }

    const pool = await getPool();

    /* Header */
    const headerQ = await pool.request()
      .input('uid', sql.UniqueIdentifier, projectUID)
      .query(`
        SELECT ProjectID, ProjectUID, Name, DisplayName, ConnectorScope,
               Description, SortOrder, IsActive, CreatedAt, UpdatedAt
        FROM admin.Projects
        WHERE ProjectUID = @uid
      `);
    if (!headerQ.recordset.length) {
      return res.status(404).json({ error: 'Project not found' });
    }
    const project = headerQ.recordset[0];

    /* Member endpoints */
    const endpointsQ = await pool.request()
      .input('pid', sql.Int, project.ProjectID)
      .query(`
        SELECT pe.ProjectEndpointID, pe.EndpointID, pe.JobType,
               pe.CronExpression, pe.TimezoneIANA, pe.Params,
               pe.Priority, pe.IsActive, pe.SortOrder,
               pe.CreatedAt, pe.UpdatedAt,
               e.Name        AS EndpointName,
               e.DisplayName AS EndpointDisplay,
               c.ConnectorID, c.Name AS ConnectorName, c.DisplayName AS ConnectorDisplay
        FROM admin.ProjectEndpoints pe
        JOIN admin.Endpoints  e ON e.EndpointID  = pe.EndpointID
        JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
        WHERE pe.ProjectID = @pid
        ORDER BY pe.SortOrder ASC, e.Name ASC
      `);

    /* Member brands */
    const brandsQ = await pool.request()
      .input('pid', sql.Int, project.ProjectID)
      .query(`
        SELECT pb.ProjectBrandID, pb.BrandUID, pb.IsActive, pb.JoinedAt,
               b.BrandName,
               b.Category   AS BrandCategory
        FROM admin.ProjectBrands pb
        LEFT JOIN admin.Brands b ON b.BrandUID = pb.BrandUID
        WHERE pb.ProjectID = @pid
        ORDER BY b.BrandName ASC
      `);

    /* Materialized jobs (one row per (ProjectEndpoint × ProjectBrand)).
       Includes last-run status for the at-a-glance UI. */
    const jobsQ = await pool.request()
      .input('pid', sql.Int, project.ProjectID)
      .query(`
        SELECT j.JobID, j.JobUID, j.Name AS JobName, j.JobType,
               j.CronExpression, j.TimezoneIANA, j.IsActive, j.Priority,
               j.NextRunAt, j.LastRunAt, j.LastRunStatus,
               j.ConsecutiveFailures,
               j.ManagedByProjectID, j.ManagedByProjectEndpointID,
               j.BrandUID, b.BrandName,
               e.EndpointID, e.Name AS EndpointName, e.DisplayName AS EndpointDisplay,
               c.ConnectorID, c.Name AS ConnectorName
        FROM admin.Jobs j
        JOIN admin.Endpoints  e ON e.EndpointID  = j.EndpointID
        JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
        LEFT JOIN admin.Brands b ON b.BrandUID = j.BrandUID
        WHERE j.ManagedByProjectID = @pid
        ORDER BY e.Name ASC, b.BrandName ASC
      `);

    /* Drift candidates: Jobs that share (EndpointID, JobType) with one of this
       project's PEs but are NOT project-managed. These are jobs that *could*
       be project members but currently use a different cron (or no project
       affiliation). Surface them in the UI so operators can see + reconcile. */
    const driftQ = await pool.request()
      .input('pid', sql.Int, project.ProjectID)
      .query(`
        SELECT j.JobID, j.JobUID, j.Name AS JobName, j.JobType,
               j.CronExpression AS JobCron, j.TimezoneIANA AS JobTZ,
               j.IsActive, j.LastRunAt, j.LastRunStatus,
               j.BrandUID, b.BrandName,
               e.EndpointID, e.Name AS EndpointName, e.DisplayName AS EndpointDisplay,
               pe.ProjectEndpointID, pe.CronExpression AS ProjectCron, pe.TimezoneIANA AS ProjectTZ,
               CASE
                 WHEN j.ManagedByProjectID IS NOT NULL AND j.ManagedByProjectID <> @pid
                   THEN 'managed-by-other-project'
                 WHEN ISNULL(j.CronExpression,'') <> ISNULL(pe.CronExpression,'')
                   THEN 'cron-divergence'
                 ELSE 'unmanaged-but-eligible'
               END AS DriftReason
        FROM admin.Jobs j
        JOIN admin.Endpoints e ON e.EndpointID = j.EndpointID
        LEFT JOIN admin.Brands b ON b.BrandUID = j.BrandUID
        JOIN admin.ProjectEndpoints pe
          ON pe.ProjectID  = @pid
         AND pe.EndpointID = j.EndpointID
         AND pe.JobType    = j.JobType
        WHERE (j.ManagedByProjectID IS NULL OR j.ManagedByProjectID <> @pid)
        ORDER BY e.Name ASC, b.BrandName ASC
      `);

    res.json({
      project,
      endpoints:       endpointsQ.recordset,
      brands:          brandsQ.recordset,
      jobs:            jobsQ.recordset,
      driftCandidates: driftQ.recordset,
    });
  } catch (e) {
    console.error('[projects/detail]', e);
    res.status(500).json({ error: e.message || 'Failed to load project' });
  }
});

/* ============================================================================
   v2 — CRUD + membership + sync
   ============================================================================ */

/* ---------- POST /api/projects ----------
   Create a new Project. Returns the created header. No PE / PB members yet —
   add via the sub-resource endpoints below. */
router.post('/', async (req, res) => {
  try {
    const { name, displayName, connectorScope, description, sortOrder, isActive } = req.body || {};
    if (!name || typeof name !== 'string' || !name.trim()) {
      return res.status(400).json({ error: 'name is required (non-empty string)' });
    }

    const pool = await getPool();
    const userID = asInt(req.user?.userID);

    // Friendly duplicate-name preflight (the UQ constraint would also catch this,
    // but a 409 with a clear message is nicer than a 500 with a SQL error).
    const dup = await pool.request()
      .input('n', sql.NVarChar(100), name.trim())
      .query('SELECT 1 FROM admin.Projects WHERE Name = @n');
    if (dup.recordset.length) {
      return res.status(409).json({ error: 'A project with that Name already exists.' });
    }

    const r = await pool.request()
      .input('name',           sql.NVarChar(100),     name.trim())
      .input('displayName',    sql.NVarChar(150),     displayName || null)
      .input('connectorScope', sql.NVarChar(50),      connectorScope || null)
      .input('description',    sql.NVarChar(sql.MAX), description || null)
      .input('sortOrder',      sql.Int,               asInt(sortOrder, 100))
      .input('isActive',       sql.Bit,               isActive === false ? 0 : 1)
      .input('createdBy',      sql.Int,               userID)
      .query(`
        INSERT INTO admin.Projects
          (Name, DisplayName, ConnectorScope, Description, SortOrder, IsActive, CreatedBy, UpdatedBy)
        OUTPUT INSERTED.*
        VALUES (@name, @displayName, @connectorScope, @description, @sortOrder, @isActive, @createdBy, @createdBy)
      `);

    res.status(201).json({ project: r.recordset[0] });
  } catch (e) {
    console.error('[projects/create]', e);
    res.status(500).json({ error: e.message || 'Failed to create project' });
  }
});

/* ---------- PATCH /api/projects/:projectUID ----------
   Update Project header. Sync runs after to propagate IsActive changes. */
router.patch('/:projectUID', async (req, res) => {
  try {
    const lookup = await getProjectIDByUID(req.params.projectUID);
    if (lookup.error) return res.status(lookup.error.status).json({ error: lookup.error.message });
    const { projectID } = lookup;

    const { name, displayName, connectorScope, description, sortOrder, isActive } = req.body || {};

    // Build dynamic SET list
    const sets = [];
    const reqQ = (await getPool()).request().input('p', sql.Int, projectID);
    if (name !== undefined) {
      if (!name || !String(name).trim()) return res.status(400).json({ error: 'name cannot be empty' });
      sets.push('Name = @name');
      reqQ.input('name', sql.NVarChar(100), String(name).trim());
    }
    if (displayName !== undefined)    { sets.push('DisplayName = @displayName');       reqQ.input('displayName',    sql.NVarChar(150),     displayName || null); }
    if (connectorScope !== undefined) { sets.push('ConnectorScope = @connectorScope'); reqQ.input('connectorScope', sql.NVarChar(50),      connectorScope || null); }
    if (description !== undefined)    { sets.push('Description = @description');       reqQ.input('description',    sql.NVarChar(sql.MAX), description || null); }
    if (sortOrder !== undefined)      { sets.push('SortOrder = @sortOrder');           reqQ.input('sortOrder',      sql.Int,               asInt(sortOrder, 100)); }
    if (isActive !== undefined)       { sets.push('IsActive = @isActive');             reqQ.input('isActive',       sql.Bit,               isActive ? 1 : 0); }

    if (!sets.length) return res.status(400).json({ error: 'No updatable fields provided' });

    sets.push('UpdatedAt = SYSUTCDATETIME()');
    if (req.user?.userID) {
      sets.push('UpdatedBy = @updatedBy');
      reqQ.input('updatedBy', sql.Int, asInt(req.user.userID));
    }

    const r = await reqQ.query(`
      UPDATE admin.Projects SET ${sets.join(', ')}
      OUTPUT INSERTED.*
      WHERE ProjectID = @p
    `);

    // Sync — IsActive changes propagate to materialized Jobs.
    const sync = await syncProject({ projectID, triggeredBy: 'project-patch' });

    res.json({ project: r.recordset[0], sync });
  } catch (e) {
    console.error('[projects/update]', e);
    res.status(500).json({ error: e.message || 'Failed to update project' });
  }
});

/* ---------- DELETE /api/projects/:projectUID ----------
   Hard-delete the Project. FK CASCADE removes ProjectEndpoints + ProjectBrands.
   Sync orphans the previously-managed Jobs (soft-delete = pause + un-manage). */
router.delete('/:projectUID', async (req, res) => {
  try {
    const lookup = await getProjectIDByUID(req.params.projectUID);
    if (lookup.error) return res.status(lookup.error.status).json({ error: lookup.error.message });
    const { projectID, projectName } = lookup;

    const pool = await getPool();

    // Capture the set of currently-managed Jobs BEFORE the cascade removes
    // the parent rows; we'll soft-delete these after the project is gone.
    const managedR = await pool.request()
      .input('p', sql.Int, projectID)
      .query('SELECT JobID FROM admin.Jobs WHERE ManagedByProjectID = @p');
    const managedJobIDs = managedR.recordset.map(r => r.JobID);

    // Delete the project. CASCADE handles ProjectEndpoints + ProjectBrands.
    await pool.request()
      .input('p', sql.Int, projectID)
      .query('DELETE FROM admin.Projects WHERE ProjectID = @p');

    // Soft-delete previously-managed Jobs: pause + un-manage. Run history preserved.
    let orphanedCount = 0;
    if (managedJobIDs.length) {
      const orphanReq = pool.request();
      const placeholders = managedJobIDs.map((id, i) => {
        orphanReq.input('j' + i, sql.Int, id);
        return '@j' + i;
      }).join(',');
      const upd = await orphanReq.query(`
        UPDATE admin.Jobs
        SET IsActive = 0,
            ManagedByProjectID = NULL,
            ManagedByProjectEndpointID = NULL,
            UpdatedAt = SYSUTCDATETIME()
        WHERE JobID IN (${placeholders})
      `);
      orphanedCount = upd.rowsAffected[0] || 0;
    }

    res.json({
      deleted: { projectID, projectName },
      orphanedJobs: orphanedCount,
    });
  } catch (e) {
    console.error('[projects/delete]', e);
    res.status(500).json({ error: e.message || 'Failed to delete project' });
  }
});

/* ---------- POST /api/projects/:projectUID/endpoints ----------
   Add an Endpoint to the project. Body: { endpointID, jobType, cronExpression,
   timezoneIANA, params, priority, isActive }. Sync materializes Jobs for every
   active member brand × this new PE. */
router.post('/:projectUID/endpoints', async (req, res) => {
  try {
    const lookup = await getProjectIDByUID(req.params.projectUID);
    if (lookup.error) return res.status(lookup.error.status).json({ error: lookup.error.message });
    const { projectID } = lookup;

    const { endpointID, jobType, cronExpression, timezoneIANA, params, priority, isActive, sortOrder } = req.body || {};
    const epID = asInt(endpointID);
    if (!epID) return res.status(400).json({ error: 'endpointID (int) is required' });
    const jt = (jobType || 'INGEST').toUpperCase();
    if (!['INGEST','ROLLUP','MIGRATE','BACKFILL'].includes(jt)) {
      return res.status(400).json({ error: 'jobType must be INGEST | ROLLUP | MIGRATE | BACKFILL' });
    }

    const pool = await getPool();

    // Verify endpoint exists.
    const eR = await pool.request().input('e', sql.Int, epID).query('SELECT 1 FROM admin.Endpoints WHERE EndpointID = @e');
    if (!eR.recordset.length) return res.status(404).json({ error: 'Endpoint not found, EndpointID=' + epID });

    // Friendly duplicate preflight (UQ_PrjEp_Project_Endpoint_Type would also catch this).
    const dup = await pool.request()
      .input('p', sql.Int,          projectID)
      .input('e', sql.Int,          epID)
      .input('t', sql.NVarChar(20), jt)
      .query('SELECT 1 FROM admin.ProjectEndpoints WHERE ProjectID = @p AND EndpointID = @e AND JobType = @t');
    if (dup.recordset.length) {
      return res.status(409).json({ error: 'This endpoint+jobType is already attached to the project.' });
    }

    const ins = await pool.request()
      .input('p',       sql.Int,               projectID)
      .input('e',       sql.Int,               epID)
      .input('t',       sql.NVarChar(20),      jt)
      .input('cron',    sql.NVarChar(50),      cronExpression || null)
      .input('tz',      sql.NVarChar(50),      timezoneIANA   || 'America/Chicago')
      .input('params',  sql.NVarChar(sql.MAX), params         || null)
      .input('prio',    sql.Int,               asInt(priority,  50))
      .input('act',     sql.Bit,               isActive === false ? 0 : 1)
      .input('sort',    sql.Int,               asInt(sortOrder, 100))
      .query(`
        INSERT INTO admin.ProjectEndpoints
          (ProjectID, EndpointID, JobType, CronExpression, TimezoneIANA, Params,
           Priority, IsActive, SortOrder)
        OUTPUT INSERTED.*
        VALUES (@p, @e, @t, @cron, @tz, @params, @prio, @act, @sort)
      `);

    const sync = await syncProject({ projectID, triggeredBy: 'pe-add' });

    res.status(201).json({ projectEndpoint: ins.recordset[0], sync });
  } catch (e) {
    console.error('[projects/endpoints/add]', e);
    res.status(500).json({ error: e.message || 'Failed to add endpoint to project' });
  }
});

/* ---------- PATCH /api/projects/:projectUID/endpoints/:projectEndpointID ----------
   Update PE settings. Sync cascades the change ("Project wins") to all
   materialized Jobs for this PE. */
router.patch('/:projectUID/endpoints/:projectEndpointID', async (req, res) => {
  try {
    const lookup = await getProjectIDByUID(req.params.projectUID);
    if (lookup.error) return res.status(lookup.error.status).json({ error: lookup.error.message });
    const { projectID } = lookup;

    const peID = asInt(req.params.projectEndpointID);
    if (!peID) return res.status(400).json({ error: 'Invalid projectEndpointID' });

    const { jobType, cronExpression, timezoneIANA, params, priority, isActive, sortOrder } = req.body || {};

    const sets = [];
    const reqQ = (await getPool()).request()
      .input('peID', sql.Int, peID)
      .input('p',    sql.Int, projectID);

    if (jobType !== undefined) {
      const jt = String(jobType).toUpperCase();
      if (!['INGEST','ROLLUP','MIGRATE','BACKFILL'].includes(jt)) {
        return res.status(400).json({ error: 'jobType must be INGEST | ROLLUP | MIGRATE | BACKFILL' });
      }
      sets.push('JobType = @jobType'); reqQ.input('jobType', sql.NVarChar(20), jt);
    }
    if (cronExpression !== undefined) { sets.push('CronExpression = @cron'); reqQ.input('cron', sql.NVarChar(50),      cronExpression || null); }
    if (timezoneIANA   !== undefined) { sets.push('TimezoneIANA   = @tz');   reqQ.input('tz',   sql.NVarChar(50),      timezoneIANA   || 'America/Chicago'); }
    if (params         !== undefined) { sets.push('Params         = @params'); reqQ.input('params', sql.NVarChar(sql.MAX), params       || null); }
    if (priority       !== undefined) { sets.push('Priority       = @prio'); reqQ.input('prio', sql.Int,               asInt(priority, 50)); }
    if (isActive       !== undefined) { sets.push('IsActive       = @act');  reqQ.input('act',  sql.Bit,               isActive ? 1 : 0); }
    if (sortOrder      !== undefined) { sets.push('SortOrder      = @sort'); reqQ.input('sort', sql.Int,               asInt(sortOrder, 100)); }

    if (!sets.length) return res.status(400).json({ error: 'No updatable fields provided' });

    sets.push('UpdatedAt = SYSUTCDATETIME()');

    const upd = await reqQ.query(`
      UPDATE admin.ProjectEndpoints
      SET ${sets.join(', ')}
      OUTPUT INSERTED.*
      WHERE ProjectEndpointID = @peID AND ProjectID = @p
    `);
    if (!upd.recordset.length) return res.status(404).json({ error: 'ProjectEndpoint not found in this project' });

    const sync = await syncProject({ projectID, triggeredBy: 'pe-patch' });

    res.json({ projectEndpoint: upd.recordset[0], sync });
  } catch (e) {
    console.error('[projects/endpoints/patch]', e);
    res.status(500).json({ error: e.message || 'Failed to update project endpoint' });
  }
});

/* ---------- DELETE /api/projects/:projectUID/endpoints/:projectEndpointID ---------- */
router.delete('/:projectUID/endpoints/:projectEndpointID', async (req, res) => {
  try {
    const lookup = await getProjectIDByUID(req.params.projectUID);
    if (lookup.error) return res.status(lookup.error.status).json({ error: lookup.error.message });
    const { projectID } = lookup;

    const peID = asInt(req.params.projectEndpointID);
    if (!peID) return res.status(400).json({ error: 'Invalid projectEndpointID' });

    const pool = await getPool();
    const r = await pool.request()
      .input('peID', sql.Int, peID)
      .input('p',    sql.Int, projectID)
      .query('DELETE FROM admin.ProjectEndpoints WHERE ProjectEndpointID = @peID AND ProjectID = @p');

    if (!r.rowsAffected[0]) return res.status(404).json({ error: 'ProjectEndpoint not found in this project' });

    // Sync orphans the previously-managed Jobs for this PE (soft-delete = pause + un-manage).
    const sync = await syncProject({ projectID, triggeredBy: 'pe-remove' });

    res.json({ deleted: { projectEndpointID: peID }, sync });
  } catch (e) {
    console.error('[projects/endpoints/delete]', e);
    res.status(500).json({ error: e.message || 'Failed to remove project endpoint' });
  }
});

/* ---------- POST /api/projects/:projectUID/brands ----------
   Add a Brand to the project. Body: { brandUID }.
   Sync materializes Jobs for every active PE × this new brand. */
router.post('/:projectUID/brands', async (req, res) => {
  try {
    const lookup = await getProjectIDByUID(req.params.projectUID);
    if (lookup.error) return res.status(lookup.error.status).json({ error: lookup.error.message });
    const { projectID } = lookup;

    const { brandUID, isActive } = req.body || {};
    if (!brandUID || !UUID_RE.test(brandUID)) {
      return res.status(400).json({ error: 'brandUID (UUID) is required' });
    }

    const pool = await getPool();

    // Verify brand exists (FK would also catch this, but better error message here).
    const bR = await pool.request().input('b', sql.UniqueIdentifier, brandUID).query('SELECT 1 FROM admin.Brands WHERE BrandUID = @b');
    if (!bR.recordset.length) return res.status(404).json({ error: 'Brand not found' });

    // Friendly duplicate preflight.
    const dup = await pool.request()
      .input('p', sql.Int,              projectID)
      .input('b', sql.UniqueIdentifier, brandUID)
      .query('SELECT 1 FROM admin.ProjectBrands WHERE ProjectID = @p AND BrandUID = @b');
    if (dup.recordset.length) {
      return res.status(409).json({ error: 'Brand is already a member of this project.' });
    }

    const userID = asInt(req.user?.userID);
    const ins = await pool.request()
      .input('p',         sql.Int,              projectID)
      .input('b',         sql.UniqueIdentifier, brandUID)
      .input('act',       sql.Bit,              isActive === false ? 0 : 1)
      .input('createdBy', sql.Int,              userID)
      .query(`
        INSERT INTO admin.ProjectBrands (ProjectID, BrandUID, IsActive, CreatedBy)
        OUTPUT INSERTED.*
        VALUES (@p, @b, @act, @createdBy)
      `);

    const sync = await syncProject({ projectID, triggeredBy: 'pb-add' });

    res.status(201).json({ projectBrand: ins.recordset[0], sync });
  } catch (e) {
    console.error('[projects/brands/add]', e);
    res.status(500).json({ error: e.message || 'Failed to add brand to project' });
  }
});

/* ---------- DELETE /api/projects/:projectUID/brands/:projectBrandID ---------- */
router.delete('/:projectUID/brands/:projectBrandID', async (req, res) => {
  try {
    const lookup = await getProjectIDByUID(req.params.projectUID);
    if (lookup.error) return res.status(lookup.error.status).json({ error: lookup.error.message });
    const { projectID } = lookup;

    const pbID = asInt(req.params.projectBrandID);
    if (!pbID) return res.status(400).json({ error: 'Invalid projectBrandID' });

    const pool = await getPool();
    const r = await pool.request()
      .input('pbID', sql.Int, pbID)
      .input('p',    sql.Int, projectID)
      .query('DELETE FROM admin.ProjectBrands WHERE ProjectBrandID = @pbID AND ProjectID = @p');

    if (!r.rowsAffected[0]) return res.status(404).json({ error: 'ProjectBrand not found in this project' });

    const sync = await syncProject({ projectID, triggeredBy: 'pb-remove' });

    res.json({ deleted: { projectBrandID: pbID }, sync });
  } catch (e) {
    console.error('[projects/brands/delete]', e);
    res.status(500).json({ error: e.message || 'Failed to remove brand from project' });
  }
});

/* ---------- POST /api/projects/:projectUID/sync ----------
   Force a sync pass for one project. Useful for debugging / after a manual
   data import / on operator demand. ?dryRun=1 returns the plan without writes. */
router.post('/:projectUID/sync', async (req, res) => {
  try {
    const lookup = await getProjectIDByUID(req.params.projectUID);
    if (lookup.error) return res.status(lookup.error.status).json({ error: lookup.error.message });
    const { projectID } = lookup;

    const dryRun = req.query.dryRun === '1' || req.body?.dryRun === true;
    const sync = await syncProject({ projectID, dryRun, triggeredBy: 'manual-sync' });
    res.json({ sync });
  } catch (e) {
    console.error('[projects/sync]', e);
    res.status(500).json({ error: e.message || 'Failed to sync project' });
  }
});

module.exports = router;
