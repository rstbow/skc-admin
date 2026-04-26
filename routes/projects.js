/**
 * Projects API — Folder System v1 (read-only).
 *
 * A Project is a standing folder that groups Endpoints + Brands. The
 * materialization rule guarantees one admin.Jobs row per
 * (ProjectEndpoint × ProjectBrand) tuple. "Project wins" — edits on the
 * Project cascade into all member Jobs at sync time.
 *
 * Distinct from admin.JobBundles (one-shot recipes). Both work alongside.
 *
 * v1 endpoints (read-only — editing comes in v2):
 *   GET /api/projects                 list active Projects with counts
 *   GET /api/projects/:projectUID     detail (Project + endpoints + brands + materialized jobs)
 *
 * Auth: JWT only. Projects are admin-internal — app2 doesn't read them.
 *
 * Backed by migration 037_projects.sql (admin.Projects, admin.ProjectEndpoints,
 * admin.ProjectBrands + ManagedByProject* columns on admin.Jobs).
 */
const express = require('express');
const { sql, getPool } = require('../config/db');
const { requireAuth } = require('../middleware/auth');

const router = express.Router();
router.use(requireAuth);

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

    res.json({
      project,
      endpoints: endpointsQ.recordset,
      brands:    brandsQ.recordset,
      jobs:      jobsQ.recordset,
    });
  } catch (e) {
    console.error('[projects/detail]', e);
    res.status(500).json({ error: e.message || 'Failed to load project' });
  }
});

module.exports = router;
