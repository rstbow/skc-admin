const express = require('express');
const { sql, getPool } = require('../config/db');
const { requireAuth } = require('../middleware/auth');
const { logAction, reqMeta } = require('../db/queries/audit');

const router = express.Router();
router.use(requireAuth);

/* ---------- GET /api/endpoints — list, optionally filtered ---------- */
router.get('/', async (req, res) => {
  try {
    const pool = await getPool();
    const request = pool.request();
    let where = '';
    if (req.query.connectorUID) {
      request.input('cuid', sql.UniqueIdentifier, req.query.connectorUID);
      where = 'WHERE c.ConnectorUID = @cuid';
    }
    const r = await request.query(`
      SELECT e.EndpointID, e.EndpointUID, e.Name, e.DisplayName, e.Description,
             e.EndpointType, e.HttpMethod, e.Path, e.TargetSchema, e.TargetTable,
             e.NaturalKeyColumns, e.RateLimitWeight, e.Version, e.IsActive,
             e.CreatedAt, e.UpdatedAt,
             c.ConnectorUID, c.Name AS ConnectorName, c.DisplayName AS ConnectorDisplay
      FROM admin.Endpoints e
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
      ${where}
      ORDER BY c.DisplayName, e.IsActive DESC, e.Name
    `);
    res.json({ endpoints: r.recordset });
  } catch (e) {
    console.error('[endpoints/list]', e);
    res.status(500).json({ error: 'Failed to load endpoints' });
  }
});

/* ---------- GET /api/endpoints/:uid ---------- */
router.get('/:uid', async (req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request()
      .input('uid', sql.UniqueIdentifier, req.params.uid)
      .query(`
        SELECT e.*, c.ConnectorUID, c.Name AS ConnectorName, c.DisplayName AS ConnectorDisplay
        FROM admin.Endpoints e
        JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
        WHERE e.EndpointUID = @uid
      `);
    if (!r.recordset.length) return res.status(404).json({ error: 'Not found' });
    res.json({ endpoint: r.recordset[0] });
  } catch (e) {
    console.error('[endpoints/get]', e);
    res.status(500).json({ error: 'Failed to load endpoint' });
  }
});

/* ---------- POST /api/endpoints — create ---------- */
router.post('/', async (req, res) => {
  try {
    const b = req.body || {};
    const required = ['connectorUID', 'name', 'displayName', 'endpointType', 'targetTable'];
    for (const k of required) {
      if (!b[k]) return res.status(400).json({ error: 'Missing required field: ' + k });
    }

    const pool = await getPool();

    // Resolve connector
    const cr = await pool.request()
      .input('cuid', sql.UniqueIdentifier, b.connectorUID)
      .query('SELECT ConnectorID FROM admin.Connectors WHERE ConnectorUID = @cuid');
    if (!cr.recordset.length) return res.status(400).json({ error: 'Unknown connectorUID' });
    const connectorID = cr.recordset[0].ConnectorID;

    const r = await pool.request()
      .input('connectorID', sql.Int, connectorID)
      .input('name', sql.NVarChar(100), b.name.trim())
      .input('displayName', sql.NVarChar(200), b.displayName.trim())
      .input('description', sql.NVarChar(sql.MAX), b.description || null)
      .input('endpointType', sql.NVarChar(30), b.endpointType)
      .input('httpMethod', sql.NVarChar(10), b.httpMethod || null)
      .input('path', sql.NVarChar(500), b.path || null)
      .input('paramsTemplate', sql.NVarChar(sql.MAX), b.paramsTemplate || null)
      .input('paginationStrategy', sql.NVarChar(30), b.paginationStrategy || null)
      .input('pollIntervalSec', sql.Int, b.pollIntervalSec || null)
      .input('pollMaxAttempts', sql.Int, b.pollMaxAttempts || null)
      .input('targetSchema', sql.NVarChar(50), b.targetSchema || 'raw')
      .input('targetTable', sql.NVarChar(128), b.targetTable)
      .input('naturalKeyColumns', sql.NVarChar(500), b.naturalKeyColumns || null)
      .input('transformProc', sql.NVarChar(200), b.transformProc || null)
      .input('rateLimitWeight', sql.Int, b.rateLimitWeight || 1)
      .input('notes', sql.NVarChar(sql.MAX), b.notes || null)
      .input('createdBy', sql.Int, req.user.userID)
      .input('updatedBy', sql.Int, req.user.userID)
      .query(`
        INSERT INTO admin.Endpoints (ConnectorID, Name, DisplayName, Description, EndpointType,
                                     HttpMethod, Path, ParamsTemplate, PaginationStrategy,
                                     PollIntervalSec, PollMaxAttempts, TargetSchema, TargetTable,
                                     NaturalKeyColumns, TransformProc, RateLimitWeight, Notes,
                                     CreatedBy, UpdatedBy)
        OUTPUT INSERTED.EndpointID, INSERTED.EndpointUID, INSERTED.Name, INSERTED.DisplayName
        VALUES (@connectorID, @name, @displayName, @description, @endpointType,
                @httpMethod, @path, @paramsTemplate, @paginationStrategy,
                @pollIntervalSec, @pollMaxAttempts, @targetSchema, @targetTable,
                @naturalKeyColumns, @transformProc, @rateLimitWeight, @notes,
                @createdBy, @updatedBy)
      `);

    const created = r.recordset[0];
    await logAction({
      userID: req.user.userID,
      action: 'ENDPOINT_CREATE',
      entityType: 'Endpoint',
      entityID: created.EndpointUID,
      details: { name: created.Name },
      ...reqMeta(req),
    });

    res.status(201).json({ endpoint: created });
  } catch (e) {
    console.error('[endpoints/create]', e);
    if (e.number === 2627 || e.number === 2601) {
      return res.status(409).json({ error: 'An endpoint with that name already exists for this connector' });
    }
    res.status(500).json({ error: 'Failed to create endpoint' });
  }
});

/* ---------- PUT /api/endpoints/:uid ---------- */
router.put('/:uid', async (req, res) => {
  try {
    const b = req.body || {};
    const pool = await getPool();
    const r = await pool.request()
      .input('uid', sql.UniqueIdentifier, req.params.uid)
      .input('displayName', sql.NVarChar(200), b.displayName)
      .input('description', sql.NVarChar(sql.MAX), b.description || null)
      .input('endpointType', sql.NVarChar(30), b.endpointType)
      .input('httpMethod', sql.NVarChar(10), b.httpMethod || null)
      .input('path', sql.NVarChar(500), b.path || null)
      .input('paramsTemplate', sql.NVarChar(sql.MAX), b.paramsTemplate || null)
      .input('paginationStrategy', sql.NVarChar(30), b.paginationStrategy || null)
      .input('pollIntervalSec', sql.Int, b.pollIntervalSec || null)
      .input('pollMaxAttempts', sql.Int, b.pollMaxAttempts || null)
      .input('targetSchema', sql.NVarChar(50), b.targetSchema || 'raw')
      .input('targetTable', sql.NVarChar(128), b.targetTable)
      .input('naturalKeyColumns', sql.NVarChar(500), b.naturalKeyColumns || null)
      .input('transformProc', sql.NVarChar(200), b.transformProc || null)
      .input('rateLimitWeight', sql.Int, b.rateLimitWeight || 1)
      .input('isActive', sql.Bit, b.isActive == null ? 1 : (b.isActive ? 1 : 0))
      .input('notes', sql.NVarChar(sql.MAX), b.notes || null)
      .input('updatedBy', sql.Int, req.user.userID)
      .query(`
        UPDATE admin.Endpoints
        SET DisplayName        = @displayName,
            Description        = @description,
            EndpointType       = @endpointType,
            HttpMethod         = @httpMethod,
            Path               = @path,
            ParamsTemplate     = @paramsTemplate,
            PaginationStrategy = @paginationStrategy,
            PollIntervalSec    = @pollIntervalSec,
            PollMaxAttempts    = @pollMaxAttempts,
            TargetSchema       = @targetSchema,
            TargetTable        = @targetTable,
            NaturalKeyColumns  = @naturalKeyColumns,
            TransformProc      = @transformProc,
            RateLimitWeight    = @rateLimitWeight,
            IsActive           = @isActive,
            Notes              = @notes,
            UpdatedBy          = @updatedBy,
            UpdatedAt          = SYSUTCDATETIME()
        OUTPUT INSERTED.EndpointUID, INSERTED.Name, INSERTED.DisplayName
        WHERE EndpointUID = @uid
      `);

    if (!r.recordset.length) return res.status(404).json({ error: 'Not found' });

    await logAction({
      userID: req.user.userID,
      action: 'ENDPOINT_UPDATE',
      entityType: 'Endpoint',
      entityID: req.params.uid,
      details: { displayName: b.displayName, isActive: b.isActive },
      ...reqMeta(req),
    });

    res.json({ endpoint: r.recordset[0] });
  } catch (e) {
    console.error('[endpoints/update]', e);
    res.status(500).json({ error: 'Failed to update endpoint' });
  }
});

/* ---------- DELETE /api/endpoints/:uid (soft) ---------- */
router.delete('/:uid', async (req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request()
      .input('uid', sql.UniqueIdentifier, req.params.uid)
      .input('updatedBy', sql.Int, req.user.userID)
      .query(`
        UPDATE admin.Endpoints
        SET IsActive = 0, UpdatedBy = @updatedBy, UpdatedAt = SYSUTCDATETIME()
        WHERE EndpointUID = @uid
      `);

    if (!r.rowsAffected[0]) return res.status(404).json({ error: 'Not found' });

    await logAction({
      userID: req.user.userID,
      action: 'ENDPOINT_DEACTIVATE',
      entityType: 'Endpoint',
      entityID: req.params.uid,
      ...reqMeta(req),
    });

    res.json({ ok: true });
  } catch (e) {
    console.error('[endpoints/delete]', e);
    res.status(500).json({ error: 'Failed to deactivate endpoint' });
  }
});

module.exports = router;
