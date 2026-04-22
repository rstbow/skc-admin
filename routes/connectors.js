const express = require('express');
const { sql, getPool } = require('../config/db');
const { requireAuth } = require('../middleware/auth');
const { logAction, reqMeta } = require('../db/queries/audit');

const router = express.Router();
router.use(requireAuth);

/* ---------- GET /api/connectors — list ---------- */
router.get('/', async (_req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request().query(`
      SELECT c.ConnectorID, c.ConnectorUID, c.Name, c.DisplayName, c.AuthType,
             c.BaseURL, c.DocsURL, c.RunnerType, c.RunnerRef, c.ApiVersion,
             c.IsActive, c.Notes, c.CreatedAt, c.UpdatedAt,
             (SELECT COUNT(*) FROM admin.Endpoints e WHERE e.ConnectorID = c.ConnectorID AND e.IsActive = 1) AS EndpointCount
      FROM admin.Connectors c
      ORDER BY c.IsActive DESC, c.DisplayName
    `);
    res.json({ connectors: r.recordset });
  } catch (e) {
    console.error('[connectors/list]', e);
    res.status(500).json({ error: 'Failed to load connectors' });
  }
});

/* ---------- GET /api/connectors/:uid ---------- */
router.get('/:uid', async (req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request()
      .input('uid', sql.UniqueIdentifier, req.params.uid)
      .query(`
        SELECT ConnectorID, ConnectorUID, Name, DisplayName, AuthType, BaseURL, DocsURL,
               DefaultRateLimitRPM, RunnerType, RunnerRef, ApiVersion, IsActive, Notes,
               CreatedAt, UpdatedAt
        FROM admin.Connectors
        WHERE ConnectorUID = @uid
      `);
    if (!r.recordset.length) return res.status(404).json({ error: 'Not found' });
    res.json({ connector: r.recordset[0] });
  } catch (e) {
    console.error('[connectors/get]', e);
    res.status(500).json({ error: 'Failed to load connector' });
  }
});

/* ---------- POST /api/connectors — create ---------- */
router.post('/', async (req, res) => {
  try {
    const b = req.body || {};
    const required = ['name', 'displayName', 'authType', 'baseURL'];
    for (const k of required) {
      if (!b[k]) return res.status(400).json({ error: 'Missing required field: ' + k });
    }

    const pool = await getPool();
    const r = await pool.request()
      .input('name', sql.NVarChar(50), b.name.toUpperCase().trim())
      .input('displayName', sql.NVarChar(100), b.displayName.trim())
      .input('authType', sql.NVarChar(30), b.authType)
      .input('baseURL', sql.NVarChar(500), b.baseURL.trim())
      .input('docsURL', sql.NVarChar(500), b.docsURL || null)
      .input('runnerType', sql.NVarChar(30), b.runnerType || 'GENERIC')
      .input('runnerRef', sql.NVarChar(200), b.runnerRef || null)
      .input('apiVersion', sql.NVarChar(20), b.apiVersion || null)
      .input('notes', sql.NVarChar(sql.MAX), b.notes || null)
      .input('createdBy', sql.Int, req.user.userID)
      .input('updatedBy', sql.Int, req.user.userID)
      .query(`
        INSERT INTO admin.Connectors (Name, DisplayName, AuthType, BaseURL, DocsURL,
                                      RunnerType, RunnerRef, ApiVersion, Notes, CreatedBy, UpdatedBy)
        OUTPUT INSERTED.ConnectorID, INSERTED.ConnectorUID, INSERTED.Name, INSERTED.DisplayName
        VALUES (@name, @displayName, @authType, @baseURL, @docsURL,
                @runnerType, @runnerRef, @apiVersion, @notes, @createdBy, @updatedBy)
      `);

    const created = r.recordset[0];
    await logAction({
      userID: req.user.userID,
      action: 'CONNECTOR_CREATE',
      entityType: 'Connector',
      entityID: created.ConnectorUID,
      details: { name: created.Name, displayName: created.DisplayName },
      ...reqMeta(req),
    });

    res.status(201).json({ connector: created });
  } catch (e) {
    console.error('[connectors/create]', e);
    if (e.number === 2627 || e.number === 2601) {
      return res.status(409).json({ error: 'A connector with that name already exists' });
    }
    res.status(500).json({ error: 'Failed to create connector' });
  }
});

/* ---------- PUT /api/connectors/:uid — update ---------- */
router.put('/:uid', async (req, res) => {
  try {
    const b = req.body || {};
    const pool = await getPool();
    const r = await pool.request()
      .input('uid', sql.UniqueIdentifier, req.params.uid)
      .input('displayName', sql.NVarChar(100), b.displayName)
      .input('authType', sql.NVarChar(30), b.authType)
      .input('baseURL', sql.NVarChar(500), b.baseURL)
      .input('docsURL', sql.NVarChar(500), b.docsURL || null)
      .input('runnerType', sql.NVarChar(30), b.runnerType || 'GENERIC')
      .input('runnerRef', sql.NVarChar(200), b.runnerRef || null)
      .input('apiVersion', sql.NVarChar(20), b.apiVersion || null)
      .input('isActive', sql.Bit, b.isActive == null ? 1 : (b.isActive ? 1 : 0))
      .input('notes', sql.NVarChar(sql.MAX), b.notes || null)
      .input('updatedBy', sql.Int, req.user.userID)
      .query(`
        UPDATE admin.Connectors
        SET DisplayName = @displayName,
            AuthType    = @authType,
            BaseURL     = @baseURL,
            DocsURL     = @docsURL,
            RunnerType  = @runnerType,
            RunnerRef   = @runnerRef,
            ApiVersion  = @apiVersion,
            IsActive    = @isActive,
            Notes       = @notes,
            UpdatedBy   = @updatedBy,
            UpdatedAt   = SYSUTCDATETIME()
        OUTPUT INSERTED.ConnectorUID, INSERTED.Name, INSERTED.DisplayName
        WHERE ConnectorUID = @uid
      `);

    if (!r.recordset.length) return res.status(404).json({ error: 'Not found' });

    await logAction({
      userID: req.user.userID,
      action: 'CONNECTOR_UPDATE',
      entityType: 'Connector',
      entityID: req.params.uid,
      details: { displayName: b.displayName, isActive: b.isActive },
      ...reqMeta(req),
    });

    res.json({ connector: r.recordset[0] });
  } catch (e) {
    console.error('[connectors/update]', e);
    res.status(500).json({ error: 'Failed to update connector' });
  }
});

/* ---------- DELETE /api/connectors/:uid — soft delete (IsActive=0) ---------- */
router.delete('/:uid', async (req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request()
      .input('uid', sql.UniqueIdentifier, req.params.uid)
      .input('updatedBy', sql.Int, req.user.userID)
      .query(`
        UPDATE admin.Connectors
        SET IsActive = 0, UpdatedBy = @updatedBy, UpdatedAt = SYSUTCDATETIME()
        WHERE ConnectorUID = @uid
      `);

    if (!r.rowsAffected[0]) return res.status(404).json({ error: 'Not found' });

    await logAction({
      userID: req.user.userID,
      action: 'CONNECTOR_DEACTIVATE',
      entityType: 'Connector',
      entityID: req.params.uid,
      ...reqMeta(req),
    });

    res.json({ ok: true });
  } catch (e) {
    console.error('[connectors/delete]', e);
    res.status(500).json({ error: 'Failed to deactivate connector' });
  }
});

module.exports = router;
