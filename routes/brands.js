const express = require('express');
const { sql, getPool } = require('../config/db');
const { requireAuth } = require('../middleware/auth');
const { logAction, reqMeta } = require('../db/queries/audit');

const router = express.Router();
router.use(requireAuth);

/* ---------- GET /api/brands — list with credential-coverage counts ---------- */
router.get('/', async (_req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request().query(`
      SELECT b.BrandUID, b.BrandName, b.BrandSlug, b.IsActive, b.SyncedAt,
             b.CreatedAt, b.UpdatedAt,
             (SELECT COUNT(*) FROM admin.BrandCredentials bc
              WHERE bc.BrandUID = b.BrandUID AND bc.IsActive = 1) AS CredentialCount
      FROM admin.Brands b
      ORDER BY b.IsActive DESC, b.BrandName
    `);
    res.json({ brands: r.recordset });
  } catch (e) {
    console.error('[brands/list]', e);
    res.status(500).json({ error: 'Failed to load brands' });
  }
});

/* ---------- GET /api/brands/:uid ---------- */
router.get('/:uid', async (req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request()
      .input('uid', sql.UniqueIdentifier, req.params.uid)
      .query('SELECT * FROM admin.Brands WHERE BrandUID = @uid');
    if (!r.recordset.length) return res.status(404).json({ error: 'Not found' });
    res.json({ brand: r.recordset[0] });
  } catch (e) {
    console.error('[brands/get]', e);
    res.status(500).json({ error: 'Failed to load brand' });
  }
});

/* ---------- POST /api/brands — create/upsert ---------- */
router.post('/', async (req, res) => {
  try {
    const b = req.body || {};
    if (!b.brandUID || !b.brandName || !b.brandSlug) {
      return res.status(400).json({ error: 'brandUID, brandName, brandSlug are required' });
    }

    const pool = await getPool();
    const r = await pool.request()
      .input('uid', sql.UniqueIdentifier, b.brandUID)
      .input('name', sql.NVarChar(200), b.brandName.trim())
      .input('slug', sql.NVarChar(100), b.brandSlug.trim().toLowerCase())
      .input('conn', sql.NVarChar(sql.MAX), b.dataDbConnString || null)
      .query(`
        MERGE admin.Brands WITH (HOLDLOCK) AS tgt
        USING (VALUES (@uid)) AS src(BrandUID)
        ON tgt.BrandUID = src.BrandUID
        WHEN MATCHED THEN
            UPDATE SET BrandName = @name, BrandSlug = @slug,
                       DataDbConnString = @conn, UpdatedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (BrandUID, BrandName, BrandSlug, DataDbConnString)
            VALUES (@uid, @name, @slug, @conn)
        OUTPUT INSERTED.BrandUID, INSERTED.BrandName, INSERTED.BrandSlug;
      `);

    await logAction({
      userID: req.user.userID,
      action: 'BRAND_UPSERT',
      entityType: 'Brand',
      entityID: b.brandUID,
      details: { brandName: b.brandName, brandSlug: b.brandSlug },
      ...reqMeta(req),
    });

    res.status(201).json({ brand: r.recordset[0] });
  } catch (e) {
    console.error('[brands/create]', e);
    if (e.number === 2627 || e.number === 2601) {
      return res.status(409).json({ error: 'A brand with that slug already exists' });
    }
    res.status(500).json({ error: 'Failed to save brand' });
  }
});

/* ---------- PUT /api/brands/:uid — update ---------- */
router.put('/:uid', async (req, res) => {
  try {
    const b = req.body || {};
    const pool = await getPool();
    const r = await pool.request()
      .input('uid', sql.UniqueIdentifier, req.params.uid)
      .input('name', sql.NVarChar(200), b.brandName)
      .input('slug', sql.NVarChar(100), b.brandSlug?.toLowerCase())
      .input('conn', sql.NVarChar(sql.MAX), b.dataDbConnString || null)
      .input('isActive', sql.Bit, b.isActive == null ? 1 : (b.isActive ? 1 : 0))
      .query(`
        UPDATE admin.Brands
        SET BrandName = @name,
            BrandSlug = @slug,
            DataDbConnString = @conn,
            IsActive = @isActive,
            UpdatedAt = SYSUTCDATETIME()
        OUTPUT INSERTED.BrandUID, INSERTED.BrandName
        WHERE BrandUID = @uid
      `);
    if (!r.recordset.length) return res.status(404).json({ error: 'Not found' });

    await logAction({
      userID: req.user.userID,
      action: 'BRAND_UPDATE',
      entityType: 'Brand',
      entityID: req.params.uid,
      details: { brandName: b.brandName, isActive: b.isActive },
      ...reqMeta(req),
    });

    res.json({ brand: r.recordset[0] });
  } catch (e) {
    console.error('[brands/update]', e);
    res.status(500).json({ error: 'Failed to update brand' });
  }
});

module.exports = router;
