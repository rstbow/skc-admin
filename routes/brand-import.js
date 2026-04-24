/**
 * Brand import from vs-ims-staging.dbo.tbl_PPA_L_Brand.
 *
 * Two endpoints:
 *   GET  /api/brand-import/candidates  → list of active brands with an
 *                                        "already-imported" flag per row
 *   POST /api/brand-import              → takes { brandIDs: [...] } and
 *                                        inserts them into admin.Brands
 *
 * Only active brands are surfaced (Inactive_ind = 0 AND Brand_Status = 'Active').
 * If the source row has a Brand_UID, we reuse it; if null we generate one.
 */
const express = require('express');
const { sql, getPool, getStagingPool } = require('../config/db');
const { requireAuth } = require('../middleware/auth');

const router = express.Router();
router.use(requireAuth);

/* ---------- GET /api/brand-import/candidates ---------- */
router.get('/candidates', async (_req, res) => {
  try {
    const [adminPool, stagingPool] = await Promise.all([getPool(), getStagingPool()]);

    // Existing admin brands — by BrandID for dedupe, BrandName for fuzzy fallback
    const existingRes = await adminPool.request().query(`
      SELECT BrandUID, BrandID, BrandName FROM admin.Brands;
    `);
    const existingByID   = new Map(existingRes.recordset.filter(r => r.BrandID).map(r => [r.BrandID, r]));
    const existingByName = new Map(existingRes.recordset.map(r => [r.BrandName.toLowerCase(), r]));

    // Source candidates — active only
    const srcRes = await stagingPool.request().query(`
      SELECT Brand_ID, Brand, SellerId, Brand_Status, Brand_UID,
             UserFirstName, UserLastName, UserEmail
      FROM dbo.tbl_PPA_L_Brand
      WHERE ISNULL(Inactive_ind, 0) = 0
        AND ISNULL(Brand_Status, '') IN ('Active', 'Trial')
      ORDER BY Brand;
    `);

    const candidates = srcRes.recordset.map(r => {
      const alreadyByID   = existingByID.get(r.Brand_ID);
      const alreadyByName = existingByName.get((r.Brand || '').toLowerCase());
      const already       = alreadyByID || alreadyByName || null;
      return {
        brandID:       r.Brand_ID,
        brandName:     r.Brand,
        sellerId:      r.SellerId,
        status:        r.Brand_Status,
        brandUID:      r.Brand_UID,
        contactName:   [r.UserFirstName, r.UserLastName].filter(Boolean).join(' ') || null,
        contactEmail:  r.UserEmail,
        alreadyImported: !!already,
        existingBrandUID: already ? already.BrandUID : null,
      };
    });

    res.json({
      candidates,
      counts: {
        total:            candidates.length,
        alreadyImported:  candidates.filter(c => c.alreadyImported).length,
        newlyImportable:  candidates.filter(c => !c.alreadyImported).length,
      },
    });
  } catch (e) {
    console.error('[brand-import/candidates]', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- POST /api/brand-import ---------- */
router.post('/', async (req, res) => {
  try {
    const { brandIDs } = req.body || {};
    if (!Array.isArray(brandIDs) || !brandIDs.length) {
      return res.status(400).json({ error: 'brandIDs (array of Brand_ID ints) is required' });
    }
    const ints = brandIDs.map((v) => parseInt(v, 10)).filter((v) => Number.isFinite(v));
    if (!ints.length) return res.status(400).json({ error: 'No valid Brand_IDs in request' });

    const [adminPool, stagingPool] = await Promise.all([getPool(), getStagingPool()]);

    // Fetch the source rows fresh so we import the latest values
    const idsCsv = ints.join(',');
    const srcRes = await stagingPool.request().query(`
      SELECT Brand_ID, Brand, Brand_UID
      FROM dbo.tbl_PPA_L_Brand
      WHERE Brand_ID IN (${idsCsv})
        AND ISNULL(Inactive_ind, 0) = 0
        AND ISNULL(Brand_Status, '') IN ('Active', 'Trial');
    `);

    if (!srcRes.recordset.length) {
      return res.status(404).json({ error: 'None of the requested brandIDs matched active source rows.' });
    }

    // Existing admin.Brands by BrandID and BrandName for dedupe
    const existingRes = await adminPool.request().query(`SELECT BrandID, BrandName FROM admin.Brands;`);
    const existingByID   = new Set(existingRes.recordset.filter(r => r.BrandID).map(r => r.BrandID));
    const existingByName = new Set(existingRes.recordset.map(r => r.BrandName.toLowerCase()));

    const results = [];
    for (const row of srcRes.recordset) {
      const existing = existingByID.has(row.Brand_ID) || existingByName.has((row.Brand || '').toLowerCase());
      if (existing) {
        results.push({ brandID: row.Brand_ID, brandName: row.Brand, status: 'skipped', reason: 'already imported' });
        continue;
      }
      try {
        // Generate a UID if the source has none. Slug is lowercase name
        // stripped of non-word chars; collision-safe via BrandID suffix if
        // needed.
        const uid = row.Brand_UID || require('crypto').randomUUID();
        const baseSlug = (row.Brand || '').toLowerCase()
          .replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 80) || ('brand-' + row.Brand_ID);
        const slug = baseSlug + '-' + row.Brand_ID;   // disambiguated by source id

        await adminPool.request()
          .input('uid',  sql.UniqueIdentifier, uid)
          .input('name', sql.NVarChar(200), row.Brand)
          .input('slug', sql.NVarChar(100), slug)
          .input('bid',  sql.Int, row.Brand_ID)
          .query(`
            INSERT INTO admin.Brands (BrandUID, BrandName, BrandSlug, BrandID, IsActive)
            VALUES (@uid, @name, @slug, @bid, 1);
          `);
        results.push({ brandID: row.Brand_ID, brandName: row.Brand, brandUID: uid, status: 'imported' });
      } catch (e) {
        results.push({ brandID: row.Brand_ID, brandName: row.Brand, status: 'error', error: e.message });
      }
    }

    const counts = {
      requested: ints.length,
      imported:  results.filter(r => r.status === 'imported').length,
      skipped:   results.filter(r => r.status === 'skipped').length,
      errored:   results.filter(r => r.status === 'error').length,
    };
    res.json({ results, counts });
  } catch (e) {
    console.error('[brand-import/post]', e);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
