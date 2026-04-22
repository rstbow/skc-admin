const express = require('express');
const { sql, getPool } = require('../config/db');
const { requireAuth } = require('../middleware/auth');
const { encrypt } = require('../config/crypto');
const { logAction, reqMeta } = require('../db/queries/audit');

const router = express.Router();
router.use(requireAuth);

const SECRET_FIELDS = ['refreshToken', 'accessToken', 'apiKey', 'appSecret'];
const MASK = '••••••••';

function maskRow(row) {
  return {
    credentialID: row.CredentialID,
    brandUID: row.BrandUID,
    connectorID: row.ConnectorID,
    connectorUID: row.ConnectorUID,
    connectorName: row.ConnectorName,
    connectorDisplay: row.ConnectorDisplay,
    accountIdentifier: row.AccountIdentifier,
    marketplaceIDs: row.MarketplaceIDs,
    hasRefreshToken: !!row.RefreshToken_Enc,
    hasAccessToken: !!row.AccessToken_Enc,
    hasApiKey: !!row.ApiKey_Enc,
    hasAppSecret: !!row.AppSecret_Enc,
    accessTokenExpiresAt: row.AccessTokenExpiresAt,
    extraConfig: row.ExtraConfig,
    isActive: row.IsActive,
    lastAuthedAt: row.LastAuthedAt,
    lastAuthError: row.LastAuthError,
    createdAt: row.CreatedAt,
    updatedAt: row.UpdatedAt,
  };
}

/* ---------- GET /api/credentials?brandUID=... — credentials for a brand ---------- */
router.get('/', async (req, res) => {
  try {
    if (!req.query.brandUID) return res.status(400).json({ error: 'brandUID is required' });
    const pool = await getPool();
    const r = await pool.request()
      .input('buid', sql.UniqueIdentifier, req.query.brandUID)
      .query(`
        SELECT bc.*, c.ConnectorUID, c.Name AS ConnectorName, c.DisplayName AS ConnectorDisplay
        FROM admin.BrandCredentials bc
        JOIN admin.Connectors c ON c.ConnectorID = bc.ConnectorID
        WHERE bc.BrandUID = @buid
        ORDER BY c.DisplayName
      `);
    res.json({ credentials: r.recordset.map(maskRow) });
  } catch (e) {
    console.error('[credentials/list]', e);
    res.status(500).json({ error: 'Failed to load credentials' });
  }
});

/* ---------- POST /api/credentials — upsert per (brand, connector) ---------- */
router.post('/', async (req, res) => {
  try {
    const b = req.body || {};
    if (!b.brandUID || !b.connectorUID) {
      return res.status(400).json({ error: 'brandUID and connectorUID are required' });
    }

    const pool = await getPool();

    // Resolve connectorID
    const cr = await pool.request()
      .input('cuid', sql.UniqueIdentifier, b.connectorUID)
      .query('SELECT ConnectorID FROM admin.Connectors WHERE ConnectorUID = @cuid');
    if (!cr.recordset.length) return res.status(400).json({ error: 'Unknown connectorUID' });
    const connectorID = cr.recordset[0].ConnectorID;

    // Encrypt any secret fields that were provided. Empty string = clear, undefined = leave alone.
    const encOrPassthrough = (v) => {
      if (v === undefined) return { provided: false, value: null };
      if (v === '' || v === null) return { provided: true, value: null }; // explicit clear
      return { provided: true, value: encrypt(v) };
    };

    const rt = encOrPassthrough(b.refreshToken);
    const at = encOrPassthrough(b.accessToken);
    const ak = encOrPassthrough(b.apiKey);
    const as = encOrPassthrough(b.appSecret);

    // Build a dynamic UPDATE-on-match so un-provided secrets are preserved.
    const setFrag = [
      'AccountIdentifier = @accountIdentifier',
      'MarketplaceIDs = @marketplaceIDs',
      'ExtraConfig = @extraConfig',
      'IsActive = @isActive',
      'UpdatedAt = SYSUTCDATETIME()',
    ];
    if (rt.provided) setFrag.push('RefreshToken_Enc = @rt');
    if (at.provided) setFrag.push('AccessToken_Enc = @at, AccessTokenExpiresAt = @atExp');
    if (ak.provided) setFrag.push('ApiKey_Enc = @ak');
    if (as.provided) setFrag.push('AppSecret_Enc = @as');

    const merge = `
      MERGE admin.BrandCredentials WITH (HOLDLOCK) AS tgt
      USING (VALUES (@brandUID, @connectorID)) AS src(BrandUID, ConnectorID)
      ON tgt.BrandUID = src.BrandUID AND tgt.ConnectorID = src.ConnectorID
      WHEN MATCHED THEN
          UPDATE SET ${setFrag.join(', ')}
      WHEN NOT MATCHED THEN
          INSERT (BrandUID, ConnectorID, AccountIdentifier, MarketplaceIDs,
                  RefreshToken_Enc, AccessToken_Enc, AccessTokenExpiresAt, ApiKey_Enc, AppSecret_Enc,
                  ExtraConfig, IsActive)
          VALUES (@brandUID, @connectorID, @accountIdentifier, @marketplaceIDs,
                  @rt, @at, @atExp, @ak, @as,
                  @extraConfig, @isActive)
      OUTPUT INSERTED.CredentialID, INSERTED.BrandUID, INSERTED.ConnectorID;
    `;

    const request = pool.request()
      .input('brandUID', sql.UniqueIdentifier, b.brandUID)
      .input('connectorID', sql.Int, connectorID)
      .input('accountIdentifier', sql.NVarChar(200), b.accountIdentifier || null)
      .input('marketplaceIDs', sql.NVarChar(500), b.marketplaceIDs || null)
      .input('extraConfig', sql.NVarChar(sql.MAX), b.extraConfig || null)
      .input('isActive', sql.Bit, b.isActive == null ? 1 : (b.isActive ? 1 : 0))
      .input('rt', sql.NVarChar(sql.MAX), rt.value)
      .input('at', sql.NVarChar(sql.MAX), at.value)
      .input('atExp', sql.DateTime2, b.accessTokenExpiresAt || null)
      .input('ak', sql.NVarChar(sql.MAX), ak.value)
      .input('as', sql.NVarChar(sql.MAX), as.value);

    const r = await request.query(merge);

    await logAction({
      userID: req.user.userID,
      action: 'CREDENTIAL_UPSERT',
      entityType: 'BrandCredential',
      entityID: b.brandUID + ':' + b.connectorUID,
      details: {
        brandUID: b.brandUID,
        connectorUID: b.connectorUID,
        secretsProvided: SECRET_FIELDS.filter((k) => b[k] !== undefined),
      },
      ...reqMeta(req),
    });

    res.status(201).json({ credential: r.recordset[0] });
  } catch (e) {
    console.error('[credentials/upsert]', e);
    res.status(500).json({ error: 'Failed to save credential: ' + e.message });
  }
});

/* ---------- DELETE /api/credentials/:credentialID — soft delete ---------- */
router.delete('/:credentialID', async (req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request()
      .input('id', sql.Int, parseInt(req.params.credentialID, 10))
      .query(`
        UPDATE admin.BrandCredentials
        SET IsActive = 0, UpdatedAt = SYSUTCDATETIME()
        WHERE CredentialID = @id
      `);
    if (!r.rowsAffected[0]) return res.status(404).json({ error: 'Not found' });

    await logAction({
      userID: req.user.userID,
      action: 'CREDENTIAL_DEACTIVATE',
      entityType: 'BrandCredential',
      entityID: req.params.credentialID,
      ...reqMeta(req),
    });

    res.json({ ok: true });
  } catch (e) {
    console.error('[credentials/delete]', e);
    res.status(500).json({ error: 'Failed to deactivate credential' });
  }
});

module.exports = router;
