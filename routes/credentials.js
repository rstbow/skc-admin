const express = require('express');
const { sql, getPool } = require('../config/db');
const { requireAuth } = require('../middleware/auth');
const { encrypt } = require('../config/crypto');
const { logAction, reqMeta } = require('../db/queries/audit');
const { testConnection } = require('../lib/connectorTests');

const router = express.Router();
router.use(requireAuth);

const SECRET_FIELDS = ['refreshToken', 'accessToken', 'apiKey', 'appSecret'];

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
    region: row.Region,
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

function encOrPassthrough(v) {
  if (v === undefined) return { provided: false, value: null };
  if (v === '' || v === null) return { provided: true, value: null };
  return { provided: true, value: encrypt(v) };
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
        ORDER BY c.DisplayName, bc.Region, bc.AccountIdentifier
      `);
    res.json({ credentials: r.recordset.map(maskRow) });
  } catch (e) {
    console.error('[credentials/list]', e);
    res.status(500).json({ error: 'Failed to load credentials' });
  }
});

/* ---------- GET /api/credentials/:credentialID — single row for editing ---------- */
router.get('/:credentialID(\\d+)', async (req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request()
      .input('id', sql.Int, parseInt(req.params.credentialID, 10))
      .query(`
        SELECT bc.*, c.ConnectorUID, c.Name AS ConnectorName, c.DisplayName AS ConnectorDisplay
        FROM admin.BrandCredentials bc
        JOIN admin.Connectors c ON c.ConnectorID = bc.ConnectorID
        WHERE bc.CredentialID = @id
      `);
    if (!r.recordset.length) return res.status(404).json({ error: 'Not found' });
    res.json({ credential: maskRow(r.recordset[0]) });
  } catch (e) {
    console.error('[credentials/get]', e);
    res.status(500).json({ error: 'Failed to load credential' });
  }
});

/* ---------- POST /api/credentials — create new (allows multiple per brand+connector) ---------- */
router.post('/', async (req, res) => {
  try {
    const b = req.body || {};
    if (!b.brandUID || !b.connectorUID) {
      return res.status(400).json({ error: 'brandUID and connectorUID are required' });
    }

    const pool = await getPool();
    const cr = await pool.request()
      .input('cuid', sql.UniqueIdentifier, b.connectorUID)
      .query('SELECT ConnectorID FROM admin.Connectors WHERE ConnectorUID = @cuid');
    if (!cr.recordset.length) return res.status(400).json({ error: 'Unknown connectorUID' });
    const connectorID = cr.recordset[0].ConnectorID;

    const rt = encOrPassthrough(b.refreshToken);
    const at = encOrPassthrough(b.accessToken);
    const ak = encOrPassthrough(b.apiKey);
    const as = encOrPassthrough(b.appSecret);

    const r = await pool.request()
      .input('brandUID', sql.UniqueIdentifier, b.brandUID)
      .input('connectorID', sql.Int, connectorID)
      .input('accountIdentifier', sql.NVarChar(200), b.accountIdentifier || null)
      .input('marketplaceIDs', sql.NVarChar(500), b.marketplaceIDs || null)
      .input('region', sql.NVarChar(10), b.region || null)
      .input('extraConfig', sql.NVarChar(sql.MAX), b.extraConfig || null)
      .input('isActive', sql.Bit, b.isActive == null ? 1 : (b.isActive ? 1 : 0))
      .input('rt', sql.NVarChar(sql.MAX), rt.value)
      .input('at', sql.NVarChar(sql.MAX), at.value)
      .input('atExp', sql.DateTime2, b.accessTokenExpiresAt || null)
      .input('ak', sql.NVarChar(sql.MAX), ak.value)
      .input('as', sql.NVarChar(sql.MAX), as.value)
      .query(`
        INSERT INTO admin.BrandCredentials
          (BrandUID, ConnectorID, AccountIdentifier, MarketplaceIDs, Region,
           RefreshToken_Enc, AccessToken_Enc, AccessTokenExpiresAt, ApiKey_Enc, AppSecret_Enc,
           ExtraConfig, IsActive)
        OUTPUT INSERTED.CredentialID, INSERTED.BrandUID, INSERTED.ConnectorID,
               INSERTED.AccountIdentifier, INSERTED.Region
        VALUES
          (@brandUID, @connectorID, @accountIdentifier, @marketplaceIDs, @region,
           @rt, @at, @atExp, @ak, @as,
           @extraConfig, @isActive);
      `);

    await logAction({
      userID: req.user.userID,
      action: 'CREDENTIAL_CREATE',
      entityType: 'BrandCredential',
      entityID: String(r.recordset[0].CredentialID),
      details: {
        brandUID: b.brandUID, connectorUID: b.connectorUID,
        accountIdentifier: b.accountIdentifier, region: b.region,
        secretsProvided: SECRET_FIELDS.filter((k) => b[k] !== undefined && b[k] !== ''),
      },
      ...reqMeta(req),
    });

    res.status(201).json({ credential: r.recordset[0] });
  } catch (e) {
    console.error('[credentials/create]', e);
    if (e.number === 2627 || e.number === 2601) {
      return res.status(409).json({ error: 'A credential with those identifiers already exists for this brand + connector. Edit that one instead or change the AccountIdentifier/Region.' });
    }
    res.status(500).json({ error: 'Failed to save credential: ' + e.message });
  }
});

/* ---------- PUT /api/credentials/:credentialID — update single row ---------- */
router.put('/:credentialID(\\d+)', async (req, res) => {
  try {
    const b = req.body || {};
    const pool = await getPool();

    const rt = encOrPassthrough(b.refreshToken);
    const at = encOrPassthrough(b.accessToken);
    const ak = encOrPassthrough(b.apiKey);
    const as = encOrPassthrough(b.appSecret);

    const setParts = [
      'AccountIdentifier = @accountIdentifier',
      'MarketplaceIDs = @marketplaceIDs',
      'Region = @region',
      'ExtraConfig = @extraConfig',
      'IsActive = @isActive',
      'UpdatedAt = SYSUTCDATETIME()',
    ];
    if (rt.provided) setParts.push('RefreshToken_Enc = @rt');
    if (at.provided) setParts.push('AccessToken_Enc = @at, AccessTokenExpiresAt = @atExp');
    if (ak.provided) setParts.push('ApiKey_Enc = @ak');
    if (as.provided) setParts.push('AppSecret_Enc = @as');

    const request = pool.request()
      .input('id', sql.Int, parseInt(req.params.credentialID, 10))
      .input('accountIdentifier', sql.NVarChar(200), b.accountIdentifier || null)
      .input('marketplaceIDs', sql.NVarChar(500), b.marketplaceIDs || null)
      .input('region', sql.NVarChar(10), b.region || null)
      .input('extraConfig', sql.NVarChar(sql.MAX), b.extraConfig || null)
      .input('isActive', sql.Bit, b.isActive == null ? 1 : (b.isActive ? 1 : 0))
      .input('rt', sql.NVarChar(sql.MAX), rt.value)
      .input('at', sql.NVarChar(sql.MAX), at.value)
      .input('atExp', sql.DateTime2, b.accessTokenExpiresAt || null)
      .input('ak', sql.NVarChar(sql.MAX), ak.value)
      .input('as', sql.NVarChar(sql.MAX), as.value);

    const r = await request.query(`
      UPDATE admin.BrandCredentials
      SET ${setParts.join(', ')}
      OUTPUT INSERTED.CredentialID
      WHERE CredentialID = @id;
    `);

    if (!r.recordset.length) return res.status(404).json({ error: 'Not found' });

    await logAction({
      userID: req.user.userID,
      action: 'CREDENTIAL_UPDATE',
      entityType: 'BrandCredential',
      entityID: req.params.credentialID,
      details: {
        accountIdentifier: b.accountIdentifier, region: b.region,
        secretsProvided: SECRET_FIELDS.filter((k) => b[k] !== undefined && b[k] !== ''),
      },
      ...reqMeta(req),
    });

    res.json({ credential: r.recordset[0] });
  } catch (e) {
    console.error('[credentials/update]', e);
    if (e.number === 2627 || e.number === 2601) {
      return res.status(409).json({ error: 'Would create a duplicate. Another credential already uses that AccountIdentifier/Region for this brand + connector.' });
    }
    res.status(500).json({ error: 'Failed to update credential: ' + e.message });
  }
});

/* ---------- POST /api/credentials/:credentialID/test — live connectivity test ---------- */
router.post('/:credentialID(\\d+)/test', async (req, res) => {
  try {
    const pool = await getPool();
    const credID = parseInt(req.params.credentialID, 10);
    const r = await pool.request()
      .input('id', sql.Int, credID)
      .query(`
        SELECT bc.CredentialID, bc.BrandUID, bc.ConnectorID, bc.AccountIdentifier,
               bc.MarketplaceIDs, bc.Region, bc.RefreshToken_Enc, bc.AccessToken_Enc,
               bc.AccessTokenExpiresAt, bc.ApiKey_Enc, bc.AppSecret_Enc, bc.ExtraConfig,
               c.ConnectorUID, c.Name, c.DisplayName, c.AuthType, c.BaseURL,
               c.AppClientID, c.AppClientSecret_Enc, c.ApiVersion, c.CredentialScope
        FROM admin.BrandCredentials bc
        JOIN admin.Connectors c ON c.ConnectorID = bc.ConnectorID
        WHERE bc.CredentialID = @id
      `);
    if (!r.recordset.length) return res.status(404).json({ error: 'Credential not found' });

    const row = r.recordset[0];
    const connector = {
      ConnectorUID: row.ConnectorUID,
      Name: row.Name,
      DisplayName: row.DisplayName,
      AuthType: row.AuthType,
      BaseURL: row.BaseURL,
      AppClientID: row.AppClientID,
      AppClientSecret_Enc: row.AppClientSecret_Enc,
      ApiVersion: row.ApiVersion,
      CredentialScope: row.CredentialScope,
    };
    const cred = {
      CredentialID: row.CredentialID,
      AccountIdentifier: row.AccountIdentifier,
      MarketplaceIDs: row.MarketplaceIDs,
      Region: row.Region,
      RefreshToken_Enc: row.RefreshToken_Enc,
      AccessToken_Enc: row.AccessToken_Enc,
      AccessTokenExpiresAt: row.AccessTokenExpiresAt,
      ApiKey_Enc: row.ApiKey_Enc,
      AppSecret_Enc: row.AppSecret_Enc,
      ExtraConfig: row.ExtraConfig,
    };

    const result = await testConnection({ connector, cred });

    // Record outcome on the credential row
    const updateReq = pool.request().input('id', sql.Int, credID);
    if (result.ok) {
      await updateReq.query(`
        UPDATE admin.BrandCredentials
        SET LastAuthedAt = SYSUTCDATETIME(),
            LastAuthError = NULL,
            UpdatedAt = SYSUTCDATETIME()
        WHERE CredentialID = @id
      `);
    } else {
      await updateReq
        .input('err', sql.NVarChar(sql.MAX), String(result.message || 'Unknown error').slice(0, 2000))
        .query(`
          UPDATE admin.BrandCredentials
          SET LastAuthError = @err,
              UpdatedAt = SYSUTCDATETIME()
          WHERE CredentialID = @id
        `);
    }

    await logAction({
      userID: req.user.userID,
      action: 'CREDENTIAL_TEST',
      entityType: 'BrandCredential',
      entityID: String(credID),
      details: { ok: result.ok, partial: !!result.partial, connector: connector.Name, message: result.message },
      ...reqMeta(req),
    });

    res.json({ result });
  } catch (e) {
    console.error('[credentials/test]', e);
    res.status(500).json({ error: 'Test failed: ' + e.message });
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
