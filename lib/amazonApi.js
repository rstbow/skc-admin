/**
 * Amazon SP-API helper.
 *
 * Responsibilities:
 *   - Exchange refresh_token for access_token via LWA (cached per credential in-process)
 *   - Call SP-API endpoints with x-amz-access-token header
 *   - Return parsed JSON or throw with structured error
 *
 * Scope: minimal helper for ad-hoc endpoint tests. The real Phase 3 runner
 * will have proper rate-limit buckets, persistent token caching, retry, etc.
 */
const { decrypt } = require('../config/crypto');

const REGION_TO_ENDPOINT = {
  NA: 'https://sellingpartnerapi-na.amazon.com',
  EU: 'https://sellingpartnerapi-eu.amazon.com',
  FE: 'https://sellingpartnerapi-fe.amazon.com',
};

// In-process token cache keyed by credentialID
// value = { accessToken, expiresAt (ms epoch) }
const TOKEN_CACHE = new Map();

async function getAccessToken(credentialID, connector, cred) {
  const cached = TOKEN_CACHE.get(credentialID);
  if (cached && cached.expiresAt > Date.now() + 60_000) {
    return cached.accessToken;
  }

  const clientID = connector.AppClientID;
  const clientSecret = connector.AppClientSecret_Enc ? decrypt(connector.AppClientSecret_Enc) : null;
  const refreshToken = cred.RefreshToken_Enc ? decrypt(cred.RefreshToken_Enc) : null;
  if (!clientID || !clientSecret) {
    throw new Error('Connector is missing LWA Client ID or Client Secret.');
  }
  if (!refreshToken) {
    throw new Error('Credential is missing a refresh token.');
  }

  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: refreshToken,
    client_id: clientID,
    client_secret: clientSecret,
  }).toString();

  const res = await fetch('https://api.amazon.com/auth/o2/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' },
    body,
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error('LWA token exchange failed: ' + (json.error_description || json.error || ('HTTP ' + res.status)));
  }

  const accessToken = json.access_token;
  const expiresAt = Date.now() + ((json.expires_in || 3600) * 1000);
  TOKEN_CACHE.set(credentialID, { accessToken, expiresAt });
  return accessToken;
}

function endpointFor(region) {
  return REGION_TO_ENDPOINT[region] || REGION_TO_ENDPOINT.NA;
}

/**
 * Call an SP-API endpoint.
 * @param {object} ctx { credentialID, connector, cred }
 * @param {string} path e.g. '/finances/v0/financialEvents?PostedAfter=2026-04-15T00:00:00Z'
 */
async function callSpApi(ctx, path) {
  const accessToken = await getAccessToken(ctx.credentialID, ctx.connector, ctx.cred);
  const url = endpointFor(ctx.cred.Region) + path;

  const res = await fetch(url, {
    headers: {
      'x-amz-access-token': accessToken,
      'Accept': 'application/json',
    },
  });
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch (_) { /* non-JSON response */ }

  if (!res.ok) {
    const err = new Error('SP-API call failed: ' + res.status + ' ' + res.statusText);
    err.status = res.status;
    err.response = json || text;
    err.path = path;
    throw err;
  }
  return json;
}

module.exports = { callSpApi, getAccessToken, endpointFor };
