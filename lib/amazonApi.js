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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Call an SP-API endpoint. Auto-retries on 429 using the Retry-After header
 * (or 2-second fallback) up to 3 times.
 *
 * @param {object} ctx { credentialID, connector, cred }
 * @param {string} path e.g. '/finances/v0/financialEvents?PostedAfter=2026-04-15T00:00:00Z'
 */
async function callSpApi(ctx, path, { maxRetries = 3 } = {}) {
  const accessToken = await getAccessToken(ctx.credentialID, ctx.connector, ctx.cred);
  const url = endpointFor(ctx.cred.Region) + path;

  let attempt = 0;
  while (true) {
    const res = await fetch(url, {
      headers: {
        'x-amz-access-token': accessToken,
        'Accept': 'application/json',
      },
    });
    const text = await res.text();
    let json = null;
    try { json = text ? JSON.parse(text) : null; } catch (_) { /* non-JSON response */ }

    if (res.status === 429 && attempt < maxRetries) {
      const ra = parseFloat(res.headers.get('retry-after') || '') || 2;
      await sleep(Math.min(60000, ra * 1000));
      attempt++;
      continue;
    }
    if (!res.ok) {
      const err = new Error('SP-API call failed: ' + res.status + ' ' + res.statusText);
      err.status = res.status;
      err.response = json || text;
      err.path = path;
      err.retryAfter = res.headers.get('retry-after');
      throw err;
    }
    return json;
  }
}

/**
 * Paginated wrapper for SP-API endpoints that return a NextToken.
 * Calls `onPage(payload)` for each page's payload. Returns page count,
 * whether we hit the safety cap, and the reason if so.
 *
 * Two independent caps:
 *   - maxPages:     hard limit on page count (default 60)
 *   - maxElapsedMs: wall-clock budget (default 60 sec)
 * Whichever trips first stops pagination. Partial results returned.
 *
 * Rate limit aware: small delay between pages to stay under typical
 * 0.5–2 rps caps. callSpApi handles 429 retries underneath.
 *
 * @param {object} ctx
 * @param {string} buildPath  Function: (nextToken | null) => '/path?query'
 * @param {function} onPage   Function: (payload) => void
 * @param {object} opts       { maxPages?, pageDelayMs?, maxElapsedMs? }
 */
async function paginateSpApi(ctx, buildPath, onPage, opts = {}) {
  const maxPages = opts.maxPages ?? 60;
  const pageDelayMs = opts.pageDelayMs ?? 300;
  const maxElapsedMs = opts.maxElapsedMs ?? 60_000;

  const startedAt = Date.now();
  let nextToken = null;
  let pages = 0;
  let hitCap = false;
  let capReason = null;

  do {
    const path = buildPath(nextToken);
    const resp = await callSpApi(ctx, path);
    const payload = (resp && resp.payload) || {};
    onPage(payload);
    nextToken = payload.NextToken || null;
    pages++;

    if (nextToken && pages >= maxPages) {
      hitCap = true;
      capReason = 'maxPages (' + maxPages + ')';
      break;
    }
    if (nextToken && (Date.now() - startedAt) >= maxElapsedMs) {
      hitCap = true;
      capReason = 'time budget (' + Math.round(maxElapsedMs / 1000) + 's)';
      break;
    }
    if (nextToken && pageDelayMs > 0) await sleep(pageDelayMs);
  } while (nextToken);

  return { pages, hitCap, capReason, elapsedMs: Date.now() - startedAt };
}

module.exports = { callSpApi, paginateSpApi, getAccessToken, endpointFor };
