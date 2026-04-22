/**
 * Connector "test connection" logic.
 *
 * For each connector, we do the minimum viable round-trip to prove the stored
 * credentials work. For OAuth connectors, that's usually:
 *   - Exchange refresh_token or client_credentials for an access_token
 *
 * For static-token connectors (Shopify custom apps):
 *   - One lightweight GET against a known endpoint that requires auth
 *
 * We keep these dependency-free — Node 20 has global fetch().
 * No external npm packages needed.
 */
const { decrypt } = require('../config/crypto');

const REGION_TO_ENDPOINT = {
  NA: 'https://sellingpartnerapi-na.amazon.com',
  EU: 'https://sellingpartnerapi-eu.amazon.com',
  FE: 'https://sellingpartnerapi-fe.amazon.com',
};

/* ========================================================================== */
/*  Amazon SP-API — LWA token exchange + optional sellers call                */
/* ========================================================================== */
async function testAmazonSP({ connector, cred }) {
  const clientID = connector.AppClientID;
  if (!clientID) {
    return { ok: false, message: 'LWA Client ID is not set on the connector.' };
  }
  const clientSecret = connector.AppClientSecret_Enc ? decrypt(connector.AppClientSecret_Enc) : null;
  if (!clientSecret) {
    return { ok: false, message: 'LWA Client Secret is not set on the connector.' };
  }
  const refreshToken = cred.RefreshToken_Enc ? decrypt(cred.RefreshToken_Enc) : null;
  if (!refreshToken) {
    return { ok: false, message: 'No refresh token stored for this brand credential.' };
  }

  // Step 1 — exchange refresh_token for access_token
  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: refreshToken,
    client_id: clientID,
    client_secret: clientSecret,
  }).toString();

  const tokRes = await fetch('https://api.amazon.com/auth/o2/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' },
    body,
  });
  const tokJson = await tokRes.json().catch(() => ({}));
  if (!tokRes.ok) {
    return {
      ok: false,
      message: 'LWA token exchange failed: ' + (tokJson.error_description || tokJson.error || ('HTTP ' + tokRes.status)),
      details: tokJson,
    };
  }

  const accessToken = tokJson.access_token;
  const expiresIn = tokJson.expires_in;

  // Step 2 — call getMarketplaceParticipations (proves SP-API reachability + seller scope)
  const endpoint = REGION_TO_ENDPOINT[cred.Region] || REGION_TO_ENDPOINT.NA;
  let marketplaces = null;
  let sellerId = null;
  try {
    const mpRes = await fetch(endpoint + '/sellers/v1/marketplaceParticipations', {
      headers: { 'x-amz-access-token': accessToken },
    });
    const mpJson = await mpRes.json().catch(() => ({}));
    if (mpRes.ok) {
      const items = mpJson.payload || [];
      marketplaces = items.map((x) => ({
        id: x.marketplace?.id,
        countryCode: x.marketplace?.countryCode,
        name: x.marketplace?.name,
      }));
      sellerId = items[0]?.participation?.sellerId || null;
    } else {
      return {
        ok: true, // LWA worked so creds ARE valid
        partial: true,
        message: 'LWA OK, but SP-API call failed (' + mpRes.status + '). Check the Region on this credential — you picked ' + (cred.Region || 'NA (default)') + '.',
        details: { expiresIn, sellerId, mpStatus: mpRes.status, mpBody: mpJson },
      };
    }
  } catch (e) {
    return {
      ok: true,
      partial: true,
      message: 'LWA OK, but SP-API network call failed: ' + e.message,
      details: { expiresIn },
    };
  }

  return {
    ok: true,
    message: 'Authenticated. Seller ID ' + (sellerId || '?') + ' has access to ' + (marketplaces?.length ?? 0) + ' marketplace(s).',
    details: { accessTokenExpiresIn: expiresIn, sellerId, marketplaces, endpoint },
  };
}

/* ========================================================================== */
/*  Shopify — Admin API ping via /shop.json                                   */
/* ========================================================================== */
async function testShopify({ connector, cred }) {
  const token = cred.ApiKey_Enc ? decrypt(cred.ApiKey_Enc) : null;
  if (!token) {
    return { ok: false, message: 'No Shopify access token stored (paste shpat_ token into "API key" field).' };
  }

  // Shop domain comes from ExtraConfig.shop_domain or AccountIdentifier
  let shopDomain = cred.AccountIdentifier;
  try {
    const cfg = cred.ExtraConfig ? JSON.parse(cred.ExtraConfig) : {};
    if (cfg.shop_domain) shopDomain = cfg.shop_domain;
  } catch (_) { /* ignore bad JSON */ }

  if (!shopDomain) {
    return { ok: false, message: 'No shop domain stored. Set "Shop domain" as the AccountIdentifier (e.g. acme.myshopify.com).' };
  }
  if (!/\.myshopify\.com$/i.test(shopDomain)) {
    return { ok: false, message: 'Shop domain must end with .myshopify.com (got: ' + shopDomain + ')' };
  }

  const apiVersion = connector.ApiVersion || '2026-04';
  const url = 'https://' + shopDomain + '/admin/api/' + apiVersion + '/shop.json';

  const res = await fetch(url, { headers: { 'X-Shopify-Access-Token': token } });
  const json = await res.json().catch(() => ({}));

  if (!res.ok) {
    return {
      ok: false,
      message: 'Shopify call failed (' + res.status + '): ' + (json.errors || json.error || 'Unknown error'),
      details: json,
    };
  }

  const shop = json.shop || {};
  return {
    ok: true,
    message: 'Authenticated. Shop: ' + shop.name + ' (plan: ' + (shop.plan_display_name || shop.plan_name || '?') + ').',
    details: {
      shopName: shop.name,
      domain: shop.domain,
      myshopifyDomain: shop.myshopify_domain,
      plan: shop.plan_display_name || shop.plan_name,
      currency: shop.currency,
      timezone: shop.iana_timezone,
    },
  };
}

/* ========================================================================== */
/*  Walmart MP — OAuth2 client_credentials exchange                           */
/* ========================================================================== */
async function testWalmartMP({ connector, cred }) {
  const clientID = connector.AppClientID;
  const clientSecret = connector.AppClientSecret_Enc ? decrypt(connector.AppClientSecret_Enc) : null;
  if (!clientID || !clientSecret) {
    return { ok: false, message: 'Walmart Client ID + Client Secret must be set on the connector.' };
  }

  const basicAuth = Buffer.from(clientID + ':' + clientSecret).toString('base64');
  const corrId = 'skc-admin-' + Date.now();

  const res = await fetch('https://marketplace.walmartapis.com/v3/token', {
    method: 'POST',
    headers: {
      'Authorization': 'Basic ' + basicAuth,
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/json',
      'WM_QOS.CORRELATION_ID': corrId,
      'WM_SVC.NAME': 'Walmart Marketplace',
    },
    body: 'grant_type=client_credentials',
  });
  const json = await res.json().catch(() => ({}));

  if (!res.ok) {
    return {
      ok: false,
      message: 'Walmart token exchange failed (' + res.status + '): ' + (json.error || json.error_description || 'see details'),
      details: json,
    };
  }

  return {
    ok: true,
    message: 'Authenticated. Got Walmart access token (expires in ' + json.expires_in + 's).',
    details: { expiresIn: json.expires_in, tokenType: json.token_type },
  };
}

/* ========================================================================== */
/*  Extensiv — OAuth2 client_credentials (BRAND-level creds)                  */
/* ========================================================================== */
async function testExtensiv({ connector, cred }) {
  // Extensiv is BRAND_ONLY — credentials live on the brand row, not the connector
  const clientID = cred.ApiKey_Enc ? decrypt(cred.ApiKey_Enc) : null;
  const clientSecret = cred.AppSecret_Enc ? decrypt(cred.AppSecret_Enc) : null;
  if (!clientID || !clientSecret) {
    return { ok: false, message: 'Extensiv client_id (API key field) and client_secret (App secret field) must both be set on this brand credential.' };
  }

  const tplGuid = cred.AccountIdentifier || null;
  const basicAuth = Buffer.from(clientID + ':' + clientSecret).toString('base64');

  const headers = {
    'Authorization': 'Basic ' + basicAuth,
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept': 'application/json',
  };
  if (tplGuid) headers['3PL-Guid'] = tplGuid;

  const res = await fetch('https://secure-wms.com/AuthServer/api/Token', {
    method: 'POST',
    headers,
    body: 'grant_type=client_credentials&scope=scope_wms.secure_wms-api/*',
  });
  const json = await res.json().catch(() => ({}));

  if (!res.ok) {
    return {
      ok: false,
      message: 'Extensiv token exchange failed (' + res.status + '): ' + (json.error_description || json.error || 'see details'),
      details: json,
    };
  }

  return {
    ok: true,
    message: 'Authenticated. Got Extensiv access token (expires in ' + (json.expires_in || '?') + 's).',
    details: { expiresIn: json.expires_in, tokenType: json.token_type },
  };
}

/* ========================================================================== */
/*  TikTok Shop — auth with app_key + app_secret + access_token               */
/* ========================================================================== */
async function testTikTokShop({ connector, cred }) {
  // TikTok's auth model requires signing each call — a full test requires
  // more plumbing than a quick smoke test. For now, just verify we have the
  // fields present; deep test will come with the runner in Phase 3+.
  const appKey = connector.AppClientID;
  const appSecret = connector.AppClientSecret_Enc ? decrypt(connector.AppClientSecret_Enc) : null;
  const accessToken = cred.AccessToken_Enc ? decrypt(cred.AccessToken_Enc) : null;
  const shopID = cred.AccountIdentifier;

  const missing = [];
  if (!appKey) missing.push('connector.AppClientID (App Key)');
  if (!appSecret) missing.push('connector.AppClientSecret (App Secret)');
  if (!accessToken) missing.push('cred.AccessToken (per-brand)');
  if (!shopID) missing.push('cred.AccountIdentifier (Shop ID)');

  if (missing.length) {
    return { ok: false, message: 'Missing required fields: ' + missing.join(', ') };
  }

  return {
    ok: true,
    partial: true,
    message: 'All required TikTok fields present. Deep connectivity test will be added with Phase 3 runner.',
  };
}

/* ========================================================================== */

const TESTERS = {
  AMAZON_SP_API: testAmazonSP,
  SHOPIFY: testShopify,
  WALMART_MP: testWalmartMP,
  EXTENSIV: testExtensiv,
  TIKTOK_SHOP: testTikTokShop,
};

async function testConnection({ connector, cred }) {
  const tester = TESTERS[connector.Name];
  if (!tester) {
    return { ok: false, message: 'No test implementation for connector "' + connector.Name + '" yet.' };
  }
  try {
    const result = await tester({ connector, cred });
    return result;
  } catch (e) {
    return { ok: false, message: 'Unexpected error: ' + e.message, details: { stack: e.stack } };
  }
}

module.exports = { testConnection };
