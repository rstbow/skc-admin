/**
 * Amazon Orders runner.
 *
 * Pulls orders from SP-API getOrders + (optionally) getOrderItems, then
 * MERGEs into raw.amz_orders + raw.amz_order_items.
 *
 * Two pull modes (selected via params.mode, default 'CREATED'):
 *
 *   CREATED  — query getOrders with CreatedAfter window. Captures every
 *              new order placed in the window. Drives the daily
 *              incremental "what sold today" feed. Always fetches items
 *              for each order (fresh order = first time we've seen it).
 *
 *   UPDATED  — query getOrders with LastUpdatedAfter window. Catches
 *              status flips on existing orders (Pending→Shipped, Cancel,
 *              partial-ship). By default skips items (line-item data is
 *              fixed at purchase time and won't have changed), but caller
 *              can force item refetch with params.fetchItems=true.
 *
 * Merge proc raw.usp_merge_amz_orders is LastUpdate-aware: an UPDATED-mode
 * pull will not be overwritten by a later out-of-order CREATED-mode pull
 * with stale LastUpdatedDate values.
 *
 * Rate-limit shape (per Amazon docs):
 *   getOrders:     0.0167 rps sustained, burst 20  — pagination is cheap
 *   getOrderItems: 0.5 rps sustained, burst 30     — per-order call
 *                  (we space at 2100ms = ~0.47 rps for safety)
 *
 * Idempotent: re-running the same window with the same data = zero writes
 * (hash match skips the UPDATE branch in the MERGE proc).
 */
const crypto = require('crypto');
const { sql, getPool } = require('../config/db');
const { getBrandPool } = require('./brandDb');
const { callSpApi, paginateSpApi } = require('./amazonApi');
const { computeWindows } = require('./windowChunks');

const BATCH_SIZE = 500;

const DEFAULT_PAGE_LIMIT       = 1000;          // hard pages cap per window
const DEFAULT_TIME_LIMIT       = 600_000;       // 10 min wall-clock per window
const DEFAULT_CHUNK_DAYS       = 30;            // window slice size
const DEFAULT_PAGE_DELAY_MS    = 1000;          // between getOrders pages
const DEFAULT_ITEM_DELAY_MS    = 2100;          // between getOrderItems calls
                                                 // (0.5 rps with margin)

/* =========================================================================
   Public entry point
   ========================================================================= */
async function runAmazonOrders({
  credentialID,
  triggeredBy = 'MANUAL',
  userID = null,
  params = {},
}) {
  if (!credentialID) throw new Error('credentialID is required');

  const ctx = await loadCredentialContext(credentialID);
  if (!ctx) throw new Error('Credential not found');
  if (ctx.connector.Name !== 'AMAZON_SP_API') {
    throw new Error('Runner only supports AMAZON_SP_API credentials');
  }

  const mode = String(params.mode || 'CREATED').toUpperCase();
  if (mode !== 'CREATED' && mode !== 'UPDATED') {
    throw new Error("params.mode must be 'CREATED' or 'UPDATED'");
  }
  // Default: fetch items only on CREATED (new orders). Caller can force
  // either way with params.fetchItems true/false.
  const fetchItems = (params.fetchItems != null)
    ? Boolean(params.fetchItems)
    : (mode === 'CREATED');

  const endpointID = await resolveEndpointID('AMZ_ORDERS');
  const jobID      = await ensureJob(endpointID, ctx.brand.BrandUID);
  const runID      = await startJobRun(jobID, triggeredBy);

  try {
    const windows = computeWindows({
      postedAfter:  params.after  || params.postedAfter,
      postedBefore: params.before || params.postedBefore,
      daysBack:     params.daysBack,
      chunkDays:    params.chunkDays || DEFAULT_CHUNK_DAYS,
      // Orders API rejects window-end values within ~2 min of now.
      endBufferMs: 2 * 60 * 1000,
    });

    console.log('[runner/amz-orders] start', {
      credentialID, brandUID: ctx.brand.BrandUID, brandName: ctx.brand.BrandName,
      mode, fetchItems,
      region: ctx.cred.Region, marketplaces: ctx.cred.MarketplaceIDs,
      chunks: windows.length,
      range: windows.length
        ? { first: windows[0].after, last: windows[windows.length-1].before }
        : null,
    });

    await updateJobRunProgress(runID, {
      chunksTotal: windows.length, chunksCompleted: 0, rowsIngested: 0,
    });

    const brandPool = await getBrandPool(ctx.brand.BrandUID);

    const totals = {
      orders: 0, items: 0,
      ordersInserted: 0, ordersUpdated: 0, ordersUnchanged: 0,
      itemsInserted: 0, itemsUpdated: 0, itemsUnchanged: 0,
      pages: 0, truncatedChunks: 0,
    };

    for (let i = 0; i < windows.length; i++) {
      const w = windows[i];
      const chunkStart = Date.now();
      console.log(`[runner/amz-orders] chunk ${i+1}/${windows.length}`, w);

      const r = await pullMergeWindow(ctx, brandPool, runID, w, {
        mode,
        fetchItems,
        pageLimit:    params.pageLimit   || DEFAULT_PAGE_LIMIT,
        timeLimitMs:  params.timeLimitMs || DEFAULT_TIME_LIMIT,
        pageDelayMs:  Number.isFinite(params.pageDelayMs) ? params.pageDelayMs : DEFAULT_PAGE_DELAY_MS,
        itemDelayMs:  Number.isFinite(params.itemDelayMs) ? params.itemDelayMs : DEFAULT_ITEM_DELAY_MS,
      });

      totals.orders          += r.orders;
      totals.items           += r.items;
      totals.ordersInserted  += r.ordersInserted;
      totals.ordersUpdated   += r.ordersUpdated;
      totals.ordersUnchanged += r.ordersUnchanged;
      totals.itemsInserted   += r.itemsInserted;
      totals.itemsUpdated    += r.itemsUpdated;
      totals.itemsUnchanged  += r.itemsUnchanged;
      totals.pages           += r.pages;
      if (r.truncated) totals.truncatedChunks++;

      await updateJobRunProgress(runID, {
        chunksCompleted: i + 1, chunksTotal: windows.length,
        rowsIngested: totals.orders + totals.items,
      });

      console.log(`[runner/amz-orders] chunk ${i+1}/${windows.length} done`, {
        orders: r.orders, items: r.items, pages: r.pages,
        truncated: r.truncated || false,
        elapsedMs: Date.now() - chunkStart,
      });
    }

    await completeJobRun(runID, {
      status: 'SUCCESS',
      rowsIngested: totals.orders + totals.items,
      errorMessage: null,
      workerType: 'NODE',
    });
    await bumpLastAuthed(credentialID);

    return {
      runID,
      ok: true,
      brand: ctx.brand,
      mode,
      chunks: windows.length,
      truncatedChunks: totals.truncatedChunks,
      truncated: totals.truncatedChunks > 0,
      truncatedReason: totals.truncatedChunks > 0
        ? totals.truncatedChunks + ' chunk(s) hit per-window cap — consider lowering chunkDays'
        : null,
      window: windows.length
        ? { after: windows[0].after, before: windows[windows.length-1].before }
        : null,
      pages: totals.pages,
      ordersProcessed: totals.orders,
      itemsProcessed: totals.items,
      ordersInserted: totals.ordersInserted,
      ordersUpdated: totals.ordersUpdated,
      ordersUnchanged: totals.ordersUnchanged,
      itemsInserted: totals.itemsInserted,
      itemsUpdated: totals.itemsUpdated,
      itemsUnchanged: totals.itemsUnchanged,
      eventsProcessed: totals.orders + totals.items,
    };
  } catch (e) {
    console.error('[runner/amz-orders]', e);
    await completeJobRun(runID, {
      status: 'FAILED',
      rowsIngested: null,
      errorMessage: (e.message || 'Unknown error').slice(0, 4000),
      workerType: 'NODE',
    });
    throw e;
  }
}

/**
 * Pull + flatten + dedupe + MERGE one window. Doesn't touch admin.JobRuns.
 */
async function pullMergeWindow(ctx, brandPool, runID, win, opts) {
  const { after, before } = win;
  const { mode, fetchItems } = opts;

  // Comma-join marketplaces; SP-API expects MarketplaceIds=A,B,C.
  // ctx.cred.MarketplaceIDs is already CSV in the BrandCredentials row.
  const marketplaceIds = String(ctx.cred.MarketplaceIDs || '')
    .split(',').map((s) => s.trim()).filter(Boolean);
  if (!marketplaceIds.length) {
    throw new Error('Credential has no MarketplaceIDs configured.');
  }

  const buildPath = (nextToken) => {
    if (nextToken) {
      // SP-API: NextToken is the *only* parameter you pass on subsequent
      // pages. Mixing it with original filters returns InvalidInput.
      return '/orders/v0/orders?NextToken=' + encodeURIComponent(nextToken);
    }
    const qs = [];
    qs.push('MarketplaceIds=' + marketplaceIds.map(encodeURIComponent).join(','));
    if (mode === 'UPDATED') {
      qs.push('LastUpdatedAfter='  + encodeURIComponent(after));
      if (before) qs.push('LastUpdatedBefore=' + encodeURIComponent(before));
    } else {
      qs.push('CreatedAfter=' + encodeURIComponent(after));
      if (before) qs.push('CreatedBefore=' + encodeURIComponent(before));
    }
    qs.push('MaxResultsPerPage=100');
    return '/orders/v0/orders?' + qs.join('&');
  };

  const allOrders = [];
  const { pages, hitCap, capReason } = await paginateSpApi(ctx, buildPath, (payload) => {
    if (Array.isArray(payload.Orders)) allOrders.push(...payload.Orders);
  }, {
    maxPages: opts.pageLimit, maxElapsedMs: opts.timeLimitMs,
    pageDelayMs: opts.pageDelayMs,
  });

  // Build header rows + dedupe inside batch (proc has its own ROW_NUMBER
  // safety net but trimming early makes batches smaller).
  const headerRows = [];
  const seen = new Set();
  for (const o of allOrders) {
    if (!o || !o.AmazonOrderId) continue;
    if (seen.has(o.AmazonOrderId)) continue;
    seen.add(o.AmazonOrderId);
    headerRows.push(toHeaderRow(o));
  }

  // Headers go in first — even if items fail, we have the order skeleton.
  let ordersInserted = 0, ordersUpdated = 0, ordersUnchanged = 0;
  for (let i = 0; i < headerRows.length; i += BATCH_SIZE) {
    const slice = headerRows.slice(i, i + BATCH_SIZE);
    const r = await execMergeBatch(brandPool, ctx.brand.BrandUID, runID, slice,
      'raw.usp_merge_amz_orders');
    ordersInserted  += r.Inserted;
    ordersUpdated   += r.Updated;
    ordersUnchanged += r.Unchanged;
  }

  // Item fetch. Optional — UPDATED mode skips by default (line-item data
  // is fixed at purchase time). Caller can force with fetchItems=true.
  let itemRows = [];
  if (fetchItems && allOrders.length) {
    itemRows = await fetchAllOrderItems(ctx, headerRows.map((r) => r.AmazonOrderID),
      opts.itemDelayMs, opts.timeLimitMs);
  }

  let itemsInserted = 0, itemsUpdated = 0, itemsUnchanged = 0;
  for (let i = 0; i < itemRows.length; i += BATCH_SIZE) {
    const slice = itemRows.slice(i, i + BATCH_SIZE);
    const r = await execMergeBatch(brandPool, ctx.brand.BrandUID, runID, slice,
      'raw.usp_merge_amz_order_items');
    itemsInserted  += r.Inserted;
    itemsUpdated   += r.Updated;
    itemsUnchanged += r.Unchanged;
  }

  return {
    orders: headerRows.length,
    items: itemRows.length,
    ordersInserted, ordersUpdated, ordersUnchanged,
    itemsInserted, itemsUpdated, itemsUnchanged,
    pages, truncated: hitCap, truncatedReason: capReason,
  };
}

/**
 * Fetch order items for every order ID, paginating per-order if Amazon
 * returns a NextToken. Returns flat array of item rows ready to merge.
 *
 * Big-window caveat: at 2.1s per call, 1000 orders = 35 min. The runner's
 * own per-window timeLimit guards against runaway, but if you're hitting
 * it consistently, lower chunkDays so each window has fewer orders.
 */
async function fetchAllOrderItems(ctx, orderIDs, itemDelayMs, timeLimitMs) {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const out = [];
  const startedAt = Date.now();

  for (let i = 0; i < orderIDs.length; i++) {
    if (Date.now() - startedAt >= timeLimitMs) {
      console.warn('[runner/amz-orders] item-fetch time budget exhausted at order ' + i + '/' + orderIDs.length);
      break;
    }
    const orderID = orderIDs[i];
    let nextToken = null;
    let pageCount = 0;
    do {
      const path = nextToken
        ? '/orders/v0/orders/' + encodeURIComponent(orderID) + '/orderItems?NextToken=' + encodeURIComponent(nextToken)
        : '/orders/v0/orders/' + encodeURIComponent(orderID) + '/orderItems';

      let resp;
      try {
        resp = await callSpApi(ctx, path);
      } catch (e) {
        // Don't kill the whole run for one missing order — just log and skip.
        console.warn('[runner/amz-orders] getOrderItems failed for ' + orderID + ': ' + e.message);
        break;
      }
      const payload = (resp && resp.payload) || {};
      const items = Array.isArray(payload.OrderItems) ? payload.OrderItems : [];
      for (const it of items) out.push(toItemRow(orderID, it));
      nextToken = payload.NextToken || null;
      pageCount++;

      if (nextToken && pageCount > 20) {
        console.warn('[runner/amz-orders] order ' + orderID + ' had >20 item pages — capping');
        nextToken = null;
      }
      if (nextToken || (i < orderIDs.length - 1)) {
        await sleep(itemDelayMs);
      }
    } while (nextToken);
  }

  return out;
}

/* =========================================================================
   Row shaping
   ========================================================================= */

function toHeaderRow(o) {
  // Hash inputs ordered by hand-curated set — every field that flips a
  // status / fulfillment / total gets included, so a real change flips the
  // hash. _RawPayload itself is NOT in the hash (it's a side-effect blob).
  const hashSeed = {
    AmazonOrderID:  o.AmazonOrderId,
    LastUpdatedDate: o.LastUpdateDate,
    OrderStatus:    o.OrderStatus,
    FulfillmentChannel: o.FulfillmentChannel,
    SalesChannel:   o.SalesChannel,
    OrderChannel:   o.OrderChannel,
    ShipServiceLevel: o.ShipServiceLevel,
    OrderTotal:     o.OrderTotal && o.OrderTotal.Amount,
    Currency:       o.OrderTotal && o.OrderTotal.CurrencyCode,
    NumberOfItemsShipped:   o.NumberOfItemsShipped,
    NumberOfItemsUnshipped: o.NumberOfItemsUnshipped,
    PaymentMethod:  o.PaymentMethod,
    BuyerEmail:     o.BuyerInfo && o.BuyerInfo.BuyerEmail,
    Ship: o.ShippingAddress
      ? {
          City: o.ShippingAddress.City,
          State: o.ShippingAddress.StateOrRegion,
          PostalCode: o.ShippingAddress.PostalCode,
          Country: o.ShippingAddress.CountryCode,
        }
      : null,
    Marketplace: { id: o.MarketplaceId, name: o.SalesChannel },
    flags: {
      biz: o.IsBusinessOrder, prime: o.IsPrime,
      replacement: o.IsReplacementOrder, sns: o.IsReplenishmentOrder,
    },
  };
  const hash = stableHash(hashSeed);

  return {
    AmazonOrderID:          o.AmazonOrderId,
    MerchantOrderID:        o.SellerOrderId || null,
    PurchaseDate:           o.PurchaseDate || null,
    LastUpdatedDate:        o.LastUpdateDate || null,
    OrderStatus:            o.OrderStatus || null,
    FulfillmentChannel:     o.FulfillmentChannel || null,
    SalesChannel:           o.SalesChannel || null,
    OrderChannel:           o.OrderChannel || null,
    ShipServiceLevel:       o.ShipServiceLevel || null,
    OrderTotal:             o.OrderTotal ? decimalForSql(o.OrderTotal.Amount) : null,
    Currency:               o.OrderTotal ? (o.OrderTotal.CurrencyCode || null) : null,
    NumberOfItemsShipped:   intOrNull(o.NumberOfItemsShipped),
    NumberOfItemsUnshipped: intOrNull(o.NumberOfItemsUnshipped),
    MarketplaceID:          o.MarketplaceId || null,
    MarketplaceName:        null,   // not provided per-order; SalesChannel is closest
    PaymentMethod:          o.PaymentMethod || null,
    BuyerEmail:             (o.BuyerInfo && o.BuyerInfo.BuyerEmail) || null,
    ShipCity:               (o.ShippingAddress && o.ShippingAddress.City) || null,
    ShipState:              (o.ShippingAddress && o.ShippingAddress.StateOrRegion) || null,
    ShipPostalCode:         (o.ShippingAddress && o.ShippingAddress.PostalCode) || null,
    ShipCountryCode:        (o.ShippingAddress && o.ShippingAddress.CountryCode) || null,
    IsBusinessOrder:        boolToString(o.IsBusinessOrder),
    IsPrime:                boolToString(o.IsPrime),
    IsReplacementOrder:     boolToString(o.IsReplacementOrder),
    // Amazon's name for SnS ("Subscribe & Save") in Orders API is
    // IsReplenishmentOrder.
    IsSnS:                  boolToString(o.IsReplenishmentOrder),
    _RawPayload:            JSON.stringify(o),
    _SourceRowHashHex:      hash.toString('hex'),
  };
}

function toItemRow(orderID, it) {
  const hashSeed = {
    AmazonOrderID: orderID, OrderItemID: it.OrderItemId,
    SKU: it.SellerSKU, ASIN: it.ASIN, Title: it.Title,
    Quantity: it.QuantityOrdered, QuantityShipped: it.QuantityShipped,
    ItemPrice: it.ItemPrice && it.ItemPrice.Amount,
    Currency: it.ItemPrice && it.ItemPrice.CurrencyCode,
    ItemTax: it.ItemTax && it.ItemTax.Amount,
    ShippingPrice: it.ShippingPrice && it.ShippingPrice.Amount,
    ShippingTax: it.ShippingTax && it.ShippingTax.Amount,
    GiftWrapPrice: it.GiftWrapPrice && it.GiftWrapPrice.Amount,
    GiftWrapTax: it.GiftWrapTax && it.GiftWrapTax.Amount,
    PromotionDiscount: it.PromotionDiscount && it.PromotionDiscount.Amount,
    ShippingDiscount: it.ShippingDiscount && it.ShippingDiscount.Amount,
    ConditionID: it.ConditionId,
  };
  const hash = stableHash(hashSeed);

  return {
    AmazonOrderID:          orderID,
    OrderItemID:            it.OrderItemId,
    ASIN:                   it.ASIN || null,
    SKU:                    it.SellerSKU || null,
    ProductName:            it.Title || null,
    Quantity:               intOrNull(it.QuantityOrdered),
    QuantityShipped:        intOrNull(it.QuantityShipped),
    ConditionID:            it.ConditionId || null,
    Currency:               (it.ItemPrice && it.ItemPrice.CurrencyCode) || null,
    ItemPrice:              it.ItemPrice ? decimalForSql(it.ItemPrice.Amount) : null,
    ItemTax:                it.ItemTax ? decimalForSql(it.ItemTax.Amount) : null,
    ShippingPrice:          it.ShippingPrice ? decimalForSql(it.ShippingPrice.Amount) : null,
    ShippingTax:            it.ShippingTax ? decimalForSql(it.ShippingTax.Amount) : null,
    GiftWrapPrice:          it.GiftWrapPrice ? decimalForSql(it.GiftWrapPrice.Amount) : null,
    GiftWrapTax:            it.GiftWrapTax ? decimalForSql(it.GiftWrapTax.Amount) : null,
    ItemPromotionDiscount:  it.PromotionDiscount ? decimalForSql(it.PromotionDiscount.Amount) : null,
    ShipPromotionDiscount:  it.ShippingDiscount ? decimalForSql(it.ShippingDiscount.Amount) : null,
    PromotionIDs:           Array.isArray(it.PromotionIds) ? it.PromotionIds.join(',') : null,
    _RawPayload:            JSON.stringify(it),
    _SourceRowHashHex:      hash.toString('hex'),
  };
}

/* =========================================================================
   DB helpers — same shape as financialEventsRunner
   ========================================================================= */

async function loadCredentialContext(credentialID) {
  const pool = await getPool();
  const r = await pool.request()
    .input('id', sql.Int, credentialID)
    .query(`
      SELECT bc.CredentialID, bc.BrandUID, bc.ConnectorID, bc.AccountIdentifier,
             bc.MarketplaceIDs, bc.Region, bc.RefreshToken_Enc, bc.AccessToken_Enc,
             bc.AccessTokenExpiresAt, bc.ApiKey_Enc, bc.AppSecret_Enc, bc.ExtraConfig,
             c.ConnectorUID, c.Name AS ConnectorName, c.DisplayName AS ConnectorDisplay,
             c.AuthType, c.BaseURL, c.AppClientID, c.AppClientSecret_Enc, c.ApiVersion,
             c.CredentialScope,
             b.BrandName, b.BrandSlug
      FROM admin.BrandCredentials bc
      JOIN admin.Connectors c ON c.ConnectorID = bc.ConnectorID
      JOIN admin.Brands     b ON b.BrandUID    = bc.BrandUID
      WHERE bc.CredentialID = @id
    `);
  if (!r.recordset.length) return null;
  const row = r.recordset[0];
  return {
    credentialID: row.CredentialID,
    brand: { BrandUID: row.BrandUID, BrandName: row.BrandName, BrandSlug: row.BrandSlug },
    connector: {
      ConnectorUID: row.ConnectorUID, Name: row.ConnectorName,
      DisplayName: row.ConnectorDisplay, AuthType: row.AuthType,
      BaseURL: row.BaseURL, AppClientID: row.AppClientID,
      AppClientSecret_Enc: row.AppClientSecret_Enc,
      ApiVersion: row.ApiVersion, CredentialScope: row.CredentialScope,
    },
    cred: {
      CredentialID: row.CredentialID, BrandUID: row.BrandUID,
      AccountIdentifier: row.AccountIdentifier,
      MarketplaceIDs: row.MarketplaceIDs, Region: row.Region,
      RefreshToken_Enc: row.RefreshToken_Enc,
      AccessToken_Enc: row.AccessToken_Enc,
      AccessTokenExpiresAt: row.AccessTokenExpiresAt,
      ApiKey_Enc: row.ApiKey_Enc, AppSecret_Enc: row.AppSecret_Enc,
      ExtraConfig: row.ExtraConfig,
    },
  };
}

async function resolveEndpointID(endpointName) {
  const pool = await getPool();
  const r = await pool.request()
    .input('n', sql.NVarChar(100), endpointName)
    .query(`
      SELECT e.EndpointID
      FROM admin.Endpoints e
      JOIN admin.Connectors c ON c.ConnectorID = e.ConnectorID
      WHERE c.Name = 'AMAZON_SP_API' AND e.Name = @n
    `);
  if (!r.recordset.length) {
    throw new Error('Endpoint ' + endpointName + ' not registered. Run db/sql/035_seed_orders_returns_endpoints.sql.');
  }
  return r.recordset[0].EndpointID;
}

async function ensureJob(endpointID, brandUID) {
  const pool = await getPool();
  const r = await pool.request()
    .input('eid', sql.Int, endpointID)
    .input('buid', sql.UniqueIdentifier, brandUID)
    .query(`
      SELECT TOP 1 JobID FROM admin.Jobs
      WHERE EndpointID = @eid AND BrandUID = @buid
      ORDER BY JobID ASC
    `);
  if (r.recordset.length) return r.recordset[0].JobID;

  const ins = await pool.request()
    .input('eid', sql.Int, endpointID)
    .input('buid', sql.UniqueIdentifier, brandUID)
    .query(`
      INSERT INTO admin.Jobs (EndpointID, BrandUID, JobType, IsActive, Priority, ConcurrencyKey)
      OUTPUT INSERTED.JobID
      VALUES (@eid, @buid, 'INGEST', 1, 50, 'AMZ_ORDERS:' + CAST(@buid AS NVARCHAR(50)));
    `);
  return ins.recordset[0].JobID;
}

async function startJobRun(jobID, triggeredBy) {
  const pool = await getPool();
  const r = await pool.request()
    .input('jid', sql.Int, jobID)
    .input('tb',  sql.NVarChar(30), triggeredBy)
    .query(`
      INSERT INTO admin.JobRuns (JobID, StartedAt, Status, TriggeredBy, WorkerType, WorkerHost)
      OUTPUT INSERTED.RunID
      VALUES (@jid, SYSUTCDATETIME(), 'RUNNING', @tb, 'NODE', HOST_NAME());
    `);
  return r.recordset[0].RunID;
}

async function updateJobRunProgress(runID, { chunksTotal, chunksCompleted, rowsIngested }) {
  const pool = await getPool();
  await pool.request()
    .input('rid', sql.BigInt, runID)
    .input('ct',  sql.Int, chunksTotal ?? null)
    .input('cc',  sql.Int, chunksCompleted ?? null)
    .input('ri',  sql.Int, rowsIngested ?? null)
    .query(`
      UPDATE admin.JobRuns
         SET ChunksTotal     = COALESCE(@ct, ChunksTotal),
             ChunksCompleted = COALESCE(@cc, ChunksCompleted),
             RowsIngested    = COALESCE(@ri, RowsIngested)
       WHERE RunID = @rid AND Status = 'RUNNING';
    `);
}

async function completeJobRun(runID, { status, rowsIngested, errorMessage, workerType }) {
  const pool = await getPool();
  let fingerprint = null;
  if (errorMessage) {
    fingerprint = crypto.createHash('sha256')
      .update(errorMessage.replace(/[0-9a-f-]{36}/gi, '<uuid>').replace(/\d+/g, '<n>'))
      .digest('hex').substring(0, 64);
  }
  await pool.request()
    .input('rid', sql.BigInt, runID)
    .input('s',   sql.NVarChar(20), status)
    .input('ri',  sql.Int, rowsIngested)
    .input('em',  sql.NVarChar(sql.MAX), errorMessage)
    .input('ef',  sql.NVarChar(64), fingerprint)
    .input('wt',  sql.NVarChar(30), workerType)
    .query(`
      UPDATE admin.JobRuns
      SET EndedAt          = SYSUTCDATETIME(),
          Status           = @s,
          RowsIngested     = @ri,
          ErrorMessage     = @em,
          ErrorFingerprint = @ef,
          WorkerType       = @wt
      WHERE RunID = @rid;
    `);
}

async function bumpLastAuthed(credentialID) {
  const pool = await getPool();
  await pool.request()
    .input('id', sql.Int, credentialID)
    .query(`
      UPDATE admin.BrandCredentials
      SET LastAuthedAt = SYSUTCDATETIME(),
          LastAuthError = NULL,
          UpdatedAt    = SYSUTCDATETIME()
      WHERE CredentialID = @id;
    `);
}

/**
 * Execute a MERGE proc against the brand data DB. Returns counts.
 * Same JSON+OPENJSON shape as financialEventsRunner — see notes there.
 */
async function execMergeBatch(brandPool, brandUID, runID, rows, procName) {
  if (!rows.length) return { Inserted: 0, Updated: 0, Unchanged: 0, Total: 0 };
  const rowsJson = JSON.stringify(rows);

  console.log('[runner/amz-orders/merge] proc=' + procName + ' batch size=' + rows.length +
    ' (' + rowsJson.length + ' chars)');

  try {
    const result = await brandPool.request()
      .input('BrandUID',    sql.UniqueIdentifier, brandUID)
      .input('SourceRunID', sql.BigInt,           runID)
      .input('RowsJson',    sql.NVarChar(sql.MAX), rowsJson)
      .execute(procName);
    const row = result.recordset && result.recordset[0];
    return row || { Inserted: 0, Updated: 0, Unchanged: 0, Total: rows.length };
  } catch (e) {
    const detail = {
      message:    e.message, code: e.code, number: e.number, state: e.state,
      class: e.class, lineNumber: e.lineNumber, procName: e.procName,
      serverName: e.serverName,
      infoMessage: e.info && e.info.message,
      originalMessage: e.originalError && e.originalError.message,
      precedingErrors: Array.isArray(e.precedingErrors)
        ? e.precedingErrors.map((p) => ({ n: p.number, m: p.message, ln: p.lineNumber }))
        : undefined,
    };
    console.error('[runner/amz-orders/merge] SQL error:', JSON.stringify(detail, null, 2));
    const wrapped = new Error(e.message + ' | ' + (detail.infoMessage || detail.originalMessage || '(no detail)'));
    wrapped.sqlDetail = detail;
    throw wrapped;
  }
}

/* =========================================================================
   Utilities
   ========================================================================= */

function stableHash(payload) {
  const canonical = JSON.stringify(payload, Object.keys(payload).sort());
  return crypto.createHash('sha256').update(canonical).digest();
}

function intOrNull(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.round(n) : null;
}

function boolToString(v) {
  if (v === true)  return 'true';
  if (v === false) return 'false';
  return null;
}

function decimalForSql(n) {
  if (n == null) return null;
  if (typeof n !== 'number') n = Number(n);
  if (!Number.isFinite(n)) return null;
  return n.toFixed(4);
}

module.exports = { runAmazonOrders };
