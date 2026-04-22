/**
 * Ad-hoc "test an endpoint against real data" routes.
 *
 * These are for validating that stored credentials work against real APIs
 * before the Phase 3 runner is built. They don't persist results.
 *
 * All routes require auth.
 */
const express = require('express');
const { sql, getPool } = require('../config/db');
const { requireAuth } = require('../middleware/auth');
const { callSpApi, paginateSpApi } = require('../lib/amazonApi');
const { fetchCogBySku } = require('../lib/brandDb');
const { logAction, reqMeta } = require('../db/queries/audit');

const router = express.Router();
router.use(requireAuth);

/**
 * Load a credential + its connector, ready for passing to lib/amazonApi.
 */
async function loadCredential(credentialID) {
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
      JOIN admin.Brands b     ON b.BrandUID    = bc.BrandUID
      WHERE bc.CredentialID = @id
    `);
  if (!r.recordset.length) return null;
  const row = r.recordset[0];
  return {
    credentialID: row.CredentialID,
    brand: { BrandUID: row.BrandUID, BrandName: row.BrandName, BrandSlug: row.BrandSlug },
    connector: {
      ConnectorUID: row.ConnectorUID,
      Name: row.ConnectorName,
      DisplayName: row.ConnectorDisplay,
      AuthType: row.AuthType,
      BaseURL: row.BaseURL,
      AppClientID: row.AppClientID,
      AppClientSecret_Enc: row.AppClientSecret_Enc,
      ApiVersion: row.ApiVersion,
      CredentialScope: row.CredentialScope,
    },
    cred: {
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
    },
  };
}

/**
 * GET /api/debug/amazon-credentials
 * Returns a light list of active Amazon credentials for UI dropdowns.
 */
router.get('/amazon-credentials', async (_req, res) => {
  try {
    const pool = await getPool();
    const r = await pool.request().query(`
      SELECT bc.CredentialID, bc.BrandUID, bc.Region, bc.AccountIdentifier,
             bc.MarketplaceIDs, bc.LastAuthedAt,
             b.BrandName, b.BrandSlug
      FROM admin.BrandCredentials bc
      JOIN admin.Brands b     ON b.BrandUID    = bc.BrandUID
      JOIN admin.Connectors c ON c.ConnectorID = bc.ConnectorID
      WHERE c.Name = 'AMAZON_SP_API' AND bc.IsActive = 1 AND b.IsActive = 1
      ORDER BY b.BrandName, bc.Region
    `);
    res.json({ credentials: r.recordset });
  } catch (e) {
    console.error('[debug/amazon-credentials]', e);
    res.status(500).json({ error: 'Failed to load credentials' });
  }
});

/**
 * POST /api/debug/amazon/financial-events
 * Body: { credentialID, daysBack }
 *
 * Calls GET /finances/v0/financialEvents for the last N days and summarizes
 * fees by type, plus returns a shipment-level breakdown.
 */
router.post('/amazon/financial-events', async (req, res) => {
  try {
    const { credentialID, daysBack, postedAfter: postedAfterOverride } = req.body || {};
    if (!credentialID) return res.status(400).json({ error: 'credentialID is required' });

    const ctx = await loadCredential(parseInt(credentialID, 10));
    if (!ctx) return res.status(404).json({ error: 'Credential not found' });
    if (ctx.connector.Name !== 'AMAZON_SP_API') {
      return res.status(400).json({ error: 'Only Amazon credentials are supported' });
    }

    // Determine time window — caller can pass explicit postedAfter (for "Today")
    // or a daysBack integer (default 7, capped at 90).
    let postedAfter;
    if (postedAfterOverride && typeof postedAfterOverride === 'string') {
      postedAfter = new Date(postedAfterOverride).toISOString();
    } else {
      const days = Math.max(1, Math.min(parseInt(daysBack, 10) || 7, 90));
      postedAfter = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    }
    const days = Math.max(
      0.04,
      Math.round((Date.now() - new Date(postedAfter).getTime()) / (1000 * 60 * 60 * 24) * 10) / 10
    );

    // Accumulator across pages
    const allEvents = {};
    const addToList = (listName, items) => {
      if (!items || !items.length) return;
      if (!allEvents[listName]) allEvents[listName] = [];
      allEvents[listName].push(...items);
    };

    const buildPath = (nextToken) => {
      if (nextToken) {
        return '/finances/v0/financialEvents?NextToken=' + encodeURIComponent(nextToken);
      }
      return '/finances/v0/financialEvents?PostedAfter=' + encodeURIComponent(postedAfter)
           + '&MaxResultsPerPage=100';
    };

    const { pages, hitCap, capReason, elapsedMs } = await paginateSpApi(ctx, buildPath, (payload) => {
      const events = payload.FinancialEvents || {};
      // Merge all known event lists (any list name ending in EventList)
      for (const key of Object.keys(events)) {
        if (key.endsWith('EventList') && Array.isArray(events[key])) {
          addToList(key, events[key]);
        }
      }
    }, { maxPages: 60, pageDelayMs: 300, maxElapsedMs: 60_000 });

    const summary = summarizeFinancialEvents(allEvents);
    const shipmentEvents = compactShipmentEvents(allEvents.ShipmentEventList || []);

    // Compute the actual date range covered (useful when truncated)
    let earliestPosted = null, latestPosted = null;
    for (const ev of allEvents.ShipmentEventList || []) {
      if (ev.PostedDate) {
        const t = new Date(ev.PostedDate).getTime();
        if (!earliestPosted || t < earliestPosted) earliestPosted = t;
        if (!latestPosted   || t > latestPosted)   latestPosted = t;
      }
    }

    // ---- COG lookup from brand's data DB ----
    const skuQtyMap = {}; // sku -> total qty shipped in window
    for (const ev of shipmentEvents) {
      for (const item of ev.items) {
        if (!item.sku) continue;
        skuQtyMap[item.sku] = (skuQtyMap[item.sku] || 0) + (item.quantity || 0);
      }
    }
    const skus = Object.keys(skuQtyMap);
    const cogResult = await fetchCogBySku(ctx.brand.BrandUID, skus);

    // Aggregate COG + per-SKU rollup
    let totalCog = 0;
    const cogBreakdown = []; // [{sku, qty, unitCog, totalCog, hasCog}]
    for (const sku of skus) {
      const qty = skuQtyMap[sku];
      const unitCog = cogResult.cogBySku[sku];
      if (unitCog != null) {
        totalCog += unitCog * qty;
        cogBreakdown.push({ sku, qty, unitCog, totalCog: round2(unitCog * qty), hasCog: true });
      } else {
        cogBreakdown.push({ sku, qty, unitCog: null, totalCog: 0, hasCog: false });
      }
    }
    cogBreakdown.sort((a, b) => (b.totalCog || 0) - (a.totalCog || 0));

    // Compute net profit
    const missingCog = cogBreakdown.filter((x) => !x.hasCog).length;
    const netProceeds = summary.netProceeds;
    const netProfit = round2(netProceeds - totalCog);
    const cogInfo = {
      available: !cogResult.unavailableReason,
      reason: cogResult.unavailableReason || null,
      column: cogResult.cogColumn,
      totalCog: round2(totalCog),
      skuCount: skus.length,
      missingCogCount: missingCog,
      breakdown: cogBreakdown.slice(0, 50), // cap for UI
    };

    await logAction({
      userID: req.user.userID,
      action: 'DEBUG_AMAZON_FINANCIAL_EVENTS',
      entityType: 'BrandCredential',
      entityID: String(credentialID),
      details: {
        daysBack: days,
        brand: ctx.brand.BrandName,
        shipmentCount: summary.shipmentCount,
        cogAvailable: cogInfo.available,
        cogMissing: missingCog,
      },
      ...reqMeta(req),
    });

    const totalEvents = Object.values(allEvents).reduce((n, arr) => n + (arr?.length || 0), 0);

    res.json({
      brand: ctx.brand,
      daysBack: days,
      postedAfter,
      summary,
      cog: cogInfo,
      netProfit,
      profitMarginPct: summary.sales.grossSales > 0
        ? round2((netProfit / summary.sales.grossSales) * 100)
        : null,
      pagination: {
        pages,
        totalEvents,
        hitCap,
        capReason,
        elapsedMs,
        eventsPerDay: days > 0 ? Math.round(totalEvents / days) : null,
        actualRange: {
          earliestPosted: earliestPosted ? new Date(earliestPosted).toISOString() : null,
          latestPosted:   latestPosted   ? new Date(latestPosted).toISOString()   : null,
        },
      },
      // First 20 shipment events for drill-down detail
      shipmentEvents: shipmentEvents.slice(0, 20),
      rawSample: {
        ShipmentEventCount: (allEvents.ShipmentEventList || []).length,
        RefundEventCount: (allEvents.RefundEventList || []).length,
        ServiceFeeEventCount: (allEvents.ServiceFeeEventList || []).length,
        AdjustmentEventCount: (allEvents.AdjustmentEventList || []).length,
        OtherEventListCounts: Object.fromEntries(
          Object.entries(allEvents)
            .filter(([k]) => !['ShipmentEventList','RefundEventList','ServiceFeeEventList','AdjustmentEventList'].includes(k))
            .map(([k, v]) => [k, v.length])
        ),
      },
    });
  } catch (e) {
    console.error('[debug/amazon/financial-events]', e);
    res.status(e.status || 500).json({
      error: e.message,
      details: e.response || null,
    });
  }
});

/**
 * Summarize FinancialEvents into totals by fee category.
 * Everything is in the shop currency (first non-null currency encountered wins).
 */
function summarizeFinancialEvents(events) {
  const currencies = new Set();
  let shipmentCount = 0;
  let orderCount = 0;
  let revenue = 0;             // Principal (what buyer paid for items)
  let shippingRevenue = 0;
  let tax = 0;
  let giftWrap = 0;
  let promotionDiscount = 0;   // negative amounts

  // Fees deducted
  let commissionFee = 0;
  let fbaFee = 0;
  let otherFees = 0;
  const otherFeeBreakdown = {}; // keyed by fee type

  let refundTotal = 0;
  let refundCount = 0;

  let serviceFeeTotal = 0;
  const serviceFeeBreakdown = {};

  let adjustmentTotal = 0;
  const adjustmentBreakdown = {};

  const orderIds = new Set();

  const addCur = (cur) => { if (cur) currencies.add(cur); };
  const val = (amt) => (amt && typeof amt.CurrencyAmount === 'number') ? amt.CurrencyAmount : 0;
  const valSigned = (amt) => val(amt);

  // ---- ShipmentEventList ----
  for (const ev of events.ShipmentEventList || []) {
    shipmentCount++;
    if (ev.AmazonOrderId) { orderIds.add(ev.AmazonOrderId); }

    for (const item of ev.ShipmentItemList || []) {
      for (const charge of item.ItemChargeList || []) {
        const amt = valSigned(charge.ChargeAmount);
        addCur(charge.ChargeAmount && charge.ChargeAmount.CurrencyCode);
        switch (charge.ChargeType) {
          case 'Principal':            revenue += amt; break;
          case 'Tax':                  tax += amt; break;
          case 'Shipping':
          case 'ShippingCharge':       shippingRevenue += amt; break;
          case 'GiftWrap':             giftWrap += amt; break;
          case 'ShippingTax':
          case 'GiftWrapTax':          tax += amt; break;
          case 'Discount':
          case 'ShippingDiscount':
          case 'PromotionAmount':      promotionDiscount += amt; break;
          default:                     /* ignore others in summary */ break;
        }
      }
      for (const fee of item.ItemFeeList || []) {
        const amt = valSigned(fee.FeeAmount);
        addCur(fee.FeeAmount && fee.FeeAmount.CurrencyCode);
        switch (fee.FeeType) {
          case 'Commission':                    commissionFee += amt; break;
          case 'FBAPerUnitFulfillmentFee':
          case 'FBAPerOrderFulfillmentFee':
          case 'FBAWeightBasedFee':             fbaFee += amt; break;
          default:
            otherFees += amt;
            otherFeeBreakdown[fee.FeeType] = (otherFeeBreakdown[fee.FeeType] || 0) + amt;
            break;
        }
      }
    }
  }
  orderCount = orderIds.size;

  // ---- RefundEventList ----
  for (const ev of events.RefundEventList || []) {
    refundCount++;
    for (const item of ev.ShipmentItemAdjustmentList || []) {
      for (const charge of item.ItemChargeAdjustmentList || []) {
        refundTotal += valSigned(charge.ChargeAmount);
        addCur(charge.ChargeAmount && charge.ChargeAmount.CurrencyCode);
      }
      for (const fee of item.ItemFeeAdjustmentList || []) {
        // Fees reimbursed on refund are positive to the seller (reduction of prior fee)
        refundTotal += valSigned(fee.FeeAmount);
      }
    }
  }

  // ---- ServiceFeeEventList (FBA monthly storage, LTSF, ads fees billed separately, etc.) ----
  for (const ev of events.ServiceFeeEventList || []) {
    for (const fee of ev.FeeList || []) {
      const amt = valSigned(fee.FeeAmount);
      addCur(fee.FeeAmount && fee.FeeAmount.CurrencyCode);
      serviceFeeTotal += amt;
      serviceFeeBreakdown[fee.FeeType] = (serviceFeeBreakdown[fee.FeeType] || 0) + amt;
    }
  }

  // ---- AdjustmentEventList ----
  for (const ev of events.AdjustmentEventList || []) {
    const amt = valSigned(ev.AdjustmentAmount);
    addCur(ev.AdjustmentAmount && ev.AdjustmentAmount.CurrencyCode);
    adjustmentTotal += amt;
    const key = ev.AdjustmentType || 'OTHER';
    adjustmentBreakdown[key] = (adjustmentBreakdown[key] || 0) + amt;
  }

  const grossSales = revenue + shippingRevenue + giftWrap;
  const totalSaleFees = commissionFee + fbaFee + otherFees;
  const netProceeds = grossSales + tax + promotionDiscount + totalSaleFees
                    + refundTotal + serviceFeeTotal + adjustmentTotal;

  return {
    currencies: Array.from(currencies),
    shipmentCount,
    orderCount,
    refundCount,

    sales: {
      productRevenue: round2(revenue),
      shippingRevenue: round2(shippingRevenue),
      giftWrap: round2(giftWrap),
      tax: round2(tax),
      promotionDiscount: round2(promotionDiscount),
      grossSales: round2(grossSales),
    },
    fees: {
      commission: round2(commissionFee),
      fba: round2(fbaFee),
      other: round2(otherFees),
      otherBreakdown: round2Obj(otherFeeBreakdown),
      totalSaleFees: round2(totalSaleFees),
    },
    refunds: {
      total: round2(refundTotal),
      count: refundCount,
    },
    serviceFees: {
      total: round2(serviceFeeTotal),
      breakdown: round2Obj(serviceFeeBreakdown),
    },
    adjustments: {
      total: round2(adjustmentTotal),
      breakdown: round2Obj(adjustmentBreakdown),
    },
    netProceeds: round2(netProceeds),

    // Quick derived rate for the period
    effectiveFeeRate: grossSales > 0
      ? Math.round(Math.abs(totalSaleFees + serviceFeeTotal) / grossSales * 10000) / 100
      : null,
  };
}

/** Trim each shipment event down to the most useful fields for UI display. */
function compactShipmentEvents(list) {
  return list.map((ev) => {
    let principal = 0, commission = 0, fba = 0, tax = 0, shipping = 0;
    const items = [];
    for (const item of ev.ShipmentItemList || []) {
      for (const c of item.ItemChargeList || []) {
        if (c.ChargeType === 'Principal') principal += c.ChargeAmount?.CurrencyAmount || 0;
        else if (c.ChargeType === 'Tax') tax += c.ChargeAmount?.CurrencyAmount || 0;
        else if (c.ChargeType === 'Shipping' || c.ChargeType === 'ShippingCharge') shipping += c.ChargeAmount?.CurrencyAmount || 0;
      }
      for (const f of item.ItemFeeList || []) {
        const amt = f.FeeAmount?.CurrencyAmount || 0;
        if (f.FeeType === 'Commission') commission += amt;
        else if (/^FBA/.test(f.FeeType)) fba += amt;
      }
      items.push({
        sku: item.SellerSKU,
        quantity: item.QuantityShipped,
        orderItemId: item.OrderItemId,
      });
    }
    return {
      amazonOrderId: ev.AmazonOrderId,
      marketplaceName: ev.MarketplaceName,
      postedDate: ev.PostedDate,
      principal: round2(principal),
      shipping: round2(shipping),
      tax: round2(tax),
      commission: round2(commission),
      fbaFee: round2(fba),
      itemCount: items.length,
      items,
    };
  });
}

function round2(n) { return Math.round((n + Number.EPSILON) * 100) / 100; }
function round2Obj(o) { const out = {}; for (const k in o) out[k] = round2(o[k]); return out; }

module.exports = router;
