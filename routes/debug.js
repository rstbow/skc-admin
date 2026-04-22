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
const { callSpApi } = require('../lib/amazonApi');
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
    const { credentialID, daysBack } = req.body || {};
    if (!credentialID) return res.status(400).json({ error: 'credentialID is required' });

    const ctx = await loadCredential(parseInt(credentialID, 10));
    if (!ctx) return res.status(404).json({ error: 'Credential not found' });
    if (ctx.connector.Name !== 'AMAZON_SP_API') {
      return res.status(400).json({ error: 'Only Amazon credentials are supported' });
    }

    const days = Math.max(1, Math.min(parseInt(daysBack, 10) || 7, 90));
    const postedAfter = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    // SP-API getListFinancialEvents — 0.5 rps rate limit, 30 burst
    // We'll fetch just the first page; pagination comes later.
    const path = '/finances/v0/financialEvents?PostedAfter=' + encodeURIComponent(postedAfter)
               + '&MaxResultsPerPage=100';
    const apiResp = await callSpApi(ctx, path);
    const events = (apiResp && apiResp.payload && apiResp.payload.FinancialEvents) || {};

    const summary = summarizeFinancialEvents(events);

    await logAction({
      userID: req.user.userID,
      action: 'DEBUG_AMAZON_FINANCIAL_EVENTS',
      entityType: 'BrandCredential',
      entityID: String(credentialID),
      details: { daysBack: days, brand: ctx.brand.BrandName, shipmentCount: summary.shipmentCount },
      ...reqMeta(req),
    });

    res.json({
      brand: ctx.brand,
      daysBack: days,
      postedAfter,
      summary,
      nextToken: apiResp && apiResp.payload && apiResp.payload.NextEventPublicationDate
                 ? null : (apiResp && apiResp.payload && apiResp.payload.NextToken) || null,
      // First 20 shipment events for drill-down detail
      shipmentEvents: compactShipmentEvents(events.ShipmentEventList || []).slice(0, 20),
      // Raw payload available for debugging but capped
      rawSample: {
        ShipmentEventCount: (events.ShipmentEventList || []).length,
        RefundEventCount: (events.RefundEventList || []).length,
        ServiceFeeEventCount: (events.ServiceFeeEventList || []).length,
        AdjustmentEventCount: (events.AdjustmentEventList || []).length,
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
