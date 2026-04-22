/**
 * Marketplace metadata per connector, organized by region.
 * Used by the Credentials page to render region + marketplace pickers.
 *
 * Shape:
 *   window._SKCMarketplaces = {
 *     AMAZON_SP_API: {
 *       regions: [{ code, name, marketplaces: [{ id, name, country }] }],
 *       hasRegions: true,
 *       mustPickRegion: true,  // each region needs its own refresh token
 *     },
 *     WALMART_MP: { ... },
 *     ...
 *   };
 */
(function () {
  window._SKCMarketplaces = {
    AMAZON_SP_API: {
      hasRegions: true,
      mustPickRegion: true,
      regions: [
        {
          code: 'NA', name: 'North America',
          endpoint: 'https://sellingpartnerapi-na.amazon.com',
          marketplaces: [
            { id: 'ATVPDKIKX0DER',  name: 'United States', country: 'US' },
            { id: 'A2EUQ1WTGCTBG2', name: 'Canada',        country: 'CA' },
            { id: 'A1AM78C64UM0Y8', name: 'Mexico',        country: 'MX' },
            { id: 'A2Q3Y263D00KWC', name: 'Brazil',        country: 'BR' },
          ],
        },
        {
          code: 'EU', name: 'Europe',
          endpoint: 'https://sellingpartnerapi-eu.amazon.com',
          marketplaces: [
            { id: 'A1F83G8C2ARO7P', name: 'United Kingdom', country: 'UK' },
            { id: 'A1PA6795UKMFR9', name: 'Germany',        country: 'DE' },
            { id: 'A13V1IB3VIYZZH', name: 'France',         country: 'FR' },
            { id: 'APJ6JRA9NG5V4',  name: 'Italy',          country: 'IT' },
            { id: 'A1RKKUPIHCS9HS', name: 'Spain',          country: 'ES' },
            { id: 'A1805IZSGTT6HS', name: 'Netherlands',    country: 'NL' },
            { id: 'A2NODRKZP88ZB9', name: 'Sweden',         country: 'SE' },
            { id: 'A1C3SOZRARQ6R3', name: 'Poland',         country: 'PL' },
            { id: 'A33AVAJ2PDY3EV', name: 'Turkey',         country: 'TR' },
            { id: 'A17E79C6D8DWNP', name: 'Saudi Arabia',   country: 'SA' },
            { id: 'A2VIGQ35RCS4UG', name: 'UAE',            country: 'AE' },
            { id: 'ARBP9OOSHTCHU',  name: 'Egypt',          country: 'EG' },
            { id: 'A21TJRUUN4KGV',  name: 'India',          country: 'IN' },
            { id: 'AMEN7PMS3EDWL',  name: 'Belgium',        country: 'BE' },
          ],
        },
        {
          code: 'FE', name: 'Far East',
          endpoint: 'https://sellingpartnerapi-fe.amazon.com',
          marketplaces: [
            { id: 'A1VC38T7YXB528', name: 'Japan',     country: 'JP' },
            { id: 'A39IBJ37TRP1C6', name: 'Australia', country: 'AU' },
            { id: 'A19VAU5U5O7RUS', name: 'Singapore', country: 'SG' },
          ],
        },
      ],
      accountIdentifierLabel: 'Seller ID (Merchant Token)',
      accountIdentifierHelp: 'Your Amazon Seller ID (also called Merchant Token). Optional but strongly recommended for traceability and webhook identification. Different per region.',
    },

    WALMART_MP: {
      hasRegions: true,
      mustPickRegion: false,
      regions: [
        { code: 'US', name: 'Walmart US', marketplaces: [{ id: 'US', name: 'United States', country: 'US' }] },
        { code: 'CA', name: 'Walmart Canada', marketplaces: [{ id: 'CA', name: 'Canada', country: 'CA' }] },
        { code: 'MX', name: 'Walmart Mexico', marketplaces: [{ id: 'MX', name: 'Mexico', country: 'MX' }] },
      ],
      accountIdentifierLabel: 'Partner ID (Seller ID)',
      accountIdentifierHelp: 'Your Walmart Marketplace Partner ID, visible in Seller Center.',
    },

    TIKTOK_SHOP: {
      hasRegions: true,
      mustPickRegion: true,
      regions: [
        { code: 'US', name: 'TikTok Shop US',  marketplaces: [{ id: 'US', name: 'United States', country: 'US' }] },
        { code: 'UK', name: 'TikTok Shop UK',  marketplaces: [{ id: 'UK', name: 'United Kingdom', country: 'UK' }] },
        { code: 'SEA', name: 'TikTok Shop Southeast Asia',
          marketplaces: [
            { id: 'ID', name: 'Indonesia',  country: 'ID' },
            { id: 'MY', name: 'Malaysia',   country: 'MY' },
            { id: 'PH', name: 'Philippines',country: 'PH' },
            { id: 'SG', name: 'Singapore',  country: 'SG' },
            { id: 'TH', name: 'Thailand',   country: 'TH' },
            { id: 'VN', name: 'Vietnam',    country: 'VN' },
          ]
        },
      ],
      accountIdentifierLabel: 'Shop ID',
      accountIdentifierHelp: 'TikTok Shop ID for this seller.',
    },

    SHOPIFY: {
      hasRegions: false,
      mustPickRegion: false,
      accountIdentifierLabel: 'Shop domain',
      accountIdentifierHelp: 'Your *.myshopify.com domain, e.g. acme-co.myshopify.com. Not the custom storefront URL.',
    },

    EXTENSIV: {
      hasRegions: false,
      mustPickRegion: false,
      accountIdentifierLabel: '3PL / Tenant ID',
      accountIdentifierHelp: 'Extensiv 3PL or tenant identifier provisioned by Extensiv.',
    },

    SKU_COMPASS: {
      hasRegions: false,
      mustPickRegion: false,
      internal: true,
      accountIdentifierLabel: 'Not applicable',
      accountIdentifierHelp:
        'SKU Compass is an internal connector. It uses the brand\'s DataDbConnString from the Brands page — ' +
        'no per-brand credentials need to be added here.',
    },
  };
})();
