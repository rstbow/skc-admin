/**
 * Amazon image URL utilities.
 *
 * Amazon serves the same physical image asset at multiple resolutions via
 * URL modifiers inserted between the asset ID and the file extension:
 *
 *     https://m.media-amazon.com/images/I/41aK6YQOHeL.jpg              ← canonical
 *     https://m.media-amazon.com/images/I/41aK6YQOHeL._SL75_.jpg       ← 75px thumbnail
 *     https://m.media-amazon.com/images/I/41aK6YQOHeL._AC_SX679_.jpg   ← 679px width
 *     https://m.media-amazon.com/images/I/41aK6YQOHeL._AC_SL1500_.jpg  ← 1500px long-edge
 *
 * Catalog Items API's `images[].images[]` returns ALL of these variants
 * per asset — handy if you want a specific size, but pollutes our
 * `raw.amz_listings.ImagesJSON` with duplicates because the asset ID is
 * the same but the URL string differs.
 *
 * Filed by app2 in handoff 2026-04-25-listings-images-thumbnail-variants.md.
 */

/**
 * Pull the asset ID out of an Amazon /I/ image URL.
 * Returns null for any URL that doesn't match the /images/I/<id> shape.
 *
 * The asset ID is alphanumeric + the literal '+' (some old-style assets
 * use it as base-64 padding). The size modifier — if any — sits between
 * the ID and the extension, prefixed with '._'.
 */
function extractAssetId(url) {
  if (typeof url !== 'string' || !url) return null;
  // Match: /images/I/<asset-id> followed by ._SIZE_ or .ext
  const m = url.match(/\/images\/I\/([A-Za-z0-9+]+)(?:\._[A-Z0-9_]+_)?\.[a-zA-Z0-9]+/);
  return m ? m[1] : null;
}

/**
 * Detect whether an Amazon image URL has a size modifier (._SL75_,
 * ._AC_SX679_, etc.). Canonical URLs do not.
 */
function hasSizeModifier(url) {
  if (typeof url !== 'string' || !url) return false;
  return /\._[A-Z0-9_]+_\.[a-zA-Z0-9]+(?:[?#]|$)/.test(url);
}

/**
 * Dedupe a list of Amazon image URLs to one canonical URL per asset.
 *
 * For each asset ID:
 *   - Prefer a URL with NO size modifier (highest resolution by Amazon's convention).
 *   - Otherwise keep whichever variant we saw first.
 *
 * Preserves input order for unique assets — important because callers
 * pass MAIN-image-first lists and don't want shuffling.
 *
 * Non-Amazon URLs (no asset ID match) pass through as-is, deduped only by
 * exact string. This keeps the function safe for mixed-source inputs and
 * means a future image hosted elsewhere isn't accidentally dropped.
 */
function uniqueAmazonAssets(urls) {
  if (!Array.isArray(urls) || !urls.length) return [];

  const seenAssets = new Map();   // assetId -> { url, hasMod, ord }
  const seenExact  = new Set();   // raw URL string for non-asset items
  const passthrough = [];         // ordered list of non-Amazon URLs we kept
  let ord = 0;

  for (const u of urls) {
    if (typeof u !== 'string' || !u) continue;
    const id = extractAssetId(u);
    if (!id) {
      // Not an Amazon /I/ URL — keep first occurrence verbatim.
      if (!seenExact.has(u)) {
        seenExact.add(u);
        passthrough.push({ url: u, ord: ord++ });
      }
      continue;
    }
    const isThumb = hasSizeModifier(u);
    const prev = seenAssets.get(id);
    if (!prev) {
      seenAssets.set(id, { url: u, hasMod: isThumb, ord: ord++ });
    } else if (prev.hasMod && !isThumb) {
      // Upgrade: we previously kept a sized variant, now found canonical.
      // Keep the original ord so output order doesn't shuffle.
      seenAssets.set(id, { url: u, hasMod: false, ord: prev.ord });
    }
    // If prev is canonical or both are sized, keep prev.
  }

  return [...seenAssets.values(), ...passthrough]
    .sort((a, b) => a.ord - b.ord)
    .map((x) => x.url);
}

module.exports = { extractAssetId, hasSizeModifier, uniqueAmazonAssets };
