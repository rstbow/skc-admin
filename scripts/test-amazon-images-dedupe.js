/**
 * Sanity test for lib/amazonImages.js. Uses the exact 8-URL sample from
 * the app2 handoff (2026-04-25-listings-images-thumbnail-variants.md)
 * to confirm the dedupe collapses to 6 unique assets.
 */
const { uniqueAmazonAssets, extractAssetId, hasSizeModifier } = require('../lib/amazonImages');

let pass = 0, fail = 0;
function assert(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) { pass++; console.log('  ✓ ' + label); }
  else    { fail++; console.log('  ✗ ' + label);
            console.log('    expected:', JSON.stringify(expected));
            console.log('    actual:  ', JSON.stringify(actual)); }
}

console.log('extractAssetId:');
assert('canonical', extractAssetId('https://m.media-amazon.com/images/I/41aK6YQOHeL.jpg'), '41aK6YQOHeL');
assert('._SL75_', extractAssetId('https://m.media-amazon.com/images/I/41aK6YQOHeL._SL75_.jpg'), '41aK6YQOHeL');
assert('._AC_SX679_', extractAssetId('https://m.media-amazon.com/images/I/71dPFGRj96L._AC_SX679_.jpg'), '71dPFGRj96L');
assert('with + char', extractAssetId('https://m.media-amazon.com/images/I/41+51mjPqbL.jpg'), '41+51mjPqbL');
assert('non-Amazon', extractAssetId('https://example.com/foo.jpg'), null);
assert('null input', extractAssetId(null), null);

console.log('\nhasSizeModifier:');
assert('canonical → false', hasSizeModifier('https://m.media-amazon.com/images/I/41aK6YQOHeL.jpg'), false);
assert('._SL75_ → true', hasSizeModifier('https://m.media-amazon.com/images/I/41aK6YQOHeL._SL75_.jpg'), true);
assert('._AC_SX679_ → true', hasSizeModifier('https://m.media-amazon.com/images/I/X._AC_SX679_.jpg'), true);

console.log('\nuniqueAmazonAssets (handoff sample, SKU 0L-TPKQ-JHAK):');
const input = [
  'https://m.media-amazon.com/images/I/71dPFGRj96L.jpg',
  'https://m.media-amazon.com/images/I/41aK6YQOHeL.jpg',
  'https://m.media-amazon.com/images/I/41aK6YQOHeL._SL75_.jpg',
  'https://m.media-amazon.com/images/I/81Am5l5rEZL.jpg',
  'https://m.media-amazon.com/images/I/415T1ojQLML.jpg',
  'https://m.media-amazon.com/images/I/415T1ojQLML._SL75_.jpg',
  'https://m.media-amazon.com/images/I/71Cy0F9KkOL.jpg',
  'https://m.media-amazon.com/images/I/41+51mjPqbL.jpg',
];
const expected = [
  'https://m.media-amazon.com/images/I/71dPFGRj96L.jpg',
  'https://m.media-amazon.com/images/I/41aK6YQOHeL.jpg',
  'https://m.media-amazon.com/images/I/81Am5l5rEZL.jpg',
  'https://m.media-amazon.com/images/I/415T1ojQLML.jpg',
  'https://m.media-amazon.com/images/I/71Cy0F9KkOL.jpg',
  'https://m.media-amazon.com/images/I/41+51mjPqbL.jpg',
];
assert('8 → 6 unique, canonical kept, order preserved', uniqueAmazonAssets(input), expected);

console.log('\nuniqueAmazonAssets edge cases:');
assert('empty array', uniqueAmazonAssets([]), []);
assert('null arg', uniqueAmazonAssets(null), []);
assert('only thumbnail (no canonical)', uniqueAmazonAssets([
  'https://m.media-amazon.com/images/I/X._SL75_.jpg',
]), ['https://m.media-amazon.com/images/I/X._SL75_.jpg']);
assert('thumbnail then canonical → upgrades to canonical, keeps original ord', uniqueAmazonAssets([
  'https://m.media-amazon.com/images/I/X._SL75_.jpg',
  'https://m.media-amazon.com/images/I/Y.jpg',
  'https://m.media-amazon.com/images/I/X.jpg',
]), [
  'https://m.media-amazon.com/images/I/X.jpg',
  'https://m.media-amazon.com/images/I/Y.jpg',
]);
assert('non-Amazon URL passes through', uniqueAmazonAssets([
  'https://m.media-amazon.com/images/I/A.jpg',
  'https://example.com/custom.jpg',
]), [
  'https://m.media-amazon.com/images/I/A.jpg',
  'https://example.com/custom.jpg',
]);
assert('multiple sizes of same asset → one canonical', uniqueAmazonAssets([
  'https://m.media-amazon.com/images/I/Z._AC_SX679_.jpg',
  'https://m.media-amazon.com/images/I/Z._SL1500_.jpg',
  'https://m.media-amazon.com/images/I/Z.jpg',
  'https://m.media-amazon.com/images/I/Z._SL75_.jpg',
]), ['https://m.media-amazon.com/images/I/Z.jpg']);

console.log('\n' + pass + ' passed, ' + fail + ' failed.');
process.exit(fail ? 1 : 0);
