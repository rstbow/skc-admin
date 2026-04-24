/**
 * Window chunking utility — slices a long date range into smaller chunks
 * any date-windowed runner can iterate over. Designed to be reusable
 * across connectors that have this problem:
 *   - Amazon SP-API financial events / orders / settlements
 *   - Shopify orders (via updated_at_min/max or created_at windows)
 *   - QuickBooks transactions (by TxnDate)
 *   - Walmart orders
 *
 * Why chunk:
 *   - Azure App Service HTTP response timeout is 230s — a one-shot
 *     180-day pull dies before it can return
 *   - Upstream rate limits mean large windows take an hour+ of wall time
 *   - Holding 500k event objects in memory for flatten+merge is ugly
 *   - If the App Service instance recycles mid-run, a chunked run only
 *     loses the chunk in flight; idempotent MERGE fills the gap on retry
 *
 * Contract:
 *   computeWindows({ postedAfter?, postedBefore?, daysBack?, chunkDays?,
 *                    endBufferMs? })
 *     → [{ after: ISO, before: ISO }, ...]
 *
 *   - If postedAfter / postedBefore are given, they bound the range directly.
 *   - Otherwise, daysBack (default 2) backs off from "now".
 *   - chunkDays (default 30) caps the width of each returned window.
 *   - Final chunk is short-ended at postedBefore/now so we don't pull
 *     events from the future.
 *   - endBufferMs (default 0) backs off the default "now" end by a safety
 *     margin. REQUIRED for SP-API Finances (which rejects PostedBefore
 *     values within ~2 min of wall clock). Runners for that API pass
 *     endBufferMs: 300_000 (5 min — conservative). For APIs without
 *     this quirk (Shopify, QBO), leave it at 0.
 *   - Guaranteed non-empty: returns at least one window, even if the
 *     requested range is < 1 day.
 */

const MS_PER_DAY = 86_400_000;
const MAX_CHUNKS = 120;  // 10 years at 30d chunks — safety valve

function computeWindows({ postedAfter, postedBefore, daysBack, chunkDays, endBufferMs } = {}) {
  const buffer = Math.max(0, parseInt(endBufferMs, 10) || 0);
  const end = postedBefore
    ? new Date(postedBefore)
    : new Date(Date.now() - buffer);
  let start;
  if (postedAfter) {
    start = new Date(postedAfter);
  } else {
    const d = Math.max(1, parseInt(daysBack, 10) || 2);
    start = new Date(end.getTime() - d * MS_PER_DAY);
  }
  if (start >= end) {
    // Degenerate but survivable — return one zero-width window so the
    // caller's loop runs once with no events.
    return [{ after: start.toISOString(), before: end.toISOString() }];
  }

  const chunkMs = Math.max(1, parseInt(chunkDays, 10) || 30) * MS_PER_DAY;
  const windows = [];
  let cursor = new Date(start);
  while (cursor < end) {
    const next = new Date(Math.min(cursor.getTime() + chunkMs, end.getTime()));
    windows.push({ after: cursor.toISOString(), before: next.toISOString() });
    cursor = next;
    if (windows.length >= MAX_CHUNKS) {
      // Safety — log + stop. Misconfigured daysBack shouldn't spin up 10k
      // chunks.
      console.warn('[windowChunks] chunk cap hit at', MAX_CHUNKS,
        '— input likely malformed. Range so far:', start.toISOString(), '→', cursor.toISOString());
      break;
    }
  }
  return windows;
}

module.exports = { computeWindows };
