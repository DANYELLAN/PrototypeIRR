import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import { dailyTimeReviewMarkup } from "../src/dailyTimeReview.js";

const require = createRequire(import.meta.url);
const appRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const review = {
  required: true, labor_date: "2026-10-06", total: "11:15", snapshot_hash: "test-snapshot",
  entries: [
    { shift: "40", production_number: "EWO26-00094", operation_id: "0010", details_type: "Machining", status: "Pending Approval", total: "06:00" },
    { shift: "40", production_number: "EWO26-00094", operation_id: "0010", details_type: "DT", details_type_ii: "M1", status: "Pending Approval", total: "00:45" },
    { shift: "40", production_number: "EWO26-00105", operation_id: "0050", details_type: "Rebore", status: "Pending Approval", total: "04:30" },
  ],
};
assert.equal(dailyTimeReviewMarkup({ required: false }), "");
assert.match(dailyTimeReviewMarkup(review, { editing: true }), /Review Time/);
assert.doesNotMatch(dailyTimeReviewMarkup(review, { editing: true }), /<dialog/);
assert.match(dailyTimeReviewMarkup(review, { error: "<script>bad</script>" }), /&lt;script&gt;/);

const css = await readFile(path.join(appRoot, "public", "cnc-time.css"), "utf8");
const script = await readFile(path.join(appRoot, "public", "cnc-time.js"), "utf8");
if (process.env.DAILY_REVIEW_PREVIEW === "1") {
  const server = createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(`<html><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>${css}</style></head>
      <body>${dailyTimeReviewMarkup(review)}<script>${script}</script></body></html>`);
  });
  server.listen(3111, "127.0.0.1", () => console.log("Review preview: http://127.0.0.1:3111"));
  await new Promise((resolve) => process.on("SIGINT", () => server.close(resolve)));
  process.exit(0);
}
const { chromium } = require(process.env.PLAYWRIGHT_PACKAGE_PATH || "playwright");
const browser = await chromium.launch({ channel: process.env.PLAYWRIGHT_CHANNEL || "msedge", headless: true });
try {
  for (const viewport of [{ width: 1280, height: 900 }, { width: 768, height: 1024 }, { width: 390, height: 844 }]) {
    const page = await browser.newPage({ viewport });
    await page.route("**/*", (route) => route.abort());
    await page.setContent(`<html><head><style>${css}</style></head><body>${dailyTimeReviewMarkup(review)}</body></html>`);
    await page.addScriptTag({ content: script });
    await page.evaluate(() => window.dispatchEvent(new Event("load")));
    const dialog = page.locator("#daily-time-review");
    assert.equal(await dialog.evaluate((node) => node.open), true);
    assert.equal(await page.locator("tbody tr").count(), 3);
    await page.keyboard.press("Escape");
    assert.equal(await dialog.evaluate((node) => node.open), true);
    const box = await dialog.boundingBox();
    assert.ok(box.x >= 0 && box.y >= 0 && box.x + box.width <= viewport.width && box.y + box.height <= viewport.height);
    await page.locator("#daily-review-correct").click();
    assert.equal(await page.locator("#daily-review-confirm").isVisible(), true);
    const reason = page.locator("#daily-review-reason");
    assert.equal(await reason.evaluate((node) => node.checkValidity()), false);
    await reason.fill("  ");
    await page.locator("#daily-review-confirm button").click();
    assert.equal(await reason.evaluate((node) => node.checkValidity()), false);
    await reason.fill("Authorized overtime to finish this work order.");
    assert.equal(await reason.evaluate((node) => node.checkValidity()), true);
    const buttonBounds = await page.locator(".daily-review-actions button").evaluateAll((buttons) => buttons.map((button) => {
      const rect = button.getBoundingClientRect();
      return { x: rect.x, right: rect.right, scroll: button.scrollWidth, width: button.clientWidth };
    }));
    assert.ok(buttonBounds.every((button) => button.x >= 0 && button.right <= viewport.width && button.scroll <= button.width + 1));
    await page.screenshot({ path: path.join(appRoot, "tests", ".tmp", `daily-review-${viewport.width}.png`) });
    await page.close();
  }
  console.log("Daily time review UI passed at desktop, tablet, and mobile sizes.");
} finally {
  await browser.close();
}
