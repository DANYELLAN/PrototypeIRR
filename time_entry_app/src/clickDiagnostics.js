import { appendFile, mkdir, readFile, rename, stat } from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";

const kinds = new Set(["pointerdown", "click", "invalid", "submit", "submit_cancelled", "page_loaded", "script_error"]);
const clean = (value, limit = 160) => String(value ?? "").replace(/[\r\n\t]/g, " ").slice(0, limit);
const safePath = (value) => {
  try { return new URL(String(value), "http://local").pathname.slice(0, 160); } catch { return ""; }
};

export function requireIT(req, res, next) {
  res.set("Cache-Control", "no-store");
  if (!req.session?.cncUser) return res.redirect("/");
  if (!req.session.cncUser.roles?.includes("it")) {
    return res.status(403).send("Button Diagnostics is available to IT only.");
  }
  return next();
}

export function createClickDiagnostics(directory) {
  const filename = path.join(directory, "clicks.jsonl");
  let pending = Promise.resolve();
  const seen = new Set();
  function record(event) {
    const task = pending.then(async () => {
      await mkdir(directory, { recursive: true });
      if ((await stat(filename).catch(() => ({ size: 0 }))).size > 5 * 1024 * 1024) {
        for (let index = 3; index >= 1; index--) {
          await rename(`${filename}.${index}`, `${filename}.${index + 1}`).catch(() => {});
        }
        await rename(filename, `${filename}.1`);
      }
      await appendFile(filename, JSON.stringify({ ...event, received_at: new Date().toISOString() }) + "\n");
    });
    pending = task.catch((error) => console.error("Click diagnostics could not be saved:", error.code || "write error"));
    return task;
  }
  function identity(req) {
    const user = req.session?.cncUser;
    return { employee: clean(user?.employee?.emp_id), station: clean(user?.machine_no) };
  }
  return {
    record,
    async read() {
      await pending;
      const parts = await Promise.all([1, 0].map((index) => readFile(index ? `${filename}.1` : filename, "utf8").catch(() => "")));
      return parts.join("\n").split("\n").filter(Boolean).slice(-1000).flatMap((line) => {
        try { return [JSON.parse(line)]; } catch { return []; }
      });
    },
    async ingest(req, res) {
      // JSON-only, same-origin endpoint; never accept form values or free-text errors.
      if (!req.is("application/json") || (req.get("sec-fetch-site") && !["same-origin", "none"].includes(req.get("sec-fetch-site")))) return res.sendStatus(403);
      const events = req.body?.events;
      if (!Array.isArray(events) || events.length > 50) return res.sendStatus(400);
      try {
        for (const event of events) {
          if (!event || !kinds.has(event.kind)) continue;
          const id = clean(event.id, 80);
          if (!id || seen.has(id)) continue;
          await record({ ...identity(req), id, kind: event.kind, client_at: clean(event.client_at, 40),
            page: safePath(event.page), action: safePath(event.action), control: clean(event.control),
            interaction: clean(event.interaction, 80), browser: clean(event.browser, 80), disabled: event.disabled === true });
          seen.add(id);
          if (seen.size > 10000) seen.delete(seen.values().next().value);
        }
        res.sendStatus(204);
      } catch { res.sendStatus(503); }
    },
    middleware(req, res, next) {
      if (req.method !== "POST" || req.path === "/diagnostics/events") return next();
      const started = performance.now();
      const interaction = clean(req.body?._click_interaction, 80);
      const request = randomUUID();
      const actor = identity(req);
      void record({ ...actor, kind: "server_received", action: req.path, interaction, request }).catch(() => {});
      let finished = false;
      res.on("finish", () => {
        finished = true;
        void record({ ...actor, kind: "server_response", action: req.path, interaction, request,
          duration_ms: Math.round(performance.now() - started), status: res.statusCode,
          outcome: req.session?.notice?.kind || "response_sent" }).catch(() => {});
      });
      res.on("close", () => {
        if (!finished) void record({ ...actor, kind: "connection_closed", action: req.path, interaction, request,
          duration_ms: Math.round(performance.now() - started) }).catch(() => {});
      });
      next();
    },
  };
}
