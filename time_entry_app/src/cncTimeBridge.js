import path from "path";
import { fileURLToPath } from "url";
import { spawn } from "child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const bridgeScript = path.resolve(__dirname, "..", "bridge", "cnc_time_bridge.py");
const BRIDGE_CACHE_TTL_MS = 15000;
const bridgeCache = new Map();
const CACHEABLE_ACTIONS = new Set([
  "get_sign_in_context",
  "get_employee_lookup",
  "get_dashboard_context",
]);

function makeCacheKey(action, extra = {}) {
  return JSON.stringify({
    action,
    payload: Object.fromEntries(Object.entries(extra).sort(([left], [right]) => left.localeCompare(right))),
  });
}

export function callCncBridge(action, extra = {}) {
  const cacheable = CACHEABLE_ACTIONS.has(action);
  if (!cacheable) {
    bridgeCache.clear();
  }

  const cacheKey = makeCacheKey(action, extra);
  const now = Date.now();
  if (cacheable) {
    const cached = bridgeCache.get(cacheKey);
    if (cached && cached.expiresAt > now) {
      return cached.promise;
    }
    if (cached) {
      bridgeCache.delete(cacheKey);
    }
  }

  const promise = new Promise((resolve, reject) => {
    const payload = JSON.stringify({ action, ...extra });
    const child = spawn("python", [bridgeScript, payload], {
      cwd: path.resolve(__dirname, "..", ".."),
      windowsHide: true,
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", reject);
    child.on("close", (code) => {
      let parsed;
      try {
        parsed = JSON.parse(stdout.trim() || "{}");
      } catch (error) {
        return reject(new Error(stderr || stdout || `Bridge failed with code ${code}`));
      }

      if (code !== 0 || !parsed.ok) {
        return reject(new Error(parsed.error || stderr || "Bridge request failed."));
      }

      resolve(parsed.data);
    });
  });

  if (cacheable) {
    bridgeCache.set(cacheKey, {
      promise,
      expiresAt: now + BRIDGE_CACHE_TTL_MS,
    });
  }

  promise.catch(() => {
    bridgeCache.delete(cacheKey);
  });

  return promise;
}
