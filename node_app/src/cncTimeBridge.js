import path from "path";
import { fileURLToPath } from "url";
import { spawn } from "child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const bridgeScript = path.resolve(__dirname, "..", "bridge", "cnc_time_bridge.py");

export function callCncBridge(action, extra = {}) {
  return new Promise((resolve, reject) => {
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
}
