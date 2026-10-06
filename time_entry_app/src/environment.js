import path from "node:path";
import { fileURLToPath } from "node:url";

// Resolve from this module so launches from any working directory use the
// same settings as the Python backend. Existing environment values win.
const envFile = fileURLToPath(new URL("../../.env", import.meta.url));
try {
  process.loadEnvFile(path.resolve(envFile));
} catch (error) {
  if (error.code !== "ENOENT") throw error;
}
