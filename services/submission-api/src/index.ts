import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { redis } from "./lib/redis.js";
import { ensureBucket } from "./lib/minio.js";
import { submitRouter } from "./routes/submit.js";
import { adminRouter } from "./routes/admin.js";
import { statusRouter } from "./routes/status.js";

const app = new Hono();

// Health check
app.get("/api/health", (c) => c.json({ ok: true }));

// Mount routes
app.route("/submit", submitRouter);
app.route("/api/admin", adminRouter);
app.route("/api/status", statusRouter);

const PORT = parseInt(process.env.PORT || "3000", 10);

async function main() {
  // Ensure MinIO bucket exists on startup
  try {
    await ensureBucket();
  } catch (err) {
    console.warn("MinIO bucket init failed (may be transient):", (err as Error).message);
  }

  serve({ fetch: app.fetch, port: PORT });
  console.log(`submission-api listening on port ${PORT}`);
}

main().catch((err) => {
  console.error("Fatal startup error:", err);
  process.exit(1);
});

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("SIGTERM received, shutting down...");
  redis.disconnect();
  process.exit(0);
});

process.on("SIGINT", async () => {
  console.log("SIGINT received, shutting down...");
  redis.disconnect();
  process.exit(0);
});
