import { Hono } from "hono";
import { randomUUID } from "node:crypto";
import { redis } from "../lib/redis.js";
import { adminAuth } from "../lib/auth.js";
import { stopSandbox } from "../lib/sandbox.js";

export const adminRouter = new Hono();

adminRouter.use("*", adminAuth);

// POST /api/admin/contestant — create a new contestant with a unique upload token
adminRouter.post("/contestant", async (c) => {
  const body = await c.req.json<{ name?: string }>();
  const name = body.name || `contestant_${randomUUID().slice(0, 8)}`;
  const contestantId = randomUUID();
  const token = randomUUID();

  // Token → contestantId mapping, TTL 72h
  await redis.set(`token:${token}`, contestantId, "EX", 72 * 3600);
  // Contestant name
  await redis.set(`contestant:${contestantId}:name`, name);

  return c.json({
    contestantId,
    token,
    uploadUrl: `/submit/${token}`,
  });
});

// POST /api/admin/test/start — start a test for a contestant
adminRouter.post("/test/start", async (c) => {
  const body = await c.req.json<{ contestantId: string }>();
  const { contestantId } = body;
  if (!contestantId) {
    return c.json({ error: "contestantId required" }, 400);
  }

  await redis.set(`test:${contestantId}:status`, "running");
  return c.json({ status: "running", contestantId });
});

// POST /api/admin/test/stop — stop a test for a contestant
adminRouter.post("/test/stop", async (c) => {
  const body = await c.req.json<{ contestantId: string }>();
  const { contestantId } = body;
  if (!contestantId) {
    return c.json({ error: "contestantId required" }, 400);
  }

  await redis.set(`test:${contestantId}:status`, "stopped");
  await stopSandbox(contestantId);

  return c.json({ status: "stopped", contestantId });
});

// GET /api/admin/config — get current test configuration
adminRouter.get("/config", async (c) => {
  const config = await redis.hgetall("test:config");
  return c.json(config || {});
});

// PUT /api/admin/config — update test configuration
adminRouter.put("/config", async (c) => {
  const body = await c.req.json<Record<string, string>>();
  if (!body || Object.keys(body).length === 0) {
    return c.json({ error: "Empty config" }, 400);
  }

  await redis.hset("test:config", body);
  return c.json({ ok: true });
});
