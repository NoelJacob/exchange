import { Hono } from "hono";
import { redis } from "../lib/redis.js";

export const statusRouter = new Hono();

statusRouter.get("/:contestantId", async (c) => {
  const contestantId = c.req.param("contestantId");

  const status = await redis.get(`test:${contestantId}:status`);
  if (!status) {
    return c.json({ error: "Contestant not found" }, 404);
  }

  const submissionRaw = await redis.get(`submission:${contestantId}`);
  let submission: unknown = undefined;
  if (submissionRaw) {
    try {
      submission = JSON.parse(submissionRaw);
    } catch {
      submission = submissionRaw;
    }
  }

  return c.json({
    contestantId,
    status,
    submission: submission || undefined,
  });
});
