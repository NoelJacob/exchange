import { Hono } from "hono";
import { createHash } from "node:crypto";
import { redis } from "../lib/redis.js";
import { minioClient } from "../lib/minio.js";
import { buildSandboxImage } from "../lib/builder.js";
import { runSandbox, waitForPort } from "../lib/sandbox.js";
import { publishSubmission } from "../lib/streams.js";

export const submitRouter = new Hono();

submitRouter.get("/:token", async (c) => {
  const token = c.req.param("token");

  const contestantId = await redis.get(`token:${token}`);
  if (!contestantId) {
    return c.html(inlineForm(token, null), 404);
  }

  return c.html(inlineForm(token, contestantId));
});

submitRouter.post("/:token", async (c) => {
  const token = c.req.param("token");

  // 1. Validate token
  const contestantId = await redis.get(`token:${token}`);
  if (!contestantId) {
    return c.json({ error: "Invalid token" }, 404);
  }

  // 2. Parse binary from multipart
  const formData = await c.req.raw.formData();
  const file = formData.get("binary") as File | null;
  if (!file) {
    return c.json({ error: "No binary file uploaded; use field name 'binary'" }, 400);
  }

  const buffer = Buffer.from(await file.arrayBuffer());

  // 3. Compute SHA-256
  const sha256 = createHash("sha256").update(buffer).digest("hex");

  // 4. Store in MinIO
  const objectName = `${contestantId}/${sha256}.bin`;
  await minioClient.putObject("submissions", objectName, buffer);

  // 5. Record submission in Redis
  const submissionKey = `submission:${contestantId}`;
  await redis.set(submissionKey, JSON.stringify({ sha256, ts: Date.now() }));

  // 6. Mark status as uploaded
  const statusKey = `test:${contestantId}:status`;
  await redis.set(statusKey, "uploaded");

  // 7. Build sandbox Docker image from MinIO binary (binary never leaves MinIO volume)
  const minioPath = `myminio/submissions/${objectName}`;
  await buildSandboxImage(contestantId, sha256, minioPath);

  // 8. Run the sandbox container
  await runSandbox(contestantId);

  // 9. Wait for both FIX (9090) and WS (8080) ports
  const fixReady = await waitForPort(contestantId, 9090, 30_000);
  const wsReady = await waitForPort(contestantId, 8080, 30_000);

  if (!fixReady || !wsReady) {
    await redis.set(statusKey, "startup_failed");
    return c.json({
      status: "startup_failed",
      contestantId,
      detail: `fix=${fixReady}, ws=${wsReady}`,
    });
  }

  // 10. Mark as running
  await redis.set(statusKey, "running");

  // 11. Publish to stream for bot fleet
  await publishSubmission(contestantId, sha256);

  return c.json({ status: "running", contestantId });
});

function inlineForm(token: string, contestantId: string | null): string {
  const status = contestantId
    ? `<p>Upload binary for contestant <strong>${escapeHtml(contestantId)}</strong></p>`
    : '<p style="color:#c00">Invalid token.</p>';
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Submit Binary</title>
<style>
body { font-family: system-ui, sans-serif; max-width: 480px; margin: 3rem auto; }
form { display: flex; flex-direction: column; gap: 1rem; }
input[type="file"] { padding: 0.5rem; }
button { padding: 0.5rem 1rem; background: #2563eb; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
button:hover { background: #1d4ed8; }
</style>
</head>
<body>
<h1>Submit Binary</h1>
${status}
<form action="/submit/${escapeHtml(token)}" method="post" enctype="multipart/form-data">
  <input type="file" name="binary" required>
  <button type="submit">Upload</button>
</form>
</body>
</html>`;
}

function escapeHtml(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
