import { Router } from "express";

const aaRouter = Router();

const CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes

let cachedData: unknown = null;
let cacheTimestamp = 0;

aaRouter.get("/aa/models", async (_req, res) => {
  const apiKey = process.env["ARTIFICIAL_ANALYSIS_API_KEY"];
  if (!apiKey) {
    res.status(500).json({ error: "ARTIFICIAL_ANALYSIS_API_KEY not configured" });
    return;
  }

  const now = Date.now();
  const cacheAge = now - cacheTimestamp;

  // Serve from cache if fresh
  if (cachedData && cacheAge < CACHE_TTL_MS) {
    res.setHeader("X-Cache", "HIT");
    res.setHeader("X-Cache-Age", String(Math.round(cacheAge / 1000)) + "s");
    res.json(cachedData);
    return;
  }

  try {
    const upstream = await fetch(
      "https://artificialanalysis.ai/api/v2/data/llms/models",
      { headers: { "x-api-key": apiKey } }
    );

    // Rate-limited — serve stale cache if we have it, otherwise 429
    if (upstream.status === 429) {
      if (cachedData) {
        res.setHeader("X-Cache", "STALE");
        res.setHeader("X-Cache-Age", String(Math.round(cacheAge / 1000)) + "s");
        res.json(cachedData);
        return;
      }
      res.status(429).json({ error: "Rate limited by upstream API — please wait a moment and try again." });
      return;
    }

    if (!upstream.ok) {
      // On any other error, serve stale if available
      if (cachedData) {
        res.setHeader("X-Cache", "STALE");
        res.json(cachedData);
        return;
      }
      res.status(upstream.status).json({ error: "Upstream API error" });
      return;
    }

    const data = await upstream.json();
    cachedData = data;
    cacheTimestamp = now;
    res.setHeader("X-Cache", "MISS");
    res.json(data);
  } catch (err) {
    if (cachedData) {
      res.setHeader("X-Cache", "STALE");
      res.json(cachedData);
      return;
    }
    res.status(500).json({ error: "Failed to fetch from Artificial Analysis" });
  }
});

export default aaRouter;
