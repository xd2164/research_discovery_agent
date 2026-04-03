import { Router } from "express";

const aaRouter = Router();

aaRouter.get("/aa/models", async (_req, res) => {
  const apiKey = process.env["ARTIFICIAL_ANALYSIS_API_KEY"];
  if (!apiKey) {
    res.status(500).json({ error: "ARTIFICIAL_ANALYSIS_API_KEY not configured" });
    return;
  }

  try {
    const upstream = await fetch(
      "https://artificialanalysis.ai/api/v2/data/llms/models",
      { headers: { "x-api-key": apiKey } }
    );

    if (!upstream.ok) {
      res.status(upstream.status).json({ error: "Upstream API error" });
      return;
    }

    const data = await upstream.json();
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch from Artificial Analysis" });
  }
});

export default aaRouter;
