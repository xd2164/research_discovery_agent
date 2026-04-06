import { Router } from "express";
import { Pool } from "pg";
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const trackRouter = Router();

// POST /api/track  { event_type, meta }
trackRouter.post("/track", async (req, res) => {
  const { event_type, meta } = req.body as { event_type?: string; meta?: object };
  if (!event_type) { res.status(400).json({ error: "event_type required" }); return; }
  try {
    await pool.query(
      `INSERT INTO usage_events (event_type, meta) VALUES ($1, $2)`,
      [event_type, meta ? JSON.stringify(meta) : null]
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("track POST error:", err);
    res.status(500).json({ error: "Failed to record event" });
  }
});

// GET /api/track/stats — summary counts
trackRouter.get("/track/stats", async (_req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE event_type = 'page_view')   AS page_views,
        COUNT(*) FILTER (WHERE event_type = 'research_run') AS research_runs,
        COUNT(*) FILTER (WHERE event_type = 'history_load') AS history_loads,
        MIN(created_at) AS first_seen,
        MAX(created_at) AS last_seen
      FROM usage_events
    `);
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch stats" });
  }
});

export default trackRouter;
