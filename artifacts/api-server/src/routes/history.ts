import { Router } from "express";
import { Pool } from "pg";
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const historyRouter = Router();

// GET /api/history?limit=20
historyRouter.get("/history", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit as string) || 20, 50);
  try {
    const { rows } = await pool.query(
      `SELECT id, query, decision, summary, paper_count, sources, created_at
       FROM research_history
       ORDER BY created_at DESC
       LIMIT $1`,
      [limit]
    );
    res.json({ history: rows });
  } catch (err) {
    console.error("history GET error:", err);
    res.status(500).json({ error: "Failed to fetch history", history: [] });
  }
});

// POST /api/history  { query, decision, summary, paper_count, sources }
historyRouter.post("/history", async (req, res) => {
  const { query, decision, summary, paper_count, sources } = req.body as {
    query?: string;
    decision?: string;
    summary?: string;
    paper_count?: number;
    sources?: string[];
  };
  if (!query?.trim()) { res.status(400).json({ error: "query is required" }); return; }
  try {
    const { rows } = await pool.query(
      `INSERT INTO research_history (query, decision, summary, paper_count, sources)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, created_at`,
      [query.trim(), decision || null, summary || null, paper_count || 0, sources || []]
    );
    res.json({ id: rows[0].id, created_at: rows[0].created_at });
  } catch (err) {
    console.error("history POST error:", err);
    res.status(500).json({ error: "Failed to save history" });
  }
});

// DELETE /api/history/:id
historyRouter.delete("/history/:id", async (req, res) => {
  try {
    await pool.query("DELETE FROM research_history WHERE id = $1", [req.params.id]);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: "Failed to delete" });
  }
});

export default historyRouter;
