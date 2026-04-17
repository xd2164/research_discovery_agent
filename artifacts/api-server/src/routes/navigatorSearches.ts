import { Router } from "express";
import { Pool } from "pg";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const router = Router();

// GET /api/navigator/searches?limit=20
router.get("/navigator/searches", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit as string) || 20, 50);
  try {
    const { rows } = await pool.query(
      `SELECT id, question, synthesis_text, sources, paper_count, searched_at
       FROM navigator_searches
       ORDER BY searched_at DESC
       LIMIT $1`,
      [limit]
    );
    res.json({ searches: rows });
  } catch (err) {
    console.error("[navigator-searches] GET error:", err);
    res.status(500).json({ error: "Failed to fetch searches", searches: [] });
  }
});

// GET /api/navigator/searches/:id  — fetch full cached result for one entry
router.get("/navigator/searches/:id", async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT id, question, synthesis_text, sources, paper_count, searched_at
       FROM navigator_searches WHERE id = $1`,
      [req.params.id]
    );
    if (!rows.length) { res.status(404).json({ error: "Not found" }); return; }
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch search" });
  }
});

// POST /api/navigator/searches  { question, synthesis_text?, sources?, paper_count? }
router.post("/navigator/searches", async (req, res) => {
  const question = (req.body?.question ?? "").trim();
  if (!question) { res.status(400).json({ error: "question is required" }); return; }
  const synthesis_text = req.body?.synthesis_text ?? null;
  const sources       = req.body?.sources ?? null;
  const paper_count   = req.body?.paper_count ?? null;
  try {
    // Upsert: delete old duplicate, insert fresh at top
    await pool.query(`DELETE FROM navigator_searches WHERE question = $1`, [question]);
    const { rows } = await pool.query(
      `INSERT INTO navigator_searches (question, synthesis_text, sources, paper_count)
       VALUES ($1, $2, $3, $4)
       RETURNING id, searched_at`,
      [question, synthesis_text, sources ? JSON.stringify(sources) : null, paper_count]
    );
    res.json({ id: rows[0].id, searched_at: rows[0].searched_at });
  } catch (err) {
    console.error("[navigator-searches] POST error:", err);
    res.status(500).json({ error: "Failed to save search" });
  }
});

// DELETE /api/navigator/searches  (clear all)
router.delete("/navigator/searches", async (_req, res) => {
  try {
    await pool.query(`DELETE FROM navigator_searches`);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: "Failed to clear searches" });
  }
});

export default router;
