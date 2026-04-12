import { Router } from "express";
import { Pool } from "pg";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const VALID_CATS = ["policy", "literacy", "workforce", "change", "tools"] as const;

// Ensure table exists on startup
pool.query(`
  CREATE TABLE IF NOT EXISTS navigator_resources (
    id        SERIAL PRIMARY KEY,
    title     TEXT NOT NULL,
    source    TEXT NOT NULL,
    url       TEXT NOT NULL,
    category  TEXT NOT NULL,
    notes     TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )
`).catch(err => console.error("navigator_resources table init error:", err));

const resourcesRouter = Router();

// GET /api/resources
resourcesRouter.get("/resources", async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT id, title, source, url, category, notes, created_at
       FROM navigator_resources
       ORDER BY category, created_at DESC`
    );
    res.json({ resources: rows });
  } catch (err) {
    console.error("resources GET error:", err);
    res.status(500).json({ error: "Failed to fetch resources", resources: [] });
  }
});

// POST /api/resources  { title, source, url, category, notes? }
resourcesRouter.post("/resources", async (req, res) => {
  const { title, source, url, category, notes } = req.body as {
    title?: string;
    source?: string;
    url?: string;
    category?: string;
    notes?: string;
  };

  if (!title?.trim())    { res.status(400).json({ error: "title is required" }); return; }
  if (!source?.trim())   { res.status(400).json({ error: "source is required" }); return; }
  if (!url?.trim())      { res.status(400).json({ error: "url is required" }); return; }
  if (!category?.trim()) { res.status(400).json({ error: "category is required" }); return; }
  if (!VALID_CATS.includes(category as any)) {
    res.status(400).json({ error: "Invalid category", valid: VALID_CATS });
    return;
  }

  try {
    const { rows } = await pool.query(
      `INSERT INTO navigator_resources (title, source, url, category, notes)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, title, source, url, category, notes, created_at`,
      [title.trim(), source.trim(), url.trim(), category.trim(), notes?.trim() || null]
    );
    res.status(201).json({ resource: rows[0] });
  } catch (err) {
    console.error("resources POST error:", err);
    res.status(500).json({ error: "Failed to save resource" });
  }
});

// DELETE /api/resources/:id
resourcesRouter.delete("/resources/:id", async (req, res) => {
  try {
    const result = await pool.query(
      "DELETE FROM navigator_resources WHERE id = $1 RETURNING id",
      [req.params.id]
    );
    if (result.rowCount === 0) {
      res.status(404).json({ error: "Resource not found" });
      return;
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("resources DELETE error:", err);
    res.status(500).json({ error: "Failed to delete resource" });
  }
});

export default resourcesRouter;
