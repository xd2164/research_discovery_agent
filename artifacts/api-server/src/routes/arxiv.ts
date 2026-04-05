import { Router } from "express";

const arxivRouter = Router();

function parseArxivXml(xml: string) {
  const entries: unknown[] = [];
  const entryMatches = xml.match(/<entry>([\s\S]*?)<\/entry>/g) || [];
  for (const entry of entryMatches) {
    const get = (tag: string) => {
      const m = entry.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`));
      return m ? m[1].trim().replace(/\s+/g, " ") : "";
    };
    const getAll = (tag: string) => {
      const matches = entry.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "g")) || [];
      return matches.map(m => {
        const inner = m.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`));
        return inner ? inner[1].trim() : "";
      });
    };
    const idRaw = get("id");
    const id = idRaw.replace(/https?:\/\/arxiv\.org\/abs\//, "").replace(/v\d+$/, "");
    const published = get("published").slice(0, 10);
    const title = get("title");
    const abstract = get("summary");
    const authorNodes = entry.match(/<author>([\s\S]*?)<\/author>/g) || [];
    const authors = authorNodes.map(a => {
      const nm = a.match(/<name>([\s\S]*?)<\/name>/);
      return nm ? nm[1].trim() : "";
    }).filter(Boolean);
    const catMatches = entry.match(/term="([^"]+)"/g) || [];
    const categories = catMatches.map(c => c.replace('term="', "").replace('"', "")).filter(Boolean);
    entries.push({ id, title, abstract, authors, published, categories });
  }
  return entries;
}

// GET /api/arxiv?id=<arxivId>
arxivRouter.get("/arxiv", async (req, res) => {
  const raw = (req.query.id as string || "").trim();
  const id = raw.replace(/https?:\/\/.*?abs\//, "").replace(/v\d+$/, "").trim();
  if (!id) { res.status(400).json({ error: "id is required" }); return; }
  try {
    const r = await fetch(`https://export.arxiv.org/api/query?id_list=${encodeURIComponent(id)}&max_results=1`);
    const xml = await r.text();
    const entries = parseArxivXml(xml);
    if (!entries.length) { res.status(404).json({ error: "Paper not found" }); return; }
    res.json(entries[0]);
  } catch (err) {
    console.error("arXiv fetch error:", err);
    res.status(500).json({ error: "Could not fetch from arXiv" });
  }
});

// GET /api/arxiv/search?q=<query>&max=<n>
arxivRouter.get("/arxiv/search", async (req, res) => {
  const q = (req.query.q as string || "").trim();
  const max = Math.min(parseInt(req.query.max as string) || 6, 12);
  if (!q) { res.status(400).json({ error: "q is required" }); return; }
  try {
    const url = `https://export.arxiv.org/api/query?search_query=${encodeURIComponent(q)}&max_results=${max}&sortBy=relevance`;
    const r = await fetch(url);
    const xml = await r.text();
    const papers = parseArxivXml(xml);
    res.json({ papers });
  } catch (err) {
    console.error("arXiv search error:", err);
    res.status(500).json({ error: "Could not search arXiv" });
  }
});

export default arxivRouter;
