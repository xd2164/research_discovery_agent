import { Router } from "express";

const papersRouter = Router();

function parseArxivXml(xml: string, source = "arXiv") {
  const papers: unknown[] = [];
  const entryMatches = xml.match(/<entry>([\s\S]*?)<\/entry>/g) || [];
  for (const entry of entryMatches) {
    const get = (tag: string) => {
      const m = entry.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`));
      return m ? m[1].trim().replace(/\s+/g, " ") : "";
    };
    const idRaw = get("id");
    const id = idRaw.replace(/https?:\/\/arxiv\.org\/abs\//, "").replace(/v\d+$/, "");
    const published = get("published").slice(0, 10);
    const title = get("title");
    const abstract = get("summary");
    const authorNodes = entry.match(/<author>([\s\S]*?)<\/author>/g) || [];
    const authors = authorNodes
      .map((a) => { const nm = a.match(/<name>([\s\S]*?)<\/name>/); return nm ? nm[1].trim() : ""; })
      .filter(Boolean);
    const url = `https://arxiv.org/abs/${id}`;
    papers.push({ id, title, abstract, authors, published, url, source, citationCount: 0 });
  }
  return papers;
}

// GET /api/papers/search?q=<query>&max=<n>
papersRouter.get("/papers/search", async (req, res) => {
  const q = (req.query.q as string || "").trim();
  const max = Math.min(parseInt(req.query.max as string) || 12, 20);
  if (!q) { res.status(400).json({ error: "q is required" }); return; }

  try {
    const url = `https://export.arxiv.org/api/query?search_query=${encodeURIComponent("all:" + q)}&max_results=${max}&sortBy=relevance`;
    const r = await fetch(url);
    const xml = await r.text();
    const papers = parseArxivXml(xml, "arXiv");
    res.json({ papers });
  } catch (err) {
    console.error("papers/search error:", err);
    res.status(500).json({ error: "Search failed", papers: [] });
  }
});

export default papersRouter;
