import { Router } from "express";

const papersRouter = Router();

interface Paper {
  id: string;
  title: string;
  abstract: string;
  authors: string[];
  published: string;
  url: string;
  source: string;
  citationCount: number;
}

// ── arXiv ──────────────────────────────────────────────────────────────────
async function searchArxiv(q: string, max: number): Promise<Paper[]> {
  const url = `https://export.arxiv.org/api/query?search_query=${encodeURIComponent("all:" + q)}&max_results=${max}&sortBy=relevance`;
  const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
  const xml = await r.text();
  const papers: Paper[] = [];
  const entries = xml.match(/<entry>([\s\S]*?)<\/entry>/g) || [];
  for (const entry of entries) {
    const get = (tag: string) => {
      const m = entry.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`));
      return m ? m[1].trim().replace(/\s+/g, " ") : "";
    };
    const idRaw = get("id");
    const id = idRaw.replace(/https?:\/\/arxiv\.org\/abs\//, "").replace(/v\d+$/, "");
    const authors = (entry.match(/<author>([\s\S]*?)<\/author>/g) || [])
      .map((a) => { const nm = a.match(/<name>([\s\S]*?)<\/name>/); return nm ? nm[1].trim() : ""; })
      .filter(Boolean);
    papers.push({
      id: `arxiv:${id}`,
      title: get("title"),
      abstract: get("summary"),
      authors,
      published: get("published").slice(0, 10),
      url: `https://arxiv.org/abs/${id}`,
      source: "arXiv",
      citationCount: 0,
    });
  }
  return papers;
}

// ── Semantic Scholar ────────────────────────────────────────────────────────
async function searchSemanticScholar(q: string, max: number): Promise<Paper[]> {
  const url = `https://api.semanticscholar.org/graph/v1/paper/search?query=${encodeURIComponent(q)}&limit=${max}&fields=title,abstract,authors,year,externalIds,citationCount,openAccessPdf`;
  const r = await fetch(url, {
    headers: { "User-Agent": "EDUAgent-ResearchHub/1.0" },
    signal: AbortSignal.timeout(8000),
  });
  if (!r.ok) return [];
  const d = await r.json() as { data?: { paperId: string; title: string; abstract?: string; authors?: { name: string }[]; year?: number; citationCount?: number; externalIds?: { DOI?: string; ArXiv?: string }; openAccessPdf?: { url?: string } }[] };
  return (d.data || []).map((p) => ({
    id: `s2:${p.paperId}`,
    title: p.title || "",
    abstract: p.abstract || "",
    authors: (p.authors || []).map((a) => a.name),
    published: p.year ? String(p.year) : "",
    url: p.openAccessPdf?.url || (p.externalIds?.DOI ? `https://doi.org/${p.externalIds.DOI}` : `https://www.semanticscholar.org/paper/${p.paperId}`),
    source: "Semantic Scholar",
    citationCount: p.citationCount || 0,
  }));
}

// ── ERIC (IES / Dept of Education) ─────────────────────────────────────────
async function searchERIC(q: string, max: number): Promise<Paper[]> {
  const url = `https://api.ies.ed.gov/eric/?search=${encodeURIComponent(q)}&format=json&rows=${max}&fields=id,title,abstract,author,publicationdateyear,url`;
  const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
  if (!r.ok) return [];
  const d = await r.json() as { response?: { docs?: { id?: string; title?: string; abstract?: string; author?: string[]; publicationdateyear?: string; url?: string }[] } };
  return (d.response?.docs || []).map((p) => ({
    id: `eric:${p.id || ""}`,
    title: p.title || "",
    abstract: p.abstract || "",
    authors: p.author || [],
    published: p.publicationdateyear ? String(p.publicationdateyear) : "",
    url: p.url || (p.id ? `https://eric.ed.gov/?id=${p.id}` : ""),
    source: "ERIC",
    citationCount: 0,
  }));
}

// ── OpenAlex ────────────────────────────────────────────────────────────────
function invertedIndexToText(inv: Record<string, number[]> | null | undefined): string {
  if (!inv) return "";
  const words: [string, number][] = [];
  for (const [word, positions] of Object.entries(inv)) {
    for (const pos of positions) words.push([word, pos]);
  }
  words.sort((a, b) => a[1] - b[1]);
  return words.map((w) => w[0]).join(" ");
}

async function searchOpenAlex(q: string, max: number): Promise<Paper[]> {
  const url = `https://api.openalex.org/works?search=${encodeURIComponent(q)}&per_page=${max}&select=id,title,abstract_inverted_index,authorships,publication_year,doi,cited_by_count&mailto=eduhub@research.org`;
  const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
  if (!r.ok) return [];
  const d = await r.json() as { results?: { id?: string; title?: string; abstract_inverted_index?: Record<string, number[]>; authorships?: { author?: { display_name?: string } }[]; publication_year?: number; doi?: string; cited_by_count?: number }[] };
  return (d.results || []).map((p) => ({
    id: `oa:${(p.id || "").replace("https://openalex.org/", "")}`,
    title: p.title || "",
    abstract: invertedIndexToText(p.abstract_inverted_index),
    authors: (p.authorships || []).map((a) => a.author?.display_name || "").filter(Boolean),
    published: p.publication_year ? String(p.publication_year) : "",
    url: p.doi ? `https://doi.org/${p.doi.replace("https://doi.org/", "")}` : (p.id || ""),
    source: "OpenAlex",
    citationCount: p.cited_by_count || 0,
  }));
}

// ── Route ───────────────────────────────────────────────────────────────────
papersRouter.get("/papers/search", async (req, res) => {
  const q = (req.query.q as string || "").trim();
  const max = Math.min(parseInt(req.query.max as string) || 12, 20);
  if (!q) { res.status(400).json({ error: "q is required" }); return; }

  const perSource = Math.ceil(max / 2);

  const [arxiv, s2, eric, oa] = await Promise.allSettled([
    searchArxiv(q, perSource),
    searchSemanticScholar(q, perSource),
    searchERIC(q, Math.min(perSource, 8)),
    searchOpenAlex(q, perSource),
  ]);

  const seen = new Set<string>();
  const papers: Paper[] = [];

  const addBatch = (result: PromiseSettledResult<Paper[]>, _src: string) => {
    if (result.status === "fulfilled") {
      for (const p of result.value) {
        const key = p.title.toLowerCase().slice(0, 60);
        if (!seen.has(key) && p.title) {
          seen.add(key);
          papers.push(p);
        }
      }
    } else {
      console.warn(`papers/search ${_src} failed:`, result.reason?.message);
    }
  };

  addBatch(arxiv, "arXiv");
  addBatch(s2, "Semantic Scholar");
  addBatch(eric, "ERIC");
  addBatch(oa, "OpenAlex");

  papers.sort((a, b) => (b.citationCount || 0) - (a.citationCount || 0));

  res.json({ papers });
});

export default papersRouter;
