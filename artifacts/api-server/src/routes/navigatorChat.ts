import { Router } from "express";
import { anthropic } from "@workspace/integrations-anthropic-ai";

const chatRouter = Router();

// ── Shared paper type ─────────────────────────────────────────────────────────
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

// ── arXiv ─────────────────────────────────────────────────────────────────────
async function searchArxiv(q: string, max: number): Promise<Paper[]> {
  const url = `https://export.arxiv.org/api/query?search_query=${encodeURIComponent("all:" + q)}&max_results=${max}&sortBy=relevance`;
  const r = await fetch(url, { signal: AbortSignal.timeout(9000) });
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
    const abstract = get("summary");
    if (!abstract) continue;
    papers.push({ id: `arxiv:${id}`, title: get("title"), abstract, authors, published: get("published").slice(0, 4), url: `https://arxiv.org/abs/${id}`, source: "arXiv", citationCount: 0 });
  }
  return papers;
}

// ── Semantic Scholar ──────────────────────────────────────────────────────────
async function searchSemanticScholar(q: string, max: number): Promise<Paper[]> {
  const url = `https://api.semanticscholar.org/graph/v1/paper/search?query=${encodeURIComponent(q)}&limit=${max}&fields=title,abstract,authors,year,externalIds,citationCount,openAccessPdf`;
  const r = await fetch(url, { headers: { "User-Agent": "NavigatorChat-ResearchHub/1.0" }, signal: AbortSignal.timeout(9000) });
  if (!r.ok) return [];
  const d = await r.json() as { data?: { paperId: string; title: string; abstract?: string; authors?: { name: string }[]; year?: number; citationCount?: number; externalIds?: { DOI?: string }; openAccessPdf?: { url?: string } }[] };
  return (d.data || [])
    .filter(p => p.abstract && p.abstract.length > 50)
    .map(p => ({
      id: `s2:${p.paperId}`,
      title: p.title || "",
      abstract: p.abstract || "",
      authors: (p.authors || []).map(a => a.name),
      published: p.year ? String(p.year) : "",
      url: p.openAccessPdf?.url || (p.externalIds?.DOI ? `https://doi.org/${p.externalIds.DOI}` : `https://www.semanticscholar.org/paper/${p.paperId}`),
      source: "Semantic Scholar",
      citationCount: p.citationCount || 0,
    }));
}

// ── ERIC ──────────────────────────────────────────────────────────────────────
async function searchERIC(q: string, max: number): Promise<Paper[]> {
  const url = `https://api.ies.ed.gov/eric/?search=${encodeURIComponent(q)}&format=json&rows=${max}&fields=id,title,abstract,author,publicationdateyear,url`;
  const r = await fetch(url, { signal: AbortSignal.timeout(9000) });
  if (!r.ok) return [];
  const d = await r.json() as { response?: { docs?: { id?: string; title?: string; abstract?: string; author?: string[]; publicationdateyear?: string; url?: string }[] } };
  return (d.response?.docs || [])
    .filter(p => p.abstract && p.abstract.length > 50)
    .map(p => ({
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

// ── OpenAlex ──────────────────────────────────────────────────────────────────
function invertedIndexToText(inv: Record<string, number[]> | null | undefined): string {
  if (!inv) return "";
  const words: [string, number][] = [];
  for (const [word, positions] of Object.entries(inv)) {
    for (const pos of positions) words.push([word, pos]);
  }
  words.sort((a, b) => a[1] - b[1]);
  return words.map(w => w[0]).join(" ");
}

async function searchOpenAlex(q: string, max: number): Promise<Paper[]> {
  const url = `https://api.openalex.org/works?search=${encodeURIComponent(q)}&per_page=${max}&select=id,title,abstract_inverted_index,authorships,publication_year,doi,cited_by_count&mailto=eduhub@research.org`;
  const r = await fetch(url, { signal: AbortSignal.timeout(9000) });
  if (!r.ok) return [];
  const d = await r.json() as { results?: { id?: string; title?: string; abstract_inverted_index?: Record<string, number[]>; authorships?: { author?: { display_name?: string } }[]; publication_year?: number; doi?: string; cited_by_count?: number }[] };
  return (d.results || [])
    .map(p => ({
      id: `oa:${(p.id || "").replace("https://openalex.org/", "")}`,
      title: p.title || "",
      abstract: invertedIndexToText(p.abstract_inverted_index),
      authors: (p.authorships || []).map(a => a.author?.display_name || "").filter(Boolean),
      published: p.publication_year ? String(p.publication_year) : "",
      url: p.doi ? `https://doi.org/${p.doi.replace("https://doi.org/", "")}` : (p.id || ""),
      source: "OpenAlex",
      citationCount: p.cited_by_count || 0,
    }))
    .filter(p => p.abstract.length > 50);
}

// ── Multi-source live search ──────────────────────────────────────────────────
async function searchLiterature(question: string, topK = 14): Promise<Paper[]> {
  const perSource = Math.ceil(topK / 2);

  const [arxiv, s2, eric, oa] = await Promise.allSettled([
    searchArxiv(question, perSource),
    searchSemanticScholar(question, perSource),
    searchERIC(question, Math.min(perSource, 8)),
    searchOpenAlex(question, perSource),
  ]);

  const seen = new Set<string>();
  const papers: Paper[] = [];

  const addBatch = (result: PromiseSettledResult<Paper[]>, src: string) => {
    if (result.status === "fulfilled") {
      for (const p of result.value) {
        const key = p.title.toLowerCase().slice(0, 60);
        if (!seen.has(key) && p.title && p.abstract) {
          seen.add(key);
          papers.push(p);
        }
      }
    } else {
      console.warn(`[navigator-chat] ${src} search failed:`, (result as PromiseRejectedResult).reason?.message);
    }
  };

  addBatch(s2, "Semantic Scholar");
  addBatch(eric, "ERIC");
  addBatch(oa, "OpenAlex");
  addBatch(arxiv, "arXiv");

  // Sort by citation count, then recency
  papers.sort((a, b) => {
    const cDiff = (b.citationCount || 0) - (a.citationCount || 0);
    if (cDiff !== 0) return cDiff;
    return parseInt(b.published || "0") - parseInt(a.published || "0");
  });

  return papers.slice(0, topK);
}

// ── System prompt ─────────────────────────────────────────────────────────────
function buildSystemPrompt(papers: Paper[]): string {
  const ctx = papers.map((p, i) => {
    const authors = p.authors.slice(0, 3).join(", ") + (p.authors.length > 3 ? " et al." : "");
    const cites = p.citationCount > 0 ? ` · ${p.citationCount.toLocaleString()} citations` : "";
    const abstract = p.abstract.slice(0, 600) + (p.abstract.length > 600 ? "…" : "");
    return (
      `[${i + 1}] "${p.title}" (${p.published || "n.d."}) — ${authors}. Source: ${p.source}${cites}\n` +
      `    Abstract: ${abstract}\n` +
      `    URL: ${p.url}`
    );
  }).join("\n\n");

  return `You are a sensemaking research assistant for the AI Literacy & Equity Navigator — a tool used by education researchers, program officers, policy analysts, and district leaders exploring AI's role in K-12 and higher education.

Your job is to synthesize insights across a live literature search into a structured evidence brief. Ground every claim in the papers provided and cite inline as [1], [2], etc.

You MUST structure your response using EXACTLY these six section headings in this order:

## Key Themes
Write 3–5 bullet points. Each bullet is one key pattern or theme with citations and evidence strength noted inline.

## Evidence Patterns
Write 4–6 bullet points. Each bullet is a single finding, convergence, divergence, or study-design observation. Include effect sizes or quantitative results where available. Do NOT use sub-headings like "Converging:" or "Diverging:".

## Equity Signals
Write 3–5 bullet points. Each bullet addresses one equity dimension — whose outcomes are measured, who is missing, what disparities are documented. Prefix bullets with [Present] or [Absent] to signal whether evidence exists.

## Headwinds & Tailwinds
Write 4–6 bullet points, one per force. Prefix each with 🔴 for a headwind or 🟢 for a tailwind. Do NOT create separate sub-sections. Interleave headwinds and tailwinds as a single list.

## Gaps & Tensions
Write 3–5 bullet points. Each bullet is one gap, contested finding, or methodological limit.

## Strategic Implications
Write 2–3 bullet points. Each bullet is one actionable insight for a program officer or strategy lead, grounded in the evidence above.

Rules:
- ALL sections MUST use bullet lists only. No prose paragraphs, no sub-headings, no bold-only header lines.
- ABSOLUTELY NO markdown tables. No pipes (|), no table headers, no column separators. Ever.
- Each bullet starts with - and flows as a single continuous sentence or two. Do NOT split a bullet into bold label + colon + description. Write it as ONE natural sentence where the key point is woven into the sentence.
- WRONG: "- **AI tools amplify inequities** : Algorithmic bias risks reinforcing... [9]. Evidence is moderate."
- RIGHT: "- AI tools can amplify existing K-12 inequities rather than neutralize them — algorithmic bias in tutoring systems risks reinforcing stratification [9], with evidence moderate but largely theoretical."
- Use plain language. Be honest when evidence is weak or absent.
- Cite inline as [1], [2], etc. within each bullet.
- Do NOT add extra sections or deviate from the six headings above.

LIVE LITERATURE SEARCH RESULTS (${papers.length} papers retrieved from Semantic Scholar, ERIC, OpenAlex, arXiv):
${ctx}`;
}

// ── POST /api/navigator/chat ──────────────────────────────────────────────────
chatRouter.post("/navigator/chat", async (req, res) => {
  const question = (req.body?.question ?? "").trim();
  if (!question) {
    res.status(400).json({ error: "question is required" });
    return;
  }

  // Set up SSE immediately so the client knows we're working
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.write(`data: ${JSON.stringify({ status: "searching" })}\n\n`);

  let papers: Paper[] = [];
  try {
    papers = await searchLiterature(question, 14);
  } catch (err) {
    console.error("[navigator-chat] literature search error:", err);
  }

  if (papers.length === 0) {
    res.write(`data: ${JSON.stringify({ error: "No relevant papers found across the literature databases. Try rephrasing your question." })}\n\n`);
    res.end();
    return;
  }

  // Send source metadata so the UI can display citation chips immediately
  res.write(`data: ${JSON.stringify({
    sources: papers.map((p, i) => ({
      idx: i + 1,
      id: p.id,
      title: p.title,
      year: p.published,
      authors: p.authors.slice(0, 3).join(", ") + (p.authors.length > 3 ? " et al." : ""),
      url: p.url,
      source: p.source,
      citationCount: p.citationCount,
    }))
  })}\n\n`);

  try {
    const stream = anthropic.messages.stream({
      model: "claude-sonnet-4-6",
      max_tokens: 8192,
      system: buildSystemPrompt(papers),
      messages: [{ role: "user", content: question }],
    });

    for await (const event of stream) {
      if (event.type === "content_block_delta" && event.delta.type === "text_delta") {
        res.write(`data: ${JSON.stringify({ content: event.delta.text })}\n\n`);
      }
    }

    res.write(`data: ${JSON.stringify({ done: true })}\n\n`);
  } catch (err) {
    console.error("[navigator-chat] LLM error:", err);
    res.write(`data: ${JSON.stringify({ error: "LLM synthesis error. Please try again." })}\n\n`);
  }

  res.end();
});

export default chatRouter;
