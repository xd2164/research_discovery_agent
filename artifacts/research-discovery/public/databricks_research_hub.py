# Databricks notebook source
# Research & Discovery — Data & AI Enablement Hub
# Databricks-ready port of Evidence Scout + FP&A Research Agent + Knowledge Graph
#
# Prerequisites
#   Cluster: DBR 14.3 LTS ML (Python 3.10+, Spark 3.5)
#   Secrets: databricks secrets put --scope research-hub --key anthropic-api-key
#            databricks secrets put --scope research-hub --key semanticscholar-api-key (optional)
#
# Catalog / Schema: set CATALOG and SCHEMA below, or override per cell.

# COMMAND ----------
# ── 0. Install dependencies ──────────────────────────────────────────────────
%pip install anthropic httpx tenacity networkx --quiet

# COMMAND ----------
# ── 1. Imports & configuration ───────────────────────────────────────────────
import json, re, time, math, textwrap, xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

import httpx
import anthropic
from tenacity import retry, stop_after_attempt, wait_exponential
from pyspark.sql import functions as F, types as T
import networkx as nx

# ── Secrets ──────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = dbutils.secrets.get("research-hub", "anthropic-api-key")
SS_API_KEY        = dbutils.secrets.get("research-hub", "semanticscholar-api-key") if True else None

# ── Storage paths (Unity Catalog or hive_metastore) ──────────────────────────
CATALOG  = "research_hub"   # change to your catalog
SCHEMA   = "evidence"       # change to your schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

TABLE_PAPERS    = f"{CATALOG}.{SCHEMA}.papers"
TABLE_SCORECARD = f"{CATALOG}.{SCHEMA}.evidence_scorecard"
TABLE_FPA       = f"{CATALOG}.{SCHEMA}.fpa_briefs"
TABLE_ROI       = f"{CATALOG}.{SCHEMA}.roi_parameters"
TABLE_GRAPH     = f"{CATALOG}.{SCHEMA}.knowledge_graph_edges"

claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
print("✓ Config ready")

# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION A — arXiv Paper Fetcher
# Mirrors: Evidence Scout → Step 1 (fetchArxiv) + api-server/src/routes/arxiv.ts
# ══════════════════════════════════════════════════════════════════════════════

ARXIV_NS = "http://www.w3.org/2005/Atom"

def _parse_arxiv_id(raw: str) -> str:
    """Accepts arXiv ID, /abs/, /pdf/, /html/ URLs — returns bare ID like 2304.03442."""
    s = raw.strip()
    # strip URL prefix
    for prefix in ["https://arxiv.org/abs/", "https://arxiv.org/pdf/",
                   "https://arxiv.org/html/", "http://arxiv.org/abs/"]:
        if s.startswith(prefix):
            s = s[len(prefix):]
    s = s.rstrip("/")
    # strip .pdf extension
    if s.endswith(".pdf"):
        s = s[:-4]
    # strip version suffix vN
    s = re.sub(r"v\d+$", "", s)
    return s.strip()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
def fetch_arxiv_paper(arxiv_id_or_url: str) -> dict:
    """Fetch a single arXiv paper and return a normalised dict."""
    arxiv_id = _parse_arxiv_id(arxiv_id_or_url)
    url = f"https://export.arxiv.org/api/query?id_list={arxiv_id}&max_results=1"
    resp = httpx.get(url, timeout=20)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    entry = root.find(f"{{{ARXIV_NS}}}entry")
    if entry is None:
        raise ValueError(f"arXiv ID not found: {arxiv_id}")

    def txt(tag):
        el = entry.find(f"{{{ARXIV_NS}}}{tag}")
        return el.text.strip() if el is not None and el.text else ""

    authors = [a.find(f"{{{ARXIV_NS}}}name").text.strip()
               for a in entry.findall(f"{{{ARXIV_NS}}}author")
               if a.find(f"{{{ARXIV_NS}}}name") is not None]

    pdf_link = ""
    for link in entry.findall(f"{{{ARXIV_NS}}}link"):
        if link.attrib.get("title") == "pdf":
            pdf_link = link.attrib.get("href", "")

    categories = [c.attrib.get("term", "")
                  for c in entry.findall("{http://arxiv.org/schemas/atom}category")]

    return {
        "id":          arxiv_id,
        "title":       txt("title").replace("\n", " "),
        "abstract":    txt("summary").replace("\n", " "),
        "authors":     authors,
        "published":   txt("published")[:10],
        "updated":     txt("updated")[:10],
        "categories":  categories,
        "pdf_url":     pdf_link,
        "source":      "arXiv",
        "fetched_at":  datetime.utcnow().isoformat(),
    }

def fetch_arxiv_batch(ids: list[str]) -> list[dict]:
    """Fetch multiple arXiv papers; skips failures."""
    results = []
    for raw_id in ids:
        try:
            results.append(fetch_arxiv_paper(raw_id))
            time.sleep(0.4)   # arXiv rate limit: ~3 req/s
        except Exception as e:
            print(f"  ⚠ Skipped {raw_id}: {e}")
    return results

# ── Test fetch ────────────────────────────────────────────────────────────────
paper = fetch_arxiv_paper("2304.03442")
print(f"✓ Fetched: {paper['title'][:70]}…")
print(f"  Authors : {', '.join(paper['authors'][:3])}")
print(f"  Published: {paper['published']}")

# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION B — Multi-Source Paper Search
# Mirrors: FP&A Agent → parallel search across Semantic Scholar + arXiv
# ══════════════════════════════════════════════════════════════════════════════

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
def search_semantic_scholar(query: str, max_results: int = 12) -> list[dict]:
    headers = {"x-api-key": SS_API_KEY} if SS_API_KEY else {}
    params  = {
        "query":  query,
        "limit":  max_results,
        "fields": "title,abstract,authors,year,citationCount,externalIds,url",
    }
    resp = httpx.get("https://api.semanticscholar.org/graph/v1/paper/search",
                     headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    papers = []
    for p in resp.json().get("data", []):
        papers.append({
            "id":            p.get("paperId", ""),
            "title":         p.get("title", ""),
            "abstract":      (p.get("abstract") or "")[:1000],
            "authors":       [a["name"] for a in (p.get("authors") or [])],
            "published":     str(p.get("year", ""))[:4],
            "citationCount": p.get("citationCount", 0),
            "url":           p.get("url", ""),
            "source":        "Semantic Scholar",
            "fetched_at":    datetime.utcnow().isoformat(),
        })
    return papers

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
def search_arxiv(query: str, max_results: int = 12) -> list[dict]:
    params = {
        "search_query": f"all:{query}",
        "start":        0,
        "max_results":  max_results,
        "sortBy":       "relevance",
    }
    resp = httpx.get("https://export.arxiv.org/api/query", params=params, timeout=20)
    resp.raise_for_status()
    root    = ET.fromstring(resp.text)
    papers  = []
    for entry in root.findall(f"{{{ARXIV_NS}}}entry"):
        def _t(tag):
            el = entry.find(f"{{{ARXIV_NS}}}{tag}")
            return el.text.strip() if el is not None and el.text else ""
        raw_id = _t("id").split("/abs/")[-1]
        papers.append({
            "id":          _parse_arxiv_id(raw_id),
            "title":       _t("title").replace("\n", " "),
            "abstract":    _t("summary").replace("\n", " ")[:1000],
            "authors":     [a.find(f"{{{ARXIV_NS}}}name").text.strip()
                            for a in entry.findall(f"{{{ARXIV_NS}}}author")
                            if a.find(f"{{{ARXIV_NS}}}name") is not None],
            "published":   _t("published")[:10],
            "citationCount": 0,
            "url":         f"https://arxiv.org/abs/{_parse_arxiv_id(raw_id)}",
            "source":      "arXiv",
            "fetched_at":  datetime.utcnow().isoformat(),
        })
    return papers

def search_papers(query: str, max_results: int = 12) -> list[dict]:
    """Search Semantic Scholar first; fall back to arXiv on failure."""
    try:
        results = search_semantic_scholar(query, max_results)
        if results:
            return results
    except Exception as e:
        print(f"  SS fallback to arXiv ({e})")
    try:
        return search_arxiv(query, max_results)
    except Exception as e:
        print(f"  arXiv search failed: {e}")
    return []

def multi_angle_search(question: str, angles: list[dict] | None = None,
                       max_per_angle: int = 12) -> list[dict]:
    """
    Parallel multi-angle paper search — mirrors FP&A Agent's search strategy.
    angles: list of {"angle": str, "query": str}
    Falls back to auto-generated angles if not provided.
    """
    if not angles:
        words = re.sub(r"[^a-z0-9\s]", "", question.lower()).split()
        kw    = " ".join(w for w in words if len(w) > 3)[:60]
        angles = [
            {"angle": "Intervention evidence",  "query": f"{kw} AI education intervention outcomes"},
            {"angle": "Learning outcomes",       "query": f"{kw} student learning achievement AI"},
            {"angle": "Systematic review",       "query": f"{kw} meta-analysis systematic review education"},
            {"angle": "Equity & access",         "query": f"{kw} equity access underserved students AI"},
        ]

    from concurrent.futures import ThreadPoolExecutor, as_completed
    all_papers, seen = [], set()

    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(search_papers, a["query"], max_per_angle): a for a in angles}
        for fut in as_completed(futs):
            for p in fut.result():
                yr = int((p.get("published") or "0")[:4] or 0)
                if p["id"] not in seen and yr >= 2015:
                    seen.add(p["id"])
                    all_papers.append(p)

    all_papers.sort(key=lambda p: -(p.get("citationCount") or 0))
    print(f"✓ Multi-angle search: {len(all_papers)} unique papers from {len(angles)} angles")
    return all_papers

# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION C — Evidence Scorecard (18 indicators × 6 dimensions)
# Mirrors: Evidence Scout → Evidence Score button + runEvidenceStrength()
# ══════════════════════════════════════════════════════════════════════════════

EV_DIMS = [
    {"key": "studyRigor",      "label": "Study Rigor",           "indicators": [
        {"key": "designType",  "label": "Study design",             "desc": "5=RCT/meta-analysis · 3=observational · 1=opinion/theoretical"},
        {"key": "controls",    "label": "Methodological controls",  "desc": "5=blinding+randomization+confounders · 3=some controls · 1=none"},
        {"key": "stats",       "label": "Statistical rigor",        "desc": "5=appropriate methods+power+corrections · 3=basic stats · 1=absent"},
    ]},
    {"key": "sampleScale",     "label": "Sample & Scale",        "indicators": [
        {"key": "size",        "label": "Sample size",              "desc": "5=large adequate N · 3=moderate · 1=tiny/unclear"},
        {"key": "represent",   "label": "Representativeness",       "desc": "5=diverse multi-demographic · 3=single group · 1=non-representative"},
        {"key": "sites",       "label": "Setting diversity",        "desc": "5=multi-site multi-country · 3=single site · 1=lab only"},
    ]},
    {"key": "effectPrecision", "label": "Effect Precision",      "indicators": [
        {"key": "magnitude",   "label": "Effect size",              "desc": "5=large meaningful ES reported · 3=direction only · 1=absent"},
        {"key": "ci",          "label": "Confidence intervals",     "desc": "5=CI reported + meaningful · 3=p-values only · 1=not reported"},
        {"key": "outcomes",    "label": "Outcome validity",         "desc": "5=validated pre-registered instruments · 3=standard measures · 1=ad-hoc"},
    ]},
    {"key": "replication",     "label": "Replication Support",   "indicators": [
        {"key": "direct",      "label": "Direct replications",      "desc": "5=multiple independent replications · 3=one replication · 1=none"},
        {"key": "conceptual",  "label": "Conceptual replications",  "desc": "5=varied methods same construct · 3=some related work · 1=novel only"},
        {"key": "metasupport", "label": "Meta-analytic support",    "desc": "5=reviews/meta-analyses agree · 3=some synthesis support · 1=no synthesis"},
    ]},
    {"key": "equityFit",       "label": "Equity & Context Fit",  "indicators": [
        {"key": "population",  "label": "Population focus",         "desc": "5=directly targets LMIC/low-income · 3=partially relevant · 1=not applicable"},
        {"key": "adaptation",  "label": "Contextual fit",           "desc": "5=culturally adapted + validated · 3=adaptable · 1=no adaptation pathway"},
        {"key": "equityOutcomes","label":"Equity outcomes",         "desc": "5=disparities measured + addressed · 3=mentioned · 1=not considered"},
    ]},
    {"key": "actionability",   "label": "Actionability",         "indicators": [
        {"key": "implementation","label":"Implementation evidence", "desc": "5=real-world tested at scale · 3=pilot tested · 1=theoretical only"},
        {"key": "costEffective","label": "Cost-effectiveness",      "desc": "5=economic data reported · 3=cost mentioned · 1=absent"},
        {"key": "scalability", "label": "Scale-up feasibility",     "desc": "5=clear pathway + barriers mapped · 3=some evidence · 1=no pathway"},
    ]},
]

EV_TIERS = [
    {"min": 23, "max": 30, "label": "Tier 1 — Strong Evidence"},
    {"min": 16, "max": 22, "label": "Tier 2 — Moderate Evidence"},
    {"min":  9, "max": 15, "label": "Tier 3 — Emerging Evidence"},
    {"min":  1, "max":  8, "label": "Tier 4 — Foundational"},
]

def _build_scorecard_prompt(paper: dict) -> str:
    ind_lines   = "\n".join(
        f'  "{d["key"]}.{i["key"]}": {{"score": 0, "rationale": "one sentence"}}'
        for d in EV_DIMS for i in d["indicators"]
    )
    guide_lines = "\n".join(
        f'- {d["key"]}.{i["key"]}: {i["desc"]}'
        for d in EV_DIMS for i in d["indicators"]
    )
    return f"""You are an evidence analyst for the Gates Foundation. Score this paper on 18 indicators. Return ONLY valid JSON.

Paper: "{paper['title']}"
Abstract: {paper['abstract'][:2000]}

Return this exact JSON (scores are integers 1–5):
{{
{ind_lines},
  "verdict": "Two-sentence investment readiness verdict for a program officer.",
  "investmentSignal": "Strong|Moderate|Cautious|Insufficient"
}}

Scoring guide:
{guide_lines}

Output only the JSON object."""

def score_paper(paper: dict) -> dict:
    """
    Score a single paper on all 18 evidence indicators.
    Returns a flat dict ready for Delta write.
    """
    prompt = _build_scorecard_prompt(paper)
    msg = claude.messages.create(
        model="claude-opus-4-5",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    m = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        raise ValueError("Claude returned no JSON")
    flat = json.loads(m.group())

    # Build structured result
    scores_by_dim = {}
    for dim in EV_DIMS:
        dim_scores = []
        for ind in dim["indicators"]:
            key   = f'{dim["key"]}.{ind["key"]}'
            entry = flat.get(key, {"score": 3, "rationale": "Not determined."})
            dim_scores.append({
                "indicator": ind["label"],
                "key":       key,
                "score":     int(entry.get("score", 3)),
                "rationale": entry.get("rationale", ""),
            })
        dim_avg = sum(d["score"] for d in dim_scores) / len(dim_scores)
        scores_by_dim[dim["key"]] = {
            "label":      dim["label"],
            "avg":        round(dim_avg, 2),
            "indicators": dim_scores,
        }

    total = sum(v["avg"] for v in scores_by_dim.values())
    tier  = next((t for t in EV_TIERS if t["min"] <= total <= t["max"]), EV_TIERS[-1])

    return {
        "arxiv_id":        paper["id"],
        "title":           paper["title"],
        "authors":         json.dumps(paper.get("authors", [])),
        "published":       paper.get("published", ""),
        "total_score":     round(total, 2),
        "tier":            tier["label"],
        "investment_signal": flat.get("investmentSignal", ""),
        "verdict":         flat.get("verdict", ""),
        "scores_json":     json.dumps(scores_by_dim),
        "scored_at":       datetime.utcnow().isoformat(),
    }

def score_papers_batch(papers: list[dict], delay: float = 1.0) -> list[dict]:
    """Score a list of papers; prints progress."""
    results = []
    for i, p in enumerate(papers, 1):
        print(f"  [{i}/{len(papers)}] Scoring: {p['title'][:60]}…")
        try:
            results.append(score_paper(p))
        except Exception as e:
            print(f"    ⚠ Failed: {e}")
        time.sleep(delay)
    return results

# ── Demo: score one paper ─────────────────────────────────────────────────────
score_result = score_paper(paper)
print(f"✓ Scorecard: {score_result['total_score']}/30 · {score_result['tier']}")
print(f"  Signal   : {score_result['investment_signal']}")
print(f"  Verdict  : {score_result['verdict']}")

# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION D — FP&A Research Agent (EDUAgent Decision-Readiness Framework)
# Mirrors: fpa-standalone.html → runFPA() + synthesis prompt
# ══════════════════════════════════════════════════════════════════════════════

FPA_IOs = [
    {"label": "Foundational Literacy & Numeracy",   "desc": "Reading, writing, numeracy for all children by end of primary"},
    {"label": "Secondary Completion",               "desc": "Equitable completion of quality secondary education"},
    {"label": "Postsecondary Access & Completion",  "desc": "Credential attainment for low-income students"},
    {"label": "Workforce Readiness",                "desc": "Skills for 21st-century labour markets"},
    {"label": "Systems Capacity",                   "desc": "Institutions that can sustain and scale quality learning"},
]

FPA_LADDER = [
    "Theoretical — logic model only",
    "Expert consensus — no primary data",
    "Correlational — observational data",
    "Quasi-experimental — natural experiment or matching",
    "Experimental — RCT (efficacy)",
    "Experimental — RCT (effectiveness at scale)",
]

def _fpa_synthesis_prompt(question: str, papers: list[dict]) -> str:
    paper_ctx = "\n\n".join(
        f"[{i+1}] {p['title']} ({(p.get('published') or '')[:4] or 'n.d.'}) — {p.get('source','?')} · {p.get('url','')}\n"
        f"    {', '.join((p.get('authors') or [])[:3])}"
        f"{'  et al.' if len(p.get('authors') or []) > 3 else ''}"
        f"{f'  · {p[\"citationCount\"]} citations' if (p.get('citationCount') or 0) > 0 else ''}\n"
        f"    {(p.get('abstract') or '')[:380]}"
        for i, p in enumerate(papers[:25])
    )
    io_ref     = "\n".join(f"- {io['label']}: {io['desc']}" for io in FPA_IOs)
    ladder_ref = " · ".join(f"Rung {i+1} — {r}" for i, r in enumerate(FPA_LADDER))

    return f"""You are a senior education research analyst at the Gates Foundation using the EDUAgent Decision-Readiness framework.

RESEARCH QUESTION: "{question}"

AMBITION 2045 GOALS:
{io_ref}

EVIDENCE LADDER: {ladder_ref}

RETRIEVED PAPERS — 2015 onwards ({len(papers)} papers):
{paper_ctx or "No papers retrieved — synthesise from your knowledge of education research including WWC, Campbell Collaboration, MDRC, J-PAL, and meta-analytic literature."}

CITATION RULES:
- Every factual claim must cite a source.
- Retrieved papers: cite as [N] inline.
- External sources: cite as (Author Year).
- Do NOT make uncited assertions.

OUTPUT FORMAT — use EXACTLY these headers in this order:

## DECISION_READINESS
Output a single JSON object on one line:
{{"evidence_strength":"Medium","confidence":"Moderate","key_risk":"one sentence","investment_readiness":"one sentence action recommendation"}}
evidence_strength options: Low | Medium | High | Very High
confidence options: Low | Moderate | Strong

## CONFIDENCE MAP
Three lines only:
STRONG: [comma-separated]
WEAK: [comma-separated]
UNKNOWN: [comma-separated]

## ROI_PARAMETERS
Output a SINGLE JSON object on ONE line. Feeds into Monte Carlo ROI model.
Pillar 1=K-12 (SD learning gain). Pillar 2=Postsecondary (pp pass-rate uplift).
Evidence tiers: 1=direct study exact intervention, 2=close proxy same mechanism, 3=broad category proxy, 4=structural/theoretical.
{{"pillar":"Pillar 1","effect_size":{{"low":0.08,"base":0.15,"high":0.25,"unit":"SD","evidence_type":"RCT","evidence_score":0.72,"source_count":5}},"evidence_tier":2,"similarity_score":0.71,"persistence_factor":{{"low":0.45,"base":0.55,"high":0.65,"source":"Bailey et al. 2017"}},"pipeline_steps":[{{"step":"SD gain → HS graduation","rate":0.10,"confidence":"Moderate"}}],"subgroup_modifiers":[{{"subgroup":"Black/Latino students","multiplier":1.1,"direction":"larger","source":"Theobald et al. 2020"}}],"proxy_studies":[{{"study":"Example RCT","effect_size":0.15,"evidence_type":"RCT","tier":1,"similarity_score":0.85,"rationale":"Direct intervention match"}}],"baseline_rates":{{"pass_rate":null,"retention":null,"note":"Population-specific baselines not surfaced"}},"credential_premium":{{"low":32000,"base":40000,"high":48000,"degree_type":"bachelor","source":"Chetty et al. 2014"}}}}

## Executive Summary
2–3 sentences with at least 2 inline citations.

## Ambition 2045 Goal & Outcomes
Which goals are implicated. Cite papers most directly mapping to each.

## Synthesised Findings
6–8 bullets. Each must cite at least one source. Include effect sizes, populations, study designs.

## Assumption Analysis
2–3 key assumptions. For each:
**Assumption: "[stated assumption]"**
- Supported by: [citations]
- Contested by: [citations or none]
- Missing evidence: [what is needed]

## Discovery Insights
**Cluster:** [dominant pattern]
**Gap:** [most significant evidence gap]
**Signal:** [emerging or underexplored trend]

## Evidence Gaps
4 bullets: missing evidence, underserved populations, methodological limits.

## Programme Officer Recommendations
3 concrete, actionable recommendations grounded in the evidence."""


def _extract_section(text: str, header: str) -> str:
    """Extract text between ## HEADER and next ## or end of string."""
    pattern = rf"##\s+{re.escape(header)}\s*\n([\s\S]*?)(?=\n##\s|\Z)"
    m = re.search(pattern, text)
    return m.group(1).strip() if m else ""

def _extract_json_block(text: str) -> dict | None:
    """Find and parse the first JSON object in a string."""
    m = re.search(r"\{[\s\S]*?\}", text)
    if not m:
        return None
    try:
        return json.loads(m.group())
    except Exception:
        # try cleaning markdown fences
        cleaned = re.sub(r"```[a-z]*", "", m.group()).strip()
        try:
            return json.loads(cleaned)
        except Exception:
            return None

def run_fpa_agent(question: str,
                  angles: list[dict] | None = None,
                  max_per_angle: int = 12) -> dict:
    """
    Full FP&A Research Agent pipeline:
      1. Multi-angle paper search
      2. EDUAgent synthesis (Claude)
      3. Parse decision-readiness, ROI parameters, all sections
      4. Return structured result
    """
    print(f"\n🔍 FP&A Agent: {question[:80]}…")

    # Step 1 — Search
    papers = multi_angle_search(question, angles=angles, max_per_angle=max_per_angle)

    # Step 2 — Synthesis
    print("  🤖 Synthesising with EDUAgent…")
    prompt = _fpa_synthesis_prompt(question, papers)
    msg = claude.messages.create(
        model="claude-opus-4-5",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    synthesis = msg.content[0].text.strip()

    # Step 3 — Parse sections
    dr_raw  = _extract_section(synthesis, "DECISION_READINESS")
    roi_raw = _extract_section(synthesis, "ROI_PARAMETERS")
    cm_raw  = _extract_section(synthesis, "CONFIDENCE MAP")

    dr_data  = _extract_json_block(dr_raw)  or {}
    roi_data = _extract_json_block(roi_raw) or {}

    confidence_map = {}
    for line in cm_raw.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            confidence_map[k.strip().lower()] = [x.strip() for x in v.split(",")]

    return {
        "question":          question,
        "papers_retrieved":  len(papers),
        "papers":            papers,
        "synthesis_raw":     synthesis,
        "decision_readiness": dr_data,
        "confidence_map":    confidence_map,
        "roi_parameters":    roi_data,
        "executive_summary": _extract_section(synthesis, "Executive Summary"),
        "synthesised_findings": _extract_section(synthesis, "Synthesised Findings"),
        "evidence_gaps":     _extract_section(synthesis, "Evidence Gaps"),
        "recommendations":   _extract_section(synthesis, "Programme Officer Recommendations"),
        "discovery_insights":_extract_section(synthesis, "Discovery Insights"),
        "run_at":            datetime.utcnow().isoformat(),
    }

# ── Demo ──────────────────────────────────────────────────────────────────────
fpa_result = run_fpa_agent(
    "What is the evidence for AI tutoring systems improving math outcomes for K-12 students?"
)
dr = fpa_result["decision_readiness"]
print(f"\n✓ Evidence strength : {dr.get('evidence_strength')}")
print(f"  Confidence        : {dr.get('confidence')}")
print(f"  Key risk          : {dr.get('key_risk')}")
print(f"\n  Executive Summary :\n{textwrap.fill(fpa_result['executive_summary'], 80)}")

# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION E — Knowledge Graph Builder
# Mirrors: Evidence Scout → Knowledge Graph (concept nodes + edges)
# ══════════════════════════════════════════════════════════════════════════════

def _build_graph_prompt(paper: dict) -> str:
    return f"""You are a research knowledge-graph builder. Analyse this paper and return ONLY valid JSON.

Paper: "{paper['title']}"
Abstract: {paper['abstract'][:1500]}

Return a JSON object with:
{{
  "core_concept": "the central concept of this paper in 2–4 words",
  "nodes": [
    {{"id": "unique_id", "label": "concept label", "type": "method|finding|population|outcome|theory"}},
    ...  (6–10 nodes total, including the core concept)
  ],
  "edges": [
    {{"from": "id1", "to": "id2", "relation": "short relation label"}},
    ...  (6–12 edges)
  ]
}}

Output only the JSON."""

def build_knowledge_graph(paper: dict) -> dict:
    """Extract a concept knowledge graph from a paper via Claude."""
    prompt = _build_graph_prompt(paper)
    msg = claude.messages.create(
        model="claude-opus-4-5",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    m   = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        raise ValueError("No JSON in graph response")
    data = json.loads(m.group())
    data["arxiv_id"]   = paper["id"]
    data["paper_title"]= paper["title"]
    data["built_at"]   = datetime.utcnow().isoformat()
    return data

def build_merged_graph(papers: list[dict]) -> nx.DiGraph:
    """Build a merged NetworkX DiGraph from multiple papers."""
    G = nx.DiGraph()
    for p in papers:
        try:
            gd = build_knowledge_graph(p)
            for node in gd.get("nodes", []):
                G.add_node(node["id"], label=node.get("label",""),
                           node_type=node.get("type",""),
                           paper_id=p["id"])
            for edge in gd.get("edges", []):
                G.add_edge(edge["from"], edge["to"],
                           relation=edge.get("relation",""),
                           paper_id=p["id"])
            time.sleep(0.5)
        except Exception as e:
            print(f"  ⚠ Graph failed for {p['id']}: {e}")
    print(f"✓ Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G

# ── Demo: graph one paper ─────────────────────────────────────────────────────
graph_data = build_knowledge_graph(paper)
print(f"✓ Graph extracted: {len(graph_data['nodes'])} nodes, {len(graph_data['edges'])} edges")
print(f"  Core concept: {graph_data.get('core_concept')}")

# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION F — Delta Table Schemas & Writers
# ══════════════════════════════════════════════════════════════════════════════

# ── F1. Papers table ──────────────────────────────────────────────────────────
PAPERS_SCHEMA = T.StructType([
    T.StructField("id",            T.StringType()),
    T.StructField("title",         T.StringType()),
    T.StructField("abstract",      T.StringType()),
    T.StructField("authors",       T.StringType()),   # JSON array
    T.StructField("published",     T.StringType()),
    T.StructField("source",        T.StringType()),
    T.StructField("url",           T.StringType()),
    T.StructField("citationCount", T.LongType()),
    T.StructField("fetched_at",    T.StringType()),
])

def write_papers(papers: list[dict], mode: str = "append"):
    rows = [{
        "id":            p.get("id",""),
        "title":         p.get("title",""),
        "abstract":      p.get("abstract",""),
        "authors":       json.dumps(p.get("authors",[])),
        "published":     str(p.get("published",""))[:10],
        "source":        p.get("source",""),
        "url":           p.get("url",""),
        "citationCount": int(p.get("citationCount") or 0),
        "fetched_at":    p.get("fetched_at", datetime.utcnow().isoformat()),
    } for p in papers]
    df = spark.createDataFrame(rows, schema=PAPERS_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_PAPERS)
    print(f"✓ Wrote {len(rows)} rows → {TABLE_PAPERS}")

# ── F2. Evidence Scorecard table ──────────────────────────────────────────────
SCORECARD_SCHEMA = T.StructType([
    T.StructField("arxiv_id",          T.StringType()),
    T.StructField("title",             T.StringType()),
    T.StructField("authors",           T.StringType()),
    T.StructField("published",         T.StringType()),
    T.StructField("total_score",       T.DoubleType()),
    T.StructField("tier",              T.StringType()),
    T.StructField("investment_signal", T.StringType()),
    T.StructField("verdict",           T.StringType()),
    T.StructField("scores_json",       T.StringType()),   # full dim/indicator JSON
    T.StructField("scored_at",         T.StringType()),
])

def write_scorecards(results: list[dict], mode: str = "append"):
    df = spark.createDataFrame(results, schema=SCORECARD_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_SCORECARD)
    print(f"✓ Wrote {len(results)} scorecards → {TABLE_SCORECARD}")

# ── F3. FP&A Briefs table ─────────────────────────────────────────────────────
FPA_SCHEMA = T.StructType([
    T.StructField("question",             T.StringType()),
    T.StructField("papers_retrieved",     T.IntegerType()),
    T.StructField("evidence_strength",    T.StringType()),
    T.StructField("confidence",           T.StringType()),
    T.StructField("key_risk",             T.StringType()),
    T.StructField("investment_readiness", T.StringType()),
    T.StructField("confidence_map_json",  T.StringType()),
    T.StructField("executive_summary",    T.StringType()),
    T.StructField("synthesised_findings", T.StringType()),
    T.StructField("evidence_gaps",        T.StringType()),
    T.StructField("recommendations",      T.StringType()),
    T.StructField("discovery_insights",   T.StringType()),
    T.StructField("synthesis_raw",        T.StringType()),
    T.StructField("run_at",              T.StringType()),
])

def write_fpa_brief(result: dict, mode: str = "append"):
    dr = result.get("decision_readiness", {})
    row = [{
        "question":             result["question"],
        "papers_retrieved":     result["papers_retrieved"],
        "evidence_strength":    dr.get("evidence_strength",""),
        "confidence":           dr.get("confidence",""),
        "key_risk":             dr.get("key_risk",""),
        "investment_readiness": dr.get("investment_readiness",""),
        "confidence_map_json":  json.dumps(result.get("confidence_map",{})),
        "executive_summary":    result["executive_summary"],
        "synthesised_findings": result["synthesised_findings"],
        "evidence_gaps":        result["evidence_gaps"],
        "recommendations":      result["recommendations"],
        "discovery_insights":   result["discovery_insights"],
        "synthesis_raw":        result["synthesis_raw"],
        "run_at":               result["run_at"],
    }]
    df = spark.createDataFrame(row, schema=FPA_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_FPA)
    print(f"✓ Wrote FP&A brief → {TABLE_FPA}")

# ── F4. ROI Parameters table ──────────────────────────────────────────────────
ROI_SCHEMA = T.StructType([
    T.StructField("question",          T.StringType()),
    T.StructField("pillar",            T.StringType()),
    T.StructField("evidence_tier",     T.IntegerType()),
    T.StructField("similarity_score",  T.DoubleType()),
    T.StructField("effect_low",        T.DoubleType()),
    T.StructField("effect_base",       T.DoubleType()),
    T.StructField("effect_high",       T.DoubleType()),
    T.StructField("effect_unit",       T.StringType()),
    T.StructField("evidence_type",     T.StringType()),
    T.StructField("evidence_score",    T.DoubleType()),
    T.StructField("source_count",      T.IntegerType()),
    T.StructField("pipeline_json",     T.StringType()),
    T.StructField("subgroups_json",    T.StringType()),
    T.StructField("proxy_studies_json",T.StringType()),
    T.StructField("roi_raw_json",      T.StringType()),
    T.StructField("run_at",            T.StringType()),
])

def write_roi_parameters(question: str, roi: dict, run_at: str, mode: str = "append"):
    es = roi.get("effect_size", {})
    row = [{
        "question":           question,
        "pillar":             roi.get("pillar",""),
        "evidence_tier":      int(roi.get("evidence_tier") or 0),
        "similarity_score":   float(roi.get("similarity_score") or 0),
        "effect_low":         float(es.get("low") or 0),
        "effect_base":        float(es.get("base") or 0),
        "effect_high":        float(es.get("high") or 0),
        "effect_unit":        es.get("unit",""),
        "evidence_type":      es.get("evidence_type",""),
        "evidence_score":     float(es.get("evidence_score") or 0),
        "source_count":       int(es.get("source_count") or 0),
        "pipeline_json":      json.dumps(roi.get("pipeline_steps",[])),
        "subgroups_json":     json.dumps(roi.get("subgroup_modifiers",[])),
        "proxy_studies_json": json.dumps(roi.get("proxy_studies",[])),
        "roi_raw_json":       json.dumps(roi),
        "run_at":             run_at,
    }]
    df = spark.createDataFrame(row, schema=ROI_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_ROI)
    print(f"✓ Wrote ROI parameters → {TABLE_ROI}")

# ── F5. Knowledge Graph edges table ───────────────────────────────────────────
GRAPH_SCHEMA = T.StructType([
    T.StructField("arxiv_id",    T.StringType()),
    T.StructField("paper_title", T.StringType()),
    T.StructField("from_id",     T.StringType()),
    T.StructField("from_label",  T.StringType()),
    T.StructField("from_type",   T.StringType()),
    T.StructField("to_id",       T.StringType()),
    T.StructField("to_label",    T.StringType()),
    T.StructField("to_type",     T.StringType()),
    T.StructField("relation",    T.StringType()),
    T.StructField("built_at",    T.StringType()),
])

def write_knowledge_graph(graph_data: dict, mode: str = "append"):
    node_map = {n["id"]: n for n in graph_data.get("nodes", [])}
    rows = []
    for e in graph_data.get("edges", []):
        src = node_map.get(e["from"], {})
        tgt = node_map.get(e["to"],   {})
        rows.append({
            "arxiv_id":    graph_data.get("arxiv_id",""),
            "paper_title": graph_data.get("paper_title",""),
            "from_id":     e["from"],
            "from_label":  src.get("label",""),
            "from_type":   src.get("type",""),
            "to_id":       e["to"],
            "to_label":    tgt.get("label",""),
            "to_type":     tgt.get("type",""),
            "relation":    e.get("relation",""),
            "built_at":    graph_data.get("built_at",""),
        })
    if rows:
        df = spark.createDataFrame(rows, schema=GRAPH_SCHEMA)
        df.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_GRAPH)
        print(f"✓ Wrote {len(rows)} graph edges → {TABLE_GRAPH}")

# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION G — Full Pipeline: Persist demo results to Delta
# ══════════════════════════════════════════════════════════════════════════════

# Papers
write_papers([paper], mode="overwrite")
write_papers(fpa_result["papers"])

# Evidence Scorecard
write_scorecards([score_result])

# FP&A brief + ROI
write_fpa_brief(fpa_result)
if fpa_result["roi_parameters"]:
    write_roi_parameters(fpa_result["question"], fpa_result["roi_parameters"], fpa_result["run_at"])

# Knowledge graph
write_knowledge_graph(graph_data)

print("\n✓ All data persisted to Delta tables.")

# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION H — Batch Processing (run at scale)
# ══════════════════════════════════════════════════════════════════════════════

BATCH_ARXIV_IDS = [
    "2304.03442",   # Generative Agents (simulacra)
    "2411.10109",   # Scaling up personas
    "2502.00640",   # LLM optimisation via simulation
    "2511.00222",   # Simulation + RL
    "2507.22049",   # Social simulation + psych validation
    "2310.06837",   # Simulating student responses
    # ── add more IDs below ───────────────────────────────────
]

BATCH_FPA_QUESTIONS = [
    "What is the evidence for AI tutoring improving math outcomes for K-12 students?",
    "What does the research say about LLM-powered simulation for education research?",
    # ── add more questions below ──────────────────────────────
]

def run_batch(arxiv_ids: list[str], fpa_questions: list[str]):
    print("=" * 60)
    print("BATCH RUN — Evidence Scout + FP&A Agent")
    print("=" * 60)

    # 1. Fetch all arXiv papers
    print(f"\n── Fetching {len(arxiv_ids)} arXiv papers…")
    papers = fetch_arxiv_batch(arxiv_ids)
    write_papers(papers)

    # 2. Score each paper
    print(f"\n── Scoring {len(papers)} papers (Evidence Scorecard)…")
    scorecards = score_papers_batch(papers, delay=1.5)
    write_scorecards(scorecards)

    # 3. Build knowledge graph for each paper
    print(f"\n── Building knowledge graphs…")
    for p in papers:
        try:
            gd = build_knowledge_graph(p)
            write_knowledge_graph(gd)
            time.sleep(0.5)
        except Exception as e:
            print(f"  ⚠ {p['id']}: {e}")

    # 4. Run FP&A Agent for each question
    for q in fpa_questions:
        print(f"\n── FP&A Agent: {q[:70]}…")
        try:
            result = run_fpa_agent(q)
            write_fpa_brief(result)
            if result["roi_parameters"]:
                write_roi_parameters(q, result["roi_parameters"], result["run_at"])
            write_papers(result["papers"])
        except Exception as e:
            print(f"  ⚠ FP&A failed: {e}")
        time.sleep(2)

    print("\n✓ Batch complete.")

# Uncomment to run the full batch:
# run_batch(BATCH_ARXIV_IDS, BATCH_FPA_QUESTIONS)

# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION I — Exploration Queries
# ══════════════════════════════════════════════════════════════════════════════

# Top-scoring papers
display(
    spark.sql(f"""
        SELECT arxiv_id, title, total_score, tier, investment_signal, verdict
        FROM   {TABLE_SCORECARD}
        ORDER  BY total_score DESC
    """)
)

# COMMAND ----------
# Investment signal breakdown
display(
    spark.sql(f"""
        SELECT investment_signal,
               COUNT(*)                              AS papers,
               ROUND(AVG(total_score), 2)            AS avg_score,
               ROUND(MIN(total_score), 2)            AS min_score,
               ROUND(MAX(total_score), 2)            AS max_score
        FROM   {TABLE_SCORECARD}
        GROUP  BY investment_signal
        ORDER  BY avg_score DESC
    """)
)

# COMMAND ----------
# FP&A briefs summary
display(
    spark.sql(f"""
        SELECT question, papers_retrieved, evidence_strength,
               confidence, key_risk, run_at
        FROM   {TABLE_FPA}
        ORDER  BY run_at DESC
    """)
)

# COMMAND ----------
# ROI parameters overview
display(
    spark.sql(f"""
        SELECT question, pillar, evidence_tier, similarity_score,
               effect_low, effect_base, effect_high, effect_unit,
               evidence_type, source_count
        FROM   {TABLE_ROI}
        ORDER  BY run_at DESC
    """)
)

# COMMAND ----------
# Knowledge graph — most connected concepts
display(
    spark.sql(f"""
        SELECT from_label AS concept, from_type AS type,
               COUNT(*) AS out_degree
        FROM   {TABLE_GRAPH}
        GROUP  BY from_label, from_type
        ORDER  BY out_degree DESC
        LIMIT  20
    """)
)

# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# END OF NOTEBOOK
# Tables written:
#   {CATALOG}.{SCHEMA}.papers              — fetched arXiv + Semantic Scholar papers
#   {CATALOG}.{SCHEMA}.evidence_scorecard  — 18-indicator scores per paper
#   {CATALOG}.{SCHEMA}.fpa_briefs          — full EDUAgent synthesis briefs
#   {CATALOG}.{SCHEMA}.roi_parameters      — structured ROI model inputs
#   {CATALOG}.{SCHEMA}.knowledge_graph_edges — concept graph edge list
# ══════════════════════════════════════════════════════════════════════════════
