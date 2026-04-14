# Databricks notebook source
# ══════════════════════════════════════════════════════════════════════════════
# Research & Discovery — Data & AI Enablement Hub
# COMPLETE DATABRICKS NOTEBOOK — All Six Tools
#
# Tools covered:
#   1. Evidence Scout          → evidence.papers, evidence.scorecard
#   2. EDUAgent / ROI Agent    → roi.briefs, roi.parameters
#   3. Market Insights (AA)    → market.aa_model_benchmarks, market.market_signals
#   4. AI Literacy Navigator   → navigator.resources, navigator.bibliography
#   5. Knowledge Graph         → graph.nodes, graph.edges
#   6. Synthesis Layer         → synthesis.briefs, synthesis.provenance
#
# Prerequisites
#   Cluster : DBR 14.3 LTS ML (Python 3.10+, Spark 3.5)
#   Secrets : databricks secrets put --scope research-hub --key anthropic-api-key
#             databricks secrets put --scope research-hub --key semanticscholar-api-key   (optional)
#             databricks secrets put --scope research-hub --key artificial-analysis-api-key
#
# Unity Catalog layout
#   research_hub.evidence.*    — Literature / Evidence Scout
#   research_hub.roi.*         — ROI Research Agent (EDUAgent synthesis)
#   research_hub.market.*      — Market Insights (Artificial Analysis)
#   research_hub.navigator.*   — AI Literacy & Equity Navigator
#   research_hub.graph.*       — Knowledge Graph
#   research_hub.synthesis.*   — Cross-tool Synthesis Layer + SQL views
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# ── 0. Install dependencies ──────────────────────────────────────────────────
%pip install anthropic httpx tenacity networkx --quiet

# COMMAND ----------
# ── 1. Imports & configuration ───────────────────────────────────────────────
import json, re, time, math, textwrap, uuid, xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import httpx
import anthropic
from tenacity import retry, stop_after_attempt, wait_exponential
from pyspark.sql import functions as F, types as T
import networkx as nx

# ── Secrets ────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = dbutils.secrets.get("research-hub", "anthropic-api-key")
AA_API_KEY        = dbutils.secrets.get("research-hub", "artificial-analysis-api-key")
SS_API_KEY        = dbutils.secrets.get("research-hub", "semanticscholar-api-key") if True else None

# ── Unity Catalog setup ────────────────────────────────────────────────────────
CATALOG = "research_hub"
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ["evidence", "roi", "market", "navigator", "graph", "synthesis"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# ── Table references ───────────────────────────────────────────────────────────
TABLE_PAPERS         = f"{CATALOG}.evidence.papers"
TABLE_SCORECARD      = f"{CATALOG}.evidence.scorecard"
TABLE_ROI_BRIEFS     = f"{CATALOG}.roi.briefs"
TABLE_ROI_PARAMS     = f"{CATALOG}.roi.parameters"
TABLE_AA_BENCHMARKS  = f"{CATALOG}.market.aa_model_benchmarks"
TABLE_MARKET_SIGNALS = f"{CATALOG}.market.market_signals"
TABLE_NAV_RESOURCES  = f"{CATALOG}.navigator.resources"
TABLE_NAV_BIBLIO     = f"{CATALOG}.navigator.bibliography"
TABLE_GRAPH_NODES    = f"{CATALOG}.graph.nodes"
TABLE_GRAPH_EDGES    = f"{CATALOG}.graph.edges"
TABLE_SYN_BRIEFS     = f"{CATALOG}.synthesis.briefs"
TABLE_SYN_PROVENANCE = f"{CATALOG}.synthesis.provenance"

claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _uid() -> str:
    return str(uuid.uuid4())

print("✓ Config ready — 6 schemas, 12 tables registered")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION A — arXiv & Semantic Scholar Paper Fetching
# Mirrors: Evidence Scout → fetchArxiv(), api-server/src/routes/arxiv.ts
# ══════════════════════════════════════════════════════════════════════════════

ARXIV_NS = "http://www.w3.org/2005/Atom"

def _parse_arxiv_id(raw: str) -> str:
    """Normalise arXiv IDs or full URLs to bare ID like 2304.03442."""
    s = raw.strip()
    for prefix in ["https://arxiv.org/abs/", "https://arxiv.org/pdf/",
                   "https://arxiv.org/html/", "http://arxiv.org/abs/"]:
        if s.startswith(prefix):
            s = s[len(prefix):]
    s = s.rstrip("/")
    if s.endswith(".pdf"):
        s = s[:-4]
    return re.sub(r"v\d+$", "", s).strip()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
def fetch_arxiv_paper(arxiv_id_or_url: str) -> dict:
    arxiv_id = _parse_arxiv_id(arxiv_id_or_url)
    url  = f"https://export.arxiv.org/api/query?id_list={arxiv_id}&max_results=1"
    resp = httpx.get(url, timeout=20)
    resp.raise_for_status()
    root  = ET.fromstring(resp.text)
    entry = root.find(f"{{{ARXIV_NS}}}entry")
    if entry is None:
        raise ValueError(f"arXiv ID not found: {arxiv_id}")

    def txt(tag):
        el = entry.find(f"{{{ARXIV_NS}}}{tag}")
        return el.text.strip() if el is not None and el.text else ""

    authors = [a.find(f"{{{ARXIV_NS}}}name").text.strip()
               for a in entry.findall(f"{{{ARXIV_NS}}}author")
               if a.find(f"{{{ARXIV_NS}}}name") is not None]
    pdf_link = next(
        (l.attrib.get("href","") for l in entry.findall(f"{{{ARXIV_NS}}}link")
         if l.attrib.get("title") == "pdf"), "")
    categories = [c.attrib.get("term","")
                  for c in entry.findall("{http://arxiv.org/schemas/atom}category")]

    return {
        "paper_id":      arxiv_id,
        "source":        "arXiv",
        "doi":           "",
        "url":           f"https://arxiv.org/abs/{arxiv_id}",
        "pdf_url":       pdf_link,
        "title":         txt("title").replace("\n", " "),
        "abstract":      txt("summary").replace("\n", " "),
        "authors":       authors,
        "published_date":txt("published")[:10],
        "journal":       "",
        "categories":    categories,
        "citation_count":0,
        "query_used":    "",
        "tool_source":   "evidence_scout",
        "fetched_at":    _now(),
    }

def fetch_arxiv_batch(ids: list[str]) -> list[dict]:
    results = []
    for raw_id in ids:
        try:
            results.append(fetch_arxiv_paper(raw_id))
            time.sleep(0.4)
        except Exception as e:
            print(f"  ⚠ Skipped {raw_id}: {e}")
    return results

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
        doi = (p.get("externalIds") or {}).get("DOI","")
        papers.append({
            "paper_id":      p.get("paperId",""),
            "source":        "Semantic Scholar",
            "doi":           doi,
            "url":           p.get("url",""),
            "pdf_url":       "",
            "title":         p.get("title",""),
            "abstract":      (p.get("abstract") or "")[:1000],
            "authors":       [a["name"] for a in (p.get("authors") or [])],
            "published_date":str(p.get("year",""))[:4],
            "journal":       "",
            "categories":    [],
            "citation_count":p.get("citationCount",0),
            "query_used":    query,
            "tool_source":   "evidence_scout",
            "fetched_at":    _now(),
        })
    return papers

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
def search_arxiv(query: str, max_results: int = 12) -> list[dict]:
    params = {"search_query": f"all:{query}", "start": 0,
              "max_results": max_results, "sortBy": "relevance"}
    resp  = httpx.get("https://export.arxiv.org/api/query", params=params, timeout=20)
    resp.raise_for_status()
    root  = ET.fromstring(resp.text)
    papers = []
    for entry in root.findall(f"{{{ARXIV_NS}}}entry"):
        def _t(tag):
            el = entry.find(f"{{{ARXIV_NS}}}{tag}")
            return el.text.strip() if el is not None and el.text else ""
        raw_id = _t("id").split("/abs/")[-1]
        pid    = _parse_arxiv_id(raw_id)
        papers.append({
            "paper_id":      pid,
            "source":        "arXiv",
            "doi":           "",
            "url":           f"https://arxiv.org/abs/{pid}",
            "pdf_url":       "",
            "title":         _t("title").replace("\n"," "),
            "abstract":      _t("summary").replace("\n"," ")[:1000],
            "authors":       [a.find(f"{{{ARXIV_NS}}}name").text.strip()
                              for a in entry.findall(f"{{{ARXIV_NS}}}author")
                              if a.find(f"{{{ARXIV_NS}}}name") is not None],
            "published_date":_t("published")[:10],
            "journal":       "",
            "categories":    [],
            "citation_count":0,
            "query_used":    query,
            "tool_source":   "evidence_scout",
            "fetched_at":    _now(),
        })
    return papers

def search_papers(query: str, max_results: int = 12) -> list[dict]:
    """Search Semantic Scholar first; fall back to arXiv."""
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
    """Parallel multi-angle paper search — mirrors FP&A Agent search strategy."""
    if not angles:
        kw = " ".join(w for w in re.sub(r"[^a-z0-9\s]","",
                      question.lower()).split() if len(w) > 3)[:60]
        angles = [
            {"angle": "Intervention evidence",  "query": f"{kw} AI education intervention outcomes"},
            {"angle": "Learning outcomes",       "query": f"{kw} student learning achievement AI"},
            {"angle": "Systematic review",       "query": f"{kw} meta-analysis systematic review education"},
            {"angle": "Equity & access",         "query": f"{kw} equity access underserved students AI"},
        ]
    all_papers, seen = [], set()
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(search_papers, a["query"], max_per_angle): a for a in angles}
        for fut in as_completed(futs):
            for p in fut.result():
                yr = int((p.get("published_date") or "0")[:4] or 0)
                if p["paper_id"] not in seen and yr >= 2015:
                    seen.add(p["paper_id"])
                    all_papers.append(p)
    all_papers.sort(key=lambda p: -(p.get("citation_count") or 0))
    print(f"✓ Multi-angle search: {len(all_papers)} unique papers from {len(angles)} angles")
    return all_papers

print("✓ Section A — arXiv + Semantic Scholar fetchers defined")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION B — Evidence Scorecard (18 indicators × 6 dimensions)
# Mirrors: Evidence Scout → Evidence Score button + runEvidenceStrength()
# ══════════════════════════════════════════════════════════════════════════════

EV_DIMS = [
    {"key": "studyRigor",      "label": "Study Rigor",          "indicators": [
        {"key": "designType",   "label": "Study design",            "desc": "5=RCT/meta-analysis · 3=observational · 1=opinion/theoretical"},
        {"key": "controls",     "label": "Methodological controls", "desc": "5=blinding+randomization+confounders · 3=some controls · 1=none"},
        {"key": "stats",        "label": "Statistical rigor",       "desc": "5=appropriate methods+power+corrections · 3=basic stats · 1=absent"},
    ]},
    {"key": "sampleScale",     "label": "Sample & Scale",       "indicators": [
        {"key": "size",         "label": "Sample size",             "desc": "5=large adequate N · 3=moderate · 1=tiny/unclear"},
        {"key": "represent",    "label": "Representativeness",      "desc": "5=diverse multi-demographic · 3=single group · 1=non-representative"},
        {"key": "sites",        "label": "Setting diversity",       "desc": "5=multi-site multi-country · 3=single site · 1=lab only"},
    ]},
    {"key": "effectPrecision", "label": "Effect Precision",     "indicators": [
        {"key": "magnitude",    "label": "Effect size",             "desc": "5=large meaningful ES reported · 3=direction only · 1=absent"},
        {"key": "ci",           "label": "Confidence intervals",    "desc": "5=CI reported + meaningful · 3=p-values only · 1=not reported"},
        {"key": "outcomes",     "label": "Outcome validity",        "desc": "5=validated pre-registered instruments · 3=standard measures · 1=ad-hoc"},
    ]},
    {"key": "replication",     "label": "Replication Support",  "indicators": [
        {"key": "direct",       "label": "Direct replications",     "desc": "5=multiple independent replications · 3=one replication · 1=none"},
        {"key": "conceptual",   "label": "Conceptual replications", "desc": "5=varied methods same construct · 3=some related work · 1=novel only"},
        {"key": "metasupport",  "label": "Meta-analytic support",   "desc": "5=reviews/meta-analyses agree · 3=some synthesis support · 1=no synthesis"},
    ]},
    {"key": "equityFit",       "label": "Equity & Context Fit", "indicators": [
        {"key": "population",   "label": "Population focus",        "desc": "5=directly targets LMIC/low-income · 3=partially relevant · 1=not applicable"},
        {"key": "adaptation",   "label": "Contextual fit",          "desc": "5=culturally adapted + validated · 3=adaptable · 1=no adaptation pathway"},
        {"key": "equityOutcomes","label": "Equity outcomes",        "desc": "5=disparities measured + addressed · 3=mentioned · 1=not considered"},
    ]},
    {"key": "actionability",   "label": "Actionability",        "indicators": [
        {"key": "implementation","label": "Implementation evidence","desc": "5=real-world tested at scale · 3=pilot tested · 1=theoretical only"},
        {"key": "costEffective", "label": "Cost-effectiveness",     "desc": "5=economic data reported · 3=cost mentioned · 1=absent"},
        {"key": "scalability",   "label": "Scale-up feasibility",   "desc": "5=clear pathway + barriers mapped · 3=some evidence · 1=no pathway"},
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
        for d in EV_DIMS for i in d["indicators"])
    guide_lines = "\n".join(
        f'- {d["key"]}.{i["key"]}: {i["desc"]}'
        for d in EV_DIMS for i in d["indicators"])
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
    """Score a single paper on all 18 evidence indicators using Claude."""
    prompt = _build_scorecard_prompt(paper)
    msg = claude.messages.create(
        model="claude-opus-4-5", max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    m   = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        raise ValueError("Claude returned no JSON")
    flat = json.loads(m.group())

    scores_by_dim = {}
    for dim in EV_DIMS:
        dim_scores = []
        for ind in dim["indicators"]:
            key   = f'{dim["key"]}.{ind["key"]}'
            entry = flat.get(key, {"score": 3, "rationale": "Not determined."})
            dim_scores.append({
                "indicator": ind["label"], "key": key,
                "score":     int(entry.get("score", 3)),
                "rationale": entry.get("rationale",""),
            })
        dim_avg = sum(d["score"] for d in dim_scores) / len(dim_scores)
        scores_by_dim[dim["key"]] = {
            "label": dim["label"], "avg": round(dim_avg, 2), "indicators": dim_scores}

    total = sum(v["avg"] for v in scores_by_dim.values())
    tier  = next((t for t in EV_TIERS if t["min"] <= total <= t["max"]), EV_TIERS[-1])

    return {
        "paper_id":         paper["paper_id"],
        "title":            paper["title"],
        "evidence_rung":    int(total // 6),
        "evidence_type":    flat.get("investmentSignal",""),
        "study_design":     scores_by_dim.get("studyRigor",{}).get("indicators",[{}])[0].get("rationale",""),
        "outcome_domain":   "",
        "population":       scores_by_dim.get("equityFit",{}).get("indicators",[{},{}])[0].get("rationale",""),
        "sample_size":      0,
        "effect_direction": "",
        "effect_size_raw":  "",
        "score_relevance":  round(scores_by_dim.get("actionability",{}).get("avg",0)/5, 2),
        "score_rigor":      round(scores_by_dim.get("studyRigor",{}).get("avg",0)/5, 2),
        "score_recency":    round(scores_by_dim.get("replication",{}).get("avg",0)/5, 2),
        "score_equity":     round(scores_by_dim.get("equityFit",{}).get("avg",0)/5, 2),
        "total_score":      round(total, 2),
        "tier":             tier["label"],
        "investment_signal":flat.get("investmentSignal",""),
        "verdict":          flat.get("verdict",""),
        "scores_json":      json.dumps(scores_by_dim),
        "scored_at":        _now(),
    }

def score_papers_batch(papers: list[dict], delay: float = 1.0) -> list[dict]:
    results = []
    for i, p in enumerate(papers, 1):
        print(f"  [{i}/{len(papers)}] Scoring: {p['title'][:60]}…")
        try:
            results.append(score_paper(p))
        except Exception as e:
            print(f"    ⚠ Failed: {e}")
        time.sleep(delay)
    return results

print("✓ Section B — Evidence Scorecard (18 indicators) defined")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION C — EDUAgent / ROI Research Agent
# Mirrors: fpa-standalone.html → runFPA() + EDUAgent synthesis prompt
# ══════════════════════════════════════════════════════════════════════════════

FPA_IOs = [
    {"label": "Foundational Literacy & Numeracy",  "desc": "Reading, writing, numeracy for all children by end of primary"},
    {"label": "Secondary Completion",              "desc": "Equitable completion of quality secondary education"},
    {"label": "Postsecondary Access & Completion", "desc": "Credential attainment for low-income students"},
    {"label": "Workforce Readiness",               "desc": "Skills for 21st-century labour markets"},
    {"label": "Systems Capacity",                  "desc": "Institutions that can sustain and scale quality learning"},
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
        f"[{i+1}] {p['title']} ({(p.get('published_date') or '')[:4] or 'n.d.'}) — {p.get('source','?')} · {p.get('url','')}\n"
        f"    {', '.join((p.get('authors') or [])[:3])}"
        f"{'  et al.' if len(p.get('authors') or []) > 3 else ''}"
        f"{f'  · {p[\"citation_count\"]} citations' if (p.get('citation_count') or 0) > 0 else ''}\n"
        f"    {(p.get('abstract') or '')[:380]}"
        for i, p in enumerate(papers[:25]))
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
    m = re.search(rf"##\s+{re.escape(header)}\s*\n([\s\S]*?)(?=\n##\s|\Z)", text)
    return m.group(1).strip() if m else ""

def _extract_json_block(text: str) -> dict | None:
    m = re.search(r"\{[\s\S]*?\}", text)
    if not m:
        return None
    try:
        return json.loads(m.group())
    except Exception:
        try:
            return json.loads(re.sub(r"```[a-z]*", "", m.group()).strip())
        except Exception:
            return None

def run_roi_agent(question: str, angles: list[dict] | None = None,
                  max_per_angle: int = 12) -> dict:
    """Full ROI Research Agent pipeline: search → EDUAgent synthesis → parse."""
    print(f"\n🔍 ROI Agent: {question[:80]}…")
    papers  = multi_angle_search(question, angles=angles, max_per_angle=max_per_angle)
    print("  🤖 Synthesising with EDUAgent…")
    prompt  = _fpa_synthesis_prompt(question, papers)
    msg     = claude.messages.create(
        model="claude-opus-4-5", max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    synthesis = msg.content[0].text.strip()
    dr_raw    = _extract_section(synthesis, "DECISION_READINESS")
    roi_raw   = _extract_section(synthesis, "ROI_PARAMETERS")
    cm_raw    = _extract_section(synthesis, "CONFIDENCE MAP")
    dr_data   = _extract_json_block(dr_raw)  or {}
    roi_data  = _extract_json_block(roi_raw) or {}
    confidence_map = {}
    for line in cm_raw.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            confidence_map[k.strip().lower()] = [x.strip() for x in v.split(",")]
    return {
        "brief_id":              _uid(),
        "question":              question,
        "decision_context":      "",
        "investment_amount_usd": 0.0,
        "program_stage":         "explore",
        "papers_retrieved":      len(papers),
        "papers":                papers,
        "evidence_strength":     dr_data.get("evidence_strength",""),
        "confidence":            dr_data.get("confidence",""),
        "key_risk":              dr_data.get("key_risk",""),
        "investment_readiness":  dr_data.get("investment_readiness",""),
        "executive_summary":     _extract_section(synthesis, "Executive Summary"),
        "synthesised_findings":  _extract_section(synthesis, "Synthesised Findings"),
        "evidence_gaps":         _extract_section(synthesis, "Evidence Gaps"),
        "recommendations":       _extract_section(synthesis, "Programme Officer Recommendations"),
        "discovery_insights":    _extract_section(synthesis, "Discovery Insights"),
        "equity_considerations": "",
        "lmic_signals":          "",
        "confidence_map_json":   json.dumps(confidence_map),
        "roi_parameters":        roi_data,
        "synthesis_raw":         synthesis,
        "run_at":                _now(),
    }

print("✓ Section C — EDUAgent / ROI Research Agent defined")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION D — Knowledge Graph Builder
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
    {{"id": "unique_id", "label": "concept label", "type": "Concept|Outcome|Intervention|Population|Method"}},
    ...  (6–10 nodes total, including the core concept)
  ],
  "edges": [
    {{"from": "id1", "to": "id2", "relation": "short relation label", "weight": 0.8}},
    ...  (6–12 edges)
  ]
}}

Output only the JSON."""

def build_knowledge_graph(paper: dict) -> dict:
    """Extract concept knowledge graph from a paper via Claude."""
    prompt = _build_graph_prompt(paper)
    msg = claude.messages.create(
        model="claude-opus-4-5", max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    raw  = msg.content[0].text.strip()
    m    = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        raise ValueError("No JSON in graph response")
    data = json.loads(m.group())
    data["paper_id"]    = paper["paper_id"]
    data["paper_title"] = paper["title"]
    data["built_at"]    = _now()
    return data

def build_merged_graph(papers: list[dict]) -> nx.DiGraph:
    """Build a merged NetworkX DiGraph across multiple papers."""
    G = nx.DiGraph()
    for p in papers:
        try:
            gd = build_knowledge_graph(p)
            for node in gd.get("nodes", []):
                G.add_node(node["id"], label=node.get("label",""),
                           node_type=node.get("type",""), paper_id=p["paper_id"])
            for edge in gd.get("edges", []):
                G.add_edge(edge["from"], edge["to"],
                           relation=edge.get("relation",""), paper_id=p["paper_id"])
            time.sleep(0.5)
        except Exception as e:
            print(f"  ⚠ Graph failed for {p['paper_id']}: {e}")
    print(f"✓ Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G

print("✓ Section D — Knowledge Graph builder defined")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION E — Market Insights (Artificial Analysis live benchmarks)
# Mirrors: Market Insights Deepdive → api-server/src/routes/artificialAnalysis.ts
# ══════════════════════════════════════════════════════════════════════════════

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def fetch_aa_benchmarks() -> list[dict]:
    """Pull live model benchmark data from Artificial Analysis API."""
    resp = httpx.get(
        "https://artificialanalysis.ai/api/v0/models",
        headers={"Authorization": f"Bearer {AA_API_KEY}"},
        timeout=30,
    )
    resp.raise_for_status()
    models = []
    for m in resp.json().get("data", []):
        perf = m.get("performance", {})
        cost = m.get("pricing", {})
        models.append({
            "model_id":           m.get("id",""),
            "model_name":         m.get("name",""),
            "provider":           m.get("provider",""),
            "model_family":       m.get("family",""),
            "release_date":       m.get("release_date",""),
            "intelligence_index": float(perf.get("intelligence_index") or 0),
            "mmlu_score":         float(perf.get("mmlu") or 0),
            "math_score":         float(perf.get("math") or 0),
            "coding_score":       float(perf.get("coding") or 0),
            "reasoning_score":    float(perf.get("reasoning") or 0),
            "multilingual_score": float(perf.get("multilingual") or 0),
            "input_cost_per_1m":  float(cost.get("input_per_1m_tokens") or 0),
            "output_cost_per_1m": float(cost.get("output_per_1m_tokens") or 0),
            "context_window_k":   int(m.get("context_window_k") or 0),
            "latency_ms_p50":     int(perf.get("latency_ms_p50") or 0),
            "throughput_tok_s":   int(perf.get("throughput_tok_s") or 0),
            "is_open_weights":    bool(m.get("open_weights", False)),
            "lmic_accessible":    bool(m.get("lmic_accessible", False)),
            "data_residency":     m.get("data_residency",""),
            "source_url":         m.get("url",""),
            "fetched_at":         _now(),
        })
    return models

print("✓ Section E — Market Insights / Artificial Analysis fetcher defined")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION F — Delta Table Schemas (all 12 tables)
# ══════════════════════════════════════════════════════════════════════════════

# ── F1. evidence.papers ───────────────────────────────────────────────────────
PAPERS_SCHEMA = T.StructType([
    T.StructField("paper_id",       T.StringType(),  False),
    T.StructField("source",         T.StringType(),  True),
    T.StructField("doi",            T.StringType(),  True),
    T.StructField("url",            T.StringType(),  True),
    T.StructField("pdf_url",        T.StringType(),  True),
    T.StructField("title",          T.StringType(),  True),
    T.StructField("abstract",       T.StringType(),  True),
    T.StructField("authors",        T.ArrayType(T.StringType()), True),
    T.StructField("published_date", T.StringType(),  True),
    T.StructField("journal",        T.StringType(),  True),
    T.StructField("categories",     T.ArrayType(T.StringType()), True),
    T.StructField("citation_count", T.IntegerType(), True),
    T.StructField("query_used",     T.StringType(),  True),
    T.StructField("tool_source",    T.StringType(),  True),
    T.StructField("fetched_at",     T.StringType(),  True),
])

# ── F2. evidence.scorecard ────────────────────────────────────────────────────
SCORECARD_SCHEMA = T.StructType([
    T.StructField("paper_id",          T.StringType(),  False),
    T.StructField("title",             T.StringType(),  True),
    T.StructField("evidence_rung",     T.IntegerType(), True),
    T.StructField("evidence_type",     T.StringType(),  True),
    T.StructField("study_design",      T.StringType(),  True),
    T.StructField("outcome_domain",    T.StringType(),  True),
    T.StructField("population",        T.StringType(),  True),
    T.StructField("sample_size",       T.IntegerType(), True),
    T.StructField("effect_direction",  T.StringType(),  True),
    T.StructField("effect_size_raw",   T.StringType(),  True),
    T.StructField("score_relevance",   T.DoubleType(),  True),
    T.StructField("score_rigor",       T.DoubleType(),  True),
    T.StructField("score_recency",     T.DoubleType(),  True),
    T.StructField("score_equity",      T.DoubleType(),  True),
    T.StructField("total_score",       T.DoubleType(),  True),
    T.StructField("tier",              T.StringType(),  True),
    T.StructField("investment_signal", T.StringType(),  True),
    T.StructField("verdict",           T.StringType(),  True),
    T.StructField("scores_json",       T.StringType(),  True),
    T.StructField("scored_at",         T.StringType(),  True),
])

# ── F3. roi.briefs ────────────────────────────────────────────────────────────
ROI_BRIEFS_SCHEMA = T.StructType([
    T.StructField("brief_id",             T.StringType(),  False),
    T.StructField("question",             T.StringType(),  True),
    T.StructField("decision_context",     T.StringType(),  True),
    T.StructField("investment_amount_usd",T.DoubleType(),  True),
    T.StructField("program_stage",        T.StringType(),  True),
    T.StructField("papers_retrieved",     T.IntegerType(), True),
    T.StructField("evidence_strength",    T.StringType(),  True),
    T.StructField("confidence",           T.StringType(),  True),
    T.StructField("key_risk",             T.StringType(),  True),
    T.StructField("investment_readiness", T.StringType(),  True),
    T.StructField("executive_summary",    T.StringType(),  True),
    T.StructField("synthesised_findings", T.StringType(),  True),
    T.StructField("evidence_gaps",        T.StringType(),  True),
    T.StructField("recommendations",      T.StringType(),  True),
    T.StructField("discovery_insights",   T.StringType(),  True),
    T.StructField("equity_considerations",T.StringType(),  True),
    T.StructField("lmic_signals",         T.StringType(),  True),
    T.StructField("confidence_map_json",  T.StringType(),  True),
    T.StructField("run_at",              T.StringType(),  True),
])

# ── F4. roi.parameters ───────────────────────────────────────────────────────
ROI_PARAMS_SCHEMA = T.StructType([
    T.StructField("brief_id",           T.StringType(),  False),
    T.StructField("question",           T.StringType(),  True),
    T.StructField("pillar",             T.StringType(),  True),
    T.StructField("evidence_tier",      T.IntegerType(), True),
    T.StructField("similarity_score",   T.DoubleType(),  True),
    T.StructField("effect_low",         T.DoubleType(),  True),
    T.StructField("effect_base",        T.DoubleType(),  True),
    T.StructField("effect_high",        T.DoubleType(),  True),
    T.StructField("effect_unit",        T.StringType(),  True),
    T.StructField("evidence_type",      T.StringType(),  True),
    T.StructField("evidence_score",     T.DoubleType(),  True),
    T.StructField("source_count",       T.IntegerType(), True),
    T.StructField("pipeline_json",      T.StringType(),  True),
    T.StructField("subgroups_json",     T.StringType(),  True),
    T.StructField("proxy_studies_json", T.StringType(),  True),
    T.StructField("roi_raw_json",       T.StringType(),  True),
    T.StructField("run_at",             T.StringType(),  True),
])

# ── F5. market.aa_model_benchmarks ───────────────────────────────────────────
AA_BENCHMARKS_SCHEMA = T.StructType([
    T.StructField("model_id",           T.StringType(),  False),
    T.StructField("model_name",         T.StringType(),  True),
    T.StructField("provider",           T.StringType(),  True),
    T.StructField("model_family",       T.StringType(),  True),
    T.StructField("release_date",       T.StringType(),  True),
    T.StructField("intelligence_index", T.DoubleType(),  True),
    T.StructField("mmlu_score",         T.DoubleType(),  True),
    T.StructField("math_score",         T.DoubleType(),  True),
    T.StructField("coding_score",       T.DoubleType(),  True),
    T.StructField("reasoning_score",    T.DoubleType(),  True),
    T.StructField("multilingual_score", T.DoubleType(),  True),
    T.StructField("input_cost_per_1m",  T.DoubleType(),  True),
    T.StructField("output_cost_per_1m", T.DoubleType(),  True),
    T.StructField("context_window_k",   T.IntegerType(), True),
    T.StructField("latency_ms_p50",     T.IntegerType(), True),
    T.StructField("throughput_tok_s",   T.IntegerType(), True),
    T.StructField("is_open_weights",    T.BooleanType(), True),
    T.StructField("lmic_accessible",    T.BooleanType(), True),
    T.StructField("data_residency",     T.StringType(),  True),
    T.StructField("source_url",         T.StringType(),  True),
    T.StructField("fetched_at",         T.StringType(),  True),
])

# ── F6. market.market_signals ─────────────────────────────────────────────────
MARKET_SIGNALS_SCHEMA = T.StructType([
    T.StructField("signal_id",      T.StringType(),  False),
    T.StructField("signal_type",    T.StringType(),  True),
    T.StructField("title",          T.StringType(),  True),
    T.StructField("source_org",     T.StringType(),  True),
    T.StructField("source_url",     T.StringType(),  True),
    T.StructField("summary",        T.StringType(),  True),
    T.StructField("key_claim",      T.StringType(),  True),
    T.StructField("investment_usd", T.DoubleType(),  True),
    T.StructField("geography",      T.StringType(),  True),
    T.StructField("ai_sector",      T.StringType(),  True),
    T.StructField("relevance_score",T.DoubleType(),  True),
    T.StructField("lmic_flag",      T.BooleanType(), True),
    T.StructField("tags",           T.ArrayType(T.StringType()), True),
    T.StructField("signal_date",    T.StringType(),  True),
    T.StructField("ingested_at",    T.StringType(),  True),
])

# ── F7. navigator.resources ───────────────────────────────────────────────────
NAV_RESOURCES_SCHEMA = T.StructType([
    T.StructField("resource_id",   T.StringType(),  False),
    T.StructField("title",         T.StringType(),  True),
    T.StructField("source_org",    T.StringType(),  True),
    T.StructField("url",           T.StringType(),  True),
    T.StructField("category",      T.StringType(),  True),
    T.StructField("audience",      T.ArrayType(T.StringType()), True),
    T.StructField("level",         T.StringType(),  True),
    T.StructField("format",        T.StringType(),  True),
    T.StructField("topics",        T.ArrayType(T.StringType()), True),
    T.StructField("equity_themes", T.ArrayType(T.StringType()), True),
    T.StructField("lmic_relevant", T.BooleanType(), True),
    T.StructField("k12_relevant",  T.BooleanType(), True),
    T.StructField("summary",       T.StringType(),  True),
    T.StructField("why_useful",    T.StringType(),  True),
    T.StructField("watch_out",     T.StringType(),  True),
    T.StructField("freshness",     T.StringType(),  True),
    T.StructField("featured",      T.BooleanType(), True),
    T.StructField("notes",         T.StringType(),  True),
    T.StructField("added_by",      T.StringType(),  True),
    T.StructField("created_at",    T.StringType(),  True),
    T.StructField("updated_at",    T.StringType(),  True),
])

# ── F8. navigator.bibliography ────────────────────────────────────────────────
NAV_BIBLIO_SCHEMA = T.StructType([
    T.StructField("bib_ref",       T.IntegerType(), False),
    T.StructField("paper_id",      T.StringType(),  True),
    T.StructField("title",         T.StringType(),  True),
    T.StructField("authors",       T.ArrayType(T.StringType()), True),
    T.StructField("year",          T.IntegerType(), True),
    T.StructField("journal",       T.StringType(),  True),
    T.StructField("url",           T.StringType(),  True),
    T.StructField("source_type",   T.StringType(),  True),
    T.StructField("equity_tags",   T.ArrayType(T.StringType()), True),
    T.StructField("evidence_rung", T.IntegerType(), True),
    T.StructField("abstract",      T.StringType(),  True),
    T.StructField("ingested_at",   T.StringType(),  True),
])

# ── F9. graph.nodes ───────────────────────────────────────────────────────────
GRAPH_NODES_SCHEMA = T.StructType([
    T.StructField("node_id",    T.StringType(),  False),
    T.StructField("label",      T.StringType(),  True),
    T.StructField("node_type",  T.StringType(),  True),
    T.StructField("paper_ids",  T.ArrayType(T.StringType()), True),
    T.StructField("frequency",  T.IntegerType(), True),
    T.StructField("created_at", T.StringType(),  True),
])

# ── F10. graph.edges ──────────────────────────────────────────────────────────
GRAPH_EDGES_SCHEMA = T.StructType([
    T.StructField("edge_id",      T.StringType(),  False),
    T.StructField("paper_id",     T.StringType(),  True),
    T.StructField("paper_title",  T.StringType(),  True),
    T.StructField("from_id",      T.StringType(),  True),
    T.StructField("from_label",   T.StringType(),  True),
    T.StructField("from_type",    T.StringType(),  True),
    T.StructField("to_id",        T.StringType(),  True),
    T.StructField("to_label",     T.StringType(),  True),
    T.StructField("to_type",      T.StringType(),  True),
    T.StructField("relation",     T.StringType(),  True),
    T.StructField("weight",       T.DoubleType(),  True),
    T.StructField("evidence_rung",T.IntegerType(), True),
    T.StructField("built_at",     T.StringType(),  True),
])

# ── F11. synthesis.briefs ─────────────────────────────────────────────────────
SYN_BRIEFS_SCHEMA = T.StructType([
    T.StructField("brief_id",          T.StringType(),  False),
    T.StructField("question",          T.StringType(),  True),
    T.StructField("brief_type",        T.StringType(),  True),
    T.StructField("program_context",   T.StringType(),  True),
    T.StructField("streams_used",      T.ArrayType(T.StringType()), True),
    T.StructField("papers_count",      T.IntegerType(), True),
    T.StructField("signals_count",     T.IntegerType(), True),
    T.StructField("resources_count",   T.IntegerType(), True),
    T.StructField("executive_brief",   T.StringType(),  True),
    T.StructField("market_context",    T.StringType(),  True),
    T.StructField("evidence_summary",  T.StringType(),  True),
    T.StructField("equity_lens",       T.StringType(),  True),
    T.StructField("recommendations",   T.StringType(),  True),
    T.StructField("confidence",        T.StringType(),  True),
    T.StructField("evidence_strength", T.StringType(),  True),
    T.StructField("created_by",        T.StringType(),  True),
    T.StructField("created_at",        T.StringType(),  True),
])

# ── F12. synthesis.provenance ─────────────────────────────────────────────────
SYN_PROVENANCE_SCHEMA = T.StructType([
    T.StructField("brief_id",    T.StringType(),  False),
    T.StructField("source_type", T.StringType(),  True),
    T.StructField("source_id",   T.StringType(),  True),
    T.StructField("source_title",T.StringType(),  True),
    T.StructField("source_url",  T.StringType(),  True),
    T.StructField("relevance",   T.DoubleType(),  True),
    T.StructField("cited_at",    T.StringType(),  True),
])

print("✓ Section F — 12 Delta table schemas defined")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION G — Write helpers (all 12 tables)
# ══════════════════════════════════════════════════════════════════════════════

def write_papers(papers: list[dict], mode: str = "append"):
    df = spark.createDataFrame(papers, schema=PAPERS_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_PAPERS)
    print(f"✓ {len(papers)} papers → {TABLE_PAPERS}")

def write_scorecards(scorecards: list[dict], mode: str = "append"):
    df = spark.createDataFrame(scorecards, schema=SCORECARD_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_SCORECARD)
    print(f"✓ {len(scorecards)} scorecards → {TABLE_SCORECARD}")

def write_roi_brief(result: dict, mode: str = "append"):
    brief = {k: v for k, v in result.items()
             if k not in ("papers","roi_parameters","synthesis_raw")}
    df = spark.createDataFrame([brief], schema=ROI_BRIEFS_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_ROI_BRIEFS)
    roi = result.get("roi_parameters") or {}
    if roi:
        es = roi.get("effect_size",{})
        row = [{
            "brief_id":           result["brief_id"],
            "question":           result["question"],
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
            "run_at":             result["run_at"],
        }]
        pdf = spark.createDataFrame(row, schema=ROI_PARAMS_SCHEMA)
        pdf.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_ROI_PARAMS)
    print(f"✓ ROI brief → {TABLE_ROI_BRIEFS}")

def write_aa_benchmarks(models: list[dict], mode: str = "overwrite"):
    df = spark.createDataFrame(models, schema=AA_BENCHMARKS_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_AA_BENCHMARKS)
    print(f"✓ {len(models)} benchmarks → {TABLE_AA_BENCHMARKS}")

def write_market_signals(signals: list[dict], mode: str = "append"):
    df = spark.createDataFrame(signals, schema=MARKET_SIGNALS_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_MARKET_SIGNALS)
    print(f"✓ {len(signals)} signals → {TABLE_MARKET_SIGNALS}")

def write_nav_resources(resources: list[dict], mode: str = "append"):
    df = spark.createDataFrame(resources, schema=NAV_RESOURCES_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_NAV_RESOURCES)
    print(f"✓ {len(resources)} resources → {TABLE_NAV_RESOURCES}")

def write_nav_bibliography(papers: list[dict], mode: str = "overwrite"):
    df = spark.createDataFrame(papers, schema=NAV_BIBLIO_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_NAV_BIBLIO)
    print(f"✓ {len(papers)} bibliography entries → {TABLE_NAV_BIBLIO}")

def write_graph(graph_data: dict, mode: str = "append"):
    node_map = {n["id"]: n for n in graph_data.get("nodes",[])}
    edge_rows = []
    for e in graph_data.get("edges",[]):
        src = node_map.get(e["from"],{})
        tgt = node_map.get(e["to"],  {})
        edge_rows.append({
            "edge_id":     _uid(),
            "paper_id":    graph_data.get("paper_id",""),
            "paper_title": graph_data.get("paper_title",""),
            "from_id":     e["from"],
            "from_label":  src.get("label",""),
            "from_type":   src.get("type",""),
            "to_id":       e["to"],
            "to_label":    tgt.get("label",""),
            "to_type":     tgt.get("type",""),
            "relation":    e.get("relation",""),
            "weight":      float(e.get("weight",0.5)),
            "evidence_rung":0,
            "built_at":    graph_data.get("built_at",""),
        })
    node_rows = [{
        "node_id":   n["id"],
        "label":     n.get("label",""),
        "node_type": n.get("type",""),
        "paper_ids": [graph_data.get("paper_id","")],
        "frequency": 1,
        "created_at":graph_data.get("built_at",""),
    } for n in graph_data.get("nodes",[])]
    if node_rows:
        ndf = spark.createDataFrame(node_rows, schema=GRAPH_NODES_SCHEMA)
        ndf.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_GRAPH_NODES)
    if edge_rows:
        edf = spark.createDataFrame(edge_rows, schema=GRAPH_EDGES_SCHEMA)
        edf.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_GRAPH_EDGES)
    print(f"✓ Graph: {len(node_rows)} nodes, {len(edge_rows)} edges → {CATALOG}.graph.*")

def write_synthesis_brief(brief: dict, provenance: list[dict] = None, mode: str = "append"):
    bdf = spark.createDataFrame([brief], schema=SYN_BRIEFS_SCHEMA)
    bdf.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_SYN_BRIEFS)
    if provenance:
        pdf = spark.createDataFrame(provenance, schema=SYN_PROVENANCE_SCHEMA)
        pdf.write.format("delta").mode(mode).option("mergeSchema","true").saveAsTable(TABLE_SYN_PROVENANCE)
    print(f"✓ Synthesis brief {brief['brief_id']} → {TABLE_SYN_BRIEFS}")

print("✓ Section G — Write helpers defined")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION H — Cross-tool SQL views
# ══════════════════════════════════════════════════════════════════════════════

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.synthesis.v_evidence_heatmap AS
SELECT
    p.categories[0]               AS topic,
    s.evidence_rung,
    s.effect_direction,
    COUNT(*)                       AS paper_count,
    ROUND(AVG(s.total_score), 2)  AS avg_score
FROM {TABLE_SCORECARD} s
JOIN {TABLE_PAPERS} p ON s.paper_id = p.paper_id
GROUP BY topic, s.evidence_rung, s.effect_direction
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.synthesis.v_model_frontier AS
SELECT
    model_name, provider,
    intelligence_index,
    input_cost_per_1m,
    ROUND(intelligence_index / NULLIF(input_cost_per_1m,0), 1) AS intelligence_per_dollar,
    is_open_weights, lmic_accessible
FROM {TABLE_AA_BENCHMARKS}
ORDER BY intelligence_per_dollar DESC
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.synthesis.v_roi_by_pillar AS
SELECT
    rp.pillar,
    rb.evidence_strength,
    ROUND(AVG(rp.effect_base), 3) AS avg_base_effect,
    ROUND(AVG(rp.effect_low),  3) AS avg_low_effect,
    ROUND(AVG(rp.effect_high), 3) AS avg_high_effect,
    COUNT(DISTINCT rp.brief_id)   AS brief_count
FROM {TABLE_ROI_PARAMS} rp
JOIN {TABLE_ROI_BRIEFS} rb ON rp.brief_id = rb.brief_id
GROUP BY rp.pillar, rb.evidence_strength
ORDER BY avg_base_effect DESC
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.synthesis.v_equity_coverage AS
SELECT
    eq_theme,
    COUNT(*)                         AS resource_count,
    SUM(CAST(lmic_relevant AS INT))  AS lmic_resources,
    SUM(CAST(k12_relevant  AS INT))  AS k12_resources,
    SUM(CAST(featured AS INT))       AS featured_count
FROM {TABLE_NAV_RESOURCES}
LATERAL VIEW explode(equity_themes) AS eq_theme
GROUP BY eq_theme ORDER BY resource_count DESC
""")

print("✓ Section H — 4 cross-tool SQL views created")
print(f"  {CATALOG}.synthesis.v_evidence_heatmap")
print(f"  {CATALOG}.synthesis.v_model_frontier")
print(f"  {CATALOG}.synthesis.v_roi_by_pillar")
print(f"  {CATALOG}.synthesis.v_equity_coverage")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION I — Demo run (single paper + one ROI question)
# Comment out or replace with your own inputs
# ══════════════════════════════════════════════════════════════════════════════

# ── Fetch + score one arXiv paper ────────────────────────────────────────────
demo_paper = fetch_arxiv_paper("2304.03442")
print(f"✓ Fetched: {demo_paper['title'][:70]}…")

demo_score = score_paper(demo_paper)
print(f"✓ Scored : {demo_score['total_score']}/30 · {demo_score['tier']}")
print(f"  Signal : {demo_score['investment_signal']}")

write_papers([demo_paper], mode="overwrite")
write_scorecards([demo_score])

# ── Build knowledge graph ────────────────────────────────────────────────────
graph_data = build_knowledge_graph(demo_paper)
print(f"✓ Graph  : {len(graph_data['nodes'])} nodes, {len(graph_data['edges'])} edges")
write_graph(graph_data, mode="overwrite")

# ── Run ROI Agent ────────────────────────────────────────────────────────────
roi_result = run_roi_agent(
    "What is the evidence for AI tutoring systems improving math outcomes for K-12 students?"
)
print(f"\n✓ Evidence strength : {roi_result['evidence_strength']}")
print(f"  Confidence        : {roi_result['confidence']}")
print(f"  Key risk          : {roi_result['key_risk']}")
write_papers(roi_result["papers"])
write_roi_brief(roi_result)

# ── Fetch Market Insights (Artificial Analysis) ──────────────────────────────
aa_models = fetch_aa_benchmarks()
print(f"✓ AA benchmarks: {len(aa_models)} models fetched")
write_aa_benchmarks(aa_models)

print("\n✓ Demo run complete — all tables populated")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION J — Batch processing (run at scale)
# ══════════════════════════════════════════════════════════════════════════════

BATCH_ARXIV_IDS = [
    "2304.03442",   # Generative Agents (simulacra)
    "2411.10109",   # Scaling up personas
    "2502.00640",   # LLM optimisation via simulation
    "2511.00222",   # Simulation + RL
    "2507.22049",   # Social simulation + psych validation
    "2310.06837",   # Simulating student responses
    # ── add more IDs ─────────────────────────────────────────────
]

BATCH_ROI_QUESTIONS = [
    "What is the evidence for AI tutoring improving math outcomes for K-12 students?",
    "What does the research say about LLM-powered simulation for education research?",
    # ── add more questions ────────────────────────────────────────
]

def run_batch(arxiv_ids: list[str], roi_questions: list[str]):
    print("=" * 60)
    print("BATCH RUN — Evidence Scout + ROI Agent")
    print("=" * 60)

    print(f"\n── Fetching {len(arxiv_ids)} arXiv papers…")
    papers = fetch_arxiv_batch(arxiv_ids)
    write_papers(papers)

    print(f"\n── Scoring {len(papers)} papers…")
    scorecards = score_papers_batch(papers, delay=1.5)
    write_scorecards(scorecards)

    print(f"\n── Building knowledge graphs…")
    for p in papers:
        try:
            write_graph(build_knowledge_graph(p))
            time.sleep(0.5)
        except Exception as e:
            print(f"  ⚠ {p['paper_id']}: {e}")

    for q in roi_questions:
        print(f"\n── ROI Agent: {q[:70]}…")
        try:
            result = run_roi_agent(q)
            write_papers(result["papers"])
            write_roi_brief(result)
        except Exception as e:
            print(f"  ⚠ ROI failed: {e}")
        time.sleep(2)

    print(f"\n── Refreshing Market Insights…")
    try:
        write_aa_benchmarks(fetch_aa_benchmarks())
    except Exception as e:
        print(f"  ⚠ AA fetch failed: {e}")

    print("\n✓ Batch complete.")

# Uncomment to run:
# run_batch(BATCH_ARXIV_IDS, BATCH_ROI_QUESTIONS)


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION K — Exploration queries
# ══════════════════════════════════════════════════════════════════════════════

# Top-scoring papers
display(spark.sql(f"""
    SELECT paper_id, title, total_score, tier, investment_signal
    FROM {TABLE_SCORECARD} ORDER BY total_score DESC LIMIT 20
"""))

# COMMAND ----------
# Investment signal breakdown
display(spark.sql(f"""
    SELECT investment_signal, COUNT(*) AS papers,
           ROUND(AVG(total_score),2) AS avg_score
    FROM {TABLE_SCORECARD}
    GROUP BY investment_signal ORDER BY avg_score DESC
"""))

# COMMAND ----------
# ROI evidence by pillar
display(spark.sql(f"SELECT * FROM {CATALOG}.synthesis.v_roi_by_pillar"))

# COMMAND ----------
# Model intelligence-per-dollar frontier
display(spark.sql(f"SELECT * FROM {CATALOG}.synthesis.v_model_frontier LIMIT 20"))

# COMMAND ----------
# Evidence heat map by topic
display(spark.sql(f"SELECT * FROM {CATALOG}.synthesis.v_evidence_heatmap ORDER BY avg_score DESC LIMIT 30"))

# COMMAND ----------
# Navigator equity coverage
display(spark.sql(f"SELECT * FROM {CATALOG}.synthesis.v_equity_coverage"))

# COMMAND ----------
# Knowledge graph — most connected concepts
display(spark.sql(f"""
    SELECT from_label AS concept, from_type AS type,
           COUNT(*) AS out_degree
    FROM {TABLE_GRAPH_EDGES}
    GROUP BY from_label, from_type
    ORDER BY out_degree DESC LIMIT 20
"""))

# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# CATALOG SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 68)
print("  Data & AI Enablement Hub — Databricks Unity Catalog")
print("  Catalog: research_hub")
print("=" * 68)
for tool, tables in {
    "Evidence Scout":     [TABLE_PAPERS, TABLE_SCORECARD],
    "ROI Research Agent": [TABLE_ROI_BRIEFS, TABLE_ROI_PARAMS],
    "Market Insights":    [TABLE_AA_BENCHMARKS, TABLE_MARKET_SIGNALS],
    "Navigator":          [TABLE_NAV_RESOURCES, TABLE_NAV_BIBLIO],
    "Knowledge Graph":    [TABLE_GRAPH_NODES, TABLE_GRAPH_EDGES],
    "Synthesis Layer":    [TABLE_SYN_BRIEFS, TABLE_SYN_PROVENANCE],
}.items():
    print(f"\n  {tool}")
    for t in tables:
        print(f"    {t}")
print("\n  Cross-tool views → research_hub.synthesis.v_*")
print("=" * 68)
