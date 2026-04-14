# Databricks notebook source
# ══════════════════════════════════════════════════════════════════════════════
# Research & Discovery — Data & AI Enablement Hub
# COMPLETE DATABRICKS SCHEMA — All Six Tools
#
# Tools covered:
#   1. Evidence Scout          → papers, evidence_scorecard
#   2. Market Insights         → aa_model_benchmarks, market_signals
#   3. AI Literacy Navigator   → navigator_resources, navigator_bibliography
#   4. ROI Research Agent      → roi_briefs, roi_parameters
#   5. Knowledge Graph         → knowledge_graph_nodes, knowledge_graph_edges
#   6. Synthesis Layer         → synthesis_briefs, synthesis_provenance
#
# Prerequisites
#   Cluster : DBR 14.3 LTS ML (Python 3.10+, Spark 3.5)
#   Secrets : databricks secrets put --scope research-hub --key anthropic-api-key
#             databricks secrets put --scope research-hub --key artificial-analysis-api-key
#
# Unity Catalog layout
#   research_hub.evidence.*      — Literature / Evidence Scout
#   research_hub.market.*        — Market Insights (Artificial Analysis)
#   research_hub.navigator.*     — AI Literacy & Equity Navigator
#   research_hub.roi.*           — ROI Research Agent
#   research_hub.graph.*         — Knowledge Graph
#   research_hub.synthesis.*     — Synthesis Layer (cross-tool briefs)
# ══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# ── 0. Install dependencies ──────────────────────────────────────────────────
%pip install anthropic httpx tenacity networkx --quiet

# COMMAND ----------
# ── 1. Imports & configuration ───────────────────────────────────────────────
import json, re, time, math, textwrap
from datetime import datetime, timezone
from typing import Any

import httpx
import anthropic
from tenacity import retry, stop_after_attempt, wait_exponential
from pyspark.sql import functions as F, types as T

ANTHROPIC_API_KEY = dbutils.secrets.get("research-hub", "anthropic-api-key")
AA_API_KEY        = dbutils.secrets.get("research-hub", "artificial-analysis-api-key")

CATALOG = "research_hub"
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

for schema in ["evidence", "market", "navigator", "roi", "graph", "synthesis"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

def now() -> str:
    return datetime.now(timezone.utc).isoformat()

print("✓ Config ready — catalogs and schemas initialised")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — EVIDENCE SCOUT
# Tables: research_hub.evidence.papers
#         research_hub.evidence.scorecard
# ══════════════════════════════════════════════════════════════════════════════

TABLE_PAPERS    = f"{CATALOG}.evidence.papers"
TABLE_SCORECARD = f"{CATALOG}.evidence.scorecard"

PAPERS_SCHEMA = T.StructType([
    # ── Identity ──────────────────────────────────────────────────────────────
    T.StructField("paper_id",       T.StringType(),  False),  # arXiv ID or DOI
    T.StructField("source",         T.StringType(),  True),   # "arXiv" | "Semantic Scholar" | "ERIC" | "OpenAlex"
    T.StructField("doi",            T.StringType(),  True),
    T.StructField("url",            T.StringType(),  True),
    T.StructField("pdf_url",        T.StringType(),  True),
    # ── Bibliographic ─────────────────────────────────────────────────────────
    T.StructField("title",          T.StringType(),  True),
    T.StructField("abstract",       T.StringType(),  True),
    T.StructField("authors",        T.ArrayType(T.StringType()), True),
    T.StructField("published_date", T.StringType(),  True),   # YYYY-MM-DD
    T.StructField("journal",        T.StringType(),  True),
    T.StructField("categories",     T.ArrayType(T.StringType()), True),
    T.StructField("citation_count", T.IntegerType(), True),
    # ── Context ───────────────────────────────────────────────────────────────
    T.StructField("query_used",     T.StringType(),  True),   # search query that surfaced it
    T.StructField("tool_source",    T.StringType(),  True),   # "evidence_scout" | "roi_agent" | "manual"
    T.StructField("fetched_at",     T.StringType(),  True),
])

SCORECARD_SCHEMA = T.StructType([
    T.StructField("paper_id",          T.StringType(),  False),
    T.StructField("title",             T.StringType(),  True),
    # ── Evidence ladder ───────────────────────────────────────────────────────
    T.StructField("evidence_rung",     T.IntegerType(), True),   # 1–6
    T.StructField("evidence_type",     T.StringType(),  True),   # RCT | QED | Implementation | etc.
    T.StructField("study_design",      T.StringType(),  True),
    # ── Outcome dimensions ────────────────────────────────────────────────────
    T.StructField("outcome_domain",    T.StringType(),  True),   # Cognitive | Behavioral | Affective
    T.StructField("population",        T.StringType(),  True),
    T.StructField("sample_size",       T.IntegerType(), True),
    T.StructField("effect_direction",  T.StringType(),  True),   # Positive | Negative | Mixed | None
    T.StructField("effect_size_raw",   T.StringType(),  True),
    # ── AI scoring dimensions ─────────────────────────────────────────────────
    T.StructField("score_relevance",   T.DoubleType(),  True),   # 0–1
    T.StructField("score_rigor",       T.DoubleType(),  True),
    T.StructField("score_recency",     T.DoubleType(),  True),
    T.StructField("score_equity",      T.DoubleType(),  True),
    T.StructField("total_score",       T.DoubleType(),  True),
    T.StructField("tier",              T.StringType(),  True),   # "Strong" | "Moderate" | "Emerging" | "Weak"
    T.StructField("investment_signal", T.StringType(),  True),
    T.StructField("verdict",           T.StringType(),  True),   # 2–3 sentence summary
    T.StructField("scores_json",       T.StringType(),  True),   # full raw JSON blob
    T.StructField("scored_at",         T.StringType(),  True),
])

def write_papers(papers: list[dict], mode: str = "append"):
    df = spark.createDataFrame(papers, schema=PAPERS_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(TABLE_PAPERS)
    print(f"✓ {len(papers)} papers → {TABLE_PAPERS}")

def write_scorecards(scorecards: list[dict], mode: str = "append"):
    df = spark.createDataFrame(scorecards, schema=SCORECARD_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(TABLE_SCORECARD)
    print(f"✓ {len(scorecards)} scorecards → {TABLE_SCORECARD}")

# ── Quick insight queries ─────────────────────────────────────────────────────
def evidence_insights():
    print("\n── Evidence Scout Insights ──────────────────────────────────")
    spark.sql(f"""
        SELECT tier, COUNT(*) AS paper_count, ROUND(AVG(total_score),2) AS avg_score
        FROM {TABLE_SCORECARD}
        GROUP BY tier ORDER BY avg_score DESC
    """).show()

    spark.sql(f"""
        SELECT effect_direction, COUNT(*) AS n
        FROM {TABLE_SCORECARD}
        GROUP BY effect_direction ORDER BY n DESC
    """).show()

    spark.sql(f"""
        SELECT s.tier, p.categories[0] AS primary_category, COUNT(*) AS n
        FROM {TABLE_SCORECARD} s
        JOIN {TABLE_PAPERS} p ON s.paper_id = p.paper_id
        GROUP BY s.tier, primary_category ORDER BY n DESC LIMIT 20
    """).show()

print("✓ Section 1 — Evidence Scout schemas defined")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — MARKET INSIGHTS (Artificial Analysis live benchmarks)
# Tables: research_hub.market.aa_model_benchmarks
#         research_hub.market.market_signals
# ══════════════════════════════════════════════════════════════════════════════

TABLE_AA_BENCHMARKS  = f"{CATALOG}.market.aa_model_benchmarks"
TABLE_MARKET_SIGNALS = f"{CATALOG}.market.market_signals"

AA_BENCHMARKS_SCHEMA = T.StructType([
    # ── Model identity ────────────────────────────────────────────────────────
    T.StructField("model_id",           T.StringType(),  False),
    T.StructField("model_name",         T.StringType(),  True),
    T.StructField("provider",           T.StringType(),  True),   # OpenAI | Anthropic | Google | Meta | Mistral | etc.
    T.StructField("model_family",       T.StringType(),  True),   # GPT-4 | Claude 3 | Gemini | Llama | etc.
    T.StructField("release_date",       T.StringType(),  True),
    # ── Performance (Artificial Analysis 2026 benchmark) ──────────────────────
    T.StructField("intelligence_index", T.DoubleType(),  True),   # overall AA intelligence score
    T.StructField("mmlu_score",         T.DoubleType(),  True),
    T.StructField("math_score",         T.DoubleType(),  True),
    T.StructField("coding_score",       T.DoubleType(),  True),
    T.StructField("reasoning_score",    T.DoubleType(),  True),
    T.StructField("multilingual_score", T.DoubleType(),  True),
    # ── Cost & speed ──────────────────────────────────────────────────────────
    T.StructField("input_cost_per_1m",  T.DoubleType(),  True),   # USD per 1M input tokens
    T.StructField("output_cost_per_1m", T.DoubleType(),  True),
    T.StructField("context_window_k",   T.IntegerType(), True),   # context length in K tokens
    T.StructField("latency_ms_p50",     T.IntegerType(), True),   # median first-token latency
    T.StructField("throughput_tok_s",   T.IntegerType(), True),   # output tokens / sec
    # ── Access & sovereignty flags ────────────────────────────────────────────
    T.StructField("is_open_weights",    T.BooleanType(), True),
    T.StructField("lmic_accessible",    T.BooleanType(), True),   # available in LMIC regions
    T.StructField("data_residency",     T.StringType(),  True),   # "US" | "EU" | "Global" | "On-prem"
    # ── Metadata ──────────────────────────────────────────────────────────────
    T.StructField("source_url",         T.StringType(),  True),
    T.StructField("fetched_at",         T.StringType(),  True),
])

MARKET_SIGNALS_SCHEMA = T.StructType([
    # ── Signal identity ───────────────────────────────────────────────────────
    T.StructField("signal_id",      T.StringType(),  False),   # UUID
    T.StructField("signal_type",    T.StringType(),  True),    # "hyperscaler" | "investment" | "field_scan" | "policy"
    T.StructField("title",          T.StringType(),  True),
    T.StructField("source_org",     T.StringType(),  True),    # Microsoft | Google | OECD | etc.
    T.StructField("source_url",     T.StringType(),  True),
    # ── Content ───────────────────────────────────────────────────────────────
    T.StructField("summary",        T.StringType(),  True),
    T.StructField("key_claim",      T.StringType(),  True),
    T.StructField("investment_usd", T.DoubleType(),  True),    # announced investment, NULL if N/A
    T.StructField("geography",      T.StringType(),  True),    # "US" | "EU" | "LMIC" | "Global"
    T.StructField("ai_sector",      T.StringType(),  True),    # "EdTech" | "Infrastructure" | "Foundation Models" | etc.
    # ── Relevance to portfolio ────────────────────────────────────────────────
    T.StructField("relevance_score",T.DoubleType(),  True),    # 0–1 (AI scored)
    T.StructField("lmic_flag",      T.BooleanType(), True),    # signals LMIC-relevant?
    T.StructField("tags",           T.ArrayType(T.StringType()), True),
    # ── Freshness ─────────────────────────────────────────────────────────────
    T.StructField("signal_date",    T.StringType(),  True),    # date of original signal
    T.StructField("ingested_at",    T.StringType(),  True),
])

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
            "model_id":           m.get("id", ""),
            "model_name":         m.get("name", ""),
            "provider":           m.get("provider", ""),
            "model_family":       m.get("family", ""),
            "release_date":       m.get("release_date", ""),
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
            "data_residency":     m.get("data_residency", ""),
            "source_url":         m.get("url", ""),
            "fetched_at":         now(),
        })
    return models

def write_aa_benchmarks(models: list[dict], mode: str = "overwrite"):
    df = spark.createDataFrame(models, schema=AA_BENCHMARKS_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(TABLE_AA_BENCHMARKS)
    print(f"✓ {len(models)} model benchmarks → {TABLE_AA_BENCHMARKS}")

def write_market_signals(signals: list[dict], mode: str = "append"):
    df = spark.createDataFrame(signals, schema=MARKET_SIGNALS_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(TABLE_MARKET_SIGNALS)
    print(f"✓ {len(signals)} market signals → {TABLE_MARKET_SIGNALS}")

def market_insights():
    print("\n── Market Insights ──────────────────────────────────────────")
    spark.sql(f"""
        SELECT provider, COUNT(*) AS model_count,
               ROUND(AVG(intelligence_index),1) AS avg_intel,
               ROUND(MIN(input_cost_per_1m),4) AS min_input_cost,
               ROUND(MAX(input_cost_per_1m),4) AS max_input_cost
        FROM {TABLE_AA_BENCHMARKS}
        GROUP BY provider ORDER BY avg_intel DESC
    """).show()

    spark.sql(f"""
        SELECT model_name, provider, intelligence_index, input_cost_per_1m,
               is_open_weights, lmic_accessible
        FROM {TABLE_AA_BENCHMARKS}
        ORDER BY intelligence_index DESC LIMIT 10
    """).show()

print("✓ Section 2 — Market Insights schemas defined")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — AI LITERACY & EQUITY NAVIGATOR
# Tables: research_hub.navigator.resources
#         research_hub.navigator.bibliography
# ══════════════════════════════════════════════════════════════════════════════

TABLE_NAV_RESOURCES = f"{CATALOG}.navigator.resources"
TABLE_NAV_BIBLIO    = f"{CATALOG}.navigator.bibliography"

NAV_RESOURCES_SCHEMA = T.StructType([
    # ── Identity ──────────────────────────────────────────────────────────────
    T.StructField("resource_id",   T.StringType(),  False),
    T.StructField("title",         T.StringType(),  True),
    T.StructField("source_org",    T.StringType(),  True),
    T.StructField("url",           T.StringType(),  True),
    # ── Classification ────────────────────────────────────────────────────────
    T.StructField("category",      T.StringType(),  True),   # policy | literacy | workforce | change | tools
    T.StructField("audience",      T.ArrayType(T.StringType()), True),  # practitioner | grantee | leader | policymaker
    T.StructField("level",         T.StringType(),  True),   # beginner | intermediate | advanced
    T.StructField("format",        T.StringType(),  True),   # report | framework | tool | course | dataset
    T.StructField("topics",        T.ArrayType(T.StringType()), True),
    # ── Equity dimensions ─────────────────────────────────────────────────────
    T.StructField("equity_themes", T.ArrayType(T.StringType()), True),  # Algorithm Bias | Digital Divide | Racial Justice | etc.
    T.StructField("lmic_relevant", T.BooleanType(), True),
    T.StructField("k12_relevant",  T.BooleanType(), True),
    # ── Curation metadata ─────────────────────────────────────────────────────
    T.StructField("summary",       T.StringType(),  True),
    T.StructField("why_useful",    T.StringType(),  True),
    T.StructField("watch_out",     T.StringType(),  True),
    T.StructField("freshness",     T.StringType(),  True),   # current | foundational | needs_update
    T.StructField("featured",      T.BooleanType(), True),
    T.StructField("notes",         T.StringType(),  True),
    T.StructField("added_by",      T.StringType(),  True),   # "hub" | email/user ID
    T.StructField("created_at",    T.StringType(),  True),
    T.StructField("updated_at",    T.StringType(),  True),
])

NAV_BIBLIOGRAPHY_SCHEMA = T.StructType([
    # ── Identity ──────────────────────────────────────────────────────────────
    T.StructField("bib_ref",       T.IntegerType(), False),  # [1]–[74] reference number
    T.StructField("paper_id",      T.StringType(),  True),   # arXiv ID or DOI
    T.StructField("title",         T.StringType(),  True),
    T.StructField("authors",       T.ArrayType(T.StringType()), True),
    T.StructField("year",          T.IntegerType(), True),
    T.StructField("journal",       T.StringType(),  True),
    T.StructField("url",           T.StringType(),  True),
    # ── Source type tag ───────────────────────────────────────────────────────
    T.StructField("source_type",   T.StringType(),  True),   # arXiv | OpenAlex | ERIC | other
    # ── Equity relevance ──────────────────────────────────────────────────────
    T.StructField("equity_tags",   T.ArrayType(T.StringType()), True),
    T.StructField("evidence_rung", T.IntegerType(), True),   # 1–6 if scored
    T.StructField("abstract",      T.StringType(),  True),
    T.StructField("ingested_at",   T.StringType(),  True),
])

def write_nav_resources(resources: list[dict], mode: str = "append"):
    df = spark.createDataFrame(resources, schema=NAV_RESOURCES_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(TABLE_NAV_RESOURCES)
    print(f"✓ {len(resources)} navigator resources → {TABLE_NAV_RESOURCES}")

def write_nav_bibliography(papers: list[dict], mode: str = "overwrite"):
    df = spark.createDataFrame(papers, schema=NAV_BIBLIOGRAPHY_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(TABLE_NAV_BIBLIO)
    print(f"✓ {len(papers)} bibliography entries → {TABLE_NAV_BIBLIO}")

def navigator_insights():
    print("\n── Navigator Resource Insights ──────────────────────────────")
    spark.sql(f"""
        SELECT category, freshness, COUNT(*) AS n
        FROM {TABLE_NAV_RESOURCES}
        GROUP BY category, freshness ORDER BY category, n DESC
    """).show()

    spark.sql(f"""
        SELECT eq_theme, COUNT(*) AS n
        FROM {TABLE_NAV_RESOURCES}
        LATERAL VIEW explode(equity_themes) AS eq_theme
        GROUP BY eq_theme ORDER BY n DESC
    """).show()

print("✓ Section 3 — Navigator schemas defined")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — ROI RESEARCH AGENT
# Tables: research_hub.roi.briefs
#         research_hub.roi.parameters
# ══════════════════════════════════════════════════════════════════════════════

TABLE_ROI_BRIEFS  = f"{CATALOG}.roi.briefs"
TABLE_ROI_PARAMS  = f"{CATALOG}.roi.parameters"

ROI_BRIEFS_SCHEMA = T.StructType([
    # ── Query ─────────────────────────────────────────────────────────────────
    T.StructField("brief_id",             T.StringType(),  False),  # UUID
    T.StructField("question",             T.StringType(),  True),
    T.StructField("decision_context",     T.StringType(),  True),
    T.StructField("investment_amount_usd",T.DoubleType(),  True),   # if provided
    T.StructField("program_stage",        T.StringType(),  True),   # "explore" | "design" | "scale"
    # ── Evidence synthesis ────────────────────────────────────────────────────
    T.StructField("papers_retrieved",     T.IntegerType(), True),
    T.StructField("evidence_strength",    T.StringType(),  True),   # Strong | Moderate | Emerging | Insufficient
    T.StructField("confidence",           T.StringType(),  True),   # High | Medium | Low
    T.StructField("key_risk",             T.StringType(),  True),
    T.StructField("investment_readiness", T.StringType(),  True),
    # ── Outputs ───────────────────────────────────────────────────────────────
    T.StructField("executive_summary",    T.StringType(),  True),
    T.StructField("synthesised_findings", T.StringType(),  True),
    T.StructField("evidence_gaps",        T.StringType(),  True),
    T.StructField("recommendations",      T.StringType(),  True),
    T.StructField("discovery_insights",   T.StringType(),  True),
    # ── Equity flags ──────────────────────────────────────────────────────────
    T.StructField("equity_considerations",T.StringType(),  True),
    T.StructField("lmic_signals",         T.StringType(),  True),
    # ── Metadata ──────────────────────────────────────────────────────────────
    T.StructField("confidence_map_json",  T.StringType(),  True),
    T.StructField("run_at",               T.StringType(),  True),
])

ROI_PARAMS_SCHEMA = T.StructType([
    T.StructField("brief_id",           T.StringType(),  False),
    T.StructField("question",           T.StringType(),  True),
    T.StructField("pillar",             T.StringType(),  True),   # Cognitive | Behavioral | Affective | Workforce
    # ── Effect sizes ──────────────────────────────────────────────────────────
    T.StructField("evidence_tier",      T.IntegerType(), True),   # 1–6 (evidence ladder)
    T.StructField("similarity_score",   T.DoubleType(),  True),   # 0–1 fit to query
    T.StructField("effect_low",         T.DoubleType(),  True),
    T.StructField("effect_base",        T.DoubleType(),  True),
    T.StructField("effect_high",        T.DoubleType(),  True),
    T.StructField("effect_unit",        T.StringType(),  True),   # "SD" | "%" | "months" | etc.
    T.StructField("evidence_type",      T.StringType(),  True),   # RCT | QED | Observational
    T.StructField("evidence_score",     T.DoubleType(),  True),
    T.StructField("source_count",       T.IntegerType(), True),
    # ── JSON blobs ────────────────────────────────────────────────────────────
    T.StructField("pipeline_json",      T.StringType(),  True),   # cost/benefit pipeline steps
    T.StructField("subgroups_json",     T.StringType(),  True),   # equity subgroup modifiers
    T.StructField("proxy_studies_json", T.StringType(),  True),
    T.StructField("roi_raw_json",       T.StringType(),  True),
    T.StructField("run_at",             T.StringType(),  True),
])

def write_roi_briefs(briefs: list[dict], mode: str = "append"):
    df = spark.createDataFrame(briefs, schema=ROI_BRIEFS_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(TABLE_ROI_BRIEFS)
    print(f"✓ {len(briefs)} ROI briefs → {TABLE_ROI_BRIEFS}")

def write_roi_params(params: list[dict], mode: str = "append"):
    df = spark.createDataFrame(params, schema=ROI_PARAMS_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(TABLE_ROI_PARAMS)
    print(f"✓ {len(params)} ROI parameter rows → {TABLE_ROI_PARAMS}")

def roi_insights():
    print("\n── ROI Research Agent Insights ──────────────────────────────")
    spark.sql(f"""
        SELECT evidence_strength, confidence, COUNT(*) AS brief_count
        FROM {TABLE_ROI_BRIEFS}
        GROUP BY evidence_strength, confidence ORDER BY brief_count DESC
    """).show()

    spark.sql(f"""
        SELECT pillar, ROUND(AVG(effect_base),3) AS avg_effect,
               ROUND(AVG(evidence_score),2) AS avg_evidence_score,
               COUNT(*) AS n
        FROM {TABLE_ROI_PARAMS}
        GROUP BY pillar ORDER BY avg_effect DESC
    """).show()

print("✓ Section 4 — ROI Agent schemas defined")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — KNOWLEDGE GRAPH
# Tables: research_hub.graph.nodes
#         research_hub.graph.edges
# ══════════════════════════════════════════════════════════════════════════════

TABLE_GRAPH_NODES = f"{CATALOG}.graph.nodes"
TABLE_GRAPH_EDGES = f"{CATALOG}.graph.edges"

GRAPH_NODES_SCHEMA = T.StructType([
    T.StructField("node_id",      T.StringType(),  False),
    T.StructField("label",        T.StringType(),  True),
    T.StructField("node_type",    T.StringType(),  True),  # Concept | Outcome | Intervention | Population | Method
    T.StructField("paper_ids",    T.ArrayType(T.StringType()), True),  # source papers
    T.StructField("frequency",    T.IntegerType(), True),  # times mentioned across papers
    T.StructField("created_at",   T.StringType(),  True),
])

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
    T.StructField("relation",     T.StringType(),  True),  # "improves" | "correlates_with" | "targets" | etc.
    T.StructField("weight",       T.DoubleType(),  True),  # edge strength 0–1
    T.StructField("evidence_rung",T.IntegerType(), True),  # inherited from source paper
    T.StructField("built_at",     T.StringType(),  True),
])

def write_graph_nodes(nodes: list[dict], mode: str = "append"):
    df = spark.createDataFrame(nodes, schema=GRAPH_NODES_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(TABLE_GRAPH_NODES)
    print(f"✓ {len(nodes)} graph nodes → {TABLE_GRAPH_NODES}")

def write_graph_edges(edges: list[dict], mode: str = "append"):
    df = spark.createDataFrame(edges, schema=GRAPH_EDGES_SCHEMA)
    df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(TABLE_GRAPH_EDGES)
    print(f"✓ {len(edges)} graph edges → {TABLE_GRAPH_EDGES}")

def graph_insights():
    print("\n── Knowledge Graph Insights ─────────────────────────────────")
    spark.sql(f"""
        SELECT from_type, to_type, relation, COUNT(*) AS edge_count
        FROM {TABLE_GRAPH_EDGES}
        GROUP BY from_type, to_type, relation ORDER BY edge_count DESC LIMIT 20
    """).show()

    spark.sql(f"""
        SELECT to_label, COUNT(*) AS in_degree
        FROM {TABLE_GRAPH_EDGES}
        WHERE to_type = 'Outcome'
        GROUP BY to_label ORDER BY in_degree DESC LIMIT 15
    """).show()

print("✓ Section 5 — Knowledge Graph schemas defined")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — SYNTHESIS LAYER (cross-tool output)
# Tables: research_hub.synthesis.briefs
#         research_hub.synthesis.provenance
# ══════════════════════════════════════════════════════════════════════════════

TABLE_SYNTHESIS_BRIEFS    = f"{CATALOG}.synthesis.briefs"
TABLE_SYNTHESIS_PROVENANCE= f"{CATALOG}.synthesis.provenance"

SYNTHESIS_BRIEFS_SCHEMA = T.StructType([
    # ── Request ───────────────────────────────────────────────────────────────
    T.StructField("brief_id",          T.StringType(),  False),  # UUID
    T.StructField("question",          T.StringType(),  True),
    T.StructField("brief_type",        T.StringType(),  True),   # "investment_memo" | "landscape_scan" | "equity_brief" | "custom"
    T.StructField("program_context",   T.StringType(),  True),
    # ── Streams used ──────────────────────────────────────────────────────────
    T.StructField("streams_used",      T.ArrayType(T.StringType()), True),  # ["evidence_scout","market_insights"]
    T.StructField("papers_count",      T.IntegerType(), True),
    T.StructField("signals_count",     T.IntegerType(), True),
    T.StructField("resources_count",   T.IntegerType(), True),
    # ── Output ────────────────────────────────────────────────────────────────
    T.StructField("executive_brief",   T.StringType(),  True),
    T.StructField("market_context",    T.StringType(),  True),
    T.StructField("evidence_summary",  T.StringType(),  True),
    T.StructField("equity_lens",       T.StringType(),  True),
    T.StructField("recommendations",   T.StringType(),  True),
    T.StructField("confidence",        T.StringType(),  True),   # High | Medium | Low
    T.StructField("evidence_strength", T.StringType(),  True),
    # ── Metadata ──────────────────────────────────────────────────────────────
    T.StructField("created_by",        T.StringType(),  True),   # user ID / email
    T.StructField("created_at",        T.StringType(),  True),
])

SYNTHESIS_PROVENANCE_SCHEMA = T.StructType([
    T.StructField("brief_id",    T.StringType(),  False),
    T.StructField("source_type", T.StringType(),  True),  # "paper" | "signal" | "resource" | "benchmark"
    T.StructField("source_id",   T.StringType(),  True),  # paper_id / signal_id / resource_id
    T.StructField("source_title",T.StringType(),  True),
    T.StructField("source_url",  T.StringType(),  True),
    T.StructField("relevance",   T.DoubleType(),  True),  # 0–1 contribution weight
    T.StructField("cited_at",    T.StringType(),  True),
])

def write_synthesis_brief(brief: dict, provenance: list[dict], mode: str = "append"):
    bdf = spark.createDataFrame([brief], schema=SYNTHESIS_BRIEFS_SCHEMA)
    bdf.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(TABLE_SYNTHESIS_BRIEFS)
    if provenance:
        pdf = spark.createDataFrame(provenance, schema=SYNTHESIS_PROVENANCE_SCHEMA)
        pdf.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(TABLE_SYNTHESIS_PROVENANCE)
    print(f"✓ Synthesis brief {brief['brief_id']} → {TABLE_SYNTHESIS_BRIEFS} ({len(provenance)} provenance rows)")

def synthesis_insights():
    print("\n── Synthesis Layer Insights ──────────────────────────────────")
    spark.sql(f"""
        SELECT brief_type, confidence, COUNT(*) AS briefs,
               AVG(papers_count) AS avg_papers, AVG(signals_count) AS avg_signals
        FROM {TABLE_SYNTHESIS_BRIEFS}
        GROUP BY brief_type, confidence ORDER BY briefs DESC
    """).show()

    spark.sql(f"""
        SELECT source_type, COUNT(*) AS citations, ROUND(AVG(relevance),2) AS avg_relevance
        FROM {TABLE_SYNTHESIS_PROVENANCE}
        GROUP BY source_type ORDER BY citations DESC
    """).show()

print("✓ Section 6 — Synthesis Layer schemas defined")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — CROSS-TOOL INSIGHT VIEWS (SQL)
# Run these any time to pull structured insights across all tools
# ══════════════════════════════════════════════════════════════════════════════

# View 1: Evidence heat map — which topics have strong evidence?
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

# View 2: Model cost-intelligence frontier (for procurement decisions)
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.synthesis.v_model_frontier AS
SELECT
    model_name, provider,
    intelligence_index,
    input_cost_per_1m,
    ROUND(intelligence_index / NULLIF(input_cost_per_1m, 0), 1) AS intelligence_per_dollar,
    is_open_weights, lmic_accessible
FROM {TABLE_AA_BENCHMARKS}
ORDER BY intelligence_per_dollar DESC
""")

# View 3: ROI signal by outcome pillar
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
GROUP BY rp.pillar, rb.evidence_strength ORDER BY avg_base_effect DESC
""")

# View 4: Navigator equity coverage
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

print("✓ Section 7 — Cross-tool views created")
print("\nViews available:")
print(f"  {CATALOG}.synthesis.v_evidence_heatmap")
print(f"  {CATALOG}.synthesis.v_model_frontier")
print(f"  {CATALOG}.synthesis.v_roi_by_pillar")
print(f"  {CATALOG}.synthesis.v_equity_coverage")


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — FULL TABLE SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

ALL_TABLES = {
    "Evidence Scout":       [TABLE_PAPERS, TABLE_SCORECARD],
    "Market Insights":      [TABLE_AA_BENCHMARKS, TABLE_MARKET_SIGNALS],
    "Navigator":            [TABLE_NAV_RESOURCES, TABLE_NAV_BIBLIO],
    "ROI Research Agent":   [TABLE_ROI_BRIEFS, TABLE_ROI_PARAMS],
    "Knowledge Graph":      [TABLE_GRAPH_NODES, TABLE_GRAPH_EDGES],
    "Synthesis Layer":      [TABLE_SYNTHESIS_BRIEFS, TABLE_SYNTHESIS_PROVENANCE],
}

print("\n" + "=" * 70)
print("  Data & AI Enablement Hub — Databricks Unity Catalog")
print("  Catalog: research_hub")
print("=" * 70)
for tool, tables in ALL_TABLES.items():
    print(f"\n  {tool}")
    for t in tables:
        _, schema, table = t.split(".")
        print(f"    research_hub.{schema}.{table}")
print("\n" + "=" * 70)
print("  Cross-tool views → research_hub.synthesis.v_*")
print("=" * 70)


# COMMAND ----------
# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — RUN ALL INSIGHTS
# Quick health check across all tools
# ══════════════════════════════════════════════════════════════════════════════

evidence_insights()
market_insights()
navigator_insights()
roi_insights()
graph_insights()
synthesis_insights()

print("\n✓ All insight queries complete.")
