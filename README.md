# Research & Discovery — Data & AI Enablement Hub

A self-contained research synthesis platform built for education policy, workforce, and AI investment decisions. Combines live market signals with academic literature to produce structured, evidence-graded briefs.

## Tools & Capabilities

| Tool | Description |
|---|---|
| **Research & Discovery** | Synthesis layer combining market signals and research literature into structured briefs |
| **EDUAgent** | Agentic evidence synthesis across RCTs, meta-analyses, and grantee reports |
| **Market Insights Deepdive** | Live AI model benchmarks and hyperscaler signals via Artificial Analysis API |
| **Evidence Scout** | Citation chain traversal and evidence gap mapping |
| **AI Literacy & Equity Navigator** | Equity-anchored resource hub with sensemaking chat over live literature |
| **ROI Research Agent Demo** | NSF logic-model causal chain with evidence-graded ROI parameters |

## NSF Logic-Model Framework

The ROI Research Agent structures evidence across five stages:

| Stage | Categories |
|---|---|
| **Activities** | Basic Research · Use-Inspired Research · Human Capital Development · Research Infrastructure |
| **Outputs** | Publications & Patents · Applied Tools & Products · Degrees & Training · Facilities & Platforms |
| **Outcomes** | New Methods & Processes · New Products & Solutions · Workforce Development · Research Capacity & Capability |
| **Impacts** | Revenue · New firms · New industries · Productivity growth · Employment |

## Stack

- **Frontend**: Vite + vanilla HTML/JS (research-discovery), served as static artifact
- **Backend**: Express + TypeScript API server with PostgreSQL
- **AI**: Anthropic Claude via Replit AI Integrations proxy
- **Data**: Artificial Analysis API (live model benchmarks), Semantic Scholar / OpenAlex (literature)
- **Package manager**: pnpm workspaces (monorepo)

## Project Structure

```
artifacts/
  api-server/          Express API — literature search, navigator searches, ROI extraction
  research-discovery/  Frontend hub — all tools rendered as a single HTML dashboard
  mockup-sandbox/      Component preview server (canvas prototyping)
```

## Getting Started

```bash
pnpm install
pnpm --filter @workspace/api-server run dev
pnpm --filter @workspace/research-discovery run dev
```

Requires environment variables:
- `DATABASE_URL` — PostgreSQL connection string
- `ARTIFICIAL_ANALYSIS_API_KEY` — for Market Insights live data
- `SESSION_SECRET` — for API session management
