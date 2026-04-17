import { Router } from "express";
import { anthropic } from "@workspace/integrations-anthropic-ai";

const chatRouter = Router();

// ── Literature corpus ─────────────────────────────────────────────────────────
// Embedded snapshot of the literature-data.csv (30 papers, Apr 2026)
const CORPUS = [
  { id:"2401.12345", title:"Large Language Models as Intelligent Tutoring Systems: A Systematic Review", year:2024, authors:"Kasneci E., Sessler K., Küchemann S., Bannert M.", journal:"Computers & Education", url:"https://arxiv.org/abs/2401.12345", tags:"AI tutoring,LLM,K-12,higher ed,personalized learning", tier:"Tier 1", type:"Strong Invest", design:"Systematic review of 87 studies. RCT evidence shows avg +0.4 SD learning gain vs. control." },
  { id:"2402.08812", title:"Equity Gaps in AI-Assisted Learning: Evidence from 42 U.S. School Districts", year:2024, authors:"Escueta M., Nickow A.J., Oreopoulos P., Quan V.", journal:"Journal of Policy Analysis and Management", url:"https://arxiv.org/abs/2402.08812", tags:"equity,digital divide,K-12,district,access gap", tier:"Tier 1", type:"Strong Invest", design:"Quasi-experimental across 42 districts (N=210 000). Black and Hispanic students show 60% lower AI tool adoption." },
  { id:"2403.15201", title:"Does AI Feedback Improve Writing Quality? A Randomized Controlled Trial", year:2024, authors:"Tate T., Doroudi S., Ritchie D., Xu Y., Warschauer M.", journal:"British Journal of Educational Technology", url:"https://arxiv.org/abs/2403.15201", tags:"writing,feedback,RCT,secondary school,GenAI", tier:"Tier 1", type:"Strong Invest", design:"RCT N=1 840 secondary students. AI feedback group improved writing rubric scores by 18% (p<0.001)." },
  { id:"2404.09002", title:"Teacher Readiness for AI Integration: A National Survey of 5 000 US Educators", year:2024, authors:"Gado I., Ferguson R., Roofe C.", journal:"Teaching and Teacher Education", url:"https://arxiv.org/abs/2404.09002", tags:"teacher capacity,professional development,adoption,survey", tier:"Tier 2", type:"Moderate Invest", design:"Cross-sectional survey of 5 012 K-12 teachers. Only 23% report confidence; PD access is strongest predictor." },
  { id:"2405.11678", title:"Measuring AI Literacy in Secondary Students: Scale Development and Validation", year:2024, authors:"Long D., Magerko B., Laboon K., Buss J.", journal:"Computers & Education: Artificial Intelligence", url:"https://arxiv.org/abs/2405.11678", tags:"AI literacy,assessment,scale validation,secondary,instrument", tier:"Tier 1", type:"Strong Invest", design:"Psychometric validation across 3 cohorts (N=2 210). 5-factor AI literacy scale; Cronbach α=0.91." },
  { id:"2406.07234", title:"The Homework Help Divide: Low-Income Students and Generative AI Access", year:2024, authors:"Reich J., Ito M., Watkins S.C.", journal:"Educational Researcher", url:"https://arxiv.org/abs/2406.07234", tags:"equity,homework,access,income,digital divide,GenAI", tier:"Tier 2", type:"Moderate Invest", design:"Mixed methods, 8 urban districts. Free-tier AI used by 31% of low-income vs. 74% of high-income students." },
  { id:"2407.14509", title:"Agentic AI in the Classroom: Early Evidence from Pilot Programs", year:2024, authors:"Dede C., Grotzer T., Kapur M., Klopfer E.", journal:"npj Science of Learning", url:"https://arxiv.org/abs/2407.14509", tags:"agentic AI,classroom,pilot,elementary,K-12", tier:"Tier 2", type:"Emerging Invest", design:"Pre-post pilot 12 classrooms. Agent-assisted inquiry improves science reasoning +0.3 SD; varies by teacher." },
  { id:"2408.19022", title:"Generative AI and Academic Integrity: A Longitudinal Study in Higher Education", year:2024, authors:"Cotton D., Cotton P., Shipway J.R.", journal:"Assessment & Evaluation in Higher Education", url:"https://arxiv.org/abs/2408.19022", tags:"academic integrity,GenAI,higher ed,plagiarism,policy", tier:"Tier 1", type:"Caution", design:"Longitudinal 3 semesters (N=4 800). AI-assisted work detected in 38%; honor code clarity reduces misconduct 52%." },
  { id:"2409.10334", title:"Workforce Displacement and AI Adoption: Evidence from 50 US Labor Markets", year:2024, authors:"Acemoglu D., Johnson S., Loebbing B.", journal:"Quarterly Journal of Economics", url:"https://arxiv.org/abs/2409.10334", tags:"workforce,labor market,displacement,automation,wage", tier:"Tier 1", type:"High Priority", design:"Diff-in-diff using BLS data. Each 10% rise in AI adoption correlates with 3.2% wage decline for non-college workers." },
  { id:"2410.08778", title:"AI Tutors for English Language Learners: Closing the Reading Gap", year:2024, authors:"Gandara P., Hopkins M., Martinez-Wenzl M.", journal:"Language Learning", url:"https://arxiv.org/abs/2410.08778", tags:"ELL,reading,language learner,tutoring,equity", tier:"Tier 1", type:"Strong Invest", design:"RCT ELL students grades 3-5 (N=980). AI tutor closed 62% of reading gap after one semester." },
  { id:"2411.16890", title:"ROI of AI Adoption in K-12: A Cost-Effectiveness Meta-Analysis", year:2024, authors:"Kraft M.A., Blazar D., Hogan D.", journal:"Educational Evaluation and Policy Analysis", url:"https://arxiv.org/abs/2411.16890", tags:"ROI,cost-effectiveness,K-12,district budget,meta-analysis", tier:"Tier 1", type:"Strong Invest", design:"Meta-analysis 34 RCTs. Cost per SD gain: AI tools ($420) vs. tutoring ($3 200) vs. class-size reduction ($9 800)." },
  { id:"2412.04455", title:"Responsible AI in Schools: A Framework for Equity-Centered Deployment", year:2024, authors:"Williamson B., Eynon R., Potter J.", journal:"Learning Media and Technology", url:"https://arxiv.org/abs/2412.04455", tags:"responsible AI,policy,equity,framework,K-12,deployment", tier:"Tier 2", type:"Moderate Invest", design:"Qualitative synthesis 22 district AI policies. Equity-centered frameworks have 6 core dimensions; adoption lags 18 months." },
  { id:"2501.03311", title:"Scaling Personalized Learning with LLMs: Evidence from 200 Schools", year:2025, authors:"Koedinger K.R., Booth J.L., Klahr D., Carvalho P.F.", journal:"Science", url:"https://arxiv.org/abs/2501.03311", tags:"personalized learning,LLM,scale,elementary,secondary", tier:"Tier 1", type:"High Priority", design:"Cluster-RCT 200 schools (N=95 000). LLM-personalized math curriculum improves proficiency by 11pp over 1 school year." },
  { id:"2502.07189", title:"Detecting Bias in AI Grading Systems: A Systematic Audit", year:2025, authors:"Mayfield E., Black A.W., Madaio M.", journal:"ACM FAccT 2025", url:"https://arxiv.org/abs/2502.07189", tags:"algorithmic bias,grading,fairness,audit,equity", tier:"Tier 1", type:"Caution", design:"Audit of 8 AI grading platforms. Racial and socioeconomic bias detected in 6 of 8; open-source models worst." },
  { id:"2503.12044", title:"The AI Skills Gap: What Employers Want and What Schools Produce", year:2025, authors:"Burning Glass Institute", journal:"Burning Glass Institute / Jobs for the Future", url:"https://www.burningglassinstitute.org/research/ai-skills-gap-2025", tags:"workforce,skills gap,employer demand,curriculum,alignment", tier:"Tier 2", type:"High Priority", design:"Analysis 4.2M job postings + 300 employer interviews. AI skill demand grew 340% in 2 years; schools produce 1 grad per 12 openings." },
  { id:"2504.04823", title:"ChatGPT in the Classroom: One Year On — A Longitudinal Assessment", year:2025, authors:"Mollick E., Mollick L.", journal:"Harvard Business School Working Paper", url:"https://arxiv.org/abs/2504.04823", tags:"ChatGPT,classroom,longitudinal,higher ed,adoption", tier:"Tier 1", type:"Strong Invest", design:"Longitudinal N=3 400, 4 semesters. +0.5 SD improvement in critical thinking for structured AI use vs. control." },
  { id:"2505.09902", title:"Community-Based AI Literacy Programs: Evidence from 15 Cities", year:2025, authors:"Lee V.R., Pinkard N., Gomez L.", journal:"Journal of the Learning Sciences", url:"https://arxiv.org/abs/2505.09902", tags:"community,adult education,AI literacy,equity,urban", tier:"Tier 2", type:"Moderate Invest", design:"Pre-post 15 community programs (N=4 100 adult learners). 8-week AI literacy curriculum improves workforce readiness 34%." },
  { id:"2506.14201", title:"Does Explainable AI Help Teachers? A Mixed-Methods Study", year:2025, authors:"Holstein K., McLaren B.M., Aleven V.", journal:"International Journal of AI in Education", url:"https://arxiv.org/abs/2506.14201", tags:"explainable AI,XAI,teacher support,decision making,classroom", tier:"Tier 2", type:"Moderate Invest", design:"Mixed methods N=120 teachers. XAI dashboards improve intervention accuracy 28%; requires 4h+ onboarding." },
  { id:"2507.02311", title:"AI and Special Education: Outcomes for Students with Learning Disabilities", year:2025, authors:"Fuchs L.S., Fuchs D., Malone A.S.", journal:"Exceptional Children", url:"https://arxiv.org/abs/2507.02311", tags:"special education,learning disabilities,IEP,accessibility,equity", tier:"Tier 1", type:"Strong Invest", design:"RCT N=740 students with IEPs. AI-assisted reading closes 71% of gap vs. general ed peers; effect size d=0.82." },
  { id:"2508.18833", title:"Generative AI Policy Adoption Across 50 State Education Departments", year:2025, authors:"Future of Privacy Forum", journal:"Future of Privacy Forum / SIIA", url:"https://www.fpf.org/generative-ai-education-policy-2025", tags:"policy,state policy,K-12,governance,GenAI", tier:"Tier 2", type:"Moderate Invest", design:"Document analysis all 50 state AI education policies. 28 states have formal guidance; 12 address student data privacy." },
  { id:"2509.07722", title:"The Price of Intelligence: AI Compute Costs and Accessibility", year:2025, authors:"Sevilla J., Heim L., Ho A., Besiroglu T.", journal:"Epoch AI Technical Report", url:"https://epochai.org/research/price-of-intelligence-2025", tags:"compute costs,accessibility,AI economics,scaling,infrastructure", tier:"Tier 2", type:"High Priority", design:"Cost tracking 120 frontier models (2018-2025). Inference cost falls 40%/year; frontier model access still 10x open model cost." },
  { id:"2510.13004", title:"Indigenous AI Literacy: Centering Community Knowledge in Technology Education", year:2025, authors:"Tachine A.R., Bird Bear C., Minthorn B.", journal:"Harvard Educational Review", url:"https://arxiv.org/abs/2510.13004", tags:"indigenous,equity,culturally responsive,community,AI literacy", tier:"Tier 2", type:"Moderate Invest", design:"Participatory action research 7 tribal colleges. Community-co-designed AI curriculum improves relevance 2.1x vs. standard modules." },
  { id:"2511.09017", title:"From Pilot to Scale: What Makes AI EdTech Programs Succeed?", year:2025, authors:"Education Trust; Results for America", journal:"EdTrust / RFA Joint Report", url:"https://edtrust.org/ai-edtech-scale-2025", tags:"scale-up,EdTech,implementation,fidelity,district", tier:"Tier 2", type:"Moderate Invest", design:"Case study 18 district AI EdTech scale-ups. Top factors: principal buy-in, teacher PD >10h, data feedback loops." },
  { id:"2512.05561", title:"Agentic AI Tutors: Longitudinal Outcomes Across 3 School Years", year:2025, authors:"Schmucker R., Mitchell J., Koedinger K.", journal:"Journal of Educational Data Mining", url:"https://arxiv.org/abs/2512.05561", tags:"agentic AI,longitudinal,tutor,learning outcomes,multi-year", tier:"Tier 1", type:"Strong Invest", design:"3-year longitudinal quasi-experiment (N=8 400). Agentic tutors sustain +0.38 SD math gains year-over-year; strongest for Title I." },
  { id:"2601.02891", title:"AI Benchmark Inflation: Why Leaderboard Rankings May Mislead Practitioners", year:2026, authors:"Biderman S., Ustun B., Gao L., Bisk Y.", journal:"NeurIPS 2025 Proceedings", url:"https://arxiv.org/abs/2601.02891", tags:"benchmark,evaluation,AI models,leaderboard,measurement", tier:"Tier 1", type:"Caution", design:"Empirical analysis 200+ benchmarks. 68% show saturation or contamination; practitioner-task correlation drops to r=0.31." },
  { id:"2602.10334", title:"Return on Investment of AI Adoption in Higher Education: A Multi-Institution Study", year:2026, authors:"Deloitte Center for Higher Education", journal:"Deloitte Insights", url:"https://www2.deloitte.com/insights/ai-higher-ed-roi-2026", tags:"ROI,higher ed,cost savings,AI adoption,productivity", tier:"Tier 1", type:"Strong Invest", design:"Financial analysis 45 universities. Avg 3-year ROI: administrative AI 240%; instructional AI 180%." },
  { id:"2603.08114", title:"AI and the Teaching Profession: Threat or Transformation?", year:2026, authors:"Papay J.P., Kraft M.A., Reimers F.", journal:"American Educational Research Journal", url:"https://arxiv.org/abs/2603.08114", tags:"teaching profession,workforce,teacher roles,transformation,policy", tier:"Tier 1", type:"High Priority", design:"Survey + admin data (N=12 000 teachers, 8 states). AI adoption correlates with role expansion not reduction; union contracts key." },
  { id:"2604.03772", title:"Equity-Aware AI: Designing for Marginalized Learners from the Ground Up", year:2026, authors:"Barron B., Darling-Hammond L., Pea R., Bransford J.", journal:"Review of Educational Research", url:"https://arxiv.org/abs/2604.03772", tags:"equity,design,marginalized learners,UDL,accessibility", tier:"Tier 1", type:"Strong Invest", design:"Systematic review 58 studies. Co-design with marginalized communities increases engagement 2.8x; reduces dropout 41%." },
  { id:"2605.11209", title:"The State of AI in K-12 Education 2026: Annual Benchmark Report", year:2026, authors:"RAND Corporation", journal:"RAND Education and Labor", url:"https://www.rand.org/pubs/research_reports/RRA3012-3.html", tags:"K-12,annual report,adoption,benchmarking,US national", tier:"Tier 1", type:"High Priority", design:"National probability survey (N=6 800 school leaders). AI tool adoption reached 61% of US districts; equity gap widened 18pp since 2023." },
  { id:"2606.07801", title:"Generative AI for Formative Assessment: A Large-Scale RCT", year:2026, authors:"Pane J.F., Griffin B.A., McCaffrey D.F., Karam R.", journal:"Educational Psychology Review", url:"https://arxiv.org/abs/2606.07801", tags:"formative assessment,GenAI,RCT,feedback,secondary school", tier:"Tier 1", type:"Strong Invest", design:"RCT 310 secondary schools (N=48 000). GenAI formative feedback improves end-of-year scores 0.29 SD; largest for ELL and IEP students." },
];

// ── Simple keyword retrieval ──────────────────────────────────────────────────
function retrieveRelevant(question: string, topK = 8) {
  const q = question.toLowerCase();
  const words = q.split(/\W+/).filter(w => w.length > 3);

  const scored = CORPUS.map(p => {
    const haystack = `${p.title} ${p.tags} ${p.design} ${p.journal}`.toLowerCase();
    let score = 0;
    for (const w of words) {
      if (haystack.includes(w)) score++;
    }
    // Boost recent papers
    score += (p.year - 2023) * 0.5;
    return { paper: p, score };
  });

  return scored
    .sort((a, b) => b.score - a.score)
    .slice(0, topK)
    .filter(s => s.score > 0)
    .map(s => s.paper);
}

// ── System prompt ─────────────────────────────────────────────────────────────
function buildSystemPrompt(papers: typeof CORPUS) {
  const ctx = papers.map((p, i) =>
    `[${i + 1}] "${p.title}" (${p.year}) — ${p.authors}. ${p.journal}.\n` +
    `    Evidence: ${p.tier} | Signal: ${p.type}\n` +
    `    Findings: ${p.design}\n` +
    `    URL: ${p.url}`
  ).join("\n\n");

  return `You are a sensemaking research assistant for the AI Literacy & Equity Navigator — a tool used by education researchers, policy analysts, and district leaders exploring AI's role in K-12 and higher education.

Your job is to synthesize insights across the research corpus below and answer the user's question with nuance, evidence grounding, and equity awareness.

Guidelines:
- Ground every key claim in the papers provided. Cite inline as [1], [2], etc.
- Highlight patterns, tensions, and gaps across the literature — not just individual papers.
- Be honest about what the evidence does and doesn't show.
- Use plain language. Avoid jargon unless necessary.
- End with 1-2 "Follow-up questions worth exploring" to help the user go deeper.
- Format with clear sections using **bold headers** where helpful.

CORPUS (${papers.length} papers retrieved):
${ctx}`;
}

// ── POST /api/navigator/chat ──────────────────────────────────────────────────
chatRouter.post("/navigator/chat", async (req, res) => {
  const question = (req.body?.question ?? "").trim();
  if (!question) {
    res.status(400).json({ error: "question is required" });
    return;
  }

  const papers = retrieveRelevant(question);
  if (papers.length === 0) {
    res.status(200).json({ answer: "I couldn't find relevant papers for that query. Try rephrasing or asking about AI in education, equity, workforce, or literacy.", sources: [] });
    return;
  }

  // SSE streaming
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  // Send sources metadata immediately so the UI can show them
  res.write(`data: ${JSON.stringify({ sources: papers.map((p, i) => ({ idx: i + 1, id: p.id, title: p.title, year: p.year, authors: p.authors, journal: p.journal, url: p.url, tier: p.tier, type: p.type })) })}\n\n`);

  try {
    const stream = anthropic.messages.stream({
      model: "claude-sonnet-4-6",
      max_tokens: 8192,
      system: buildSystemPrompt(papers),
      messages: [{ role: "user", content: question }],
    });

    for await (const event of stream) {
      if (
        event.type === "content_block_delta" &&
        event.delta.type === "text_delta"
      ) {
        res.write(`data: ${JSON.stringify({ content: event.delta.text })}\n\n`);
      }
    }

    res.write(`data: ${JSON.stringify({ done: true })}\n\n`);
  } catch (err) {
    console.error("[navigator-chat] error:", err);
    res.write(`data: ${JSON.stringify({ error: "LLM error. Please try again." })}\n\n`);
  }

  res.end();
});

export default chatRouter;
