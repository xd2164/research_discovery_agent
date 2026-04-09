import { Router } from "express";

const aaRouter = Router();

const CACHE_TTL_MS = 2 * 60 * 60 * 1000;       // 2 hours
const RATE_LIMIT_BACKOFF_MS = 30 * 60 * 1000;  // 30 min before retrying after 429

let cachedData: unknown = null;
let cacheTimestamp = 0;
let rateLimitedUntil = 0;

// ── Startup cache warm-up ─────────────────────────────────────────────────────
// Pre-fetch live data when the server starts so the first user request is fast
// and avoids falling back to sample data.
async function warmCache() {
  const apiKey = process.env["ARTIFICIAL_ANALYSIS_API_KEY"];
  if (!apiKey) return;
  try {
    console.log("[aa] warming cache on startup…");
    const res = await fetch("https://artificialanalysis.ai/api/v2/data/llms/models", {
      headers: { "x-api-key": apiKey },
    });
    if (res.ok) {
      cachedData = await res.json();
      cacheTimestamp = Date.now();
      console.log("[aa] cache warmed successfully");
    } else if (res.status === 429) {
      rateLimitedUntil = Date.now() + RATE_LIMIT_BACKOFF_MS;
      console.warn(`[aa] warm-up rate-limited; backoff until ${new Date(rateLimitedUntil).toISOString()}`);
    } else {
      console.warn(`[aa] warm-up failed: HTTP ${res.status}`);
    }
  } catch (err) {
    console.warn("[aa] warm-up error:", err);
  }
}
// Run after a 3-second delay so the server is fully up first
setTimeout(warmCache, 3000);

// ── Fallback dataset ─────────────────────────────────────────────────────────
// Representative sample of real models with approximate public benchmark scores.
// Used when upstream API is unavailable (rate-limited or down).
const FALLBACK_DATA = {
  _source: "fallback",
  data: [
    // OpenAI
    { id:"gpt-4o",            name:"GPT-4o",              model_creator:{name:"OpenAI"},      evaluations:{artificial_analysis_intelligence_index:74,artificial_analysis_coding_index:72,artificial_analysis_math_index:68}, pricing:{price_1m_input_tokens:2.5,price_1m_output_tokens:10},  median_output_tokens_per_second:90,  median_time_to_first_token_seconds:0.45 },
    { id:"gpt-4o-mini",       name:"GPT-4o mini",         model_creator:{name:"OpenAI"},      evaluations:{artificial_analysis_intelligence_index:52,artificial_analysis_coding_index:50,artificial_analysis_math_index:48}, pricing:{price_1m_input_tokens:0.15,price_1m_output_tokens:0.6}, median_output_tokens_per_second:110, median_time_to_first_token_seconds:0.32 },
    { id:"o1",                name:"o1",                  model_creator:{name:"OpenAI"},      evaluations:{artificial_analysis_intelligence_index:88,artificial_analysis_coding_index:90,artificial_analysis_math_index:95}, pricing:{price_1m_input_tokens:15,  price_1m_output_tokens:60},  median_output_tokens_per_second:30,  median_time_to_first_token_seconds:3.2  },
    { id:"o1-mini",           name:"o1-mini",             model_creator:{name:"OpenAI"},      evaluations:{artificial_analysis_intelligence_index:76,artificial_analysis_coding_index:79,artificial_analysis_math_index:82}, pricing:{price_1m_input_tokens:3,    price_1m_output_tokens:12},  median_output_tokens_per_second:45,  median_time_to_first_token_seconds:2.1  },
    { id:"o3-mini",           name:"o3-mini",             model_creator:{name:"OpenAI"},      evaluations:{artificial_analysis_intelligence_index:81,artificial_analysis_coding_index:85,artificial_analysis_math_index:90}, pricing:{price_1m_input_tokens:1.1,  price_1m_output_tokens:4.4}, median_output_tokens_per_second:55,  median_time_to_first_token_seconds:1.8  },
    { id:"gpt-4-turbo",       name:"GPT-4 Turbo",         model_creator:{name:"OpenAI"},      evaluations:{artificial_analysis_intelligence_index:68,artificial_analysis_coding_index:67,artificial_analysis_math_index:64}, pricing:{price_1m_input_tokens:10,   price_1m_output_tokens:30},  median_output_tokens_per_second:50,  median_time_to_first_token_seconds:0.6  },
    // Anthropic
    { id:"claude-3.5-sonnet", name:"Claude 3.5 Sonnet",   model_creator:{name:"Anthropic"},   evaluations:{artificial_analysis_intelligence_index:77,artificial_analysis_coding_index:80,artificial_analysis_math_index:72}, pricing:{price_1m_input_tokens:3,    price_1m_output_tokens:15},  median_output_tokens_per_second:82,  median_time_to_first_token_seconds:0.55 },
    { id:"claude-3.5-haiku",  name:"Claude 3.5 Haiku",    model_creator:{name:"Anthropic"},   evaluations:{artificial_analysis_intelligence_index:56,artificial_analysis_coding_index:60,artificial_analysis_math_index:52}, pricing:{price_1m_input_tokens:0.8,  price_1m_output_tokens:4},   median_output_tokens_per_second:120, median_time_to_first_token_seconds:0.28 },
    { id:"claude-3-opus",     name:"Claude 3 Opus",       model_creator:{name:"Anthropic"},   evaluations:{artificial_analysis_intelligence_index:66,artificial_analysis_coding_index:64,artificial_analysis_math_index:60}, pricing:{price_1m_input_tokens:15,   price_1m_output_tokens:75},  median_output_tokens_per_second:30,  median_time_to_first_token_seconds:0.9  },
    { id:"claude-3.7-sonnet", name:"Claude 3.7 Sonnet",   model_creator:{name:"Anthropic"},   evaluations:{artificial_analysis_intelligence_index:84,artificial_analysis_coding_index:87,artificial_analysis_math_index:80}, pricing:{price_1m_input_tokens:3,    price_1m_output_tokens:15},  median_output_tokens_per_second:78,  median_time_to_first_token_seconds:0.6  },
    // Google
    { id:"gemini-1.5-pro",    name:"Gemini 1.5 Pro",      model_creator:{name:"Google"},      evaluations:{artificial_analysis_intelligence_index:69,artificial_analysis_coding_index:67,artificial_analysis_math_index:65}, pricing:{price_1m_input_tokens:1.25, price_1m_output_tokens:5},   median_output_tokens_per_second:75,  median_time_to_first_token_seconds:0.5  },
    { id:"gemini-1.5-flash",  name:"Gemini 1.5 Flash",    model_creator:{name:"Google"},      evaluations:{artificial_analysis_intelligence_index:55,artificial_analysis_coding_index:53,artificial_analysis_math_index:51}, pricing:{price_1m_input_tokens:0.075,price_1m_output_tokens:0.3}, median_output_tokens_per_second:180, median_time_to_first_token_seconds:0.2  },
    { id:"gemini-2.0-flash",  name:"Gemini 2.0 Flash",    model_creator:{name:"Google"},      evaluations:{artificial_analysis_intelligence_index:72,artificial_analysis_coding_index:71,artificial_analysis_math_index:70}, pricing:{price_1m_input_tokens:0.1,  price_1m_output_tokens:0.4}, median_output_tokens_per_second:200, median_time_to_first_token_seconds:0.18 },
    { id:"gemini-2.0-pro",    name:"Gemini 2.0 Pro",      model_creator:{name:"Google"},      evaluations:{artificial_analysis_intelligence_index:80,artificial_analysis_coding_index:79,artificial_analysis_math_index:77}, pricing:{price_1m_input_tokens:1.25, price_1m_output_tokens:5},   median_output_tokens_per_second:65,  median_time_to_first_token_seconds:0.55 },
    // Meta
    { id:"muse-spark",        name:"Muse Spark",           model_creator:{name:"Meta"},        evaluations:{artificial_analysis_intelligence_index:52.1,artificial_analysis_coding_index:47.5,artificial_analysis_math_index:null,gpqa:0.884,hle:0.399,scicode:0.515,ifbench:0.759,lcr:0.697,terminalbench_hard:0.455,tau2:0.915}, pricing:{price_1m_input_tokens:0,price_1m_output_tokens:0},  median_output_tokens_per_second:0, median_time_to_first_token_seconds:0, release_date:"2026-04-08" },
    { id:"llama-3.1-405b",    name:"Llama 3.1 405B",      model_creator:{name:"Meta"},        evaluations:{artificial_analysis_intelligence_index:61,artificial_analysis_coding_index:58,artificial_analysis_math_index:56}, pricing:{price_1m_input_tokens:3,    price_1m_output_tokens:3},   median_output_tokens_per_second:40,  median_time_to_first_token_seconds:0.8  },
    { id:"llama-3.1-70b",     name:"Llama 3.1 70B",       model_creator:{name:"Meta"},        evaluations:{artificial_analysis_intelligence_index:49,artificial_analysis_coding_index:48,artificial_analysis_math_index:45}, pricing:{price_1m_input_tokens:0.35, price_1m_output_tokens:0.4}, median_output_tokens_per_second:90,  median_time_to_first_token_seconds:0.35 },
    { id:"llama-3.3-70b",     name:"Llama 3.3 70B",       model_creator:{name:"Meta"},        evaluations:{artificial_analysis_intelligence_index:57,artificial_analysis_coding_index:56,artificial_analysis_math_index:54}, pricing:{price_1m_input_tokens:0.35, price_1m_output_tokens:0.4}, median_output_tokens_per_second:95,  median_time_to_first_token_seconds:0.33 },
    // xAI
    { id:"grok-2",            name:"Grok 2",              model_creator:{name:"xAI"},         evaluations:{artificial_analysis_intelligence_index:67,artificial_analysis_coding_index:65,artificial_analysis_math_index:62}, pricing:{price_1m_input_tokens:2,    price_1m_output_tokens:10},  median_output_tokens_per_second:60,  median_time_to_first_token_seconds:0.5  },
    { id:"grok-3",            name:"Grok 3",              model_creator:{name:"xAI"},         evaluations:{artificial_analysis_intelligence_index:82,artificial_analysis_coding_index:80,artificial_analysis_math_index:78}, pricing:{price_1m_input_tokens:3,    price_1m_output_tokens:15},  median_output_tokens_per_second:55,  median_time_to_first_token_seconds:0.6  },
    // Mistral AI
    { id:"mistral-large-2",   name:"Mistral Large 2",     model_creator:{name:"Mistral AI"},  evaluations:{artificial_analysis_intelligence_index:60,artificial_analysis_coding_index:62,artificial_analysis_math_index:55}, pricing:{price_1m_input_tokens:2,    price_1m_output_tokens:6},   median_output_tokens_per_second:70,  median_time_to_first_token_seconds:0.4  },
    { id:"mistral-small-3",   name:"Mistral Small 3",     model_creator:{name:"Mistral AI"},  evaluations:{artificial_analysis_intelligence_index:44,artificial_analysis_coding_index:45,artificial_analysis_math_index:40}, pricing:{price_1m_input_tokens:0.1,  price_1m_output_tokens:0.3}, median_output_tokens_per_second:115, median_time_to_first_token_seconds:0.25 },
    // DeepSeek (China)
    { id:"deepseek-v3",       name:"DeepSeek V3",         model_creator:{name:"DeepSeek"},    evaluations:{artificial_analysis_intelligence_index:71,artificial_analysis_coding_index:74,artificial_analysis_math_index:77}, pricing:{price_1m_input_tokens:0.27, price_1m_output_tokens:1.1}, median_output_tokens_per_second:80,  median_time_to_first_token_seconds:0.45 },
    { id:"deepseek-r1",       name:"DeepSeek R1",         model_creator:{name:"DeepSeek"},    evaluations:{artificial_analysis_intelligence_index:79,artificial_analysis_coding_index:82,artificial_analysis_math_index:88}, pricing:{price_1m_input_tokens:0.55, price_1m_output_tokens:2.19},median_output_tokens_per_second:50,  median_time_to_first_token_seconds:2.5  },
    { id:"deepseek-r1-zero",  name:"DeepSeek R1 Zero",    model_creator:{name:"DeepSeek"},    evaluations:{artificial_analysis_intelligence_index:70,artificial_analysis_coding_index:73,artificial_analysis_math_index:82}, pricing:{price_1m_input_tokens:0.55, price_1m_output_tokens:2.19},median_output_tokens_per_second:48,  median_time_to_first_token_seconds:2.8  },
    // Amazon
    { id:"nova-pro",          name:"Amazon Nova Pro",     model_creator:{name:"Amazon"},      evaluations:{artificial_analysis_intelligence_index:58,artificial_analysis_coding_index:56,artificial_analysis_math_index:52}, pricing:{price_1m_input_tokens:0.8,  price_1m_output_tokens:3.2}, median_output_tokens_per_second:100, median_time_to_first_token_seconds:0.38 },
    { id:"nova-micro",        name:"Amazon Nova Micro",   model_creator:{name:"Amazon"},      evaluations:{artificial_analysis_intelligence_index:40,artificial_analysis_coding_index:38,artificial_analysis_math_index:35}, pricing:{price_1m_input_tokens:0.035,price_1m_output_tokens:0.14},median_output_tokens_per_second:200, median_time_to_first_token_seconds:0.15 },
    // Cohere
    { id:"command-r-plus",    name:"Command R+",          model_creator:{name:"Cohere"},      evaluations:{artificial_analysis_intelligence_index:50,artificial_analysis_coding_index:49,artificial_analysis_math_index:44}, pricing:{price_1m_input_tokens:2.5,  price_1m_output_tokens:10},  median_output_tokens_per_second:65,  median_time_to_first_token_seconds:0.5  },
    // MiniMax (China)
    { id:"minimax-01",        name:"MiniMax-01",          model_creator:{name:"MiniMax"},     evaluations:{artificial_analysis_intelligence_index:57,artificial_analysis_coding_index:55,artificial_analysis_math_index:53}, pricing:{price_1m_input_tokens:0.2,  price_1m_output_tokens:1.1}, median_output_tokens_per_second:75,  median_time_to_first_token_seconds:0.5  },
    // Kimi (China)
    { id:"kimi-k1.5",         name:"Kimi k1.5",           model_creator:{name:"Kimi"},        evaluations:{artificial_analysis_intelligence_index:63,artificial_analysis_coding_index:65,artificial_analysis_math_index:70}, pricing:{price_1m_input_tokens:0.5,  price_1m_output_tokens:2},   median_output_tokens_per_second:60,  median_time_to_first_token_seconds:1.2  },
    // Baidu (China)
    { id:"ernie-4.0",         name:"ERNIE 4.0",           model_creator:{name:"Baidu"},       evaluations:{artificial_analysis_intelligence_index:44,artificial_analysis_coding_index:42,artificial_analysis_math_index:40}, pricing:{price_1m_input_tokens:0.5,  price_1m_output_tokens:1.5}, median_output_tokens_per_second:55,  median_time_to_first_token_seconds:0.7  },
    // Samsung (South Korea)
    { id:"samsung-gauss2",    name:"Samsung Gauss 2",     model_creator:{name:"Samsung"},     evaluations:{artificial_analysis_intelligence_index:38,artificial_analysis_coding_index:36,artificial_analysis_math_index:34}, pricing:{price_1m_input_tokens:1,    price_1m_output_tokens:3},   median_output_tokens_per_second:50,  median_time_to_first_token_seconds:0.8  },
    // AI21 Labs (Israel)
    { id:"jamba-1.5-large",   name:"Jamba 1.5 Large",     model_creator:{name:"AI21 Labs"},   evaluations:{artificial_analysis_intelligence_index:53,artificial_analysis_coding_index:51,artificial_analysis_math_index:48}, pricing:{price_1m_input_tokens:2,    price_1m_output_tokens:8},   median_output_tokens_per_second:80,  median_time_to_first_token_seconds:0.45 },
    // Z AI / Zhipu (China)
    { id:"glm-4-plus",        name:"GLM-4 Plus",          model_creator:{name:"Z AI"},        evaluations:{artificial_analysis_intelligence_index:48,artificial_analysis_coding_index:47,artificial_analysis_math_index:46}, pricing:{price_1m_input_tokens:0.7,  price_1m_output_tokens:1.4}, median_output_tokens_per_second:65,  median_time_to_first_token_seconds:0.6  },
    // Qwen / Alibaba (China)
    { id:"qwen-max",          name:"Qwen Max",            model_creator:{name:"Qwen"},        evaluations:{artificial_analysis_intelligence_index:64,artificial_analysis_coding_index:66,artificial_analysis_math_index:69}, pricing:{price_1m_input_tokens:0.4,  price_1m_output_tokens:1.2}, median_output_tokens_per_second:70,  median_time_to_first_token_seconds:0.5  },
    { id:"qwen-2.5-72b",      name:"Qwen 2.5 72B",        model_creator:{name:"Qwen"},        evaluations:{artificial_analysis_intelligence_index:59,artificial_analysis_coding_index:62,artificial_analysis_math_index:65}, pricing:{price_1m_input_tokens:0.13, price_1m_output_tokens:0.4}, median_output_tokens_per_second:85,  median_time_to_first_token_seconds:0.4  },
  ]
};

aaRouter.get("/aa/models", async (_req, res) => {
  const apiKey = process.env["ARTIFICIAL_ANALYSIS_API_KEY"];
  if (!apiKey) {
    res.status(500).json({ error: "ARTIFICIAL_ANALYSIS_API_KEY not configured" });
    return;
  }

  const now = Date.now();
  const cacheAge = now - cacheTimestamp;

  // Serve from cache if fresh
  if (cachedData && cacheAge < CACHE_TTL_MS) {
    res.setHeader("X-Cache", "HIT");
    res.setHeader("X-Cache-Age", String(Math.round(cacheAge / 1000)) + "s");
    res.json(cachedData);
    return;
  }

  // If we're inside a rate-limit backoff window, don't hammer the upstream
  if (now < rateLimitedUntil) {
    const secsLeft = Math.round((rateLimitedUntil - now) / 1000);
    console.log(`[aa] in rate-limit backoff for ${secsLeft}s more`);
    if (cachedData) {
      res.setHeader("X-Cache", "STALE");
      res.json(cachedData);
    } else {
      res.setHeader("X-Cache", "FALLBACK");
      res.json(FALLBACK_DATA);
    }
    return;
  }

  try {
    const upstream = await fetch(
      "https://artificialanalysis.ai/api/v2/data/llms/models",
      { headers: { "x-api-key": apiKey } }
    );
    console.log(`[aa] upstream status: ${upstream.status}`);

    // Rate-limited — serve stale cache, then fallback sample data
    if (upstream.status === 429) {
      rateLimitedUntil = now + RATE_LIMIT_BACKOFF_MS;
      console.warn(`[aa] rate-limited; backing off until ${new Date(rateLimitedUntil).toISOString()}`);
      if (cachedData) {
        res.setHeader("X-Cache", "STALE");
        res.setHeader("X-Cache-Age", String(Math.round(cacheAge / 1000)) + "s");
        res.json(cachedData);
        return;
      }
      // No cache — serve fallback dataset with flag
      res.setHeader("X-Cache", "FALLBACK");
      res.json(FALLBACK_DATA);
      return;
    }

    if (!upstream.ok) {
      console.warn(`[aa] upstream error: ${upstream.status}`);
      if (cachedData) {
        res.setHeader("X-Cache", "STALE");
        res.json(cachedData);
        return;
      }
      res.setHeader("X-Cache", "FALLBACK");
      res.json(FALLBACK_DATA);
      return;
    }

    const data = await upstream.json();
    cachedData = data;
    cacheTimestamp = now;
    console.log("[aa] live data fetched and cached");
    res.setHeader("X-Cache", "MISS");
    res.json(data);
  } catch (err) {
    if (cachedData) {
      res.setHeader("X-Cache", "STALE");
      res.json(cachedData);
      return;
    }
    res.setHeader("X-Cache", "FALLBACK");
    res.json(FALLBACK_DATA);
  }
});

export default aaRouter;
