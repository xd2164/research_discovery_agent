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
// Snapshot of top models using the current 2026 Artificial Analysis benchmark
// scale. Scores are from live API data (Apr 2026). Used when upstream is
// rate-limited or unavailable.
const FALLBACK_DATA = {
  _source: "fallback",
  data: [
    // ── OpenAI ──────────────────────────────────────────────────────────────
    { id:"gpt-5-4-xhigh",        name:"GPT-5.4 (xhigh)",               model_creator:{name:"OpenAI"},    evaluations:{artificial_analysis_intelligence_index:57.2,artificial_analysis_coding_index:54,artificial_analysis_math_index:58}, pricing:{price_1m_input_tokens:30,  price_1m_output_tokens:120}, median_output_tokens_per_second:25,  median_time_to_first_token_seconds:4.2  },
    { id:"gpt-5-3-codex-xhigh",  name:"GPT-5.3 Codex (xhigh)",         model_creator:{name:"OpenAI"},    evaluations:{artificial_analysis_intelligence_index:54.0,artificial_analysis_coding_index:62,artificial_analysis_math_index:55}, pricing:{price_1m_input_tokens:20,  price_1m_output_tokens:80},  median_output_tokens_per_second:30,  median_time_to_first_token_seconds:3.8  },
    { id:"gpt-5-2-medium",       name:"GPT-5.2 (medium)",               model_creator:{name:"OpenAI"},    evaluations:{artificial_analysis_intelligence_index:46.6,artificial_analysis_coding_index:50,artificial_analysis_math_index:48}, pricing:{price_1m_input_tokens:5,   price_1m_output_tokens:20},  median_output_tokens_per_second:55,  median_time_to_first_token_seconds:2.1  },
    { id:"gpt-5-high",           name:"GPT-5 (high)",                   model_creator:{name:"OpenAI"},    evaluations:{artificial_analysis_intelligence_index:44.6,artificial_analysis_coding_index:48,artificial_analysis_math_index:46}, pricing:{price_1m_input_tokens:3,   price_1m_output_tokens:12},  median_output_tokens_per_second:60,  median_time_to_first_token_seconds:1.8  },
    { id:"gpt-5-4-nano-xhigh",   name:"GPT-5.4 nano (xhigh)",          model_creator:{name:"OpenAI"},    evaluations:{artificial_analysis_intelligence_index:44.4,artificial_analysis_coding_index:46,artificial_analysis_math_index:45}, pricing:{price_1m_input_tokens:0.5, price_1m_output_tokens:2},   median_output_tokens_per_second:120, median_time_to_first_token_seconds:0.6  },
    { id:"gpt-4o",               name:"GPT-4o",                         model_creator:{name:"OpenAI"},    evaluations:{artificial_analysis_intelligence_index:38.0,artificial_analysis_coding_index:40,artificial_analysis_math_index:36}, pricing:{price_1m_input_tokens:2.5, price_1m_output_tokens:10},  median_output_tokens_per_second:90,  median_time_to_first_token_seconds:0.45 },
    { id:"gpt-4o-mini",          name:"GPT-4o mini",                    model_creator:{name:"OpenAI"},    evaluations:{artificial_analysis_intelligence_index:28.0,artificial_analysis_coding_index:30,artificial_analysis_math_index:27}, pricing:{price_1m_input_tokens:0.15,price_1m_output_tokens:0.6}, median_output_tokens_per_second:110, median_time_to_first_token_seconds:0.32 },
    // ── Anthropic ───────────────────────────────────────────────────────────
    { id:"claude-opus-4-6-ar",   name:"Claude Opus 4.6 (Adaptive Reasoning)", model_creator:{name:"Anthropic"}, evaluations:{artificial_analysis_intelligence_index:53.0,artificial_analysis_coding_index:55,artificial_analysis_math_index:56}, pricing:{price_1m_input_tokens:15,  price_1m_output_tokens:75},  median_output_tokens_per_second:20,  median_time_to_first_token_seconds:5.0  },
    { id:"claude-opus-4-6-nr",   name:"Claude Opus 4.6 (Non-reasoning)", model_creator:{name:"Anthropic"}, evaluations:{artificial_analysis_intelligence_index:46.5,artificial_analysis_coding_index:50,artificial_analysis_math_index:47}, pricing:{price_1m_input_tokens:15,  price_1m_output_tokens:75},  median_output_tokens_per_second:55,  median_time_to_first_token_seconds:0.8  },
    { id:"claude-sonnet-4-6-nr", name:"Claude Sonnet 4.6 (Non-reasoning)", model_creator:{name:"Anthropic"}, evaluations:{artificial_analysis_intelligence_index:44.4,artificial_analysis_coding_index:48,artificial_analysis_math_index:45}, pricing:{price_1m_input_tokens:3,   price_1m_output_tokens:15},  median_output_tokens_per_second:80,  median_time_to_first_token_seconds:0.55 },
    { id:"claude-3-7-sonnet",    name:"Claude 3.7 Sonnet",              model_creator:{name:"Anthropic"}, evaluations:{artificial_analysis_intelligence_index:43.0,artificial_analysis_coding_index:46,artificial_analysis_math_index:44}, pricing:{price_1m_input_tokens:3,   price_1m_output_tokens:15},  median_output_tokens_per_second:78,  median_time_to_first_token_seconds:0.6  },
    { id:"claude-3-5-sonnet",    name:"Claude 3.5 Sonnet",              model_creator:{name:"Anthropic"}, evaluations:{artificial_analysis_intelligence_index:37.0,artificial_analysis_coding_index:40,artificial_analysis_math_index:36}, pricing:{price_1m_input_tokens:3,   price_1m_output_tokens:15},  median_output_tokens_per_second:82,  median_time_to_first_token_seconds:0.55 },
    // ── Google ──────────────────────────────────────────────────────────────
    { id:"gemini-3-1-pro",       name:"Gemini 3.1 Pro Preview",         model_creator:{name:"Google"},    evaluations:{artificial_analysis_intelligence_index:57.2,artificial_analysis_coding_index:55,artificial_analysis_math_index:59}, pricing:{price_1m_input_tokens:5,   price_1m_output_tokens:20},  median_output_tokens_per_second:50,  median_time_to_first_token_seconds:1.2  },
    { id:"gemini-3-flash-r",     name:"Gemini 3 Flash Preview (Reasoning)", model_creator:{name:"Google"}, evaluations:{artificial_analysis_intelligence_index:46.4,artificial_analysis_coding_index:48,artificial_analysis_math_index:47}, pricing:{price_1m_input_tokens:0.3, price_1m_output_tokens:1.2}, median_output_tokens_per_second:90,  median_time_to_first_token_seconds:0.8  },
    { id:"gemini-2-0-flash",     name:"Gemini 2.0 Flash",               model_creator:{name:"Google"},    evaluations:{artificial_analysis_intelligence_index:40.0,artificial_analysis_coding_index:42,artificial_analysis_math_index:40}, pricing:{price_1m_input_tokens:0.1, price_1m_output_tokens:0.4}, median_output_tokens_per_second:200, median_time_to_first_token_seconds:0.18 },
    { id:"gemini-2-0-pro",       name:"Gemini 2.0 Pro",                 model_creator:{name:"Google"},    evaluations:{artificial_analysis_intelligence_index:44.0,artificial_analysis_coding_index:46,artificial_analysis_math_index:45}, pricing:{price_1m_input_tokens:1.25,price_1m_output_tokens:5},   median_output_tokens_per_second:65,  median_time_to_first_token_seconds:0.55 },
    // ── Meta (Open-weights) ─────────────────────────────────────────────────
    { id:"muse-spark",           name:"Muse Spark",                     model_creator:{name:"Meta"},      evaluations:{artificial_analysis_intelligence_index:52.1,artificial_analysis_coding_index:47.5,artificial_analysis_math_index:null,gpqa:0.884,hle:0.399,scicode:0.515,ifbench:0.759,lcr:0.697,terminalbench_hard:0.455,tau2:0.915}, pricing:{price_1m_input_tokens:0,price_1m_output_tokens:0}, median_output_tokens_per_second:0, median_time_to_first_token_seconds:0, release_date:"2026-04-08" },
    { id:"llama-3-3-70b",        name:"Llama 3.3 70B",                  model_creator:{name:"Meta"},      evaluations:{artificial_analysis_intelligence_index:35.0,artificial_analysis_coding_index:37,artificial_analysis_math_index:34}, pricing:{price_1m_input_tokens:0.35,price_1m_output_tokens:0.4}, median_output_tokens_per_second:95,  median_time_to_first_token_seconds:0.33 },
    { id:"llama-3-1-405b",       name:"Llama 3.1 405B",                 model_creator:{name:"Meta"},      evaluations:{artificial_analysis_intelligence_index:33.0,artificial_analysis_coding_index:35,artificial_analysis_math_index:32}, pricing:{price_1m_input_tokens:3,   price_1m_output_tokens:3},   median_output_tokens_per_second:40,  median_time_to_first_token_seconds:0.8  },
    // ── xAI ─────────────────────────────────────────────────────────────────
    { id:"grok-3",               name:"Grok 3",                         model_creator:{name:"xAI"},       evaluations:{artificial_analysis_intelligence_index:48.0,artificial_analysis_coding_index:50,artificial_analysis_math_index:49}, pricing:{price_1m_input_tokens:3,   price_1m_output_tokens:15},  median_output_tokens_per_second:55,  median_time_to_first_token_seconds:0.6  },
    { id:"grok-2",               name:"Grok 2",                         model_creator:{name:"xAI"},       evaluations:{artificial_analysis_intelligence_index:38.0,artificial_analysis_coding_index:40,artificial_analysis_math_index:38}, pricing:{price_1m_input_tokens:2,   price_1m_output_tokens:10},  median_output_tokens_per_second:60,  median_time_to_first_token_seconds:0.5  },
    // ── DeepSeek (China, Open-weights) ──────────────────────────────────────
    { id:"deepseek-r1",          name:"DeepSeek R1",                    model_creator:{name:"DeepSeek"},  evaluations:{artificial_analysis_intelligence_index:42.0,artificial_analysis_coding_index:48,artificial_analysis_math_index:50}, pricing:{price_1m_input_tokens:0.55,price_1m_output_tokens:2.19},median_output_tokens_per_second:50,  median_time_to_first_token_seconds:2.5  },
    { id:"deepseek-v3",          name:"DeepSeek V3",                    model_creator:{name:"DeepSeek"},  evaluations:{artificial_analysis_intelligence_index:38.0,artificial_analysis_coding_index:44,artificial_analysis_math_index:45}, pricing:{price_1m_input_tokens:0.27,price_1m_output_tokens:1.1}, median_output_tokens_per_second:80,  median_time_to_first_token_seconds:0.45 },
    // ── Kimi / Moonshot (China) ──────────────────────────────────────────────
    { id:"kimi-k2-5-r",          name:"Kimi K2.5 (Reasoning)",          model_creator:{name:"Kimi"},      evaluations:{artificial_analysis_intelligence_index:46.8,artificial_analysis_coding_index:49,artificial_analysis_math_index:50}, pricing:{price_1m_input_tokens:0.5, price_1m_output_tokens:2},   median_output_tokens_per_second:45,  median_time_to_first_token_seconds:1.5  },
    // ── Z AI / Zhipu (China) ────────────────────────────────────────────────
    { id:"glm-5-turbo",          name:"GLM-5-Turbo",                    model_creator:{name:"Z AI"},      evaluations:{artificial_analysis_intelligence_index:46.8,artificial_analysis_coding_index:48,artificial_analysis_math_index:47}, pricing:{price_1m_input_tokens:0.7, price_1m_output_tokens:1.4}, median_output_tokens_per_second:70,  median_time_to_first_token_seconds:0.5  },
    // ── Qwen / Alibaba (China, Open-weights) ────────────────────────────────
    { id:"qwen3-5-397b-r",       name:"Qwen3.5 397B (Reasoning)",       model_creator:{name:"Alibaba"},   evaluations:{artificial_analysis_intelligence_index:45.0,artificial_analysis_coding_index:48,artificial_analysis_math_index:50}, pricing:{price_1m_input_tokens:0.4, price_1m_output_tokens:1.2}, median_output_tokens_per_second:40,  median_time_to_first_token_seconds:2.0  },
    { id:"qwen2-5-72b",          name:"Qwen 2.5 72B",                   model_creator:{name:"Alibaba"},   evaluations:{artificial_analysis_intelligence_index:36.0,artificial_analysis_coding_index:40,artificial_analysis_math_index:42}, pricing:{price_1m_input_tokens:0.13,price_1m_output_tokens:0.4}, median_output_tokens_per_second:85,  median_time_to_first_token_seconds:0.4  },
    // ── Mistral (Open-weights) ───────────────────────────────────────────────
    { id:"mistral-large-2",      name:"Mistral Large 2",                model_creator:{name:"Mistral AI"},evaluations:{artificial_analysis_intelligence_index:34.0,artificial_analysis_coding_index:37,artificial_analysis_math_index:32}, pricing:{price_1m_input_tokens:2,   price_1m_output_tokens:6},   median_output_tokens_per_second:70,  median_time_to_first_token_seconds:0.4  },
    { id:"mistral-small-3",      name:"Mistral Small 3",                model_creator:{name:"Mistral AI"},evaluations:{artificial_analysis_intelligence_index:26.0,artificial_analysis_coding_index:28,artificial_analysis_math_index:24}, pricing:{price_1m_input_tokens:0.1, price_1m_output_tokens:0.3}, median_output_tokens_per_second:115, median_time_to_first_token_seconds:0.25 },
    // ── Amazon ──────────────────────────────────────────────────────────────
    { id:"nova-pro",             name:"Amazon Nova Pro",                model_creator:{name:"Amazon"},    evaluations:{artificial_analysis_intelligence_index:36.0,artificial_analysis_coding_index:38,artificial_analysis_math_index:34}, pricing:{price_1m_input_tokens:0.8, price_1m_output_tokens:3.2}, median_output_tokens_per_second:100, median_time_to_first_token_seconds:0.38 },
    // ── MiMo / Shanghai AI Lab (China, Open-weights) ────────────────────────
    { id:"mimo-v2-omni",         name:"MiMo-V2-Omni-0327",             model_creator:{name:"Shanghai AI Lab"}, evaluations:{artificial_analysis_intelligence_index:44.9,artificial_analysis_coding_index:50,artificial_analysis_math_index:52}, pricing:{price_1m_input_tokens:0.2,price_1m_output_tokens:0.6}, median_output_tokens_per_second:75,  median_time_to_first_token_seconds:0.45 },
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
