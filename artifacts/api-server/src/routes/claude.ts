import { Router } from "express";
import Anthropic from "@anthropic-ai/sdk";

const claudeRouter = Router();

const anthropic = new Anthropic({
  baseURL: process.env.AI_INTEGRATIONS_ANTHROPIC_BASE_URL,
  apiKey: process.env.AI_INTEGRATIONS_ANTHROPIC_API_KEY,
});

claudeRouter.post("/claude", async (req, res) => {
  const body = req.body as { prompt?: string; messages?: { role: string; content: string }[] };

  let messages: Anthropic.MessageParam[];
  if (body.messages && Array.isArray(body.messages)) {
    messages = body.messages as Anthropic.MessageParam[];
  } else if (body.prompt) {
    messages = [{ role: "user", content: body.prompt }];
  } else {
    res.status(400).json({ error: "prompt or messages is required" });
    return;
  }

  try {
    const message = await anthropic.messages.create({
      model: "claude-sonnet-4-6",
      max_tokens: 8192,
      messages,
    });
    res.json({ content: message.content });
  } catch (err) {
    console.error("Claude API error:", err);
    res.status(500).json({ error: "Claude request failed" });
  }
});

export default claudeRouter;
