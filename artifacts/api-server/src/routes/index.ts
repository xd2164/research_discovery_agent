import { Router, type IRouter } from "express";
import healthRouter from "./health";
import aaRouter from "./artificialAnalysis";
import claudeRouter from "./claude";
import arxivRouter from "./arxiv";
import papersRouter from "./papers";

const router: IRouter = Router();

router.use(healthRouter);
router.use(aaRouter);
router.use(claudeRouter);
router.use(arxivRouter);
router.use(papersRouter);

export default router;
