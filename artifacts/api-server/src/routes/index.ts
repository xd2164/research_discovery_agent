import { Router, type IRouter } from "express";
import healthRouter from "./health";
import aaRouter from "./artificialAnalysis";
import claudeRouter from "./claude";
import arxivRouter from "./arxiv";
import papersRouter from "./papers";
import historyRouter from "./history";
import trackRouter from "./track";
import resourcesRouter from "./resources";
import navigatorChatRouter from "./navigatorChat";

const router: IRouter = Router();

router.use(healthRouter);
router.use(aaRouter);
router.use(claudeRouter);
router.use(arxivRouter);
router.use(papersRouter);
router.use(historyRouter);
router.use(trackRouter);
router.use(resourcesRouter);
router.use(navigatorChatRouter);

export default router;
