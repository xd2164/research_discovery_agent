import { Router, type IRouter } from "express";
import healthRouter from "./health";
import aaRouter from "./artificialAnalysis";

const router: IRouter = Router();

router.use(healthRouter);
router.use(aaRouter);

export default router;
