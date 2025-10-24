import express from 'express';
import { renderSummaryReport } from '../controllers/summaryReportController.js';
import {
    renderMonthlyReport,
    handleMonthlyRebuild,
    handleMonthlyPlanUpdate
} from '../controllers/monthlyReportController.js';
import {
    renderMonthlyScheduleConfig,
    handleMonthlyScheduleUpdate,
    handleMonthlyScheduleRunNow
} from '../controllers/monthlyScheduleController.js';
import {
    renderSourcesConfig,
    handleSourceAdd,
    handleSourceUpdate,
    handleSourceDelete
} from '../controllers/sourcesController.js';

const router = express.Router();

router.get('/monthly', renderMonthlyReport);
router.get('/monthly/schedule', renderMonthlyScheduleConfig);
router.post('/monthly/schedule', handleMonthlyScheduleUpdate);
router.post('/monthly/schedule/run', handleMonthlyScheduleRunNow);
router.get('/monthly/rebuild', handleMonthlyRebuild);
router.post('/monthly/rebuild', handleMonthlyRebuild);
router.post('/monthly/plans', handleMonthlyPlanUpdate);
router.get('/config/sources', renderSourcesConfig);
router.post('/config/sources/add', handleSourceAdd);
router.post('/config/sources/:id/update', handleSourceUpdate);
router.post('/config/sources/:id/delete', handleSourceDelete);
router.get('/summary', renderSummaryReport);

export default router;
