import express from 'express';
import { renderSummaryReport } from '../controllers/summaryReportController.js';
import { renderGoogleAdsReport } from '../controllers/googleAdsReportController.js';
import { renderSalesDriveReport } from '../controllers/salesDriveReportController.js';
import { renderCombinedReport } from '../controllers/combinedReportController.js';
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
router.get('/google-ads', renderGoogleAdsReport);
router.get('/salesdrive', renderSalesDriveReport);
router.get('/combined', renderCombinedReport);

export default router;
