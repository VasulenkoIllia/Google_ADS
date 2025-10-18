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

const router = express.Router();

router.get('/monthly', renderMonthlyReport);
router.post('/monthly/rebuild', handleMonthlyRebuild);
router.post('/monthly/plans', handleMonthlyPlanUpdate);
router.get('/summary', renderSummaryReport);
router.get('/google-ads', renderGoogleAdsReport);
router.get('/salesdrive', renderSalesDriveReport);
router.get('/combined', renderCombinedReport);

export default router;
