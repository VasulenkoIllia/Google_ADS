import express from 'express';
import { renderSummaryReport } from '../controllers/summaryReportController.js';
import { renderGoogleAdsReport } from '../controllers/googleAdsReportController.js';
import { renderSalesDriveReport } from '../controllers/salesDriveReportController.js';
import { renderCombinedReport } from '../controllers/combinedReportController.js';

const router = express.Router();

router.get('/summary', renderSummaryReport);
router.get('/google-ads', renderGoogleAdsReport);
router.get('/salesdrive', renderSalesDriveReport);
router.get('/combined', renderCombinedReport);

export default router;
