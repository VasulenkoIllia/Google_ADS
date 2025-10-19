import express from 'express';
import { renderHome } from '../controllers/homeController.js';
import {
    renderPinPrompt,
    handlePinVerify,
    handlePinLogout,
    handlePinSettingsUpdate
} from '../controllers/securityController.js';
import { renderSettings } from '../controllers/settingsController.js';

const router = express.Router();

router.get('/', renderHome);
router.get('/settings', renderSettings);
router.get('/auth/pin', renderPinPrompt);
router.post('/auth/pin/verify', handlePinVerify);
router.post('/auth/pin/logout', handlePinLogout);
router.post('/auth/pin/settings', handlePinSettingsUpdate);

export default router;
