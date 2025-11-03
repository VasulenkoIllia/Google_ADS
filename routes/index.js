import express from 'express';
import { renderHome } from '../controllers/homeController.js';
import {
    renderPinPrompt,
    handlePinVerify,
    handlePinLogout,
    handlePinSettingsUpdate
} from '../controllers/securityController.js';
import { renderSettings } from '../controllers/settingsController.js';
import {
    beginGoogleAuth,
    handleGoogleAuthCallback,
    handleGoogleAuthDisconnect
} from '../controllers/googleAuthController.js';

const router = express.Router();

router.get('/', renderHome);
router.get('/settings', renderSettings);
router.get('/auth/pin', renderPinPrompt);
router.post('/auth/pin/verify', handlePinVerify);
router.post('/auth/pin/logout', handlePinLogout);
router.post('/auth/pin/settings', handlePinSettingsUpdate);
router.get('/auth/google', beginGoogleAuth);
router.get('/auth/google/callback', handleGoogleAuthCallback);
router.post('/auth/google/disconnect', handleGoogleAuthDisconnect);

export default router;
