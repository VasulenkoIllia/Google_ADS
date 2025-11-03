import { loadSalesdriveSources } from '../services/salesdriveSourcesService.js';
import { getMonthlyScheduleOverview } from '../services/monthlyScheduleService.js';
import { loadSecurityConfig } from '../services/securityConfigService.js';
import { buildOverlayMeta } from '../services/reportDataService.js';
import { getGoogleAuthStatus } from '../services/googleAuthService.js';

export async function renderSettings(req, res, next) {
    try {
        const [sources, scheduleOverview, securityConfig, googleAuthStatus] = await Promise.all([
            loadSalesdriveSources(),
            getMonthlyScheduleOverview(),
            loadSecurityConfig(),
            getGoogleAuthStatus()
        ]);

        const { success = '', error = '', tab = 'sources' } = req.query || {};
        const overlayMeta = buildOverlayMeta({
            message: 'Готовим настройки…'
        });

        return res.render('settings/index', {
            activeTab: typeof tab === 'string' ? tab : 'sources',
            sources,
            scheduleOverview,
            securityConfig,
            successMessage: typeof success === 'string' && success.length > 0 ? success : null,
            errorMessage: typeof error === 'string' && error.length > 0 ? error : null,
            reportOverlayMeta: overlayMeta,
            googleAuthStatus
        });
    } catch (error) {
        next(error);
    }
}
