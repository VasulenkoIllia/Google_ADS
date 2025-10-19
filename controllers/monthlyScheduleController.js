import {
    updateMonthlyScheduleConfig,
    getMonthlyScheduleOverview,
    runMonthlyScheduleNow,
    buildSchedulePayloadFromForm
} from '../services/monthlyScheduleService.js';
import { buildOverlayMeta } from '../services/reportDataService.js';

function buildScheduleViewModel(config, nextRunAt, isRunning) {
    const nextRun = nextRunAt ? new Date(nextRunAt) : null;
    const nextRunLabel = nextRun
        ? nextRun.toLocaleString('uk-UA', { dateStyle: 'medium', timeStyle: 'short' })
        : 'Не заплановано';
    const lastRun = config.lastRunAt ? new Date(config.lastRunAt) : null;
    const lastRunLabel = lastRun
        ? lastRun.toLocaleString('uk-UA', { dateStyle: 'medium', timeStyle: 'short' })
        : 'Ще не запускалось';

    return {
        config,
        nextRunAt: nextRunLabel,
        lastRunAt: lastRunLabel,
        isRunning,
        lastRunStatus: config.lastRunStatus,
        lastRunSummary: config.lastRunSummary,
        lastRunError: config.lastRunError
    };
}

export async function renderMonthlyScheduleConfig(req, res) {
    try {
        const overview = await getMonthlyScheduleOverview();
        const viewModel = buildScheduleViewModel(overview.config, overview.nextRunAt, overview.isRunning);
        const { success = '', error = '' } = req.query || {};

        return res.render('reports/monthlySchedule', {
            ...viewModel,
            successMessage: typeof success === 'string' && success.length > 0 ? success : null,
            errorMessage: typeof error === 'string' && error.length > 0 ? error : null,
            reportOverlayMeta: buildOverlayMeta({
                waitMs: null,
                queueAhead: null,
                estimatedTotalRequests: null,
                message: 'Готуємо налаштування…'
            })
        });
    } catch (error) {
        console.error('[monthlySchedule] render failed:', error);
        return res.status(500).render('error', {
            message: 'Не вдалося відобразити налаштування планувальника.',
            error
        });
    }
}

export async function handleMonthlyScheduleUpdate(req, res) {
    try {
        const payload = buildSchedulePayloadFromForm(req.body || {});
        await updateMonthlyScheduleConfig(payload);
        const success = encodeURIComponent('Налаштування збережено.');
        return res.redirect(`/reports/monthly/schedule?success=${success}`);
    } catch (error) {
        console.error('[monthlySchedule] update failed:', error);
        const message = encodeURIComponent(error.message || 'Не вдалося зберегти налаштування.');
        return res.redirect(`/reports/monthly/schedule?error=${message}`);
    }
}

export async function handleMonthlyScheduleRunNow(req, res) {
    try {
        const result = await runMonthlyScheduleNow();
        if (result.status === 'busy') {
            const info = encodeURIComponent('Планувальник вже виконує оновлення.');
            return res.redirect(`/reports/monthly/schedule?error=${info}`);
        }
        if (result.status === 'error') {
            const message = encodeURIComponent(result.error?.message || 'Помилка під час формування місячних звітів.');
            return res.redirect(`/reports/monthly/schedule?error=${message}`);
        }
        const processedCount = Array.isArray(result.summary?.processed) ? result.summary.processed.length : 0;
        const hasWarnings = Array.isArray(result.summary?.errors) && result.summary.errors.length > 0;
        const success = encodeURIComponent(
            `Запуск завершено (${processedCount} місяців оновлено${hasWarnings ? ', є попередження' : ''}).`
        );
        return res.redirect(`/reports/monthly/schedule?success=${success}`);
    } catch (error) {
        console.error('[monthlySchedule] runNow failed:', error);
        const message = encodeURIComponent(error.message || 'Не вдалося запустити оновлення.');
        return res.redirect(`/reports/monthly/schedule?error=${message}`);
    }
}
