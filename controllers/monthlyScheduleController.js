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
        ? nextRun.toLocaleString('ru-RU', { dateStyle: 'medium', timeStyle: 'short' })
        : 'Не запланировано';
    const lastRun = config.lastRunAt ? new Date(config.lastRunAt) : null;
    const lastRunLabel = lastRun
        ? lastRun.toLocaleString('ru-RU', { dateStyle: 'medium', timeStyle: 'short' })
        : 'Ещё не запускалось';

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
                message: 'Готовим настройки…'
            })
        });
    } catch (error) {
        console.error('[monthlySchedule] render failed:', error);
        return res.status(500).render('error', {
            message: 'Не удалось отобразить настройки планировщика.',
            source: 'monthlyScheduleController: render',
            error
        });
    }
}

export async function handleMonthlyScheduleUpdate(req, res) {
    try {
        const payload = buildSchedulePayloadFromForm(req.body || {});
        await updateMonthlyScheduleConfig(payload);
        const success = encodeURIComponent('Настройки сохранены.');
        const returnTo = typeof req.body?.returnTo === 'string' && req.body.returnTo.startsWith('/')
            ? req.body.returnTo
            : '/reports/monthly/schedule';
        const separator = returnTo.includes('?') ? '&' : '?';
        return res.redirect(`${returnTo}${separator}success=${success}`);
    } catch (error) {
        console.error('[monthlySchedule] update failed:', error);
        const message = encodeURIComponent(error.message || 'Не удалось сохранить настройки.');
        const returnTo = typeof req.body?.returnTo === 'string' && req.body.returnTo.startsWith('/')
            ? req.body.returnTo
            : '/reports/monthly/schedule';
        const separator = returnTo.includes('?') ? '&' : '?';
        return res.redirect(`${returnTo}${separator}error=${message}`);
    }
}

export async function handleMonthlyScheduleRunNow(req, res) {
    try {
        const result = await runMonthlyScheduleNow();
        const returnTo = typeof req.body?.returnTo === 'string' && req.body.returnTo.startsWith('/')
            ? req.body.returnTo
            : '/reports/monthly/schedule';
        const separator = returnTo.includes('?') ? '&' : '?';
        if (result.status === 'busy') {
            const info = encodeURIComponent('Планировщик уже выполняет обновление.');
            return res.redirect(`${returnTo}${separator}error=${info}`);
        }
        if (result.status === 'error') {
            const message = encodeURIComponent(result.error?.message || 'Ошибка при формировании месячных отчётов.');
            return res.redirect(`${returnTo}${separator}error=${message}`);
        }
        const processedCount = Array.isArray(result.summary?.processed) ? result.summary.processed.length : 0;
        const hasWarnings = Array.isArray(result.summary?.errors) && result.summary.errors.length > 0;
        const success = encodeURIComponent(
            `Запуск завершён (${processedCount} месяцев обновлено${hasWarnings ? ', есть предупреждения' : ''}).`
        );
        return res.redirect(`${returnTo}${separator}success=${success}`);
    } catch (error) {
        console.error('[monthlySchedule] runNow failed:', error);
        const message = encodeURIComponent(error.message || 'Не удалось запустить обновление.');
        const returnTo = typeof req.body?.returnTo === 'string' && req.body.returnTo.startsWith('/')
            ? req.body.returnTo
            : '/reports/monthly/schedule';
        const separator = returnTo.includes('?') ? '&' : '?';
        return res.redirect(`${returnTo}${separator}error=${message}`);
    }
}
