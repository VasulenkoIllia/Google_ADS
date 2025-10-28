import { getSalesdriveSources } from '../services/reportDataService.js';
import { getMonthlyScheduleOverview } from '../services/monthlyScheduleService.js';
import { loadSecurityConfig } from '../services/securityConfigService.js';

const REPORT_LIST = [
    {
        id: 'summary',
        title: 'Сводный отчёт',
        description: 'Динамический обзор показателей SalesDrive и Google Ads с ключевыми KPI за выбранный период.'
    },
    {
        id: 'monthly',
        title: 'Статический месячный отчёт',
        description: 'Сохранённые итоги по источникам с возможностью вручную обновлять планы и фактические показатели.'
    }
];

export async function renderHome(req, res, next) {
    try {
        const sources = getSalesdriveSources();
        const sourcesCount = Array.isArray(sources) ? sources.length : 0;
        const scheduleOverview = await getMonthlyScheduleOverview();
        const securityConfig = await loadSecurityConfig();
        const { success = '', error = '' } = req.query || {};
        const scheduleConfig = scheduleOverview?.config || {};
        const scheduleEnabled = Boolean(scheduleConfig.enabled);
        const scheduleInfo = {
            enabled: scheduleEnabled,
            statusLabel: scheduleEnabled ? 'Включено' : 'Выключено',
            frequencyLabel: scheduleConfig.frequency === 'weekly' ? 'Раз в неделю' : 'Ежедневно',
            time: scheduleConfig.time || '—',
            rangeLabel: (() => {
                const range = scheduleConfig.range || {};
                if (range.type === 'all') {
                    return 'Все доступные месяцы';
                }
                if (range.type === 'last') {
                    return `Последние ${range.count || 1} мес.`;
                }
                return 'Только текущий месяц';
            })(),
            nextRunLabel: scheduleOverview?.nextRunAt
                ? new Date(scheduleOverview.nextRunAt).toLocaleString('ru-RU', { dateStyle: 'medium', timeStyle: 'short' })
                : 'Не запланировано'
        };
        const accessEnabled = Boolean(securityConfig.pinEnabled && securityConfig.pinSalt && securityConfig.pinHash);
        const accessInfo = {
            enabled: accessEnabled,
            statusLabel: accessEnabled ? 'Включено' : 'Выключено',
            username: typeof securityConfig.username === 'string' && securityConfig.username.length > 0
                ? securityConfig.username
                : 'gouads'
        };
        return res.render('home', {
            reports: REPORT_LIST,
            sourcesCount,
            scheduleInfo,
            accessInfo,
            scheduleEnabled,
            successMessage: typeof success === 'string' && success.length > 0 ? success : null,
            errorMessage: typeof error === 'string' && error.length > 0 ? error : null
        });
    } catch (error) {
        next(error);
    }
}
