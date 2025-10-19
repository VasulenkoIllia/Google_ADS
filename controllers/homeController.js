import { getSalesdriveSources, buildOverlayMeta } from '../services/reportDataService.js';
import { getMonthlyScheduleOverview } from '../services/monthlyScheduleService.js';

const REPORT_LIST = [
    {
        id: 'summary',
        title: 'Зведений звіт',
        description: 'Комбінований огляд показників Google Ads та SalesDrive з деталізацією за джерелами.'
    },
    {
        id: 'google-ads',
        title: 'Звіт Google Ads',
        description: 'Статистика витрат, кліків та показів для рекламних товарів Google Ads.'
    },
    {
        id: 'salesdrive',
        title: 'Звіт SalesDrive CRM',
        description: 'Перегляд замовлень та фінансових показників з SalesDrive за обраний період.'
    },
    {
        id: 'combined',
        title: 'Об’єднаний звіт',
        description: 'Співставлення замовлень SalesDrive з витратами Google Ads для аналізу ефективності.'
    },
    {
        id: 'monthly',
        title: 'Статичний місячний звіт',
        description: 'Автоматично сформовані підсумки по всіх джерелах із можливістю коригування планових показників.'
    }
];

export async function renderHome(req, res, next) {
    try {
        const sources = getSalesdriveSources();
        const sourcesCount = Array.isArray(sources) ? sources.length : 0;
        const scheduleOverview = await getMonthlyScheduleOverview();
        const overlayMeta = buildOverlayMeta({
            extraQueuedRequests: Math.max(sourcesCount - 1, 0),
            remainingSources: sourcesCount,
            message: 'Готуємо дані…'
        });
        return res.render('home', {
            reports: REPORT_LIST,
            sourcesCount,
            scheduleOverview,
            reportOverlayMeta: overlayMeta
        });
    } catch (error) {
        next(error);
    }
}
