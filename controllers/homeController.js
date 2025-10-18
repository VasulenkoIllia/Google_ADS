import { SALESDRIVE_ISTOCHNIKI } from '../services/reportDataService.js';

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
    }
];

export function renderHome(req, res) {
    return res.render('home', {
        reports: REPORT_LIST,
        sourcesCount: Array.isArray(SALESDRIVE_ISTOCHNIKI) ? SALESDRIVE_ISTOCHNIKI.length : 0
    });
}
