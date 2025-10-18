import {
    composeMonthlyReportData,
    ensureDataDirectories,
    rebuildMonthlyFacts,
    updateMonthlyPlans
} from '../services/monthlyReportService.js';
import {
    buildReportJobKey,
    getOrCreateReportJob,
    getRateLimitMeta,
    MIN_PLACEHOLDER_WAIT_SECONDS
} from '../services/reportDataService.js';

function parseYearMonth(inputYear, inputMonth) {
    const current = new Date();
    let year = Number.parseInt(inputYear, 10);
    let month = Number.parseInt(inputMonth, 10);
    if (!Number.isFinite(year) || year < 2000) {
        year = current.getUTCFullYear();
    }
    if (!Number.isFinite(month) || month < 1 || month > 12) {
        month = current.getUTCMonth() + 1;
    }
    return { year, month };
}

export async function renderMonthlyReport(req, res) {
    try {
        await ensureDataDirectories();
        const report = await composeMonthlyReportData();
        const activeMonthKey = typeof req.query?.activeMonth === 'string' ? req.query.activeMonth : null;
        const successMessage = typeof req.query?.success === 'string' && req.query.success.length > 0
            ? req.query.success
            : null;
        const errorMessage = typeof req.query?.error === 'string' && req.query.error.length > 0
            ? req.query.error
            : null;
        return res.render('reports/monthly', {
            months: report.months,
            activeMonthKey,
            successMessage,
            errorMessage
        });
    } catch (error) {
        console.error('[monthlyReport] render failed:', error);
        return res.status(500).render('error', {
            message: 'Не вдалося побудувати місячний звіт.',
            error
        });
    }
}

export async function handleMonthlyRebuild(req, res) {
    try {
        const { year: rawYearBody, month: rawMonthBody } = req.body || {};
        const { year: rawYearQuery, month: rawMonthQuery } = req.query || {};
        const rawYear = rawYearBody ?? rawYearQuery;
        const rawMonth = rawMonthBody ?? rawMonthQuery;
        const { year, month } = parseYearMonth(rawYear, rawMonth);
        await ensureDataDirectories();
        const monthKey = String(month).padStart(2, '0');
        const daysInMonth = new Date(year, month, 0).getDate();
        const startDate = `${year}-${monthKey}-01`;
        const endDate = `${year}-${monthKey}-${String(daysInMonth).padStart(2, '0')}`;
        const reportKey = buildReportJobKey({
            startDate,
            endDate,
            selectedSourceIds: [],
            salesDriveFilter: {},
            salesDriveLimit: null,
            planOverrides: {},
            reportType: `monthly-static-${year}-${monthKey}`
        });

        const rateLimitMeta = getRateLimitMeta();
        const runDate = new Date();

        const job = getOrCreateReportJob(reportKey, (jobRef) => {
            if (typeof jobRef.updateProgress === 'function') {
                jobRef.updateProgress({ message: `Формуємо звіт за ${monthKey}.${year}` });
            }
            return rebuildMonthlyFacts(year, month, { asOf: runDate, reportJob: jobRef });
        });

        if (job.status === 'ready') {
            const successQuery = encodeURIComponent(`Звіт за ${monthKey}.${year} оновлено`);
            return res.redirect(`/reports/monthly?activeMonth=${year}-${monthKey}&success=${successQuery}#month-${year}-${monthKey}`);
        }

        if (job.status === 'error') {
            return res.status(500).render('error', {
                message: job.error?.message || 'Не вдалося примусово сформувати звіт.',
                error: job.error || {}
            });
        }

        const progress = job.progress || {};
        const waitMsCandidate = Number.isFinite(job.waitMs)
            ? job.waitMs
            : rateLimitMeta.minuteIntervalSeconds * 1000;
        const waitSeconds = Math.max(
            Math.ceil(waitMsCandidate / 1000),
            MIN_PLACEHOLDER_WAIT_SECONDS
        );

        res.set('Retry-After', waitSeconds.toString());
        return res.status(202).render('loading', {
            waitSeconds,
            reloadUrl: `/reports/monthly/rebuild?year=${year}&month=${month}`,
            message: progress.message || `Формуємо звіт за ${monthKey}.${year}…`,
            alerts: Array.isArray(progress.alerts) ? progress.alerts : [],
            salesDriveLimit: rateLimitMeta.minuteLimit,
            intervalSeconds: rateLimitMeta.minuteIntervalSeconds,
            hourlyLimit: rateLimitMeta.hourlyLimit,
            dailyLimit: rateLimitMeta.dailyLimit,
            queueInfo: {
                ...progress,
                waitMs: waitMsCandidate,
                waitSeconds
            }
        });
    } catch (error) {
        console.error('[monthlyReport] rebuild failed:', error);
        return res.status(500).render('error', {
            message: 'Не вдалося примусово сформувати звіт.',
            error
        });
    }
}

export async function handleMonthlyPlanUpdate(req, res) {
    try {
        const { year: rawYear, month: rawMonth } = req.body || {};
        const { year, month } = parseYearMonth(rawYear, rawMonth);
        const sourcePlans = {};
        const sourcesPayload = req.body?.sources || {};
        Object.entries(sourcesPayload).forEach(([ident, values]) => {
            if (!values) {
                return;
            }
            const entry = {};
            if (Object.prototype.hasOwnProperty.call(values, 'sales')) {
                const rawSales = values.sales;
                if (rawSales !== '' && rawSales !== undefined && rawSales !== null) {
                    const parsed = Number.parseFloat(rawSales);
                    entry.sales = Number.isFinite(parsed) ? parsed : 0;
                }
            }
            if (Object.prototype.hasOwnProperty.call(values, 'profit')) {
                const rawProfit = values.profit;
                if (rawProfit !== '' && rawProfit !== undefined && rawProfit !== null) {
                    const parsed = Number.parseFloat(rawProfit);
                    entry.profit = Number.isFinite(parsed) ? parsed : 0;
                }
            }
            if (Object.keys(entry).length > 0) {
                sourcePlans[ident] = entry;
            }
        });

        await ensureDataDirectories();
        await updateMonthlyPlans(year, month, {
            sources: sourcePlans
        });

        const monthKey = String(month).padStart(2, '0');
        const successQuery = encodeURIComponent('Плани оновлено');
        return res.redirect(`/reports/monthly?activeMonth=${year}-${monthKey}&success=${successQuery}#month-${year}-${monthKey}`);
    } catch (error) {
        console.error('[monthlyReport] plan update failed:', error);
        const monthKey = String(month).padStart(2, '0');
        const message = encodeURIComponent(error.message || 'Не вдалося оновити плани для місяця.');
        return res.redirect(`/reports/monthly?activeMonth=${year}-${monthKey}&error=${message}#month-${year}-${monthKey}`);
    }
}
