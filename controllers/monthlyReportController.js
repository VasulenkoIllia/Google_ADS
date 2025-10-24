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
    MIN_PLACEHOLDER_WAIT_SECONDS,
    buildOverlayMeta
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
        const latestMonth = Array.isArray(report.months) && report.months.length > 0
            ? report.months[report.months.length - 1]
            : null;
        const sourcesCount = Array.isArray(latestMonth?.sources) ? latestMonth.sources.length : 0;
        const overlayMeta = buildOverlayMeta({
            extraQueuedRequests: Math.max(sourcesCount - 1, 0),
            remainingSources: sourcesCount,
            message: 'Готовим статический месячный отчёт…'
        });
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
            errorMessage,
            reportOverlayMeta: overlayMeta
        });
    } catch (error) {
        console.error('[monthlyReport] render failed:', error);
        return res.status(500).render('error', {
            message: 'Не удалось построить месячный отчёт.',
            source: 'monthlyReportController: отображение отчёта',
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
                jobRef.updateProgress({ message: `Формируем отчёт за ${monthKey}.${year}` });
            }
            return rebuildMonthlyFacts(year, month, { asOf: runDate, reportJob: jobRef });
        });

        if (job.status === 'ready') {
            const successQuery = encodeURIComponent(`Отчёт за ${monthKey}.${year} обновлён`);
            return res.redirect(`/reports/monthly?activeMonth=${year}-${monthKey}&success=${successQuery}#month-${year}-${monthKey}`);
        }

        if (job.status === 'error') {
            return res.status(500).render('error', {
                message: job.error?.message || 'Не удалось пересобрать отчёт.',
                source: 'monthlyReportController: принудительное формирование',
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
            message: progress.message || `Формируем отчёт за ${monthKey}.${year}…`,
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
            message: 'Не удалось пересобрать отчёт.',
            source: 'monthlyReportController: rebuild handler',
            error
        });
    }
}

export async function handleMonthlyPlanUpdate(req, res) {
    let parsedContext = { year: null, month: null };
    try {
        const { year: rawYear, month: rawMonth } = req.body || {};
        parsedContext = parseYearMonth(rawYear, rawMonth);
        const { year, month } = parsedContext;
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
        const successQuery = encodeURIComponent('Планы сохранены');
        return res.redirect(`/reports/monthly?activeMonth=${year}-${monthKey}&success=${successQuery}#month-${year}-${monthKey}`);
    } catch (error) {
        console.error('[monthlyReport] plan update failed:', error);
        let fallbackYear = parsedContext.year;
        let fallbackMonth = parsedContext.month;
        if (!Number.isFinite(fallbackYear) || !Number.isFinite(fallbackMonth)) {
            try {
                const parsed = parseYearMonth(req.body?.year, req.body?.month);
                fallbackYear = parsed.year;
                fallbackMonth = parsed.month;
            } catch (parseError) {
                console.error('[monthlyReport] fallback parse failed:', parseError);
                fallbackYear = new Date().getUTCFullYear();
                fallbackMonth = new Date().getUTCMonth() + 1;
            }
        }
        const monthKey = String(fallbackMonth).padStart(2, '0');
        const message = encodeURIComponent(error.message || 'Не удалось обновить планы для месяца.');
        return res.redirect(`/reports/monthly?activeMonth=${fallbackYear}-${monthKey}&error=${message}#month-${fallbackYear}-${monthKey}`);
    }
}
