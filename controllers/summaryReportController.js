import {
    MIN_PLACEHOLDER_WAIT_SECONDS,
    resolveDateRange,
    normalizeToArray,
    sanitizeSalesDriveFilter,
    resolvePlanConfig,
    getRateLimitMeta,
    shouldProcessDirectly,
    buildReportJobKey,
    getOrCreateReportJob,
    buildReportData,
    DateRangeError,
    getSalesdriveSources
} from '../services/reportDataService.js';

const REPORT_TYPE = 'summary';

function extractLimitOverride(rawLimit) {
    if (rawLimit === undefined) {
        return null;
    }
    const lastValue = Array.isArray(rawLimit) ? rawLimit[rawLimit.length - 1] : rawLimit;
    const parsed = Number.parseInt(lastValue, 10);
    return Number.isFinite(parsed) ? parsed : null;
}

function extractPlanRawValues(query) {
    const planSalesRaw = Array.isArray(query.planSales)
        ? query.planSales[query.planSales.length - 1]
        : query.planSales;
    const planProfitRaw = Array.isArray(query.planProfit)
        ? query.planProfit[query.planProfit.length - 1]
        : query.planProfit;
    return { planSalesRaw, planProfitRaw };
}

function buildSalesDriveRequestOptions(filter, limit) {
    const options = {};
    if (filter && Object.keys(filter).length > 0) {
        options.filter = filter;
    }
    if (limit !== null) {
        options.limit = limit;
    }
    return options;
}

export async function renderSummaryReport(req, res) {
    try {
        const { startDate, endDate } = resolveDateRange(req.query.startDate, req.query.endDate);
        const selectedSourceIds = normalizeToArray(req.query.source ?? []);
        const salesDriveFilter = sanitizeSalesDriveFilter(req.query.filter || {});
        const limitOverride = extractLimitOverride(req.query.limit);
        const salesDriveRequestOptions = buildSalesDriveRequestOptions(salesDriveFilter, limitOverride);

        const { planSalesRaw, planProfitRaw } = extractPlanRawValues(req.query);
        const { planOverrides, planInputs, planDefaults } = resolvePlanConfig(planSalesRaw, planProfitRaw);
        const salesDriveSources = getSalesdriveSources();

        const rateLimitMeta = getRateLimitMeta();
        const directDecision = shouldProcessDirectly(selectedSourceIds);

        if (directDecision.canProcessDirect) {
            const directResult = await buildReportData(
                {
                    startDate,
                    endDate,
                    selectedSourceIds,
                    salesDriveRequestOptions,
                    planOverrides
                },
                { reportJob: null }
            );

            return res.render('reports/summary', {
                startDate,
                endDate,
                salesDriveIstocniki: salesDriveSources,
                selectedSources: selectedSourceIds,
                salesDriveFilter,
                salesDriveLimit: salesDriveRequestOptions.limit,
                planInputs,
                planDefaults,
                rateLimitMeta,
                ...directResult
            });
        }

        const reportKey = buildReportJobKey({
            startDate,
            endDate,
            selectedSourceIds,
            salesDriveFilter,
            salesDriveLimit: salesDriveRequestOptions.limit,
            planOverrides,
            reportType: REPORT_TYPE
        });

        const job = getOrCreateReportJob(reportKey, (jobRef) =>
            buildReportData(
                {
                    startDate,
                    endDate,
                    selectedSourceIds,
                    salesDriveRequestOptions,
                    planOverrides
                },
                { reportJob: jobRef }
            )
        );

        if (job.status === 'ready' && job.result) {
            return res.render('reports/summary', {
                startDate,
                endDate,
                salesDriveIstocniki: salesDriveSources,
                selectedSources: selectedSourceIds,
                salesDriveFilter,
                salesDriveLimit: salesDriveRequestOptions.limit,
                planInputs,
                planDefaults,
                rateLimitMeta,
                ...job.result
            });
        }

        if (job.status === 'error') {
            return res.status(500).render('error', {
                message: job.error?.message || 'Не вдалося сформувати звіт.',
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
        const reloadUrl = req.originalUrl || req.url || '/reports/summary';

        res.set('Retry-After', waitSeconds.toString());
        return res.status(202).render('loading', {
            waitSeconds,
            reloadUrl,
            message: progress.message || 'Формуємо звіт…',
            alerts: Array.isArray(progress.alerts) ? progress.alerts : [],
            salesDriveLimit: progress.maxPerInterval ?? rateLimitMeta.minuteLimit,
            intervalSeconds: Math.ceil(
                (progress.intervalMs ?? rateLimitMeta.minuteIntervalSeconds * 1000) / 1000
            ),
            hourlyLimit: rateLimitMeta.hourlyLimit,
            dailyLimit: rateLimitMeta.dailyLimit,
            queueInfo: {
                ...progress,
                waitMs: waitMsCandidate,
                waitSeconds
            }
        });
    } catch (error) {
        console.error('[summaryReport] Failed to render report:', error);
        if (error instanceof DateRangeError || error.statusCode === 400) {
            const includeStack = (process.env.NODE_ENV || '').toLowerCase() !== 'production';
            return res.status(400).render('error', {
                message: error.message,
                error: includeStack ? error : {}
            });
        }
        return res.status(500).render('error', {
            message: 'Internal Server Error',
            error
        });
    }
}
