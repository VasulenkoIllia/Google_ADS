import {
    SALESDRIVE_ISTOCHNIKI,
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
    DateRangeError
} from '../services/reportDataService.js';

const REPORT_TYPE = 'combined';

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

export async function renderCombinedReport(req, res) {
    try {
        const { startDate, endDate } = resolveDateRange(req.query.startDate, req.query.endDate);
        const selectedSourceIds = normalizeToArray(req.query.source ?? []);
        const salesDriveFilter = sanitizeSalesDriveFilter(req.query.filter || {});
        const limitOverride = extractLimitOverride(req.query.limit);
        const salesDriveRequestOptions = buildSalesDriveRequestOptions(salesDriveFilter, limitOverride);

        const { planSalesRaw, planProfitRaw } = extractPlanRawValues(req.query);
        const { planOverrides, planInputs, planDefaults } = resolvePlanConfig(planSalesRaw, planProfitRaw);

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

            return res.render('reports/combined', {
                startDate,
                endDate,
                salesDriveIstocniki: SALESDRIVE_ISTOCHNIKI,
                selectedSources: selectedSourceIds,
                salesDriveFilter,
                salesDriveLimit: salesDriveRequestOptions.limit,
                planInputs,
                planDefaults,
                rateLimitMeta,
                combinedData: directResult.combinedData,
                summaryReport: directResult.summaryReport,
                alerts: directResult.alerts,
                rateLimitCooldown: directResult.rateLimitCooldown,
                rateLimitCooldownSeconds: directResult.rateLimitCooldownSeconds,
                hourlyStats: directResult.hourlyStats
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
            return res.render('reports/combined', {
                startDate,
                endDate,
                salesDriveIstocniki: SALESDRIVE_ISTOCHNIKI,
                selectedSources: selectedSourceIds,
                salesDriveFilter,
                salesDriveLimit: salesDriveRequestOptions.limit,
                planInputs,
                planDefaults,
                rateLimitMeta,
                combinedData: job.result.combinedData,
                summaryReport: job.result.summaryReport,
                alerts: job.result.alerts,
                rateLimitCooldown: job.result.rateLimitCooldown,
                rateLimitCooldownSeconds: job.result.rateLimitCooldownSeconds,
                hourlyStats: job.result.hourlyStats
            });
        }

        if (job.status === 'error') {
            return res.status(500).render('error', {
                message: job.error?.message || 'Не вдалося сформувати об’єднаний звіт.',
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
        const reloadUrl = req.originalUrl || req.url || '/reports/combined';

        res.set('Retry-After', waitSeconds.toString());
        return res.status(202).render('loading', {
            waitSeconds,
            reloadUrl,
            message: progress.message || 'Підганяємо дані для об’єднаного звіту…',
            alerts: Array.isArray(progress.alerts) ? progress.alerts : [],
            salesDriveLimit: progress.maxPerInterval ?? rateLimitMeta.minuteLimit,
            intervalSeconds: Math.ceil(
                (progress.intervalMs ?? rateLimitMeta.minuteIntervalSeconds * 1000) / 1000
            ),
            hourlyLimit: rateLimitMeta.hourlyLimit,
            queueInfo: {
                ...progress,
                waitMs: waitMsCandidate,
                waitSeconds
            }
        });
    } catch (error) {
        console.error('[combinedReport] Failed to render report:', error);
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
