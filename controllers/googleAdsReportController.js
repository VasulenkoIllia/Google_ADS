import {
    SALESDRIVE_ISTOCHNIKI,
    resolveDateRange,
    normalizeToArray,
    resolveSourcesForRequest,
    getGoogleAdsData,
    DateRangeError
} from '../services/reportDataService.js';

function mapGoogleAdsDataToRows(dataMap, sourcesToProcess, isFiltering) {
    const allowedIdents = sourcesToProcess.map(source => source.ident);
    const entries = Object.entries(dataMap || {});

    return entries
        .filter(([ident]) => !isFiltering || allowedIdents.includes(ident))
        .map(([ident, payload]) => {
            const costNumber = Number.parseFloat(payload.costUah ?? '0') || 0;
            const impressionsNumber = Number.parseInt(payload.impressions ?? '0', 10) || 0;
            const clicksNumber = Number.parseInt(payload.clicks ?? '0', 10) || 0;
            return {
                istocnikProdaziIdent: ident,
                title: payload.title,
                costUah: costNumber.toFixed(2),
                impressions: impressionsNumber.toLocaleString('uk-UA'),
                clicks: clicksNumber.toLocaleString('uk-UA'),
                rawCost: costNumber,
                rawImpressions: impressionsNumber,
                rawClicks: clicksNumber
            };
        });
}

function calculateTotals(rows) {
    return rows.reduce(
        (acc, row) => {
            acc.totalCostUah += row.rawCost;
            acc.totalImpressions += row.rawImpressions;
            acc.totalClicks += row.rawClicks;
            return acc;
        },
        { totalCostUah: 0, totalImpressions: 0, totalClicks: 0 }
    );
}

export async function renderGoogleAdsReport(req, res) {
    try {
        const { startDate, endDate } = resolveDateRange(req.query.startDate, req.query.endDate);
        const selectedSourceIds = normalizeToArray(req.query.source ?? []);
        const { sourcesToProcess, isFiltering } = resolveSourcesForRequest(selectedSourceIds);

        const googleAdsResult = await getGoogleAdsData({ startDate, endDate });
        const alerts = Array.isArray(googleAdsResult.errors) ? googleAdsResult.errors : [];

        const rows = mapGoogleAdsDataToRows(
            googleAdsResult.data,
            sourcesToProcess,
            isFiltering
        );
        const totals = calculateTotals(rows);

        return res.render('reports/googleAds', {
            startDate,
            endDate,
            salesDriveIstocniki: SALESDRIVE_ISTOCHNIKI,
            selectedSources: selectedSourceIds,
            alerts,
            googleAdsData: rows,
            totals: {
                totalCostUah: totals.totalCostUah.toFixed(2),
                totalImpressions: totals.totalImpressions.toLocaleString('uk-UA'),
                totalClicks: totals.totalClicks.toLocaleString('uk-UA')
            }
        });
    } catch (error) {
        console.error('[googleAdsReport] Failed to render report:', error);
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
