import { OAuth2Client } from 'google-auth-library';
import axios from "axios";
import dotenv from 'dotenv';

dotenv.config();

// --- CONFIG FROM .env ---
const {
    GLOBAL_DATE_START_DAY: DEFAULT_DATE_START_DAY,
    GLOBAL_DATE_END_DAY: DEFAULT_DATE_END_DAY,
    SALESDRIVE_URL,
    SALESDRIVE_API_KEY,
    CUSTOMER_ID,
    LOGIN_CUSTOMER_ID,
    DEVELOPER_TOKEN,
    CLIENT_ID,
    CLIENT_SECRET,
    REFRESH_TOKEN,
    PLAN_SALES_MONTH,
    PLAN_PROFIT_MONTH
} = process.env;

const SALESDRIVE_ISTOCHNIKI = JSON.parse(process.env.SALESDRIVE_ISTOCHNIKI || '[]');

const DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;
const ZERO_EPSILON = 1e-6;

class DateRangeError extends Error {
    constructor(message) {
        super(message);
        this.name = 'DateRangeError';
        this.statusCode = 400;
    }
}

function coerceQueryParam(value) {
    if (Array.isArray(value)) {
        return value[0];
    }
    return value;
}

function resolveDateRange(requestedStart, requestedEnd) {
    if (!DEFAULT_DATE_START_DAY || !DEFAULT_DATE_END_DAY) {
        throw new Error('GLOBAL_DATE_START_DAY and GLOBAL_DATE_END_DAY must be set in the environment.');
    }

    const normalizedStart = coerceQueryParam(requestedStart);
    const normalizedEnd = coerceQueryParam(requestedEnd);

    const startDate = normalizedStart && normalizedStart.trim() ? normalizedStart.trim() : DEFAULT_DATE_START_DAY;
    const endDate = normalizedEnd && normalizedEnd.trim() ? normalizedEnd.trim() : DEFAULT_DATE_END_DAY;

    if (!DATE_REGEX.test(startDate)) {
        throw new DateRangeError('startDate must be provided in YYYY-MM-DD format.');
    }

    if (!DATE_REGEX.test(endDate)) {
        throw new DateRangeError('endDate must be provided in YYYY-MM-DD format.');
    }

    if (startDate > endDate) {
        throw new DateRangeError('startDate cannot be later than endDate.');
    }

    return { startDate, endDate };
}

function isPlainObject(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function normalizeToArray(value) {
    const input = Array.isArray(value) ? value : [value];
    return input
        .flatMap(item => {
            if (Array.isArray(item)) {
                return normalizeToArray(item);
            }
            if (typeof item === 'string') {
                return item
                    .split(',')
                    .map(part => part.trim())
                    .filter(part => part !== '');
            }
            if (item === undefined || item === null) {
                return [];
            }
            return [item];
        })
        .map(item => (typeof item === 'string' ? item.trim() : item))
        .filter(item => item !== '' && item !== null && item !== undefined);
}

function mergeFilterObjects(base = {}, overrides = {}) {
    const result = { ...base };

    Object.entries(overrides || {}).forEach(([key, value]) => {
        if (value === undefined || value === null) {
            return;
        }

        if (Array.isArray(value)) {
            const normalizedArray = normalizeToArray(value);
            if (normalizedArray.length > 0) {
                result[key] = normalizedArray;
            }
            return;
        }

        if (isPlainObject(value)) {
            const baseValue = isPlainObject(result[key]) ? { ...result[key] } : {};
            const mergedChild = mergeFilterObjects(baseValue, value);
            if (Object.keys(mergedChild).length > 0) {
                result[key] = mergedChild;
            } else if (result[key] !== undefined) {
                delete result[key];
            }
            return;
        }

        if (typeof value === 'string') {
            const trimmed = value.trim();
            if (trimmed !== '') {
                result[key] = trimmed;
            }
            return;
        }

        result[key] = value;
    });

    return result;
}

function sanitizeSalesDriveFilter(rawFilter) {
    if (!isPlainObject(rawFilter)) {
        return {};
    }
    const sanitized = mergeFilterObjects({}, rawFilter);
    delete sanitized.istocnikProdazi;
    return sanitized;
}

function parseNumeric(value) {
    if (value === undefined || value === null || value === '') {
        return null;
    }
    const num = typeof value === 'number' ? value : parseFloat(value);
    return Number.isFinite(num) ? num : null;
}

function pickNumeric(...candidates) {
    for (const candidate of candidates) {
        const parsed = parseNumeric(candidate);
        if (parsed !== null) {
            return parsed;
        }
    }
    return null;
}

function determineProfitRatioColor(value) {
    if (!Number.isFinite(value)) {
        return 'neutral';
    }
    if (value > 25) {
        return 'salad';
    }
    if (value > 20) {
        return 'green';
    }
    if (value >= 15) {
        return 'yellow';
    }
    return 'red';
}

// --- GOOGLE ADS API FUNCTIONS ---

async function getGoogleAdsData({ startDate, endDate }) {
    try {
        console.log('1. Getting a fresh access_token for Google Ads...');
        const auth = new OAuth2Client({ clientId: CLIENT_ID, clientSecret: CLIENT_SECRET, redirectUri: 'http://localhost' });
        auth.setCredentials({ refresh_token: REFRESH_TOKEN });
        const { token } = await auth.getAccessToken();
        if (!token) throw new Error('Failed to get access_token for Google Ads.');
        console.log('Access token for Google Ads obtained successfully.');
        console.log('\n2. Requesting product data from Google Ads...');


        const query = `
            SELECT shopping_product.item_id, shopping_product.title, metrics.cost_micros, metrics.impressions, metrics.clicks
            FROM shopping_product
            WHERE shopping_product.status = 'ELIGIBLE'
              AND segments.date BETWEEN '${startDate}' AND '${endDate}'
        `;

        const url = `https://googleads.googleapis.com/v21/customers/${CUSTOMER_ID}/googleAds:searchStream`;
        const headers = {
            'Content-Type': 'application/json',
            'developer-token': DEVELOPER_TOKEN,
            'login-customer-id': LOGIN_CUSTOMER_ID,
            'Authorization': `Bearer ${token}`,
        };

        const response = await axios.post(url, { query }, { headers });
        const results = response.data.flatMap(batch => batch.results || []);

        const adsData = results.map(item => {
            const titleParts = item.shoppingProduct.title.split(' ');
            const istocnikProdaziIdent = titleParts[titleParts.length - 1];
            const costMicros = parseFloat(item.metrics.costMicros || 0);
            const costUah = (costMicros / 1_000_000) * 1.2; // Convert to UAH and add 20%

            return {
                istocnikProdaziIdent: istocnikProdaziIdent,
                title: item.shoppingProduct.title,
                costUah: costUah,
                impressions: BigInt(item.metrics.impressions || 0),
                clicks: BigInt(item.metrics.clicks || 0),
            };
        });

        console.log(`✅ Received ${adsData.length} records from Google Ads.`);

        // Aggregate Google Ads data by identifier
        const aggregatedAds = adsData.reduce((acc, ad) => {
            const ident = ad.istocnikProdaziIdent;
            if (!acc[ident]) {
                acc[ident] = {
                    title: ad.title, // Keep the title from the first ad encountered
                    costUah: 0,
                    impressions: 0n,
                    clicks: 0n,
                };
            }
            acc[ident].costUah += ad.costUah;
            acc[ident].impressions += ad.impressions;
            acc[ident].clicks += ad.clicks;
            return acc;
        }, {});

        // Convert BigInt to string for JSON serialization
        Object.keys(aggregatedAds).forEach(key => {
            aggregatedAds[key].costUah = aggregatedAds[key].costUah.toFixed(2);
            aggregatedAds[key].impressions = aggregatedAds[key].impressions.toString();
            aggregatedAds[key].clicks = aggregatedAds[key].clicks.toString();
        });

        return { data: aggregatedAds, errors: [] };
    } catch (err) {
        const contextualError = err.response?.data?.error?.message
            || err.response?.data?.message
            || err.message
            || 'Unknown Google Ads API error';
        const message = `Помилка Google Ads: ${contextualError}`;
        console.error('\n❌ An error occurred in Google Ads:', err.response ? JSON.stringify(err.response.data, null, 2) : err.message);
        return { data: {}, errors: [message] };
    }
}

// --- SALESDRIVE API FUNCTIONS ---

async function getSalesDriveDataForSource(sourceId, { startDate, endDate }, overrides = {}) {
    const {
        filter: filterOverride = {},
        limit: overrideLimit
    } = overrides || {};
    const limit = Math.min(parseInt(overrideLimit, 10) || 100, 100); // API documentation caps the page size at 100
    let page = 1;
    const aggregatedOrders = [];
    let totalsSnapshot = null;
    let totalItemsHint = null;
    const baseFilter = {
        orderTime: {
            from: `${startDate} 00:00:00`,
            to: `${endDate} 23:59:59`,
        },
        statusId: "__ALL__", // Maintain current behaviour; can be overridden later if needed
        istocnikProdazi: sourceId,
    };
    const effectiveFilter = mergeFilterObjects(baseFilter, filterOverride);

    try {
        // Paginate until the API signals no more data
        while (true) {
            const params = {
                page,
                limit,
                filter: effectiveFilter,
            };

            const response = await axios.get(SALESDRIVE_URL, {
                headers: { 'Form-Api-Key': SALESDRIVE_API_KEY },
                params,
                timeout: 30000,
            });

            const pageOrders = response.data?.data || [];
            const pageTotals = response.data?.totals || {};
            const pagination = response.data?.pagination || {};

            aggregatedOrders.push(...pageOrders);

            if (!totalsSnapshot && Object.keys(pageTotals).length > 0) {
                totalsSnapshot = pageTotals;
            }

            if (!totalItemsHint) {
                totalItemsHint = pagination.total || pagination.totalCount || pagination.count || null;
            }

            const reachedEndByPageSize = pageOrders.length < limit;
            const totalPagesHint = pagination.totalPages || pagination.pageCount || pagination.lastPage || null;
            const coveredAllHintedItems = totalItemsHint ? aggregatedOrders.length >= totalItemsHint : false;
            const exhaustedPageHints = totalPagesHint ? page >= Number(totalPagesHint) : false;

            if (reachedEndByPageSize || coveredAllHintedItems || exhaustedPageHints || pageOrders.length === 0) {
                break;
            }

            page += 1;
        }

        return {
            orders: aggregatedOrders,
            totals: totalsSnapshot || {},
            count: totalItemsHint || aggregatedOrders.length,
            errors: [],
        };
    } catch (error) {
        const apiMessage = error.response?.data?.message || error.response?.data?.error || error.message;
        console.error(`Error fetching SalesDrive orders for source ${sourceId}:`, error.response?.data || error.message);
        return { orders: [], totals: {}, count: 0, errors: [apiMessage || 'Невідома помилка SalesDrive'] };
    }
}

// --- DATA COMBINING FUNCTION ---

function calculateSummaryReport(googleAdsTotals, salesDriveTotals, { startDate, endDate, planOverrides = {} }) {
    // --- Primary Metrics ---
    const adSpend = googleAdsTotals.totalCostUah; // Already in UAH with VAT
    const impressions = googleAdsTotals.totalImpressions;
    const clicks = googleAdsTotals.totalClicks;
    const transactions = salesDriveTotals.totalTransactions;
    const sales = salesDriveTotals.totalPaymentAmount;
    const costOfGoods = salesDriveTotals.totalCostPriceAmount;

    // --- Calculated Metrics ---
    const safeDivide = (numerator, denominator) => (denominator > 0 ? numerator / denominator : 0);
    const toCurrencyString = (value) => (Number.isFinite(value) ? value.toFixed(2) : '0.00');
    const toPercentString = (value) => {
        if (Number.isFinite(value)) {
            return value.toFixed(2);
        }
        if (value === Infinity) {
            return '∞';
        }
        if (value === -Infinity) {
            return '-∞';
        }
        return '0.00';
    };

    const cpc = safeDivide(adSpend, clicks);
    const ctr = safeDivide(clicks, impressions) * 100;
    const clickToTransConversion = safeDivide(transactions, clicks) * 100;
    const cpa = safeDivide(adSpend, transactions);
    const margin = sales - costOfGoods;
    const profit = margin - adSpend;
    const aov = safeDivide(sales, transactions);
    const roi = adSpend > 0 ? safeDivide(margin, adSpend) * 100 : (margin > 0 ? Infinity : 0);
    const avgProfitPerTransaction = safeDivide(profit, transactions);
    const adSpendToSalesRatio = safeDivide(adSpend, sales) * 100;
    const cogsToSalesRatio = safeDivide(costOfGoods, sales) * 100;
    const profitToSalesRatio = safeDivide(profit, sales) * 100;
    const profitToSalesColor = determineProfitRatioColor(profitToSalesRatio);
    const profitToSalesStyleMap = {
        red: 'color: #dc3545;',
        yellow: 'color: #ffc107;',
        green: 'color: #28a745;',
        salad: 'color: #32cd32;',
        neutral: ''
    };
    const profitToSalesStyle = profitToSalesStyleMap[profitToSalesColor] || '';

    // --- Plan & Projection Metrics ---
    const fallbackPlanSales = parseFloat(PLAN_SALES_MONTH || '0') || 0;
    const fallbackPlanProfit = parseFloat(PLAN_PROFIT_MONTH || '0') || 0;
    const planSalesMonthly = parseFloat(planOverrides.sales ?? fallbackPlanSales) || 0;
    const planProfitMonthly = parseFloat(planOverrides.profit ?? fallbackPlanProfit) || 0;

    const start = new Date(startDate);
    const end = new Date(endDate);
    const daysInPeriod = (end.getTime() - start.getTime()) / (1000 * 3600 * 24) + 1;
    const daysInMonth = new Date(end.getFullYear(), end.getMonth() + 1, 0).getDate();
    const monthStart = new Date(end.getFullYear(), end.getMonth(), 1);
    const elapsedDaysInMonth = Math.floor((end.getTime() - monthStart.getTime()) / (1000 * 3600 * 24)) + 1;
    const normalizedElapsedDays = Math.min(Math.max(elapsedDaysInMonth, 0), daysInMonth || elapsedDaysInMonth);

    const projectedSales = sales;
    const projectedProfit = profit;

    const planSalesPerDay = daysInMonth > 0 ? planSalesMonthly / daysInMonth : 0;
    const planProfitPerDay = daysInMonth > 0 ? planProfitMonthly / daysInMonth : 0;
    const planSalesForPeriod = planSalesPerDay * daysInPeriod;
    const planProfitForPeriod = planProfitPerDay * daysInPeriod;
    const planCumulativeSales = planSalesForPeriod;
    const planCumulativeProfit = planProfitForPeriod;

    const salesPlanDeviation = planSalesForPeriod > 0 ? ((projectedSales / planSalesForPeriod) - 1) * 100 : 0;
    const profitPlanDeviation = planProfitForPeriod > 0 ? ((projectedProfit / planProfitForPeriod) - 1) * 100 : 0;
    const salesFactDeviation = planSalesForPeriod > 0 ? ((sales / planSalesForPeriod) - 1) * 100 : 0;
    const profitFactDeviation = planProfitForPeriod > 0 ? ((profit / planProfitForPeriod) - 1) * 100 : 0;
    const salesCumulativePlanDeviation = planCumulativeSales > 0 ? ((sales / planCumulativeSales) - 1) * 100 : 0;
    const profitCumulativePlanDeviation = planCumulativeProfit > 0 ? ((profit / planCumulativeProfit) - 1) * 100 : 0;


    return {
        adSpend: toCurrencyString(adSpend),
        cpc: toCurrencyString(cpc),
        impressions,
        ctr: toPercentString(ctr),
        clicks,
        clickToTransConversion: toPercentString(clickToTransConversion),
        transactions,
        cpa: toCurrencyString(cpa),
        sales: toCurrencyString(sales),
        costOfGoods: toCurrencyString(costOfGoods),
        margin: toCurrencyString(margin),
        profit: toCurrencyString(profit),
        aov: toCurrencyString(aov),
        roi: toPercentString(roi),
        avgProfitPerTransaction: toCurrencyString(avgProfitPerTransaction),
        adSpendToSalesRatio: toPercentString(adSpendToSalesRatio),
        cogsToSalesRatio: toPercentString(cogsToSalesRatio),
        profitToSalesRatio: toPercentString(profitToSalesRatio),
        profitToSalesRatioColor: profitToSalesColor,
        profitToSalesRatioStyle: profitToSalesStyle,
        planSales: toCurrencyString(planSalesForPeriod),
        planSalesMonthly: toCurrencyString(planSalesMonthly),
        planSalesPerDay: toCurrencyString(planSalesPerDay),
        projectedSales: toCurrencyString(projectedSales),
        salesPlanDeviation: toPercentString(salesPlanDeviation),
        planCumulativeSales: toCurrencyString(planCumulativeSales),
        salesFactCumulative: toCurrencyString(sales),
        salesFactDeviation: toPercentString(salesFactDeviation),
        salesCumulativePlanDeviation: toPercentString(salesCumulativePlanDeviation),
        planProfit: toCurrencyString(planProfitForPeriod),
        planProfitMonthly: toCurrencyString(planProfitMonthly),
        planProfitPerDay: toCurrencyString(planProfitPerDay),
        projectedProfit: toCurrencyString(projectedProfit),
        profitPlanDeviation: toPercentString(profitPlanDeviation),
        planCumulativeProfit: toCurrencyString(planCumulativeProfit),
        profitFactCumulative: toCurrencyString(profit),
        profitFactDeviation: toPercentString(profitFactDeviation),
        profitCumulativePlanDeviation: toPercentString(profitCumulativePlanDeviation),
        elapsedDaysInMonth: normalizedElapsedDays,
        daysInMonth,
        daysInPeriod,
    };
}


async function prepareAllDataForView({ startDate, endDate, selectedSourceIds = [], salesDriveRequestOptions = {}, planOverrides = {} }) {
    console.log("🚀 Starting data preparation for all views...");
    console.log(`Using date range: ${startDate} to ${endDate}`);
    const selectedIdSet = new Set((selectedSourceIds || []).map(id => id.toString()));
    const isFiltering = selectedIdSet.size > 0;
    if (isFiltering) {
        console.log(`Filtering by source IDs: ${Array.from(selectedIdSet).join(', ')}`);
    }
    const filterForLog = isPlainObject(salesDriveRequestOptions.filter) ? salesDriveRequestOptions.filter : {};
    if (Object.keys(filterForLog).length > 0) {
        console.log('Applying additional SalesDrive filters:', JSON.stringify(filterForLog));
    }
    if (salesDriveRequestOptions.limit && salesDriveRequestOptions.limit !== 100) {
        console.log(`Using SalesDrive page limit override: ${salesDriveRequestOptions.limit}`);
    }

    const googleAdsResult = await getGoogleAdsData({ startDate, endDate });
    const googleAdsDataMap = googleAdsResult.data || {};
    const alerts = Array.isArray(googleAdsResult.errors) ? [...googleAdsResult.errors] : [];
    let rateLimitCooldown = alerts.some(msg => typeof msg === 'string' && msg.toLowerCase().includes('limit'));
    let allOrders = [];
    let salesDriveTotals = {
        totalPaymentAmount: 0,
        totalCostPriceAmount: 0,
        totalProfitAmount: 0,
        totalTransactions: 0,
    };
    const perSourceSalesTotals = {};

    let sourcesToProcess = SALESDRIVE_ISTOCHNIKI;
    if (isFiltering) {
        sourcesToProcess = SALESDRIVE_ISTOCHNIKI.filter(s => selectedIdSet.has(s.id.toString()));
        if (sourcesToProcess.length === 0) {
            console.warn('Selected sources were not found. Falling back to all configured sources.');
            sourcesToProcess = SALESDRIVE_ISTOCHNIKI;
        }
    }

    for (const source of sourcesToProcess) {
        console.log(`\nProcessing source: ${source.ident} (ID: ${source.id})`);
        const { orders, totals, errors: sourceErrors } = await getSalesDriveDataForSource(
            source.id,
            { startDate, endDate },
            salesDriveRequestOptions
        );
        console.log(`Found ${orders.length} orders in SalesDrive.`);
        if (Array.isArray(sourceErrors) && sourceErrors.length > 0) {
            sourceErrors.forEach(errMessage => {
                const friendlyMessage = `Помилка SalesDrive (${source.ident}): ${errMessage}`;
                alerts.push(friendlyMessage);
                if (typeof friendlyMessage === 'string' && friendlyMessage.toLowerCase().includes('limit')) {
                    rateLimitCooldown = true;
                }
            });
        }

        const ordersWithSource = [];
        let manualPaymentTotal = 0;
        let manualProfitTotal = 0;
        let manualCostTotal = 0;
        let manualTransactionCount = 0;

        for (const order of orders) {
            const decoratedOrder = {
                ...order,
                istocnikProdaziIdent: source.ident // Ensure identifier is attached
            };
            ordersWithSource.push(decoratedOrder);

            const paymentValue = pickNumeric(
                order.paymentAmount,
                order.totalCost,
                order.totalSum,
                order.total
            );
            if (paymentValue !== null) {
                manualPaymentTotal += paymentValue;
            }

            const costValue = pickNumeric(order.costPriceAmount, order.costPrice, order.totalCostPriceAmount);
            if (costValue !== null) {
                manualCostTotal += costValue;
            }

            const explicitProfit = pickNumeric(order.profitAmount, order.profit);
            if (explicitProfit !== null) {
                manualProfitTotal += explicitProfit;
            } else if (paymentValue !== null || costValue !== null) {
                const fallbackProfit = (paymentValue ?? 0) - (costValue ?? 0);
                manualProfitTotal += fallbackProfit;
            }

            manualTransactionCount += 1;
        }

        allOrders = allOrders.concat(ordersWithSource);
        const totalsPaymentRaw = totals?.paymentAmount;
        const totalsProfitRaw = totals?.profitAmount;
        const totalsCountRaw = totals?.count;
        const totalsCostRaw = totals?.costPriceAmount;

        const totalsPayment = parseNumeric(totalsPaymentRaw);
        const totalsProfit = parseNumeric(totalsProfitRaw);
        const totalsCount = parseNumeric(totalsCountRaw);
        const totalsCost = parseNumeric(totalsCostRaw);

        const hasTotalsPayment = totalsPaymentRaw !== undefined && totalsPaymentRaw !== null && totalsPaymentRaw !== '';
        const hasTotalsProfit = totalsProfitRaw !== undefined && totalsProfitRaw !== null && totalsProfitRaw !== '';
        const hasTotalsCount = totalsCountRaw !== undefined && totalsCountRaw !== null && totalsCountRaw !== '';
        const hasTotalsCost = totalsCostRaw !== undefined && totalsCostRaw !== null && totalsCostRaw !== '';

        const paymentShouldUseManual = !hasTotalsPayment || totalsPayment === null || (Math.abs(totalsPayment) <= ZERO_EPSILON && Math.abs(manualPaymentTotal) > ZERO_EPSILON);
        const profitShouldUseManual = !hasTotalsProfit || totalsProfit === null || (Math.abs(totalsProfit) <= ZERO_EPSILON && Math.abs(manualProfitTotal) > ZERO_EPSILON);
        const countShouldUseManual = !hasTotalsCount || totalsCount === null || (Math.round(totalsCount) === 0 && manualTransactionCount > 0);
        const costShouldUseManual = !hasTotalsCost || totalsCost === null || (Math.abs(totalsCost) <= ZERO_EPSILON && Math.abs(manualCostTotal) > ZERO_EPSILON);

        const paymentIncrement = paymentShouldUseManual ? manualPaymentTotal : (totalsPayment ?? 0);
        const profitIncrement = profitShouldUseManual ? manualProfitTotal : (totalsProfit ?? 0);
        const transactionIncrement = countShouldUseManual ? manualTransactionCount : Math.max(0, Math.round(totalsCount));
        const costIncrement = costShouldUseManual ? manualCostTotal : (totalsCost ?? 0);

        salesDriveTotals.totalPaymentAmount += paymentIncrement;
        salesDriveTotals.totalProfitAmount += profitIncrement;
        salesDriveTotals.totalTransactions += transactionIncrement;
        salesDriveTotals.totalCostPriceAmount += costIncrement;

        perSourceSalesTotals[source.ident] = {
            totalPaymentAmount: paymentIncrement,
            totalProfitAmount: profitIncrement,
            totalTransactions: transactionIncrement,
            totalCostPriceAmount: costIncrement,
        };
    }

    // 1. Prepare Google Ads data for its own tab and calculate totals
    const perSourceGoogleTotals = {};
    const sourcesToProcessIdents = sourcesToProcess.map(s => s.ident);
    const filteredGoogleAdsDataMap = Object.fromEntries(
        Object.entries(googleAdsDataMap).filter(([ident]) => {
            // If no sources are selected, include all. Otherwise, check if the ident is in the list.
            return sourcesToProcessIdents.length === 0 || sourcesToProcessIdents.includes(ident);
        })
    );

    let googleAdsTotals = { totalCostUah: 0, totalImpressions: 0, totalClicks: 0 };
    const googleAdsDataForTable = Object.entries(filteredGoogleAdsDataMap).map(([ident, ads]) => {
        const cost = Number.parseFloat(ads.costUah) || 0;
        const impressions = Number.parseInt(ads.impressions, 10) || 0;
        const clicks = Number.parseInt(ads.clicks, 10) || 0;

        perSourceGoogleTotals[ident] = {
            totalCostUah: cost,
            totalImpressions: impressions,
            totalClicks: clicks,
        };

        googleAdsTotals.totalCostUah += cost;
        googleAdsTotals.totalImpressions += impressions;
        googleAdsTotals.totalClicks += clicks;
        return {
            istocnikProdaziIdent: ident,
            ...ads
        };
    });

    // 2. Prepare SalesDrive data for its own tab
    const salesDriveData = allOrders.map(order => ({
        id: order.id,
        fName: order.primaryContact?.fName || "",
        lName: order.primaryContact?.lName || "",
        orderTime: order.orderTime || "",
        products: order.products?.[0]?.text || "N/A",
        statusName: order.statusName || "N/A",
        totalCost: order.totalCost || 0,
        istocnikProdaziIdent: order.istocnikProdaziIdent,
    }));

    // 3. Prepare Combined data for the third tab
    const combinedData = allOrders.map(order => {
        const googleAdsData = googleAdsDataMap[order.istocnikProdaziIdent] || null;
        return {
            id: order.id,
            fName: order.primaryContact?.fName || "",
            lName: order.primaryContact?.lName || "",
            orderTime: order.orderTime || "",
            istocnikProdaziIdent: order.istocnikProdaziIdent,
            products: order.products?.[0]?.text || "N/A",
            statusName: order.statusName || "N/A",
            totalCost: order.totalCost || 0,
            googleAdsData: googleAdsData
        };
    });

    // 4. Calculate the summary report
    const summaryReport = calculateSummaryReport(googleAdsTotals, salesDriveTotals, { startDate, endDate, planOverrides });

    const sourceSummaries = sourcesToProcess.map(source => {
        const ident = source.ident;
        const googleTotalsForSource = perSourceGoogleTotals[ident] || { totalCostUah: 0, totalImpressions: 0, totalClicks: 0 };
        const salesTotalsForSource = perSourceSalesTotals[ident] || { totalPaymentAmount: 0, totalCostPriceAmount: 0, totalProfitAmount: 0, totalTransactions: 0 };

        const summary = calculateSummaryReport(
            {
                totalCostUah: googleTotalsForSource.totalCostUah || 0,
                totalImpressions: googleTotalsForSource.totalImpressions || 0,
                totalClicks: googleTotalsForSource.totalClicks || 0,
            },
            {
                totalPaymentAmount: salesTotalsForSource.totalPaymentAmount || 0,
                totalCostPriceAmount: salesTotalsForSource.totalCostPriceAmount || 0,
                totalProfitAmount: salesTotalsForSource.totalProfitAmount || 0,
                totalTransactions: salesTotalsForSource.totalTransactions || 0,
            },
            { startDate, endDate, planOverrides }
        );

        return {
            id: source.id,
            ident: ident,
            name: source.name || ident,
            summary,
            salesTotals: salesTotalsForSource,
            googleTotals: googleTotalsForSource,
        };
    });

    console.log(`\n--- Data Preparation Complete ---`);
    return {
        googleAdsData: googleAdsDataForTable,
        salesDriveData,
        combinedData,
        summaryReport,
        sourceSummaries,
        alerts,
        rateLimitCooldown
    };
}

// --- CONTROLLER EXPORTS ---

export const renderCombinedData = async (req, res) => {
    try {
        const { startDate, endDate } = resolveDateRange(req.query.startDate, req.query.endDate);
        const rawSelectedSources = req.query.source !== undefined ? req.query.source : [];
        const selectedSourceIds = normalizeToArray(rawSelectedSources);
        const activeTab = req.query.activeTab || 'summary-tab';
        const rawFilterFromQuery = req.query.filter || {};
        const salesDriveFilter = sanitizeSalesDriveFilter(rawFilterFromQuery);
        const limitCandidate = Array.isArray(req.query.limit)
            ? req.query.limit[req.query.limit.length - 1]
            : req.query.limit;
        const limitOverride = limitCandidate !== undefined ? parseInt(limitCandidate, 10) : NaN;
        const salesDriveRequestOptions = {};
        if (Object.keys(salesDriveFilter).length > 0) {
            salesDriveRequestOptions.filter = salesDriveFilter;
        }
        if (!Number.isNaN(limitOverride)) {
            salesDriveRequestOptions.limit = limitOverride;
        }

        const fallbackPlanSales = parseFloat(PLAN_SALES_MONTH || '0') || 0;
        const fallbackPlanProfit = parseFloat(PLAN_PROFIT_MONTH || '0') || 0;
        const planSalesRaw = Array.isArray(req.query.planSales)
            ? req.query.planSales[req.query.planSales.length - 1]
            : req.query.planSales;
        const planProfitRaw = Array.isArray(req.query.planProfit)
            ? req.query.planProfit[req.query.planProfit.length - 1]
            : req.query.planProfit;

        const parsePlanInput = (value) => {
            if (value === undefined || value === null || value === '') {
                return null;
            }
            const parsed = parseFloat(value);
            return Number.isFinite(parsed) ? parsed : null;
        };

        const planSalesParsed = parsePlanInput(planSalesRaw);
        const planProfitParsed = parsePlanInput(planProfitRaw);

        const planOverrides = {};
        if (planSalesParsed !== null) {
            planOverrides.sales = planSalesParsed;
        }
        if (planProfitParsed !== null) {
            planOverrides.profit = planProfitParsed;
        }

        const planInputs = {
            sales: planSalesRaw !== undefined ? planSalesRaw : '',
            profit: planProfitRaw !== undefined ? planProfitRaw : ''
        };

        const planDefaults = {
            sales: fallbackPlanSales,
            profit: fallbackPlanProfit
        };

        const {
            googleAdsData,
            salesDriveData,
            combinedData,
            summaryReport,
            alerts,
            rateLimitCooldown,
            sourceSummaries
        } = await prepareAllDataForView({
            startDate,
            endDate,
            selectedSourceIds,
            salesDriveRequestOptions,
            planOverrides
        });
        res.render('index', {
            googleAdsData,
            salesDriveData,
            combinedData,
            summaryReport,
            sourceSummaries,
            startDate,
            endDate,
            salesDriveIstocniki: SALESDRIVE_ISTOCHNIKI,
            selectedSources: selectedSourceIds,
            activeTab,
            salesDriveFilter,
            salesDriveLimit: salesDriveRequestOptions.limit,
            alerts,
            rateLimitCooldown,
            planInputs,
            planDefaults
        });
    } catch (error) {
        console.error("Server Error:", error);
        if (error instanceof DateRangeError || error.statusCode === 400) {
            const includeStack = (process.env.NODE_ENV || '').toLowerCase() !== 'production';
            return res.status(400).render('error', {
                message: error.message,
                error: includeStack ? error : {}
            });
        }
        res.status(500).render('error', { message: "Internal Server Error", error });
    }
};
