import fs from 'fs/promises';
import path from 'path';
import {
    buildReportData,
    waitForSalesDriveIdle,
    getSalesdriveSources
} from './reportDataService.js';

const ROOT_DATA_DIR = path.resolve('data');
const MONTHLY_DATA_DIR = path.join(ROOT_DATA_DIR, 'monthly');
const PLANS_DATA_DIR = path.join(ROOT_DATA_DIR, 'plans');
const MONTHLY_START_YEAR = 2025;
const MONTHLY_START_MONTH = 9; // September 2025

const DECIMAL_DIGITS = 2;

async function ensureDirectory(dirPath) {
    await fs.mkdir(dirPath, { recursive: true });
}

function getMonthKey(year, month) {
    return `${year}-${String(month).padStart(2, '0')}`;
}

function getMonthFilePath(year, month) {
    return path.join(MONTHLY_DATA_DIR, `${getMonthKey(year, month)}.json`);
}

function getPlanFilePath(year) {
    return path.join(PLANS_DATA_DIR, `${year}.json`);
}

function getMonthStartDate(year, month) {
    return new Date(Date.UTC(year, month - 1, 1));
}

function getMonthEndDate(year, month) {
    const end = new Date(Date.UTC(year, month, 0));
    end.setUTCHours(23, 59, 59, 999);
    return end;
}

function getDaysInMonth(year, month) {
    return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

function getElapsedDays(year, month, asOf = new Date()) {
    const asOfUtc = new Date(asOf);
    const start = getMonthStartDate(year, month);
    const daysInThisMonth = getDaysInMonth(year, month);
    if (asOfUtc < start) {
        return 0;
    }
    const end = getMonthEndDate(year, month);
    const effectiveEnd = asOfUtc > end ? end : asOfUtc;
    const diffMs = effectiveEnd.getTime() - start.getTime();
    const elapsed = Math.floor(diffMs / (24 * 60 * 60 * 1000)) + 1;
    return Math.max(0, Math.min(daysInThisMonth, elapsed));
}

async function readJson(filePath, fallback = null) {
    try {
        const buf = await fs.readFile(filePath, 'utf8');
        return JSON.parse(buf);
    } catch (error) {
        if (error.code === 'ENOENT') {
            return fallback;
        }
        throw error;
    }
}

async function writeJsonAtomic(filePath, data) {
    const dir = path.dirname(filePath);
    await ensureDirectory(dir);
    const tempPath = `${filePath}.${Date.now()}.tmp`;
    await fs.writeFile(tempPath, JSON.stringify(data, null, 2), 'utf8');
    await fs.rename(tempPath, filePath);
}

function toNumber(value) {
    if (value === null || value === undefined || value === '') {
        return 0;
    }
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
        return parsed;
    }
    if (typeof value === 'bigint') {
        return Number(value);
    }
    return 0;
}

function safeDivide(numerator, denominator) {
    if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator === 0) {
        return 0;
    }
    return numerator / denominator;
}

function createEmptyMetrics() {
    return {
        adSpend: 0,
        impressions: 0,
        clicks: 0,
        transactions: 0,
        sales: 0,
        costOfGoods: 0
    };
}

function sumBaseMetrics(collection) {
    return collection.reduce((acc, item) => {
        acc.adSpend += item.metrics.adSpend;
        acc.impressions += item.metrics.impressions;
        acc.clicks += item.metrics.clicks;
        acc.transactions += item.metrics.transactions;
        acc.sales += item.metrics.sales;
        acc.costOfGoods += item.metrics.costOfGoods;
        return acc;
    }, createEmptyMetrics());
}

function resolvePlanForSource(ident, planConfig) {
    const defaultPlan = {};
    const overrides = planConfig?.sources || {};
    const override = overrides[ident] || {};
    const sales = Number.isFinite(override.sales) ? override.sales : Number.isFinite(defaultPlan.sales) ? defaultPlan.sales : 0;
    const profit = Number.isFinite(override.profit) ? override.profit : Number.isFinite(defaultPlan.profit) ? defaultPlan.profit : 0;
    return {
        sales,
        profit,
        isOverride: overrides[ident] !== undefined
    };
}

function normalizePlanValue(value) {
    if (!Number.isFinite(value) || value < 0) {
        return 0;
    }
    return value;
}

function computeDerivedMetrics(base, plan, daysInMonth, elapsedDays) {
    const margin = base.sales - base.costOfGoods;
    const profit = margin - base.adSpend;
    const projectedSales = elapsedDays > 0 ? safeDivide(base.sales, elapsedDays) * daysInMonth : 0;
    const projectedProfit = elapsedDays > 0 ? safeDivide(profit, elapsedDays) * daysInMonth : 0;
    const planSalesMonthly = plan.sales || 0;
    const planProfitMonthly = plan.profit || 0;
    const planSalesDaily = daysInMonth > 0 ? planSalesMonthly / daysInMonth : 0;
    const planProfitDaily = daysInMonth > 0 ? planProfitMonthly / daysInMonth : 0;
    const planSalesCumulative = planSalesDaily * elapsedDays;
    const planProfitCumulative = planProfitDaily * elapsedDays;

    const deviationProjectedSales = planSalesMonthly > 0
        ? (projectedSales / planSalesMonthly - 1) * 100
        : null;
    const deviationSalesCumulative = planSalesCumulative > 0
        ? (base.sales / planSalesCumulative - 1) * 100
        : null;
    const deviationProjectedProfit = planProfitMonthly > 0
        ? (projectedProfit / planProfitMonthly - 1) * 100
        : null;
    const deviationProfitCumulative = planProfitCumulative > 0
        ? (profit / planProfitCumulative - 1) * 100
        : null;

    return {
        margin,
        profit,
        costPerClick: base.clicks > 0 ? base.adSpend / base.clicks : 0,
        ctr: base.impressions > 0 ? (base.clicks / base.impressions) * 100 : 0,
        clickToTransConversion: base.clicks > 0 ? (base.transactions / base.clicks) * 100 : 0,
        costPerTransaction: base.transactions > 0 ? base.adSpend / base.transactions : 0,
        avgCheck: base.transactions > 0 ? base.sales / base.transactions : 0,
        roi: base.adSpend > 0 ? (margin / base.adSpend) * 100 : 0,
        avgProfitPerTransaction: base.transactions > 0 ? profit / base.transactions : 0,
        adSpendShare: base.sales > 0 ? (base.adSpend / base.sales) * 100 : 0,
        costShare: base.sales > 0 ? (base.costOfGoods / base.sales) * 100 : 0,
        profitShare: base.sales > 0 ? (profit / base.sales) * 100 : 0,
        projectedSales,
        projectedProfit,
        planSalesMonthly,
        planProfitMonthly,
        planSalesCumulative,
        planProfitCumulative,
        deviationProjectedSales,
        deviationSalesCumulative,
        deviationProjectedProfit,
        deviationProfitCumulative,
        factSalesCumulative: base.sales,
        factProfitCumulative: profit
    };
}

function roundTo(value, digits = DECIMAL_DIGITS) {
    if (!Number.isFinite(value)) {
        return 0;
    }
    const factor = 10 ** digits;
    return Math.round(value * factor) / factor;
}

function formatNumber(value, options = {}) {
    if (value === null || value === undefined) {
        return '—';
    }
    if (!Number.isFinite(value)) {
        return '—';
    }
    const { minimumFractionDigits = 0, maximumFractionDigits = 2, style = 'decimal' } = options;
    return new Intl.NumberFormat('uk-UA', { style, minimumFractionDigits, maximumFractionDigits }).format(value);
}

function formatCurrency(value) {
    if (!Number.isFinite(value)) {
        return '—';
    }
    return formatNumber(value, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatPercent(value) {
    if (value === null || value === undefined || !Number.isFinite(value)) {
        return '—';
    }
    return `${formatNumber(value, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`;
}

function formatProjection(value) {
    if (!Number.isFinite(value)) {
        return '—';
    }
    return formatCurrency(value);
}

async function collectMonthlyFacts(year, month, { asOf, reportJob } = {}) {
    const now = asOf ? new Date(asOf) : new Date();
    const startDate = getMonthStartDate(year, month);
    const monthEnd = getMonthEndDate(year, month);
    const effectiveEnd = now > monthEnd ? monthEnd : now;
    const startDateStr = startDate.toISOString().slice(0, 10);
    const endDateStr = effectiveEnd.toISOString().slice(0, 10);
    const daysInMonth = getDaysInMonth(year, month);
    const elapsedDays = getElapsedDays(year, month, now);

    const report = await buildReportData(
        {
            startDate: startDateStr,
            endDate: endDateStr,
            selectedSourceIds: [],
            salesDriveRequestOptions: {},
            planOverrides: {}
        },
        { reportJob: reportJob || null }
    );

    await waitForSalesDriveIdle();

    const summaryByIdent = new Map();
    if (Array.isArray(report.sourceSummaries)) {
        report.sourceSummaries.forEach(entry => {
            if (entry && entry.ident) {
                summaryByIdent.set(entry.ident, entry);
            }
        });
    }

    const configuredSources = getSalesdriveSources();
    const sources = configuredSources.map(source => {
        const summary = summaryByIdent.get(source.ident) || {};
        const googleTotals = summary.googleTotals || {};
        const salesTotals = summary.salesTotals || {};
        const metrics = {
            adSpend: toNumber(googleTotals.totalCostUah),
            impressions: toNumber(googleTotals.totalImpressions),
            clicks: toNumber(googleTotals.totalClicks),
            transactions: toNumber(salesTotals.totalTransactions),
            sales: toNumber(salesTotals.totalPaymentAmount),
            costOfGoods: toNumber(salesTotals.totalCostPriceAmount)
        };
        return {
            id: source.id,
            ident: source.ident,
            name: source.nameView || source.ident,
            nameView: source.nameView || source.ident,
            metrics
        };
    });

    const totalsMetrics = sumBaseMetrics(sources);
    const facts = {
        year,
        month,
        startDate: startDateStr,
        endDate: endDateStr,
        daysInMonth,
        elapsedDays,
        generatedAt: new Date().toISOString(),
        sources,
        totals: {
            metrics: totalsMetrics
        }
    };

    return facts;
}

async function saveMonthlyFacts(year, month, facts) {
    await writeJsonAtomic(getMonthFilePath(year, month), facts);
}

async function loadMonthlyFacts(year, month) {
    return readJson(getMonthFilePath(year, month), null);
}

export async function listMonthEntries() {
    const current = new Date();
    const currentYear = current.getUTCFullYear();
    const currentMonth = current.getUTCMonth() + 1;
    const months = [];
    let y = MONTHLY_START_YEAR;
    let m = MONTHLY_START_MONTH;

    while (y < currentYear || (y === currentYear && m <= currentMonth)) {
        months.push({ year: y, month: m, key: getMonthKey(y, m) });
        m += 1;
        if (m > 12) {
            m = 1;
            y += 1;
        }
    }

    return months;
}

async function loadYearPlans(year) {
    const defaultStructure = {
        year,
        months: {},
        updatedAt: null
    };
    const data = await readJson(getPlanFilePath(year), defaultStructure);
    if (!data.months) {
        data.months = {};
    }
    return data;
}

async function saveYearPlans(year, plans) {
    await writeJsonAtomic(getPlanFilePath(year), plans);
}

function getMonthPlan(plans, month) {
    return plans.months[month] || {
        default: { sales: 0, profit: 0 },
        sources: {},
        updatedAt: null
    };
}

function setMonthPlan(plans, month, data) {
    plans.months[month] = {
        default: {
            sales: Number.isFinite(data.default?.sales) ? data.default.sales : 0,
            profit: Number.isFinite(data.default?.profit) ? data.default.profit : 0
        },
        sources: data.sources || {},
        updatedAt: new Date().toISOString()
    };
}

function formatMonthLabel(year, month) {
    const date = new Date(Date.UTC(year, month - 1, 1));
    return new Intl.DateTimeFormat('uk-UA', { month: 'long', year: 'numeric' }).format(date);
}

function buildDisplayMetrics(base, derived) {
    return {
        raw: {
            ...base,
            ...derived
        },
        formatted: {
            adSpend: formatCurrency(base.adSpend),
            impressions: formatNumber(base.impressions),
            clicks: formatNumber(base.clicks),
            transactions: formatNumber(base.transactions),
            sales: formatCurrency(base.sales),
            costOfGoods: formatCurrency(base.costOfGoods),
            margin: formatCurrency(derived.margin),
            profit: formatCurrency(derived.profit),
            costPerClick: formatCurrency(derived.costPerClick),
            ctr: formatPercent(derived.ctr),
            clickToTransConversion: formatPercent(derived.clickToTransConversion),
            costPerTransaction: formatCurrency(derived.costPerTransaction),
            avgCheck: formatCurrency(derived.avgCheck),
            roi: formatPercent(derived.roi),
            avgProfitPerTransaction: formatCurrency(derived.avgProfitPerTransaction),
            adSpendShare: formatPercent(derived.adSpendShare),
            costShare: formatPercent(derived.costShare),
            profitShare: formatPercent(derived.profitShare),
            projectedSales: formatProjection(derived.projectedSales),
            projectedProfit: formatProjection(derived.projectedProfit),
            planSalesMonthly: formatCurrency(derived.planSalesMonthly),
            planProfitMonthly: formatCurrency(derived.planProfitMonthly),
            planSalesCumulative: formatCurrency(derived.planSalesCumulative),
            planProfitCumulative: formatCurrency(derived.planProfitCumulative),
            deviationProjectedSales: derived.deviationProjectedSales === null ? '—' : formatPercent(derived.deviationProjectedSales),
            deviationSalesCumulative: derived.deviationSalesCumulative === null ? '—' : formatPercent(derived.deviationSalesCumulative),
            deviationProjectedProfit: derived.deviationProjectedProfit === null ? '—' : formatPercent(derived.deviationProjectedProfit),
            deviationProfitCumulative: derived.deviationProfitCumulative === null ? '—' : formatPercent(derived.deviationProfitCumulative),
            factSalesCumulative: formatCurrency(derived.factSalesCumulative),
            factProfitCumulative: formatCurrency(derived.factProfitCumulative)
        }
    };
}

function aggregatePlanTotals(sourceRows) {
    return sourceRows.reduce(
        (acc, row) => {
            acc.sales += row.plan.sales || 0;
            acc.profit += row.plan.profit || 0;
            return acc;
        },
        { sales: 0, profit: 0 }
    );
}

function buildMonthReportObject({ year, month, facts, planConfig, isCurrentMonth, asOfDate }) {
    const daysInMonth = facts?.daysInMonth || getDaysInMonth(year, month);
    const elapsedDays = facts?.elapsedDays || getElapsedDays(year, month, asOfDate);
    const planSources = planConfig?.sources || {};

    const fallbackSources = getSalesdriveSources().map(source => ({
        id: source.id,
        ident: source.ident,
        name: source.nameView || source.ident,
        nameView: source.nameView || source.ident,
        metrics: createEmptyMetrics()
    }));

    const sourceRows = (facts?.sources || fallbackSources).map(source => ({
        id: source.id,
        ident: source.ident,
        name: source.name || source.ident,
        nameView: source.nameView || source.name || source.ident,
        metrics: source.metrics || createEmptyMetrics()
    })).map(source => {
        const plan = resolvePlanForSource(source.ident, { sources: planSources });
        const derived = computeDerivedMetrics(source.metrics, plan, daysInMonth, elapsedDays);
        const metrics = buildDisplayMetrics(source.metrics, derived);
        return {
            id: source.id,
            ident: source.ident,
            name: source.name,
            metrics: source.metrics,
            plan,
            derived,
            display: metrics,
            hasFacts: Object.values(source.metrics).some(value => value !== 0),
            planOverride: plan.isOverride || false
        };
    });

    const totalsBase = sumBaseMetrics(sourceRows);
    const aggregatePlan = aggregatePlanTotals(sourceRows);
    const totalsDerived = computeDerivedMetrics(
        totalsBase,
        { sales: aggregatePlan.sales, profit: aggregatePlan.profit },
        daysInMonth,
        elapsedDays
    );
    const totalsDisplay = buildDisplayMetrics(totalsBase, totalsDerived);

    return {
        year,
        month,
        key: getMonthKey(year, month),
        label: formatMonthLabel(year, month),
        isCurrent: Boolean(isCurrentMonth),
        hasFacts: Boolean(facts),
        generatedAt: facts?.generatedAt || null,
        startDate: facts?.startDate || getMonthStartDate(year, month).toISOString().slice(0, 10),
        endDate: facts?.endDate || getMonthEndDate(year, month).toISOString().slice(0, 10),
        daysInMonth,
        elapsedDays,
        sources: sourceRows,
        totals: {
            base: totalsBase,
            derived: totalsDerived,
            display: totalsDisplay
        },
        planConfig: {
            sources: planSources
        }
    };
}

export async function rebuildMonthlyFacts(year, month, { asOf, reportJob } = {}) {
    const facts = await collectMonthlyFacts(year, month, { asOf, reportJob });
    await saveMonthlyFacts(year, month, facts);
    return facts;
}

export async function composeMonthlyReportData() {
    const months = await listMonthEntries();
    const now = new Date();
    const currentYear = now.getUTCFullYear();
    const currentMonth = now.getUTCMonth() + 1;
    const reportMonths = [];
    const plansCache = new Map();

    for (const monthEntry of months) {
        const { year, month, key } = monthEntry;
        let plans = plansCache.get(year);
        if (!plans) {
            plans = await loadYearPlans(year);
            plansCache.set(year, plans);
        }
        const monthPlan = getMonthPlan(plans, key.split('-')[1]);
        const facts = await loadMonthlyFacts(year, month);
        const monthReport = buildMonthReportObject({
            year,
            month,
            facts,
            planConfig: monthPlan,
            isCurrentMonth: year === currentYear && month === currentMonth,
            asOfDate: now
        });
        reportMonths.push(monthReport);
    }

    return {
        months: reportMonths,
        generatedAt: reportMonths.find(m => m.isCurrent)?.generatedAt || null
    };
}

export async function updateMonthlyPlans(year, month, planPayload) {
    const yearPlans = await loadYearPlans(year);
    const monthKey = String(month).padStart(2, '0');

    const sanitizedSources = {};
    const sourcePayload = planPayload.sources || {};
    Object.entries(sourcePayload).forEach(([ident, values]) => {
        if (!values) {
            return;
        }
        const entry = {};
        if (Object.prototype.hasOwnProperty.call(values, 'sales')) {
            const salesValue = Number(values.sales);
            entry.sales = normalizePlanValue(salesValue);
        }
        if (Object.prototype.hasOwnProperty.call(values, 'profit')) {
            const profitValue = Number(values.profit);
            entry.profit = normalizePlanValue(profitValue);
        }
        if (Object.keys(entry).length > 0) {
            sanitizedSources[ident] = entry;
        }
    });

    const defaultSales = Number(planPayload.default?.sales);
    const defaultProfit = Number(planPayload.default?.profit);
    const normalizedDefault = {
        sales: normalizePlanValue(defaultSales),
        profit: normalizePlanValue(defaultProfit)
    };

    setMonthPlan(yearPlans, monthKey, {
        default: normalizedDefault,
        sources: sanitizedSources
    });

    yearPlans.updatedAt = new Date().toISOString();
    await saveYearPlans(year, yearPlans);
}

export async function getPlansForYear(year) {
    return loadYearPlans(year);
}

export async function getPlansForMonth(year, month) {
    const plans = await loadYearPlans(year);
    const monthKey = String(month).padStart(2, '0');
    return getMonthPlan(plans, monthKey);
}

export async function ensureDataDirectories() {
    await ensureDirectory(MONTHLY_DATA_DIR);
    await ensureDirectory(PLANS_DATA_DIR);
}
