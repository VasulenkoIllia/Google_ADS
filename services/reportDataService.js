import { OAuth2Client } from 'google-auth-library';
import axios from "axios";
import dotenv from 'dotenv';
import { RateLimiter } from '../utils/rateLimiter.js';
import { loadSalesdriveSourcesSync } from './salesdriveSourcesService.js';

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
    PLAN_PROFIT_MONTH,
    SALESDRIVE_RATE_LIMIT_MAX_PER_MINUTE,
    SALESDRIVE_RATE_LIMIT_INTERVAL_MS,
    SALESDRIVE_RATE_LIMIT_QUEUE_SIZE,
    SALESDRIVE_RETRY_MAX_ATTEMPTS,
    SALESDRIVE_RETRY_BASE_DELAY_MS
} = process.env;

function getSalesdriveSourcesInternal() {
    return loadSalesdriveSourcesSync();
}

const DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;
const ZERO_EPSILON = 1e-6;
const DEFAULT_SALESDRIVE_MAX_PER_MINUTE = 20;
const DEFAULT_SALESDRIVE_INTERVAL_MS = 60000;
const DEFAULT_SALESDRIVE_QUEUE_SIZE = 120;
const DEFAULT_SALESDRIVE_RETRY_ATTEMPTS = 3;
const DEFAULT_SALESDRIVE_RETRY_BASE_DELAY_MS = 5000;
const DEFAULT_SALESDRIVE_HOURLY_LIMIT = 200;
const DEFAULT_SALESDRIVE_DAILY_LIMIT = 2000;
export const MIN_PLACEHOLDER_WAIT_SECONDS = 5;

export function getSalesdriveSources() {
    return getSalesdriveSourcesInternal();
}

function parsePositiveInt(value, fallback) {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed) && parsed > 0) {
        return parsed;
    }
    return fallback;
}

function parsePositiveNumber(value, fallback) {
    const parsed = Number.parseFloat(value);
    if (Number.isFinite(parsed) && parsed > 0) {
        return parsed;
    }
    return fallback;
}

function wait(ms) {
    const safeMs = Math.max(0, Number.isFinite(ms) ? ms : 0);
    if (safeMs === 0) {
        return Promise.resolve();
    }
    return new Promise(resolve => {
        setTimeout(resolve, safeMs);
    });
}

function parseRetryAfter(retryAfterHeader) {
    if (retryAfterHeader === undefined || retryAfterHeader === null) {
        return null;
    }
    const value = Array.isArray(retryAfterHeader) ? retryAfterHeader[0] : retryAfterHeader;
    if (value === undefined || value === null) {
        return null;
    }
    const numericDelay = Number.parseFloat(value);
    if (Number.isFinite(numericDelay) && numericDelay >= 0) {
        return numericDelay * 1000;
    }
    const parsedDate = Date.parse(value);
    if (!Number.isNaN(parsedDate)) {
        return Math.max(parsedDate - Date.now(), 0);
    }
    return null;
}

export const SALES_DRIVE_RATE_LIMIT_MAX_REQUESTS = parsePositiveInt(
    SALESDRIVE_RATE_LIMIT_MAX_PER_MINUTE,
    DEFAULT_SALESDRIVE_MAX_PER_MINUTE
);
export const SALES_DRIVE_RATE_LIMIT_INTERVAL = parsePositiveNumber(
    SALESDRIVE_RATE_LIMIT_INTERVAL_MS,
    DEFAULT_SALESDRIVE_INTERVAL_MS
);
const SALES_DRIVE_RATE_LIMIT_QUEUE_SIZE = parsePositiveInt(
    SALESDRIVE_RATE_LIMIT_QUEUE_SIZE,
    DEFAULT_SALESDRIVE_QUEUE_SIZE
);
const SALES_DRIVE_MAX_RETRY_ATTEMPTS = Math.max(
    0,
    parsePositiveInt(SALESDRIVE_RETRY_MAX_ATTEMPTS, DEFAULT_SALESDRIVE_RETRY_ATTEMPTS)
);
const SALES_DRIVE_RETRY_BASE_DELAY_MS = parsePositiveNumber(
    SALESDRIVE_RETRY_BASE_DELAY_MS,
    DEFAULT_SALESDRIVE_RETRY_BASE_DELAY_MS
);
const SALES_DRIVE_HOURLY_WINDOW_MS = 60 * 60 * 1000;
export const SALES_DRIVE_HOURLY_LIMIT = parsePositiveInt(
    process.env.SALESDRIVE_HOURLY_LIMIT,
    DEFAULT_SALESDRIVE_HOURLY_LIMIT
);
const SALES_DRIVE_DAILY_WINDOW_MS = 24 * 60 * 60 * 1000;
export const SALES_DRIVE_DAILY_LIMIT = parsePositiveInt(
    process.env.SALESDRIVE_DAILY_LIMIT,
    DEFAULT_SALESDRIVE_DAILY_LIMIT
);

function alignToHour(timestamp = Date.now()) {
    const aligned = new Date(timestamp);
    aligned.setMinutes(0, 0, 0);
    return aligned.getTime();
}

function alignToDay(timestamp = Date.now()) {
    const aligned = new Date(timestamp);
    aligned.setHours(0, 0, 0, 0);
    return aligned.getTime();
}

const salesDriveRateLimiter = new RateLimiter({
    maxRequests: SALES_DRIVE_RATE_LIMIT_MAX_REQUESTS,
    intervalMs: SALES_DRIVE_RATE_LIMIT_INTERVAL,
    queueLimit: SALES_DRIVE_RATE_LIMIT_QUEUE_SIZE,
    onDelay: (waitMs, queueLength) => {
        console.warn(
            `SalesDrive rate limit reached. Waiting ${Math.ceil(waitMs)} ms before next request (queued: ${queueLength}).`
        );
    }
});

let salesDriveRetryState = { triggered: false, maxDelayMs: 0 };
let salesDriveHourlyWindow = { startedAt: alignToHour(), count: 0 };
let salesDriveDailyWindow = { startedAt: alignToDay(), count: 0 };

function markSalesDriveRetry(delayMs = 0) {
    salesDriveRetryState.triggered = true;
    if (Number.isFinite(delayMs) && delayMs > salesDriveRetryState.maxDelayMs) {
        salesDriveRetryState.maxDelayMs = delayMs;
    }
}

function consumeSalesDriveRetryFlag() {
    const snapshot = {
        triggered: salesDriveRetryState.triggered,
        maxDelayMs: salesDriveRetryState.maxDelayMs
    };
    salesDriveRetryState = { triggered: false, maxDelayMs: 0 };
    return snapshot;
}

export function evaluateRateLimit(extraQueuedRequests = 0) {
    if (typeof salesDriveRateLimiter.estimateWait !== 'function') {
        return null;
    }
    return salesDriveRateLimiter.estimateWait(extraQueuedRequests);
}

function resetHourlyWindowIfNeeded(now = Date.now()) {
    const currentHourStart = alignToHour(now);
    if (currentHourStart !== salesDriveHourlyWindow.startedAt) {
        salesDriveHourlyWindow.startedAt = currentHourStart;
        salesDriveHourlyWindow.count = 0;
    }
}

function noteHourlyRequest() {
    const now = Date.now();
    resetHourlyWindowIfNeeded(now);
    salesDriveHourlyWindow.count += 1;
    return getHourlyStats(now);
}

export function getHourlyStats(now = Date.now()) {
    resetHourlyWindowIfNeeded(now);
    const limit = SALES_DRIVE_HOURLY_LIMIT;
    const used = Math.max(0, Math.min(salesDriveHourlyWindow.count, limit));
    const remaining = Math.max(limit - used, 0);
    const resetAt = salesDriveHourlyWindow.startedAt + SALES_DRIVE_HOURLY_WINDOW_MS;
    const resetInMs = Math.max(resetAt - now, 0);
    return {
        limit,
        used,
        remaining,
        resetAt,
        resetInMs
    };
}

function resetDailyWindowIfNeeded(now = Date.now()) {
    const currentDayStart = alignToDay(now);
    if (currentDayStart !== salesDriveDailyWindow.startedAt) {
        salesDriveDailyWindow.startedAt = currentDayStart;
        salesDriveDailyWindow.count = 0;
    }
}

function noteDailyRequest() {
    const now = Date.now();
    resetDailyWindowIfNeeded(now);
    salesDriveDailyWindow.count += 1;
    return getDailyStats(now);
}

export function getDailyStats(now = Date.now()) {
    resetDailyWindowIfNeeded(now);
    const limit = SALES_DRIVE_DAILY_LIMIT;
    const used = Math.max(0, Math.min(salesDriveDailyWindow.count, limit));
    const remaining = Math.max(limit - used, 0);
    const resetAt = salesDriveDailyWindow.startedAt + SALES_DRIVE_DAILY_WINDOW_MS;
    const resetInMs = Math.max(resetAt - now, 0);
    return {
        limit,
        used,
        remaining,
        resetAt,
        resetInMs
    };
}

function reportJobPushProgress(job, patch = {}) {
    if (!job || typeof job.updateProgress !== 'function') {
        return;
    }
    const hourlyStats = getHourlyStats();
    const dailyStats = getDailyStats();
    job.updateProgress({
        ...patch,
        hourlyLimit: hourlyStats.limit,
        hourlyUsed: hourlyStats.used,
        hourlyRemaining: hourlyStats.remaining,
        hourlyResetMs: hourlyStats.resetInMs,
        hourlyResetSeconds: Math.ceil(hourlyStats.resetInMs / 1000),
        hourlyResetAt: hourlyStats.resetAt,
        dailyLimit: dailyStats.limit,
        dailyUsed: dailyStats.used,
        dailyRemaining: dailyStats.remaining,
        dailyResetMs: dailyStats.resetInMs,
        dailyResetSeconds: Math.ceil(dailyStats.resetInMs / 1000),
        dailyResetAt: dailyStats.resetAt
    });
}

export class DateRangeError extends Error {
    constructor(message) {
        super(message);
        this.name = 'DateRangeError';
        this.statusCode = 400;
    }
}

const REPORT_JOB_TTL_MS = 5 * 60 * 1000;
const REPORT_JOB_ERROR_TTL_MS = 30 * 1000;
const reportJobs = new Map();

export function buildReportJobKey({
    startDate,
    endDate,
    selectedSourceIds = [],
    salesDriveFilter = {},
    salesDriveLimit = null,
    planOverrides = {},
    reportType = 'summary'
}) {
    const normalizedSources = [...selectedSourceIds].map(id => id != null ? id.toString() : '').sort();
    const normalizedFilter = salesDriveFilter && typeof salesDriveFilter === 'object'
        ? salesDriveFilter
        : {};
    const normalizedPlan = planOverrides && typeof planOverrides === 'object'
        ? planOverrides
        : {};

    return JSON.stringify({
        startDate,
        endDate,
        sources: normalizedSources,
        filter: normalizedFilter,
        limit: salesDriveLimit ?? null,
        plan: normalizedPlan,
        reportType
    });
}

function createReportJob(key, workFactory) {
    const job = {
        status: 'pending',
        createdAt: Date.now(),
        updatedAt: Date.now(),
        progress: {},
        waitMs: null,
        promise: null,
        result: null,
        error: null,
        cleanupTimer: null
    };

    job.updateProgress = (patch = {}) => {
        if (!patch || typeof patch !== 'object') {
            return;
        }
        job.progress = { ...job.progress, ...patch };
        if (patch.waitMs !== undefined && Number.isFinite(patch.waitMs)) {
            job.waitMs = Math.max(0, patch.waitMs);
        }
        job.updatedAt = Date.now();
    };

    job.promise = (async () => {
        try {
            const result = await workFactory(job);
            job.result = result;
            job.status = 'ready';
            job.waitMs = 0;
            reportJobPushProgress(job, { waitMs: 0, queueAhead: 0, message: 'Звіт сформовано' });
            job.cleanupTimer = setTimeout(() => {
                if (reportJobs.get(key) === job) {
                    reportJobs.delete(key);
                }
            }, REPORT_JOB_TTL_MS);
            return result;
        } catch (err) {
            job.error = err;
            job.status = 'error';
            reportJobPushProgress(job, { message: err?.message || 'Помилка формування звіту', waitMs: null });
            job.cleanupTimer = setTimeout(() => {
                if (reportJobs.get(key) === job) {
                    reportJobs.delete(key);
                }
            }, REPORT_JOB_ERROR_TTL_MS);
            return null;
        }
    })();

    return job;
}

export function getOrCreateReportJob(key, workFactory) {
    let job = reportJobs.get(key);
    if (job) {
        const age = Date.now() - job.updatedAt;
        const shouldRefresh = (job.status === 'ready' && age > REPORT_JOB_TTL_MS)
            || (job.status === 'error' && age > REPORT_JOB_ERROR_TTL_MS);
        if (shouldRefresh) {
            if (job.cleanupTimer) {
                clearTimeout(job.cleanupTimer);
            }
            reportJobs.delete(key);
            job = null;
        }
    }

    if (!job) {
        job = createReportJob(key, workFactory);
        reportJobs.set(key, job);
    }

    return job;
}

function coerceQueryParam(value) {
    if (Array.isArray(value)) {
        return value[0];
    }
    return value;
}

export function resolveDateRange(requestedStart, requestedEnd) {
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

export function normalizeToArray(value) {
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

export function sanitizeSalesDriveFilter(rawFilter) {
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

export async function getGoogleAdsData({ startDate, endDate }) {
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

async function rateLimitedSalesDriveRequest(params, context = {}, attempt = 1) {
    const {
        extraQueuedRequests = 0,
        sourceIdent,
        sourceId,
        remainingSources,
        reason,
        reportJob
    } = context || {};

    const sanitizedQueuedRequests = Math.max(extraQueuedRequests, 0);
    const baselineEstimate = evaluateRateLimit(sanitizedQueuedRequests) || {};

    reportJobPushProgress(reportJob, {
        waitMs: baselineEstimate.waitMs ?? 0,
        queueAhead: baselineEstimate.queueAhead ?? salesDriveRateLimiter.pendingRequests ?? 0,
        extraQueuedRequests: sanitizedQueuedRequests,
        estimatedTotalRequests: (baselineEstimate.queueAhead ?? salesDriveRateLimiter.pendingRequests ?? 0) + sanitizedQueuedRequests + 1,
        remainingSources,
        sourceIdent,
        sourceId,
        reason,
        intervalMs: baselineEstimate.intervalMs ?? SALES_DRIVE_RATE_LIMIT_INTERVAL,
        maxPerInterval: baselineEstimate.maxRequests ?? SALES_DRIVE_RATE_LIMIT_MAX_REQUESTS,
        attempt
    });

    const immutableParams = {
        ...params,
        filter: isPlainObject(params.filter) ? { ...params.filter } : params.filter
    };
    const requestConfig = {
        headers: { 'Form-Api-Key': SALESDRIVE_API_KEY },
        params: immutableParams,
        timeout: 30000
    };

    const executeRequest = async () => {
        const hourlyStats = noteHourlyRequest();
        const dailyStats = noteDailyRequest();
        reportJobPushProgress(reportJob, {
            message: sourceIdent ? `Отримуємо дані для ${sourceIdent}` : 'Виконуємо запит до SalesDrive',
            waitMs: baselineEstimate.waitMs ?? 0,
            queueAhead: baselineEstimate.queueAhead ?? salesDriveRateLimiter.pendingRequests ?? 0,
            extraQueuedRequests: sanitizedQueuedRequests,
            estimatedTotalRequests: (baselineEstimate.queueAhead ?? salesDriveRateLimiter.pendingRequests ?? 0) + sanitizedQueuedRequests + 1,
            remainingSources,
            sourceIdent,
            sourceId,
            reason,
            intervalMs: baselineEstimate.intervalMs ?? SALES_DRIVE_RATE_LIMIT_INTERVAL,
            maxPerInterval: baselineEstimate.maxRequests ?? SALES_DRIVE_RATE_LIMIT_MAX_REQUESTS,
            hourlyLimit: hourlyStats.limit,
            hourlyRemaining: hourlyStats.remaining,
            hourlyResetMs: hourlyStats.resetInMs,
            hourlyResetSeconds: Math.ceil(hourlyStats.resetInMs / 1000),
            dailyLimit: dailyStats.limit,
            dailyRemaining: dailyStats.remaining,
            dailyResetMs: dailyStats.resetInMs,
            dailyResetSeconds: Math.ceil(dailyStats.resetInMs / 1000)
        });

        const response = await axios.get(SALESDRIVE_URL, requestConfig);

        const postEstimate = evaluateRateLimit(Math.max(sanitizedQueuedRequests - 1, 0)) || {};
        reportJobPushProgress(reportJob, {
            waitMs: postEstimate.waitMs ?? 0,
            queueAhead: postEstimate.queueAhead ?? salesDriveRateLimiter.pendingRequests ?? 0,
            extraQueuedRequests: sanitizedQueuedRequests,
            estimatedTotalRequests: (postEstimate.queueAhead ?? salesDriveRateLimiter.pendingRequests ?? 0) + sanitizedQueuedRequests + 1,
            remainingSources,
            sourceIdent,
            sourceId,
            reason,
            intervalMs: postEstimate.intervalMs ?? SALES_DRIVE_RATE_LIMIT_INTERVAL,
            maxPerInterval: postEstimate.maxRequests ?? SALES_DRIVE_RATE_LIMIT_MAX_REQUESTS
        });

        return response;
    };

    try {
        const response = await salesDriveRateLimiter.schedule(executeRequest);
        return response;
    } catch (error) {
        const status = error.response?.status;
        const retryAfterHeader = error.response?.headers?.['retry-after'];
        const retryAfterMs = parseRetryAfter(retryAfterHeader);
        const nextAttempt = attempt + 1;
        const networkError = error.code && typeof error.code === 'string';
        const retryableStatus = status === 429 || status === 500 || status === 502 || status === 503 || status === 504;
        const shouldRetry = attempt < SALES_DRIVE_MAX_RETRY_ATTEMPTS
            && (retryableStatus || networkError || retryAfterMs !== null);

        if (shouldRetry) {
            const backoffMultiplier = Math.pow(2, attempt - 1);
            const baseDelay = SALES_DRIVE_RETRY_BASE_DELAY_MS * backoffMultiplier;
            const chosenDelay = retryAfterMs !== null ? retryAfterMs : baseDelay;
            const boundedDelay = Math.max(1000, Math.min(chosenDelay, SALES_DRIVE_RATE_LIMIT_INTERVAL));

            console.warn(
                `SalesDrive request attempt ${attempt} failed with status ${status || error.code || 'unknown'}; retrying in ${Math.ceil(boundedDelay)} ms (attempt ${nextAttempt}/${SALES_DRIVE_MAX_RETRY_ATTEMPTS}).`
            );
            markSalesDriveRetry(boundedDelay);
            reportJobPushProgress(reportJob, {
                waitMs: boundedDelay,
                queueAhead: salesDriveRateLimiter.pendingRequests ?? 0,
                extraQueuedRequests: sanitizedQueuedRequests,
                estimatedTotalRequests: (salesDriveRateLimiter.pendingRequests ?? 0) + sanitizedQueuedRequests + 1,
                remainingSources,
                sourceIdent,
                sourceId,
                reason,
                status,
                message: `Очікуємо повторний запит через ${Math.ceil(boundedDelay / 1000)} с`
            });
            await wait(boundedDelay);
            return rateLimitedSalesDriveRequest(params, context, nextAttempt);
        }

        if (retryableStatus || retryAfterMs !== null) {
            if (attempt < SALES_DRIVE_MAX_RETRY_ATTEMPTS) {
                const waitMsCandidate = retryAfterMs !== null
                    ? retryAfterMs
                    : SALES_DRIVE_RATE_LIMIT_INTERVAL;
                markSalesDriveRetry(waitMsCandidate);
                reportJobPushProgress(reportJob, {
                    waitMs: waitMsCandidate,
                    queueAhead: salesDriveRateLimiter.pendingRequests ?? 0,
                    extraQueuedRequests: sanitizedQueuedRequests,
                    estimatedTotalRequests: (salesDriveRateLimiter.pendingRequests ?? 0) + sanitizedQueuedRequests + 1,
                    remainingSources,
                    sourceIdent,
                    sourceId,
                    reason,
                    status,
                    message: `SalesDrive наклав обмеження, очікуємо ${Math.ceil(waitMsCandidate / 1000)} с`
                });
                await wait(waitMsCandidate);
                return rateLimitedSalesDriveRequest(params, context, nextAttempt);
            }
            console.warn(`SalesDrive повернув статус ${status}; досягнуто межі повторів (${SALES_DRIVE_MAX_RETRY_ATTEMPTS}).`);
        }

        throw error;
    }
}
async function getSalesDriveDataForSource(sourceId, { startDate, endDate }, overrides = {}, runtimeMeta = {}) {
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
    const {
        rateLimitContext: incomingRateLimitContext = {},
        reportJob
    } = runtimeMeta || {};
    const remainingSourcesAfterCurrent = Math.max(incomingRateLimitContext.remainingSourcesAfterCurrent || 0, 0);
    const rateLimitContext = {
        ...incomingRateLimitContext,
        sourceId,
        sourceIdent: incomingRateLimitContext.sourceIdent || String(sourceId),
        remainingSourcesAfterCurrent,
        remainingSources: Math.max(incomingRateLimitContext.remainingSources ?? (remainingSourcesAfterCurrent + 1), 0),
        extraQueuedRequests: incomingRateLimitContext.extraQueuedRequests !== undefined
            ? Math.max(incomingRateLimitContext.extraQueuedRequests, 0)
            : remainingSourcesAfterCurrent,
        reason: incomingRateLimitContext.reason || `source:${incomingRateLimitContext.sourceIdent || sourceId}`,
        reportJob
    };
    let remainingPagesEstimate = 0;

    try {
        // Paginate until the API signals no more data
        while (true) {
            rateLimitContext.currentPage = page;
            rateLimitContext.pendingPagesForSource = Math.max(remainingPagesEstimate, 0);
            rateLimitContext.extraQueuedRequests = Math.max(
                remainingSourcesAfterCurrent + Math.max(remainingPagesEstimate, 0),
                0
            );

            const params = {
                page,
                limit,
                filter: effectiveFilter,
            };

            const response = await rateLimitedSalesDriveRequest(params, rateLimitContext);

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
            const totalPagesHintRaw = pagination.totalPages ?? pagination.pageCount ?? pagination.lastPage ?? null;
            const totalPagesHint = parsePositiveInt(totalPagesHintRaw, null);
            const coveredAllHintedItems = totalItemsHint ? aggregatedOrders.length >= totalItemsHint : false;
            const exhaustedPageHints = totalPagesHint ? page >= Number(totalPagesHint) : false;

            if (Number.isFinite(totalPagesHint) && totalPagesHint > 0) {
                const remainingPages = Math.max(totalPagesHint - page, 0);
                remainingPagesEstimate = remainingPages;
                rateLimitContext.pendingPagesForSource = remainingPages;
                rateLimitContext.extraQueuedRequests = Math.max(
                    remainingSourcesAfterCurrent + remainingPages,
                    0
                );
                reportJobPushProgress(reportJob, {
                    pendingPagesForSource: remainingPages,
                    remainingSources: rateLimitContext.remainingSources,
                    remainingSourcesAfterCurrent
                });
            } else {
                remainingPagesEstimate = 0;
                rateLimitContext.pendingPagesForSource = 0;
                rateLimitContext.extraQueuedRequests = Math.max(remainingSourcesAfterCurrent, 0);
                reportJobPushProgress(reportJob, {
                    pendingPagesForSource: 0,
                    remainingSources: rateLimitContext.remainingSources,
                    remainingSourcesAfterCurrent
                });
            }

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
        reportJobPushProgress(reportJob, {
            message: rateLimitContext.sourceIdent
                ? `SalesDrive (${rateLimitContext.sourceIdent}): ${apiMessage}`
                : `SalesDrive: ${apiMessage}`,
            waitMs: getHourlyStats().resetInMs,
            queueAhead: salesDriveRateLimiter.pendingRequests ?? 0,
            extraQueuedRequests: rateLimitContext.extraQueuedRequests ?? 0,
            remainingSources: rateLimitContext.remainingSources,
            remainingSourcesAfterCurrent,
            sourceIdent: rateLimitContext.sourceIdent,
            sourceId,
            status: error.response?.status || 'error'
        });
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


async function buildReportData(
    { startDate, endDate, selectedSourceIds = [], salesDriveRequestOptions = {}, planOverrides = {} },
    { reportJob } = {}
) {
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
    let rateLimitCooldownSeconds = null;
    let allOrders = [];
    let salesDriveTotals = {
        totalPaymentAmount: 0,
        totalCostPriceAmount: 0,
        totalProfitAmount: 0,
        totalTransactions: 0,
    };
    const perSourceSalesTotals = {};

    const allConfiguredSources = getSalesdriveSources();
    let sourcesToProcess = allConfiguredSources;
    if (isFiltering) {
        sourcesToProcess = allConfiguredSources.filter(s => selectedIdSet.has(s.id.toString()));
        if (sourcesToProcess.length === 0) {
            console.warn('Selected sources were not found. Falling back to all configured sources.');
            sourcesToProcess = allConfiguredSources;
        }
    }
    const sourceNameMap = new Map(
        allConfiguredSources.map(source => [source.ident, source.nameView || source.name || source.ident])
    );

    const initialExtraRequests = Math.max(sourcesToProcess.length - 1, 0);
    const initialEstimate = evaluateRateLimit(initialExtraRequests) || {};
    const initialWaitMs = Number.isFinite(initialEstimate.waitMs)
        ? initialEstimate.waitMs
        : SALES_DRIVE_RATE_LIMIT_INTERVAL;
    const initialQueueAhead = Number.isFinite(initialEstimate.queueAhead)
        ? initialEstimate.queueAhead
        : salesDriveRateLimiter.pendingRequests ?? 0;
    reportJobPushProgress(reportJob, {
        message: `Починаємо формування звіту (${sourcesToProcess.length} джерел)`,
        sourceIdent: null,
        sourceId: null,
        remainingSources: sourcesToProcess.length,
        remainingSourcesAfterCurrent: Math.max(sourcesToProcess.length - 1, 0),
        intervalMs: SALES_DRIVE_RATE_LIMIT_INTERVAL,
        maxPerInterval: SALES_DRIVE_RATE_LIMIT_MAX_REQUESTS,
        queueAhead: initialQueueAhead,
        extraQueuedRequests: initialExtraRequests,
        estimatedTotalRequests: initialQueueAhead + initialExtraRequests + 1,
        waitMs: initialWaitMs
    });

    for (let sourceIndex = 0; sourceIndex < sourcesToProcess.length; sourceIndex += 1) {
        const source = sourcesToProcess[sourceIndex];
        console.log(`\nProcessing source: ${source.ident} (ID: ${source.id})`);
        const remainingSourcesAfterCurrent = sourcesToProcess.length - sourceIndex - 1;
        const rateLimitContext = {
            sourceIdent: source.ident,
            sourceId: source.id,
            remainingSources: sourcesToProcess.length - sourceIndex,
            remainingSourcesAfterCurrent,
            extraQueuedRequests: Math.max(remainingSourcesAfterCurrent, 0),
            reason: `source:${source.ident}`
        };
        reportJobPushProgress(reportJob, {
            message: `Збираємо дані для ${source.ident}`,
            sourceIdent: source.ident,
            sourceId: source.id,
            remainingSources: sourcesToProcess.length - sourceIndex,
            remainingSourcesAfterCurrent
        });
        const { orders, totals, errors: sourceErrors } = await getSalesDriveDataForSource(
            source.id,
            { startDate, endDate },
            salesDriveRequestOptions,
            { rateLimitContext, reportJob }
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

        reportJobPushProgress(reportJob, {
            message: `Обробляємо замовлення для ${source.ident}`,
            sourceIdent: source.ident,
            sourceId: source.id,
            remainingSources: sourcesToProcess.length - sourceIndex,
            remainingSourcesAfterCurrent,
            pendingPagesForSource: rateLimitContext.pendingPagesForSource ?? 0
        });

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

        reportJobPushProgress(reportJob, {
            message: `Завершено джерело ${source.ident}`,
            sourceIdent: source.ident,
            sourceId: source.id,
            remainingSources: Math.max(remainingSourcesAfterCurrent, 0),
            remainingSourcesAfterCurrent: Math.max(remainingSourcesAfterCurrent, 0),
            pendingPagesForSource: 0
        });
    }

    reportJobPushProgress(reportJob, {
        message: 'Фіналізуємо звіт',
        sourceIdent: null,
        sourceId: null,
        remainingSources: 0,
        remainingSourcesAfterCurrent: 0,
        pendingPagesForSource: 0,
        waitMs: 0,
        queueAhead: 0
    });

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
            sourceNameView: sourceNameMap.get(ident) || ident,
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
        sourceNameView: sourceNameMap.get(order.istocnikProdaziIdent) || order.istocnikProdaziIdent,
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
            sourceNameView: sourceNameMap.get(order.istocnikProdaziIdent) || order.istocnikProdaziIdent,
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
            name: source.nameView || source.name || ident,
            nameView: source.nameView || source.name || ident,
            summary,
            salesTotals: salesTotalsForSource,
            googleTotals: googleTotalsForSource,
        };
    });

    const retrySnapshot = consumeSalesDriveRetryFlag();
    if (retrySnapshot.triggered) {
        rateLimitCooldown = true;
        const candidateSeconds = Math.ceil((retrySnapshot.maxDelayMs || SALES_DRIVE_RETRY_BASE_DELAY_MS) / 1000);
        rateLimitCooldownSeconds = Math.max(rateLimitCooldownSeconds || 0, candidateSeconds);
    }

    const limiterSnapshot = salesDriveRateLimiter.consumeCooldownFlag();
    if (limiterSnapshot.triggered) {
        rateLimitCooldown = true;
        const candidateSeconds = Math.ceil((limiterSnapshot.delayMs || SALES_DRIVE_RATE_LIMIT_INTERVAL) / 1000);
        rateLimitCooldownSeconds = Math.max(rateLimitCooldownSeconds || 0, candidateSeconds);
    }

    if (rateLimitCooldownSeconds !== null && rateLimitCooldownSeconds < 1) {
        rateLimitCooldownSeconds = 1;
    }

    const hourlyStats = getHourlyStats();
    const dailyStats = getDailyStats();

    if (dailyStats.remaining === 0) {
        rateLimitCooldown = true;
        const resetSeconds = Math.ceil(dailyStats.resetInMs / 1000);
        rateLimitCooldownSeconds = Math.max(rateLimitCooldownSeconds || 0, resetSeconds);
    }

    console.log(`\n--- Data Preparation Complete ---`);
    return {
        googleAdsData: googleAdsDataForTable,
        googleAdsTotals,
        salesDriveData,
        salesDriveTotals,
        combinedData,
        summaryReport,
        sourceSummaries,
        alerts,
        rateLimitCooldown,
        rateLimitCooldownSeconds,
        hourlyStats,
        dailyStats
    };
}

function parsePlanInputValue(value) {
    if (value === undefined || value === null || value === '') {
        return null;
    }
    const parsed = parseFloat(value);
    return Number.isFinite(parsed) ? parsed : null;
}

export function resolvePlanConfig(planSalesRaw, planProfitRaw) {
    const fallbackPlanSales = parseFloat(PLAN_SALES_MONTH || '0') || 0;
    const fallbackPlanProfit = parseFloat(PLAN_PROFIT_MONTH || '0') || 0;

    const planOverrides = {};
    const parsedSales = parsePlanInputValue(planSalesRaw);
    const parsedProfit = parsePlanInputValue(planProfitRaw);

    if (parsedSales !== null) {
        planOverrides.sales = parsedSales;
    }
    if (parsedProfit !== null) {
        planOverrides.profit = parsedProfit;
    }

    const planInputs = {
        sales: planSalesRaw !== undefined ? planSalesRaw : '',
        profit: planProfitRaw !== undefined ? planProfitRaw : ''
    };

    const planDefaults = {
        sales: fallbackPlanSales,
        profit: fallbackPlanProfit
    };

    return {
        planOverrides,
        planInputs,
        planDefaults
    };
}

export function getRateLimitMeta() {
    return {
        minuteLimit: SALES_DRIVE_RATE_LIMIT_MAX_REQUESTS,
        minuteIntervalSeconds: Math.ceil(SALES_DRIVE_RATE_LIMIT_INTERVAL / 1000),
        hourlyLimit: SALES_DRIVE_HOURLY_LIMIT,
        dailyLimit: SALES_DRIVE_DAILY_LIMIT
    };
}

export function getSalesDriveLimiterState() {
    return {
        pendingRequests: salesDriveRateLimiter.pendingRequests,
        isCoolingDown: salesDriveRateLimiter.isCoolingDown,
        downstreamDelayMs: salesDriveRateLimiter.downstreamDelayMs
    };
}

export function resolveSourcesForRequest(selectedSourceIds = []) {
    const selectedIdSet = new Set((selectedSourceIds || []).map(id => id != null ? id.toString() : ''));
    const allSources = getSalesdriveSources();

    if (selectedIdSet.size === 0) {
        return { sourcesToProcess: allSources, selectedIdSet, isFiltering: false };
    }

    const filtered = allSources.filter(source => selectedIdSet.has(source?.id?.toString()));
    if (filtered.length === 0) {
        return { sourcesToProcess: allSources, selectedIdSet, isFiltering: true, fellBackToAll: true };
    }

    return { sourcesToProcess: filtered, selectedIdSet, isFiltering: true, fellBackToAll: false };
}

export function shouldProcessDirectly(selectedSourceIds = []) {
    const { sourcesToProcess } = resolveSourcesForRequest(selectedSourceIds);
    const estimatedMinuteRequests = Math.max(sourcesToProcess.length, 1);
    const limiterState = getSalesDriveLimiterState();
    const hourlyStatsBefore = getHourlyStats();
    const dailyStatsBefore = getDailyStats();
    const minuteLimit = SALES_DRIVE_RATE_LIMIT_MAX_REQUESTS;

    const canProcessDirect = estimatedMinuteRequests <= minuteLimit
        && hourlyStatsBefore.remaining >= estimatedMinuteRequests
        && dailyStatsBefore.remaining >= estimatedMinuteRequests
        && limiterState.pendingRequests === 0;

    return {
        canProcessDirect,
        estimatedMinuteRequests,
        limiterState,
        hourlyStatsBefore,
        dailyStatsBefore
    };
}

export async function waitForSalesDriveIdle() {
    await salesDriveRateLimiter.waitForAll();
}

export function buildOverlayMeta(overrides = {}) {
    const {
        extraQueuedRequests = 0,
        waitMs,
        queueAhead,
        estimatedTotalRequests,
        remainingSources,
        hourlyStats,
        dailyStats,
        rateLimitMeta
    } = overrides || {};

    const effectiveRateLimitMeta = rateLimitMeta || getRateLimitMeta();
    const limiterState = getSalesDriveLimiterState();
    const estimate = evaluateRateLimit(
        Number.isFinite(extraQueuedRequests) && extraQueuedRequests >= 0 ? extraQueuedRequests : 0
    ) || {};

    const calculatedWaitMs = Number.isFinite(waitMs)
        ? waitMs
        : (Number.isFinite(estimate.waitMs) ? estimate.waitMs : null);
    const waitSeconds = Number.isFinite(calculatedWaitMs)
        ? Math.max(Math.ceil(calculatedWaitMs / 1000), 0)
        : null;

    const effectiveHourlyStats = hourlyStats || getHourlyStats();
    const effectiveDailyStats = dailyStats || getDailyStats();

    const resolvedQueueAhead = Number.isFinite(queueAhead)
        ? queueAhead
        : (Number.isFinite(estimate.queueAhead)
            ? estimate.queueAhead
            : (Number.isFinite(limiterState.pendingRequests) ? limiterState.pendingRequests : null));

    const resolvedEstimatedTotal = Number.isFinite(estimatedTotalRequests)
        ? estimatedTotalRequests
        : (Number.isFinite(estimate.estimatedTotalRequests)
            ? estimate.estimatedTotalRequests
            : (resolvedQueueAhead !== null ? resolvedQueueAhead + 1 : null));

    const hourlyRemaining = Number.isFinite(overrides.hourlyRemaining)
        ? overrides.hourlyRemaining
        : (Number.isFinite(effectiveHourlyStats?.remaining) ? effectiveHourlyStats.remaining : null);
    const hourlyResetSeconds = Number.isFinite(overrides.hourlyResetSeconds)
        ? overrides.hourlyResetSeconds
        : (Number.isFinite(effectiveHourlyStats?.resetInMs)
            ? Math.max(Math.ceil(effectiveHourlyStats.resetInMs / 1000), 0)
            : null);

    const dailyRemaining = Number.isFinite(overrides.dailyRemaining)
        ? overrides.dailyRemaining
        : (Number.isFinite(effectiveDailyStats?.remaining) ? effectiveDailyStats.remaining : null);
    const dailyResetSeconds = Number.isFinite(overrides.dailyResetSeconds)
        ? overrides.dailyResetSeconds
        : (Number.isFinite(effectiveDailyStats?.resetInMs)
            ? Math.max(Math.ceil(effectiveDailyStats.resetInMs / 1000), 0)
            : null);

    return {
        rateLimitMeta: effectiveRateLimitMeta,
        waitSeconds,
        queueAhead: resolvedQueueAhead,
        estimatedTotalRequests: resolvedEstimatedTotal,
        remainingSources: Number.isFinite(remainingSources) ? remainingSources : null,
        hourlyRemaining,
        hourlyResetSeconds,
        dailyRemaining,
        dailyResetSeconds,
        message: typeof overrides.message === 'string' ? overrides.message : null,
        sourceIdent: typeof overrides.sourceIdent === 'string' ? overrides.sourceIdent : null,
        reloadUrl: typeof overrides.reloadUrl === 'string' ? overrides.reloadUrl : null
    };
}

export {
    buildReportData
};
