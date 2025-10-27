#!/usr/bin/env node

import 'dotenv/config';
import axios from 'axios';

const { SALESDRIVE_URL, SALESDRIVE_API_KEY } = process.env;

function usage() {
    console.log('Usage: node scripts/fetchSalesdriveStatuses.js [--start=YYYY-MM-DD] [--end=YYYY-MM-DD] [--limit=NN] [--recent=NN]');
    console.log('Defaults: start = first day of current year, end = today, limit = 100. Use --recent to cap total orders.');
}

function parseArgs(argv) {
    const parsed = {};
    for (const raw of argv) {
        if (raw === '--help' || raw === '-h') {
            parsed.help = true;
            continue;
        }
        const [key, value] = raw.split('=');
        if (!value) {
            continue;
        }
        if (key === '--start') {
            parsed.start = value;
        } else if (key === '--end') {
            parsed.end = value;
        } else if (key === '--limit') {
            const limit = Number.parseInt(value, 10);
            if (Number.isFinite(limit) && limit > 0) {
                parsed.limit = limit;
            }
        } else if (key === '--max' || key === '--recent') {
            const maxOrders = Number.parseInt(value, 10);
            if (Number.isFinite(maxOrders) && maxOrders > 0) {
                parsed.maxOrders = maxOrders;
            }
        }
    }
    return parsed;
}

function assertEnv() {
    const missing = [];
    if (!SALESDRIVE_URL) missing.push('SALESDRIVE_URL');
    if (!SALESDRIVE_API_KEY) missing.push('SALESDRIVE_API_KEY');
    if (missing.length > 0) {
        throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
    }
}

function formatDate(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

function resolveDateRange(args) {
    const today = new Date();
    const defaultStart = new Date(today.getFullYear(), 0, 1);
    const startRaw = args.start;
    const endRaw = args.end;
    const start = startRaw ? new Date(startRaw) : defaultStart;
    const end = endRaw ? new Date(endRaw) : today;
    if (Number.isNaN(start.valueOf())) {
        throw new Error(`Invalid --start value: ${startRaw}`);
    }
    if (Number.isNaN(end.valueOf())) {
        throw new Error(`Invalid --end value: ${endRaw}`);
    }
    if (start > end) {
        throw new Error('Start date must be before or equal to end date.');
    }
    return {
        start,
        end
    };
}

async function fetchPage({ page, limit, start, end }) {
    const params = {
        page,
        limit,
        filter: {
            orderTime: {
                from: `${start} 00:00:00`,
                to: `${end} 23:59:59`
            }
        }
    };

    const response = await axios.get(SALESDRIVE_URL, {
        params,
        headers: {
            'X-Api-Key': SALESDRIVE_API_KEY
        },
        timeout: 30000,
        validateStatus: () => true
    });

    if (response.status >= 200 && response.status < 300) {
        return response.data || {};
    }

    const error = new Error(`SalesDrive responded with status ${response.status}`);
    error.response = response;
    throw error;
}

function resolveNextPage(currentPage, pagination, receivedCount) {
    const totalPagesRaw = pagination?.totalPages ?? pagination?.lastPage ?? pagination?.pageCount ?? pagination?.pages ?? null;
    const totalPages = Number.parseInt(totalPagesRaw, 10);
    if (Number.isFinite(totalPages) && totalPages > 0 && currentPage >= totalPages) {
        return null;
    }
    if (!receivedCount) {
        return null;
    }
    return currentPage + 1;
}

function delay(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

function parseRateLimitDelay(source) {
    if (source === undefined || source === null) {
        return null;
    }
    if (typeof source === 'number' && Number.isFinite(source)) {
        return source >= 0 ? source : null;
    }
    if (typeof source !== 'string') {
        return null;
    }
    const trimmed = source.trim();
    const numeric = Number.parseFloat(trimmed);
    if (Number.isFinite(numeric)) {
        return Math.max(0, numeric * 1000);
    }
    const periodMatch = /API\s+period:\s*(\d+)\s*(second|seconds|minute|minutes|hour|hours)/i.exec(trimmed)
        || /(\d+)\s*(second|seconds|minute|minutes|hour|hours)/i.exec(trimmed);
    if (periodMatch) {
        const value = Number.parseInt(periodMatch[1], 10);
        if (!Number.isFinite(value)) {
            return null;
        }
        const unit = periodMatch[2].toLowerCase();
        if (unit.startsWith('second')) {
            return Math.max(0, value * 1000);
        }
        if (unit.startsWith('minute')) {
            return Math.max(0, value * 60 * 1000);
        }
        if (unit.startsWith('hour')) {
            return Math.max(0, value * 60 * 60 * 1000);
        }
    }
    const parsedDate = Date.parse(trimmed);
    if (!Number.isNaN(parsedDate)) {
        return Math.max(parsedDate - Date.now(), 0);
    }
    return null;
}

async function main() {
    const args = parseArgs(process.argv.slice(2));
    if (args.help) {
        usage();
        process.exit(0);
    }

    try {
        assertEnv();
    } catch (error) {
        console.error(error.message);
        process.exit(1);
    }

    let dateRange;
    try {
        dateRange = resolveDateRange(args);
    } catch (error) {
        console.error(error.message);
        process.exit(1);
    }

    const statusRegistry = new Map();
    let page = 1;
    let totalOrders = 0;
    const maxOrders = args.maxOrders || null;
    const limit = Math.min(args.limit || 100, maxOrders || Number.MAX_SAFE_INTEGER);
    const sampleLimit = Math.min(maxOrders || 10, 50);
    const interRequestDelayMs = 3100;
    const sampleOrders = [];
    let latestOrder = null;
    let latestOrderScore = Number.NEGATIVE_INFINITY;
    const startDateStr = formatDate(dateRange.start);
    const endDateStr = formatDate(dateRange.end);

    console.log(`Fetching SalesDrive orders from ${startDateStr} to ${endDateStr}...`);

    while (page !== null) {
        try {
            const payload = await fetchPage({
                page,
                limit,
                start: startDateStr,
                end: endDateStr
            });
            const orders = Array.isArray(payload.data) ? payload.data : [];
            const remainingCapacity = maxOrders !== null ? Math.max(maxOrders - totalOrders, 0) : orders.length;
            const effectiveOrders = maxOrders !== null && remainingCapacity < orders.length
                ? orders.slice(0, remainingCapacity)
                : orders;

            effectiveOrders.forEach(order => {
                const rawStatusId = order?.statusId ?? order?.status_id ?? null;
                const statusId = rawStatusId !== null && rawStatusId !== undefined
                    ? String(rawStatusId).trim()
                    : null;
                const statusName = order?.statusName ?? order?.status_name ?? '';
                if (statusId || statusName) {
                    const key = statusId ?? `name:${statusName}`;
                    if (!statusRegistry.has(key)) {
                        statusRegistry.set(key, {
                            id: statusId,
                            name: typeof statusName === 'string' ? statusName.trim() : ''
                        });
                    }
                }
                const score = (() => {
                    const timeValue = Date.parse(order?.orderTime ?? '');
                    if (!Number.isNaN(timeValue)) {
                        return timeValue;
                    }
                    const numericId = Number.parseInt(order?.id, 10);
                    return Number.isFinite(numericId) ? numericId : Number.NEGATIVE_INFINITY;
                })();
                if (score >= latestOrderScore) {
                    latestOrderScore = score;
                    latestOrder = order;
                }
                if (sampleOrders.length < sampleLimit) {
                    sampleOrders.push(order);
                }
            });
            totalOrders += effectiveOrders.length;

            if (maxOrders !== null && totalOrders >= maxOrders) {
                break;
            }

            const pagination = payload.pagination || {};
            const nextPage = resolveNextPage(page, pagination, orders.length);
            if (nextPage === null) {
                break;
            }
            page = nextPage;
            if (interRequestDelayMs > 0) {
                await delay(interRequestDelayMs);
            }
        } catch (error) {
            const status = error.response?.status;
            const payload = error.response?.data;
            const message = typeof payload?.message === 'string' ? payload.message : error.message;
            const rateLimitHit = status === 429
                || (status === 400 && typeof message === 'string' && message.toLowerCase().includes('api limit'));
            if (rateLimitHit) {
                const retryAfterHeader = error.response?.headers?.['retry-after'];
                const waitMs = parseRateLimitDelay(retryAfterHeader)
                    ?? parseRateLimitDelay(message)
                    ?? 60_000;
                const waitSeconds = Math.max(1, Math.ceil(waitMs / 1000));
                console.warn(`Rate limit reached (page ${page}, status ${status || 'unknown'}). Waiting ${waitSeconds} s before retrying...`);
                await delay(waitMs);
                continue;
            }
            console.error(`Request failed on page ${page} (status ${status || 'unknown'}).`);
            console.error(payload || error.message);
            process.exit(1);
        }
    }

    const uniqueStatuses = Array.from(statusRegistry.values()).sort((a, b) => {
        const idA = a.id !== null ? a.id : `~${a.name}`;
        const idB = b.id !== null ? b.id : `~${b.name}`;
        return String(idA).localeCompare(String(idB), 'ru');
    });

    console.log(`\nCollected ${uniqueStatuses.length} unique statuses from ${totalOrders} orders:`);
    uniqueStatuses.forEach((status, index) => {
        const labelId = status.id !== null && status.id !== undefined ? `[${status.id}]` : '[—]';
        const labelName = status.name && status.name.length ? status.name : 'Без названия';
        console.log(`${index + 1}. ${labelId} ${labelName}`);
    });

    if (sampleOrders.length > 0) {
        console.log('\nSample orders:');
        sampleOrders.forEach((order, index) => {
            const label = order?.orderTime || `Order #${order?.id ?? 'N/A'}`;
            const statusName = order?.statusName ?? order?.status_name ?? 'N/A';
            const statusId = order?.statusId ?? order?.status_id ?? '—';
            console.log(`${index + 1}. ${label} — [${statusId}] ${statusName}`);
        });
    }

    if (latestOrder) {
        console.log('\nMost recent order payload:');
        console.log(JSON.stringify(latestOrder, null, 2));
    }
}

main();
