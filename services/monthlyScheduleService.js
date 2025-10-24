import fs from 'fs/promises';
import path from 'path';
import {
    ensureDataDirectories,
    rebuildMonthlyFacts,
    listMonthEntries
} from './monthlyReportService.js';

const DATA_ROOT = path.resolve('data');
const CONFIG_DIR = path.join(DATA_ROOT, 'config');
const SCHEDULE_CONFIG_PATH = path.join(CONFIG_DIR, 'monthly-schedule.json');

const DEFAULT_SCHEDULE_CONFIG = {
    enabled: true,
    frequency: 'daily', // 'daily' | 'weekly'
    time: '01:00',
    weekday: 'monday', // only relevant for weekly
    range: {
        type: 'current', // 'current' | 'last' | 'all'
        count: 1
    },
    updatedAt: null,
    lastRunAt: null,
    lastRunStatus: null,
    lastRunSummary: null,
    lastRunError: null
};

const WEEKDAY_INDEX = {
    sunday: 0,
    monday: 1,
    tuesday: 2,
    wednesday: 3,
    thursday: 4,
    friday: 5,
    saturday: 6
};

let schedulerState = {
    timer: null,
    nextRunAt: null,
    isRunning: false
};

async function ensureScheduleConfigFile() {
    await fs.mkdir(CONFIG_DIR, { recursive: true });
    try {
        await fs.access(SCHEDULE_CONFIG_PATH);
    } catch (error) {
        if (error.code === 'ENOENT') {
            await fs.writeFile(SCHEDULE_CONFIG_PATH, JSON.stringify(DEFAULT_SCHEDULE_CONFIG, null, 2), 'utf8');
        } else {
            throw error;
        }
    }
}

function normalizeTimeString(time) {
    if (typeof time !== 'string') {
        return '01:00';
    }
    const match = /^(\d{1,2}):(\d{2})$/.exec(time.trim());
    if (!match) {
        return '01:00';
    }
    let hours = Number.parseInt(match[1], 10);
    let minutes = Number.parseInt(match[2], 10);
    if (!Number.isFinite(hours) || hours < 0 || hours > 23) {
        hours = 1;
    }
    if (!Number.isFinite(minutes) || minutes < 0 || minutes > 59) {
        minutes = 0;
    }
    return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}`;
}

function normalizeWeekday(value) {
    if (typeof value !== 'string') {
        return 'monday';
    }
    const normalized = value.toLowerCase();
    return WEEKDAY_INDEX.hasOwnProperty(normalized) ? normalized : 'monday';
}

function clampRangeCount(count) {
    const parsed = Number.parseInt(count, 10);
    if (!Number.isFinite(parsed) || parsed < 1) {
        return 1;
    }
    if (parsed > 12) {
        return 12;
    }
    return parsed;
}

function normalizeRangeConfig(rangeInput = {}) {
    const typeRaw = typeof rangeInput.type === 'string' ? rangeInput.type.toLowerCase() : 'current';
    if (typeRaw === 'all') {
        return { type: 'all' };
    }
    if (typeRaw === 'last') {
        return { type: 'last', count: clampRangeCount(rangeInput.count) };
    }
    return { type: 'current' };
}

function normalizeScheduleConfig(raw = {}) {
    const normalized = { ...DEFAULT_SCHEDULE_CONFIG, ...raw };
    normalized.enabled = Boolean(raw.enabled ?? true);
    normalized.frequency = raw.frequency === 'weekly' ? 'weekly' : 'daily';
    normalized.time = normalizeTimeString(raw.time ?? DEFAULT_SCHEDULE_CONFIG.time);
    normalized.weekday = normalizeWeekday(raw.weekday ?? DEFAULT_SCHEDULE_CONFIG.weekday);
    normalized.range = normalizeRangeConfig(raw.range);
    normalized.updatedAt = typeof raw.updatedAt === 'string' ? raw.updatedAt : DEFAULT_SCHEDULE_CONFIG.updatedAt;
    normalized.lastRunAt = typeof raw.lastRunAt === 'string' ? raw.lastRunAt : DEFAULT_SCHEDULE_CONFIG.lastRunAt;
    normalized.lastRunStatus = typeof raw.lastRunStatus === 'string' ? raw.lastRunStatus : DEFAULT_SCHEDULE_CONFIG.lastRunStatus;
    normalized.lastRunSummary = raw.lastRunSummary && typeof raw.lastRunSummary === 'object'
        ? raw.lastRunSummary
        : DEFAULT_SCHEDULE_CONFIG.lastRunSummary;
    normalized.lastRunError = typeof raw.lastRunError === 'string' ? raw.lastRunError : DEFAULT_SCHEDULE_CONFIG.lastRunError;
    return normalized;
}

export async function loadMonthlyScheduleConfig() {
    await ensureScheduleConfigFile();
    const raw = await fs.readFile(SCHEDULE_CONFIG_PATH, 'utf8');
    try {
        const parsed = JSON.parse(raw);
        return normalizeScheduleConfig(parsed);
    } catch (error) {
        console.error('[monthlySchedule] Failed to parse config, using defaults:', error);
        return { ...DEFAULT_SCHEDULE_CONFIG };
    }
}

export async function saveMonthlyScheduleConfig(config) {
    const normalized = normalizeScheduleConfig(config);
    normalized.updatedAt = new Date().toISOString();
    await fs.writeFile(SCHEDULE_CONFIG_PATH, JSON.stringify(normalized, null, 2), 'utf8');
    return normalized;
}

function parseTimeParts(timeString) {
    const [hours, minutes] = timeString.split(':').map(part => Number.parseInt(part, 10));
    const validHours = Number.isFinite(hours) ? Math.max(0, Math.min(23, hours)) : 1;
    const validMinutes = Number.isFinite(minutes) ? Math.max(0, Math.min(59, minutes)) : 0;
    return { hours: validHours, minutes: validMinutes };
}

export function computeNextRunDate(config, fromDate = new Date()) {
    const reference = new Date(fromDate);
    const { hours, minutes } = parseTimeParts(config.time);
    if (!config.enabled) {
        return null;
    }
    const candidate = new Date(reference);
    candidate.setSeconds(0, 0);
    candidate.setHours(hours, minutes, 0, 0);

    if (config.frequency === 'weekly') {
        const targetDay = WEEKDAY_INDEX[normalizeWeekday(config.weekday)];
        const currentDay = candidate.getDay();
        let dayDiff = targetDay - currentDay;
        if (dayDiff < 0 || (dayDiff === 0 && candidate <= reference)) {
            dayDiff += 7;
        }
        if (dayDiff !== 0) {
            candidate.setDate(candidate.getDate() + dayDiff);
        } else if (candidate <= reference) {
            candidate.setDate(candidate.getDate() + 7);
        }
        return candidate;
    }

    // Daily by default
    if (candidate <= reference) {
        candidate.setDate(candidate.getDate() + 1);
    }
    return candidate;
}

function computePreviousMonths(count, referenceDate = new Date()) {
    const months = [];
    const current = new Date(Date.UTC(referenceDate.getUTCFullYear(), referenceDate.getUTCMonth(), 1));
    for (let i = 0; i < count; i += 1) {
        const year = current.getUTCFullYear();
        const month = current.getUTCMonth() + 1;
        months.push({ year, month, key: `${year}-${String(month).padStart(2, '0')}` });
        current.setUTCMonth(current.getUTCMonth() - 1);
    }
    return months.reverse();
}

async function resolveMonthsToProcess(config, now = new Date()) {
    const range = config.range || { type: 'current' };
    if (range.type === 'all') {
        return listMonthEntries();
    }
    if (range.type === 'last') {
        const entries = await listMonthEntries();
        if (entries.length === 0) {
            return computePreviousMonths(range.count || 1, now);
        }
        return entries.slice(-Math.min(range.count || 1, entries.length));
    }
    // current month by default
    const currentYear = now.getUTCFullYear();
    const currentMonth = now.getUTCMonth() + 1;
    return [{ year: currentYear, month: currentMonth, key: `${currentYear}-${String(currentMonth).padStart(2, '0')}` }];
}

async function runMonthlyScheduleJobInternal(trigger = 'auto') {
    if (schedulerState.isRunning) {
        return { status: 'busy' };
    }
    schedulerState.isRunning = true;
    const startedAt = new Date();
    const config = await loadMonthlyScheduleConfig();
    const summary = {
        triggeredBy: trigger,
        startedAt: startedAt.toISOString(),
        processed: [],
        errors: []
    };

    try {
        await ensureDataDirectories();
        const months = await resolveMonthsToProcess(config, startedAt);
        for (const monthInfo of months) {
            try {
                await rebuildMonthlyFacts(monthInfo.year, monthInfo.month, { asOf: startedAt });
                summary.processed.push(monthInfo.key);
            } catch (error) {
                console.error(`[monthlySchedule] Failed to rebuild ${monthInfo.key}:`, error);
                summary.errors.push({
                    key: monthInfo.key,
                    message: error.message
                });
            }
        }

        const updatedConfig = {
            ...config,
            lastRunAt: new Date().toISOString(),
            lastRunStatus: summary.errors.length > 0 ? 'partial' : 'success',
            lastRunSummary: summary,
            lastRunError: summary.errors.length > 0 ? 'Часть месяцев не удалось обновить.' : null
        };
        await saveMonthlyScheduleConfig(updatedConfig);
        console.log('[monthlySchedule] Завершён запуск планировщика:', JSON.stringify(summary));
        return {
            status: updatedConfig.lastRunStatus,
            summary
        };
    } catch (error) {
        console.error('[monthlySchedule] Job failed:', error);
        const failedConfig = {
            ...(await loadMonthlyScheduleConfig()),
            lastRunAt: new Date().toISOString(),
            lastRunStatus: 'error',
            lastRunSummary: summary,
            lastRunError: error.message || 'Неизвестная ошибка при формировании отчётов.'
        };
        await saveMonthlyScheduleConfig(failedConfig);
        return { status: 'error', error };
    } finally {
        schedulerState.isRunning = false;
    }
}

async function scheduleNextRun() {
    if (schedulerState.timer) {
        clearTimeout(schedulerState.timer);
        schedulerState.timer = null;
    }
    const config = await loadMonthlyScheduleConfig();
    if (!config.enabled) {
        schedulerState.nextRunAt = null;
        console.log('[monthlySchedule] Планировщик отключён, автоматические запуски не планируются.');
        return;
    }
    const nextRun = computeNextRunDate(config, new Date());
    if (!nextRun) {
        schedulerState.nextRunAt = null;
        return;
    }
    const delay = Math.max(nextRun.getTime() - Date.now(), 5 * 1000);
    schedulerState.nextRunAt = nextRun;
    console.log(`[monthlySchedule] Следующий запуск запланирован на ${nextRun.toISOString()}.`);
    schedulerState.timer = setTimeout(async () => {
        schedulerState.timer = null;
        await runMonthlyScheduleJobInternal('auto');
        await scheduleNextRun();
    }, delay);
}

export async function initializeMonthlyScheduler() {
    await ensureScheduleConfigFile();
    await scheduleNextRun();
}

export async function restartMonthlyScheduler() {
    await scheduleNextRun();
}

export function getMonthlySchedulerState() {
    return {
        nextRunAt: schedulerState.nextRunAt,
        isRunning: schedulerState.isRunning
    };
}

export async function runMonthlyScheduleNow() {
    const result = await runMonthlyScheduleJobInternal('manual');
    await scheduleNextRun();
    return result;
}

export async function updateMonthlyScheduleConfig(payload) {
    const current = await loadMonthlyScheduleConfig();
    const nextConfig = {
        ...current,
        enabled: Boolean(payload.enabled),
        frequency: payload.frequency === 'weekly' ? 'weekly' : 'daily',
        time: normalizeTimeString(payload.time || current.time),
        weekday: normalizeWeekday(payload.weekday || current.weekday),
        range: normalizeRangeConfig(payload.range)
    };
    const saved = await saveMonthlyScheduleConfig(nextConfig);
    await scheduleNextRun();
    return saved;
}

export async function getMonthlyScheduleOverview() {
    const config = await loadMonthlyScheduleConfig();
    const runtime = getMonthlySchedulerState();
    const nextRunAt = config.enabled ? computeNextRunDate(config, new Date()) : null;
    return {
        config,
        nextRunAt: runtime.nextRunAt || nextRunAt,
        isRunning: runtime.isRunning
    };
}

function formatRangePayload(rangeType, countRaw) {
    if (rangeType === 'all') {
        return { type: 'all' };
    }
    if (rangeType === 'last') {
        return { type: 'last', count: clampRangeCount(countRaw) };
    }
    return { type: 'current' };
}

export function buildSchedulePayloadFromForm(body = {}) {
    const enabled = body.enabled === 'on' || body.enabled === 'true';
    const frequency = body.frequency === 'weekly' ? 'weekly' : 'daily';
    const time = body.time || DEFAULT_SCHEDULE_CONFIG.time;
    const weekday = body.weekday || DEFAULT_SCHEDULE_CONFIG.weekday;
    const rangeType = body.rangeType || 'current';
    const rangeCount = body.rangeCount;
    return {
        enabled,
        frequency,
        time,
        weekday,
        range: formatRangePayload(rangeType, rangeCount)
    };
}
