const DEFAULT_QUEUE_LIMIT = Infinity;

function ensurePositiveInteger(value, fallback) {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed) && parsed > 0) {
        return parsed;
    }
    return fallback;
}

function ensurePositiveNumber(value, fallback) {
    const parsed = Number.parseFloat(value);
    if (Number.isFinite(parsed) && parsed > 0) {
        return parsed;
    }
    return fallback;
}

function toSafeFunction(fn) {
    if (typeof fn !== 'function') {
        return null;
    }
    return fn;
}

function delay(ms) {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    });
}

export class RateLimiter {
    constructor(options = {}) {
        const {
            maxRequests,
            intervalMs,
            queueLimit = DEFAULT_QUEUE_LIMIT,
            onDelay
        } = options;

        const safeMaxRequests = ensurePositiveInteger(maxRequests, 1);
        const safeIntervalMs = ensurePositiveNumber(intervalMs, 1000);
        const safeQueueLimit = queueLimit === DEFAULT_QUEUE_LIMIT
            ? DEFAULT_QUEUE_LIMIT
            : ensurePositiveInteger(queueLimit, DEFAULT_QUEUE_LIMIT);

        this.maxRequests = safeMaxRequests;
        this.intervalMs = safeIntervalMs;
        this.queueLimit = safeQueueLimit;
        this.onDelay = toSafeFunction(onDelay);

        this.queue = [];
        this.requestTimestamps = [];
        this.timer = null;
        this._isCoolingDown = false;
        this.lastDelayMs = 0;
        this._cooldownTriggered = false;
    }

    get isCoolingDown() {
        return this._isCoolingDown;
    }

    get pendingRequests() {
        return this.queue.length;
    }

    get downstreamDelayMs() {
        return this.lastDelayMs;
    }

    consumeCooldownFlag() {
        const flag = this._cooldownTriggered;
        this._cooldownTriggered = false;
        if (flag) {
            const delayMs = this.lastDelayMs;
            this.lastDelayMs = 0;
            return { triggered: true, delayMs };
        }
        return { triggered: false, delayMs: 0 };
    }

    _pruneOld(now = Date.now()) {
        this.requestTimestamps = this.requestTimestamps.filter(timestamp => now - timestamp < this.intervalMs);
    }

    async schedule(taskFn) {
        if (typeof taskFn !== 'function') {
            throw new TypeError('RateLimiter.schedule expects a function that returns a promise.');
        }

        if (this.pendingRequests >= this.queueLimit) {
            throw new Error('Rate limiter queue limit exceeded.');
        }

        return new Promise((resolve, reject) => {
            this.queue.push({ taskFn, resolve, reject });
            this._flushQueue();
        });
    }

    async waitForAll() {
        while (this.queue.length > 0 || this._isCoolingDown) {
            const waitTime = this._isCoolingDown ? Math.max(this.lastDelayMs, 0) : 10;
            await delay(waitTime);
        }
    }

    _flushQueue() {
        this._pruneOld();
        if (this.queue.length === 0) {
            return;
        }

        const now = Date.now();
        if (this.requestTimestamps.length >= this.maxRequests) {
            const timeSinceOldest = now - this.requestTimestamps[0];
            const waitTime = Math.max(this.intervalMs - timeSinceOldest, 0);

            if (waitTime === 0) {
                this.requestTimestamps.shift();
                this._flushQueue();
                return;
            }

            if (!this.timer) {
                this._isCoolingDown = true;
                this._cooldownTriggered = true;
                this.lastDelayMs = waitTime;
                if (this.onDelay) {
                    try {
                        this.onDelay(waitTime, this.queue.length);
                    } catch (err) {
                        console.warn('RateLimiter onDelay callback failed:', err);
                    }
                }
                this.timer = setTimeout(() => {
                    this.timer = null;
                    this._isCoolingDown = false;
                    this._flushQueue();
                }, waitTime);
            }

            return;
        }

        const { taskFn, resolve, reject } = this.queue.shift();
        this.requestTimestamps.push(now);

        Promise.resolve()
            .then(() => taskFn())
            .then(result => resolve(result))
            .catch(error => reject(error))
            .finally(() => {
                this._flushQueue();
            });
    }

    estimateWait(extraRequests = 0) {
        const now = Date.now();
        this._pruneOld(now);

        const maxRequests = Math.max(this.maxRequests, 1);
        const intervalMs = this.intervalMs;
        const queueAhead = this.queue.length;
        const active = this.requestTimestamps.length;

        let baseWaitMs = 0;
        if (active >= maxRequests && this.requestTimestamps.length > 0) {
            const oldest = this.requestTimestamps[0];
            baseWaitMs = Math.max(intervalMs - (now - oldest), 0);
        }

        if (this._isCoolingDown && this.lastDelayMs > baseWaitMs) {
            baseWaitMs = this.lastDelayMs;
        }

        const extraWaitBeforeMs = Math.floor(queueAhead / maxRequests) * intervalMs;

        const tasksAheadInWindow = queueAhead % maxRequests;
        const slotsUsedWithCurrent = tasksAheadInWindow + 1;
        let remainingSlotsInWindow = maxRequests - slotsUsedWithCurrent;
        if (remainingSlotsInWindow < 0) {
            remainingSlotsInWindow = 0;
        }

        const extraRequestsAfterCurrent = Math.max(extraRequests, 0);
        const remainingExtraAfterCurrent = Math.max(extraRequestsAfterCurrent - remainingSlotsInWindow, 0);
        const extraWaitAfterMs = remainingExtraAfterCurrent > 0
            ? Math.ceil(remainingExtraAfterCurrent / maxRequests) * intervalMs
            : 0;

        const waitMs = Math.max(0, baseWaitMs + extraWaitBeforeMs + extraWaitAfterMs);

        return {
            waitMs,
            baseWaitMs,
            extraWaitBeforeMs,
            extraWaitAfterMs,
            queueAhead,
            active,
            intervalMs,
            maxRequests,
            extraRequests: extraRequestsAfterCurrent,
            remainingSlotsInWindow,
            coolingDown: this._isCoolingDown
        };
    }
}
