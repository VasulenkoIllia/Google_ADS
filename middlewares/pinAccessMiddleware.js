import crypto from 'crypto';
import { isPinEnabled } from '../services/securityConfigService.js';

const PIN_COOKIE_NAME = 'pin_access';
const PIN_SESSION_MAX_MS = 60 * 60 * 1000; // 1 hour

function getCookieSecret() {
    return process.env.PIN_COOKIE_SECRET || process.env.COOKIE_SECRET || 'pin-cookie-secret';
}

function signTimestamp(timestamp) {
    return crypto.createHmac('sha256', getCookieSecret()).update(String(timestamp)).digest('hex');
}

function parseCookieValue(cookieValue) {
    if (!cookieValue || typeof cookieValue !== 'string') {
        return null;
    }
    const [timestampStr, signature] = cookieValue.split('.');
    if (!timestampStr || !signature) {
        return null;
    }
    const timestamp = Number.parseInt(timestampStr, 10);
    if (!Number.isFinite(timestamp)) {
        return null;
    }
    const expectedSignature = signTimestamp(timestampStr);
    if (!crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expectedSignature))) {
        return null;
    }
    return { timestamp };
}

function setPinCookie(res, timestamp = Date.now()) {
    const signature = signTimestamp(String(timestamp));
    const value = `${timestamp}.${signature}`;
    res.cookie(PIN_COOKIE_NAME, value, {
        httpOnly: true,
        sameSite: 'lax',
        secure: process.env.NODE_ENV === 'production',
        maxAge: PIN_SESSION_MAX_MS
    });
}

export function clearPinCookie(res) {
    res.clearCookie(PIN_COOKIE_NAME, {
        httpOnly: true,
        sameSite: 'lax',
        secure: process.env.NODE_ENV === 'production'
    });
}

function shouldBypass(reqPath) {
    if (!reqPath) {
        return false;
    }
    if (reqPath === '/auth/pin' || reqPath === '/auth/pin/verify' || reqPath === '/auth/pin/logout') {
        return true;
    }
    return reqPath.startsWith('/assets')
        || reqPath.startsWith('/favicon')
        || reqPath.startsWith('/healthz');
}

export function issuePinSession(res) {
    setPinCookie(res, Date.now());
}

export function pinAccessMiddleware(req, res, next) {
    isPinEnabled()
        .then((enabled) => {
            if (!enabled) {
                return next();
            }
            if (shouldBypass(req.path)) {
                return next();
            }
            const parsed = parseCookieValue(req.cookies?.[PIN_COOKIE_NAME]);
            if (parsed) {
                const age = Date.now() - parsed.timestamp;
                if (age < PIN_SESSION_MAX_MS) {
                    // refresh cookie if older than 30 minutes
                    if (age > PIN_SESSION_MAX_MS / 2) {
                        setPinCookie(res);
                    }
                    return next();
                }
            }
            const accepted = req.accepts(['html', 'json']);
            if (accepted === 'json') {
                return res.status(401).json({
                    error: 'PASSWORD_REQUIRED',
                    message: 'Для доступа требуется пароль.'
                });
            }
            const nextUrl = encodeURIComponent(req.originalUrl || req.url || '/');
            return res.redirect(`/auth/pin?next=${nextUrl}`);
        })
        .catch((error) => {
            console.error('[securityConfig] Failed to evaluate password access:', error);
            return next();
        });
}
