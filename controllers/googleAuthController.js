import { randomBytes } from 'crypto';
import { createOAuthClient, persistGoogleRefreshToken, clearGoogleAuthState, GOOGLE_REFRESH_TOKEN_MAX_AGE_DAYS } from '../services/googleAuthService.js';

const STATE_COOKIE_NAME = 'google_oauth_state';
const STATE_TTL_MS = 10 * 60 * 1000; // 10 minutes
const DEFAULT_RETURN_TO = '/settings?tab=google-auth';
const GOOGLE_ADS_SCOPE = 'https://www.googleapis.com/auth/adwords';

function resolveBaseUrl(req) {
    const forwardedProto = req.headers['x-forwarded-proto'];
    const forwardedHost = req.headers['x-forwarded-host'];
    const protocol = typeof forwardedProto === 'string' && forwardedProto.length > 0
        ? forwardedProto.split(',')[0].trim()
        : (req.protocol || 'http');
    const host = typeof forwardedHost === 'string' && forwardedHost.length > 0
        ? forwardedHost.split(',')[0].trim()
        : req.get('host');
    return `${protocol}://${host}`;
}

function sanitizeReturnTo(raw) {
    if (typeof raw !== 'string') {
        return DEFAULT_RETURN_TO;
    }
    const trimmed = raw.trim();
    if (!trimmed.startsWith('/')) {
        return DEFAULT_RETURN_TO;
    }
    return trimmed;
}

function buildSettingsRedirectUrl({ success, error, returnTo = DEFAULT_RETURN_TO }) {
    const url = new URL(`http://placeholder${returnTo}`);
    if (typeof success === 'string' && success.length > 0) {
        url.searchParams.set('success', success);
    }
    if (typeof error === 'string' && error.length > 0) {
        url.searchParams.set('error', error);
    }
    return url.pathname + (url.search ? url.search : '');
}

function parseStateCookie(req) {
    const rawCookie = req.signedCookies?.[STATE_COOKIE_NAME];
    if (!rawCookie) {
        return null;
    }
    try {
        const parsed = typeof rawCookie === 'string' ? JSON.parse(rawCookie) : rawCookie;
        if (!parsed || typeof parsed !== 'object') {
            return null;
        }
        return parsed;
    } catch {
        return null;
    }
}

function isSecureRequest(req) {
    if (req.secure) {
        return true;
    }
    const proto = req.headers['x-forwarded-proto'];
    if (typeof proto === 'string') {
        return proto.split(',').map(item => item.trim().toLowerCase()).includes('https');
    }
    return false;
}

function storeStateCookie(req, res, payload) {
    res.cookie(STATE_COOKIE_NAME, JSON.stringify(payload), {
        signed: true,
        httpOnly: true,
        sameSite: 'lax',
        maxAge: STATE_TTL_MS,
        secure: isSecureRequest(req)
    });
}

export function beginGoogleAuth(req, res, next) {
    try {
        const returnTo = sanitizeReturnTo(req.query?.returnTo);
        const nonce = randomBytes(16).toString('hex');
        const statePayload = {
            nonce,
            returnTo,
            createdAt: Date.now()
        };
        storeStateCookie(req, res, statePayload);

        const redirectUri = `${resolveBaseUrl(req)}/auth/google/callback`;
        const oauthClient = createOAuthClient({ redirectUri });
        const authUrl = oauthClient.generateAuthUrl({
            access_type: 'offline',
            scope: [GOOGLE_ADS_SCOPE],
            prompt: 'consent select_account',
            state: nonce,
            include_granted_scopes: true
        });

        return res.redirect(authUrl);
    } catch (error) {
        console.error('[googleAuth] Failed to start OAuth flow:', error);
        const redirectUrl = buildSettingsRedirectUrl({
            error: 'Не удалось запустить авторизацию Google Ads. Проверьте настройки CLIENT_ID / CLIENT_SECRET.'
        });
        return res.redirect(redirectUrl);
    }
}

export async function handleGoogleAuthCallback(req, res) {
    const returnTo = sanitizeReturnTo(req.query?.returnTo);
    const stateCookie = parseStateCookie(req);
    res.clearCookie(STATE_COOKIE_NAME);

    if (!stateCookie) {
        const redirectUrl = buildSettingsRedirectUrl({
            error: 'Состояние авторизации истекло. Попробуйте ещё раз.',
            returnTo
        });
        return res.redirect(redirectUrl);
    }

    const { nonce, createdAt, returnTo: storedReturnTo } = stateCookie;
    const effectiveReturnTo = sanitizeReturnTo(returnTo || storedReturnTo);
    const now = Date.now();
    if (!nonce || typeof nonce !== 'string' || nonce.length === 0 || !Number.isFinite(createdAt) || now - createdAt > STATE_TTL_MS) {
        const redirectUrl = buildSettingsRedirectUrl({
            error: 'Состояние авторизации истекло. Попробуйте ещё раз.',
            returnTo: effectiveReturnTo
        });
        return res.redirect(redirectUrl);
    }

    if (req.query?.state !== nonce) {
        const redirectUrl = buildSettingsRedirectUrl({
            error: 'Некорректный параметр state от Google. Повторите вход.',
            returnTo: effectiveReturnTo
        });
        return res.redirect(redirectUrl);
    }

    if (typeof req.query?.error === 'string') {
        const redirectUrl = buildSettingsRedirectUrl({
            error: `Google отказал в авторизации: ${req.query.error}`,
            returnTo: effectiveReturnTo
        });
        return res.redirect(redirectUrl);
    }

    const code = req.query?.code;
    if (typeof code !== 'string' || code.length === 0) {
        const redirectUrl = buildSettingsRedirectUrl({
            error: 'Google не вернул код авторизации. Попробуйте снова.',
            returnTo: effectiveReturnTo
        });
        return res.redirect(redirectUrl);
    }

    try {
        const redirectUri = `${resolveBaseUrl(req)}/auth/google/callback`;
        const oauthClient = createOAuthClient({ redirectUri });
        const tokenResponse = await oauthClient.getToken(code);
        const tokens = tokenResponse?.tokens || tokenResponse || {};
        if (typeof tokens.refresh_token !== 'string' || tokens.refresh_token.trim().length === 0) {
            const redirectUrl = buildSettingsRedirectUrl({
                error: 'Google не выдал refresh_token. Убедитесь, что при входе выбран пункт «Разрешить» для офлайн-доступа.',
                returnTo: effectiveReturnTo
            });
            return res.redirect(redirectUrl);
        }
        await persistGoogleRefreshToken({
            refreshToken: tokens.refresh_token,
            scope: tokens.scope || null,
            accountEmail: tokens.id_token ? null : null
        });

        const successMessage = `Google Ads авторизация обновлена. Токен действителен ${GOOGLE_REFRESH_TOKEN_MAX_AGE_DAYS} дней.`;
        const redirectUrl = buildSettingsRedirectUrl({
            success: successMessage,
            returnTo: effectiveReturnTo
        });
        return res.redirect(redirectUrl);
    } catch (error) {
        console.error('[googleAuth] Failed to complete OAuth callback:', error);
        const redirectUrl = buildSettingsRedirectUrl({
            error: 'Не удалось получить токен Google Ads. Проверьте логи сервера.',
            returnTo: effectiveReturnTo
        });
        return res.redirect(redirectUrl);
    }
}

export async function handleGoogleAuthDisconnect(req, res) {
    try {
        await clearGoogleAuthState();
        const redirectUrl = buildSettingsRedirectUrl({
            success: 'Привязка Google Ads удалена. Выполните вход заново.',
            returnTo: sanitizeReturnTo(req.body?.returnTo)
        });
        return res.redirect(redirectUrl);
    } catch (error) {
        console.error('[googleAuth] Failed to clear stored token:', error);
        const redirectUrl = buildSettingsRedirectUrl({
            error: 'Не удалось удалить сохранённый токен Google Ads.',
            returnTo: sanitizeReturnTo(req.body?.returnTo)
        });
        return res.redirect(redirectUrl);
    }
}
