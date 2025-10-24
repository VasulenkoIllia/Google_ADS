import {
    verifyCredentials,
    updatePinSettings,
    loadSecurityConfig,
    isValidPassword
} from '../services/securityConfigService.js';
import { issuePinSession, clearPinCookie } from '../middlewares/pinAccessMiddleware.js';

function resolveRedirectTarget(nextParam) {
    if (typeof nextParam === 'string' && nextParam.startsWith('/')) {
        return nextParam;
    }
    return '/';
}

export async function renderPinPrompt(req, res) {
    const { next: nextParam = '/' } = req.query || {};
    const nextUrl = resolveRedirectTarget(nextParam);
    const config = await loadSecurityConfig();
    if (!config.pinEnabled) {
        return res.redirect(nextUrl);
    }
    return res.render('auth/pin', {
        nextUrl,
        expectedUsername: config.username,
        errorMessage: typeof req.query?.error === 'string' ? req.query.error : null
    });
}

export async function handlePinVerify(req, res) {
    try {
        const { username = '', pin, next: nextParam = '/' } = req.body || {};
        const nextUrl = resolveRedirectTarget(nextParam);
        const isValid = await verifyCredentials(username, pin);
        if (!isValid) {
            const message = encodeURIComponent('Неверные данные для входа.');
            return res.redirect(`/auth/pin?error=${message}&next=${encodeURIComponent(nextUrl)}`);
        }
        issuePinSession(res);
        return res.redirect(nextUrl);
    } catch (error) {
        console.error('[security] Password verification failed:', error);
        const message = encodeURIComponent('Не удалось проверить пароль.');
        return res.redirect(`/auth/pin?error=${message}`);
    }
}

export function handlePinLogout(req, res) {
    clearPinCookie(res);
    return res.redirect('/auth/pin');
}

export async function handlePinSettingsUpdate(req, res) {
    try {
        const { enablePin = 'off' } = req.body || {};
        const rawNewPin = typeof req.body?.newPin === 'string' ? req.body.newPin.trim() : '';
        const rawConfirmPin = typeof req.body?.confirmPin === 'string' ? req.body.confirmPin.trim() : '';
        const rawUsername = typeof req.body?.username === 'string' ? req.body.username.trim() : '';
        const enable = enablePin === 'on';
        let shouldIssueSession = false;
        if (enable) {
            if (!rawNewPin && !rawConfirmPin) {
                // allow enabling existing pin without changes
                await updatePinSettings({ enabled: true, newPin: null, username: rawUsername });
                shouldIssueSession = true;
            } else {
                if (rawNewPin !== rawConfirmPin) {
                    throw new Error('Пароль и подтверждение не совпадают.');
                }
                if (!isValidPassword(rawNewPin)) {
                    throw new Error('Пароль должен содержать от 4 до 32 символов.');
                }
                await updatePinSettings({ enabled: true, newPin: rawNewPin, username: rawUsername });
                shouldIssueSession = true;
            }
        } else {
            await updatePinSettings({ enabled: false, newPin: null, username: rawUsername });
            clearPinCookie(res);
        }
        if (shouldIssueSession) {
            issuePinSession(res);
        }
        const success = encodeURIComponent('Настройки доступа сохранены.');
        const returnTo = typeof req.body?.returnTo === 'string' && req.body.returnTo.startsWith('/')
            ? req.body.returnTo
            : '/';
        const separator = returnTo.includes('?') ? '&' : '?';
        return res.redirect(`${returnTo}${separator}success=${success}`);
    } catch (error) {
        console.error('[security] update settings failed:', error);
        const message = encodeURIComponent(error.message || 'Не удалось сохранить настройки доступа.');
        const returnTo = typeof req.body?.returnTo === 'string' && req.body.returnTo.startsWith('/')
            ? req.body.returnTo
            : '/';
        const separator = returnTo.includes('?') ? '&' : '?';
        return res.redirect(`${returnTo}${separator}error=${message}`);
    }
}
