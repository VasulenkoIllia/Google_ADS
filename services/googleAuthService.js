import fs from 'fs/promises';
import path from 'path';
import { OAuth2Client } from 'google-auth-library';
import dotenv from 'dotenv';

dotenv.config();

const DATA_ROOT = path.resolve('data');
const CONFIG_DIR = path.join(DATA_ROOT, 'config');
const TOKEN_FILE = path.join(CONFIG_DIR, 'google-ads-auth.json');

export const GOOGLE_REFRESH_TOKEN_MAX_AGE_DAYS = 6;
const GOOGLE_REFRESH_TOKEN_MAX_AGE_MS = GOOGLE_REFRESH_TOKEN_MAX_AGE_DAYS * 24 * 60 * 60 * 1000;

const REQUIRED_OAUTH_ENV = Object.freeze(['CLIENT_ID', 'CLIENT_SECRET']);

let cachedState = null;
let cachedMtimeMs = null;

function collectMissingEnv(requiredNames) {
    return requiredNames.filter((name) => {
        const raw = process.env[name];
        return typeof raw !== 'string' || raw.trim().length === 0;
    });
}

function assertOAuthEnv() {
    const missing = collectMissingEnv(REQUIRED_OAUTH_ENV);
    if (missing.length > 0) {
        const hint = missing.join(', ');
        throw new Error(`[googleAuth] Missing required environment variables: ${hint}. Populate them in .env before using Google OAuth.`);
    }
}

async function ensureConfigDir() {
    await fs.mkdir(CONFIG_DIR, { recursive: true });
}

function normalizeState(raw = {}) {
    if (!raw || typeof raw !== 'object') {
        return {};
    }
    const refreshToken = typeof raw.refreshToken === 'string' ? raw.refreshToken.trim() : '';
    const issuedAt = Number.isFinite(raw.issuedAt) ? raw.issuedAt : null;
    const scope = typeof raw.scope === 'string' ? raw.scope : null;
    const expiryDate = Number.isFinite(raw.expiryDate) ? raw.expiryDate : null;
    const accountEmail = typeof raw.accountEmail === 'string' ? raw.accountEmail : null;

    return {
        refreshToken: refreshToken.length > 0 ? refreshToken : '',
        issuedAt,
        scope,
        expiryDate,
        accountEmail
    };
}

async function readStateFromDisk() {
    try {
        const stats = await fs.stat(TOKEN_FILE);
        const mtimeMs = stats.mtimeMs;
        if (cachedState && cachedMtimeMs === mtimeMs) {
            return cachedState;
        }
        const raw = await fs.readFile(TOKEN_FILE, 'utf8');
        const parsed = JSON.parse(raw);
        const normalized = normalizeState(parsed);
        cachedState = normalized;
        cachedMtimeMs = mtimeMs;
        return normalized;
    } catch (error) {
        if (error.code === 'ENOENT') {
            cachedState = null;
            cachedMtimeMs = null;
            return {};
        }
        throw error;
    }
}

export async function loadGoogleAuthState() {
    const state = await readStateFromDisk();
    return state || {};
}

function computeExpiryMeta(issuedAt) {
    if (!Number.isFinite(issuedAt) || issuedAt <= 0) {
        return {
            expiresAt: null,
            msRemaining: null,
            daysRemaining: null,
            isExpired: true,
            status: 'missing'
        };
    }
    const expiresAt = issuedAt + GOOGLE_REFRESH_TOKEN_MAX_AGE_MS;
    const msRemaining = expiresAt - Date.now();
    const daysRemaining = Math.ceil(msRemaining / (24 * 60 * 60 * 1000));
    if (msRemaining <= 0) {
        return {
            expiresAt,
            msRemaining,
            daysRemaining: 0,
            isExpired: true,
            status: 'expired'
        };
    }
    if (msRemaining <= 48 * 60 * 60 * 1000) {
        return {
            expiresAt,
            msRemaining,
            daysRemaining,
            isExpired: false,
            status: 'warning'
        };
    }
    return {
        expiresAt,
        msRemaining,
        daysRemaining,
        isExpired: false,
        status: 'ok'
    };
}

export async function getGoogleAuthStatus() {
    const state = await loadGoogleAuthState();
    const { refreshToken = '', issuedAt = null, accountEmail = null, scope = null } = state;
    const expiryMeta = computeExpiryMeta(issuedAt);

    return {
        hasRefreshToken: refreshToken.length > 0,
        refreshToken: refreshToken.length > 0 ? refreshToken : null,
        issuedAt,
        accountEmail,
        scope,
        expiresAt: expiryMeta.expiresAt,
        msRemaining: expiryMeta.msRemaining,
        daysRemaining: expiryMeta.daysRemaining,
        isExpired: expiryMeta.isExpired,
        status: expiryMeta.status,
        maxAgeDays: GOOGLE_REFRESH_TOKEN_MAX_AGE_DAYS
    };
}

function serializeState(state) {
    const payload = {
        refreshToken: state.refreshToken,
        issuedAt: state.issuedAt,
        scope: state.scope ?? null,
        expiryDate: state.expiryDate ?? null,
        accountEmail: state.accountEmail ?? null
    };
    return JSON.stringify(payload, null, 2);
}

export async function saveGoogleAuthState(nextState) {
    if (!nextState || typeof nextState !== 'object') {
        throw new Error('[googleAuth] Invalid state payload provided for save.');
    }
    const normalized = normalizeState(nextState);
    if (!normalized.refreshToken) {
        throw new Error('[googleAuth] Refresh token is required to save auth state.');
    }
    await ensureConfigDir();
    const serialized = serializeState({
        ...normalized,
        issuedAt: Number.isFinite(normalized.issuedAt) ? normalized.issuedAt : Date.now()
    });
    await fs.writeFile(TOKEN_FILE, serialized, 'utf8');
    try {
        const stats = await fs.stat(TOKEN_FILE);
        cachedState = normalizeState(JSON.parse(serialized));
        cachedMtimeMs = stats.mtimeMs;
    } catch {
        cachedState = normalizeState(JSON.parse(serialized));
        cachedMtimeMs = Date.now();
    }
    return cachedState;
}

export async function updateGoogleAuthState(partial) {
    const current = await loadGoogleAuthState();
    const merged = {
        ...current,
        ...(partial || {})
    };
    if (!merged.refreshToken) {
        throw new Error('[googleAuth] Cannot update auth state without refresh token.');
    }
    await saveGoogleAuthState(merged);
    return merged;
}

export async function clearGoogleAuthState() {
    cachedState = null;
    cachedMtimeMs = null;
    try {
        await fs.unlink(TOKEN_FILE);
    } catch (error) {
        if (error.code !== 'ENOENT') {
            throw error;
        }
    }
}

export function createOAuthClient(options = {}) {
    assertOAuthEnv();
    const { CLIENT_ID, CLIENT_SECRET, GOOGLE_OAUTH_REDIRECT_URI } = process.env;
    const resolvedRedirectUri = typeof options.redirectUri === 'string' && options.redirectUri.length > 0
        ? options.redirectUri
        : (typeof GOOGLE_OAUTH_REDIRECT_URI === 'string' && GOOGLE_OAUTH_REDIRECT_URI.length > 0
            ? GOOGLE_OAUTH_REDIRECT_URI
            : undefined
        );

    const clientConfig = {
        clientId: CLIENT_ID,
        clientSecret: CLIENT_SECRET
    };
    if (resolvedRedirectUri) {
        clientConfig.redirectUri = resolvedRedirectUri;
    }

    return new OAuth2Client(clientConfig);
}

export async function persistGoogleRefreshToken({ refreshToken, scope = null, accountEmail = null }) {
    if (typeof refreshToken !== 'string' || refreshToken.trim().length === 0) {
        throw new Error('[googleAuth] Refresh token must be a non-empty string.');
    }
    const trimmed = refreshToken.trim();
    const state = {
        refreshToken: trimmed,
        issuedAt: Date.now(),
        scope: scope || null,
        accountEmail: accountEmail || null
    };
    await saveGoogleAuthState(state);
    return state;
}

export function isRefreshTokenExpired(status) {
    if (!status) {
        return true;
    }
    if (status.isExpired) {
        return true;
    }
    return false;
}

