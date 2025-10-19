import fs from 'fs/promises';
import path from 'path';
import crypto from 'crypto';

const DATA_ROOT = path.resolve('data');
const CONFIG_DIR = path.join(DATA_ROOT, 'config');
const SECURITY_CONFIG_PATH = path.join(CONFIG_DIR, 'security.json');

const DEFAULT_SECURITY_CONFIG = {
    pinEnabled: true,
    username: 'gouads',
    pinSalt: 'e8ba30233ee06c2be3e5269f4ff93b14',
    pinHash: 'a82e099f113f81acedea55bfaf5e834b879d869c27dd77ee3487894d9a2f2773',
    updatedAt: null
};

async function ensureSecurityConfigFile() {
    await fs.mkdir(CONFIG_DIR, { recursive: true });
    try {
        await fs.access(SECURITY_CONFIG_PATH);
    } catch (error) {
        if (error.code === 'ENOENT') {
            await fs.writeFile(SECURITY_CONFIG_PATH, JSON.stringify(DEFAULT_SECURITY_CONFIG, null, 2), 'utf8');
        } else {
            throw error;
        }
    }
}

function normalizeConfig(raw = {}) {
    const normalizedUsername = typeof raw.username === 'string' ? raw.username.trim() : '';
    return {
        pinEnabled: Boolean(raw.pinEnabled),
        username: normalizedUsername.length > 0 ? normalizedUsername : DEFAULT_SECURITY_CONFIG.username,
        pinSalt: typeof raw.pinSalt === 'string' ? raw.pinSalt : null,
        pinHash: typeof raw.pinHash === 'string' ? raw.pinHash : null,
        updatedAt: typeof raw.updatedAt === 'string' ? raw.updatedAt : null
    };
}

export async function loadSecurityConfig() {
    await ensureSecurityConfigFile();
    const raw = await fs.readFile(SECURITY_CONFIG_PATH, 'utf8');
    try {
        const parsed = JSON.parse(raw);
        let normalized = normalizeConfig(parsed);
        if (!normalized.pinSalt || !normalized.pinHash) {
            console.warn('[securityConfig] No password configured, applying default credentials.');
            normalized = await writeSecurityConfig(DEFAULT_SECURITY_CONFIG);
            return normalized;
        }
        if (!normalized.username) {
            normalized = await writeSecurityConfig({
                ...normalized,
                username: DEFAULT_SECURITY_CONFIG.username
            });
        }
        return normalized;
    } catch (error) {
        console.error('[securityConfig] Failed to parse config, using defaults:', error);
        return writeSecurityConfig(DEFAULT_SECURITY_CONFIG);
    }
}

async function writeSecurityConfig(config) {
    const normalized = normalizeConfig(config);
    normalized.updatedAt = new Date().toISOString();
    await fs.writeFile(SECURITY_CONFIG_PATH, JSON.stringify(normalized, null, 2), 'utf8');
    return normalized;
}

function generateSalt() {
    return crypto.randomBytes(16).toString('hex');
}

function hashSecret(secret, salt) {
    return crypto.createHmac('sha256', salt).update(secret).digest('hex');
}

export function isValidPassword(rawPassword) {
    if (typeof rawPassword !== 'string') {
        return false;
    }
    const value = rawPassword.trim();
    return value.length >= 4 && value.length <= 32;
}

export async function setPin(password, username = null) {
    if (!isValidPassword(password)) {
        throw new Error('Пароль має містити від 4 до 32 символів.');
    }
    const salt = generateSalt();
    const pinHash = hashSecret(password.trim(), salt);
    const config = await loadSecurityConfig();
    const nextUsername = typeof username === 'string' && username.trim().length > 0
        ? username.trim()
        : (config.username || DEFAULT_SECURITY_CONFIG.username);
    const updated = await writeSecurityConfig({
        ...config,
        pinEnabled: true,
        username: nextUsername,
        pinSalt: salt,
        pinHash
    });
    return updated;
}

export async function disablePin() {
    const config = await loadSecurityConfig();
    const updated = await writeSecurityConfig({
        ...config,
        pinEnabled: false
    });
    return updated;
}

export async function updatePinSettings({ enabled, newPin, username }) {
    const config = await loadSecurityConfig();
    const nextUsername = typeof username === 'string' && username.trim().length > 0
        ? username.trim()
        : (config.username || DEFAULT_SECURITY_CONFIG.username);
    if (enabled) {
        if (newPin) {
            return setPin(newPin, nextUsername);
        }
        if (!config.pinHash || !config.pinSalt) {
            throw new Error('Введіть новий пароль, щоб увімкнути захист.');
        }
        const updated = await writeSecurityConfig({
            ...config,
            pinEnabled: true,
            username: nextUsername
        });
        return updated;
    }
    const updated = await writeSecurityConfig({
        ...config,
        pinEnabled: false,
        username: nextUsername
    });
    return updated;
}

export async function verifyCredentials(usernameCandidate, passwordCandidate) {
    if (!isValidPassword(passwordCandidate)) {
        return false;
    }
    const normalizedUsername = typeof usernameCandidate === 'string' ? usernameCandidate.trim() : '';
    const config = await loadSecurityConfig();
    if (!config.pinEnabled || !config.pinSalt || !config.pinHash) {
        return false;
    }
    if (normalizedUsername.length === 0 || normalizedUsername !== config.username) {
        return false;
    }
    const expected = hashSecret(passwordCandidate.trim(), config.pinSalt);
    const expectedBuf = Buffer.from(expected, 'hex');
    const actualBuf = Buffer.from(config.pinHash, 'hex');
    if (expectedBuf.length !== actualBuf.length) {
        return false;
    }
    return crypto.timingSafeEqual(expectedBuf, actualBuf);
}

export async function isPinEnabled() {
    const config = await loadSecurityConfig();
    return Boolean(config.pinEnabled && config.pinSalt && config.pinHash);
}
