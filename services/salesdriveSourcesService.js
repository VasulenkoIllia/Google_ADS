import fs from 'fs/promises';
import { readFileSync, writeFileSync, mkdirSync } from 'fs';
import path from 'path';

const DATA_ROOT = path.resolve('data');
const CONFIG_DIR = path.join(DATA_ROOT, 'config');
const SOURCES_FILE = path.join(CONFIG_DIR, 'salesdrive-sources.json');

const DEFAULT_SOURCES = [
    { id: 63, ident: 'PL-151-LED', nameView: '' },
    { id: 1006, ident: 'Headlamp-HL2-1006', nameView: '' },
    { id: 47, ident: 'BL-C861', nameView: '' },
    { id: 45, ident: 'sh-1101', nameView: '' },
    { id: 190, ident: 'BL-1831-Т6', nameView: '' },
    { id: 191, ident: 'RB-206', nameView: '' },
    { id: 114, ident: 'WD-419', nameView: '' },
    { id: 163, ident: 'X-Balog-8070', nameView: '' },
    { id: 189, ident: '1837-T6', nameView: '' },
    { id: 168, ident: 'HX-816', nameView: '' },
    { id: 175, ident: 'BL-2188-2', nameView: '' },
    { id: 246, ident: 'DL-1570', nameView: '' },
    { id: 247, ident: 'DL-0010', nameView: '' },
    { id: 248, ident: 'DL-0008', nameView: '' },
    { id: 192, ident: 'RB-B14', nameView: '' },
    { id: 193, ident: 'BL-2804-T6', nameView: '' },
    { id: 249, ident: 'HC-618S', nameView: '' }
];

async function ensureConfigFile() {
    await fs.mkdir(CONFIG_DIR, { recursive: true });
    try {
        await fs.access(SOURCES_FILE);
    } catch (error) {
        if (error.code === 'ENOENT') {
            await fs.writeFile(SOURCES_FILE, JSON.stringify(DEFAULT_SOURCES, null, 2), 'utf8');
        } else {
            throw error;
        }
    }
}

export async function loadSalesdriveSources() {
    await ensureConfigFile();
    const raw = await fs.readFile(SOURCES_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
        return [];
    }
    return parsed.map(normalizeSource);
}

export function loadSalesdriveSourcesSync() {
    try {
        const raw = readFileSync(SOURCES_FILE, 'utf8');
        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed)) {
            return [];
        }
        return parsed.map(normalizeSource);
    } catch (error) {
        if (error.code === 'ENOENT') {
            try {
                mkdirSync(CONFIG_DIR, { recursive: true });
            } catch (mkdirError) {
                // ignore sync mkdir errors; async ensure handles this path elsewhere
            }
            try {
                writeFileSync(SOURCES_FILE, JSON.stringify(DEFAULT_SOURCES, null, 2), 'utf8');
            } catch (writeError) {
                // ignore inability to write during sync fallback
            }
            return DEFAULT_SOURCES.map(normalizeSource);
        }
        throw error;
    }
}

function normalizeSource(entry) {
    const id = Number.parseInt(entry.id, 10);
    return {
        id: Number.isFinite(id) ? id : 0,
        ident: typeof entry.ident === 'string' ? entry.ident.trim() : '',
        nameView: typeof entry.nameView === 'string' ? entry.nameView.trim() : ''
    };
}

async function saveSalesdriveSources(sources) {
    const normalized = sources.map(normalizeSource).filter(source => source.id > 0 && source.ident !== '');
    normalized.sort((a, b) => a.id - b.id);
    await fs.writeFile(SOURCES_FILE, JSON.stringify(normalized, null, 2), 'utf8');
    return normalized;
}

export async function addSalesdriveSource(payload) {
    const current = await loadSalesdriveSources();
    const normalized = normalizeSource(payload);
    if (normalized.id <= 0 || normalized.ident === '') {
        throw new Error('ID та ident джерела мають бути заповнені.');
    }
    if (current.some(source => source.id === normalized.id)) {
        throw new Error(`Джерело з id ${normalized.id} вже існує.`);
    }
    if (current.some(source => source.ident.toLowerCase() === normalized.ident.toLowerCase())) {
        throw new Error(`Джерело з ident ${normalized.ident} вже існує.`);
    }
    current.push(normalized);
    return saveSalesdriveSources(current);
}

export async function updateSalesdriveSource(id, payload) {
    const current = await loadSalesdriveSources();
    const targetId = Number.parseInt(id, 10);
    const index = current.findIndex(source => source.id === targetId);
    if (index === -1) {
        throw new Error(`Джерело з id ${id} не знайдено.`);
    }
    const updated = normalizeSource({ ...current[index], ...payload, id: targetId });
    if (updated.ident === '') {
        throw new Error('Ident не може бути порожнім.');
    }
    if (current.some((source, idx) => idx !== index && source.ident.toLowerCase() === updated.ident.toLowerCase())) {
        throw new Error(`Джерело з ident ${updated.ident} вже існує.`);
    }
    current[index] = updated;
    return saveSalesdriveSources(current);
}

export async function removeSalesdriveSource(id) {
    const current = await loadSalesdriveSources();
    const targetId = Number.parseInt(id, 10);
    const next = current.filter(source => source.id !== targetId);
    if (next.length === current.length) {
        throw new Error(`Джерело з id ${id} не знайдено.`);
    }
    return saveSalesdriveSources(next);
}
