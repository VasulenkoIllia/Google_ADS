import {
    loadSalesdriveSources,
    addSalesdriveSource,
    updateSalesdriveSource,
    removeSalesdriveSource
} from '../services/salesdriveSourcesService.js';
import { buildOverlayMeta } from '../services/reportDataService.js';

function buildRedirectUrl(message = '', type = 'success', base = '/reports/config/sources') {
    if (!message) {
        return base;
    }
    const params = new URLSearchParams();
    params.set(type, message);
    const separator = base.includes('?') ? '&' : '?';
    return `${base}${separator}${params.toString()}`;
}

function resolveReturnTo(req, fallback = '/reports/config/sources') {
    const candidate = (req.body && req.body.returnTo) || (req.query && req.query.returnTo);
    if (typeof candidate === 'string' && candidate.startsWith('/')) {
        return candidate;
    }
    return fallback;
}

export async function renderSourcesConfig(req, res) {
    try {
        const sources = await loadSalesdriveSources();
        const { success = '', error = '' } = req.query || {};
        const overlayMeta = buildOverlayMeta({
            remainingSources: Array.isArray(sources) ? sources.length : null
        });
        return res.render('config/sources', {
            sources,
            successMessage: typeof success === 'string' && success.length > 0 ? success : null,
            errorMessage: typeof error === 'string' && error.length > 0 ? error : null,
            reportOverlayMeta: overlayMeta
        });
    } catch (error) {
        console.error('[sourcesConfig] render failed:', error);
        return res.status(500).render('error', {
            message: 'Не удалось загрузить список источников.',
            source: 'sourcesController: render',
            error
        });
    }
}

export async function handleSourceAdd(req, res) {
    try {
        const { id, ident, nameView } = req.body || {};
        await addSalesdriveSource({ id, ident, nameView });
        const target = resolveReturnTo(req);
        return res.redirect(buildRedirectUrl('Источник успешно добавлен.', 'success', target));
    } catch (error) {
        console.error('[sourcesConfig] add failed:', error);
        const target = resolveReturnTo(req);
        return res.redirect(buildRedirectUrl(error.message || 'Ошибка при добавлении источника.', 'error', target));
    }
}

export async function handleSourceUpdate(req, res) {
    try {
        const { id } = req.params || {};
        const { ident, nameView } = req.body || {};
        await updateSalesdriveSource(id, { ident, nameView });
        const target = resolveReturnTo(req);
        return res.redirect(buildRedirectUrl('Изменения сохранены.', 'success', target));
    } catch (error) {
        console.error('[sourcesConfig] update failed:', error);
        const target = resolveReturnTo(req);
        return res.redirect(buildRedirectUrl(error.message || 'Ошибка обновления источника.', 'error', target));
    }
}

export async function handleSourceDelete(req, res) {
    try {
        const { id } = req.params || {};
        await removeSalesdriveSource(id);
        const target = resolveReturnTo(req);
        return res.redirect(buildRedirectUrl('Источник удалён.', 'success', target));
    } catch (error) {
        console.error('[sourcesConfig] remove failed:', error);
        const target = resolveReturnTo(req);
        return res.redirect(buildRedirectUrl(error.message || 'Не удалось удалить источник.', 'error', target));
    }
}
