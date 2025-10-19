import {
    loadSalesdriveSources,
    addSalesdriveSource,
    updateSalesdriveSource,
    removeSalesdriveSource
} from '../services/salesdriveSourcesService.js';
import { buildOverlayMeta } from '../services/reportDataService.js';

function buildRedirectUrl(message = '', type = 'success') {
    if (!message) {
        return '/reports/config/sources';
    }
    const params = new URLSearchParams();
    params.set(type, message);
    return `/reports/config/sources?${params.toString()}`;
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
            message: 'Не вдалося завантажити список джерел.',
            error
        });
    }
}

export async function handleSourceAdd(req, res) {
    try {
        const { id, ident, nameView } = req.body || {};
        await addSalesdriveSource({ id, ident, nameView });
        return res.redirect(buildRedirectUrl('Джерело додано успішно.'));
    } catch (error) {
        console.error('[sourcesConfig] add failed:', error);
        return res.redirect(buildRedirectUrl(error.message || 'Помилка додавання джерела.', 'error'));
    }
}

export async function handleSourceUpdate(req, res) {
    try {
        const { id } = req.params || {};
        const { ident, nameView } = req.body || {};
        await updateSalesdriveSource(id, { ident, nameView });
        return res.redirect(buildRedirectUrl('Зміни збережено.'));
    } catch (error) {
        console.error('[sourcesConfig] update failed:', error);
        return res.redirect(buildRedirectUrl(error.message || 'Помилка оновлення джерела.', 'error'));
    }
}

export async function handleSourceDelete(req, res) {
    try {
        const { id } = req.params || {};
        await removeSalesdriveSource(id);
        return res.redirect(buildRedirectUrl('Джерело видалено.'));
    } catch (error) {
        console.error('[sourcesConfig] remove failed:', error);
        return res.redirect(buildRedirectUrl(error.message || 'Не вдалося видалити джерело.', 'error'));
    }
}
