# Repository Guidelines

## Project Structure & Module Organization
- `app.js` bootstraps Express, loads `.env` with `dotenv`, and mounts the root router.
- `routes/index.js` обробляє домашню сторінку; нові звіти додавайте у `routes/reports.js`.
- Контролери розділені за звітами (`homeController.js`, `summaryReportController.js`, `googleAdsReportController.js`, `salesDriveReportController.js`, `combinedReportController.js`, `monthlyReportController.js`), а спільна логіка винесена в `services/reportDataService.js` та `services/monthlyReportService.js`.
- Статичні місячні дані зберігаються у `data/monthly/YYYY-MM.json`, плани — у `data/plans/YYYY.json`.
- Список джерел SalesDrive зберігається у `data/config/salesdrive-sources.json` (див. `services/salesdriveSourcesService.js`); керування доступне через `/reports/config/sources`.
- `views/` містить шаблони (`home.pug`, `reports/*.pug`, `loading.pug`, `error.pug`); спільні фрагменти зберігайте у `views/partials/`.
- Runtime secrets stay in `.env`; keep the file out of version control and document required keys separately.

## Build, Test, and Development Commands
- `npm install` to pull dependencies before the first run or after lockfile changes.
- `npm start` runs `node app.js` with production-ready logging and uses live API credentials.
- `npm run dev` starts `nodemon app.js` for local work; it reloads on file changes and requires a populated `.env`.

## Environment Variables
- `SALESDRIVE_RATE_LIMIT_MAX_PER_MINUTE` (optional, default `20`): cap SalesDrive requests per minute.
- `SALESDRIVE_RATE_LIMIT_INTERVAL_MS` (optional, default `60000`): window for the limiter when the API changes its quota cadence.
- `SALESDRIVE_RATE_LIMIT_QUEUE_SIZE` (optional, default `120`): safety valve to avoid unbounded request buffering.
- `SALESDRIVE_RETRY_MAX_ATTEMPTS` (optional, default `3`): maximum resend attempts for throttled SalesDrive calls.
- `SALESDRIVE_RETRY_BASE_DELAY_MS` (optional, default `5000`): base delay for exponential backoff after a 429/5xx.
- `SALESDRIVE_HOURLY_LIMIT` (optional, default `200`): максимальна кількість запитів до SalesDrive за одну годину.
- `SALESDRIVE_DAILY_LIMIT` (optional, default `2000`): максимальна кількість запитів до SalesDrive за добу.

## Coding Style & Naming Conventions
- Stick with ES modules, `async/await`, and camelCase identifiers (`renderSummaryReport` тощо); prefer descriptive names for нових джерел та хелперів.
- Use 4-space indentation in JavaScript/JSON and 2-space indentation in Pug. Retain semicolons and keep import order logical: core modules, third-party, then local files.
- Collect environment reads near the top of a module and fail fast with clear errors when critical keys are missing.

## Testing Guidelines
- No automated suite exists yet; introduce Jest + Supertest and wire it to `npm test` when adding regression coverage.
- Until then, verify updates by running `npm run dev`, переходячи за маршрутами `/reports/*`, та готуючи мокові відповіді API за потреби. Для статичного місячного звіту використовуйте кнопки оновлення і перевіряйте збереження файлів у `data/`.

## Commit & Pull Request Guidelines
- Write concise, present-tense commit subjects (`server: fetch combined data`); avoid bundling unrelated features.
- PRs should outline intent, note affected routes/controllers, summarize manual or automated test runs, and list any new environment variables or configuration steps.
- Attach UI screenshots when modifying шаблони у `views/reports/` to show the resulting tables.

## Configuration & Security Tips
- Keep Google Ads and SalesDrive keys in local process managers or `.env`; rotate tokens after sharing access.
- Redact personally identifiable data from logs, fixtures, and issue attachments before committing or posting.
- Керування списком джерел SalesDrive виконується через `/reports/config/sources`; зміни зберігаються у JSON і підхоплюються всіма звітами.
