# Repository Guidelines

## Project Structure & Module Organization
- `app.js` bootstraps Express, loads `.env` with `dotenv`, and mounts the root router.
- `routes/index.js` exposes HTTP entry points, forwarding to controllers; add new routers under `routes/`.
- `controllers/dataController.js` aggregates Google Ads and SalesDrive data; split helpers into additional files when logic grows.
- `views/` hosts Pug templates (`index.pug` dashboard, `error.pug` fallback); store shared fragments in `views/partials/`.
- Runtime secrets stay in `.env`; keep the file out of version control and document required keys separately.

## Build, Test, and Development Commands
- `npm install` to pull dependencies before the first run or after lockfile changes.
- `npm start` runs `node app.js` with production-ready logging and uses live API credentials.
- `npm run dev` starts `nodemon app.js` for local work; it reloads on file changes and requires a populated `.env`.

## Coding Style & Naming Conventions
- Stick with ES modules, `async/await`, and camelCase identifiers (`renderCombinedData`); prefer descriptive names for new sources and helpers.
- Use 4-space indentation in JavaScript/JSON and 2-space indentation in Pug. Retain semicolons and keep import order logical: core modules, third-party, then local files.
- Collect environment reads near the top of a module and fail fast with clear errors when critical keys are missing.

## Testing Guidelines
- No automated suite exists yet; introduce Jest + Supertest and wire it to `npm test` when adding regression coverage.
- Until then, verify updates by running `npm run dev`, exercising the `/` dashboard tabs, and capturing sample payloads via mocked responses when external APIs are unavailable.

## Commit & Pull Request Guidelines
- Write concise, present-tense commit subjects (`server: fetch combined data`); avoid bundling unrelated features.
- PRs should outline intent, note affected routes/controllers, summarize manual or automated test runs, and list any new environment variables or configuration steps.
- Attach UI screenshots when modifying `views/index.pug` to show the resulting tables.

## Configuration & Security Tips
- Keep Google Ads and SalesDrive keys in local process managers or `.env`; rotate tokens after sharing access.
- Redact personally identifiable data from logs, fixtures, and issue attachments before committing or posting.
