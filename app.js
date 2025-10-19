import express from 'express';
import cookieParser from 'cookie-parser';
import indexRouter from './routes/index.js';
import reportsRouter from './routes/reports.js';
import dotenv from 'dotenv';
import { ensureDataDirectories } from './services/monthlyReportService.js';
import { loadSalesdriveSources } from './services/salesdriveSourcesService.js';
import { initializeMonthlyScheduler } from './services/monthlyScheduleService.js';
import { pinAccessMiddleware } from './middlewares/pinAccessMiddleware.js';

// Завантажуємо змінні з .env файлу в process.env
dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

// Налаштування шаблонізатора Pug
app.set('views', './views');
app.set('view engine', 'pug');
app.set('query parser', 'extended');

app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(cookieParser(process.env.COOKIE_SECRET || 'reports-cookie-secret'));

app.get('/healthz', (req, res) => {
  res.status(200).json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    uptimeSeconds: process.uptime()
  });
});

app.use(pinAccessMiddleware);

app.use('/', indexRouter);
app.use('/reports', reportsRouter);

Promise.all([ensureDataDirectories(), loadSalesdriveSources(), initializeMonthlyScheduler()])
  .catch(error => {
    console.error('[monthlyReport] Не вдалося підготувати каталоги даних:', error);
  });

app.listen(port, () => {
  console.log(`Сервер запущено на http://localhost:${port}`);
});
