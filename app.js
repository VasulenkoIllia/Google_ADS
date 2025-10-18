import express from 'express';
import indexRouter from './routes/index.js';
import reportsRouter from './routes/reports.js';
import dotenv from 'dotenv';
import { ensureDataDirectories, rebuildMonthlyFacts } from './services/monthlyReportService.js';

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

app.use('/', indexRouter);
app.use('/reports', reportsRouter);

function scheduleMonthlyJob() {
  const scheduleNextRun = () => {
    const now = new Date();
    const next = new Date(now);
    next.setHours(1, 0, 0, 0);
    if (next <= now) {
      next.setDate(next.getDate() + 1);
    }
    const delay = next.getTime() - now.getTime();
    setTimeout(async () => {
      try {
        const runDate = new Date();
        const year = runDate.getFullYear();
        const month = runDate.getMonth() + 1;
        await rebuildMonthlyFacts(year, month, { asOf: runDate });
        console.log(`[monthlyReport] Автоматичне оновлення для ${year}-${String(month).padStart(2, '0')} завершено.`);
      } catch (error) {
        console.error('[monthlyReport] Помилка автоматичного оновлення:', error);
      } finally {
        scheduleNextRun();
      }
    }, delay);
  };
  scheduleNextRun();
}

ensureDataDirectories()
  .then(() => {
    scheduleMonthlyJob();
  })
  .catch(error => {
    console.error('[monthlyReport] Не вдалося підготувати каталоги даних:', error);
  });

app.listen(port, () => {
  console.log(`Сервер запущено на http://localhost:${port}`);
});
