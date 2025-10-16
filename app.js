import express from 'express';
import indexRouter from './routes/index.js';
import dotenv from 'dotenv';

// Завантажуємо змінні з .env файлу в process.env
dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

// Налаштування шаблонізатора Pug
app.set('views', './views');
app.set('view engine', 'pug');
app.set('query parser', 'extended');

app.use('/', indexRouter);

app.listen(port, () => {
  console.log(`Сервер запущено на http://localhost:${port}`);
});
