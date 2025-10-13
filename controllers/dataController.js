import { OAuth2Client } from 'google-auth-library';
import axios from "axios";
import dotenv from 'dotenv';

// Завантажуємо змінні з .env файлу в process.env
dotenv.config();

// --- КОНФІГУРАЦІЯ З .env ---

// Глобальні налаштування дати
const GLOBAL_DATE_START_DAY = process.env.GLOBAL_DATE_START_DAY;
const GLOBAL_DATE_END_DAY = process.env.GLOBAL_DATE_END_DAY;

// SalesDrive
const SALESDRIVE_URL = process.env.SALESDRIVE_URL;
const SALESDRIVE_API_KEY = process.env.SALESDRIVE_API_KEY;
// Парсимо JSON рядок з .env
const SALESDRIVE_ISTOCHNIKI = JSON.parse(process.env.SALESDRIVE_ISTOCHNIKI || '[]');

// Google Ads
const CUSTOMER_ID = process.env.CUSTOMER_ID;
const LOGIN_CUSTOMER_ID = process.env.LOGIN_CUSTOMER_ID;
const DEVELOPER_TOKEN = process.env.DEVELOPER_TOKEN;
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const REFRESH_TOKEN = process.env.REFRESH_TOKEN;

// --- ФІЛЬТРИ ДЛЯ ЗАПИТІВ ---
const SALESDRIVE_FILTER_DATA = {
  page: 1,
  limit: 20,
  filter: {
    orderTime: {
      from: `${GLOBAL_DATE_START_DAY} 00:00:00`,
      to: `${GLOBAL_DATE_END_DAY} 23:59:59`,
    },
    statusId: "__ALL__",
    istocnikProdazi: process.env.SALESDRIVE_FILTER_ISTOCHNIK_ID,
  },
};

// --- SALESDRIVE API ФУНКЦІЇ ---

async function getSalesDriveOrders() {
  try {
    const headers = { 'Form-Api-Key': SALESDRIVE_API_KEY };
    const response = await axios.get(SALESDRIVE_URL, {
      headers,
      params: SALESDRIVE_FILTER_DATA,
      timeout: 30000,
    });

    console.log("Отримано заявок SalesDrive:", response.data.data.length);

    const istochnikiMap = SALESDRIVE_ISTOCHNIKI.reduce((acc, source) => {
      acc[source.id] = source.ident;
      return acc;
    }, {});

    const orders = response.data.data.map((order) => {
      const istocnikProdaziId = order.istocnikProdazi;
      const istocnikProdaziIdent = istochnikiMap[istocnikProdaziId] || "";

      return {
        source: 'SalesDrive',
        id: order.id,
        lName: order.primaryContact?.lName || "",
        fName: order.primaryContact?.fName || "",
        createTime: order.primaryContact?.createTime || "",
        comment: order.comment || "",
        orderTime: order.orderTime || "",
        istocnikProdazi: istocnikProdaziId,
        istocnikProdaziIdent: istocnikProdaziIdent,
        products: order.products[0]?.text || ""
      };
    });
    return orders;
  } catch (error) {
    console.error(
      "Помилка при запиті SalesDrive:",
      error.response?.data || error.message
    );
    return [];
  }
}

// --- GOOGLE ADS API ФУНКЦІЇ ---

async function makeGoogleAdsApiRequest(token, query, currentCustomerId) {
  const API_VERSION = 'v21';
  const url = `https://googleads.googleapis.com/${API_VERSION}/customers/${currentCustomerId}/googleAds:searchStream`;
  const headers = {
    'Content-Type': 'application/json',
    'developer-token': DEVELOPER_TOKEN,
    'login-customer-id': LOGIN_CUSTOMER_ID,
    'Authorization': `Bearer ${token}`,
  };
  const body = { query };

  try {
    const response = await axios.post(url, body, { headers });
    return response.data.flatMap(batch => batch.results || []);
  } catch (error) {
    console.error('Помилка виконання запиту до Google Ads API:', error.message);
    if (error.response) {
      console.error('Дані помилки Google Ads API:', JSON.stringify(error.response.data, null, 2));
    }
    throw error;
  }
}

async function getGoogleAdsData() {
  try {
    console.log('1. Отримую свіжий access_token для Google Ads...');
    const auth = new OAuth2Client({ clientId: CLIENT_ID, clientSecret: CLIENT_SECRET, redirectUri: 'http://localhost' });
    auth.setCredentials({ refresh_token: REFRESH_TOKEN });
    const { token } = await auth.getAccessToken();
    if (!token) throw new Error('Не вдалося отримати access_token для Google Ads.');
    console.log('Access token Google Ads успішно отримано.');

    console.log('\n2. Запитую дані товарів з Google Ads...');

    const query = `
        SELECT
            shopping_product.resource_name,
            shopping_product.status,
            metrics.cost_micros,
            shopping_product.item_id,
            shopping_product.title,
            metrics.impressions,
            metrics.clicks
        FROM shopping_product
        WHERE
            shopping_product.status = 'ELIGIBLE'
          AND segments.date BETWEEN '${GLOBAL_DATE_START_DAY}' AND '${GLOBAL_DATE_END_DAY}'
    `;

    const results = await makeGoogleAdsApiRequest(token, query, CUSTOMER_ID);

    const adsData = results.map(item => {
      const titleParts = item.shoppingProduct.title.split(' ');
      const istocnikProdaziIdent = titleParts[titleParts.length - 1];

      return {
        source: 'GoogleAds',
        istocnikProdaziIdent: istocnikProdaziIdent,
        resourceName: item.shoppingProduct.resource_name,
        status: item.shoppingProduct.status,
        costMicros: item.metrics.cost_micros,
        itemId: item.shoppingProduct.item_id,
        title: item.shoppingProduct.title,
        impressions: item.metrics.impressions,
        clicks: item.metrics.clicks
      };
    });

    console.log("✅ Отримано даних з Google Ads:", adsData.length);
    return adsData;
  } catch (err) {
    console.error('\n❌ Сталася помилка в Google Ads:', err.message);
    return [];
  }
}

// --- ФУНКЦІЯ ОБ'ЄДНАННЯ ДАНИХ ---

async function combineAllData() {
  console.log("🚀 Починаю об'єднання даних з SalesDrive та Google Ads...");
  console.log(`Використовуваний діапазон дат: ${GLOBAL_DATE_START_DAY} по ${GLOBAL_DATE_END_DAY}`);

  const googleAdsResults = await getGoogleAdsData();
  const salesDriveResults = await getSalesDriveOrders();

  const googleAdsMap = new Map();
  googleAdsResults.forEach(adItem => {
    if (!googleAdsMap.has(adItem.istocnikProdaziIdent)) {
      googleAdsMap.set(adItem.istocnikProdaziIdent, []);
    }
    googleAdsMap.get(adItem.istocnikProdaziIdent).push(adItem);
  });

  const combinedData = salesDriveResults.map(order => {
    const ident = order.istocnikProdaziIdent;
    const matchingAds = googleAdsMap.get(ident) || [];

    let googleAdsData = null;
    if (matchingAds.length > 0) {
      googleAdsData = matchingAds.reduce((acc, ad) => {
        acc.costMicros += Number(ad.costMicros) || 0;
        acc.impressions += Number(ad.impressions) || 0;
        acc.clicks += Number(ad.clicks) || 0;
        if (!acc.resourceName) {
          acc.resourceName = ad.resourceName;
          acc.status = ad.status;
          acc.itemId = ad.itemId;
          acc.title = ad.title;
        }
        return acc;
      }, {
        costMicros: 0,
        impressions: 0,
        clicks: 0,
        resourceName: '',
        status: '',
        itemId: '',
        title: ''
      });
    }

    return {
      startDate: GLOBAL_DATE_START_DAY,
      endDate: GLOBAL_DATE_END_DAY,
      ...order,
      googleAdsData: googleAdsData
    };
  });

  console.log("\n--- Об'єднані дані ---");
  console.log(`Всього об'єднаних записів: ${combinedData.length}`);
  return combinedData;
}

export const getCombinedData = async (req, res) => {
    try {
        const data = await combineAllData();
        res.json(data);
    } catch (error) {
        console.error("Помилка на сервері:", error);
        res.status(500).json({ message: "Внутрішня помилка сервера" });
    }
};

export const renderCombinedData = async (req, res) => {
    try {
        const data = await combineAllData();
        const startDate = data.length > 0 ? data[0].startDate : 'N/A';
        const endDate = data.length > 0 ? data[0].endDate : 'N/A';
        res.render('index', { data, startDate, endDate });
    } catch (error) {
        console.error("Помилка на сервері:", error);
        res.status(500).render('error', { message: "Внутрішня помилка сервера", error });
    }
};
