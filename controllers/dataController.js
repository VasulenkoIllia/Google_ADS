import { OAuth2Client } from 'google-auth-library';
import axios from "axios";
import dotenv from 'dotenv';

dotenv.config();

// --- CONFIG FROM .env ---
const {
    GLOBAL_DATE_START_DAY: DEFAULT_DATE_START_DAY,
    GLOBAL_DATE_END_DAY: DEFAULT_DATE_END_DAY,
    SALESDRIVE_URL,
    SALESDRIVE_API_KEY,
    CUSTOMER_ID,
    LOGIN_CUSTOMER_ID,
    DEVELOPER_TOKEN,
    CLIENT_ID,
    CLIENT_SECRET,
    REFRESH_TOKEN
} = process.env;

const SALESDRIVE_ISTOCHNIKI = JSON.parse(process.env.SALESDRIVE_ISTOCHNIKI || '[]');

const DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;

class DateRangeError extends Error {
    constructor(message) {
        super(message);
        this.name = 'DateRangeError';
        this.statusCode = 400;
    }
}

function coerceQueryParam(value) {
    if (Array.isArray(value)) {
        return value[0];
    }
    return value;
}

function resolveDateRange(requestedStart, requestedEnd) {
    if (!DEFAULT_DATE_START_DAY || !DEFAULT_DATE_END_DAY) {
        throw new Error('GLOBAL_DATE_START_DAY and GLOBAL_DATE_END_DAY must be set in the environment.');
    }

    const normalizedStart = coerceQueryParam(requestedStart);
    const normalizedEnd = coerceQueryParam(requestedEnd);

    const startDate = normalizedStart && normalizedStart.trim() ? normalizedStart.trim() : DEFAULT_DATE_START_DAY;
    const endDate = normalizedEnd && normalizedEnd.trim() ? normalizedEnd.trim() : DEFAULT_DATE_END_DAY;

    if (!DATE_REGEX.test(startDate)) {
        throw new DateRangeError('startDate must be provided in YYYY-MM-DD format.');
    }

    if (!DATE_REGEX.test(endDate)) {
        throw new DateRangeError('endDate must be provided in YYYY-MM-DD format.');
    }

    if (startDate > endDate) {
        throw new DateRangeError('startDate cannot be later than endDate.');
    }

    return { startDate, endDate };
}

// --- GOOGLE ADS API FUNCTIONS ---

async function getGoogleAdsData({ startDate, endDate }) {
    try {
        console.log('1. Getting a fresh access_token for Google Ads...');
        const auth = new OAuth2Client({ clientId: CLIENT_ID, clientSecret: CLIENT_SECRET, redirectUri: 'http://localhost' });
        auth.setCredentials({ refresh_token: REFRESH_TOKEN });
        const { token } = await auth.getAccessToken();
        if (!token) throw new Error('Failed to get access_token for Google Ads.');
        console.log('Access token for Google Ads obtained successfully.');
        console.log('\n2. Requesting product data from Google Ads...');


        const query = `
            SELECT shopping_product.item_id, shopping_product.title, metrics.cost_micros, metrics.impressions, metrics.clicks
            FROM shopping_product
            WHERE shopping_product.status = 'ELIGIBLE'
              AND segments.date BETWEEN '${startDate}' AND '${endDate}'
        `;

        const url = `https://googleads.googleapis.com/v21/customers/${CUSTOMER_ID}/googleAds:searchStream`;
        const headers = {
            'Content-Type': 'application/json',
            'developer-token': DEVELOPER_TOKEN,
            'login-customer-id': LOGIN_CUSTOMER_ID,
            'Authorization': `Bearer ${token}`,
        };

        const response = await axios.post(url, { query }, { headers });
        const results = response.data.flatMap(batch => batch.results || []);

        const adsData = results.map(item => {
            const titleParts = item.shoppingProduct.title.split(' ');
            const istocnikProdaziIdent = titleParts[titleParts.length - 1];
            return {
                istocnikProdaziIdent: istocnikProdaziIdent,
                title: item.shoppingProduct.title,
                costMicros: BigInt(item.metrics.costMicros || 0),
                impressions: BigInt(item.metrics.impressions || 0),
                clicks: BigInt(item.metrics.clicks || 0),
            };
        });

        console.log(`✅ Received ${adsData.length} records from Google Ads.`);

        // Aggregate Google Ads data by identifier
        const aggregatedAds = adsData.reduce((acc, ad) => {
            const ident = ad.istocnikProdaziIdent;
            if (!acc[ident]) {
                acc[ident] = {
                    title: ad.title, // Keep the title from the first ad encountered
                    costMicros: 0n,
                    impressions: 0n,
                    clicks: 0n,
                };
            }
            acc[ident].costMicros += ad.costMicros;
            acc[ident].impressions += ad.impressions;
            acc[ident].clicks += ad.clicks;
            return acc;
        }, {});

        // Convert BigInt to string for JSON serialization
        Object.keys(aggregatedAds).forEach(key => {
            aggregatedAds[key].costMicros = aggregatedAds[key].costMicros.toString();
            aggregatedAds[key].impressions = aggregatedAds[key].impressions.toString();
            aggregatedAds[key].clicks = aggregatedAds[key].clicks.toString();
        });

        return aggregatedAds;
    } catch (err) {
        console.error('\n❌ An error occurred in Google Ads:', err.response ? JSON.stringify(err.response.data, null, 2) : err.message);
        return {};
    }
}

// --- SALESDRIVE API FUNCTIONS ---

async function getSalesDriveOrdersForSource(sourceId, { startDate, endDate }) {
    const params = {
        page: 1,
        limit: 500,
        filter: {
            orderTime: {
                from: `${startDate} 00:00:00`,
                to: `${endDate} 23:59:59`,
            },
            statusId: "__ALL__",
            istocnikProdazi: sourceId,
        },
    };

    try {
        const response = await axios.get(SALESDRIVE_URL, {
            headers: { 'Form-Api-Key': SALESDRIVE_API_KEY },
            params,
            timeout: 30000,
        });
        return response.data.data || [];
    } catch (error) {
        console.error(`Error fetching SalesDrive orders for source ${sourceId}:`, error.response?.data || error.message);
        return [];
    }
}

// --- DATA COMBINING FUNCTION ---

async function prepareAllDataForView({ startDate, endDate }) {
    console.log("🚀 Starting data preparation for all views...");
    console.log(`Using date range: ${startDate} to ${endDate}`);

    const googleAdsDataMap = await getGoogleAdsData({ startDate, endDate });
    let allOrders = [];

    for (const source of SALESDRIVE_ISTOCHNIKI) {
        console.log(`\nProcessing source: ${source.ident} (ID: ${source.id})`);
        const orders = await getSalesDriveOrdersForSource(source.id, { startDate, endDate });
        console.log(`Found ${orders.length} orders in SalesDrive.`);

        const ordersWithSource = orders.map(order => ({
            ...order,
            istocnikProdaziIdent: source.ident // Ensure identifier is attached
        }));
        allOrders = allOrders.concat(ordersWithSource);
    }

    // 1. Prepare Google Ads data for its own tab
    const googleAdsDataForTable = Object.entries(googleAdsDataMap).map(([ident, ads]) => ({
        istocnikProdaziIdent: ident,
        ...ads
    }));

    // 2. Prepare SalesDrive data for its own tab
    const salesDriveData = allOrders.map(order => ({
        id: order.id,
        fName: order.primaryContact?.fName || "",
        lName: order.primaryContact?.lName || "",
        orderTime: order.orderTime || "",
        products: order.products?.[0]?.text || "N/A",
        statusName: order.statusName || "N/A",
        totalCost: order.totalCost || 0,
        istocnikProdaziIdent: order.istocnikProdaziIdent,
    }));

    // 3. Prepare Combined data for the third tab
    const combinedData = allOrders.map(order => {
        const googleAdsData = googleAdsDataMap[order.istocnikProdaziIdent] || null;
        return {
            id: order.id,
            fName: order.primaryContact?.fName || "",
            lName: order.primaryContact?.lName || "",
            orderTime: order.orderTime || "",
            istocnikProdaziIdent: order.istocnikProdaziIdent,
            products: order.products?.[0]?.text || "N/A",
            statusName: order.statusName || "N/A",
            totalCost: order.totalCost || 0,
            googleAdsData: googleAdsData
        };
    });

    console.log(`\n--- Data Preparation Complete ---`);
    return { googleAdsData: googleAdsDataForTable, salesDriveData, combinedData };
}

// --- CONTROLLER EXPORTS ---

export const renderCombinedData = async (req, res) => {
    try {
        const { startDate, endDate } = resolveDateRange(req.query.startDate, req.query.endDate);
        const { googleAdsData, salesDriveData, combinedData } = await prepareAllDataForView({ startDate, endDate });
        res.render('index', {
            googleAdsData,
            salesDriveData,
            combinedData,
            startDate,
            endDate
        });
    } catch (error) {
        console.error("Server Error:", error);
        if (error instanceof DateRangeError || error.statusCode === 400) {
            const includeStack = (process.env.NODE_ENV || '').toLowerCase() !== 'production';
            return res.status(400).render('error', {
                message: error.message,
                error: includeStack ? error : {}
            });
        }
        res.status(500).render('error', { message: "Internal Server Error", error });
    }
};
