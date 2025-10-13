import express from 'express';
import { getCombinedData, renderCombinedData } from '../controllers/dataController.js';

const router = express.Router();

/* GET home page. */
router.get('/', renderCombinedData);

/* GET api data. */
router.get('/api/data', getCombinedData);

export default router;
