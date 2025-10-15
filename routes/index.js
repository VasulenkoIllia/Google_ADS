import express from 'express';
import { renderCombinedData } from '../controllers/dataController.js';

const router = express.Router();

/* GET home page. */
router.get('/', renderCombinedData);

export default router;
