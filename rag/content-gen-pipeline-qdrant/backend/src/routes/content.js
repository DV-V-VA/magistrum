const express = require('express');
const router = express.Router();
const contentController = require('../controllers/contentController');

// Generate content suggestions
router.post('/suggest', contentController.generateSuggestions);

// Get suggestion history
router.get('/history', contentController.getHistory);

// RAG-based content generation
router.post('/rag', contentController.generateRAGContent);

// Analyze company data
router.post('/analyze', contentController.analyzeCompanyData);

// Get content statistics
router.get('/stats', contentController.getStats);

module.exports = router; 