const express = require('express');
const router = express.Router();
const feedbackController = require('../controllers/feedbackController');

// Submit feedback
router.post('/', feedbackController.submitFeedback);

// Get feedback statistics
router.get('/stats', feedbackController.getFeedbackStats);

// Get feedback history
router.get('/history', feedbackController.getFeedbackHistory);

module.exports = router; 