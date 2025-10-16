// In-memory storage for demo purposes (replace with database in production)
const feedbackHistory = [];

class FeedbackController {
  async submitFeedback(req, res) {
    try {
      const { suggestionId, rating, comment, contentType } = req.body;

      if (!suggestionId || !rating) {
        return res.status(400).json({
          error: 'Suggestion ID and rating are required'
        });
      }

      // Validate rating
      if (rating < 1 || rating > 5) {
        return res.status(400).json({
          error: 'Rating must be between 1 and 5'
        });
      }

      const feedback = {
        id: Date.now().toString(),
        suggestionId,
        rating: parseInt(rating),
        comment: comment || '',
        contentType: contentType || 'unknown',
        timestamp: new Date().toISOString()
      };

      feedbackHistory.push(feedback);

      res.json({
        success: true,
        data: {
          message: 'Feedback submitted successfully',
          feedback
        },
        metadata: {
          timestamp: feedback.timestamp,
          id: feedback.id
        }
      });
    } catch (error) {
      console.error('❌ Error submitting feedback:', error);
      res.status(500).json({
        error: 'Failed to submit feedback',
        details: error.message
      });
    }
  }

  async getFeedbackStats(req, res) {
    try {
      const stats = {
        totalFeedback: feedbackHistory.length,
        averageRating: 0,
        ratingDistribution: {
          1: 0, 2: 0, 3: 0, 4: 0, 5: 0
        },
        byContentType: {},
        recentFeedback: {
          last24Hours: 0,
          last7Days: 0,
          last30Days: 0
        }
      };

      if (feedbackHistory.length > 0) {
        const totalRating = feedbackHistory.reduce((sum, feedback) => sum + feedback.rating, 0);
        stats.averageRating = (totalRating / feedbackHistory.length).toFixed(2);

        // Calculate rating distribution
        feedbackHistory.forEach(feedback => {
          stats.ratingDistribution[feedback.rating]++;
        });

        // Calculate by content type
        feedbackHistory.forEach(feedback => {
          stats.byContentType[feedback.contentType] = (stats.byContentType[feedback.contentType] || 0) + 1;
        });

        // Calculate recent feedback
        const now = new Date();
        const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
        const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

        feedbackHistory.forEach(feedback => {
          const feedbackDate = new Date(feedback.timestamp);
          
          if (feedbackDate >= oneDayAgo) {
            stats.recentFeedback.last24Hours++;
          }
          if (feedbackDate >= sevenDaysAgo) {
            stats.recentFeedback.last7Days++;
          }
          if (feedbackDate >= thirtyDaysAgo) {
            stats.recentFeedback.last30Days++;
          }
        });
      }

      res.json({
        success: true,
        data: stats
      });
    } catch (error) {
      console.error('❌ Error getting feedback stats:', error);
      res.status(500).json({
        error: 'Failed to get feedback stats',
        details: error.message
      });
    }
  }

  async getFeedbackHistory(req, res) {
    try {
      const { contentType, rating, limit = 50, offset = 0 } = req.query;

      let filteredFeedback = feedbackHistory;

      // Filter by content type
      if (contentType) {
        filteredFeedback = filteredFeedback.filter(feedback => feedback.contentType === contentType);
      }

      // Filter by rating
      if (rating) {
        filteredFeedback = filteredFeedback.filter(feedback => feedback.rating === parseInt(rating));
      }

      // Apply pagination
      const paginatedFeedback = filteredFeedback
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
        .slice(parseInt(offset), parseInt(offset) + parseInt(limit));

      res.json({
        success: true,
        data: paginatedFeedback,
        metadata: {
          total: filteredFeedback.length,
          limit: parseInt(limit),
          offset: parseInt(offset),
          hasMore: parseInt(offset) + parseInt(limit) < filteredFeedback.length
        }
      });
    } catch (error) {
      console.error('❌ Error getting feedback history:', error);
      res.status(500).json({
        error: 'Failed to get feedback history',
        details: error.message
      });
    }
  }
}

module.exports = new FeedbackController(); 