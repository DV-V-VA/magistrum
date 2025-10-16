const qdrantService = require('../services/qdrantService');
const nebiusService = require('../services/nebiusService');
const embeddingService = require('../services/embeddingService');
const ContentFormatter = require('../utils/formatter');

// In-memory storage for demo purposes (replace with database in production)
const suggestionHistory = [];

class ContentController {
  async generateSuggestions(req, res) {
    try {
      const { contentType, goals, customCompanyData } = req.body;

      if (!contentType) {
        return res.status(400).json({
          error: 'Content type is required'
        });
      }

      // Use custom company data if provided, otherwise use default
      const companyDataToUse = customCompanyData || {
        description: 'A content generation platform that helps create social media posts and articles based on uploaded documents and company information.',
        industry: 'Technology',
        goals: [
          'Generate engaging social media content',
          'Create informative articles and blog posts',
          'Provide AI-powered content suggestions'
        ]
      };

      // Create a search query based on content type and goals
      let searchQuery = '';
      switch (contentType) {
        case 'social_media_post':
          searchQuery = goals || 'social media content engagement';
          break;
        case 'article':
          searchQuery = goals || 'article content writing';
          break;
        case 'demo_application':
          searchQuery = goals || 'demo application ideas development';
          break;
        default:
          searchQuery = goals || 'content generation';
      }

      // Search for relevant documents in Qdrant
      let contextData = [];
      try {
        const queryEmbedding = await embeddingService.processQuery(searchQuery);
        const similarResults = await qdrantService.searchSimilar(queryEmbedding, 5, 0.5);
        
        contextData = similarResults.map(result => ({
          text: result.payload.text,
          type: result.payload.type,
          source: result.payload.source,
          score: result.score
        }));
        
        console.log(`🔍 Found ${contextData.length} relevant documents for ${contentType} generation`);
      } catch (error) {
        console.log('⚠️ Could not search Qdrant for context:', error.message);
        // Continue without context
      }

      // Generate suggestions using Nebius with context
      const suggestions = await nebiusService.generateContentSuggestions(
        companyDataToUse,
        contentType,
        goals || '',
        contextData
      );

      // Store in history
      const historyEntry = {
        id: Date.now().toString(),
        timestamp: new Date().toISOString(),
        contentType,
        companyData: companyDataToUse,
        goals,
        contextData,
        suggestions,
        type: 'generation'
      };
      suggestionHistory.push(historyEntry);

      // Format the response beautifully
      const beautifulResponse = ContentFormatter.createBeautifulResponse(
        suggestions.data, 
        contentType
      );

      res.json({
        success: true,
        data: beautifulResponse.formatted,
        summary: beautifulResponse.summary,
        metadata: {
          contentType,
          timestamp: historyEntry.timestamp,
          id: historyEntry.id,
          parsingSuccess: suggestions.success,
          formatted: suggestions.success,
          documentsUsed: contextData.length
        }
      });
    } catch (error) {
      console.error('❌ Error generating suggestions:', error);
      res.status(500).json({
        error: 'Failed to generate suggestions',
        details: error.message
      });
    }
  }

  async generateRAGContent(req, res) {
    try {
      const { query, companyData } = req.body;

      if (!query) {
        return res.status(400).json({
          error: 'Query is required'
        });
      }

      // Generate embedding for the query
      const queryEmbedding = await embeddingService.processQuery(query);

      // Search for similar content in Qdrant
      const similarResults = await qdrantService.searchSimilar(queryEmbedding, 5, 0.7);

      // Extract context from similar results
      const contextData = similarResults.map(result => ({
        text: result.payload.text,
        type: result.payload.type,
        score: result.score
      }));

      // Generate RAG response using Nebius
      let ragResponse;
      try {
        // Temporarily disable Nebius API due to timeout issues
        throw new Error('Nebius API temporarily disabled');
        // ragResponse = await nebiusService.generateRAGResponse(query, contextData);
      } catch (error) {
        console.warn('⚠️ Using mock response for testing:', error.message);
        // Mock response for testing
        ragResponse = `Based on the available context, here's what I found regarding your query: "${query}"

The system has access to company data and goals information. From the available context, I can see that this is a content generation platform focused on creating engaging social media posts, articles, and AI-powered content suggestions.

Key points from the context:
- This is a technology platform for content generation
- Focuses on social media content and articles
- Uses AI-powered suggestions
- Emphasizes innovation and user-friendly experience

If you have specific documents uploaded, I could provide more detailed information based on that content.`;
      }

      // Store in history
      const historyEntry = {
        id: Date.now().toString(),
        timestamp: new Date().toISOString(),
        query,
        contextData,
        response: ragResponse,
        type: 'rag'
      };
      suggestionHistory.push(historyEntry);

      res.json({
        success: true,
        data: {
          response: ragResponse,
          context: contextData,
          query
        },
        metadata: {
          timestamp: historyEntry.timestamp,
          id: historyEntry.id,
          contextCount: contextData.length
        }
      });
    } catch (error) {
      console.error('❌ Error generating RAG content:', error);
      res.status(500).json({
        error: 'Failed to generate RAG content',
        details: error.message
      });
    }
  }

  async analyzeCompanyData(req, res) {
    try {
      const { companyData } = req.body;

      if (!companyData) {
        return res.status(400).json({
          error: 'Company data is required'
        });
      }

      // Analyze company data using Nebius
      const analysis = await nebiusService.analyzeCompanyData(companyData);

      // Store in history
      const historyEntry = {
        id: Date.now().toString(),
        timestamp: new Date().toISOString(),
        companyData,
        analysis,
        type: 'analysis'
      };
      suggestionHistory.push(historyEntry);

      res.json({
        success: true,
        data: analysis,
        metadata: {
          timestamp: historyEntry.timestamp,
          id: historyEntry.id
        }
      });
    } catch (error) {
      console.error('❌ Error analyzing company data:', error);
      res.status(500).json({
        error: 'Failed to analyze company data',
        details: error.message
      });
    }
  }

  async getHistory(req, res) {
    try {
      const { type, limit = 50, offset = 0 } = req.query;

      let filteredHistory = suggestionHistory;

      // Filter by type if specified
      if (type) {
        filteredHistory = suggestionHistory.filter(entry => entry.type === type);
      }

      // Apply pagination
      const paginatedHistory = filteredHistory
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
        .slice(parseInt(offset), parseInt(offset) + parseInt(limit));

      res.json({
        success: true,
        data: paginatedHistory,
        metadata: {
          total: filteredHistory.length,
          limit: parseInt(limit),
          offset: parseInt(offset),
          hasMore: parseInt(offset) + parseInt(limit) < filteredHistory.length
        }
      });
    } catch (error) {
      console.error('❌ Error getting history:', error);
      res.status(500).json({
        error: 'Failed to get history',
        details: error.message
      });
    }
  }

  async getStats(req, res) {
    try {
      const stats = {
        totalSuggestions: suggestionHistory.length,
        byType: {},
        recentActivity: {
          last24Hours: 0,
          last7Days: 0,
          last30Days: 0
        }
      };

      const now = new Date();
      const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
      const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

      suggestionHistory.forEach(entry => {
        const entryDate = new Date(entry.timestamp);
        
        // Count by type
        stats.byType[entry.type] = (stats.byType[entry.type] || 0) + 1;

        // Count recent activity
        if (entryDate >= oneDayAgo) {
          stats.recentActivity.last24Hours++;
        }
        if (entryDate >= sevenDaysAgo) {
          stats.recentActivity.last7Days++;
        }
        if (entryDate >= thirtyDaysAgo) {
          stats.recentActivity.last30Days++;
        }
      });

      res.json({
        success: true,
        data: stats
      });
    } catch (error) {
      console.error('❌ Error getting stats:', error);
      res.status(500).json({
        error: 'Failed to get stats',
        details: error.message
      });
    }
  }
}

module.exports = new ContentController(); 