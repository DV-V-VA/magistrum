const { QdrantClient } = require('@qdrant/js-client-rest');

class QdrantService {
  constructor() {
    this.url = process.env.QDRANT_URL || 'http://localhost:6333';
    this.apiKey = process.env.QDRANT_API_KEY || null;
    this.collectionName = process.env.QDRANT_COLLECTION_NAME || 'company_data';
    this.vectorSize = 8192; // Qwen3-Embedding-8B produces 8192-dimensional vectors
    
    // Initialize the Qdrant client
    this.client = new QdrantClient({
      url: this.url,
      apiKey: this.apiKey,
    });
  }

  async init() {
    try {
      // Check if collection exists, create if not
      const collectionExists = await this.collectionExists();
      if (!collectionExists) {
        await this.createCollection();
      }
      console.log('✅ Qdrant service initialized with official client');
    } catch (error) {
      console.error('❌ Failed to initialize Qdrant service:', error);
      throw error;
    }
  }

  async collectionExists() {
    try {
      const collection = await this.client.getCollection(this.collectionName);
      return collection.status === 'ok';
    } catch (error) {
      return false;
    }
  }

  async createCollection() {
    const collectionConfig = {
      vectors: {
        size: this.vectorSize,
        distance: 'Cosine'
      }
    };

    try {
      await this.client.createCollection(this.collectionName, collectionConfig);
      console.log(`✅ Collection '${this.collectionName}' created successfully`);
    } catch (error) {
      console.error('❌ Failed to create collection:', error.message);
      throw error;
    }
  }

  async addPoints(points) {
    try {
      const pointsData = points.map(point => ({
        id: point.id,
        vector: point.vector,
        payload: point.payload
      }));

      console.log(`🔍 Adding ${points.length} points to Qdrant...`);
      console.log(`🔍 First point sample:`, JSON.stringify(pointsData[0], null, 2));

      await this.client.upsert(this.collectionName, {
        points: pointsData
      });

      console.log(`✅ Added ${points.length} points to collection`);
      return { status: 'ok', points_count: points.length };
    } catch (error) {
      console.error('❌ Failed to add points:', error.message);
      console.error('❌ Error details:', error);
      throw error;
    }
  }

  async searchSimilar(vector, limit = 5, scoreThreshold = 0.7) {
    try {
      const searchResult = await this.client.search(this.collectionName, {
        vector: vector,
        limit: limit,
        score_threshold: scoreThreshold,
        with_payload: true
      });

      return searchResult;
    } catch (error) {
      console.error('❌ Failed to search similar vectors:', error.message);
      throw error;
    }
  }

  async deletePoint(pointId) {
    try {
      await this.client.delete(this.collectionName, {
        points: [pointId]
      });
      console.log(`✅ Deleted point ${pointId}`);
    } catch (error) {
      console.error('❌ Failed to delete point:', error.message);
      throw error;
    }
  }

  async getCollectionInfo() {
    try {
      const collection = await this.client.getCollection(this.collectionName);
      return collection;
    } catch (error) {
      console.error('❌ Failed to get collection info:', error.message);
      throw error;
    }
  }

  async clearCollection() {
    try {
      await this.client.delete(this.collectionName, {
        points_selector: {
          filter: {} // Empty filter to delete all points
        }
      });
      console.log(`✅ Cleared collection '${this.collectionName}'`);
    } catch (error) {
      console.error('❌ Failed to clear collection:', error.message);
      throw error;
    }
  }
}

module.exports = new QdrantService(); 