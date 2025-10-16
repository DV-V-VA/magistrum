const axios = require('axios');
require('dotenv').config();

// Example script demonstrating RAG usage with Qdrant
class RAGExample {
  constructor() {
    this.baseUrl = process.env.BACKEND_URL || 'http://localhost:3001';
  }

  async runExample() {
    console.log('🚀 Starting RAG Example with Qdrant\n');

    try {
      // Step 1: Upload company data
      await this.uploadCompanyData();

      // Step 2: Test RAG queries
      await this.testRAGQueries();

      // Step 3: Check data stats
      await this.checkStats();

    } catch (error) {
      console.error('❌ Example failed:', error.message);
    }
  }

  async uploadCompanyData() {
    console.log('📤 Step 1: Uploading Company Data...');

    const companyData = {
      description: "TechCorp is a leading artificial intelligence company that specializes in developing cutting-edge machine learning solutions for enterprise clients. We focus on natural language processing, computer vision, and predictive analytics.",
      goals: [
        "Increase market share by 25% in the next fiscal year",
        "Launch our new AI-powered analytics platform by Q3",
        "Expand operations to three new European markets",
        "Achieve 99.9% customer satisfaction rate",
        "Reduce operational costs by 15% through AI automation"
      ],
      targets: [
        "Enterprise-level corporations with 1000+ employees",
        "Healthcare organizations seeking AI-driven diagnostics",
        "Financial institutions requiring fraud detection",
        "E-commerce platforms needing recommendation engines",
        "Manufacturing companies implementing Industry 4.0"
      ],
      products: [
        "AI Analytics Platform - Real-time business intelligence with predictive insights",
        "NLP Processing Engine - Advanced text analysis and sentiment detection",
        "Computer Vision Suite - Image recognition and object detection",
        "Predictive Analytics Tool - Forecasting and trend analysis",
        "AI Consulting Services - Custom AI solution development"
      ],
      industry: "Artificial Intelligence and Machine Learning",
      values: [
        "Innovation at the core of everything we do",
        "Customer success is our primary metric",
        "Data-driven decision making",
        "Ethical AI development and deployment",
        "Continuous learning and improvement"
      ]
    };

    try {
      const response = await axios.post(`${this.baseUrl}/api/data/upload`, {
        companyData
      });

      console.log('✅ Company data uploaded successfully!');
      console.log(`📊 Vector count: ${response.data.data.vectorCount}`);
      console.log(`🆔 Data ID: ${response.data.metadata.id}\n`);

    } catch (error) {
      console.error('❌ Failed to upload company data:', error.response?.data?.error || error.message);
      throw error;
    }
  }

  async testRAGQueries() {
    console.log('🔍 Step 2: Testing RAG Queries...\n');

    const queries = [
      "What are our main business goals for this year?",
      "Who are our target customers?",
      "What AI products do we offer?",
      "What is our company's approach to innovation?",
      "How do we ensure customer success?",
      "What are our core values as a company?"
    ];

    for (let i = 0; i < queries.length; i++) {
      const query = queries[i];
      console.log(`\n📝 Query ${i + 1}: "${query}"`);
      
      try {
        const response = await axios.post(`${this.baseUrl}/api/content/rag`, {
          query,
          companyData: {}
        });

        const data = response.data.data;
        
        console.log('🤖 Generated Response:');
        console.log(`   ${data.response}\n`);
        
        console.log('📚 Retrieved Context:');
        data.context.forEach((ctx, index) => {
          console.log(`   ${index + 1}. [${ctx.type}] Score: ${ctx.score.toFixed(3)}`);
          console.log(`      ${ctx.text.substring(0, 100)}...`);
        });
        
        console.log('─'.repeat(80));

      } catch (error) {
        console.error(`❌ Query failed: ${error.response?.data?.error || error.message}`);
      }
    }
  }

  async checkStats() {
    console.log('\n📊 Step 3: Checking Data Statistics...');

    try {
      const response = await axios.get(`${this.baseUrl}/api/data/stats`);
      const stats = response.data.data;

      console.log('✅ Data Statistics:');
      console.log(`   Has company data: ${stats.hasCompanyData}`);
      console.log(`   Vector count: ${stats.vectorCount}`);
      console.log(`   Collection size: ${stats.collectionSize}`);
      console.log(`   Qdrant status: ${stats.qdrantStatus}`);
      console.log(`   Last upload: ${stats.lastUpload}`);
      
      if (stats.lastUpdate) {
        console.log(`   Last update: ${stats.lastUpdate}`);
      }

    } catch (error) {
      console.error('❌ Failed to get stats:', error.response?.data?.error || error.message);
    }
  }

  async testAdvancedFeatures() {
    console.log('\n🔧 Testing Advanced RAG Features...');

    // Test with different search parameters
    const advancedQuery = {
      query: "What AI solutions do we provide for healthcare?",
      searchParams: {
        limit: 10,
        scoreThreshold: 0.5
      }
    };

    try {
      const response = await axios.post(`${this.baseUrl}/api/content/rag`, advancedQuery);
      console.log('✅ Advanced query completed successfully');
      
      const data = response.data.data;
      console.log(`📊 Retrieved ${data.context.length} context items`);
      console.log(`🎯 Average score: ${(data.context.reduce((sum, ctx) => sum + ctx.score, 0) / data.context.length).toFixed(3)}`);

    } catch (error) {
      console.error('❌ Advanced features test failed:', error.response?.data?.error || error.message);
    }
  }
}

// Helper function to test Qdrant connection
async function testQdrantConnection() {
  console.log('🔗 Testing Qdrant Connection...');
  
  try {
    const response = await axios.get('http://localhost:6333/collections');
    console.log('✅ Qdrant is running and accessible');
    console.log(`📚 Available collections: ${response.data.collections.length}`);
    
    return true;
  } catch (error) {
    console.error('❌ Qdrant connection failed:', error.message);
    console.log('💡 Make sure Qdrant is running: docker run -p 6333:6333 qdrant/qdrant:latest');
    return false;
  }
}

// Helper function to test backend connection
async function testBackendConnection() {
  console.log('🔗 Testing Backend Connection...');
  
  try {
    const response = await axios.get('http://localhost:3001/api/data/stats');
    console.log('✅ Backend is running and accessible');
    return true;
  } catch (error) {
    console.error('❌ Backend connection failed:', error.message);
    console.log('💡 Make sure the backend is running: npm start');
    return false;
  }
}

// Main execution
async function main() {
  console.log('🎯 RAG Implementation Example with Qdrant\n');
  
  // Test connections first
  const qdrantOk = await testQdrantConnection();
  const backendOk = await testBackendConnection();
  
  if (!qdrantOk || !backendOk) {
    console.log('\n❌ Prerequisites not met. Please start the required services.');
    return;
  }
  
  console.log('\n' + '='.repeat(80));
  
  // Run the example
  const example = new RAGExample();
  await example.runExample();
  
  // Test advanced features
  await example.testAdvancedFeatures();
  
  console.log('\n🎉 RAG Example completed successfully!');
  console.log('\n💡 Next steps:');
  console.log('   1. Try the web interface at http://localhost:3000');
  console.log('   2. Experiment with different queries');
  console.log('   3. Upload your own company data');
  console.log('   4. Customize the RAG parameters');
}

// Run if this file is executed directly
if (require.main === module) {
  main().catch(console.error);
}

module.exports = RAGExample; 