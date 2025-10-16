#!/bin/bash

echo "🚀 Setting up Nebius-Qdrant Content Suggestion Platform"
echo "=================================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo "✅ Docker and Docker Compose are installed"

# Create environment files if they don't exist
if [ ! -f backend/.env ]; then
    echo "📝 Creating backend environment file..."
    cp backend/env.example backend/.env
    echo "⚠️  Please update backend/.env with your API credentials"
fi

if [ ! -f frontend/.env ]; then
    echo "📝 Creating frontend environment file..."
    cp frontend/env.example frontend/.env
fi

echo ""
echo "🔧 Configuration Steps:"
echo "1. Update backend/.env with your Nebius API credentials:"
echo "   - NEBIUS_API_KEY=your_api_key_here"
echo "   - NEBIUS_FOLDER_ID=your_folder_id_here (for fallback)"
echo "   - EMBEDDING_SERVICE_API_KEY=your_openai_api_key_here"
echo ""
echo "2. Update frontend/.env if needed:"
echo "   - REACT_APP_API_BASE_URL (default: http://localhost:3001)"
echo ""

read -p "Have you updated the environment files? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "⚠️  Please update the environment files before continuing"
    exit 1
fi

echo ""
echo "🐳 Starting services with Docker Compose..."
docker-compose up -d

echo ""
echo "⏳ Waiting for services to start..."
sleep 10

# Check if services are running
echo "🔍 Checking service status..."

# Check Qdrant
if curl -s http://localhost:6333/collections > /dev/null; then
    echo "✅ Qdrant is running on http://localhost:6333"
else
    echo "❌ Qdrant is not responding"
fi

# Check Backend
if curl -s http://localhost:3001/health > /dev/null; then
    echo "✅ Backend API is running on http://localhost:3001"
else
    echo "❌ Backend API is not responding"
fi

# Check Frontend
if curl -s http://localhost:3000 > /dev/null; then
    echo "✅ Frontend is running on http://localhost:3000"
else
    echo "❌ Frontend is not responding"
fi

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📱 Access your application:"
echo "   Frontend: http://localhost:3000"
echo "   Backend API: http://localhost:3001"
echo "   Qdrant Dashboard: http://localhost:6333/dashboard"
echo ""
echo "📚 Next steps:"
echo "1. Open http://localhost:3000 in your browser"
echo "2. Go to 'Data Upload' to add your company information"
echo "3. Use 'Content Generator' to create AI-powered suggestions"
echo "4. Check 'Analytics' for insights and performance metrics"
echo ""
echo "🛠️  Useful commands:"
echo "   View logs: docker-compose logs -f"
echo "   Stop services: docker-compose down"
echo "   Restart services: docker-compose restart"
echo "   Update and restart: docker-compose up -d --build" 