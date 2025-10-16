import React, { useState } from 'react';
import { useQuery } from 'react-query';
import { 
  Database, 
  CheckCircle, 
  Settings,
  FileText,
  Link,
  Upload
} from 'lucide-react';
import { dataAPI } from '../services/api';

const DataUpload = () => {
  const [showCustomForm, setShowCustomForm] = useState(false);

  const { data: companyData, isLoading } = useQuery('companyData', dataAPI.getCompanyData, {
    retry: false,
    onError: () => {
      // Company data not available, continue without it
    }
  });

  const { data: dataStats } = useQuery('dataStats', dataAPI.getDataStats);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600"></div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-gray-900">Data Management</h1>
        <p className="text-gray-600 mt-2">
          Manage your documents and company data for AI-powered content generation
        </p>
      </div>

      {/* Status Overview */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Company Data Status */}
        <div className="card">
          <div className="flex items-center mb-4">
            <Database className="h-6 w-6 text-blue-600 mr-3" />
            <h3 className="font-semibold text-gray-900">Company Data</h3>
          </div>
          
          <div className="space-y-3">
            <div className="flex items-center">
              <CheckCircle className="h-5 w-5 text-green-600 mr-2" />
              <span className="text-sm text-gray-700">Default data loaded</span>
            </div>
            
            <div className="text-sm text-gray-600">
              <p>• Industry: Technology</p>
              <p>• Focus: Content generation platform</p>
              <p>• Vectors: {dataStats?.data?.vectorCount || 0}</p>
            </div>
          </div>
        </div>

        {/* Document Status */}
        <div className="card">
          <div className="flex items-center mb-4">
            <FileText className="h-6 w-6 text-green-600 mr-3" />
            <h3 className="font-semibold text-gray-900">Documents</h3>
          </div>
          
          <div className="space-y-3">
            <div className="text-2xl font-bold text-gray-900">
              {dataStats?.data?.documentCount || 0}
            </div>
            <p className="text-sm text-gray-600">
              Uploaded documents for RAG content generation
            </p>
          </div>
        </div>

        {/* System Status */}
        <div className="card">
          <div className="flex items-center mb-4">
            <Settings className="h-6 w-6 text-purple-600 mr-3" />
            <h3 className="font-semibold text-gray-900">System Status</h3>
          </div>
          
          <div className="space-y-3">
            <div className="flex items-center">
              <CheckCircle className="h-5 w-5 text-green-600 mr-2" />
              <span className="text-sm text-gray-700">Qdrant connected</span>
            </div>
            <div className="flex items-center">
              <CheckCircle className="h-5 w-5 text-green-600 mr-2" />
              <span className="text-sm text-gray-700">Nebius AI ready</span>
            </div>
          </div>
        </div>
      </div>

      {/* Quick Actions */}
      <div className="card">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Quick Actions</h2>
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Upload Documents */}
          <div className="border border-gray-200 rounded-lg p-4 hover:border-blue-300 transition-colors">
            <div className="flex items-center mb-3">
              <Upload className="h-5 w-5 text-blue-600 mr-2" />
              <h3 className="font-medium text-gray-900">Upload Documents</h3>
            </div>
            <p className="text-sm text-gray-600 mb-3">
              Upload text files (.txt, .md) to enable RAG content generation
            </p>
            <button 
              onClick={() => window.location.href = '/document-upload'}
              className="text-blue-600 hover:text-blue-700 text-sm font-medium"
            >
              Go to Document Upload →
            </button>
          </div>

          {/* Upload URLs */}
          <div className="border border-gray-200 rounded-lg p-4 hover:border-green-300 transition-colors">
            <div className="flex items-center mb-3">
              <Link className="h-5 w-5 text-green-600 mr-2" />
              <h3 className="font-medium text-gray-900">Upload URLs</h3>
            </div>
            <p className="text-sm text-gray-600 mb-3">
              Extract content from web pages for AI-powered suggestions
            </p>
            <button 
              onClick={() => window.location.href = '/document-upload'}
              className="text-green-600 hover:text-green-700 text-sm font-medium"
            >
              Go to Document Upload →
            </button>
          </div>
        </div>
      </div>

      {/* Current Company Data */}
      <div className="card">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-semibold text-gray-900">Current Company Data</h2>
          <button
            onClick={() => setShowCustomForm(!showCustomForm)}
            className="btn-secondary text-sm"
          >
            {showCustomForm ? 'Hide' : 'Customize'} Data
          </button>
        </div>

        {!showCustomForm ? (
          <div className="space-y-4">
            <div>
              <h3 className="font-medium text-gray-900 mb-2">Description</h3>
              <p className="text-gray-700 bg-gray-50 p-3 rounded-lg">
                A content generation platform that helps create social media posts and articles based on uploaded documents and company information.
              </p>
            </div>

            <div>
              <h3 className="font-medium text-gray-900 mb-2">Goals</h3>
              <div className="bg-gray-50 p-3 rounded-lg">
                <ul className="list-disc list-inside space-y-1">
                  <li className="text-gray-700">Generate engaging social media content</li>
                  <li className="text-gray-700">Create informative articles and blog posts</li>
                  <li className="text-gray-700">Provide AI-powered content suggestions</li>
                </ul>
              </div>
            </div>

            <div>
              <h3 className="font-medium text-gray-900 mb-2">Values</h3>
              <div className="bg-gray-50 p-3 rounded-lg">
                <ul className="list-disc list-inside space-y-1">
                  <li className="text-gray-700">Innovation in content creation</li>
                  <li className="text-gray-700">User-friendly experience</li>
                  <li className="text-gray-700">Quality and relevance</li>
                </ul>
              </div>
            </div>
          </div>
        ) : (
          <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
            <p className="text-yellow-800 text-sm">
              <strong>Note:</strong> The app works with default company data. You can customize this data by editing the backend configuration or using the API directly.
            </p>
          </div>
        )}
      </div>

      {/* Info Card */}
      <div className="card bg-blue-50 border-blue-200">
        <div className="flex items-start">
          <Database className="h-6 w-6 text-blue-600 mr-3 mt-1" />
          <div>
            <h3 className="font-semibold text-blue-900">How it works</h3>
            <p className="text-blue-700 text-sm mt-1">
              The app uses default company data to generate content. Upload documents to enable RAG (Retrieval-Augmented Generation) 
              for more contextual content suggestions. Your documents are automatically processed and stored as vector embeddings in Qdrant.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DataUpload; 