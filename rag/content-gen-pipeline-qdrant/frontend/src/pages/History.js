import React, { useState } from 'react';
import { useQuery } from 'react-query';
import { 
  History as HistoryIcon, 
  FileText, 
  Search, 
  Sparkles,
  Calendar,
  Filter,
  Copy,
  Eye
} from 'lucide-react';
import { contentAPI } from '../services/api';

const HistoryPage = () => {
  const [selectedType, setSelectedType] = useState('');
  const [selectedEntry, setSelectedEntry] = useState(null);

  const { data: historyData, isLoading } = useQuery(
    ['contentHistory', selectedType],
    () => contentAPI.getHistory({ type: selectedType || undefined }),
    {
      keepPreviousData: true
    }
  );

  const formatDate = (dateString) => {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const getTypeIcon = (type) => {
    switch (type) {
      case 'generation':
        return Sparkles;
      case 'rag':
        return Search;
      case 'analysis':
        return FileText;
      default:
        return HistoryIcon;
    }
  };

  const getTypeLabel = (type) => {
    switch (type) {
      case 'generation':
        return 'Content Generation';
      case 'rag':
        return 'RAG Query';
      case 'analysis':
        return 'Data Analysis';
      default:
        return type;
    }
  };

  const copyToClipboard = (text) => {
    navigator.clipboard.writeText(text);
    // You could add a toast notification here
  };

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
        <h1 className="text-3xl font-bold text-gray-900">History</h1>
        <p className="text-gray-600 mt-2">
          View your past content suggestions and AI interactions
        </p>
      </div>

      {/* Filters */}
      <div className="card">
        <div className="flex items-center justify-between">
          <h2 className="text-xl font-semibold text-gray-900">Activity History</h2>
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2">
              <Filter className="h-4 w-4 text-gray-500" />
              <select
                value={selectedType}
                onChange={(e) => setSelectedType(e.target.value)}
                className="input-field w-auto"
              >
                <option value="">All Types</option>
                <option value="generation">Content Generation</option>
                <option value="rag">RAG Queries</option>
                <option value="analysis">Data Analysis</option>
              </select>
            </div>
          </div>
        </div>

        {/* History List */}
        <div className="mt-6 space-y-4">
          {historyData?.data?.length > 0 ? (
            historyData.data.map((entry) => {
              const Icon = getTypeIcon(entry.type);
              return (
                <div
                  key={entry.id}
                  className="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow cursor-pointer"
                  onClick={() => setSelectedEntry(entry)}
                >
                  <div className="flex items-start justify-between">
                    <div className="flex items-start space-x-3">
                      <div className="p-2 bg-primary-50 rounded-lg">
                        <Icon className="h-5 w-5 text-primary-600" />
                      </div>
                      <div className="flex-1">
                        <div className="flex items-center space-x-2">
                          <h3 className="font-semibold text-gray-900">
                            {getTypeLabel(entry.type)}
                          </h3>
                          <span className="text-xs text-gray-500">
                            {formatDate(entry.timestamp)}
                          </span>
                        </div>
                        
                        {entry.type === 'generation' && (
                          <p className="text-gray-600 text-sm mt-1">
                            Generated {entry.contentType} suggestions
                          </p>
                        )}
                        
                        {entry.type === 'rag' && (
                          <p className="text-gray-600 text-sm mt-1">
                            Query: "{entry.query?.substring(0, 100)}..."
                          </p>
                        )}
                        
                        {entry.type === 'analysis' && (
                          <p className="text-gray-600 text-sm mt-1">
                            Company data analysis completed
                          </p>
                        )}
                      </div>
                    </div>
                    
                    <div className="flex space-x-2">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedEntry(entry);
                        }}
                        className="p-1 text-gray-400 hover:text-gray-600"
                      >
                        <Eye className="h-4 w-4" />
                      </button>
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          copyToClipboard(JSON.stringify(entry, null, 2));
                        }}
                        className="p-1 text-gray-400 hover:text-gray-600"
                      >
                        <Copy className="h-4 w-4" />
                      </button>
                    </div>
                  </div>
                </div>
              );
            })
          ) : (
            <div className="text-center py-12">
              <HistoryIcon className="h-12 w-12 mx-auto mb-4 text-gray-300" />
              <p className="text-gray-500">No history found</p>
              <p className="text-sm text-gray-400 mt-1">
                Start generating content to see your history here
              </p>
            </div>
          )}
        </div>

        {/* Pagination */}
        {historyData?.metadata?.hasMore && (
          <div className="mt-6 flex justify-center">
            <button className="btn-secondary">
              Load More
            </button>
          </div>
        )}
      </div>

      {/* Detail Modal */}
      {selectedEntry && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-lg max-w-4xl w-full max-h-[90vh] overflow-y-auto">
            <div className="p-6">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-xl font-semibold text-gray-900">
                  {getTypeLabel(selectedEntry.type)} Details
                </h2>
                <button
                  onClick={() => setSelectedEntry(null)}
                  className="text-gray-400 hover:text-gray-600"
                >
                  <span className="sr-only">Close</span>
                  ×
                </button>
              </div>

              <div className="space-y-4">
                <div className="flex items-center space-x-2 text-sm text-gray-500">
                  <Calendar className="h-4 w-4" />
                  <span>{formatDate(selectedEntry.timestamp)}</span>
                </div>

                {selectedEntry.type === 'generation' && (
                  <div className="space-y-4">
                    <div>
                      <h3 className="font-medium text-gray-900 mb-2">Content Type</h3>
                      <p className="text-gray-700">{selectedEntry.contentType}</p>
                    </div>
                    
                    {selectedEntry.goals && (
                      <div>
                        <h3 className="font-medium text-gray-900 mb-2">Goals</h3>
                        <p className="text-gray-700">{selectedEntry.goals}</p>
                      </div>
                    )}

                    <div>
                      <h3 className="font-medium text-gray-900 mb-2">Suggestions</h3>
                      <div className="bg-gray-50 rounded-lg p-4">
                        <pre className="text-sm text-gray-700 whitespace-pre-wrap">
                          {JSON.stringify(selectedEntry.suggestions, null, 2)}
                        </pre>
                      </div>
                    </div>
                  </div>
                )}

                {selectedEntry.type === 'rag' && (
                  <div className="space-y-4">
                    <div>
                      <h3 className="font-medium text-gray-900 mb-2">Query</h3>
                      <p className="text-gray-700">{selectedEntry.query}</p>
                    </div>

                    <div>
                      <h3 className="font-medium text-gray-900 mb-2">Response</h3>
                      <div className="bg-gray-50 rounded-lg p-4">
                        <p className="text-gray-700 whitespace-pre-wrap">
                          {selectedEntry.response}
                        </p>
                      </div>
                    </div>

                    {selectedEntry.contextData && selectedEntry.contextData.length > 0 && (
                      <div>
                        <h3 className="font-medium text-gray-900 mb-2">Context Sources</h3>
                        <div className="space-y-2">
                          {selectedEntry.contextData.map((ctx, index) => (
                            <div key={index} className="bg-gray-50 rounded-lg p-3">
                              <div className="text-sm text-gray-600">
                                <span className="font-medium">Type:</span> {ctx.type} | 
                                <span className="font-medium ml-2">Score:</span> {ctx.score?.toFixed(3)}
                              </div>
                              <p className="text-gray-700 mt-1">{ctx.text}</p>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {selectedEntry.type === 'analysis' && (
                  <div>
                    <h3 className="font-medium text-gray-900 mb-2">Analysis Results</h3>
                    <div className="bg-gray-50 rounded-lg p-4">
                      <pre className="text-sm text-gray-700 whitespace-pre-wrap">
                        {JSON.stringify(selectedEntry.analysis, null, 2)}
                      </pre>
                    </div>
                  </div>
                )}
              </div>

              <div className="mt-6 flex justify-end space-x-3">
                <button
                  onClick={() => copyToClipboard(JSON.stringify(selectedEntry, null, 2))}
                  className="btn-secondary flex items-center"
                >
                  <Copy className="h-4 w-4 mr-2" />
                  Copy JSON
                </button>
                <button
                  onClick={() => setSelectedEntry(null)}
                  className="btn-primary"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default HistoryPage; 