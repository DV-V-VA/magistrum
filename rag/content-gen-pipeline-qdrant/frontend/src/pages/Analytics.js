import React from 'react';
import { useQuery } from 'react-query';
import { 
  BarChart3, 
  TrendingUp, 
  Users, 
  FileText,
  Target,
  Activity,
  Calendar,
  PieChart,
  BarChart,
  LineChart
} from 'lucide-react';
import { contentAPI, dataAPI, feedbackAPI } from '../services/api';

const Analytics = () => {
  const { data: contentStats, isLoading: contentLoading } = useQuery(
    'contentStats',
    contentAPI.getStats
  );

  const { data: dataStats, isLoading: dataLoading } = useQuery(
    'dataStats',
    dataAPI.getDataStats
  );

  const { data: feedbackStats, isLoading: feedbackLoading } = useQuery(
    'feedbackStats',
    feedbackAPI.getFeedbackStats
  );

  const isLoading = contentLoading || dataLoading || feedbackLoading;

  const formatNumber = (num) => {
    return new Intl.NumberFormat().format(num);
  };

  const getPercentage = (value, total) => {
    if (total === 0) return 0;
    return ((value / total) * 100).toFixed(1);
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
        <h1 className="text-3xl font-bold text-gray-900">Analytics</h1>
        <p className="text-gray-600 mt-2">
          Detailed insights and performance metrics for your content platform
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="card">
          <div className="flex items-center">
            <div className="p-3 bg-blue-50 rounded-lg">
              <FileText className="h-6 w-6 text-blue-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Total Suggestions</p>
              <p className="text-2xl font-bold text-gray-900">
                {formatNumber(contentStats?.data?.totalSuggestions || 0)}
              </p>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="flex items-center">
            <div className="p-3 bg-green-50 rounded-lg">
              <Target className="h-6 w-6 text-green-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Vector Count</p>
              <p className="text-2xl font-bold text-gray-900">
                {formatNumber(dataStats?.data?.vectorCount || 0)}
              </p>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="flex items-center">
            <div className="p-3 bg-purple-50 rounded-lg">
              <Activity className="h-6 w-6 text-purple-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">24h Activity</p>
              <p className="text-2xl font-bold text-gray-900">
                {formatNumber(contentStats?.data?.recentActivity?.last24Hours || 0)}
              </p>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="flex items-center">
            <div className="p-3 bg-orange-50 rounded-lg">
              <Users className="h-6 w-6 text-orange-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Avg Rating</p>
              <p className="text-2xl font-bold text-gray-900">
                {feedbackStats?.data?.averageRating || '0.0'}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Content Type Distribution */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="card">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">Content Type Distribution</h2>
          <div className="space-y-4">
            {contentStats?.data?.byType ? (
              Object.entries(contentStats.data.byType).map(([type, count]) => (
                <div key={type} className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    <div className="w-3 h-3 bg-primary-600 rounded-full"></div>
                    <span className="font-medium text-gray-900 capitalize">
                      {type.replace(/([A-Z])/g, ' $1').trim()}
                    </span>
                  </div>
                  <div className="flex items-center space-x-2">
                    <div className="w-32 bg-gray-200 rounded-full h-2">
                      <div
                        className="bg-primary-600 h-2 rounded-full"
                        style={{
                          width: `${getPercentage(count, contentStats.data.totalSuggestions)}%`
                        }}
                      ></div>
                    </div>
                    <span className="text-sm text-gray-600 w-12 text-right">
                      {count}
                    </span>
                  </div>
                </div>
              ))
            ) : (
              <div className="text-center py-8 text-gray-500">
                <PieChart className="h-12 w-12 mx-auto mb-4 text-gray-300" />
                <p>No content type data available</p>
              </div>
            )}
          </div>
        </div>

        {/* Recent Activity */}
        <div className="card">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">Recent Activity</h2>
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <span className="text-gray-600">Last 24 hours</span>
              <span className="font-semibold text-gray-900">
                {contentStats?.data?.recentActivity?.last24Hours || 0}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-gray-600">Last 7 days</span>
              <span className="font-semibold text-gray-900">
                {contentStats?.data?.recentActivity?.last7Days || 0}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-gray-600">Last 30 days</span>
              <span className="font-semibold text-gray-900">
                {contentStats?.data?.recentActivity?.last30Days || 0}
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Feedback Analytics */}
      {feedbackStats?.data && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="card">
            <h2 className="text-xl font-semibold text-gray-900 mb-4">Rating Distribution</h2>
            <div className="space-y-3">
              {[5, 4, 3, 2, 1].map((rating) => {
                const count = feedbackStats.data.ratingDistribution[rating] || 0;
                const percentage = getPercentage(count, feedbackStats.data.totalFeedback);
                return (
                  <div key={rating} className="flex items-center space-x-3">
                    <div className="flex items-center space-x-1 w-8">
                      <span className="text-sm font-medium text-gray-600">{rating}</span>
                      <span className="text-yellow-500">★</span>
                    </div>
                    <div className="flex-1 bg-gray-200 rounded-full h-2">
                      <div
                        className="bg-yellow-500 h-2 rounded-full"
                        style={{ width: `${percentage}%` }}
                      ></div>
                    </div>
                    <span className="text-sm text-gray-600 w-16 text-right">
                      {count} ({percentage}%)
                    </span>
                  </div>
                );
              })}
            </div>
          </div>

          <div className="card">
            <h2 className="text-xl font-semibold text-gray-900 mb-4">Feedback by Content Type</h2>
            <div className="space-y-3">
              {feedbackStats.data.byContentType ? (
                Object.entries(feedbackStats.data.byContentType).map(([type, count]) => (
                  <div key={type} className="flex items-center justify-between">
                    <span className="font-medium text-gray-900 capitalize">
                      {type.replace(/([A-Z])/g, ' $1').trim()}
                    </span>
                    <span className="text-sm text-gray-600">{count} feedback</span>
                  </div>
                ))
              ) : (
                <div className="text-center py-8 text-gray-500">
                  <BarChart className="h-12 w-12 mx-auto mb-4 text-gray-300" />
                  <p>No feedback data available</p>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* System Status */}
      <div className="card">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">System Status</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="flex items-center space-x-3">
            <div className={`w-3 h-3 rounded-full ${dataStats?.data?.hasCompanyData ? 'bg-green-500' : 'bg-red-500'}`}></div>
            <div>
              <p className="font-medium text-gray-900">Company Data</p>
              <p className="text-sm text-gray-600">
                {dataStats?.data?.hasCompanyData ? 'Connected' : 'Not Set'}
              </p>
            </div>
          </div>

          <div className="flex items-center space-x-3">
            <div className="w-3 h-3 bg-green-500 rounded-full"></div>
            <div>
              <p className="font-medium text-gray-900">Qdrant Database</p>
              <p className="text-sm text-gray-600">
                {dataStats?.data?.vectorCount || 0} vectors stored
              </p>
            </div>
          </div>

          <div className="flex items-center space-x-3">
            <div className="w-3 h-3 bg-green-500 rounded-full"></div>
            <div>
              <p className="font-medium text-gray-900">Nebius AI</p>
              <p className="text-sm text-gray-600">Connected</p>
            </div>
          </div>
        </div>
      </div>

      {/* Performance Insights */}
      <div className="card">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Performance Insights</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <h3 className="font-medium text-gray-900 mb-2">Content Generation</h3>
            <div className="space-y-2 text-sm text-gray-600">
              <p>• Total suggestions generated: {formatNumber(contentStats?.data?.totalSuggestions || 0)}</p>
              <p>• Most popular type: {Object.entries(contentStats?.data?.byType || {}).sort(([,a], [,b]) => b - a)[0]?.[0] || 'N/A'}</p>
              <p>• Average daily activity: {Math.round((contentStats?.data?.recentActivity?.last7Days || 0) / 7)}</p>
            </div>
          </div>

          <div>
            <h3 className="font-medium text-gray-900 mb-2">Data Management</h3>
            <div className="space-y-2 text-sm text-gray-600">
              <p>• Vector database size: {formatNumber(dataStats?.data?.vectorCount || 0)} embeddings</p>
              <p>• Last data update: {dataStats?.data?.lastUpdate ? new Date(dataStats.data.lastUpdate).toLocaleDateString() : 'Never'}</p>
              <p>• Data status: {dataStats?.data?.hasCompanyData ? 'Active' : 'Inactive'}</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Analytics; 