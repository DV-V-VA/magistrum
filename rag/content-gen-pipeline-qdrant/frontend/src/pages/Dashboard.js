import React from 'react';
import { useQuery } from 'react-query';
import { Link } from 'react-router-dom';
import { 
  BarChart3, 
  FileText, 
  Upload, 
  Sparkles,
  TrendingUp,
  Users,
  Target,
  Activity,
  Database
} from 'lucide-react';
import { contentAPI, dataAPI } from '../services/api';

const Dashboard = () => {
  const { data: contentStats, isLoading: contentLoading } = useQuery(
    'contentStats',
    contentAPI.getStats
  );

  const { data: dataStats, isLoading: dataLoading } = useQuery(
    'dataStats',
    dataAPI.getDataStats
  );

  const quickActions = [
    {
      title: 'Generate Content',
      description: 'Create articles, demos, and social media posts',
      icon: Sparkles,
      href: '/content-generator',
      color: 'bg-blue-500',
    },
    {
      title: 'Upload Documents',
      description: 'Add documents and URLs for RAG content generation',
      icon: Upload,
      href: '/document-upload',
      color: 'bg-green-500',
    },
    {
      title: 'Data Management',
      description: 'View system status and manage data',
      icon: Database,
      href: '/data-upload',
      color: 'bg-purple-500',
    },
    {
      title: 'Analytics',
      description: 'Detailed insights and performance metrics',
      icon: BarChart3,
      href: '/analytics',
      color: 'bg-orange-500',
    },
  ];

  const stats = [
    {
      title: 'Total Suggestions',
      value: contentStats?.data?.totalSuggestions || 0,
      icon: FileText,
      color: 'text-blue-600',
      bgColor: 'bg-blue-50',
    },
    {
      title: 'Vector Count',
      value: dataStats?.data?.vectorCount || 0,
      icon: Target,
      color: 'text-green-600',
      bgColor: 'bg-green-50',
    },
    {
      title: 'Recent Activity',
      value: contentStats?.data?.recentActivity?.last24Hours || 0,
      icon: Activity,
      color: 'text-purple-600',
      bgColor: 'bg-purple-50',
    },
    {
      title: 'Data Status',
      value: dataStats?.data?.hasCompanyData ? 'Connected' : 'Not Set',
      icon: Users,
      color: dataStats?.data?.hasCompanyData ? 'text-green-600' : 'text-red-600',
      bgColor: dataStats?.data?.hasCompanyData ? 'bg-green-50' : 'bg-red-50',
    },
  ];

  if (contentLoading || dataLoading) {
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
        <h1 className="text-3xl font-bold text-gray-900">Dashboard</h1>
        <p className="text-gray-600 mt-2">
          Welcome to your content suggestion platform powered by Nebius AI and Qdrant
        </p>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {stats.map((stat, index) => {
          const Icon = stat.icon;
          return (
            <div key={index} className="card">
              <div className="flex items-center">
                <div className={`p-3 rounded-lg ${stat.bgColor}`}>
                  <Icon className={`h-6 w-6 ${stat.color}`} />
                </div>
                <div className="ml-4">
                  <p className="text-sm font-medium text-gray-600">{stat.title}</p>
                  <p className="text-2xl font-bold text-gray-900">{stat.value}</p>
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Quick Actions */}
      <div>
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Quick Actions</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {quickActions.map((action, index) => {
            const Icon = action.icon;
            return (
              <Link
                key={index}
                to={action.href}
                className="card hover:shadow-md transition-shadow duration-200 group"
              >
                <div className="flex items-center space-x-4">
                  <div className={`p-3 rounded-lg ${action.color} group-hover:scale-110 transition-transform duration-200`}>
                    <Icon className="h-6 w-6 text-white" />
                  </div>
                  <div>
                    <h3 className="font-semibold text-gray-900 group-hover:text-primary-600 transition-colors">
                      {action.title}
                    </h3>
                    <p className="text-sm text-gray-600">{action.description}</p>
                  </div>
                </div>
              </Link>
            );
          })}
        </div>
      </div>

      {/* Recent Activity */}
      <div className="card">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Recent Activity</h2>
        <div className="space-y-4">
          {contentStats?.data?.recentActivity?.last24Hours > 0 ? (
            <div className="flex items-center space-x-3">
              <div className="w-2 h-2 bg-green-500 rounded-full"></div>
              <span className="text-sm text-gray-600">
                {contentStats.data.recentActivity.last24Hours} suggestions generated in the last 24 hours
              </span>
            </div>
          ) : (
            <div className="flex items-center space-x-3">
              <div className="w-2 h-2 bg-gray-400 rounded-full"></div>
              <span className="text-sm text-gray-600">No recent activity</span>
            </div>
          )}
          
          {dataStats?.data?.hasCompanyData ? (
            <div className="flex items-center space-x-3">
              <div className="w-2 h-2 bg-blue-500 rounded-full"></div>
              <span className="text-sm text-gray-600">
                Company data loaded with {dataStats.data.vectorCount} vectors
              </span>
            </div>
          ) : (
            <div className="flex items-center space-x-3">
              <div className="w-2 h-2 bg-green-500 rounded-full"></div>
              <span className="text-sm text-gray-600">
                Default company data available
              </span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default Dashboard; 