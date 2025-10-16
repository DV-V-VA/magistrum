import React, { useState } from 'react';
import { useMutation, useQuery } from 'react-query';
import { useForm } from 'react-hook-form';
import { 
  Sparkles, 
  FileText, 
  Play, 
  Share2, 
  Loader2,
  Copy,
  ThumbsUp,
  ThumbsDown
} from 'lucide-react';
import toast from 'react-hot-toast';
import { contentAPI, dataAPI } from '../services/api';

const ContentGenerator = () => {
  const [suggestions, setSuggestions] = useState(null);
  const [selectedSuggestion, setSelectedSuggestion] = useState(null);

  const { register, handleSubmit, reset, formState: { errors } } = useForm();

  const { data: documents } = useQuery('documents', dataAPI.getDocuments, {
    retry: false,
    onError: () => {
      // Documents not available, continue without it
    }
  });

  const generateSuggestionsMutation = useMutation(
    contentAPI.generateSuggestions,
    {
      onSuccess: (data) => {
        setSuggestions(data.data);
        toast.success('Content suggestions generated successfully!');
      },
      onError: (error) => {
        toast.error('Failed to generate suggestions');
      }
    }
  );



  const onSubmitSuggestions = (data) => {
    const payload = {
      contentType: data.contentType,
      goals: data.additionalGoals || ''
    };

    generateSuggestionsMutation.mutate(payload);
  };



  const copyToClipboard = (text) => {
    navigator.clipboard.writeText(text);
    toast.success('Copied to clipboard!');
  };

  const formatPostForCopy = (suggestion) => {
    const content = suggestion.content || suggestion.post_content || suggestion.description || '';
    const hashtags = (suggestion.formattedHashtags || suggestion.hashtags || []).join(' ');
    const engagement = suggestion.engagementStrategy || '';
    
    let formatted = `${suggestion.title || suggestion.post_title || 'Post'}\n\n`;
    formatted += `${content}\n\n`;
    if (hashtags) formatted += `${hashtags}\n\n`;
    if (engagement) formatted += `Engagement Strategy: ${engagement}`;
    
    return formatted;
  };

  const submitFeedback = (suggestionId, rating) => {
    // TODO: Implement feedback submission
    toast.success(`Feedback submitted: ${rating === 'up' ? '👍' : '👎'}`);
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-gray-900">Content Generator</h1>
        <p className="text-gray-600 mt-2">
          Generate AI-powered content suggestions using Nebius and Qdrant RAG
        </p>
      </div>

      {/* Header only - no tabs needed */}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Left Panel - Form */}
        <div className="space-y-6">
          <div className="card">
            <h2 className="text-xl font-semibold text-gray-900 mb-4">Generate Suggestions</h2>
            <form onSubmit={handleSubmit(onSubmitSuggestions)} className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Content Type *
                </label>
                <select
                  {...register('contentType', { required: 'Content type is required' })}
                  className="input-field"
                >
                  <option value="">Select content type</option>
                  <option value="social_media_post">Social Media Post</option>
                  <option value="article">Article</option>
                  <option value="demo_application">Demo Application Ideas</option>
                </select>
                {errors.contentType && (
                  <p className="text-red-600 text-sm mt-1">{errors.contentType.message}</p>
                )}
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Additional Goals/Context (Optional)
                </label>
                <textarea
                  {...register('additionalGoals')}
                  rows={3}
                  className="textarea-field"
                  placeholder="Any specific goals, tone, or context for the content..."
                />
              </div>

              <button
                type="submit"
                disabled={generateSuggestionsMutation.isLoading}
                className="btn-primary w-full flex items-center justify-center"
              >
                {generateSuggestionsMutation.isLoading ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    Generating...
                  </>
                ) : (
                  <>
                    <Sparkles className="h-4 w-4 mr-2" />
                    Generate Content
                  </>
                )}
              </button>
            </form>
          </div>

          {/* Info Card */}
          <div className="card bg-blue-50 border-blue-200">
            <div className="flex items-start">
              <Sparkles className="h-6 w-6 text-blue-600 mr-3 mt-1" />
              <div>
                <h3 className="font-semibold text-blue-900">How it works</h3>
                <p className="text-blue-700 text-sm mt-1">
                  Generate content using default company data and uploaded documents as reference. Upload documents to enable more contextual suggestions.
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* Right Panel - Results */}
        <div className="card">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">Generated Suggestions</h2>
          
          {suggestions ? (
            <div className="space-y-4">
              {/* Context Info */}
              {suggestions.metadata?.documentsUsed > 0 && (
                <div className="bg-green-50 border border-green-200 rounded-lg p-3 mb-4">
                  <p className="text-green-800 text-sm">
                    📚 Generated using {suggestions.metadata.documentsUsed} uploaded document(s) as reference
                  </p>
                </div>
              )}
              
              {suggestions.map((suggestion, index) => (
                <div key={index} className="border border-gray-200 rounded-lg p-4">
                  <div className="flex items-start justify-between mb-3">
                    <h3 className="font-semibold text-gray-900">
                      {suggestion.title || suggestion.post_title || `Suggestion ${index + 1}`}
                    </h3>
                    <div className="flex space-x-2">
                      <button
                        onClick={() => copyToClipboard(formatPostForCopy(suggestion))}
                        className="text-gray-400 hover:text-gray-600"
                      >
                        <Copy className="h-4 w-4" />
                      </button>
                      <button
                        onClick={() => submitFeedback(index, 'up')}
                        className="text-gray-400 hover:text-green-600"
                      >
                        <ThumbsUp className="h-4 w-4" />
                      </button>
                      <button
                        onClick={() => submitFeedback(index, 'down')}
                        className="text-gray-400 hover:text-red-600"
                      >
                        <ThumbsDown className="h-4 w-4" />
                      </button>
                    </div>
                  </div>
                  
                  <div className="prose prose-sm max-w-none">
                    <p className="text-gray-700 mb-3">
                      {suggestion.content || suggestion.post_content || suggestion.description}
                    </p>
                    
                    {suggestion.formattedHashtags && suggestion.formattedHashtags.length > 0 && (
                      <div className="flex flex-wrap gap-1 mb-3">
                        {suggestion.formattedHashtags.map((hashtag, i) => (
                          <span key={i} className="bg-blue-100 text-blue-800 text-xs px-2 py-1 rounded">
                            {hashtag}
                          </span>
                        ))}
                      </div>
                    )}
                    
                    {suggestion.engagementStrategy && (
                      <div className="bg-gray-50 p-3 rounded-lg">
                        <p className="text-sm text-gray-600">
                          <strong>Engagement Strategy:</strong> {suggestion.engagementStrategy}
                        </p>
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-center py-12">
              <Sparkles className="h-12 w-12 text-gray-400 mx-auto mb-4" />
              <p className="text-gray-500">No suggestions generated yet</p>
              <p className="text-sm text-gray-400">Fill out the form and click generate to get started</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ContentGenerator; 