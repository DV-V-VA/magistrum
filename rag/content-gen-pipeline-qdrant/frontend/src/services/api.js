import axios from 'axios';
import toast from 'react-hot-toast';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:3001';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: parseInt(process.env.REACT_APP_API_TIMEOUT) || 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor
api.interceptors.request.use(
  (config) => {
    // Add any auth tokens here if needed
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor
api.interceptors.response.use(
  (response) => {
    return response;
  },
  (error) => {
    const message = error.response?.data?.error || error.message || 'An error occurred';
    toast.error(message);
    return Promise.reject(error);
  }
);

// Content API
export const contentAPI = {
  generateSuggestions: async (data) => {
    const response = await api.post('/api/content/suggest', data);
    return response.data;
  },

  generateRAGContent: async (data) => {
    const response = await api.post('/api/content/rag', data);
    return response.data;
  },

  analyzeCompanyData: async (data) => {
    const response = await api.post('/api/content/analyze', data);
    return response.data;
  },

  getHistory: async (params = {}) => {
    const response = await api.get('/api/content/history', { params });
    return response.data;
  },

  getStats: async () => {
    const response = await api.get('/api/content/stats');
    return response.data;
  },
};

// Data API
export const dataAPI = {
  uploadCompanyData: async (data) => {
    const response = await api.post('/api/data/upload', data);
    return response.data;
  },

  uploadFiles: async (files) => {
    const formData = new FormData();
    files.forEach(file => {
      formData.append('files', file);
    });
    
    const response = await api.post('/api/data/files', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data;
  },

  uploadLinks: async (urls) => {
    const response = await api.post('/api/data/links', { urls });
    return response.data;
  },

  getCompanyData: async () => {
    const response = await api.get('/api/data/company');
    return response.data;
  },

  getDocuments: async () => {
    const response = await api.get('/api/data/documents');
    return response.data;
  },

  deleteDocument: async (id) => {
    const response = await api.delete(`/api/data/documents/${id}`);
    return response.data;
  },

  updateCompanyData: async (data) => {
    const response = await api.put('/api/data/company', data);
    return response.data;
  },

  deleteCompanyData: async () => {
    const response = await api.delete('/api/data/company');
    return response.data;
  },

  getDataStats: async () => {
    const response = await api.get('/api/data/stats');
    return response.data;
  },
};

// Feedback API
export const feedbackAPI = {
  submitFeedback: async (data) => {
    const response = await api.post('/api/feedback', data);
    return response.data;
  },

  getFeedbackStats: async () => {
    const response = await api.get('/api/feedback/stats');
    return response.data;
  },

  getFeedbackHistory: async (params = {}) => {
    const response = await api.get('/api/feedback/history', { params });
    return response.data;
  },
};

// Health check
export const healthCheck = async () => {
  const response = await api.get('/health');
  return response.data;
};

export default api; 