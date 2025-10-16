import React, { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import { 
  Upload, 
  Link, 
  FileText, 
  Globe, 
  Loader2,
  CheckCircle,
  AlertCircle,
  Trash2,
  X,
  File,
  ExternalLink
} from 'lucide-react';
import toast from 'react-hot-toast';
import { dataAPI } from '../services/api';

const DocumentUpload = () => {
  const [activeTab, setActiveTab] = useState('files');
  const [uploadedFiles, setUploadedFiles] = useState([]);
  const [urls, setUrls] = useState('');
  const [dragActive, setDragActive] = useState(false);
  const [uploadingFiles, setUploadingFiles] = useState(new Set());
  const queryClient = useQueryClient();

  const { data: documents, isLoading: documentsLoading } = useQuery(
    'documents', 
    dataAPI.getDocuments,
    {
      retry: false,
      onError: () => {
        // Documents not available, continue without them
      }
    }
  );

  const uploadFilesMutation = useMutation(dataAPI.uploadFiles, {
    onSuccess: (data) => {
      queryClient.invalidateQueries('documents');
      queryClient.invalidateQueries('dataStats');
      
      if (data.data.failedCount > 0) {
        toast.success(`Successfully processed ${data.data.successfulCount} files! ${data.data.failedCount} files failed.`);
      } else {
        toast.success(`Successfully processed ${data.data.successfulCount} files!`);
      }
      
      setUploadedFiles([]);
      setUploadingFiles(new Set());
    },
    onError: (error) => {
      const errorMessage = error.response?.data?.error || 'Failed to upload files';
      toast.error(errorMessage);
      setUploadingFiles(new Set());
    }
  });

  const uploadLinksMutation = useMutation(dataAPI.uploadLinks, {
    onSuccess: (data) => {
      queryClient.invalidateQueries('documents');
      queryClient.invalidateQueries('dataStats');
      toast.success(`Successfully processed ${data.data.successfulCount} links!`);
      setUrls('');
    },
    onError: (error) => {
      toast.error('Failed to process links');
    }
  });

  const deleteDocumentMutation = useMutation(dataAPI.deleteDocument, {
    onSuccess: (data) => {
      queryClient.invalidateQueries('documents');
      queryClient.invalidateQueries('dataStats');
      toast.success('Document deleted successfully!');
    },
    onError: (error) => {
      toast.error('Failed to delete document');
    }
  });

  const handleFileSelect = (e) => {
    const files = Array.from(e.target.files);
    
    // Validate file types and sizes
    const validFiles = files.filter(file => {
      const extension = file.name.toLowerCase().split('.').pop();
      const allowedTypes = ['docx', 'txt', 'md'];
      
      if (!allowedTypes.includes(extension)) {
        toast.error(`Unsupported file type: ${file.name}. Only DOCX, TXT, and MD files are supported.`);
        return false;
      }
      
      if (file.size > 10 * 1024 * 1024) { // 10MB limit
        toast.error(`File too large: ${file.name}. Maximum size is 10MB.`);
        return false;
      }
      
      return true;
    });
    
    setUploadedFiles(prev => [...prev, ...validFiles]);
  };

  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      const files = Array.from(e.dataTransfer.files);
      
      // Validate file types and sizes
      const validFiles = files.filter(file => {
        const extension = file.name.toLowerCase().split('.').pop();
        const allowedTypes = ['docx', 'txt', 'md'];
        
        if (!allowedTypes.includes(extension)) {
          toast.error(`Unsupported file type: ${file.name}. Only DOCX, TXT, and MD files are supported.`);
          return false;
        }
        
        if (file.size > 10 * 1024 * 1024) { // 10MB limit
          toast.error(`File too large: ${file.name}. Maximum size is 10MB.`);
          return false;
        }
        
        return true;
      });
      
      setUploadedFiles(prev => [...prev, ...validFiles]);
    }
  };

  const removeFile = (index) => {
    setUploadedFiles(prev => prev.filter((_, i) => i !== index));
  };

  const handleFileUpload = () => {
    if (uploadedFiles.length === 0) {
      toast.error('Please select files to upload');
      return;
    }

    // Set uploading state for all files
    setUploadingFiles(new Set(uploadedFiles.map(file => file.name)));
    
    uploadFilesMutation.mutate(uploadedFiles);
  };

  const handleLinkUpload = () => {
    const urlList = urls.split('\n').map(url => url.trim()).filter(url => url);
    
    if (urlList.length === 0) {
      toast.error('Please enter at least one URL');
      return;
    }

    uploadLinksMutation.mutate(urlList);
  };

  const handleDeleteDocument = (documentId) => {
    if (window.confirm('Are you sure you want to delete this document? This action cannot be undone.')) {
      deleteDocumentMutation.mutate(documentId);
    }
  };

  const getFileIcon = (fileName) => {
    const extension = fileName.toLowerCase().split('.').pop();
    switch (extension) {
      case 'docx':
        return <FileText className="h-5 w-5 text-blue-500" />;
      case 'txt':
      case 'md':
        return <FileText className="h-5 w-5 text-gray-500" />;
      default:
        return <File className="h-5 w-5 text-gray-400" />;
    }
  };

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-gray-900">Document Upload</h1>
        <p className="text-gray-600 mt-2">
          Upload files and links to enhance your AI content suggestions with additional context
        </p>
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-200">
        <nav className="-mb-px flex space-x-8">
          <button
            onClick={() => setActiveTab('files')}
            className={`py-2 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'files'
                ? 'border-primary-500 text-primary-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            <Upload className="inline h-4 w-4 mr-2" />
            Upload Files
          </button>
          <button
            onClick={() => setActiveTab('links')}
            className={`py-2 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'links'
                ? 'border-primary-500 text-primary-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            <Link className="inline h-4 w-4 mr-2" />
            Add Links
          </button>
          <button
            onClick={() => setActiveTab('documents')}
            className={`py-2 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'documents'
                ? 'border-primary-500 text-primary-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            <FileText className="inline h-4 w-4 mr-2" />
            Documents ({documents?.data?.length || 0})
          </button>
        </nav>
      </div>

      {/* File Upload Tab */}
      {activeTab === 'files' && (
        <div className="card">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">Upload Files</h2>
          
          {/* Drag & Drop Area */}
          <div
            className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors ${
              dragActive 
                ? 'border-primary-500 bg-primary-50' 
                : 'border-gray-300 hover:border-gray-400'
            }`}
            onDragEnter={handleDrag}
            onDragLeave={handleDrag}
            onDragOver={handleDrag}
            onDrop={handleDrop}
          >
            <Upload className="h-12 w-12 text-gray-400 mx-auto mb-4" />
            <p className="text-lg font-medium text-gray-900 mb-2">
              Drop files here or click to browse
            </p>
            <p className="text-gray-600 mb-4">
              Supports DOCX, TXT, and MD files (max 10MB each)
            </p>
            <input
              type="file"
              multiple
              accept=".docx,.txt,.md"
              onChange={handleFileSelect}
              className="hidden"
              id="file-upload"
            />
            <label
              htmlFor="file-upload"
              className="btn-primary cursor-pointer inline-flex items-center"
            >
              <Upload className="h-4 w-4 mr-2" />
              Choose Files
            </label>
          </div>

          {/* Selected Files */}
          {uploadedFiles.length > 0 && (
            <div className="mt-6">
              <h3 className="text-lg font-medium text-gray-900 mb-3">Selected Files</h3>
              <div className="space-y-2">
                {uploadedFiles.map((file, index) => (
                  <div key={index} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                    <div className="flex items-center space-x-3">
                      {uploadingFiles.has(file.name) ? (
                        <Loader2 className="h-5 w-5 text-primary-600 animate-spin" />
                      ) : (
                        getFileIcon(file.name)
                      )}
                      <div>
                        <p className="font-medium text-gray-900">{file.name}</p>
                        <p className="text-sm text-gray-500">{formatFileSize(file.size)}</p>
                      </div>
                    </div>
                    {!uploadingFiles.has(file.name) && (
                      <button
                        onClick={() => removeFile(index)}
                        className="text-red-500 hover:text-red-700"
                      >
                        <X className="h-4 w-4" />
                      </button>
                    )}
                  </div>
                ))}
              </div>
              
              <div className="mt-4 flex space-x-3">
                <button
                  onClick={handleFileUpload}
                  disabled={uploadFilesMutation.isLoading || uploadedFiles.length === 0}
                  className="btn-primary flex items-center"
                >
                  {uploadFilesMutation.isLoading ? (
                    <>
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                      Processing...
                    </>
                  ) : (
                    <>
                      <Upload className="h-4 w-4 mr-2" />
                      Upload Files
                    </>
                  )}
                </button>
                <button
                  onClick={() => setUploadedFiles([])}
                  className="btn-secondary"
                >
                  Clear All
                </button>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Links Tab */}
      {activeTab === 'links' && (
        <div className="card">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">Add Links</h2>
          
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                URLs (one per line)
              </label>
              <textarea
                value={urls}
                onChange={(e) => setUrls(e.target.value)}
                rows={6}
                className="textarea-field"
                placeholder="https://example.com/article1&#10;https://example.com/article2&#10;https://example.com/blog-post"
              />
              <p className="text-sm text-gray-500 mt-1">
                Enter URLs to extract content from web pages. The system will analyze the content and use it for AI suggestions.
              </p>
            </div>

            <div className="flex space-x-3">
              <button
                onClick={handleLinkUpload}
                disabled={uploadLinksMutation.isLoading}
                className="btn-primary flex items-center"
              >
                {uploadLinksMutation.isLoading ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    Processing...
                  </>
                ) : (
                  <>
                    <Link className="h-4 w-4 mr-2" />
                    Process Links
                  </>
                )}
              </button>
              <button
                onClick={() => setUrls('')}
                className="btn-secondary"
              >
                Clear
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Documents Tab */}
      {activeTab === 'documents' && (
        <div className="card">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">Uploaded Documents</h2>
          
          {documentsLoading ? (
            <div className="flex items-center justify-center h-32">
              <Loader2 className="h-8 w-8 animate-spin text-primary-600" />
            </div>
          ) : documents?.data?.length > 0 ? (
            <div className="space-y-4">
              {documents.data.map((doc, index) => (
                <div key={index} className="border border-gray-200 rounded-lg p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center space-x-3">
                      {doc.fileName ? (
                        getFileIcon(doc.fileName)
                      ) : (
                        <Globe className="h-5 w-5 text-blue-500" />
                      )}
                      <div>
                        <h3 className="font-medium text-gray-900">
                          {doc.title || doc.fileName || doc.url}
                        </h3>
                        <p className="text-sm text-gray-500">
                          {doc.fileName ? (
                            `${doc.fileType} • ${doc.wordCount} words`
                          ) : (
                            `${doc.wordCount} words • ${new Date(doc.timestamp).toLocaleDateString()}`
                          )}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center space-x-2">
                      {doc.url && (
                        <a
                          href={doc.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-blue-600 hover:text-blue-800"
                        >
                          <ExternalLink className="h-4 w-4" />
                        </a>
                      )}
                      <button
                        onClick={() => handleDeleteDocument(doc.fileName || doc.url)}
                        disabled={deleteDocumentMutation.isLoading}
                        className="text-red-500 hover:text-red-700"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-center py-12">
              <FileText className="h-12 w-12 text-gray-300 mx-auto mb-4" />
              <p className="text-gray-500">No documents uploaded yet</p>
              <p className="text-sm text-gray-400">Upload files or add links to get started</p>
            </div>
          )}
        </div>
      )}

      {/* Info Card */}
      <div className="card bg-blue-50 border-blue-200">
        <div className="flex items-start">
          <FileText className="h-6 w-6 text-blue-600 mr-3 mt-1" />
          <div>
            <h3 className="font-semibold text-blue-900">Document Processing</h3>
            <p className="text-blue-700 text-sm mt-1">
              Uploaded documents and links will be processed, chunked, and converted into vector embeddings. 
              This content will be used to provide more relevant and contextual AI suggestions in the Content Generator.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DocumentUpload; 