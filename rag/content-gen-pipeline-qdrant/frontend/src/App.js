import React from 'react';
import { Routes, Route } from 'react-router-dom';
import { QueryClient } from 'react-query';
import Header from './components/Header';
import Sidebar from './components/Sidebar';
import Dashboard from './pages/Dashboard';
import ContentGenerator from './pages/ContentGenerator';
import DataUpload from './pages/DataUpload';
import DocumentUpload from './components/DocumentUpload';
import HistoryPage from './pages/History';
import Analytics from './pages/Analytics';

function App() {
  return (
    <div className="min-h-screen bg-gray-50">
      <Header />
      <div className="flex">
        <Sidebar />
        <main className="flex-1 p-6">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/content-generator" element={<ContentGenerator />} />
            <Route path="/data-upload" element={<DataUpload />} />
            <Route path="/document-upload" element={<DocumentUpload />} />
            <Route path="/history" element={<HistoryPage />} />
            <Route path="/analytics" element={<Analytics />} />
          </Routes>
        </main>
      </div>
    </div>
  );
}

export default App; 