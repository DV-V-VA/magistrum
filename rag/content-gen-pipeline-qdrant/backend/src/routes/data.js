const express = require('express');
const router = express.Router();
const multer = require('multer');
const dataController = require('../controllers/dataController');

// Configure multer for file uploads
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024, // 10MB limit
    files: 5 // Max 5 files at once
  },
  fileFilter: (req, file, cb) => {
    // Check file type - support DOCX, TXT, and MD files
    const allowedTypes = ['.docx', '.txt', '.md'];
    const fileExtension = file.originalname.toLowerCase().substring(file.originalname.lastIndexOf('.'));
    
    if (allowedTypes.includes(fileExtension)) {
      cb(null, true);
    } else {
      cb(new Error(`Unsupported file type: ${fileExtension}. Only DOCX, TXT, and MD files are supported.`));
    }
  }
});

// Upload company data
router.post('/upload', dataController.uploadCompanyData);

// Upload files
router.post('/files', upload.array('files'), dataController.uploadFiles);

// Upload links
router.post('/links', dataController.uploadLinks);

// Get company data
router.get('/company', dataController.getCompanyData);

// Get documents
router.get('/documents', dataController.getDocuments);

// Delete document
router.delete('/documents/:id', dataController.deleteDocument);

// Update company data
router.put('/company', dataController.updateCompanyData);

// Delete company data
router.delete('/company', dataController.deleteCompanyData);

// Get data statistics
router.get('/stats', dataController.getDataStats);

module.exports = router; 