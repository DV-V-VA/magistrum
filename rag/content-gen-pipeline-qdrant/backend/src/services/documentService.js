const axios = require('axios');
const fs = require('fs');
const path = require('path');
const mammoth = require('mammoth');
const cheerio = require('cheerio');
const { v4: uuidv4 } = require('uuid');

class DocumentService {
  constructor() {
    this.supportedFileTypes = {
      '.docx': this.extractDocxText,
      '.txt': this.extractTxtText,
      '.md': this.extractTxtText
    };
  }

  async processFile(filePath, fileName) {
    try {
      const fileExtension = path.extname(fileName).toLowerCase();
      
      if (!this.supportedFileTypes[fileExtension]) {
        throw new Error(`Unsupported file type: ${fileExtension}`);
      }

      const extractor = this.supportedFileTypes[fileExtension];
      const content = await extractor(filePath);
      
      return {
        fileName,
        fileType: fileExtension,
        content: content.trim(),
        wordCount: content.split(/\s+/).length,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      console.error('❌ File processing error:', error);
      throw new Error(`Failed to process file ${fileName}: ${error.message}`);
    }
  }

  async extractDocxText(filePath) {
    try {
      const result = await mammoth.extractRawText({ path: filePath });
      return result.value;
    } catch (error) {
      throw new Error(`DOCX extraction failed: ${error.message}`);
    }
  }

  async extractTxtText(filePath) {
    try {
      return fs.readFileSync(filePath, 'utf8');
    } catch (error) {
      throw new Error(`Text file reading failed: ${error.message}`);
    }
  }

  async extractUrlContent(url) {
    try {
      console.log(`🔗 Extracting content from: ${url}`);
      
      const response = await axios.get(url, {
        timeout: 10000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
      });

      const $ = cheerio.load(response.data);
      
      // Remove script and style elements
      $('script, style, nav, footer, header, .ad, .advertisement, .sidebar').remove();
      
      // Extract main content
      let content = '';
      
      // Try to find main content areas
      const mainSelectors = [
        'main',
        'article',
        '.content',
        '.post-content',
        '.entry-content',
        '#content',
        '.main-content'
      ];

      for (const selector of mainSelectors) {
        const element = $(selector);
        if (element.length > 0) {
          content = element.text();
          break;
        }
      }

      // If no main content found, use body text
      if (!content) {
        content = $('body').text();
      }

      // Clean up the content
      content = content
        .replace(/\s+/g, ' ')
        .replace(/\n+/g, '\n')
        .trim();

      // Extract title
      const title = $('title').text() || $('h1').first().text() || 'Untitled';

      return {
        url,
        title: title.trim(),
        content: content,
        wordCount: content.split(/\s+/).length,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      console.error('❌ URL content extraction error:', error);
      // Return a placeholder content instead of throwing error
      return {
        url,
        title: 'Content extraction failed',
        content: `Content from ${url} could not be extracted. Error: ${error.message}`,
        wordCount: 0,
        timestamp: new Date().toISOString()
      };
    }
  }

  async chunkContent(content, chunkSize = 1000, overlap = 200) {
    const words = content.split(/\s+/);
    const chunks = [];
    
    for (let i = 0; i < words.length; i += chunkSize - overlap) {
      const chunk = words.slice(i, i + chunkSize).join(' ');
      if (chunk.trim()) {
        chunks.push(chunk.trim());
      }
    }
    
    return chunks;
  }

  async processDocuments(documents) {
    const processedChunks = [];
    
    for (const doc of documents) {
      try {
        const chunks = await this.chunkContent(doc.content);
        
        chunks.forEach((chunk, index) => {
          processedChunks.push({
            id: uuidv4(), // Use UUID instead of URL-based ID
            text: chunk,
            metadata: {
              source: doc.fileName || doc.url,
              type: doc.fileType || 'url',
              title: doc.title || doc.fileName || doc.url,
              chunkIndex: index,
              totalChunks: chunks.length,
              timestamp: doc.timestamp
            }
          });
        });
      } catch (error) {
        console.error(`❌ Failed to process document: ${doc.fileName || doc.url}`, error);
      }
    }
    
    return processedChunks;
  }

  validateUrl(url) {
    try {
      new URL(url);
      return true;
    } catch {
      return false;
    }
  }

  validateFileType(fileName) {
    const extension = path.extname(fileName).toLowerCase();
    return this.supportedFileTypes.hasOwnProperty(extension);
  }
}

module.exports = new DocumentService(); 