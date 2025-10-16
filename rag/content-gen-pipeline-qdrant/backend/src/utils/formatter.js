class ContentFormatter {
  static formatSocialMediaPosts(posts) {
    if (!Array.isArray(posts)) {
      return posts;
    }

    return posts.map((post, index) => ({
      id: index + 1,
      title: post.post_title || post.title || `Post ${index + 1}`,
      content: post.post_content || post.content || '',
      hashtags: Array.isArray(post.suggested_hashtags || post.hashtags) 
        ? post.suggested_hashtags || post.hashtags 
        : [],
      platform: post.best_platform || post.platform || 'LinkedIn',
      engagementStrategy: post.engagement_strategy || post.engagementStrategy || '',
      formattedHashtags: this.formatHashtags(post.suggested_hashtags || post.hashtags),
      platformIcon: this.getPlatformIcon(post.best_platform || post.platform),
      platformColor: this.getPlatformColor(post.best_platform || post.platform)
    }));
  }

  static formatArticleIdeas(articles) {
    if (!Array.isArray(articles)) {
      return articles;
    }

    return articles.map((article, index) => ({
      id: index + 1,
      title: article.title || `Article ${index + 1}`,
      description: article.description || article.brief_description || '',
      keyPoints: Array.isArray(article.keyPoints || article.key_points) 
        ? article.keyPoints || article.key_points 
        : [],
      targetAudience: article.targetAudience || article.target_audience || '',
      readingTime: article.readingTime || article.estimated_reading_time || '5-10 min'
    }));
  }

  static formatDemoIdeas(demos) {
    if (!Array.isArray(demos)) {
      return demos;
    }

    return demos.map((demo, index) => ({
      id: index + 1,
      title: demo.title || `Demo ${index + 1}`,
      description: demo.description || demo.demo_description || '',
      keyFeatures: Array.isArray(demo.keyFeatures || demo.key_features) 
        ? demo.keyFeatures || demo.key_features 
        : [],
      targetAudience: demo.targetAudience || demo.target_audience || '',
      duration: demo.duration || demo.estimated_demo_duration || '15-30 min'
    }));
  }

  static formatHashtags(hashtags) {
    if (!Array.isArray(hashtags)) {
      return [];
    }
    return hashtags.map(tag => 
      tag.startsWith('#') ? tag : `#${tag}`
    );
  }

  static getPlatformIcon(platform) {
    const icons = {
      'LinkedIn': '💼',
      'Twitter': '🐦',
      'Instagram': '📸',
      'Facebook': '📘',
      'YouTube': '📺',
      'TikTok': '🎵',
      'default': '📱'
    };
    return icons[platform] || icons.default;
  }

  static getPlatformColor(platform) {
    const colors = {
      'LinkedIn': '#0077B5',
      'Twitter': '#1DA1F2',
      'Instagram': '#E4405F',
      'Facebook': '#1877F2',
      'YouTube': '#FF0000',
      'TikTok': '#000000',
      'default': '#6B7280'
    };
    return colors[platform] || colors.default;
  }

  static formatContentForDisplay(content, contentType) {
    switch (contentType) {
      case 'socialMedia':
        return this.formatSocialMediaPosts(content);
      case 'articles':
        return this.formatArticleIdeas(content);
      case 'demos':
        return this.formatDemoIdeas(content);
      default:
        return content;
    }
  }

  static createBeautifulResponse(data, contentType) {
    const formatted = this.formatContentForDisplay(data, contentType);
    
    return {
      formatted,
      summary: {
        count: Array.isArray(formatted) ? formatted.length : 1,
        type: contentType,
        timestamp: new Date().toISOString()
      },
      metadata: {
        hasSocialMedia: contentType === 'socialMedia',
        hasArticles: contentType === 'articles',
        hasDemos: contentType === 'demos'
      }
    };
  }
}

module.exports = ContentFormatter; 