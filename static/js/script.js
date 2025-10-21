// Main JavaScript file with enhanced animations
console.log('GeneLab on FastAPI loaded!');

// Animation for element appearance with delay
document.addEventListener('DOMContentLoaded', function() {
    // Animation for gene cards and features
    const animateCards = () => {
        const cards = document.querySelectorAll('.gene-card, .feature-card, .detail-section');
        cards.forEach((card, index) => {
            card.style.opacity = '0';
            card.style.transform = 'translateY(30px)';
            
            setTimeout(() => {
                card.style.transition = 'opacity 0.6s ease, transform 0.6s ease';
                card.style.opacity = '1';
                card.style.transform = 'translateY(0)';
            }, index * 100);
        });
    };

    // Parallax effect for hero section
    const setupParallax = () => {
        const hero = document.querySelector('.hero');
        if (hero) {
            window.addEventListener('scroll', () => {
                const scrolled = window.pageYOffset;
                const parallaxSpeed = 0.5;
                hero.style.transform = `translateY(${scrolled * parallaxSpeed}px)`;
            });
        }
    };

    // Interactive effects for cards
    const setupCardInteractions = () => {
        const cards = document.querySelectorAll('.gene-card, .feature-card, .ortholog-card');
        cards.forEach(card => {
            card.addEventListener('mouseenter', function() {
                this.style.transform = 'translateY(-8px) scale(1.02)';
            });
            
            card.addEventListener('mouseleave', function() {
                this.style.transform = 'translateY(0) scale(1)';
            });
        });
    };

    // Initialize all functions
    animateCards();
    setupParallax();
    setupCardInteractions();

    // Dynamic data loading (can be extended for real API)
    const loadGeneData = async (geneSymbol) => {
        try {
            // In real application, this would be a fetch request to API
            console.log(`Loading data for gene: ${geneSymbol}`);
        } catch (error) {
            console.error('Error loading data:', error);
        }
    };

    // Event handlers for interactive elements
    const setupEventListeners = () => {
        // Add handlers for filters, search, etc.
        const searchInput = document.querySelector('.search-input');
        if (searchInput) {
            searchInput.addEventListener('input', function(e) {
                console.log('Search:', e.target.value);
                // Search implementation
            });
        }
    };

    setupEventListeners();
});

// Function for formatting long text
function truncateText(text, maxLength = 200) {
    if (text.length <= maxLength) return text;
    return text.substr(0, maxLength) + '...';
}

// Function for beautiful JSON data display
function formatGeneData(data) {
    if (typeof data === 'object') {
        return JSON.stringify(data, null, 2);
    }
    return data;
}

// Loading state management
function showLoadingState() {
    const loadingElement = document.createElement('div');
    loadingElement.className = 'loading-state glass';
    loadingElement.innerHTML = `
        <div class="loading-spinner"></div>
        <p>Loading data...</p>
    `;
    document.querySelector('.main-content').appendChild(loadingElement);
}

function hideLoadingState() {
    const loadingElement = document.querySelector('.loading-state');
    if (loadingElement) {
        loadingElement.remove();
    }
}