// Main JavaScript file for gene search functionality
console.log('GeneLab Search Interface loaded!');

class GeneSearch {
    constructor() {
        this.searchInput = document.getElementById('geneSearch');
        this.searchButton = document.getElementById('searchButton');
        this.autocompleteResults = document.getElementById('autocompleteResults');
        this.loadingState = document.getElementById('loadingState');
        this.errorState = document.getElementById('errorState');
        this.geneDetails = document.getElementById('geneDetails');
        this.searchQuery = document.getElementById('searchQuery');
        
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.setupTemplates();
    }

    setupEventListeners() {
        // Search button click
        this.searchButton.addEventListener('click', () => {
            this.performSearch(this.searchInput.value.trim());
        });

        // Enter key in search input
        this.searchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.performSearch(this.searchInput.value.trim());
            }
        });

        // Input for autocomplete
        this.searchInput.addEventListener('input', (e) => {
            const query = e.target.value.trim();
            if (query.length > 1) {
                this.showAutocomplete(query);
            } else {
                this.hideAutocomplete();
            }
        });

        // Click outside to hide autocomplete
        document.addEventListener('click', (e) => {
            if (!this.searchInput.contains(e.target) && !this.autocompleteResults.contains(e.target)) {
                this.hideAutocomplete();
            }
        });

        // Escape key to hide autocomplete
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this.hideAutocomplete();
            }
        });
    }

    setupTemplates() {
        // Templates are already defined in HTML
    }

    async performSearch(query) {
        if (!query) return;

        this.hideAutocomplete();
        this.showLoading();
        this.hideError();
        this.hideGeneDetails();

        try {
            const response = await fetch(`/api/search/${encodeURIComponent(query)}`);
            const data = await response.json();

            if (data.exact_match) {
                this.displayGene(data.gene);
            } else if (data.suggestions && data.suggestions.length > 0) {
                // If there are suggestions but no exact match, show the first one
                const firstSuggestion = data.suggestions[0];
                this.performSearch(firstSuggestion.symbol);
            } else {
                this.showError(query);
            }
        } catch (error) {
            console.error('Search error:', error);
            this.showError(query);
        } finally {
            this.hideLoading();
        }
    }

    async showAutocomplete(query) {
        try {
            const response = await fetch(`/api/search/${encodeURIComponent(query)}`);
            const data = await response.json();

            if (data.suggestions && data.suggestions.length > 0) {
                this.displayAutocomplete(data.suggestions);
            } else {
                this.hideAutocomplete();
            }
        } catch (error) {
            console.error('Autocomplete error:', error);
            this.hideAutocomplete();
        }
    }

    async performSearch(query) {
        if (!query) return;

        this.hideAutocomplete();
        this.showLoading();
        this.hideError();
        this.hideGeneDetails();

        try {
            const response = await fetch(`/api/search/${encodeURIComponent(query)}`);
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();

            if (data.error) {
                throw new Error(data.error);
            }

            if (data.exact_match) {
                this.displayGene(data.gene);
            } else if (data.suggestions && data.suggestions.length > 0) {
                const firstSuggestion = data.suggestions[0];
                this.performSearch(firstSuggestion.symbol);
            } else {
                this.showError(query);
            }
        } catch (error) {
            console.error('Search error:', error);
            this.showError(query, error.message);
        } finally {
            this.hideLoading();
        }
    }

    showError(query, errorMessage = '') {
        this.searchQuery.textContent = query;
        const errorText = this.errorState.querySelector('.error-message');
        if (errorText && errorMessage) {
            errorText.textContent = errorMessage;
        }
        this.errorState.classList.remove('hidden');
    }

    displayAutocomplete(suggestions) {
        this.autocompleteResults.innerHTML = '';
        
        suggestions.forEach(suggestion => {
            const suggestionElement = this.createSuggestionElement(suggestion);
            this.autocompleteResults.appendChild(suggestionElement);
        });
        
        this.autocompleteResults.classList.add('active');
    }

    createSuggestionElement(suggestion) {
        const template = document.getElementById('suggestionTemplate');
        const clone = template.content.cloneNode(true);
        
        const suggestionItem = clone.querySelector('.suggestion-item');
        const symbolElement = clone.querySelector('.suggestion-symbol');
        const nameElement = clone.querySelector('.suggestion-name');
        const locationElement = clone.querySelector('.suggestion-location');
        
        symbolElement.textContent = suggestion.symbol;
        nameElement.textContent = suggestion.name;
        locationElement.textContent = suggestion.location || 'Location not available';
        
        suggestionItem.addEventListener('click', () => {
            this.searchInput.value = suggestion.symbol;
            this.performSearch(suggestion.symbol);
        });
        
        return clone;
    }

    hideAutocomplete() {
        this.autocompleteResults.classList.remove('active');
        this.autocompleteResults.innerHTML = '';
    }

    displayGene(geneData) {
        const template = document.getElementById('geneTemplate');
        const clone = template.content.cloneNode(true);
        
        // Fill basic gene information
        clone.querySelector('.gene-symbol').textContent = geneData.symbol;
        clone.querySelector('.gene-type-badge').textContent = geneData.locus_group;
        clone.querySelector('.gene-subtitle').textContent = geneData.hgnc_name;
        
        // Fill basic information
        clone.querySelector('.cytoband').textContent = geneData.cytoband || 'N/A';
        clone.querySelector('.locus-group').textContent = geneData.locus_group || 'N/A';
        clone.querySelector('.last-modified').textContent = geneData.last_modified || 'N/A';
        
        // Fill identifiers
        this.fillIdentifiers(clone, geneData.gene_ids);
        
        // Fill database references
        this.fillDatabaseReferences(clone, geneData);
        
        // Fill aliases
        this.fillAliases(clone, geneData);
        
        // Fill orthologs
        this.fillOrthologs(clone, geneData.orthologs);
        
        // Clear previous content and add new
        this.geneDetails.innerHTML = '';
        this.geneDetails.appendChild(clone);
        this.showGeneDetails();
        
        // Scroll to gene details
        this.geneDetails.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    fillIdentifiers(container, geneIds) {
        const identifiersContainer = container.getElementById('identifiersContainer');
        
        if (!geneIds || geneIds.length === 0) {
            identifiersContainer.innerHTML = '<div class="field"><span>No identifiers available</span></div>';
            return;
        }
        
        identifiersContainer.innerHTML = '';
        
        geneIds.forEach(idItem => {
            const field = document.createElement('div');
            field.className = 'field';
            
            const label = document.createElement('label');
            label.textContent = this.formatFieldName(idItem.name);
            
            const value = document.createElement('span');
            if (Array.isArray(idItem.value)) {
                value.textContent = idItem.value.join(', ');
            } else {
                value.textContent = idItem.value || 'N/A';
            }
            
            field.appendChild(label);
            field.appendChild(value);
            identifiersContainer.appendChild(field);
        });
    }

    fillDatabaseReferences(container, geneData) {
        // OMIM
        const omimField = container.querySelector('.omim-field');
        if (geneData.omim && geneData.omim.length > 0) {
            omimField.querySelector('.omim-ids').textContent = geneData.omim.join(', ');
        } else {
            omimField.style.display = 'none';
        }
        
        // MANE Select
        const maneField = container.querySelector('.mane-field');
        if (geneData.mane_select && geneData.mane_select.length > 0) {
            maneField.querySelector('.mane-select').textContent = geneData.mane_select.join(', ');
        } else {
            maneField.style.display = 'none';
        }
    }

    fillAliases(container, geneData) {
        // Alias symbols
        const aliasSymbolsField = container.querySelector('.alias-symbols-field');
        if (geneData.hgnc_alias_symbols && geneData.hgnc_alias_symbols.length > 0) {
            aliasSymbolsField.querySelector('.alias-symbols').textContent = geneData.hgnc_alias_symbols.join(', ');
        } else {
            aliasSymbolsField.style.display = 'none';
        }
        
        // Alias names
        const aliasNamesField = container.querySelector('.alias-names-field');
        if (geneData.hgnc_alias_names && geneData.hgnc_alias_names.length > 0) {
            aliasNamesField.querySelector('.alias-names').textContent = geneData.hgnc_alias_names.join(', ');
        } else {
            aliasNamesField.style.display = 'none';
        }
        
        // Previous names
        const prevNamesField = container.querySelector('.prev-names-field');
        if (geneData.hgnc_prev_name && geneData.hgnc_prev_name.length > 0) {
            prevNamesField.querySelector('.prev-names').textContent = geneData.hgnc_prev_name.join(', ');
        } else {
            prevNamesField.style.display = 'none';
        }
    }

    fillOrthologs(container, orthologs) {
        const orthologsContainer = container.getElementById('orthologsContainer');
        
        if (!orthologs || orthologs.length === 0) {
            orthologsContainer.innerHTML = '<div class="no-orthologs">No orthologs data available</div>';
            return;
        }
        
        orthologsContainer.innerHTML = '';
        
        orthologs.forEach(ortholog => {
            const orthologCard = document.createElement('div');
            orthologCard.className = 'ortholog-card';
            
            orthologCard.innerHTML = `
                <div class="ortholog-header">
                    <h4>${ortholog.common_name}</h4>
                    <span class="species-badge">${ortholog.taxname}</span>
                </div>
                <div class="field-group">
                    <div class="field">
                        <label>Symbol:</label>
                        <span>${ortholog.symbol}</span>
                    </div>
                    <div class="field">
                        <label>Description:</label>
                        <span>${ortholog.description}</span>
                    </div>
                    ${ortholog.synonyms && ortholog.synonyms.length > 0 ? `
                    <div class="field">
                        <label>Synonyms:</label>
                        <span>${ortholog.synonyms.join(', ')}</span>
                    </div>
                    ` : ''}
                </div>
                ${ortholog.summary && ortholog.summary.length > 0 ? `
                <div class="ortholog-summary">
                    <p>${ortholog.summary[0].description}</p>
                </div>
                ` : ''}
            `;
            
            orthologsContainer.appendChild(orthologCard);
        });
    }

    formatFieldName(name) {
        return name.split('_').map(word => 
            word.charAt(0).toUpperCase() + word.slice(1)
        ).join(' ');
    }

    showLoading() {
        this.loadingState.classList.remove('hidden');
    }

    hideLoading() {
        this.loadingState.classList.add('hidden');
    }

    showError(query) {
        this.searchQuery.textContent = query;
        this.errorState.classList.remove('hidden');
    }

    hideError() {
        this.errorState.classList.add('hidden');
    }

    showGeneDetails() {
        this.geneDetails.classList.remove('hidden');
    }

    hideGeneDetails() {
        this.geneDetails.classList.add('hidden');
    }
}

// Initialize the search functionality when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    new GeneSearch();
    
    // Focus search input on page load
    const searchInput = document.getElementById('geneSearch');
    if (searchInput) {
        setTimeout(() => {
            searchInput.focus();
        }, 500);
    }
});

// Utility function for smooth scrolling
function smoothScrollTo(element) {
    element.scrollIntoView({
        behavior: 'smooth',
        block: 'start'
    });
}