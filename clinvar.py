import requests
import time
import re
import os
import logging
import tempfile
from logging_config import setup_logging
from dataclasses import dataclass
from typing import List

from config import (
    PATH_TO_GENAGE_PARSED_GENES,
    PATH_TO_LOGS,
    PATH_TO_RAG,
    RATE_LIMIT_NEBIUS,
    PATH_TO_PARSED_TEXTS
)
PATH_TO_LOGS = os.path.join(tempfile.gettempdir(), "clinvar.log")
setup_logging(PATH_TO_LOGS)
logger = logging.getLogger(__name__)


@dataclass
class QueryInput:
    protein_symbol: str
    synonyms: List[str]


def search_ncbi_pubmed(variant_name, gene_name):
    """
    Search NCBI PubMed for literature about variants
    """
    if not variant_name or variant_name == 'N/A':
        return None
    
    clean_variant = extract_variant_identifier(variant_name)
    logger.info(f"Using variant identifier: {clean_variant}")
    
    try:
        logger.info(f"Searching NCBI PubMed...")
        
        search_queries = [
            f'"{clean_variant}" AND "{gene_name}"',
            f'("{clean_variant}" OR "{variant_name}") AND "{gene_name}"',
        ]
        
        for query in search_queries:
            logger.info(f"Trying query: {query}")
            
            search_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
            search_params = {
                'db': 'pubmed',
                'term': query,
                'retmax': 10,
                'retmode': 'json',
                'sort': 'relevance'
            }
            
            search_response = requests.get(search_url, params=search_params, timeout=30)
            
            if search_response.status_code == 200:
                search_data = search_response.json()
                id_list = search_data.get('esearchresult', {}).get('idlist', [])
                
                if id_list:
                    logger.info(f"Found {len(id_list)} articles")
                    return id_list
                else:
                    logger.info(f"No articles found for this query")
            
            time.sleep(1)
            
    except Exception as e:
        logger.info(f"NCBI PubMed search failed: {e}")
    
    return None

def extract_variant_identifier(variant_name):
    """
    Extract the most relevant part of the variant name for search
    """
    patterns = [
        r'c\.[^\)\s]+',
        r'p\.[^\)\s]+',
        r'rs\d+',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, variant_name)
        if match:
            clean = match.group()
            clean = re.sub(r'[.,;)]+$', '', clean)
            return clean
    
    if ':' in variant_name:
        return variant_name.split(':')[-1].split('(')[0].strip()
    
    return variant_name

def search_clinvar_variants_by_gene(gene_name, max_results=50):
    """
    Search ClinVar variants - get more variants to ensure we find 5 with sufficient sources
    """
    base_url = "https://clinicaltables.nlm.nih.gov/api/variants/v4/search"
    
    params = {
        'terms': gene_name,
        'maxList': max_results
    }
    
    try:
        logger.info(f"Querying ClinVar for gene: {gene_name}")
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Found {data[0]} total variants")
        
        if len(data) >= 4 and data[3]:
            variants_data = data[3]
            variants = []
            
            for i, variant_data in enumerate(variants_data):
                variant = {
                    'variation_id': variant_data[0],
                    'variant_name': variant_data[1],
                    'gene_symbol': gene_name,
                    'condition': 'N/A',
                    'clinical_significance': 'N/A'
                }
                variants.append(variant)
                logger.info(f"{i+1}. {variant['variant_name']}")
            
            return variants
            
    except Exception as e:
        logger.info(f"ClinVar API error: {e}")
    
    return []

def get_variants_with_required_sources(gene_name, required_mutations=5, required_sources=5, max_variants_to_check=50):
    """
    Main function that strictly gets required mutations with required sources each
    """
    logger.info(f"PROCESSING GENE: {gene_name}")
    logger.info("=" * 50)
    
    # Get variants from ClinVar
    all_variants = search_clinvar_variants_by_gene(gene_name, max_variants_to_check)
    
    if not all_variants:
        logger.info(f"No variants found for {gene_name}")
        return []
    
    logger.info(f"Searching for {required_mutations} mutations with at least {required_sources} sources each...")
    
    qualified_variants = []
    checked_count = 0
    
    for variant in all_variants:
        checked_count += 1
        logger.info(f"\nChecking mutation {checked_count}: {variant['variant_name']}")
        
        # Get literature from NCBI PubMed
        pmids = search_ncbi_pubmed(variant['variant_name'], gene_name)
        if pmids is None:
            pmids = []

        # Only keep variants that have at least the required number of sources
        if len(pmids) >= required_sources:
            # Trim to exactly required_sources
            variant['pmids'] = pmids[:required_sources]
            qualified_variants.append(variant)
            
            logger.info(f"QUALIFIED: Found {len(pmids)} sources (keeping {required_sources})")
            
            # Show the kept sources
            for i, pmid in enumerate(variant['pmids']):
                logger.info(f"PMID {i+1}: {pmid}")
            
            #Stop when we have enough qualified mutations
            if len(qualified_variants) >= required_mutations:
                logger.info(f"Reached target of {required_mutations} qualified mutations")
                break
        else:
            logger.info(f"SKIPPED: Only {len(pmids)} sources (need {required_sources})")
        
        time.sleep(2)
    
    # Final summary
    logger.info(f"\nFINAL SUMMARY for {gene_name}:")
    logger.info(f"Checked {checked_count} mutations")
    logger.info(f"Found {len(qualified_variants)} mutations with at least {required_sources} sources each")
    
    for i, variant in enumerate(qualified_variants, 1):
        logger.info(f"{i}. {variant['variant_name']}: {len(variant['pmids'])} sources")
    
    return qualified_variants

def create_query_input_from_results(gene_name: str, variants_results: list) -> QueryInput:
    """
    Convert variants results into a QueryInput object.
    
    Args:
        gene_name: The gene name to use as protein_symbol
        variants_results: List of variant results from get_variants_with_required_sources
    
    Returns:
        QueryInput object with gene_name as protein_symbol and article titles as synonyms
    """
    
    # Extract all publication titles from all variants
    all_pmids = []
    
    for variant in variants_results:
        if 'pmids' in variant:
            for pmid in variant['pmids']:
                if pmid and pmid not in all_pmids:
                    all_pmids.append(pmid)
    
    logger.info(f"Created QueryInput for {gene_name} with {len(all_pmids)} unique articles")
    
    return QueryInput(
        protein_symbol=gene_name,
        synonyms=all_pmids
    )

def run_clinvar(gene_name):
    logger.info("Starting NCBI PubMed search with strict requirements...")
    # 5 mutations with 5 sources each
    results = get_variants_with_required_sources(
        gene_name=gene_name,
        required_mutations=5,
        required_sources=5,
        max_variants_to_check=50
    )
    
    if results:
        # Convert to QueryInput
        query_input = create_query_input_from_results(gene_name, results)
        
        logger.info(f"Protein Symbol: {query_input.protein_symbol}")
        logger.info(f"Number of articles: {len(query_input.synonyms)}")
        
        # logger.info first few articles
        for i, article in enumerate(query_input.synonyms[:3], 1):
            logger.info(f"Article {i}: {article[:100]}...")
    return query_input
        

# Example usage
#if __name__ == "__main__":
#    gene_name="APOE"
#    print(run_clinvar(gene_name))
