import json
import logging
import os
from dataclasses import asdict
from pathlib import Path
import tempfile

from clinvar import run_clinvar
from aging_llm import AgingLLM
from config import (
    PATH_TO_COMPLETE_GENES,
    PATH_TO_LOGS,
    PATH_TO_PARSED_GENES,
    PATH_TO_PARSED_TEXTS,
    PATH_TO_RAG,
)
from gene import (
    get_target_gene_with_orthologs_from_file,
    parse_target_gene_with_orthologs,
)
from logging_config import setup_logging
from text_parser_wrapper import run_text_parser_all

PATH_TO_LOGS = os.path.join(tempfile.gettempdir(), "micropipeline.log")
setup_logging(PATH_TO_LOGS)
logger = logging.getLogger(__name__)


def run_pipeline(
    gene_name: str,
    save_output: bool = True,
    force_rerun: bool = False,
    path_to_output: Path = PATH_TO_PARSED_GENES,
):
    """Run data parsing pipeline for a gene"""

    logger.info("Starting pipeline")

    # Declare paths
    gene_file = Path(path_to_output, f"{gene_name}.json")
    parsed_full_text_folder_path = Path(PATH_TO_PARSED_TEXTS, gene_name)
    parsed_full_text_folder_path_mutations = Path(PATH_TO_PARSED_TEXTS, f"{gene_name}/mutations")
    complete_gene_file = Path(PATH_TO_COMPLETE_GENES, f"{gene_name}.json")
    rag_path = Path(PATH_TO_RAG, gene_name)
    rag_path_mutations = Path(PATH_TO_RAG, f"{gene_name}/mutations")

    # Create Gene obj
    try:
        if force_rerun or not gene_file.exists():
            target_gene = parse_target_gene_with_orthologs(
                gene_name=gene_name,
                gene_file=gene_file,
                save_output=save_output,
                force_rerun=force_rerun,
            )

        else:
            logger.info(f"Trying to read from {gene_file}")
            try:
                target_gene = get_target_gene_with_orthologs_from_file(gene_file)
            except Exception as e:
                logger.error(f"Failed to read file target gene from file {gene_file}")
                logger.error(e)
                target_gene = parse_target_gene_with_orthologs(
                    gene_name=gene_name,
                    gene_file=gene_file,
                    save_output=save_output,
                    force_rerun=force_rerun,
                )
    except Exception as e:
        logger.error("Error in gene obj creation!")
        logger.error(e)
        raise e

    # Find mutations using Clinvar & create query
    try:
        query_input = run_clinvar(gene_name)
        logger.info(f"Extracted target pmids are: {query_input}")
    except Exception as e:
        logger.error("Error in query creation!")
        logger.error(e)
        raise e

    # Extract full texts about mutations
    try:
        if (
            force_rerun
            or not Path(parsed_full_text_folder_path, "triage/fulltext_xml/mutations").exists()
        ):
            logger.info(f"Started parsing mutation texts for {gene_name}")
            run_text_parser_all(query_input, str(parsed_full_text_folder_path_mutations))
        else:
            logger.info(f"Will reuse full texts at {parsed_full_text_folder_path_mutations}")
    except Exception as e:
        logger.error("Error in full text extraction!")
        logger.error(e)
        raise e
    
    try:
        # Run LLM
        section_names = ["Gene Overview", "Variants/Alleles", "Relation to Aging/Longevity", "Interactions", "Related Genes"]
        llm = AgingLLM(target_gene.symbol)
        ### Construct rag if forced or no rag
        if force_rerun or not rag_path.exists():
            rag_path = llm.text_rag(
                str(Path(parsed_full_text_folder_path, "triage/fulltext_xml/mutations"))
            )
            print(rag_path)
        ### Get llm summary for gene
        #for section in section_names:
        target_gene.llm_variants = llm.llm_response(target_gene.symbol, str(rag_path), section_name="Variants/Alleles")
        # нужно указать section_name из листа выше
    except Exception as e:
        logger.error("Error in LLM!")
        logger.error(e)
        raise e
    
if __name__=='__main__':
    run_pipeline("A2M")