import logging
from pathlib import Path

from config import PATH_TO_LOGS, PATH_TO_PARSED_GENES, PATH_TO_PARSED_TEXTS
from gene import (
    get_target_gene_with_orthologs_from_file,
    parse_target_gene_with_orthologs,
)
from logging_config import setup_logging
from text_parser_wrapper import run_text_parser_all

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

    # Create Gene obj
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

    # Create query
    query_input = target_gene.get_synonym_list_for_gene()
    logger.info(f"Extracted target gene synonyms are: {query_input}")

    # Extract full texts
    if force_rerun or not parsed_full_text_folder_path.exists():
        logger.info(f"Started parsing texts for {gene_name}")
        run_text_parser_all(query_input, str(parsed_full_text_folder_path))
    else:
        logger.info(f"Will reuse full texts at {parsed_full_text_folder_path}")

    logger.info(f"Finished pipeline for {gene_name}")

    return target_gene
