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

    gene_file = Path(path_to_output, f"{gene_name}.json")

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
    query_input = target_gene.get_synonym_list_for_gene()
    logger.info(f"Extracted target gene synonyms are: {query_input}")

    logger.info(f"Started parsing texts for {query_input.protein_symbol}")
    run_text_parser_all(
        query_input, str(Path(PATH_TO_PARSED_TEXTS, target_gene.symbol))
    )

    logger.info(f"Finished pipeline for {gene_name}")

    return target_gene
