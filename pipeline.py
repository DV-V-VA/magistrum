import json
import logging
from dataclasses import asdict
from pathlib import Path

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
    resolve_gene_name,
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

    # Resolve gene name
    gene_name = resolve_gene_name(gene_name)

    # Declare paths
    gene_file = Path(path_to_output, f"{gene_name}.json")
    parsed_full_text_folder_path = Path(PATH_TO_PARSED_TEXTS, gene_name)
    complete_gene_file = Path(PATH_TO_COMPLETE_GENES, f"{gene_name}.json")
    rag_path = Path(PATH_TO_RAG, gene_name)
    # rag_path_mutations = Path(PATH_TO_RAG, gene_name)

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

    # Create query
    try:
        query_input = target_gene.get_synonym_list_for_gene()
        logger.info(f"Extracted target gene synonyms are: {query_input}")
    except Exception as e:
        logger.error("Error in query creation!")
        logger.error(e)
        raise e

    # Extract full texts
    try:
        if (
            force_rerun
            or not Path(parsed_full_text_folder_path, "triage/fulltext_xml").exists()
        ):
            logger.info(f"Started parsing texts for {gene_name}")
            run_text_parser_all(query_input, str(parsed_full_text_folder_path))
        else:
            logger.info(f"Will reuse full texts at {parsed_full_text_folder_path}")
    except Exception as e:
        logger.error("Error in full text extraction!")
        logger.error(e)
        raise e

    try:
        # Run LLM
        llm = AgingLLM(target_gene.symbol)
        ### Construct rag if forced or no rag
        if force_rerun or not rag_path.exists():
            rag_path = llm.text_rag(
                str(Path(parsed_full_text_folder_path, "triage/fulltext_xml"))
            )
            print(rag_path)
        ### Get llm summary for gene
        target_gene.llm_summary = llm.llm_response(target_gene.symbol, str(rag_path))
        # нужно указать section_name из листа выше
    except Exception as e:
        logger.error("Error in LLM!")
        logger.error(e)
        raise e

    if save_output:
        with open(complete_gene_file, "w") as f:
            logger.info(
                f"Results for complete file will be saved at {complete_gene_file}"
            )
            f.write(json.dumps(asdict(target_gene), indent=4))

    logger.info(f"Finished pipeline for {gene_name}")

    return target_gene
