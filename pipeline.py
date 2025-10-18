from gene import read_hugo_db, build_gene_index
import logging
from logging_config import setup_logging
from config import PATH_TO_LOGS, PATH_TO_PARSED_GENES
from orthologs_ncbi import get_orthologs_for_gene_ncbi
from pathlib import Path
from gene import Gene, GeneID
from orthologs_ncbi import Ortholog
import json

from dataclasses import asdict


setup_logging(PATH_TO_LOGS)  # TODO remove later
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
        all_genes_index = build_gene_index(read_hugo_db())

        target_gene = all_genes_index[gene_name]
        target_gene.orthologs = get_orthologs_for_gene_ncbi(gene_name).orthologs

        if save_output:
            with open(gene_file, "w") as f:
                logger.info(f"Results will be saved at {gene_file}")
                f.write(json.dumps(asdict(target_gene), indent=4))

    else:
        logger.info(f"Trying to read from {gene_file}")

        with open(gene_file) as f:
            target_gene = Gene(**json.load(f))

        target_gene.gene_ids = [
            GeneID(**gene_id)
            for gene_id in target_gene.gene_ids
            if isinstance(gene_id, dict)
        ]

        target_gene.orthologs = [
            Ortholog(**ortholog)
            for ortholog in target_gene.orthologs
            if isinstance(ortholog, dict)
        ]

    logger.info(f"Finished pipeline for {gene_name}")

    return target_gene
