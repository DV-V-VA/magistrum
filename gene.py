from dataclasses import dataclass, field, asdict
from config import PATH_TO_HUGO_DB, PATH_TO_LOGS
from pathlib import Path
import json
import logging
from logging_config import setup_logging

from orthologs_ncbi import Ortholog, get_orthologs_for_gene_ncbi
from datetime import datetime

setup_logging(PATH_TO_LOGS)  # TODO remove later
logger = logging.getLogger(__name__)


@dataclass
class GeneID:
    name: str
    value: list[str]


@dataclass
class Gene:
    symbol: str
    locus_group: str
    last_modified: str
    cytoband: str = field(default="No cytoband data found")

    n_papers: int = field(default_factory=int)

    refseq_summary: str = field(default="No summary found")
    uniprot_full_names: list[str] = field(default_factory=list)
    all_aliases: list[str] = field(default_factory=list)

    hgnc_name: str = field(default="No name found")

    hgnc_prev_name: list[str] = field(default_factory=list)
    hgnc_prev_symbols: list[str] = field(default_factory=list)

    hgnc_alias_symbols: list[str] = field(default_factory=list)
    hgnc_alias_names: list[str] = field(default_factory=list)

    omim: list[str] = field(default_factory=list)

    mane_select: list[str] = field(default_factory=list)
    gene_ids: list[GeneID] = field(default_factory=list)

    orthologs: list[Ortholog] = field(default_factory=list)

    prev_modified: list[str] = field(default_factory=list)


def build_gene_index(genes: list[Gene]) -> dict[str, Gene]:
    """Create gene index from list"""
    logger.info("Started indexing HUGO db")
    index = {}
    for gene in genes:
        index[gene.symbol] = gene
    logger.info("Finished indexing HUGO db")
    return index


def read_hugo_db(path_to_hugo_db: Path = PATH_TO_HUGO_DB) -> list[Gene]:
    """Read HUGO db and fill out Gene instances from it"""

    logger.info("Started reading HUGO db")

    with open(path_to_hugo_db) as json_file:
        json_data = json.load(json_file)

    all_gene_data = []
    for instance in json_data["response"]["docs"]:
        gene_ids = []
        for k, v in instance.items():
            if "id" in k:
                gene_ids.append(GeneID(name=k, value=v))

        try:
            all_gene_data.append(
                Gene(
                    symbol=instance.get("symbol"),
                    locus_group=instance.get("locus_group"),
                    last_modified=str(datetime.now()),
                    cytoband=instance.get("location", "No cytoband data found"),
                    hgnc_name=instance.get("name", "No name found"),
                    hgnc_prev_name=instance.get("prev_name", []),
                    hgnc_alias_symbols=instance.get("alias_symbol", []),
                    hgnc_alias_names=instance.get("alias_name", []),
                    omim=instance.get("omim_id", []),
                    mane_select=instance.get("mane_select", []),
                    gene_ids=gene_ids,
                )
            )
        except Exception as e:
            logger.error(f"Couldnt parse the following instance: {instance}")
            raise e

    logger.info("Finished reading HUGO db")
    logger.info(f"Total instances: {len(all_gene_data)}")
    return all_gene_data


def parse_target_gene_with_orthologs(
    gene_name: str, gene_file: Path, save_output: bool = True, force_rerun: bool = False
) -> Gene:
    """Get gene with orthologs"""
    prev_modified = []
    if gene_file.exists():
        try:
            with open(gene_file) as f:
                prev_target_gene = Gene(**json.load(f))
            prev_modified.extend(prev_target_gene.prev_modified)
            prev_modified.append(prev_target_gene.last_modified)
        except Exception:
            logger.error(
                f"Error reading modification history from {gene_file}, will skip and rewrite"
            )
    all_genes_index = build_gene_index(read_hugo_db())

    target_gene = all_genes_index[gene_name]
    target_gene.orthologs = get_orthologs_for_gene_ncbi(
        gene_name, save_output=save_output, force_rerun=force_rerun
    ).orthologs
    target_gene.prev_modified = prev_modified

    if save_output:
        with open(gene_file, "w") as f:
            logger.info(f"Results will be saved at {gene_file}")
            f.write(json.dumps(asdict(target_gene), indent=4))

    return target_gene


def get_target_gene_with_orthologs_from_file(gene_file: Path):
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

    return target_gene
