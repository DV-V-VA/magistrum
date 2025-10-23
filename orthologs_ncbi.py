import json
import logging
import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from config import NCBI_API_KEY, PATH_TO_LOGS, PATH_TO_ORTHOLOGS, SPECIES_OF_INTEREST
from logging_config import setup_logging
from utils import download_rate_limiter

setup_logging(PATH_TO_LOGS)  # TODO remove later
logger = logging.getLogger(__name__)


class OrthologRetrieveError(Exception):
    pass


class OrthologResolveQueryTypeError(Exception):
    pass


@dataclass
class Ortholog:
    query_gene: str
    taxname: str
    common_name: str
    tax_id: str
    symbol: str
    synonyms: list[str]
    description: str
    summary: list[Any]
    protein_sequence: str = field(default="")
    last_modified: str = str(datetime.now())
    prev_modified: list[str] = field(default_factory=list)


@dataclass
class NCBIDatasetsResponse:
    orthologs: list[Ortholog]
    parsed_response: list = field(repr=False)
    # raw_response: str = field(repr=False, default="")


def resolve_query_type(gene: str | int) -> list[str]:
    """Function that resolves gene to gene-id, symbol or accession"""

    pattern = r"^[A-Z]{2}_[0-9]+\.[0-9]+$"

    if isinstance(gene, int):
        return ["gene-id", str(gene)]
    elif isinstance(gene, str) and bool(re.match(pattern, gene)):
        return ["accession", gene]
    elif isinstance(gene, str):
        try:
            gene = int(gene)
            return ["gene-id", str(gene)]
        except ValueError:
            return ["symbol", str(gene)]
    else:
        raise OrthologResolveQueryTypeError


@download_rate_limiter("ncbi", 10)
def get_orthologs_for_gene_ncbi(
    gene: int | str,
    species_of_interest: list[int] = list(SPECIES_OF_INTEREST.values()),
    save_output: bool = True,
    force_rerun: bool = False,
    path_to_output: Path = PATH_TO_ORTHOLOGS,
    api_key: str | None = NCBI_API_KEY,
) -> NCBIDatasetsResponse:
    """Get gene orthologs via gene-id, accession or symbol via NCBI API"""

    logger.info(f"Start NCBI search for: {gene}")

    gene_othologs_file = Path(path_to_output, f"{gene}.json")
    gene_othologs_file_raw = Path(path_to_output, f"{gene}_raw.json")

    if force_rerun or not gene_othologs_file.exists():
        logger.debug(f"Resolving query type for: {gene}")

        resolved_query = resolve_query_type(gene)
        resolved_api_key = ["--api-key", api_key] if api_key is not None else []
        logger.debug(f"Resolved as: {resolved_query[0]}")
        cmd = [
            "datasets",
            "summary",
            "gene",
            *resolved_query,
            *[arg for s in species_of_interest for arg in ("--ortholog", str(s))],
            "--as-json-lines",
            *resolved_api_key,
        ]

        logger.debug(f"CMD: {cmd}")

        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        if proc.returncode != 0:
            logger.error("Error running command:", proc.stderr)
            raise OrthologRetrieveError

        raw_response = proc.stdout
        parsed_response = []
        for line in raw_response.splitlines():
            if line.strip():
                parsed_response.append(json.loads(line))

        if not parsed_response:
            logger.warning(f"No data found for {gene}")

        if save_output:
            with open(gene_othologs_file, "w") as f:
                logger.info(f"Results will be saved at {gene_othologs_file}")
                f.write(json.dumps(parsed_response, indent=4))
            with open(gene_othologs_file_raw, "w") as f:
                logger.debug(f"Raw results will be saved at {gene_othologs_file_raw}")
                f.write(json.dumps(parsed_response, indent=4))

        logger.info(f"Finished NCBI search for {gene}")

    else:
        logger.info(f"Trying to read from {gene_othologs_file}")

        with open(gene_othologs_file) as f:
            parsed_response = json.load(f)

    orthologs = []
    for ortholog in parsed_response:
        orthologs.append(
            Ortholog(
                query_gene=str(gene),
                taxname=ortholog.get("taxname"),
                common_name=ortholog.get("common_name"),
                tax_id=ortholog.get("tax_id"),
                symbol=ortholog.get("symbol"),
                synonyms=ortholog.get("synonyms"),
                description=ortholog.get("description"),
                summary=ortholog.get("summary"),
            )
        )

    return NCBIDatasetsResponse(
        orthologs=orthologs,
        parsed_response=parsed_response,  # raw_response=raw_response if raw_response
    )
