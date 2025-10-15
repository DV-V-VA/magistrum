import subprocess
import json
from pathlib import Path
import logging
import re
from dataclasses import dataclass, field
from typing import Any

from config import SPECIES_OF_INTEREST, PATH_TO_ORTHOLOGS, LOG_PATH, NCBI_API_KEY
from logging_config import setup_logging
from utils import download_rate_limiter

setup_logging(LOG_PATH)  # TODO remove later
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
    tax_id: int
    symbol: str
    synonyms: list[str]
    description: str
    summary: list[Any]


@dataclass
class NCBIDatasetsResponse:
    orthologs: list[Ortholog]
    parsed_response: list = field(repr=False)
    raw_response: str = field(repr=False)


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
            return ["symbol", gene]
    else:
        raise OrthologResolveQueryTypeError


@download_rate_limiter("ncbi", 10)
def get_orthologs_for_gene(
    gene: int | str,
    species_of_interest: list[int] = SPECIES_OF_INTEREST.values(),
    save_output: bool = True,
    path_to_output: Path = PATH_TO_ORTHOLOGS,
    api_key: str | None = NCBI_API_KEY,
) -> NCBIDatasetsResponse:
    """Get gene orthologs via gene-id, accession or symbol via NCBI API"""

    logger.info(f"Start search for: {gene}")
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
        with open(Path(path_to_output, f"{gene}.json"), "w") as f:
            f.write(json.dumps(parsed_response, indent=4))

    logger.info(f"Finished for {gene}")

    orthologs = []
    for ortholog in parsed_response:
        orthologs.append(
            Ortholog(
                query_gene=gene,
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
        orthologs=orthologs, parsed_response=parsed_response, raw_response=raw_response
    )
