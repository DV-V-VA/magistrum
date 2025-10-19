import logging
import subprocess

from config import PATH_TO_LOGS
from gene import QueryInput
from logging_config import setup_logging

setup_logging(PATH_TO_LOGS)
logger = logging.getLogger(__name__)


def run_text_parser_all(query_input: QueryInput, out_prefix: str):
    """Run text parsing for gene"""

    logger.info(
        f"Parsing texts for {query_input.protein_symbol} and its "
        + "synonyms {query_input.synonyms}",
    )

    cmd = [
        "python",
        "-m",
        "proteinkb_parse.main",
        "build-kb",
        "--protein",
        query_input.protein_symbol,
        "--syn",
        *query_input.synonyms,
        "--out_prefix",
        out_prefix,
    ]

    logger.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd)

    if result.returncode == 0:
        logger.info(
            f"Finished text parsing for {query_input.protein_symbol} "
            + "and its synonyms {query_input.synonyms}. "
            + "Results will be at {out_prefix}",
        )
        print("ok")
    else:
        logger.error(result.stderr)
        pass
