import subprocess

# import logging
# from config import PATH_TO_LOGS
# from logging_config import setup_logging
from config import PATH_TO_PARSED_TEXTS
from gene import QueryInput

# setup_logging(PATH_TO_LOGS)
# logger = logging.getLogger(__name__)


def run_text_parser_all(
    query_input: QueryInput, out_prefix: str = str(PATH_TO_PARSED_TEXTS)
):
    """Run text parsing for gene"""

    # logger.info(
    #     f"Parsing texts for {protein} and its synonyms {syns}",
    # )

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

    # logger.debug("Running command:", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        # logger.info(
        #     f"Finished text parsing for {protein} and its synonyms {syns}. "
        #     + "Results will be at {out_prefix}",
        # )
        print("ok")
    else:
        # logger.error(
        #     f"Failed to parse text for {protein} and its synonyms {syns}",
        # )
        # logger.error(result.stderr)
        pass
