import logging

import pandas as pd

from config import PATH_TO_LOGS
from logging_config import setup_logging
from pipeline import run_pipeline

df = pd.read_csv("/home/sasha/longevity_project_dev/data/genage_genes/genage_human.csv")

setup_logging(PATH_TO_LOGS)
logger = logging.getLogger(__name__)

for gene in list(df.sort_values(by="symbol")["symbol"]):
    try:
        run_pipeline(gene, force_rerun=True)
    except Exception as e:
        logger.error(gene)
        logger.error(e)
