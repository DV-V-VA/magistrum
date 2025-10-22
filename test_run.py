import logging

import pandas as pd

from config import PATH_TO_LOGS
from logging_config import setup_logging
from pipeline import run_pipeline

df = pd.read_csv("/home/sasha/longevity_project_dev/data/genage_genes/genage_human.csv")
gene_list = list(df.sort_values(by="symbol")["symbol"])
gene_list.remove("APOE")
gene_list.remove("NFE2L2")
# gene_list.remove("SOX2")
# gene_list.remove("OCT4")

final_gene_list = ["NFE2L2", "APOE", "SOX2", "OCT4"]
final_gene_list.extend(gene_list)

setup_logging(PATH_TO_LOGS)
logger = logging.getLogger(__name__)

for gene in ["A2M"]:
    try:
        run_pipeline(gene, force_rerun=True)
    except Exception as e:
        logger.error(gene)
        logger.error(e)
