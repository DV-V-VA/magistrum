from pathlib import Path
from dotenv import load_dotenv
import os
from utils import SensitiveStr

load_dotenv()

NCBI_API_KEY = SensitiveStr(os.getenv('NCBI_API_KEY'))

SPECIES_OF_INTEREST = {
    "Mus musculus": 10090,
    "Rattus norvegicus": 10116,
    "Caenorhabditis elegans": 6239,
    "Drosophila melanogaster": 7227,
    "Danio rerio": 7955,
}
PATH_TO_ORTHOLOGS = Path("/home/sasha/longevity_project_dev/data/orthologs")

LOG_PATH = Path("/home/sasha/longevity_project_dev/data/logs/pipeline.log")
