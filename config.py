from pathlib import Path
from dotenv import load_dotenv
import os
from utils import SensitiveStr

load_dotenv()

NCBI_API_KEY = SensitiveStr(os.getenv('NCBI_API_KEY'))
UNIPROT_SEARCH = "https://rest.uniprot.org/uniprotkb/search"
ENSEMBL_SEARCH = "https://rest.ensembl.org/lookup/symbol/homo_sapiens/{}?expand=1"

SPECIES_OF_INTEREST = {
    "Mus musculus": 10090,
    "Rattus norvegicus": 10116,
    "Caenorhabditis elegans": 6239,
    "Drosophila melanogaster": 7227,
    "Danio rerio": 7955,
}

PATH_TO_ORTHOLOGS = Path(Path(__file__).parent, "data/orthologs")
PATH_TO_HUGO_DB = Path(Path(__file__).parent, "data/hugo_db/non_alt_loci_set.json")

LOG_PATH = Path(Path(__file__).parent, "data/logs/pipeline.log")
