import os
from pathlib import Path

from dotenv import load_dotenv

from utils import SensitiveStr

load_dotenv()


SPECIES_OF_INTEREST = {
    "Mus musculus": 10090,
    "Rattus norvegicus": 10116,
    "Caenorhabditis elegans": 6239,
    "Drosophila melanogaster": 7227,
    "Danio rerio": 7955,
}


# Keys
NCBI_API_KEY = SensitiveStr(os.getenv("NCBI_API_KEY"))


# Links
UNIPROT_SEARCH = "https://rest.uniprot.org/uniprotkb/search"
ENSEMBL_SEARCH = "https://rest.ensembl.org/lookup/symbol/homo_sapiens/{}?expand=1"


# Paths
PATH_TO_HUGO_DB = Path(Path(__file__).parent, "data/hugo_db/non_alt_loci_set.json")
PATH_TO_PARSED_GENES = Path(Path(__file__).parent, "data/parsed_genes")
PATH_TO_ORTHOLOGS = Path(Path(__file__).parent, "data/orthologs")
PATH_TO_PARSED_TEXTS = Path(Path(__file__).parent, "data/full_texts_outputs")
PATH_TO_GENAGE_HUMAN_GENES = Path(
    Path(__file__).parent, "data/genage_genes/genage_human.csv"
)
PATH_TO_GENAGE_MODEL_GENES = Path(
    Path(__file__).parent, "data/genage_genes/genage_model.csv"
)
PATH_TO_TEST_DATA_LLM = Path(Path(__file__).parent, "data/test_data")
PATH_TO_LOGS = Path(Path(__file__).parent, "data/logs/pipeline.log")
