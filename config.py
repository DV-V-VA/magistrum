from pathlib import Path

SPECIES_OF_INTEREST = {
    "Mus musculus": 10090,
    "Rattus norvegicus": 10116,
    "Caenorhabditis elegans": 6239,
    "Drosophila melanogaster": 7227,
    "Danio rerio": 7955,
}
PATH_TO_ORTHOLOGS = Path("/home/sasha/longevity_project_dev/data/orthologs")

LOG_PATH = Path("/home/sasha/longevity_project_dev/data/logs/pipeline.log")
