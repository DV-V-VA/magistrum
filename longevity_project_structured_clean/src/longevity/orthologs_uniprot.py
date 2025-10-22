from dataclasses import dataclass

import requests

from config import UNIPROT_SEARCH


@dataclass
class UniprotDatasetsResponse:
    pass


def get_orthologs_for_gene_uniprot(symbol, organism_id=9606, size=1):
    q = f"gene_exact:{symbol} AND organism_id:{organism_id}"
    params = {"query": q, "format": "json", "size": size}
    r = requests.get(UNIPROT_SEARCH, params=params, headers={"Accept": "application/json"})
    r.raise_for_status()
    return r.json()
