#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

import aiohttp

EPMC_SEARCH = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
EPMC_FTXT = "https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
EPMC_ANN = "https://www.ebi.ac.uk/europepmc/annotations_api/annotationsByArticleIds"
UNPAYWALL = "https://api.unpaywall.org/v2/{doi}"
PUBTATOR = PUBTATOR = (
    "https://www.ncbi.nlm.nih.gov/research/pubtator3-api/publications/export/biocjson?pmids={pmids}&full=true"
)

SCORE_MIN_FOR_FULLTEXT = 40  # fetch fulltext only if score > 40

HTTP_HEADERS = {
    "User-Agent": "proteinkb/0.1 (+contact@example.org)",
    "Accept": "application/json",
}

RPS_EPMC = 8
RPS_PTC = 4
RPS_UPW = 4
TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)

FUNC_WORDS = (
    r"\b(kcat|k\s*cat|k\s*/?\s*m|kcat\s*/\s*k\s*m|ic50|ec50|kd|affinity|binding|\
    activity|turnover|stability|tm|melting|thermal\s*shift|fluorescence|reporter|growth|\
        fitness|complementation|enzymatic)\b"
)
HGVS_PROT = r"p\.\(?[A-Z][a-z]{2}\d+[A-Z][a-z]{2}\)?"
ONE_LETTER = r"\b[A-Z]\d+[A-Z]\b"
SCAN_WORDS = r"(alanine scanning|site-directed mutagenesis|saturation mutagenesis|\
    phospho[- ]mutant|deletion mutant|truncation)"

RE_HGVS = re.compile(HGVS_PROT)
RE_ONE = re.compile(ONE_LETTER)
RE_FUNC = re.compile(FUNC_WORDS, re.IGNORECASE)
RE_SCAN = re.compile(SCAN_WORDS, re.IGNORECASE)


def norm(s: str) -> str:
    return (s or "").strip()


logger = logging.getLogger("proteinkb.triage")


@dataclass
class Article:
    pmid: str
    title: str
    abstract: str
    doi: Optional[str]
    pmcid: Optional[str]
    pub_types: List[str]
    year: Optional[int]


@dataclass
class Triage:
    pmid: str
    doi: Optional[str]
    pmcid: Optional[str]
    oa: bool
    oa_route: Optional[str]
    fulltext_path: Optional[str]
    score: int
    reasons: List[str]
    has_tmvar: bool
    has_epmc_ann: bool
    is_review: bool


class RL:
    def __init__(self, rps: float):
        self.rps, self._t = max(0.1, rps), 0.0

    async def tick(self):
        now = asyncio.get_event_loop().time()
        wait = max(0.0, (1.0 / self.rps) - (now - self._t))
        if wait:
            await asyncio.sleep(wait)
        self._t = asyncio.get_event_loop().time()


async def fetch_json(session, url, **kw):
    for attempt in range(5):
        try:
            async with session.get(url, **kw) as r:
                if r.status == 404:
                    return None
                if r.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(min(8, 0.5 * (2**attempt)))
                    continue
                if r.status == 204:
                    return None
                text = await r.text()
                if not text.strip():
                    await asyncio.sleep(min(8, 0.5 * (2**attempt)))
                    continue
                try:
                    return json.loads(text)
                except json.JSONDecodeError as e:
                    raise RuntimeError(
                        f"Non-JSON from {url} (status {r.status}, \
                            CT={r.headers.get('Content-Type')}): {text[:200]!r}"
                    ) from e
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                return None
            if attempt == 4:
                raise
            await asyncio.sleep(min(8, 0.5 * (2**attempt)))
        except Exception:
            if attempt == 4:
                raise
            await asyncio.sleep(min(8, 0.5 * (2**attempt)))
    return None


async def fetch_text(session, url, **kw):
    for attempt in range(5):
        try:
            async with session.get(url, **kw) as r:
                if r.status == 404:
                    return None
                if r.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(min(8, 0.5 * (2**attempt)))
                    continue
                r.raise_for_status()
                return await r.text()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                return None
            if attempt == 4:
                raise
            await asyncio.sleep(min(8, 0.5 * (2**attempt)))
        except Exception:
            if attempt == 4:
                raise
            await asyncio.sleep(min(8, 0.5 * (2**attempt)))
    return None


async def epmc_core(session, rl: RL, ext_id: str) -> Dict[str, Any]:
    await rl.tick()
    params = {"query": f"EXT_ID:{ext_id}", "resultType": "core", "format": "json"}
    j = await fetch_json(session, EPMC_SEARCH, params=params)
    hit = (j or {}).get("resultList", {}).get("result", [])
    return hit[0] if hit else {}


async def epmc_fulltext_xml(session, rl: RL, pmcid: str) -> Optional[str]:
    await rl.tick()
    url = EPMC_FTXT.format(pmcid=pmcid)
    return await fetch_text(session, url)


async def epmc_annotations_any(session, rl: RL, ids: List[str]) -> Dict[str, bool]:
    out = {}
    for i in range(0, len(ids), 8):
        chunk = ids[i : i + 8]
        await rl.tick()
        params = {"articleIds": ",".join(chunk)}
        j = await fetch_json(session, EPMC_ANN, params=params)
        if isinstance(j, dict):
            anns = j.get("annotations", [])
        elif isinstance(j, list):
            if j and isinstance(j[0], dict) and "annotations" in j[0]:
                anns = [ann for entry in j for ann in (entry.get("annotations") or [])]
            else:
                anns = j
        else:
            anns = []
        for a in anns:
            src = a.get("articleId", "")
            out[src] = True
    return out


async def pubtator_tmvar_has_mut(session, rl: RL, pmids: List[str]) -> Dict[str, bool]:
    out = {}
    for i in range(0, len(pmids), 100):
        batch = pmids[i : i + 100]
        await rl.tick()
        url = PUBTATOR.format(pmids=",".join(batch))
        j = await fetch_json(session, url)
        if not j:
            continue
        docs = j.get("documents") or j.get("PubTator3") or []
        for doc in docs:
            pid = None
            raw_id = str(doc.get("id") or "")
            if raw_id.isdigit():
                pid = raw_id
            if not pid:
                for p in doc.get("passages", []):
                    pmid_inf = p.get("infons", {}).get("article-id_pmid")
                    if pmid_inf and str(pmid_inf).isdigit():
                        pid = str(pmid_inf)
                        break
            if not pid:
                continue
            anns = doc.get("passages", [])
            flag = False
            for p in anns:
                for ann in p.get("annotations", []):
                    typ = str(ann.get("infons", {}).get("type", "")).lower()
                    if typ in (
                        "mutation",
                        "variant",
                        "genetic_variant",
                        "genetic variant",
                        "variantmention",
                    ):
                        flag = True
                        break
                if flag:
                    break
            out[pid] = flag
    return out


def quick_score(a: Article, syn_re) -> (int, list):
    txt = f"{a.title} {a.abstract}"
    reasons = []
    score = 0
    if syn_re.search(txt):
        score += 25
        reasons.append("protein_synonym")
    if RE_HGVS.search(txt) or RE_ONE.search(txt) or RE_SCAN.search(txt):
        score += 25
        reasons.append("mutation_pattern")
    if RE_FUNC.search(txt):
        score += 25
        reasons.append("functional_metric")
    if any(
        pt.lower() in ("review", "editorial", "comment") for pt in (a.pub_types or [])
    ):
        score -= 30
        reasons.append("not_primary_research")
    if a.year and a.year >= 2015:
        score += 5
    return score, reasons


def compile_synonyms(syns: List[str]):
    syns2 = [re.escape(x) for x in syns if x.strip()]
    return (
        re.compile(r"\b(" + "|".join(syns2) + r")\b", re.IGNORECASE)
        if syns2
        else re.compile("$a")
    )


def save_bytes(path: str, data: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(data)


async def triage(
    in_jsonl: str,
    out_prefix: str,
    protein_syns: List[str],
    upw_email: Optional[str] = None,
    use_semantic: bool = False,
    sem_model: str = "pritamdeka/S-Biomed-Roberta-snli-multinli-stsb",
):
    syn_re = compile_synonyms(protein_syns)
    st_model = None
    q_emb = None
    if use_semantic:
        from sentence_transformers import SentenceTransformer

        st_model = SentenceTransformer(sem_model)
        q_emb = st_model.encode(
            [
                "experimental sequence-to-function mapping",
                "site-directed mutagenesis functional assay",
                "deep mutational scanning with activity or binding",
                "lifespan extension",
            ],
            normalize_embeddings=True,
        ).mean(axis=0)

    epmc_rl = RL(RPS_EPMC)
    ptc_rl = RL(RPS_PTC)
    upw_rl = RL(RPS_UPW)

    arts: List[Article] = []
    with open(in_jsonl, "r", encoding="utf-8") as f:
        for line in f:
            j = json.loads(line)
            arts.append(
                Article(
                    pmid=str(j.get("pmid", "")),
                    title=(j.get("title", "") or "").strip(),
                    abstract=(j.get("abstract", "") or "").strip(),
                    doi=(j.get("doi") or None),
                    pmcid=(j.get("pmcid") or None),
                    pub_types=j.get("pub_types") or [],
                    year=j.get("year"),
                )
            )

    async with aiohttp.ClientSession(timeout=TIMEOUT, trust_env=True) as session:
        logger.info("Loaded %d PubMed entries for triage", len(arts))
        pmid_list = [a.pmid for a in arts if a.pmid]
        tmvar_map = await pubtator_tmvar_has_mut(session, ptc_rl, pmid_list)
        logger.info(
            "PubTator tmVar flags: %d/%d",
            sum(1 for v in tmvar_map.values() if v),
            len(pmid_list),
        )
        epmc_ids = [
            ("PMC:" + a.pmcid.replace("PMC", "")) if a.pmcid else ("MED:" + a.pmid)
            for a in arts
            if a.pmid
        ]
        ann_map = await epmc_annotations_any(session, epmc_rl, epmc_ids)
        logger.info(
            "EuropePMC annotations present for: %d/%d",
            sum(1 for k, v in ann_map.items() if v),
            len(epmc_ids),
        )

        results: List[Triage] = []
        fetched = 0
        for a in arts:
            base_score, reasons = quick_score(a, syn_re)
            has_tmvar = tmvar_map.get(a.pmid, False)
            has_ann = ann_map.get("PMID:" + a.pmid, False) or (
                a.pmcid and ann_map.get("PMC:" + a.pmcid.replace("PMC", ""), False)
            )
            if has_tmvar:
                base_score += 15
                reasons.append("tmVar_mutation")
            if has_ann:
                base_score += 10
                reasons.append("EPMC_annotations")

            if use_semantic and st_model:
                emb = st_model.encode(
                    f"{a.title}. {a.abstract}", normalize_embeddings=True
                )
                sim = float((emb * q_emb).sum())
                delta = max(0, int((sim + 1.0) * 10))
                base_score += delta
                reasons.append(f"sem_sim_{delta}")

            is_rev = any(pt.lower() == "review" for pt in (a.pub_types or []))
            oa = False
            oa_route = None
            fulltext_path = None
            pmcid = a.pmcid

            if (not is_rev) and (base_score > SCORE_MIN_FOR_FULLTEXT):
                if not pmcid and a.doi:
                    logger.debug(
                        "[%s] score=%d > %d, resolving PMCID via EPMC core by DOI",
                        a.pmid,
                        base_score,
                        SCORE_MIN_FOR_FULLTEXT,
                    )
                    core = await epmc_core(session, epmc_rl, a.doi)
                    pmcid = core.get("pmcid")
                if pmcid:
                    xml = await epmc_fulltext_xml(session, epmc_rl, pmcid)
                    if xml and len(xml) > 1000:
                        oa = True
                        oa_route = "EPMC_XML"
                        fulltext_path = os.path.join(
                            out_prefix, "fulltext_xml", f"{pmcid}.xml"
                        )
                        os.makedirs(os.path.dirname(fulltext_path), exist_ok=True)
                        with open(fulltext_path, "w", encoding="utf-8") as f:
                            f.write(xml)
                        reasons.append("OA_fulltext_EPMC")
                        fetched += 1
                        logger.info("[%s] saved EPMC XML → %s", a.pmid, fulltext_path)
                if (not oa) and a.doi and upw_email:
                    await upw_rl.tick()
                    j = await fetch_json(
                        session,
                        UNPAYWALL.format(doi=a.doi),
                        params={"email": upw_email},
                    )
                    if j and j.get("is_oa"):
                        oa = True
                        oa_route = "UNPAYWALL_PDF"
                        reasons.append("OA_via_Unpaywall")
                        logger.info("[%s] OA via Unpaywall", a.pmid)
            else:
                logger.debug(
                    "[%s] score=%d%s — skip fulltext fetch",
                    a.pmid,
                    base_score,
                    ", review" if is_rev else "",
                )

            results.append(
                Triage(
                    pmid=a.pmid,
                    doi=a.doi,
                    pmcid=pmcid or a.pmcid,
                    oa=oa,
                    oa_route=oa_route,
                    fulltext_path=fulltext_path,
                    score=base_score,
                    reasons=reasons,
                    has_tmvar=has_tmvar,
                    has_epmc_ann=has_ann,
                    is_review=is_rev,
                )
            )
        logger.info("Triage done: %d items; fulltexts saved: %d", len(results), fetched)

    os.makedirs(out_prefix, exist_ok=True)
    with open(os.path.join(out_prefix, "triage.jsonl"), "w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(asdict(r), ensure_ascii=False) + "\n")
    with open(os.path.join(out_prefix, "keep.pmids.txt"), "w", encoding="utf-8") as f:
        for r in results:
            if (not r.is_review) and r.score > SCORE_MIN_FOR_FULLTEXT:
                f.write(r.pmid + "\n")


def main():
    import argparse

    ap = argparse.ArgumentParser(
        description="Triage & fulltext fetch \
            (Europe PMC + PubTator + optional semantics)"
    )
    ap.add_argument("--in_jsonl", required=True)
    ap.add_argument("--out", default="./out/triage")
    ap.add_argument("--protein", required=True)
    ap.add_argument("--syn", nargs="*", default=[])
    ap.add_argument("--unpaywall_email", default=None)
    ap.add_argument("--use_semantic", action="store_true")
    ap.add_argument(
        "--sem_model", default="pritamdeka/S-Biomed-Roberta-snli-multinli-stsb"
    )
    args = ap.parse_args()
    syns = list({args.protein} | set(args.syn or []))
    import asyncio

    asyncio.run(
        triage(
            args.in_jsonl,
            args.out,
            syns,
            args.unpaywall_email,
            args.use_semantic,
            args.sem_model,
        )
    )


if __name__ == "__main__":
    main()
