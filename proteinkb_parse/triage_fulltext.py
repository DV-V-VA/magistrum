#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import re
import math
from datetime import datetime
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

import aiohttp

EPMC_SEARCH = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
EPMC_FTXT = "https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
EPMC_ANN = "https://www.ebi.ac.uk/europepmc/annotations_api/annotationsByArticleIds"
UNPAYWALL = "https://api.unpaywall.org/v2/{doi}"
OPENALEX_WORK = "https://api.openalex.org/works/doi:{doi}"
OPENALEX_VENUE = "https://api.openalex.org/venues/{venue_id}"

SCORE_MIN_FOR_FULLTEXT = 60

HTTP_HEADERS = {
    "User-Agent": "proteinkb/0.2 (+contact@example.org)",
    "Accept": "application/json",
}

RPS_EPMC = 8
RPS_UPW = 4
RPS_OPENALEX = 5

TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)

FUNC_WORDS = (
    r"\b(kcat|k\s*cat|k\s*/?\s*m|kcat\s*/\s*k\s*m|ic50|ec50|kd|affinity|binding|"
    r"activity|turnover|stability|tm|melting|thermal\s*shift|fluorescence|reporter|growth|"
    r"fitness|complementation|enzymatic)\b"
)
RE_FUNC = re.compile(FUNC_WORDS, re.IGNORECASE)

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
                        f"Non-JSON from {url} (status {r.status}, "
                        f"CT={r.headers.get('Content-Type')}): {text[:200]!r}"
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
    return await fetch_text(
        session, url,
        headers={"Accept": "application/xml, text/xml;q=0.9, */*;q=0.8"}
    )

async def epmc_annotations_any(session, rl: RL, ids: List[str]) -> Dict[str, bool]:
    out = {}
    for i in range(0, len(ids), 8):
        chunk = ids[i:i+8]
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

async def oax_get_work(session, rl: RL, doi: str) -> Dict[str, Any]:
    if not doi:
        return {}
    await rl.tick()
    url = OPENALEX_WORK.format(doi=doi.lower())
    params = {"mailto": "dolgusheva14vl@egmail.com"}
    return await fetch_json(session, url, headers=HTTP_HEADERS, params=params)

async def oax_get_venue(session, rl: RL, venue_id: str) -> Dict[str, Any]:
    if not venue_id:
        return {}
    await rl.tick()
    vid = venue_id.split("/")[-1]
    url = OPENALEX_VENUE.format(venue_id=vid)
    params = {"mailto": "dolgusheva14vl@egmail.com"}
    return await fetch_json(session, url, headers=HTTP_HEADERS, params=params)

async def openalex_collect_metrics(session, rl: RL, dois: List[str]):
    work_map: Dict[str, Dict[str, Any]] = {}
    venue_ids: List[str] = []

    for doi in [d for d in (dois or []) if d]:
        w = await oax_get_work(session, rl, doi)
        if not isinstance(w, dict) or not w:
            continue
        cited = int(w.get("cited_by_count") or 0)
        oa = False
        oa_block = w.get("open_access") or {}
        if isinstance(oa_block, dict) and "is_oa" in oa_block:
            oa = bool(oa_block.get("is_oa"))
        elif "is_oa" in w:
            oa = bool(w.get("is_oa"))
        host_venue = w.get("host_venue") or {}
        venue_id = host_venue.get("id")
        if venue_id:
            venue_ids.append(venue_id)
        work_map[doi] = {
            "cited_by_count": cited,
            "is_oa": oa,
            "host_venue_id": venue_id,
        }

    venue_map: Dict[str, Dict[str, Any]] = {}
    for vid in sorted(set([v for v in venue_ids if v])):
        v = await oax_get_venue(session, rl, vid)
        if not isinstance(v, dict) or not v:
            continue
        two_year = None
        ss = v.get("summary_stats") or {}
        if isinstance(ss, dict) and "2yr_mean_citedness" in ss:
            two_year = ss.get("2yr_mean_citedness")
        elif "2yr_mean_citedness" in v:
            two_year = v.get("2yr_mean_citedness")
        venue_map[vid.split("/")[-1]] = {
            "2yr_mean_citedness": two_year,
            "h_index": v.get("h_index"),
        }

    return work_map, venue_map

def novelty_points(year: Optional[int]) -> (int, Optional[str]):
    """Freshness bonus, higher for recent years."""
    if not year:
        return 0, None
    now_y = datetime.utcnow().year
    age = max(0, now_y - int(year))
    if age <= 1:
        return 20, f"novelty_{year}"
    if age <= 3:
        return 15, f"novelty_{year}"
    if age <= 5:
        return 10, f"novelty_{year}"
    if age <= 10:
        return 5, f"novelty_{year}"
    return 0, f"novelty_{year}"

def citations_points(cited_by: int) -> (int, str):
    cited_by = max(0, int(cited_by or 0))
    pts = min(20, int(math.log1p(cited_by) * 5))
    return pts, f"citations_{cited_by}"

def venue_influence_points(two_year_mean: Optional[float], h_index: Optional[int]) -> (int, str):
    reason_bits = []
    if two_year_mean is not None:
        x = float(two_year_mean)
        if x >= 10:
            pts = 20
        elif x >= 5:
            pts = 15
        elif x >= 2:
            pts = 10
        elif x >= 1:
            pts = 5
        else:
            pts = 0
        reason_bits.append(f"venue_2yr_mean_citedness_{x:.2f}")
        return pts, ";".join(reason_bits)

    h = int(h_index or 0)
    if h >= 200:
        pts = 20
    elif h >= 100:
        pts = 15
    elif h >= 50:
        pts = 10
    elif h >= 20:
        pts = 5
    else:
        pts = 0
    reason_bits.append(f"venue_h_index_{h}")
    return pts, ";".join(reason_bits)

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

def quick_score(
    a: Article,
    syn_re,
    *,
    cited_by_count: int = 0,
    venue_2yr_mean: Optional[float] = None,
    venue_h_index: Optional[int] = None,
    oa_known: bool = False,
) -> (int, List[str]):
    txt = f"{a.title} {a.abstract}"
    reasons: List[str] = []
    score = 0

    if syn_re.search(txt):
        score += 25
        reasons.append("protein_synonym")

    if RE_FUNC.search(txt):
        score += 25
        reasons.append("functional_metric")

    pts, r = novelty_points(a.year)
    score += pts
    if r:
        reasons.append(r)

    cpts, cr = citations_points(cited_by_count)
    score += cpts
    reasons.append(cr)

    vpts, vr = venue_influence_points(venue_2yr_mean, venue_h_index)
    score += vpts
    reasons.append(vr)

    if oa_known:
        score += 10
        reasons.append("open_access")

    if any(pt.lower() in ("review", "editorial", "comment") for pt in (a.pub_types or [])):
        score -= 30
        reasons.append("not_primary_research")

    return score, reasons

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
    upw_rl = RL(RPS_UPW)
    oax_rl = RL(RPS_OPENALEX)

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

    async with aiohttp.ClientSession(timeout=TIMEOUT, trust_env=True, headers=HTTP_HEADERS) as session:
        logger.info("Loaded %d PubMed entries for triage", len(arts))

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

        dois = [a.doi for a in arts if a.doi]
        work_map, venue_map = await openalex_collect_metrics(session, oax_rl, dois)

        results: List[Triage] = []
        fetched = 0

        for a in arts:
            w = work_map.get(a.doi or "", {})
            cited_by = int(w.get("cited_by_count") or 0)
            oa_known = bool(w.get("is_oa", False))

            venue_id_raw = w.get("host_venue_id")
            venue_id = venue_id_raw.split("/")[-1] if venue_id_raw else None
            v = venue_map.get(venue_id or "", {})
            v2yr = v.get("2yr_mean_citedness")
            vh = v.get("h_index")

            base_score, reasons = quick_score(
                a,
                syn_re,
                cited_by_count=cited_by,
                venue_2yr_mean=v2yr,
                venue_h_index=vh,
                oa_known=oa_known,
            )

            has_ann = ann_map.get("PMID:" + a.pmid, False) or (
                a.pmcid and ann_map.get("PMC:" + a.pmcid.replace("PMC", ""), False)
            )
            if has_ann:
                base_score += 10
                reasons.append("EPMC_annotations")

            if use_semantic and st_model:
                emb = st_model.encode(f"{a.title}. {a.abstract}", normalize_embeddings=True)
                sim = float((emb * q_emb).sum())
                delta = max(0, int((sim + 1.0) * 10))
                base_score += delta
                reasons.append(f"sem_sim_{delta}")

            is_rev = any(pt.lower() == "review" for pt in (a.pub_types or []))
            oa = oa_known
            oa_route = "OPENALEX" if oa_known else None
            fulltext_path = None
            pmcid = a.pmcid

            if (not is_rev) and (base_score > SCORE_MIN_FOR_FULLTEXT):
                if not pmcid and a.doi:
                    logger.debug(
                        "[%s] score=%d > %d, resolving PMCID via EPMC core by DOI",
                        a.pmid, base_score, SCORE_MIN_FOR_FULLTEXT,
                    )
                    core = await epmc_core(session, epmc_rl, a.doi)
                    pmcid = core.get("pmcid") or pmcid
                    if not oa and core.get("isOpenAccess"):
                        oa = True
                        oa_route = oa_route or "EPMC_FLAG"

                if pmcid:
                    xml = await epmc_fulltext_xml(session, epmc_rl, pmcid)
                    if xml and len(xml) > 1000:
                        oa = True
                        oa_route = "EPMC_XML"
                        fulltext_path = os.path.join(out_prefix, "fulltext_xml", f"{pmcid}.xml")
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
                        oa_route = "UNPAYWALL"
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
                    has_tmvar=False,       
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
        description="Triage & fulltext fetch (Europe PMC + OpenAlex + optional Unpaywall + optional semantics)"
    )
    ap.add_argument("--in_jsonl", required=True)
    ap.add_argument("--out", default="./out/triage")
    ap.add_argument("--protein", required=True)
    ap.add_argument("--syn", nargs="*", default=[])
    ap.add_argument("--unpaywall_email", default=None)
    ap.add_argument("--use_semantic", action="store_true")
    ap.add_argument("--sem_model", default="pritamdeka/S-Biomed-Roberta-snli-multinli-stsb")
    args = ap.parse_args()

    syns = list({args.protein} | set(args.syn or []))

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
