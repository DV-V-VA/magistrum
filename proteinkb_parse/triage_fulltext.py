#!/usr/bin/env python3
import asyncio
import json
import logging
import math
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

EPMC_SEARCH = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
EPMC_FTXT = "https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
EPMC_ANN = "https://www.ebi.ac.uk/europepmc/annotations_api/annotationsByArticleIds"

UNPAYWALL = "https://api.unpaywall.org/v2/{doi}"
OPENALEX_WORK = "https://api.openalex.org/works/doi:{doi}"
OPENALEX_VENUE = "https://api.openalex.org/venues/{venue_id}"

SCORE_MIN_FOR_FULLTEXT = 50

HTTP_HEADERS = {
    "User-Agent": "proteinkb/0.2 (dolgusheva14vl@gmail.com)",
    "Accept": "application/json",
}

BROWSER_UA = os.environ.get(
    "PROTEINKB_BROWSER_UA",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
    + " (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
)

RPS_EPMC = 8
RPS_UNPAYWALL = 2
RPS_OPENALEX = 5


class RL:
    def __init__(self, rps: float):
        self._interval = 1.0 / max(rps, 0.1)
        self._t = 0.0

    async def tick(self):
        now = asyncio.get_event_loop().time()
        wait = max(0.0, self._interval - (now - self._t))
        if wait > 0:
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


def html_to_xml(html_bytes: bytes) -> bytes:
    try:
        from lxml import etree
        from lxml import html as lxml_html

        doc = lxml_html.fromstring(html_bytes)
        xml_bytes = etree.tostring(doc, method="xml", encoding="utf-8")
        if not xml_bytes.strip().startswith(b"<?xml"):
            xml_bytes = b'<?xml version="1.0" encoding="utf-8"?>' + xml_bytes
        return xml_bytes
    except Exception:
        try:
            text = html_bytes.decode("utf-8", errors="replace")
        except Exception:
            text = str(html_bytes)
        wrapped = (
            '<?xml version="1.0" encoding="utf-8"?>'
            "<html><body><![CDATA[" + text + "]]></body></html>"
        )
        return wrapped.encode("utf-8")


async def fetch_binary(session, url, **kw) -> Tuple[Optional[bytes], Optional[str]]:
    for attempt in range(5):
        try:
            headers = dict(kw.pop("headers", {}) or {})
            headers.setdefault("User-Agent", BROWSER_UA)
            headers.setdefault("Accept", "*/*")
            async with session.get(url, headers=headers, **kw) as r:
                if r.status in (401, 403, 404):
                    return None, None
                if r.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(min(8, 0.5 * (2**attempt)))
                    continue
                r.raise_for_status()
                ct = r.headers.get("Content-Type", "")
                data = await r.read()
                return data, ct
        except aiohttp.ClientResponseError:
            if attempt == 4:
                return None, None
            await asyncio.sleep(min(8, 0.5 * (2**attempt)))
        except Exception:
            if attempt == 4:
                return None, None
            await asyncio.sleep(min(8, 0.5 * (2**attempt)))
    return None, None


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
        session,
        url,
        headers={"Accept": "application/xml, text/xml;q=0.9, */*;q=0.8"},
    )


async def epmc_annotations_any(session, rl: RL, ids: List[str]) -> Dict[str, bool]:
    out: Dict[str, bool] = {}
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


async def oax_get_work(session, rl: RL, doi: str) -> Dict[str, Any]:
    if not doi:
        return {}
    await rl.tick()
    url = OPENALEX_WORK.format(doi=doi.lower())
    params = {"mailto": "contact@example.org"}
    return await fetch_json(session, url, headers=HTTP_HEADERS, params=params)


async def oax_get_venue(session, rl: RL, venue_id: str) -> Dict[str, Any]:
    if not venue_id:
        return {}
    await rl.tick()
    vid = venue_id.split("/")[-1]
    url = OPENALEX_VENUE.format(venue_id=vid)
    params = {"mailto": "contact@example.org"}
    return await fetch_json(session, url, headers=HTTP_HEADERS, params=params)


async def openalex_collect_metrics(
    session, rl: RL, dois: List[str]
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    work_map: Dict[str, Dict[str, Any]] = {}
    venue_ids: List[str] = []

    for doi in [d for d in (dois or []) if d]:
        w = await oax_get_work(session, rl, doi)
        if not isinstance(w, dict) or not w:
            continue

        cited = int(w.get("cited_by_count") or 0)
        oa = False
        oa_block = w.get("open_access") or {}
        oa_url = None
        oa_pdf_url = None
        oa_landing_url = None

        if isinstance(oa_block, dict):
            if "is_oa" in oa_block:
                oa = bool(oa_block.get("is_oa"))
            bol = oa_block.get("best_oa_location") or {}
            if isinstance(bol, dict):
                oa_pdf_url = bol.get("url_for_pdf")
                oa_landing_url = bol.get("url_for_landing_page") or bol.get(
                    "landing_page_url"
                )
                oa_url = (
                    oa_pdf_url
                    or bol.get("url")
                    or oa_landing_url
                    or oa_block.get("oa_url")
                )
        elif "is_oa" in w:
            oa = bool(w.get("is_oa"))

        host_venue = w.get("host_venue") or {}
        venue_id = host_venue.get("id")
        if venue_id:
            venue_ids.append(venue_id)

        work_map[doi] = {
            "cited_by_count": cited,
            "is_oa": oa,
            "oa_url": oa_url,
            "oa_pdf_url": oa_pdf_url,
            "oa_landing_url": oa_landing_url,
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
    if not year:
        return 0, None
    try:
        age = max(0, datetime.utcnow().year - int(year))
    except Exception:
        return 0, None
    if age <= 2:
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


def venue_influence_points(
    two_year_mean: Optional[float], h_index: Optional[int]
) -> (int, str):
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
        return pts, f"venue_2yr_mean_citedness_{x:.2f}"
    if h_index is not None:
        h = int(h_index)
        if h >= 200:
            return 20, "venue_h_index_200+"
        if h >= 100:
            return 15, "venue_h_index_100+"
        if h >= 50:
            return 10, "venue_h_index_50+"
        if h >= 20:
            return 5, "venue_h_index_20+"
        return 0, "venue_h_index_0"
    return 0, "venue_unknown"


FUNC_WORDS = (
    r"\b(mutant|variant|alanine\s?scan|site[-\s]?directed|"
    r"binding|affinity|Kd|Ki|EC50|IC50|"
    r"activity|assay|reporter|growth|viability|fitness|complementation|enzymatic)\b"
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


def compile_synonyms(syns: List[str]):
    syns = [norm(s) for s in (syns or []) if norm(s)]
    if not syns:
        return None
    parts = [re.escape(s) for s in syns]
    return re.compile(r"\b(" + "|".join(parts) + r")\b", re.IGNORECASE)


def quick_score(a: Article, syn_re, *, cited_by_count=0, venue_2yr=None, venue_h=None):
    base = 0
    reasons: List[str] = []

    if syn_re:
        txt = f"{a.title}. {a.abstract}".lower()
        if syn_re.search(txt):
            base += 30
            reasons.append("protein_synonym")

    pts, reason = novelty_points(a.year)
    base += pts
    reason and reasons.append(reason)

    pts, reason = citations_points(cited_by_count)
    base += pts
    reasons.append(reason)

    pts, reason = venue_influence_points(venue_2yr, venue_h)
    base += pts
    reasons.append(reason)

    return base, reasons


async def triage(
    in_jsonl: str,
    out_prefix: str,
    protein_syns: List[str],
    upw_email: Optional[str] = None,
    use_semantic: bool = False,
    sem_model: str = "pritamdeka/S-Biomed-Roberta-snli-multinli-stsb",
):
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    epmc_rl = RL(RPS_EPMC)
    upw_rl = RL(RPS_UNPAYWALL)
    oax_rl = RL(RPS_OPENALEX)

    TIMEOUT = aiohttp.ClientTimeout(total=60)

    syn_re = compile_synonyms(protein_syns)

    arts: List[Article] = []
    with open(in_jsonl, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
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

    async with aiohttp.ClientSession(
        timeout=TIMEOUT, trust_env=True, headers=HTTP_HEADERS
    ) as session:
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
                a, syn_re, cited_by_count=cited_by, venue_2yr=v2yr, venue_h=vh
            )
            is_rev = any(pt.lower() == "review" for pt in (a.pub_types or []))

            oa = bool(oa_known)
            if oa_known:
                reasons.append("open_access")
            oa_route = "OPENALEX" if oa_known else None

            fulltext_path = None
            pmcid = a.pmcid

            if (not is_rev) and (base_score > SCORE_MIN_FOR_FULLTEXT):
                if (not pmcid) and a.doi:
                    core = await epmc_core(session, epmc_rl, a.doi)
                    pmcid = core.get("pmcid")
                    if core.get("isOpenAccess"):
                        oa = True
                        oa_route = oa_route or "EPMC_FLAG"

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

                if not fulltext_path:
                    oa_pdf = w.get("oa_pdf_url")
                    oa_land = w.get("oa_landing_url")
                    oa_fallback = w.get("oa_url")
                    candidate = oa_pdf or oa_fallback
                    if candidate:
                        hdrs = {"Accept": "application/pdf, */*"}
                        if oa_land:
                            hdrs["Referer"] = oa_land
                        data, ctype = await fetch_binary(
                            session, candidate, headers=hdrs
                        )
                        if not data and oa_fallback and candidate != oa_fallback:
                            data, ctype = await fetch_binary(
                                session, oa_fallback, headers=hdrs
                            )
                        if data and len(data) > 1024:
                            is_pdf = (ctype and "pdf" in ctype.lower()) or (
                                ".pdf" in candidate.lower()
                            )
                            if is_pdf:
                                outp = os.path.join(
                                    out_prefix,
                                    "fulltext_oa",
                                    f"{a.pmid or (a.doi or 'noid').replace('/', '_')}."
                                    + "pdf",
                                )
                                os.makedirs(os.path.dirname(outp), exist_ok=True)
                                with open(outp, "wb") as f:
                                    f.write(data)
                                fulltext_path = outp
                                oa = True
                                oa_route = oa_route or "OPENALEX_URL"
                                reasons.append("OA_fulltext_OpenAlexURL")
                                fetched += 1
                                logger.info(
                                    "[%s] saved OA PDF via OpenAlex → %s",
                                    a.pmid,
                                    fulltext_path,
                                )
                            else:
                                xml_bytes = html_to_xml(data)
                                outp = os.path.join(
                                    out_prefix,
                                    "fulltext_xml",
                                    f"{a.pmid or (a.doi or 'noid').replace('/', '_')}."
                                    + "xml",
                                )
                                os.makedirs(os.path.dirname(outp), exist_ok=True)
                                with open(outp, "wb") as f:
                                    f.write(xml_bytes)
                                fulltext_path = outp
                                oa = True
                                oa_route = oa_route or "OPENALEX_URL_HTML2XML"
                                reasons.append("OA_fulltext_OpenAlexURL_HTML2XML")
                                fetched += 1
                                logger.info(
                                    "[%s] saved OA HTML→XML via OpenAlex → %s",
                                    a.pmid,
                                    fulltext_path,
                                )

                if (not fulltext_path) and a.doi and upw_email:
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
                        loc = j.get("best_oa_location") or {}
                        upw_url = (
                            loc.get("url")
                            or loc.get("url_for_pdf")
                            or loc.get("url_for_landing_page")
                        )
                        if upw_url:
                            data, ctype = await fetch_binary(
                                session, upw_url, headers={"Accept": "*/*"}
                            )
                            if data and len(data) > 1024:
                                is_pdf = (".pdf" in (upw_url or "").lower()) or (
                                    ctype and "pdf" in ctype.lower()
                                )
                                if is_pdf:
                                    outp = os.path.join(
                                        out_prefix,
                                        "fulltext_oa",
                                        f"{
                                            a.pmid
                                            or (a.doi or 'noid').replace('/', '_')
                                        }.pdf",
                                    )
                                    os.makedirs(os.path.dirname(outp), exist_ok=True)
                                    with open(outp, "wb") as f:
                                        f.write(data)
                                    fulltext_path = outp
                                    reasons.append("OA_fulltext_Unpaywall")
                                    fetched += 1
                                    logger.info(
                                        "[%s] saved OA PDF via Unpaywall → %s",
                                        a.pmid,
                                        fulltext_path,
                                    )
                                else:
                                    xml_bytes = html_to_xml(data)
                                    placeholder = a.pmid or (a.doi or "noid").replace(
                                        "/", "_"
                                    )
                                    outp = os.path.join(
                                        out_prefix,
                                        "fulltext_xml",
                                        f"{placeholder}.xml",  # type: ignore
                                    )
                                    os.makedirs(os.path.dirname(outp), exist_ok=True)
                                    with open(outp, "wb") as f:
                                        f.write(xml_bytes)
                                    fulltext_path = outp
                                    reasons.append("OA_fulltext_Unpaywall_HTML2XML")
                                    fetched += 1
                                    logger.info(
                                        "[%s] saved OA HTML→XML via Unpaywall → %s",
                                        a.pmid,
                                        fulltext_path,
                                    )
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
                    has_epmc_ann=ann_map.get(f"MED:{a.pmid}", False)
                    or (
                        a.pmcid
                        and ann_map.get("PMC:" + a.pmcid.replace("PMC", ""), False)
                    ),
                    is_review=is_rev,
                )
            )

        logger.info("Triage done: %d items; fulltexts saved: %d", len(results), fetched)

        os.makedirs(out_prefix, exist_ok=True)
        with open(os.path.join(out_prefix, "triage.jsonl"), "w", encoding="utf-8") as f:
            for r in results:
                f.write(json.dumps(asdict(r), ensure_ascii=False) + "\n")

        with open(
            os.path.join(out_prefix, "keep.pmids.txt"), "w", encoding="utf-8"
        ) as f:
            for r in results:
                if (not r.is_review) and r.score > SCORE_MIN_FOR_FULLTEXT:
                    f.write(r.pmid + "\n")


def main():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--in-jsonl", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--protein", required=True)
    p.add_argument("--syn", action="append")
    p.add_argument("--unpaywall-email")
    p.add_argument("--use-semantic", action="store_true")
    p.add_argument(
        "--sem-model", default="pritamdeka/S-Biomed-Roberta-snli-multinli-stsb"
    )
    args = p.parse_args()

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
