
#!/usr/bin/env python3
import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, asdict
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Sequence

import aiohttp
import xml.etree.ElementTree as ET

DOI_RE = re.compile(r"\b10\.\d{4,9}/[^\s\"';,)<>]+", re.IGNORECASE)

PUBMED_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_EFETCH_URL  = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 5
BACKOFF_BASE = 0.5
BACKOFF_CAP = 8.0
EFETCH_BATCH = 200

# Limits: no key ~3 rps, with key ~10 rps
NCBI_RPS_NO_KEY = 3
NCBI_RPS_WITH_KEY = 10

PUBMED_MAX_RETRIEVABLE = 9999
ESEARCH_PAGE_SIZE = 1000

HEADERS_JSON = {"Accept": "application/json", "Accept-Encoding": "identity"}

DEFAULT_SYNONYMS = {
    "NRF2": ["NRF2", "NFE2L2", "Nrf2"],
    "SOX2": ["SOX2", "Sox2"],
    "APOE": ["APOE", "ApoE", "Apolipoprotein E"],
    "OCT4": ["OCT4", "POU5F1", "Oct4"],
}

class RateLimiter:
    def __init__(self, rate: float):
        self.rate = max(0.1, rate)
        self._tokens = self.rate
        self._last = None
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            loop = asyncio.get_event_loop()
            now = loop.time()
            if self._last is None:
                self._last = now
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self.rate, self._tokens + elapsed * self.rate)
            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self.rate
                await asyncio.sleep(wait)
                now2 = loop.time()
                elapsed2 = now2 - self._last
                self._last = now2
                self._tokens = min(self.rate, self._tokens + elapsed2 * self.rate)
            self._tokens -= 1.0


class Http:
    def __init__(self, ncbi_rps: float, timeout: int = REQUEST_TIMEOUT_SECONDS):
        self.session: Optional[aiohttp.ClientSession] = None
        self.rl_ncbi = RateLimiter(ncbi_rps)
        self.timeout = aiohttp.ClientTimeout(total=timeout, connect=10, sock_read=timeout)

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=80, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(
            connector=connector, timeout=self.timeout, trust_env=True, raise_for_status=False
        )
        return self

    async def __aexit__(self, *exc):
        if self.session:
            await self.session.close()

    async def _sleep_backoff(self, attempt: int, retry_after: Optional[str]) -> None:
        if retry_after:
            try:
                sec = float(retry_after)
                await asyncio.sleep(min(sec, BACKOFF_CAP))
                return
            except Exception:
                pass
        delay = min(BACKOFF_BASE * (2 ** attempt), BACKOFF_CAP)
        await asyncio.sleep(delay * (0.5 + 0.5 * (os.urandom(1)[0] / 255.0)))

    async def get(self, url: str, *, params: Optional[Dict[str, Any]] = None,
                  headers: Optional[Dict[str, str]] = None,
                  is_ncbi: bool = False, max_retries: int = MAX_RETRIES) -> bytes:
        assert self.session is not None
        attempt = 0
        while True:
            try:
                if is_ncbi:
                    await self.rl_ncbi.acquire()
                async with self.session.get(url, params=params, headers=headers) as resp:
                    if resp.status in (429, 500, 502, 503, 504):
                        if attempt >= max_retries:
                            text = await resp.text()
                            raise aiohttp.ClientResponseError(resp.request_info, resp.history,
                                                              status=resp.status, message=text)
                        await self._sleep_backoff(attempt, resp.headers.get("Retry-After"))
                        attempt += 1
                        continue
                    resp.raise_for_status()
                    return await resp.read()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt >= max_retries:
                    raise
                await self._sleep_backoff(attempt, None)
                attempt += 1

@dataclass
class PubMedArticle:
    pmid: str
    title: str
    abstract: str
    journal: Optional[str]
    year: Optional[int]
    doi: Optional[str]
    authors: List[str]
    url: str
    pmcid: Optional[str] = None
    pub_types: List[str] = None
    mesh: List[str] = None


def ensure_dir(path_prefix: str) -> None:
    d = os.path.dirname(path_prefix)
    if d:
        os.makedirs(d, exist_ok=True)

def build_pubmed_term(gene_synonyms: Sequence[str], field: str = "tiab") -> str:
    syn = " OR ".join([f"{s}[{field}]" if " " not in s else f"\"{s}\"[{field}]"
                       for s in gene_synonyms])
    term = f"({syn})"
    return term

def parse_year_from_pubdate(art_elem: ET.Element) -> Optional[int]:
    y = art_elem.findtext("./MedlineCitation/Article/Journal/JournalIssue/PubDate/Year") or ""
    if y.isdigit():
        return int(y)
    medline = art_elem.findtext("./MedlineCitation/Article/Journal/JournalIssue/PubDate/MedlineDate") or ""
    m = re.search(r"(\d{4})", medline)
    return int(m.group(1)) if m else None

def parse_authors(art_elem: ET.Element) -> List[str]:
    out: List[str] = []
    for a in art_elem.findall("./MedlineCitation/Article/AuthorList/Author"):
        last = a.findtext("LastName") or ""
        init = a.findtext("Initials") or ""
        coll = a.findtext("CollectiveName")
        if coll:
            out.append(coll.strip())
        elif last:
            out.append((last + (" " + init if init else "")).strip())
    return out

# def parse_ids(art_elem: ET.Element):
#     doi = None; pmcid = None
#     for aid in art_elem.findall(".//ArticleIdList/ArticleId"):
#         idt = (aid.attrib.get("IdType") or "").lower()
#         val = (aid.text or "").strip()
#         if idt == "doi":
#             doi = val
#         elif idt == "pmc":
#             pmcid = val if val.startswith("PMC") else f"PMC{val}"
#     return doi, pmcid

def _clean_doi(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    t = text.strip()
    m = DOI_RE.search(t)
    if m:
        t = m.group(0)
    t = t.strip(" .;,)")
    return t or None

def parse_ids(art_elem: ET.Element):
    doi = None; pmcid = None
    el = art_elem.find("./MedlineCitation/Article/ELocationID[@EIdType='doi']")
    if el is not None:
        doi = _clean_doi(el.text)
    if not doi:
        el2 = art_elem.find("./PubmedData/ArticleIdList/ArticleId[@IdType='doi']")
        if el2 is not None:
            doi = _clean_doi(el2.text)
    el3 = art_elem.find("./PubmedData/ArticleIdList/ArticleId[@IdType='pmc']")
    if el3 is not None:
        val = (el3.text or "").strip()
        pmcid = val if val.upper().startswith("PMC") else f"PMC{val}"
    return doi, pmcid

def parse_pubtypes(art_elem: ET.Element) -> List[str]:
    return [
        (pt.text or "").strip()
        for pt in art_elem.findall("./MedlineCitation/Article/PublicationTypeList/PublicationType")
        if (pt.text or "").strip()
    ]

def parse_mesh(art_elem: ET.Element) -> List[str]:
    out = []
    for mh in art_elem.findall("./MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName"):
        t = (mh.text or "").strip()
        if t:
            out.append(t)
    return out

def parse_efetch_xml(xml_bytes: bytes) -> List[PubMedArticle]:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        logging.warning("EFETCH XML parse error; skipping batch.")
        return []
    out: List[PubMedArticle] = []
    for art in root.findall(".//PubmedArticle"):
        try:
            pmid = (art.findtext("./MedlineCitation/PMID") or "").strip()
            title = (art.findtext("./MedlineCitation/Article/ArticleTitle") or "").strip()
            abstract_parts: List[str] = []
            for ab in art.findall("./MedlineCitation/Article/Abstract/AbstractText"):
                seg = (ab.text or "").strip()
                label = ab.attrib.get("Label") or ab.attrib.get("NlmCategory")
                if label and seg:
                    abstract_parts.append(f"{label}: {seg}")
                elif label:
                    abstract_parts.append(label)
                elif seg:
                    abstract_parts.append(seg)
            abstract = " ".join(abstract_parts)
            journal = (art.findtext("./MedlineCitation/Article/Journal/Title") or "").strip() or None
            year = parse_year_from_pubdate(art)
            doi, pmcid = parse_ids(art)
            pub_types = parse_pubtypes(art)
            mesh = parse_mesh(art)
            authors = parse_authors(art)
            url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else ""
            out.append(PubMedArticle(pmid, title, abstract, journal, year, doi, authors, url, pmcid, pub_types, mesh))
        except Exception as ex:
            logging.warning(f"Failed to parse one PubMed article: {ex}")
    return out

def _parse_json_bytes(data: bytes) -> Dict[str, Any]:
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        text = data.decode("utf-8", "replace")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            fixed = text.replace("\\'", "'")
            return json.loads(fixed)

def _check_esearch_error(esr: Dict[str, Any], ctx: str) -> None:
    err = esr.get("ERROR")
    if err:
        raise RuntimeError(f"NCBI ESearch error ({ctx}): {err}")

from datetime import date, timedelta

def fmt(d: date) -> str:
    return d.strftime("%Y/%m/%d")

def first_day_of_year(d: date) -> date:
    return date(d.year, 1, 1)

def first_day_of_month(d: date) -> date:
    return date(d.year, d.month, 1)

async def pubmed_count(http, term: str, *, api_key: Optional[str],
                       datetype: str = "pdat",
                       mindate: Optional[str] = None,
                       maxdate: Optional[str] = None) -> int:
    params = {"db": "pubmed", "term": term, "retmode": "json", "rettype": "count", "datetype": datetype}
    if mindate and maxdate:
        params["mindate"] = mindate; params["maxdate"] = maxdate
    if api_key: params["api_key"] = api_key
    data = await http.get(PUBMED_ESEARCH_URL, params=params, headers=HEADERS_JSON, is_ncbi=True)
    j = _parse_json_bytes(data)
    esr = j.get("esearchresult", {}) or {}
    _check_esearch_error(esr, f"count {mindate or ''}..{maxdate or ''}")
    cnt = esr.get("count", 0)
    return int(cnt)

async def pubmed_esearch_window(http, term: str, *, api_key: Optional[str],
                                mindate: str, maxdate: str, cap: int,
                                datetype: str = "pdat", sort: str = "pub_date") -> List[str]:
    ids: List[str] = []
    params = {"db":"pubmed","term":term,"retmode":"json","retstart":0,"retmax":min(1000,cap),
              "sort":sort,"datetype":datetype,"mindate":mindate,"maxdate":maxdate}
    if api_key: params["api_key"] = api_key
    while len(ids) < cap and params["retstart"] <= 9998:
        data = await http.get(PUBMED_ESEARCH_URL, params=params, headers=HEADERS_JSON, is_ncbi=True)
        j = _parse_json_bytes(data)
        esr = j.get("esearchresult", {}) or {}
        _check_esearch_error(esr, f"window {mindate}..{maxdate}")
        page = esr.get("idlist", []) or []
        if not page: break
        ids.extend(page)
        params["retstart"] += len(page)
        params["retmax"] = min(1000, cap - len(ids))
        if params["retmax"] <= 0: break
    logging.info("Window %s..%s: got %s", mindate, maxdate, len(ids))
    return ids

async def pubmed_esearch_basic(http, term: str, *, api_key: Optional[str],
                               cap: int, sort: str = "pub_date") -> List[str]:
    ids: List[str] = []
    params = {"db":"pubmed","term":term,"retmode":"json","retstart":0,"retmax":min(1000,cap),"sort":sort}
    if api_key: params["api_key"] = api_key
    while len(ids) < cap and params["retstart"] <= 9998:
        data = await http.get(PUBMED_ESEARCH_URL, params=params, headers=HEADERS_JSON, is_ncbi=True)
        j = _parse_json_bytes(data)
        esr = j.get("esearchresult", {}) or {}
        _check_esearch_error(esr, "basic")
        page = esr.get("idlist", []) or []
        if not page: break
        ids.extend(page)
        params["retstart"] += len(page)
        params["retmax"] = min(1000, cap - len(ids))
        if params["retmax"] <= 0: break
    logging.info("Basic: got %s", len(ids))
    return ids

async def collect_latest_by_date(http, term: str, need_n: int, *,
                                 api_key: Optional[str],
                                 datetype: str = "pdat") -> List[str]:
    all_ids: List[str] = []
    end = date.today()
    total_count = await pubmed_count(http, term, api_key=api_key, datetype=datetype)
    cap_goal = min(need_n, total_count)
    logging.info("Total: %s; target: %s", total_count, cap_goal)
    if cap_goal <= 0: return []

    while len(all_ids) < cap_goal:
        remaining = cap_goal - len(all_ids)
        window_cap = min(PUBMED_MAX_RETRIEVABLE, remaining)

        y_start = first_day_of_year(end)
        y_count = await pubmed_count(http, term, api_key=api_key, datetype=datetype,
                                     mindate=fmt(y_start), maxdate=fmt(end))
        if y_count == 0:
            end = y_start - timedelta(days=1); continue
        if y_count <= PUBMED_MAX_RETRIEVABLE:
            got = await pubmed_esearch_window(http, term, api_key=api_key,
                                              mindate=fmt(y_start), maxdate=fmt(end),
                                              cap=min(window_cap, y_count),
                                              datetype=datetype, sort="pub_date")
            all_ids.extend(got); end = y_start - timedelta(days=1); continue

        m_start = first_day_of_month(end)
        m_count = await pubmed_count(http, term, api_key=api_key, datetype=datetype,
                                     mindate=fmt(m_start), maxdate=fmt(end))
        if m_count == 0:
            end = m_start - timedelta(days=1); continue
        if m_count <= PUBMED_MAX_RETRIEVABLE:
            got = await pubmed_esearch_window(http, term, api_key=api_key,
                                              mindate=fmt(m_start), maxdate=fmt(end),
                                              cap=min(window_cap, m_count),
                                              datetype=datetype, sort="pub_date")
            all_ids.extend(got); end = m_start - timedelta(days=1); continue

        w_start = end - timedelta(days=6)
        w_count = await pubmed_count(http, term, api_key=api_key, datetype=datetype,
                                     mindate=fmt(w_start), maxdate=fmt(end))
        if w_count == 0:
            end = w_start - timedelta(days=1); continue
        if w_count <= PUBMED_MAX_RETRIEVABLE:
            got = await pubmed_esearch_window(http, term, api_key=api_key,
                                              mindate=fmt(w_start), maxdate=fmt(end),
                                              cap=min(window_cap, w_count),
                                              datetype=datetype, sort="pub_date")
            all_ids.extend(got); end = w_start - timedelta(days=1); continue

        d_count = await pubmed_count(http, term, api_key=api_key, datetype=datetype,
                                     mindate=fmt(end), maxdate=fmt(end))
        if d_count == 0:
            end = end - timedelta(days=1); continue
        if d_count > PUBMED_MAX_RETRIEVABLE:
            raise RuntimeError(f"Too many results for a single day {fmt(end)}: {d_count} > 9999")
        got = await pubmed_esearch_window(http, term, api_key=api_key,
                                          mindate=fmt(end), maxdate=fmt(end),
                                          cap=min(window_cap, d_count),
                                          datetype=datetype, sort="pub_date")
        all_ids.extend(got); end = end - timedelta(days=1)
    logging.info("Collected: %s (requested %s, available %s)", len(all_ids), cap_goal, total_count)
    return all_ids

async def pubmed_efetch(http, pmids: Sequence[str], api_key: Optional[str]) -> List[PubMedArticle]:
    order = {pmid: i for i, pmid in enumerate(pmids)}
    out: List[PubMedArticle] = []
    for i in range(0, len(pmids), EFETCH_BATCH):
        batch = pmids[i:i+EFETCH_BATCH]
        params = {"db":"pubmed","id":",".join(batch),"retmode":"xml"}
        if api_key: params["api_key"] = api_key
        xml_bytes = await http.get(PUBMED_EFETCH_URL, params=params, is_ncbi=True)
        out.extend(parse_efetch_xml(xml_bytes))
    out.sort(key=lambda a: order.get(a.pmid, 10**12))
    logging.info("eFetch: parsed %s articles", len(out))
    return out

def save_jsonl(objs, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for o in objs:
            f.write(json.dumps(asdict(o), ensure_ascii=False) + "\n")

def save_pmids(pubmed, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for a in pubmed:
            if a.pmid:
                f.write(a.pmid + "\n")

def save_pubmed_csv(pubmed, path: str) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["PMID", "Title", "Journal", "Year", "DOI", "PMCID", "PublicationTypes", "MeSH", "Authors", "URL", "Abstract"])
        for a in pubmed:
            w.writerow([a.pmid, a.title, a.journal or "", a.year or "", a.doi or "",
                        a.pmcid or "", ";".join(a.pub_types or []), ";".join(a.mesh or []),
                        "; ".join(a.authors), a.url, a.abstract])

async def run(protein: str, synonyms, out_prefix: str, max_results: Optional[int],
              field: str, ncbi_api_key: Optional[str], datetype: str = "pdat") -> None:
    gene_syns = synonyms or DEFAULT_SYNONYMS.get(protein.upper(), [protein])
    term = build_pubmed_term(gene_syns, field=field)
    d = os.path.dirname(out_prefix)
    if d: os.makedirs(d, exist_ok=True)
    ncbi_rps = NCBI_RPS_WITH_KEY if ncbi_api_key else NCBI_RPS_NO_KEY
    need_n = (10**12 if max_results is None else max(0, int(max_results)))
    logging.info("Start: term=%r, need_n=%s, datetype=%s", term, need_n, datetype)
    async with Http(ncbi_rps=ncbi_rps) as http:
        if need_n <= PUBMED_MAX_RETRIEVABLE:
            pmids = await pubmed_esearch_basic(http, term, api_key=ncbi_api_key, cap=need_n, sort="pub_date")
        else:
            pmids = await collect_latest_by_date(http, term, need_n, api_key=ncbi_api_key, datetype=datetype)
        if not pmids:
            logging.info("Empty result."); return
        articles = await pubmed_efetch(http, pmids, api_key=ncbi_api_key)
        save_pubmed_csv(articles, f"{out_prefix}.pubmed.csv")
        save_jsonl(articles, f"{out_prefix}.pubmed.jsonl")
        save_pmids(articles, f"{out_prefix}.pubmed.pmids.txt")
        logging.info("Saved %s articles", len(articles))

def main():
    ap = argparse.ArgumentParser(description="PubMed harvest (extended)")
    ap.add_argument("--protein", required=True, help="Target protein/gene")
    ap.add_argument("--syn", nargs="*", help="Synonyms")
    ap.add_argument("--out", default="./out/articles", help="Output prefix")
    ap.add_argument("--max_results", type=int, default=None, help="How many latest to collect; default: all")
    ap.add_argument("--ncbi_api_key", default=os.getenv("NCBI_API_KEY"), help="NCBI API key")
    ap.add_argument("-v", "--verbose", action="store_true", help="Verbose logs")
    ap.add_argument("--field", default="tiab", help="PubMed field for synonyms (usually 'tiab')")
    ap.add_argument("--datetype", default="pdat", choices=["pdat", "edat"], help="Date type")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(run(args.protein, args.syn, args.out, args.max_results, args.field, args.ncbi_api_key, args.datetype))
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr); sys.exit(1)

if __name__ == "__main__":
    main()
