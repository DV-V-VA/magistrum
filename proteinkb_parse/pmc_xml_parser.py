#!/usr/bin/env python3
import os, json, re, xml.etree.ElementTree as ET, logging
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Tuple
from .schema import normalize_variant, normalize_header_name, parse_value_with_error, KB_RECORD_JSON_SCHEMA, ASSAY_TYPE_BY_ENDPOINT

logger = logging.getLogger("proteinkb.pmc_parser")

def localname(tag: str) -> str:
    if tag is None:
        return ""
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag

def iter_by_local(root: ET.Element, lname: str):
    for el in root.iter():
        if localname(el.tag) == lname:
            yield el

def first_desc_by_local(root: ET.Element, lname: str):
    for el in iter_by_local(root, lname):
        return el
    return None


def text_recursive(el: ET.Element) -> str:
    parts: List[str] = []
    def rec(e):
        if e.text: parts.append(e.text)
        for ch in list(e):
            rec(ch)
            if ch.tail: parts.append(ch.tail)
    rec(el)
    return re.sub(r"\s+", " ", "".join(parts)).strip()

def get_header_cells(tbl: ET.Element) -> List[str]:
    thead = first_desc_by_local(tbl, "thead")
    if thead is not None:
        rows = [r for r in thead.iter() if localname(r.tag) == "tr"]
        if rows:
            last = rows[-1]
            cells = [c for c in last if localname(c.tag) in ("th","td")]
            if cells:
                return [text_recursive(c) for c in cells]
    tb = first_desc_by_local(tbl, "tbody")
    if tb is not None:
        first_tr = next((r for r in tb if localname(r.tag)=="tr"), None)
    else:
        first_tr = next((r for r in tbl if localname(r.tag)=="tr"), None)
    if first_tr is None:
        return []
    cells = [c for c in first_tr if localname(c.tag) in ("th","td")]
    return [text_recursive(c) for c in cells]

def iter_body_rows(tbl: ET.Element) -> List[List[str]]:
    rows: List[List[str]] = []
    tb = first_desc_by_local(tbl, "tbody")
    if tb is not None:
        for tr in (ch for ch in tb if localname(ch.tag)=="tr"):
            tds = [td for td in tr if localname(td.tag) in ("td","th")]
            rows.append([text_recursive(td) for td in tds])
        return rows
    trs = [r for r in tbl if localname(r.tag)=="tr"]
    if not trs:
        trs = [r for r in tbl.iter() if localname(r.tag)=="tr"]
    if not trs:
        return rows
    for tr in trs[1:]:
        tds = [td for td in tr if localname(td.tag) in ("td","th")]
        rows.append([text_recursive(td) for td in tds])
    return rows

def parse_units_from_header(h: str) -> Optional[str]:
    m = re.search(r"\(([^)]+)\)", h)
    if m:
        return m.group(1).strip()
    m = re.search(r",\s*([^\s,]+)$", h)
    if m:
        return m.group(1).strip()
    return None

WT_WORDS = {"wt", "wild type", "wild-type", "wildtype"}

def is_wt(s: str) -> bool:
    s2 = s.strip().lower().replace("—","-").replace("–","-")
    return s2 in WT_WORDS

@dataclass
class TableRecord:
    source_pmid: str
    source_pmcid: Optional[str]
    source_doi: Optional[str]
    file: Optional[str]
    table_id: Optional[str]
    caption: Optional[str]
    headers_raw: List[str]
    row: Dict[str, Any]

def map_headers(headers: List[str]) -> Tuple[List[str], Dict[int, str], Dict[str, Optional[str]]]:
    clean = [h.strip() for h in headers]
    idx2canon: Dict[int, str] = {}
    canon_units: Dict[str, Optional[str]] = {}
    for i, h in enumerate(clean):
        canon = normalize_header_name(h)
        idx2canon[i] = canon
        if canon not in canon_units:
            canon_units[canon] = parse_units_from_header(h)
    return clean, idx2canon, canon_units

def extract_from_table(tbl_wrap: ET.Element, pmid: str, pmcid: Optional[str], doi: Optional[str], file_path: str,
                       protein_name: str, protein_syns: List[str]) -> Tuple[List[Dict[str, Any]], Optional[TableRecord]]:
    tbl = tbl_wrap
    if localname(tbl_wrap.tag) != "table":
        t = first_desc_by_local(tbl_wrap, "table")
        if t is not None:
            tbl = t
    headers = get_header_cells(tbl)
    if not headers or len(headers) < 2:
        return [], None
    headers_clean, idx2canon, canon_units = map_headers(headers)
    rows = iter_body_rows(tbl)
    if not rows:
        return [], None

    cap_el = None
    if localname(tbl_wrap.tag) != "table":
        cap_el = first_desc_by_local(tbl_wrap, "caption")
    if cap_el is None:
        cap_el = first_desc_by_local(tbl, "caption")
    caption = text_recursive(cap_el) if cap_el is not None else None

    variant_cols = [i for i,c in idx2canon.items() if c == "Variant"]
    metric_cols = [ (i,c) for i,c in idx2canon.items() if c in ASSAY_TYPE_BY_ENDPOINT ]

    if not metric_cols:
        cap_l = (caption or "").lower()
        if any(k in cap_l for k in ["kinetic", "kcat", "km"]):
            for i in range(max(0,len(headers_clean)-2), len(headers_clean)):
                ch = normalize_header_name(headers_clean[i])
                if ch in ASSAY_TYPE_BY_ENDPOINT:
                    metric_cols.append((i,ch))

    if not metric_cols:
        return [], TableRecord(pmid, pmcid, doi, file_path, tbl_wrap.attrib.get("id"), caption, headers_clean, {})

    out: List[Dict[str, Any]] = []
    wt_vals: Dict[str, float] = {}
    for r_idx, cells in enumerate(rows):
        if not cells:
            continue
        if len(cells) < len(headers_clean):
            cells = cells + [""] * (len(headers_clean)-len(cells))

        var_text_candidates = []
        for vi in variant_cols or [0]:
            if vi < len(cells):
                var_text_candidates.append(cells[vi])
        variant_text = next((v for v in var_text_candidates if v.strip()), "")
        if not variant_text and len(cells)>0:
            variant_text = cells[0]

        if is_wt(variant_text):
            for i, canon in metric_cols:
                if i < len(cells):
                    val, err, qual = parse_value_with_error(cells[i])
                    if val is not None and qual is None:
                        wt_vals[canon] = val
            continue

        var_norm = normalize_variant(variant_text)
        for i, canon in metric_cols:
            if i >= len(cells):
                continue
            raw_cell = cells[i]
            val, err, qual = parse_value_with_error(raw_cell)
            unit = canon_units.get(canon) or canon_units.get("Units")
            record = {
                "protein": {"query_name": protein_name, "synonyms": protein_syns, "uniprot": None},
                "source": {
                    "pmid": pmid, "pmcid": pmcid, "doi": doi,
                    "table_id": tbl_wrap.attrib.get("id"),
                    "caption": caption,
                    "file": file_path,
                },
                "variant": var_norm,
                "assay": {
                    "type": ASSAY_TYPE_BY_ENDPOINT.get(canon),
                    "endpoint": canon,
                    "value": val,
                    "unit": unit,
                    "error": err,
                    "n": None,
                    "conditions": {},
                },
                "comparators": [],
                "derived": {}
            }
            if canon in wt_vals and val is not None and wt_vals[canon] != 0:
                fold = val / wt_vals[canon]
                record["comparators"].append({"name": "WT", "value": wt_vals[canon]})
                record["derived"]["fold_change_vs_wt"] = fold
                record["derived"]["direction"] = ("increase" if fold>1.05 else "decrease" if fold<0.95 else "no_change")
            out.append(record)
    meta = TableRecord(pmid, pmcid, doi, file_path, tbl_wrap.attrib.get("id"), caption, headers_clean, {})
    return out, meta

def parse_pmc_xml(xml_path: str, pmid: str, pmcid: Optional[str], doi: Optional[str],
                  protein_name: str, protein_syns: List[str]) -> List[Dict[str, Any]]:
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except ET.ParseError as e:
        logger.warning("Failed to parse XML %s: %s", xml_path, e)
        return []
    out: List[Dict[str, Any]] = []
    any_tables = False
    for tw in iter_by_local(root, "table-wrap"):
        any_tables = True
        records, _meta = extract_from_table(tw, pmid, pmcid, doi, xml_path, protein_name, protein_syns)
        out.extend(records)
    if not any_tables:
        for t in iter_by_local(root, "table"):
            records, _meta = extract_from_table(t, pmid, pmcid, doi, xml_path, protein_name, protein_syns)
            out.extend(records)
    logger.info("Parsed %s → %d records", os.path.basename(xml_path), len(out))
    return out

def parse_from_triage(triage_jsonl: str, out_jsonl: str, protein_name: str, protein_syns: List[str]) -> int:
    os.makedirs(os.path.dirname(out_jsonl), exist_ok=True)
    total = 0
    kept = 0
    with open(out_jsonl, "w", encoding="utf-8") as fout:
        with open(triage_jsonl, "r", encoding="utf-8") as f:
            for line in f:
                total += 1
                j = json.loads(line)
                if not j.get("fulltext_path"):
                    continue
                xml_path = j["fulltext_path"]
                pmid = j.get("pmid")
                pmcid = j.get("pmcid")
                doi = j.get("doi")
                logger.debug("Parsing %s (PMID %s, PMCID %s)", xml_path, pmid, pmcid)
                recs = parse_pmc_xml(xml_path, pmid, pmcid, doi, protein_name, protein_syns)
                for r in recs:
                    fout.write(json.dumps(r, ensure_ascii=False) + "\n")
                    kept += 1
    logger.info("Triaged records: %d lines read; %d extracted", total, kept)
    return kept

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Parse PMC fullTextXML tables → records JSONL")
    ap.add_argument("--triage_jsonl", required=True, help="Path to triage.jsonl")
    ap.add_argument("--out_jsonl", default="./out/kb.records.jsonl")
    ap.add_argument("--protein", required=True)
    ap.add_argument("--syn", nargs="*", default=[])
    args = ap.parse_args()
    n = parse_from_triage(args.triage_jsonl, args.out_jsonl, args.protein, args.syn or [])
    print(f"Extracted records: {n}")

if __name__ == "__main__":
    main()
