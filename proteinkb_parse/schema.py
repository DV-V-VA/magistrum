from typing import Optional, Dict, Any, Tuple
import re

AA1_TO_AA3 = {
    "A": "Ala", "R": "Arg", "N": "Asn", "D": "Asp", "C": "Cys",
    "Q": "Gln", "E": "Glu", "G": "Gly", "H": "His", "I": "Ile",
    "L": "Leu", "K": "Lys", "M": "Met", "F": "Phe", "P": "Pro",
    "S": "Ser", "T": "Thr", "W": "Trp", "Y": "Tyr", "V": "Val",
    "U": "Sec", "O": "Pyl" 
}
AA3_CAP = {k: v[0].upper() + v[1:].lower() for k, v in AA1_TO_AA3.items()}
AA3_SET = set(AA3_CAP.values())

RE_HGVS_3 = re.compile(r"p\.\(?([A-Z][a-z]{2})(\d+)([A-Z][a-z]{2})\)?")
RE_1L = re.compile(r"\b([A-Z])(\d+)([A-Z])\b(?![a-z])")
RE_3L = re.compile(r"\b([A-Z][a-z]{2})(\d+)([A-Z][a-z]{2})\b")

def norm_aa3(s: str) -> Optional[str]:
    s2 = s[0].upper() + s[1:].lower()
    return s2 if s2 in AA3_SET else None

def normalize_variant(raw: str) -> Dict[str, Any]:

    raw = (raw or "").strip()
    out = {"raw": raw, "normalized_hgvs_p": None, "type": None, "position": None, "ref_aa": None, "alt_aa": None}
    if not raw:
        return out

    m = RE_HGVS_3.search(raw)
    if m:
        ref3, pos, alt3 = m.group(1), m.group(2), m.group(3)
        ref3 = norm_aa3(ref3) or ref3
        alt3 = norm_aa3(alt3) or alt3
        out.update({
            "normalized_hgvs_p": f"p.{ref3}{pos}{alt3}",
            "type": "missense",
            "position": int(pos),
            "ref_aa": ref3,
            "alt_aa": alt3,
        })
        return out

    m = RE_1L.search(raw)
    if m:
        ref1, pos, alt1 = m.group(1), m.group(2), m.group(3)
        ref3 = AA1_TO_AA3.get(ref1)
        alt3 = AA1_TO_AA3.get(alt1)
        if ref3 and alt3:
            out.update({
                "normalized_hgvs_p": f"p.{ref3}{pos}{alt3}",
                "type": "missense",
                "position": int(pos),
                "ref_aa": ref3,
                "alt_aa": alt3,
            })
            return out

    m = RE_3L.search(raw)
    if m:
        ref3, pos, alt3 = m.group(1), m.group(2), m.group(3)
        ref3 = norm_aa3(ref3) or ref3
        alt3 = norm_aa3(alt3) or alt3
        out.update({
            "normalized_hgvs_p": f"p.{ref3}{pos}{alt3}",
            "type": "missense",
            "position": int(pos),
            "ref_aa": ref3,
            "alt_aa": alt3,
        })
        return out

    if "del" in raw.lower() or "Δ" in raw:
        out["type"] = "deletion"
    elif "ins" in raw.lower() or "dup" in raw.lower():
        out["type"] = "insertion/duplication"
    else:
        out["type"] = "other"

    return out

ENDPOINT_MAP = {
    "kcat": {"kcat", "turnover", "turnover number"},
    "Km": {"km", "k m", "k_m"},
    "kcat/Km": {"kcat/km", "catalytic efficiency", "specificity constant"},
    "IC50": {"ic50", "inhibitory concentration 50", "inhibition 50"},
    "EC50": {"ec50"},
    "KD": {"kd", "k_d", "dissociation constant"},
    "Tm": {"tm", "t m", "melting temperature"},
    "ΔΔG": {"ddg", "ΔΔg", "delta delta g", "delta g"},
    "Activity": {"activity", "relative activity", "% activity", "percent activity", "enzyme activity"},
    "Binding": {"binding", "binding signal"},
    "Fluorescence": {"fluorescence", "rfu", "a.u."},
    "Growth": {"growth", "fitness", "od600", "od 600"},
}

ASSAY_TYPE_BY_ENDPOINT = {
    "kcat": "enzyme_kinetics",
    "Km": "enzyme_kinetics",
    "kcat/Km": "enzyme_kinetics",
    "IC50": "inhibition",
    "EC50": "activation",
    "KD": "binding",
    "Tm": "stability",
    "ΔΔG": "stability",
    "Activity": "activity",
    "Binding": "binding",
    "Fluorescence": "reporter",
    "Growth": "growth",
}

def normalize_header_name(h: str) -> str:
    h0 = (h or "").strip()
    h1 = re.sub(r"\s+", " ", h0).strip().lower()
    h2 = re.sub(r"\(.*?\)", "", h1).strip()
    for canon, syns in ENDPOINT_MAP.items():
        if h2 in syns or any(h2 == s for s in syns):
            return canon
        if any(s in h2 for s in syns):
            return canon
    if any(x in h2 for x in ["mutation", "mutant", "variant", "substitution", "construct", "change", "aa change", "amino acid"]):
        return "Variant"
    if h2 in {"units", "unit"}:
        return "Units"
    if "ph" == h2:
        return "pH"
    if any(x in h2 for x in ["temperature", "temp", "°c", "deg c", "celsius"]):
        return "Temperature"
    return h0

def parse_value_with_error(text: str) -> Tuple[Optional[float], Optional[Dict[str, Any]], Optional[str]]:
    if text is None:
        return None, None, None
    t = text.strip()
    if not t:
        return None, None, None
    t = t.replace(",", ".")
    qual = None
    if t.startswith("<"):
        qual = "<"
        t = t[1:].strip()
    elif t.startswith(">"):
        qual = ">"
        t = t[1:].strip()
    import re
    pm = re.match(r"^([+-]?\d+(?:\.\d+)?)(?:\s*(?:±|\+/-)\s*([+-]?\d+(?:\.\d+)?))?(?:\s*\((\d+(?:\.\d+)?)\))?", t)
    if pm:
        val = float(pm.group(1))
        err = None
        if pm.group(2):
            err = {"type": "unspecified", "value": float(pm.group(2))}
        elif pm.group(3):
            err = {"type": "unspecified", "value": float(pm.group(3))}
        return val, err, qual
    try:
        return float(t), None, qual
    except Exception:
        return None, None, None

KB_RECORD_JSON_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Protein modification → functional outcome",
    "type": "object",
    "required": ["source", "variant", "assay"],
    "properties": {
        "protein": {
            "type": "object",
            "properties": {
                "query_name": {"type": "string"},
                "synonyms": {"type": "array", "items": {"type": "string"}},
                "uniprot": {"type": ["string", "null"]}
            },
            "additionalProperties": True
        },
        "source": {
            "type": "object",
            "required": ["pmid"],
            "properties": {
                "pmid": {"type": "string"},
                "pmcid": {"type": ["string", "null"]},
                "doi": {"type": ["string", "null"]},
                "table_id": {"type": ["string", "null"]},
                "caption": {"type": ["string", "null"]},
                "file": {"type": ["string", "null"]}
            }
        },
        "variant": {
            "type": "object",
            "required": ["raw"],
            "properties": {
                "raw": {"type": "string"},
                "normalized_hgvs_p": {"type": ["string", "null"]},
                "type": {"type": ["string", "null"]},
                "position": {"type": ["integer", "null"]},
                "ref_aa": {"type": ["string", "null"]},
                "alt_aa": {"type": ["string", "null"]}
            }
        },
        "assay": {
            "type": "object",
            "required": ["endpoint", "value"],
            "properties": {
                "type": {"type": ["string", "null"]},
                "endpoint": {"type": "string"},
                "value": {"type": ["number", "null"]},
                "unit": {"type": ["string", "null"]},
                "error": {
                    "type": ["object", "null"],
                    "properties": {
                        "type": {"type": ["string", "null"]},
                        "value": {"type": ["number", "null"]}
                    }
                },
                "n": {"type": ["integer", "null"]},
                "conditions": {"type": "object"}
            }
        },
        "comparators": {
            "type": "array",
            "items": {"type": "object"}
        },
        "derived": {
            "type": "object",
            "properties": {
                "fold_change_vs_wt": {"type": ["number", "null"]},
                "direction": {"type": ["string", "null"]}
            }
        }
    }
}

def infer_direction(fold: Optional[float]) -> Optional[str]:
    if fold is None:
        return None
    if fold > 1.05:
        return "increase"
    if fold < 0.95:
        return "decrease"
    return "no_change"
