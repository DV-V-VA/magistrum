#!/usr/bin/env python3
import argparse, asyncio, sys, logging
from . import harvest_pubmed as harvest
from . import triage_fulltext as triage
from . import pmc_xml_parser as pmc

def cmd_harvest(args):
    return asyncio.run(harvest.run(
        protein=args.protein,
        synonyms=args.syn,
        out_prefix=args.out,
        max_results=args.max_results,
        field=args.field,
        ncbi_api_key=args.ncbi_api_key,
        datetype=args.datetype,
    ))

def cmd_triage(args):
    syns = list({args.protein} | set(args.syn or []))
    return asyncio.run(triage.triage(
        in_jsonl=args.in_jsonl,
        out_prefix=args.out,
        protein_syns=syns,
        upw_email=args.unpaywall_email,
        use_semantic=args.use_semantic,
        sem_model=args.sem_model
    ))

def cmd_parse(args):
    syns = args.syn or []
    n = pmc.parse_from_triage(args.triage_jsonl, args.out_jsonl, args.protein, syns)
    print(f"Extracted records: {n}")

def cmd_build_kb(args):
    # 1) harvest
    asyncio.run(harvest.run(
        protein=args.protein,
        synonyms=args.syn,
        out_prefix=args.out_prefix + "/articles",
        max_results=args.max_results,
        field=args.field,
        ncbi_api_key=args.ncbi_api_key,
        datetype=args.datetype,
    ))
    # 2) triage
    in_jsonl = args.out_prefix + "/articles.pubmed.jsonl"
    triage_out = args.out_prefix + "/triage"
    syns = list({args.protein} | set(args.syn or []))
    asyncio.run(triage.triage(
        in_jsonl=in_jsonl,
        out_prefix=triage_out,
        protein_syns=syns,
        upw_email=args.unpaywall_email,
        use_semantic=args.use_semantic,
        sem_model=args.sem_model
    ))
    # 3) parse pmc
    pmc_out = args.out_prefix + "/kb.records.jsonl"
    pmc.parse_from_triage(triage_out + "/triage.jsonl", pmc_out, args.protein, args.syn or [])
    print("KB is ready:", pmc_out)

def main():
    ap = argparse.ArgumentParser(description="proteinkb: end-to-end pipeline to build 'modification → outcome' KB")
    ap.add_argument("-v", "--verbose", action="store_true", help="Verbose logs")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # harvest
    ap_h = sub.add_parser("harvest", help="Collect PubMed records")
    ap_h.add_argument("--protein", required=True)
    ap_h.add_argument("--syn", nargs="*")
    ap_h.add_argument("--out", default="./out/articles")
    ap_h.add_argument("--max_results", type=int, default=None)
    ap_h.add_argument("--ncbi_api_key", default=None)
    ap_h.add_argument("--field", default="tiab")
    ap_h.add_argument("--datetype", default="pdat", choices=["pdat","edat"])
    ap_h.set_defaults(func=cmd_harvest)

    # triage
    ap_t = sub.add_parser("triage", help="Sort & fulltext fetch")
    ap_t.add_argument("--in_jsonl", required=True)
    ap_t.add_argument("--out", default="./out/triage")
    ap_t.add_argument("--protein", required=True)
    ap_t.add_argument("--syn", nargs="*", default=[])
    ap_t.add_argument("--unpaywall_email", default=None)
    ap_t.add_argument("--use_semantic", action="store_true")
    ap_t.add_argument("--sem_model", default="pritamdeka/S-Biomed-Roberta-snli-multinli-stsb")
    ap_t.set_defaults(func=cmd_triage)

    # parse
    ap_p = sub.add_parser("parse-pmc", help="Parse PMC XML to records")
    ap_p.add_argument("--triage_jsonl", required=True)
    ap_p.add_argument("--out_jsonl", default="./out/kb.records.jsonl")
    ap_p.add_argument("--protein", required=True)
    ap_p.add_argument("--syn", nargs="*", default=[])
    ap_p.set_defaults(func=cmd_parse)

    # All steps
    ap_b = sub.add_parser("build-kb", help="Run all steps: harvest → triage → parse")
    ap_b.add_argument("--protein", required=True)
    ap_b.add_argument("--syn", nargs="*", default=[])
    ap_b.add_argument("--out_prefix", default="./out")
    ap_b.add_argument("--max_results", type=int, default=None)
    ap_b.add_argument("--ncbi_api_key", default=None)
    ap_b.add_argument("--field", default="tiab")
    ap_b.add_argument("--datetype", default="pdat", choices=["pdat","edat"])
    ap_b.add_argument("--unpaywall_email", default=None)
    ap_b.add_argument("--use_semantic", action="store_true")
    ap_b.add_argument("--sem_model", default="pritamdeka/S-Biomed-Roberta-snli-multinli-stsb")
    ap_b.set_defaults(func=cmd_build_kb)

    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("aiohttp.client").setLevel(logging.WARNING)
    args.func(args)

if __name__ == "__main__":
    main()
