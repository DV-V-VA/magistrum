# proteinkb parse
**"Дан человеческий белок X → собрать из литературы и смежных источников знания о связи последовательность → функция"**

## Компоненты
1. `harvest`: выгрузка PubMed (PMID→метаданные), расширено полями `PMCID`, `PublicationTypes`, `MeSH`.
2. `triage`: сортировка и фуллтекст (Europe PMC XML / Unpaywall) + признаки релевантности.
3. `parse-pmc`: парсер XML таблиц → JSONL записей **«модификация → исход»**.

## Установка
```bash
pip install -r proteinkb_parse/requirements.txt
```

## Старт
```bash
# 1) PubMed
python -m proteinkb_parse.main harvest --protein NRF2 --syn NFE2L2 Nrf2 --out ./out/articles -v

# 2) Ранжирование + фуллтексты
python -m proteinkb_parse.main triage --in_jsonl ./out/articles.pubmed.jsonl \
  --out ./out/triage --protein NRF2 --syn NFE2L2 Nrf2 \
  --unpaywall_email you@your.org

python -m proteinkb_parse.main parse-pmc --triage_jsonl ./out/triage/triage.jsonl \
  --out_jsonl ./out/kb.records.jsonl --protein NRF2 --syn NFE2L2 Nrf2

# Или всё сразу
python -m proteinkb_parse.main build-kb --protein NRF2 --syn NFE2L2 Nrf2 --out_prefix ./out
```

Файл `./out/kb.records.jsonl`: по одной записи на комбинацию {вариант, конечная точка}.
Схема в `proteinkb_parse/schema.py` (`KB_RECORD_JSON_SCHEMA`). Пример:
```json
{
  "protein": {"query_name": "NRF2", "synonyms": ["NFE2L2", "Nrf2"], "uniprot": null},
  "source": {"pmid": "12345678", "pmcid": "PMC1234567", "doi": "10.1000/j.jmb.2020.01.001",
             "table_id": "T1", "caption": "Kinetic parameters of mutants", "file": "out/triage/fulltext_xml/PMC1234567.xml"},
  "variant": {"raw": "K76N", "normalized_hgvs_p": "p.Lys76Asn", "type": "missense", "position": 76,
              "ref_aa": "Lys", "alt_aa": "Asn"},
  "assay": {"type": "enzyme_kinetics", "endpoint": "kcat", "value": 1.23, "unit": "s^-1",
            "error": {"type": "unspecified", "value": 0.10}, "n": null, "conditions": {}},
  "comparators": [{"name": "WT", "value": 2.10}],
  "derived": {"fold_change_vs_wt": 0.586, "direction": "decrease"}
}
```

## Парсер таблиц
- Находит все `<table-wrap>`/`<table>`, определяет заголовки (`<thead>` или первая строка).
- Нормализует заголовки к канону (`kcat`, `Km`, `kcat/Km`, `IC50`, `KD`, `Tm`, `ΔΔG`, `Activity`, ...).
- Ищет колонку варианта (`Variant`), иначе берёт первую колонку.
- Понимает форматы значений: `12.3 ± 0.4`, `12.3(+/−0.4)`, `<0.5` и пр.; тащит единицы из заголовка `(μM)`.
- Распознаёт варианты `p.Lys76Asn`, `K76N`, `Lys76Asn`, частично — del/ins/dup.
- Строки `WT/wild-type` используются как базовые для расчёта `fold_change_vs_wt`.

Ограничения: в сомнительных случаях парсер отдаёт "как есть" (`assay.value = null` или без единиц)