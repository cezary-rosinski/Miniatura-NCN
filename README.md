# Miniatura NCN – Analysis of trends in Polish literary studies using digital methods

This repository documents the research activity funded by the National Science Centre, Poland, under grant number **2025/09/X/HS2/00585**. The project investigates how Polish literary-studies journals are represented in citation infrastructures and tests a reproducible route from journal metadata to OpenCitations-compatible citation data.

The work has two connected parts:

1. a comparison of article and citation coverage in OpenCitations and Scopus for a list of 82 journals;
2. a pilot workflow that compares reference extraction from PDFs with structured references exposed by a publishing platform through OAI-PMH.

## Main findings

- The union of the OpenCitations and Scopus datasets contains 21,017 matched or source-specific article records: 5,237 occur in both sources, 3,184 only in OpenCitations, and 12,596 only in Scopus. The overlap is 24.9% of the union.
- A stratified, reproducible sample of 50 English-language *Forum of Poetics* PDFs produced usable CEC/GROBID reference structures for 32 files. Extraction succeeded for 2 of 17 sampled texts from 2015–2018 and for 30 of 33 texts from later periods.
- The metadata-first route processed 339 article records and 7,727 reference instances from OAI-PMH. It generated 6,162 OpenCitations metadata rows and 6,483 citation links.
- OpenCitations closure validation finished with 0 metadata errors and 0 citation errors. Six non-blocking warnings concerned titles written in all capitals.

The pilot supports a metadata-first approach: structured reference data should be captured and exposed by publishing systems whenever possible. PDF extraction is useful as a fallback, but its performance depends on document layout and historical template changes.

## Repository structure

### Workflow 1 – OpenCitations and Scopus comparison

| Script | Role |
|---|---|
| `1.1. miniatura_opencitations_polish_literary_studies.py` | Resolve journal identifiers and collect OpenCitations metadata and citation data. |
| `1.2. miniatura_opencitations_polish_literary_studies_citation_distribution.py` | Calculate journal-level citation distributions and concentration measures. |
| `1.3. miniatura_opencitations_polish_literary_studies_citation_network_analysis.py` | Build article-, author-, and venue-level citation networks. |
| `1.4. miniatura_scopus_harvesting.py` | Collect Scopus records for the journal list. |
| `1.5. miniatura_scopus_polish_literary_studies_citation_distribution.py` | Calculate the corresponding Scopus citation distributions. |
| `1.6. miniatura_opencitations_scopus_coverage_comparison.py` | Match article records across OpenCitations and Scopus. |
| `1.7. miniatura_opencitations_scopus_comparative_analysis.py` | Compare coverage, citation counts, distributions, and journal profiles. |
| `1.8. miniatura_opencitations_polish_literary_studies_vizualizations.py` | Prepare selected static and interactive visualisations. |

### Workflow 2 – OAI-PMH/PDF to OpenCitations

| Script | Role |
|---|---|
| `2.1. miniatura_literary_journals_oai-pmh_data_harvesting.py` | Harvest journal metadata and prepare local source files and PDF manifests. |
| `2.2 miniatura_opencitations_pipeline_cec_forum_poetyki` | Run the reproducible PDF pilot and the full OAI-PMH → AnyStyle → Crossref → OpenCitations pipeline for "Forum Poetyki". |
| `2.3 miniatura_opencitations_pipeline_cec_zagadnienia` | Run the reproducible PDF pilot and the full OAI-PMH → AnyStyle → Crossref → OpenCitations pipeline for "Zagadnienia Rodzajów Literackich". |
| `2.4 miniatura_opencitations_pipeline_cec_teksty_drugie` | Run the reproducible PDF pilot and the full OAI-PMH → AnyStyle → Crossref → OpenCitations pipeline for "Teksty Drugie". |
| `miniatura_parsing_anystyle.py` | AnyStyle helpers and mapping to OpenCitations fields. |
| `miniatura_parsing_from_pdf.py` | PDF-job construction and TEI reference parsing helpers. |

The `data/` directory is intentionally excluded from Git. Source data, API exports, local PDFs, caches, and generated outputs must be staged locally.

## Requirements

- Python 3.12;
- Ruby and the [AnyStyle CLI](https://github.com/inukshuk/anystyle);
- Docker and the [OpenCitations Citation Extraction Service](https://github.com/opencitations/cec);
- [`oc_validator`](https://github.com/opencitations/oc_validator) with the `closure` command;
- a Scopus API key for the Scopus harvesting workflow;
- a real contact address in `CROSSREF_MAILTO` for Crossref REST API requests.

Core Python packages include `pandas`, `requests`, `tqdm`, `python-Levenshtein`, `rapidfuzz`, `scipy`, `networkx`, `plotly`, `matplotlib`, `adjustText`, `sickle`, `beautifulsoup4`, and `lxml`.

## Inputs for the OAI-PMH/PDF workflow

Place the following files under `data/`:

```text
data/
├── forum_poetyki_harvesting_info.xlsx
├── Forum_Poetyki_harvested.pkl
└── forum_poetyki_pdfs/
    └── en/
        └── <publisher_id>_<year>_<issue>_en.pdf
```

The spreadsheet and pickle contain the OAI-PMH harvest and local file manifest. Paths in the manifest are resolved relative to the repository root.

## Running the validated pipeline

Start the CEC Docker Compose stack from a local checkout of the CEC repository. Then configure the external commands in PowerShell:

```powershell
$env:ANYSTYLE_CMD = "C:\Ruby34-x64\bin\anystyle.bat"
$env:CROSSREF_MAILTO = "name@institution.example"
$env:OC_VALIDATOR_CMD = "C:\path\to\oc_validator.exe"
```

Run the 50-document PDF pilot:

```powershell
python .\miniatura_opencitations_pipeline_cec.py --stage pdf --sample-size 50
```

Use `--refresh-cec` only when every selected PDF should be processed again instead of using cached TEI files.

Run the full OAI-PMH route:

```powershell
python .\miniatura_opencitations_pipeline_cec.py --stage oai
```

Validate existing exports without repeating earlier stages:

```powershell
python .\miniatura_opencitations_pipeline_cec.py --stage validate
```

`--skip-crossref` is available for diagnostics, but the final dataset should normally use the cached or completed Crossref enrichment stage.

## Outputs

```text
data/final_pipeline_output/
├── 01_pdf_extraction_evaluation/
│   ├── pdf_sample_manifest.csv
│   ├── pdf_sample_allocation.csv
│   ├── pdf_vs_oai_summary.csv
│   ├── pdf_extracted_references.csv
│   └── pdf_extraction_failures.csv
├── 02_oai_to_opencitations/
│   ├── oai_references_parsed_anystyle.csv
│   ├── oai_references_crossref_enriched.csv
│   ├── deduplication_mapping.csv
│   ├── example_metadata.csv
│   ├── example_citations.csv
│   ├── oc_export_exclusions.csv
│   └── validation/
├── cache/
└── run_manifest.json
```

`run_manifest.json` records input hashes, the Python version, sample size, random seed, service configuration, output counts, and completion status. The PDF sample is deterministic and stratified by publication period and OAI-PMH reference-list length.

## Interpretation and limitations

- The OpenCitations–Scopus comparison measures differences between the selected source snapshots and the implemented record-linkage procedure. It is not a complete census of Polish literary studies.
- The PDF pilot covers English-language files from one journal and should not be generalised to other publishers or platforms without further testing.
- `reference_count_ratio` compares counts only. It is not a precision, recall, or F1 score; those measures require reference-level matching.
- Crossref enrichment assigned DOI candidates automatically. The complete set of assignments has not undergone full manual validation, so scores and diagnostics should be retained when the data are reused.
- Raw Scopus exports and full-text PDFs must not be redistributed through the public data deposit. Publish only components whose redistribution is permitted by their source terms.

## Data availability

The reproducible code, documentation, redistributable derived data, OpenCitations exports, and current validation report are intended for deposition in Zenodo.

**Zenodo DOI:** `[https://doi.org/10.5281/zenodo.21809680](https://doi.org/10.5281/zenodo.21809680)`

## Licensing

- **Data:** `[TO BE ADDED after verifying the rights of each deposited component; CC0 or CC BY 4.0 is preferred where applicable]`
- **Code:** `[TO BE ADDED]`

Use separate licences for code and data. Do not include credentials, local user paths, full-text PDFs, or restricted source exports in a public release.

## Funding

This work was funded by the National Science Centre, Poland, under grant number **2025/09/X/HS2/00585**.

## Creator

[Cezary Rosiński](https://orcid.org/0000-0002-6136-7186), Institute of Literary Research of the Polish Academy of Sciences.
