import csv
import json
import pickle
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Any
from tqdm import tqdm
import pandas as pd
#%%
# =========================================================
# CONFIG
# =========================================================

# 1) AnyStyle CLI
ANYSTYLE_CMD = r"C:\Ruby34-x64\bin\anystyle.bat"

# 2) Input files
EXCEL_PATH = Path(r"data\forum_poetyki_harvesting_info.xlsx")
PICKLE_PATH = Path(r"data\Forum_Poetyki_harvested.pkl")

# 3) Folder with TEI files produced earlier from GROBID
#    Example: C:\Users\pracownik\Documents\Miniatura-NCN\grobid_tei_en
TEI_DIR = Path(r"C:data\forum_poetyki_pdf_grobid_pipeline_output\tei")

# 4) Output
OUTPUT_DIR = Path(r"data\anystyle_comparison_output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 5) Which language / source variants to use
#    "pl", "en", or "both" for GROBID-derived TEI inventory matching
LANG_MODE = "en"

# 6) Limit for quick test; set to None for full run
LIMIT_ARTICLES = 20
#%%
# =========================================================
# HELPERS
# =========================================================

NS = {"tei": "http://www.tei-c.org/ns/1.0"}


def normalize_text(text: Any) -> str:
    return " ".join(str(text or "").split()).strip()


def safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


def load_manifest(excel_path: Path) -> pd.DataFrame:
    df = pd.read_excel(excel_path)
    df = df.copy()

    for col in ["identifier", "publisher_id", "doi", "article_title"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


def load_pickle_records(pickle_path: Path) -> List[Dict]:
    with open(pickle_path, "rb") as f:
        data = pickle.load(f)
    return data


def build_pickle_df(records: List[Dict]) -> pd.DataFrame:
    rows = []
    for rec in records:
        rows.append({
            "identifier": normalize_text(rec.get("identifier")),
            "publisher_id": normalize_text(rec.get("publisher_id")),
            "doi": normalize_text(rec.get("doi")),
            "article_title": normalize_text(rec.get("article_title")),
            "journal_title": normalize_text(rec.get("journal_title")),
            "year": normalize_text(rec.get("year")),
            "issue": normalize_text(rec.get("issue")),
            "fpage": normalize_text(rec.get("fpage")),
            "lpage": normalize_text(rec.get("lpage")),
            "authors": rec.get("authors", []),
            "references": rec.get("references", []),
        })
    return pd.DataFrame(rows)


def merge_sources(df_manifest: pd.DataFrame, df_pickle: pd.DataFrame) -> pd.DataFrame:
    df = df_manifest.merge(
        df_pickle,
        on=["identifier", "publisher_id", "doi", "article_title"],
        how="left",
        suffixes=("", "_pkl")
    )
    return df


def author_list_to_string(authors: List[Dict]) -> str:
    out = []
    for a in authors or []:
        surname = normalize_text(a.get("surname"))
        given = normalize_text(a.get("given_names"))
        full_name = normalize_text(a.get("full_name"))
        if full_name:
            out.append(full_name)
        elif surname and given:
            out.append(f"{given} {surname}")
        elif surname:
            out.append(surname)
    return "; ".join(out)


def parse_with_anystyle_single_reference(ref_text: str, anystyle_cmd: str = ANYSTYLE_CMD) -> Dict:
    ref_text = normalize_text(ref_text)
    if not ref_text:
        return {}

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        input_path = tmpdir / "reference.txt"
        input_path.write_text(ref_text, encoding="utf-8")

        result = subprocess.run(
            [anystyle_cmd, "--stdout", "-f", "json", "parse", str(input_path)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace"
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"AnyStyle error {result.returncode}\nSTDERR:\n{result.stderr}\nSTDOUT:\n{result.stdout}"
            )

        stdout = result.stdout.strip()
        if not stdout:
            return {}

        data = json.loads(stdout)
        if not data:
            return {}

        return data[0]


def first_value(x):
    if isinstance(x, list):
        return normalize_text(x[0]) if x else ""
    return normalize_text(x)


def map_anystyle_to_oc(parsed: Dict) -> Dict:
    authors = []
    for a in parsed.get("author", []):
        if isinstance(a, dict):
            family = normalize_text(a.get("family"))
            given = normalize_text(a.get("given"))
            name = ", ".join([part for part in [family, given] if part])
            if name:
                authors.append(name)
        else:
            txt = normalize_text(a)
            if txt:
                authors.append(txt)

    return {
        "title": first_value(parsed.get("title")),
        "author": "; ".join(authors),
        "pub_date": first_value(parsed.get("date")),
        "venue": first_value(parsed.get("container-title")),
        "volume": first_value(parsed.get("volume")),
        "issue": first_value(parsed.get("issue")),
        "page": first_value(parsed.get("pages")),
        "type": normalize_text(parsed.get("type")),
    }


def completeness_flags(mapped: Dict) -> Dict:
    return {
        "has_author": int(bool(mapped.get("author"))),
        "has_title": int(bool(mapped.get("title"))),
        "has_pub_date": int(bool(mapped.get("pub_date"))),
        "has_venue": int(bool(mapped.get("venue"))),
        "has_volume": int(bool(mapped.get("volume"))),
        "has_issue": int(bool(mapped.get("issue"))),
        "has_page": int(bool(mapped.get("page"))),
    }


def tei_candidates_for_row(row: pd.Series, tei_dir: Path, lang_mode: str = "en") -> List[Path]:
    candidates = []

    lang_modes = [lang_mode] if lang_mode in {"pl", "en"} else ["pl", "en"]

    for lang in lang_modes:
        # prefer publisher_id
        pub_id = normalize_text(row.get("publisher_id"))
        identifier = normalize_text(row.get("identifier"))

        possible_names = []
        if pub_id:
            possible_names.extend([
                f"{pub_id}_{lang}.tei.xml",
                f"{pub_id}.tei.xml",
                f"{pub_id}_{lang}.xml",
                f"{pub_id}.xml",
            ])

        if identifier:
            safe_id = identifier.replace(":", "_").replace("/", "_")
            possible_names.extend([
                f"{safe_id}_{lang}.tei.xml",
                f"{safe_id}.tei.xml",
                f"{safe_id}_{lang}.xml",
                f"{safe_id}.xml",
            ])

        for name in possible_names:
            p = tei_dir / name
            if p.exists():
                candidates.append(p)

    # fallback: search by publisher_id stem
    pub_id = normalize_text(row.get("publisher_id"))
    if pub_id:
        globbed = list(tei_dir.glob(f"*{pub_id}*.xml"))
        for p in globbed:
            if p not in candidates:
                candidates.append(p)

    return candidates


def extract_raw_references_from_tei(tei_path: Path) -> List[str]:
    xml_text = tei_path.read_text(encoding="utf-8", errors="replace")
    root = ET.fromstring(xml_text.encode("utf-8"))

    refs = []

    # preferred route
    for note in root.findall(".//tei:note[@type='raw_reference']", NS):
        ref = normalize_text("".join(note.itertext()))
        if ref:
            refs.append(ref)

    # fallback
    if not refs:
        for bibl in root.findall(".//tei:listBibl/tei:biblStruct", NS):
            ref = normalize_text("".join(bibl.itertext()))
            if ref:
                refs.append(ref)

    return refs


def extract_gold_references_from_pickle_row(row: pd.Series) -> List[str]:
    refs = row.get("references", [])
    out = []

    if not isinstance(refs, list):
        return out

    for item in refs:
        if isinstance(item, dict):
            citation = normalize_text(item.get("citation"))
            if citation:
                out.append(citation)
        else:
            citation = normalize_text(item)
            if citation:
                out.append(citation)

    return out


def build_article_metadata(row: pd.Series) -> Dict:
    return {
        "identifier": normalize_text(row.get("identifier")),
        "publisher_id": normalize_text(row.get("publisher_id")),
        "doi": normalize_text(row.get("doi")),
        "article_title": normalize_text(row.get("article_title")),
        "journal_title": normalize_text(row.get("journal_title")),
        "year": normalize_text(row.get("year")),
        "issue": normalize_text(row.get("issue")),
        "fpage": normalize_text(row.get("fpage")),
        "lpage": normalize_text(row.get("lpage")),
        "authors": author_list_to_string(row.get("authors", [])),
    }


def parse_reference_batch(refs: List[str], source_type: str, article_meta: Dict) -> List[Dict]:
    rows = []

    for idx, ref in enumerate(refs, start=1):
        try:
            parsed = parse_with_anystyle_single_reference(ref)
            mapped = map_anystyle_to_oc(parsed)
            flags = completeness_flags(mapped)

            out = {
                **article_meta,
                "source_type": source_type,
                "reference_index": idx,
                "raw_reference": ref,
                "parsed_json": safe_json_dumps(parsed),
                **mapped,
                **flags,
            }

        except Exception as e:
            out = {
                **article_meta,
                "source_type": source_type,
                "reference_index": idx,
                "raw_reference": ref,
                "parsed_json": "",
                "title": "",
                "author": "",
                "pub_date": "",
                "venue": "",
                "volume": "",
                "issue": "",
                "page": "",
                "type": "",
                "has_author": 0,
                "has_title": 0,
                "has_pub_date": 0,
                "has_venue": 0,
                "has_volume": 0,
                "has_issue": 0,
                "has_page": 0,
                "error": str(e),
            }
            rows.append(out)
            continue

        out["error"] = ""
        rows.append(out)

    return rows


def summarize_article(article_meta: Dict, grobid_refs: List[str], gold_refs: List[str], grobid_parsed_rows: List[Dict], gold_parsed_rows: List[Dict], tei_path: str) -> Dict:
    def avg_flag(rows: List[Dict], field: str) -> float:
        if not rows:
            return 0.0
        return sum(r.get(field, 0) for r in rows) / len(rows)

    return {
        **article_meta,
        "tei_path": tei_path,
        "grobid_raw_reference_count": len(grobid_refs),
        "gold_reference_count": len(gold_refs),
        "grobid_has_title_rate": avg_flag(grobid_parsed_rows, "has_title"),
        "grobid_has_author_rate": avg_flag(grobid_parsed_rows, "has_author"),
        "grobid_has_date_rate": avg_flag(grobid_parsed_rows, "has_pub_date"),
        "grobid_has_venue_rate": avg_flag(grobid_parsed_rows, "has_venue"),
        "gold_has_title_rate": avg_flag(gold_parsed_rows, "has_title"),
        "gold_has_author_rate": avg_flag(gold_parsed_rows, "has_author"),
        "gold_has_date_rate": avg_flag(gold_parsed_rows, "has_pub_date"),
        "gold_has_venue_rate": avg_flag(gold_parsed_rows, "has_venue"),
    }

#%%
def main():
    df_manifest = load_manifest(EXCEL_PATH)
    records = load_pickle_records(PICKLE_PATH)
    df_pickle = build_pickle_df(records)
    df = merge_sources(df_manifest, df_pickle)

    if LIMIT_ARTICLES is not None:
        df = df.head(LIMIT_ARTICLES).copy()

    all_rows = []
    summary_rows = []

    for _, row in tqdm(df.iterrows(), total = len(df)):
        article_meta = build_article_metadata(row)

        tei_paths = tei_candidates_for_row(row, TEI_DIR, LANG_MODE)
        tei_path = tei_paths[0] if tei_paths else None

        grobid_refs = []
        if tei_path is not None:
            try:
                grobid_refs = extract_raw_references_from_tei(tei_path)
            except Exception as e:
                print(f"[WARN] TEI parsing failed for {article_meta['identifier']}: {e}")

        gold_refs = extract_gold_references_from_pickle_row(row)

        grobid_parsed_rows = parse_reference_batch(grobid_refs, "grobid_raw_reference", article_meta)
        gold_parsed_rows = parse_reference_batch(gold_refs, "gold_pickle_reference", article_meta)

        all_rows.extend(grobid_parsed_rows)
        all_rows.extend(gold_parsed_rows)

        summary_rows.append(
            summarize_article(
                article_meta=article_meta,
                grobid_refs=grobid_refs,
                gold_refs=gold_refs,
                grobid_parsed_rows=grobid_parsed_rows,
                gold_parsed_rows=gold_parsed_rows,
                tei_path=str(tei_path) if tei_path else "",
            )
        )

        print(
            f"{article_meta['publisher_id']} | "
            f"grobid={len(grobid_refs)} | gold={len(gold_refs)}"
        )

    df_out = pd.DataFrame(all_rows)
    df_summary = pd.DataFrame(summary_rows)

    out_csv = OUTPUT_DIR / "anystyle_parsed_references_comparison.csv"
    out_jsonl = OUTPUT_DIR / "anystyle_parsed_references_comparison.jsonl"
    out_summary = OUTPUT_DIR / "anystyle_article_summary.csv"

    df_out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    df_summary.to_csv(out_summary, index=False, encoding="utf-8-sig")

    with open(out_jsonl, "w", encoding="utf-8") as f:
        for row in all_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print("\nDONE")
    print(f"Rows: {len(df_out)}")
    print(f"Summary rows: {len(df_summary)}")
    print(f"Saved: {out_csv}")
    print(f"Saved: {out_jsonl}")
    print(f"Saved: {out_summary}")


if __name__ == "__main__":
    main()






















