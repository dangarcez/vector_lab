from __future__ import annotations

import argparse
import csv
import re
import unicodedata
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from xml.sax.saxutils import escape


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_LOGS_DIR = PROJECT_ROOT / "logs"
DEFAULT_OUTPUT_FILE = PROJECT_ROOT / "analise_logs.xlsx"
ROTATED_SUFFIX_RE = re.compile(r"\.\d+$")
# INCLUDE_LOG_FILES_REGEX = r".*"  # Regex aplicada em path.name (ex.: r"^SPPVBRK.*\.txt$")
INCLUDE_LOG_FILES_REGEX = r".*.txt"  # Regex aplicada em path.name (ex.: r"^SPPVBRK.*\.txt$")

INCLUDE_LOG_FILES_RE = re.compile(INCLUDE_LOG_FILES_REGEX)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera um arquivo Excel (.xlsx) com analises basicas dos logs."
    )
    parser.add_argument(
        "--logs-dir",
        default=str(DEFAULT_LOGS_DIR),
        help="Diretorio dos logs (padrao: ./logs).",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=str(DEFAULT_OUTPUT_FILE),
        help="Arquivo .xlsx de saida (padrao: ./analise_logs.xlsx).",
    )
    return parser.parse_args()


def list_base_log_files(logs_dir: Path) -> list[Path]:
    if not logs_dir.exists():
        raise FileNotFoundError(f"Diretorio de logs nao encontrado: {logs_dir}")

    files: list[Path] = []
    for path in sorted(logs_dir.iterdir()):
        if not path.is_file():
            continue
        if ROTATED_SUFFIX_RE.search(path.name):
            # Ignora arquivos rotacionados, como arquivo.txt.1, arquivo.txt.2, ...
            continue
        if path.suffix.lower() != ".txt":
            continue
        if not INCLUDE_LOG_FILES_RE.search(path.name):
            continue
        files.append(path)
    return files


def read_header_from_log(log_file: Path) -> list[str]:
    with log_file.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)

    if not header:
        raise ValueError(f"Arquivo de log vazio ou sem cabecalho: {log_file}")

    normalized = [value.strip() for value in header]
    if normalized:
        normalized[0] = normalized[0].lstrip("\ufeff")
    return normalized


def collect_headers(log_files: Iterable[Path]) -> dict[str, list[str]]:
    headers_by_file: dict[str, list[str]] = {}
    for log_file in log_files:
        headers_by_file[log_file.name] = read_header_from_log(log_file)
    return headers_by_file


def build_headers_per_file_sheet(headers_by_file: dict[str, list[str]]) -> list[list[object]]:
    max_headers = max((len(headers) for headers in headers_by_file.values()), default=0)
    rows: list[list[object]] = [["indice"] + [f"header_{i}" for i in range(1, max_headers + 1)]]

    for file_name in sorted(headers_by_file):
        headers = headers_by_file[file_name]
        row = [file_name] + headers + [""] * (max_headers - len(headers))
        rows.append(row)
    return rows


def build_header_frequency_sheet(headers_by_file: dict[str, list[str]]) -> list[list[object]]:
    total_files = len(headers_by_file)
    appearances = Counter[str]()
    example_file_by_header: dict[str, str] = {}

    for file_name in sorted(headers_by_file):
        headers = headers_by_file[file_name]
        for header in set(headers):
            appearances[header] += 1
            example_file_by_header.setdefault(header, file_name)

    ordered = sorted(appearances.items(), key=lambda item: (-item[1], item[0].lower(), item[0]))
    rows: list[list[object]] = [["header", "aparicoes", "total_arquivos", "arquivo exemplo", "resumo"]]

    for header, count in ordered:
        rows.append(
            [
                header,
                count,
                total_files,
                example_file_by_header.get(header, ""),
                f"{header};{count}/{total_files}",
            ]
        )
    return rows


def build_summary_sheet(headers_by_file: dict[str, list[str]]) -> list[list[object]]:
    unique_headers = {header for headers in headers_by_file.values() for header in headers}
    rows: list[list[object]] = [
        ["metrica", "valor"],
        ["quantidade_de_arquivos_analisados", len(headers_by_file)],
        ["quantidade_de_headers_unicos", len(unique_headers)],
    ]
    return rows


def excel_column_name(index_zero_based: int) -> str:
    index = index_zero_based + 1
    name = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def sanitize_sheet_name(name: str) -> str:
    sanitized = re.sub(r'[\[\]:*?/\\]', "_", name).strip()
    if not sanitized:
        sanitized = "Sheet"
    return sanitized[:31]


def visual_length(value: object) -> int:
    text = "" if value is None else str(value)
    length = 0
    for char in text:
        length += 2 if unicodedata.east_asian_width(char) in {"W", "F"} else 1
    return length


def estimate_excel_width(max_content_length: int) -> float:
    # Aproximacao pratica para evitar colunas "escondendo" conteudo.
    return min(255.0, max(10.0, (max_content_length * 1.15) + 2.0))


def make_inline_string_cell(cell_ref: str, value: str, style_index: int | None = None) -> str:
    escaped = escape(value)
    style_attr = f' s="{style_index}"' if style_index is not None else ""
    return (
        f'<c r="{cell_ref}" t="inlineStr"{style_attr}>'
        f'<is><t xml:space="preserve">{escaped}</t></is>'
        f"</c>"
    )


def make_numeric_cell(cell_ref: str, value: int | float, style_index: int | None = None) -> str:
    style_attr = f' s="{style_index}"' if style_index is not None else ""
    return f'<c r="{cell_ref}"{style_attr}><v>{value}</v></c>'


def worksheet_xml(rows: list[list[object]]) -> str:
    row_xml_parts: list[str] = []
    max_cols = max((len(r) for r in rows), default=0)
    column_lengths = [0] * max_cols

    for row in rows:
        for col_idx in range(max_cols):
            value = row[col_idx] if col_idx < len(row) else ""
            column_lengths[col_idx] = max(column_lengths[col_idx], visual_length(value))

    for row_idx, row in enumerate(rows, start=1):
        cells_xml: list[str] = []
        for col_idx in range(1, max_cols + 1):
            raw_value = row[col_idx - 1] if col_idx - 1 < len(row) else ""
            cell_ref = f"{excel_column_name(col_idx - 1)}{row_idx}"
            is_header_row = row_idx == 1
            style_index = 1 if is_header_row else (2 if row_idx % 2 == 0 else 3)

            if raw_value is None:
                cells_xml.append(make_inline_string_cell(cell_ref, "", style_index=style_index))
            elif isinstance(raw_value, bool):
                cells_xml.append(make_inline_string_cell(cell_ref, str(raw_value), style_index=style_index))
            elif isinstance(raw_value, (int, float)):
                cells_xml.append(make_numeric_cell(cell_ref, raw_value, style_index=style_index))
            else:
                cells_xml.append(make_inline_string_cell(cell_ref, str(raw_value), style_index=style_index))

        row_xml_parts.append(f'<row r="{row_idx}">{"".join(cells_xml)}</row>')

    dimension = "A1"
    if rows and max_cols:
        last_cell = f"{excel_column_name(max_cols - 1)}{len(rows)}"
        dimension = f"A1:{last_cell}"

    cols_xml = ""
    if max_cols:
        cols = []
        for i, max_len in enumerate(column_lengths, start=1):
            width = estimate_excel_width(max_len)
            cols.append(f'<col min="{i}" max="{i}" width="{width:.2f}" customWidth="1"/>')
        cols_xml = f"<cols>{''.join(cols)}</cols>"

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="{dimension}"/>'
        "<sheetViews><sheetView workbookViewId=\"0\"><pane ySplit=\"1\" topLeftCell=\"A2\" "
        "activePane=\"bottomLeft\" state=\"frozen\"/></sheetView></sheetViews>"
        "<sheetFormatPr defaultRowHeight=\"15\"/>"
        f"{cols_xml}"
        f"<sheetData>{''.join(row_xml_parts)}</sheetData>"
        "</worksheet>"
    )


def workbook_xml(sheet_names: list[str]) -> str:
    sheets_xml = []
    for i, sheet_name in enumerate(sheet_names, start=1):
        sheets_xml.append(f'<sheet name="{escape(sheet_name)}" sheetId="{i}" r:id="rId{i}"/>')
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        "<bookViews><workbookView/></bookViews>"
        f"<sheets>{''.join(sheets_xml)}</sheets>"
        "</workbook>"
    )


def workbook_rels_xml(sheet_count: int) -> str:
    rels = []
    for i in range(1, sheet_count + 1):
        rels.append(
            f'<Relationship Id="rId{i}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            f'Target="worksheets/sheet{i}.xml"/>'
        )
    rels.append(
        f'<Relationship Id="rId{sheet_count + 1}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        f"{''.join(rels)}"
        "</Relationships>"
    )


def root_rels_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        '<Relationship Id="rId2" '
        'Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" '
        'Target="docProps/core.xml"/>'
        '<Relationship Id="rId3" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" '
        'Target="docProps/app.xml"/>'
        "</Relationships>"
    )


def content_types_xml(sheet_count: int) -> str:
    overrides = [
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>',
        '<Override PartName="/docProps/core.xml" '
        'ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>',
        '<Override PartName="/docProps/app.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>',
    ]
    for i in range(1, sheet_count + 1):
        overrides.append(
            f'<Override PartName="/xl/worksheets/sheet{i}.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        )

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        f"{''.join(overrides)}"
        "</Types>"
    )


def styles_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="2">'
        '<font><sz val="11"/><name val="Calibri"/><family val="2"/></font>'
        '<font><b/><color rgb="FFFFFFFF"/><sz val="11"/><name val="Calibri"/><family val="2"/></font>'
        "</fonts>"
        '<fills count="5"><fill><patternFill patternType="none"/></fill>'
        '<fill><patternFill patternType="gray125"/></fill>'
        '<fill><patternFill patternType="solid"><fgColor rgb="1F4E78"/><bgColor indexed="64"/></patternFill></fill>'
        '<fill><patternFill patternType="solid"><fgColor rgb="EAF2FB"/><bgColor indexed="64"/></patternFill></fill>'
        '<fill><patternFill patternType="solid"><fgColor rgb="FFF8E8"/><bgColor indexed="64"/></patternFill></fill>'
        "</fills>"
        '<borders count="2">'
        '<border><left/><right/><top/><bottom/><diagonal/></border>'
        '<border>'
        '<left style="thin"><color rgb="FFD9D9D9"/></left>'
        '<right style="thin"><color rgb="FFD9D9D9"/></right>'
        '<top style="thin"><color rgb="FFD9D9D9"/></top>'
        '<bottom style="thin"><color rgb="FFD9D9D9"/></bottom>'
        '<diagonal/>'
        "</border>"
        "</borders>"
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="4">'
        '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
        '<xf numFmtId="0" fontId="1" fillId="2" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1">'
        '<alignment horizontal="center" vertical="center"/>'
        "</xf>"
        '<xf numFmtId="0" fontId="0" fillId="3" borderId="1" xfId="0" applyFill="1" applyBorder="1"/>'
        '<xf numFmtId="0" fontId="0" fillId="4" borderId="1" xfId="0" applyFill="1" applyBorder="1"/>'
        "</cellXfs>"
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        '<dxfs count="0"/>'
        '<tableStyles count="0" defaultTableStyle="TableStyleMedium9" defaultPivotStyle="PivotStyleLight16"/>'
        "</styleSheet>"
    )


def docprops_core_xml() -> str:
    created = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:dcterms="http://purl.org/dc/terms/" '
        'xmlns:dcmitype="http://purl.org/dc/dcmitype/" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        "<dc:creator>Codex</dc:creator>"
        "<cp:lastModifiedBy>Codex</cp:lastModifiedBy>"
        f'<dcterms:created xsi:type="dcterms:W3CDTF">{created}</dcterms:created>'
        f'<dcterms:modified xsi:type="dcterms:W3CDTF">{created}</dcterms:modified>'
        "</cp:coreProperties>"
    )


def docprops_app_xml(sheet_names: list[str]) -> str:
    titles = "".join(f"<vt:lpstr>{escape(name)}</vt:lpstr>" for name in sheet_names)
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" '
        'xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">'
        "<Application>Microsoft Excel</Application>"
        f"<TitlesOfParts><vt:vector size=\"{len(sheet_names)}\" baseType=\"lpstr\">{titles}</vt:vector></TitlesOfParts>"
        f"<HeadingPairs><vt:vector size=\"2\" baseType=\"variant\"><vt:variant><vt:lpstr>Worksheets</vt:lpstr></vt:variant><vt:variant><vt:i4>{len(sheet_names)}</vt:i4></vt:variant></vt:vector></HeadingPairs>"
        "</Properties>"
    )


def write_xlsx(output_file: Path, sheets: list[tuple[str, list[list[object]]]]) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    sanitized_names = []
    used_names: set[str] = set()
    for original_name, _rows in sheets:
        base_name = sanitize_sheet_name(original_name)
        final_name = base_name
        suffix = 1
        while final_name in used_names:
            suffix_str = f"_{suffix}"
            final_name = f"{base_name[: max(0, 31 - len(suffix_str))]}{suffix_str}"
            suffix += 1
        sanitized_names.append(final_name)
        used_names.add(final_name)

    with zipfile.ZipFile(output_file, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types_xml(len(sheets)))
        zf.writestr("_rels/.rels", root_rels_xml())
        zf.writestr("xl/workbook.xml", workbook_xml(sanitized_names))
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml(len(sheets)))
        zf.writestr("xl/styles.xml", styles_xml())
        zf.writestr("docProps/core.xml", docprops_core_xml())
        zf.writestr("docProps/app.xml", docprops_app_xml(sanitized_names))

        for i, (_sheet_name, rows) in enumerate(sheets, start=1):
            zf.writestr(f"xl/worksheets/sheet{i}.xml", worksheet_xml(rows))


def main() -> None:
    args = parse_args()
    logs_dir = Path(args.logs_dir).expanduser().resolve()
    output_file = Path(args.output).expanduser().resolve()

    log_files = list_base_log_files(logs_dir)
    if not log_files:
        raise RuntimeError(
            f"Nenhum arquivo de log base encontrado em {logs_dir} (arquivos .txt sem sufixo numerado)."
        )

    headers_by_file = collect_headers(log_files)

    sheets = [
        ("Headers por Arquivo", build_headers_per_file_sheet(headers_by_file)),
        ("Aparicoes de Headers", build_header_frequency_sheet(headers_by_file)),
        ("Resumo", build_summary_sheet(headers_by_file)),
    ]
    write_xlsx(output_file, sheets)

    print(f"Arquivo Excel gerado: {output_file}")
    print(f"Arquivos analisados ({len(log_files)}):")
    for path in log_files:
        print(f"- {path.name}")


if __name__ == "__main__":
    main()
