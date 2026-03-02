from __future__ import annotations

import argparse
import csv
import re
import time
from datetime import datetime, timedelta
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Modelo padrao solicitado
DEFAULT_MODEL_FILENAME = "ResourceStats_SPPNBRK_SPPNT016_JVM.txt"
DEFAULT_MODEL_DIR = Path("/log_raiz")
FALLBACK_MODEL_DIR = SCRIPT_DIR
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "logs"

# Ajuste por variavel conforme pedido
GENERATE_EVERY_SECONDS = 60  # 5 minutos
MAX_RECORDS_PER_FILE = 1000   # sem contar cabecalho


def normalize_header(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def format_timestamp(value: datetime) -> str:
    # Mesmo estilo do arquivo raiz: YYYY-MM-DD HH:MM:SS.mmm
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Gera linhas em um arquivo de ResourceStats a partir de um arquivo modelo, "
            "ajustando campos comuns dinamicamente e executando continuamente."
        )
    )
    parser.add_argument("broker", help="Valor para o campo BrokerName.")
    parser.add_argument("execution_group", help="Valor para o campo ExecutionGroupLabel.")
    parser.add_argument(
        "--model",
        default="",
        help=(
            "Arquivo modelo CSV. Se omitido, tenta "
            f"{DEFAULT_MODEL_DIR / DEFAULT_MODEL_FILENAME} e depois "
            f"{FALLBACK_MODEL_DIR / DEFAULT_MODEL_FILENAME}."
        ),
    )
    parser.add_argument(
        "--output",
        default="",
        help=(
            "Arquivo de saida. Se omitido, gera em ./logs/ com nome "
            "ResourceStats_<broker>_<execution_group>_<tipo>.txt."
        ),
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=GENERATE_EVERY_SECONDS,
        help=f"Intervalo entre registros (padrao: {GENERATE_EVERY_SECONDS}s).",
    )
    parser.add_argument(
        "--max-records-per-file",
        type=int,
        default=MAX_RECORDS_PER_FILE,
        help=f"Limite de registros por arquivo antes de rotacao (padrao: {MAX_RECORDS_PER_FILE}).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="Quantidade de iteracoes para teste. 0 = infinito (padrao).",
    )
    return parser.parse_args()


def resolve_model_path(model_arg: str) -> Path:
    if model_arg:
        return Path(model_arg).expanduser().resolve()

    candidates = [
        DEFAULT_MODEL_DIR / DEFAULT_MODEL_FILENAME,
        FALLBACK_MODEL_DIR / DEFAULT_MODEL_FILENAME,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()

    raise FileNotFoundError(
        "Arquivo modelo padrao nao encontrado. "
        f"Tentado: {', '.join(str(p) for p in candidates)}"
    )


def load_model(model_path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not model_path.exists():
        raise FileNotFoundError(f"Arquivo modelo nao encontrado: {model_path}")

    with model_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = reader.fieldnames or []
        rows = list(reader)

    if not headers:
        raise ValueError(f"O arquivo modelo {model_path} nao possui cabecalho.")
    if not rows:
        raise ValueError(f"O arquivo modelo {model_path} nao possui linhas de dados.")
    return headers, rows


def infer_output_path(model_path: Path, broker: str, execution_group: str) -> Path:
    stem = model_path.stem
    type_token = stem.rsplit("_", 1)[-1] if "_" in stem else stem
    filename = f"ResourceStats_{broker}_{execution_group}_{type_token}.txt"
    return DEFAULT_OUTPUT_DIR / filename


def ensure_output_header(output_path: Path, headers: list[str]) -> int:
    """
    Garante cabecalho no arquivo de saida.
    Retorna quantidade de registros ja existentes (sem contar cabecalho).
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not output_path.exists() or output_path.stat().st_size == 0:
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(headers)
        return 0

    with output_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        rows = list(reader)

    if not rows:
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(headers)
        return 0

    current_header = rows[0]
    if current_header != headers:
        raise ValueError(
            f"Cabecalho de {output_path.name} difere do modelo. "
            "Remova/ajuste o arquivo de saida para continuar."
        )

    return max(len(rows) - 1, 0)


def read_first_data_row(output_path: Path, headers: list[str]) -> dict[str, str] | None:
    with output_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, fieldnames=headers)
        next(reader, None)  # pula header
        row = next(reader, None)
    return row


def list_rotated_files(output_path: Path) -> list[Path]:
    prefix = f"{output_path.name}."
    files: list[tuple[int, Path]] = []

    if not output_path.parent.exists():
        return []

    for candidate in output_path.parent.iterdir():
        if not candidate.is_file():
            continue
        if not candidate.name.startswith(prefix):
            continue
        suffix = candidate.name[len(prefix) :]
        if suffix.isdigit():
            files.append((int(suffix), candidate))

    files.sort(key=lambda item: item[0])
    return [item[1] for item in files]


def next_rotation_path(output_path: Path) -> Path:
    rotated = list_rotated_files(output_path)
    if not rotated:
        return output_path.with_name(f"{output_path.name}.1")
    last_suffix = int(rotated[-1].name.split(".")[-1])
    return output_path.with_name(f"{output_path.name}.{last_suffix + 1}")


def rotate_output_file(output_path: Path) -> None:
    if not output_path.exists():
        return
    rotated_path = next_rotation_path(output_path)
    output_path.rename(rotated_path)
    print(f"[rotacao] {output_path.name} -> {rotated_path.name}")


def count_records(file_path: Path, expected_header: list[str]) -> int:
    if not file_path.exists():
        return 0

    with file_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    if not rows:
        return 0
    if rows[0] != expected_header:
        raise ValueError(
            f"Cabecalho de {file_path.name} difere do modelo esperado. "
            "Ajuste/remova o arquivo para continuar."
        )
    return max(len(rows) - 1, 0)


def total_existing_records(output_path: Path, expected_header: list[str]) -> int:
    total = count_records(output_path, expected_header)
    for rotated in list_rotated_files(output_path):
        total += count_records(rotated, expected_header)
    return total


def main() -> None:
    args = parse_args()
    if args.interval_seconds < 1:
        raise ValueError("O argumento --interval-seconds deve ser >= 1.")
    if args.max_records_per_file < 1:
        raise ValueError("O argumento --max-records-per-file deve ser >= 1.")
    if args.iterations < 0:
        raise ValueError("O argumento --iterations deve ser >= 0.")

    model_path = resolve_model_path(args.model)
    headers, model_rows = load_model(model_path)

    output_path = (
        Path(args.output).expanduser().resolve()
        if args.output
        else infer_output_path(model_path, args.broker, args.execution_group)
    )

    header_index = {normalize_header(name): i for i, name in enumerate(headers)}
    existing_records = ensure_output_header(output_path, headers)

    collection_key = "collectiontimestamp"
    broker_key = "brokername"
    eg_key = "executiongrouplabel"
    start_key = "starttimestamp"
    end_key = "endtimestamp"  # cobre EndTimestamp e EndTimeStamp apos normalizacao

    # Se iniciar acima do limite, rotaciona antes de continuar.
    while existing_records >= args.max_records_per_file:
        rotate_output_file(output_path)
        existing_records = ensure_output_header(output_path, headers)

    total_records = total_existing_records(output_path, headers)
    template_idx = total_records % len(model_rows)

    collection_timestamp = format_timestamp(datetime.now())
    if collection_key in header_index and existing_records > 0:
        first_row = read_first_data_row(output_path, headers)
        if first_row:
            candidate = first_row.get(headers[header_index[collection_key]])
            if candidate:
                collection_timestamp = candidate

    print(f"Modelo: {model_path}")
    print(f"Saida: {output_path}")
    print(f"Intervalo: {args.interval_seconds}s")
    print(f"Limite por arquivo: {args.max_records_per_file}")

    next_run = time.monotonic()
    remaining = args.iterations

    while True:
        if existing_records >= args.max_records_per_file:
            rotate_output_file(output_path)
            existing_records = ensure_output_header(output_path, headers)
            collection_timestamp = format_timestamp(datetime.now())

        template_row = model_rows[template_idx]
        row = [template_row.get(h, "") for h in headers]

        now = datetime.now()
        end_timestamp = format_timestamp(now)
        start_timestamp = format_timestamp(now - timedelta(minutes=5))

        if broker_key in header_index:
            row[header_index[broker_key]] = args.broker
        if eg_key in header_index:
            row[header_index[eg_key]] = args.execution_group
        if collection_key in header_index:
            row[header_index[collection_key]] = collection_timestamp
        if start_key in header_index:
            row[header_index[start_key]] = start_timestamp
        if end_key in header_index:
            row[header_index[end_key]] = end_timestamp

        with output_path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(row)

        existing_records += 1
        total_records += 1
        template_idx = total_records % len(model_rows)

        print(
            f"[ok] total={total_records} arquivo_atual={existing_records}/{args.max_records_per_file} "
            f"start={start_timestamp} end={end_timestamp}"
        )

        if remaining > 0:
            remaining -= 1
            if remaining == 0:
                break

        next_run += args.interval_seconds
        sleep_for = next_run - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_run = time.monotonic()


if __name__ == "__main__":
    main()
