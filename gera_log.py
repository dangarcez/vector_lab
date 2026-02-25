from __future__ import annotations

import argparse
import csv
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


# Configuracoes principais
SEEDS_DIR = Path(__file__).resolve().parent / "seeds"
LOGS_DIR = Path(__file__).resolve().parent / "logs"
DEFAULT_SEED_FILE_NAME = "SPPVBRK.seed"  # Usado se nenhum argumento for passado
MAX_RECORDS_PER_FILE = 10  # Conta apenas registros (nao conta o cabecalho)
SLEEP_SECONDS = 60  # 60 segundos = 1 minuto
ROUND_TIME_TO_MINUTE = True  # Mantem segundos em 00, como no exemplo


INT_RANGE_RE = re.compile(r"^\s*(-?\d+)\s*-\s*(-?\d+)\s*$")


@dataclass(frozen=True)
class FieldSpec:
    name: str
    kind: str
    fixed_value: str | None = None
    min_value: int | None = None
    max_value: int | None = None

    def generate(self, timestamp_value: str) -> str:
        if self.kind == "str":
            return self.fixed_value or ""
        if self.kind == "time":
            return timestamp_value
        if self.kind == "int":
            assert self.min_value is not None and self.max_value is not None
            return str(random.randint(self.min_value, self.max_value))
        raise ValueError(f"Tipo de campo nao suportado: {self.kind}")


def parse_seed(seed_path: Path) -> list[FieldSpec]:
    if not seed_path.exists():
        raise FileNotFoundError(f"Seed nao encontrado: {seed_path}")

    fields: list[FieldSpec] = []
    for line_number, raw_line in enumerate(seed_path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue

        try:
            name, field_type, raw_value = line.split(":", 2)
        except ValueError as exc:
            raise ValueError(
                f"Linha invalida no seed {seed_path.name}:{line_number}: {raw_line!r}"
            ) from exc

        name = name.strip()
        kind = field_type.strip().lower()
        raw_value = raw_value.strip()

        if not name:
            raise ValueError(f"Nome de coluna vazio em {seed_path.name}:{line_number}")

        if kind == "str":
            fields.append(FieldSpec(name=name, kind="str", fixed_value=raw_value))
            continue

        if kind == "time":
            fields.append(FieldSpec(name=name, kind="time"))
            continue

        if kind == "int":
            match = INT_RANGE_RE.match(raw_value)
            if not match:
                raise ValueError(
                    f"Range invalido para int em {seed_path.name}:{line_number}: {raw_value!r}"
                )
            min_value = int(match.group(1))
            max_value = int(match.group(2))
            if min_value > max_value:
                raise ValueError(
                    f"Range invertido para int em {seed_path.name}:{line_number}: {raw_value!r}"
                )
            fields.append(
                FieldSpec(
                    name=name,
                    kind="int",
                    min_value=min_value,
                    max_value=max_value,
                )
            )
            continue

        raise ValueError(f"Tipo nao suportado em {seed_path.name}:{line_number}: {field_type!r}")

    if not fields:
        raise ValueError(f"O seed {seed_path.name} nao possui colunas validas.")

    return fields


def format_timestamp_now() -> str:
    now = datetime.now()
    if ROUND_TIME_TO_MINUTE:
        now = now.replace(second=0, microsecond=0)
    return now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-2]


def write_csv_row(file_path: Path, row: list[str]) -> None:
    with file_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_ALL, lineterminator="\n")
        writer.writerow(row)


def load_or_initialize_output(file_path: Path, header: list[str]) -> int:
    """
    Garante que o arquivo base exista e possua cabecalho.
    Retorna a quantidade de registros ja existentes (sem contar cabecalho).
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)

    if not file_path.exists():
        write_csv_row(file_path, header)
        return 0

    with file_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    if not rows:
        write_csv_row(file_path, header)
        return 0

    if rows[0] != header:
        raise ValueError(
            f"Cabecalho atual de {file_path.name} difere do seed. "
            "Ajuste o arquivo ou remova-o antes de rodar o script."
        )

    return max(len(rows) - 1, 0)


def next_rotation_path(file_path: Path) -> Path:
    prefix = f"{file_path.name}."
    max_index = 0

    for candidate in file_path.parent.iterdir():
        if not candidate.is_file():
            continue
        if not candidate.name.startswith(prefix):
            continue
        suffix = candidate.name[len(prefix) :]
        if suffix.isdigit():
            max_index = max(max_index, int(suffix))

    return file_path.with_name(f"{file_path.name}.{max_index + 1}")


def rotate_output_file(file_path: Path) -> None:
    if not file_path.exists():
        return
    rotated_path = next_rotation_path(file_path)
    file_path.rename(rotated_path)
    print(f"[rotacao] {file_path.name} -> {rotated_path.name}")


def build_record(fields: list[FieldSpec]) -> list[str]:
    timestamp_value = format_timestamp_now()
    return [field.generate(timestamp_value) for field in fields]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera logs continuamente a partir de um arquivo .seed."
    )
    parser.add_argument(
        "seed",
        nargs="?",
        default=DEFAULT_SEED_FILE_NAME,
        help=(
            "Nome do arquivo .seed dentro de ./seeds (ex.: SPPVBRK.seed) "
            "ou caminho relativo/absoluto para um seed."
        ),
    )
    return parser.parse_args()


def resolve_seed_path(seed_argument: str) -> Path:
    candidate = Path(seed_argument)
    if candidate.suffix == "":
        candidate = candidate.with_suffix(".seed")

    if candidate.is_absolute():
        return candidate

    project_root = Path(__file__).resolve().parent
    if candidate.parts and candidate.parts[0] == "seeds":
        return project_root / candidate

    return SEEDS_DIR / candidate


def main() -> None:
    args = parse_args()
    seed_path = resolve_seed_path(args.seed)
    output_path = LOGS_DIR / f"{seed_path.stem}.txt"

    fields = parse_seed(seed_path)
    header = [field.name for field in fields]

    current_records = load_or_initialize_output(output_path, header)
    print(
        f"Iniciando geracao a partir de {seed_path.name} -> {output_path.name} "
        f"(registros atuais: {current_records}, limite: {MAX_RECORDS_PER_FILE})"
    )

    while True:
        if current_records >= MAX_RECORDS_PER_FILE:
            rotate_output_file(output_path)
            current_records = load_or_initialize_output(output_path, header)

        record = build_record(fields)
        write_csv_row(output_path, record)
        current_records += 1
        print(f"[{current_records}/{MAX_RECORDS_PER_FILE}] registro adicionado em {output_path.name}")
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
