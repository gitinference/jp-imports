import polars as pl
import io
from pathlib import Path
import httpx

OUTPUT_DIR = Path("./output_data")
OUTPUT_DIR.mkdir(exist_ok=True)

URL = "https://datos.estadisticas.pr/dataset/027ddbe1-c51c-46bf-aec3-a62d5d7e8539/resource/b8367825-a3de-41cf-8794-e42c10987b6f/download/ftrade_all_iepr.csv"
CHUNK_SIZE_LINES = 50_000


def get_existing_ids() -> set[str]:
    """Scans all nested CSV files recursively to gather already saved IDs."""
    # Use rglob or '**/*.csv' to find files in subdirectories (year/month/data.csv)
    existing_files = list(OUTPUT_DIR.glob("**/*.csv"))
    if not existing_files:
        return set()

    df_existing = (
        pl.scan_csv(existing_files, schema_overrides={"id": pl.String})
        .select(pl.col("id").cast(pl.String))
        .drop_nulls()
        .collect()
    )
    return set(df_existing["id"].to_list())


def process_batch(df_chunk: pl.DataFrame, existing_ids: set[str]):
    """Filters chunk against existing IDs, partitions into year/month directories, and appends to data.csv."""
    # Standardize 'id' column to string if present
    if "id" in df_chunk.columns:
        df_chunk = df_chunk.with_columns(pl.col("id").cast(pl.String))
        if existing_ids:
            df_chunk = df_chunk.filter(~pl.col("id").is_in(existing_ids))

    if df_chunk.is_empty():
        return

    if "id" in df_chunk.columns:
        existing_ids.update(df_chunk["id"].to_list())

    # Locate Year and Month columns dynamically (handles cases like 'Year', 'year', 'Month', 'month')
    year_col = next(
        (c for c in ["Year", "year", "YEAR"] if c in df_chunk.columns), None
    )
    month_col = next(
        (c for c in ["Month", "month", "MONTH"] if c in df_chunk.columns), None
    )

    # Fallback if Year or Month column isn't found
    if not year_col or not month_col:
        target_dir = OUTPUT_DIR / "unknown"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / "data.csv"
        file_exists = target_file.exists() and target_file.stat().st_size > 0
        with open(target_file, mode="a" if file_exists else "w", encoding="utf-8") as f:
            df_chunk.write_csv(f, include_header=not file_exists)
        return

    # Partition batch by Year and Month
    for (year_val, month_val), df_group in df_chunk.group_by([year_col, month_col]):
        # Sanitize folder names and format month as 2 digits if numeric (e.g., '01', '02')
        yr_str = str(year_val).strip()
        mo_str = (
            str(month_val).strip().zfill(2)
            if str(month_val).strip().isdigit()
            else str(month_val).strip()
        )

        # Build directory path: output_data/YEAR/MONTH/
        target_dir = OUTPUT_DIR / yr_str / mo_str
        target_dir.mkdir(parents=True, exist_ok=True)

        target_file = target_dir / "data.csv"
        file_exists = target_file.exists() and target_file.stat().st_size > 0

        # Append to year/month/data.csv
        with open(target_file, mode="a" if file_exists else "w", encoding="utf-8") as f:
            df_group.write_csv(f, include_header=not file_exists)


def stream_csv_with_polars(url: str):
    existing_ids = get_existing_ids()

    with httpx.stream("GET", url, follow_redirects=True, verify=False) as response:
        response.raise_for_status()

        line_buffer = []
        header = None

        for line in response.iter_lines():
            if not line.strip():
                continue

            if header is None:
                header = line
                continue

            line_buffer.append(line)

            if len(line_buffer) >= CHUNK_SIZE_LINES:
                csv_bytes = (header + "\n" + "\n".join(line_buffer)).encode("utf-8")

                # Treat all columns as string during chunk reading to avoid type infer errors
                df_chunk = pl.read_csv(io.BytesIO(csv_bytes), infer_schema_length=0)

                process_batch(df_chunk, existing_ids)
                line_buffer.clear()

        if line_buffer:
            csv_bytes = (header + "\n" + "\n".join(line_buffer)).encode("utf-8")
            df_chunk = pl.read_csv(io.BytesIO(csv_bytes), infer_schema_length=0)
            process_batch(df_chunk, existing_ids)


if __name__ == "__main__":
    stream_csv_with_polars(URL)
