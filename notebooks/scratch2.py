from pathlib import Path
import polars as pl
import io
import csv
import httpx

OUTPUT_DIR = Path("./output_data")
OUTPUT_DIR.mkdir(exist_ok=True)

URL = "https://datos.estadisticas.pr/dataset/027ddbe1-c51c-46bf-aec3-a62d5d7e8539/resource/b8367825-a3de-41cf-8794-e42c10987b6f/download/ftrade_all_iepr.csv"
CHUNK_SIZE_LINES = 50_000


with httpx.stream("GET", url=URL, verify=False) as response:
    response.raise_for_status()

    lines = response.iter_lines()

    csv_reader = csv.reader(lines)

    header = next(csv_reader)
    print(f"Headers: {header}")

    for row in csv_reader:
        print(row)
