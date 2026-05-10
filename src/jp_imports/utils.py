import hashlib
import importlib.resources as resources
import logging
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

import duckdb
import polars as pl
from CensusForge import CensusAPI
from jp_tools import download


class TradeUtils:
    """
    This class pulls data from the CENSUS and the Puerto Rico Institute of Statistics

    """

    def __init__(
        self,
        saving_dir: str = "data/",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.conn = duckdb.connect()

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filename=log_file,
        )

    def pull_int_jp(self, update: bool = False) -> pl.DataFrame:
        """
        Pulls data from the Puerto Rico Institute of Statistics used by the JP.
            Saved them in the raw directory as parquet files.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        file_path = Path(self.saving_dir) / "raw" / "jp_data.parquet"
        name_hash = hashlib.md5(str(file_path).encode()).hexdigest()
        temp_csv = Path(tempfile.gettempdir()) / f"{name_hash}.csv"

        if not file_path.exists() or update:

            download(
                url="https://datos.estadisticas.pr/dataset/027ddbe1-c51c-46bf-aec3-a62d5d7e8539/resource/b8367825-a3de-41cf-8794-e42c10987b6f/download/ftrade_all_iepr.csv",
                filename=str(temp_csv),
                verify=False,
            )
            df = pl.read_csv(temp_csv, ignore_errors=True)

            agri_prod = pl.read_json(
                str(resources.files("jp_imports").joinpath("resources/code_agr.json"))
            ).transpose()
            agri_prod = (
                agri_prod.with_columns(pl.nth(0).cast(pl.String).str.zfill(4))
                .to_series()
                .to_list()
            )
            df = df.rename({col: col.lower() for col in df.collect_schema().names()})
            df = df.with_columns(
                date=pl.col("year").cast(pl.String)
                + "-"
                + pl.col("month").cast(pl.String)
                + "-01",
                unit_1=pl.col("unit_1").str.to_lowercase(),
                unit_2=pl.col("unit_2").str.to_lowercase(),
                hts_code=pl.col("commodity_code")
                .cast(pl.String)
                .str.zfill(10)
                .str.replace("'", ""),
                trade_id=pl.when(pl.col("trade") == "i").then(1).otherwise(2),
            )

            df = df.with_columns(pl.col("date").cast(pl.Date))

            df = df.with_columns(
                agri_prod=pl.when(pl.col("hts_code").is_in(agri_prod))
                .then(1)
                .otherwise(0)
            )
            df = df.with_columns(
                sitc=pl.when(pl.col("sitc_short_desc").str.starts_with("Civilian"))
                .then(9998)
                .when(pl.col("sitc_short_desc").str.starts_with("-"))
                .then(9999)
                .otherwise(pl.col("sitc"))
            )
            df = df.filter(pl.col("hts_code").is_not_null())
            df = df.select(
                pl.col(
                    "date",
                    "country",
                    "trade_id",
                    "agri_prod",
                    "hts_code",
                    "hts_desc",
                    "data",
                    "qty_1",
                    "unit_1",
                    "qty_2",
                    "unit_2",
                    "sitc",
                    "naics",
                )
            )

            df.write_parquet(file_path)

        return pl.read_parquet(file_path)

    def pull_int_org(self, update: bool = False, export: bool = True) -> pl.DataFrame:
        """
        Downloads, extracts, and unifies organizational data from the Puerto Rico
        Institute of Statistics.

        The process involves a nested extraction: a parent ZIP contains multiple
        inner ZIPs, which in turn contain the target CSV files. Due to known
        formatting issues with the source ZIP files, the method employs a fallback
        to the '7z' system utility if the standard zipfile library fails.

        Args:
            update (bool): If True, ignores existing local parquet files and
                re-downloads/processes the data. Defaults to False.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the unified data from all
                extracted CSVs.

        Note:
            Requires '7z' to be installed in the system PATH as a fallback for
            malformed archive handling.
        """

        # Define Output file and hash
        parquet_export_path = Path(self.saving_dir) / "raw" / "jp_org_exports.parquet"
        parquet_import_path = Path(self.saving_dir) / "raw" / "jp_org_imports.parquet"
        name_hash = hashlib.md5(str(parquet_export_path).encode()).hexdigest()

        # Define Temporary directories
        temp_zip = Path(tempfile.gettempdir()) / f"jp_org_zip_{name_hash}.zip"
        stage1_dir = Path(tempfile.gettempdir()) / f"stage1_{name_hash}"
        stage2_dir = Path(tempfile.gettempdir()) / f"stage2_{name_hash}"

        if not parquet_export_path.exists() or update:

            download(
                url="http://apps.estadisticas.pr/iepr/LinkClick.aspx?fileticket=JVyYmIHqbqc%3d&tabid=284&mid=244930",
                filename=(str(temp_zip)),
            )

            # Stage 1: Extract all Parent zip file
            # WARNING: Added the use of subprocess due to mal formated zip from estadisticas.pr.gov

            stage1_dir.mkdir(parents=True, exist_ok=True)

            try:
                with zipfile.ZipFile(temp_zip, "r") as zip_ref:
                    zip_ref.extractall(stage1_dir)

            except zipfile.BadZipFile:
                print("Warning: Used 7z subprocess due to mall formated file")
                subprocess.run(
                    ["7z", "x", str(temp_zip), f"-o{stage1_dir}", "-y"],
                    capture_output=True,
                )

            stage2_dir.mkdir(parents=True, exist_ok=True)
            inner_zips = list(stage1_dir.rglob("*.zip"))

            for izip in inner_zips:
                try:
                    with zipfile.ZipFile(izip, "r") as zip_ref:
                        # Extracting to our second temp directory
                        zip_ref.extractall(stage2_dir)
                except zipfile.BadZipFile:
                    print(
                        f"Warning: {izip.name} was also malformed. Switching back to 7-zip for this file."
                    )
                    subprocess.run(
                        ["7z", "x", str(izip), f"-o{stage2_dir}", "-y"],
                        capture_output=True,
                    )

            # Stage 3: unify data into a single file
            csv_files = list(stage2_dir.rglob("*.csv"))
            if not csv_files:
                raise FileNotFoundError(
                    "Extraction successful, but no CSV files were found to process."
                )

            df_list = [pl.read_csv(f, ignore_errors=True) for f in csv_files]
            df = pl.concat(df_list)
            df = df.with_columns(
                country=pl.col("country").str.to_lowercase(),
                hts=pl.col("HTS").str.replace("'", ""),
                date=(
                    pl.col("year").cast(pl.String)
                    + "-"
                    + pl.col("month").cast(pl.String).str.zfill(2)
                    + "-01"
                ).str.to_datetime("%Y-%m-%d"),
            )
            df = df.select(
                pl.col(
                    "date",
                    "country",
                    "hts",
                    "unit_1",
                    "qty_1",
                    "unit_2",
                    "qty_2",
                    "value",
                    "import_export",
                )
            )

            # Save Export Data
            df_exports = df.filter(pl.col("import_export") == "e").drop("import_export")
            df_exports.write_parquet(parquet_export_path)

            # Save import data
            df_imports = df.filter(pl.col("import_export") == "i").drop("import_export")
            df_imports.write_parquet(parquet_import_path)

            # Stage 4: Cleanup
            shutil.rmtree(stage1_dir)
            shutil.rmtree(stage2_dir)
            temp_zip.unlink(missing_ok=True)

        if export:
            return pl.read_parquet(parquet_export_path)
        else:
            return pl.read_parquet(parquet_import_path)

    def pull_census_hts(self, exports: bool, state: str) -> pl.DataFrame:
        """
        Pulls HTS data from the Census and saves them in a parquet file.

        Parameters
        ----------
        end_year: int
            The last year to pull data from.
        start_year: int
            The first year to pull data from.
        exports: bool
            If True, pulls exports data. If False, pulls imports data.
        state: str
            The state to pull data from (e.g. "PR" for Puerto Rico).

        Returns
        -------
        None
        """

        for _year in range(2010, 2023):
            exports_path = Path(
                f"{self.saving_dir}raw/census-hts-exports-{state}-{_year}.parquet"
            )
            imports_path = Path(
                f"{self.saving_dir}raw/census-hts-imports-{state}-{_year}.parquet"
            )

            if exports_path.exists() and imports_path.exists():
                continue

            req_exports = CensusAPI().timeseries_query(
                dataset="timeseries-intltrade-exports-statehs",
                params_list=[
                    "CTY_CODE",
                    "CTY_NAME",
                    "ALL_VAL_MO",
                    "COMM_LVL",
                    "E_COMMODITY",
                ],
                year=_year,
                extra=f"STATE={state}",
                skip_checks=True,
            )

            req_imports = CensusAPI().timeseries_query(
                dataset="timeseries-intltrade-imports-statehs",
                params_list=[
                    "CTY_CODE",
                    "CTY_NAME",
                    "GEN_VAL_MO",
                    "COMM_LVL",
                    "I_COMMODITY",
                ],
                year=_year,
                extra=f"STATE={state}",
                skip_checks=True,
            )

            df_exports = pl.DataFrame(req_exports)
            df_exports.write_parquet(exports_path)

            df_imports = pl.DataFrame(req_imports)
            df_imports.write_parquet(imports_path)
        if exports:
            return self.conn.execute(
                f"SELECT * FROM '{self.saving_dir}raw/census-hts-exports-{state}-*.parquet';"
            ).pl()
        else:
            return self.conn.execute(
                f"SELECT * FROM '{self.saving_dir}raw/census-hts-imports-{state}-*.parquet';"
            ).pl()

    def pull_census_naics(self, exports: bool, state: str) -> pl.DataFrame:
        """
        Pulls NAICS data from the Census and saves them in a parquet file.

        Parameters
        ----------
        end_year: int
            The last year to pull data from.
        start_year: int
            The first year to pull data from.
        exports: bool
            If True, pulls exports data. If False, pulls imports data.
        state: str
            The state to pull data from (e.g. "PR" for Puerto Rico).

        Returns
        -------
        None
        """

        for _year in range(2010, 2023):
            exports_path = Path(
                f"{self.saving_dir}raw/census-naics-exports-{state}-{_year}.parquet"
            )
            imports_path = Path(
                f"{self.saving_dir}raw/census-naics-imports-{state}-{_year}.parquet"
            )

            if exports_path.exists() and imports_path.exists():
                continue

            req_exports = CensusAPI().timeseries_query(
                dataset="timeseries-intltrade-exports-statenaics",
                params_list=[
                    "CTY_CODE",
                    "CTY_NAME",
                    "ALL_VAL_MO",
                    "COMM_LVL",
                    "NAICS",
                ],
                year=_year,
                extra=f"STATE={state}",
                skip_checks=True,
            )

            req_imports = CensusAPI().timeseries_query(
                dataset="timeseries-intltrade-imports-statenaics",
                params_list=[
                    "CTY_CODE",
                    "CTY_NAME",
                    "GEN_VAL_MO",
                    "COMM_LVL",
                    "NAICS",
                ],
                year=_year,
                extra=f"STATE={state}",
                skip_checks=True,
            )

            df_exports = pl.DataFrame(req_exports)
            df_exports = df_exports.rename(df_exports.row(0, named=True))
            df_exports = df_exports.slice(1)
            df_exports.write_parquet(exports_path)

            df_imports = pl.DataFrame(req_imports)
            df_imports = df_imports.rename(df_imports.row(0, named=True))
            df_imports = df_imports.slice(1)
            df_imports.write_parquet(imports_path)
        if exports:
            return self.conn.execute(
                f"SELECT * FROM '{self.saving_dir}raw/census-naics-exports-{state}-*.parquet';"
            ).pl()
        else:
            return self.conn.execute(
                f"SELECT * FROM '{self.saving_dir}raw/census-naics-imports-{state}-*.parquet';"
            ).pl()
