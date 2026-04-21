import datetime
import hashlib
import importlib.resources as resources
import logging
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

import comtradeapicall
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
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.data_file = database_file
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
        file_path = Path(f"{self.saving_dir}raw/jp_data.parquet")
        if not file_path.exists() or update:

            download(
                url="https://datos.estadisticas.pr/dataset/027ddbe1-c51c-46bf-aec3-a62d5d7e8539/resource/b8367825-a3de-41cf-8794-e42c10987b6f/download/ftrade_all_iepr.csv",
                filename=f"{tempfile.gettempdir()}/{hash(file_path)}.csv",
            )
            df = pl.read_csv(
                f"{tempfile.gettempdir()}/{hash(file_path)}.csv", ignore_errors=True
            )

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

    def pull_int_org(self, update: bool = False) -> pl.DataFrame:

        # Define Output file and hash
        parquet_path = Path("data") / "processed" / "jp_org.parquet"
        name_hash = hashlib.md5(str(parquet_path).encode()).hexdigest()

        # Define Temporary directories
        temp_zip = Path(tempfile.gettempdir()) / f"jp_org_zip_{name_hash}.zip"
        stage1_dir = Path(tempfile.gettempdir()) / f"stage1_{name_hash}"
        stage2_dir = Path(tempfile.gettempdir()) / f"stage2_{name_hash}"

        if not parquet_path.exists() or update:

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

            if csv_files:
                df_list = [pl.read_csv(f, ignore_errors=True) for f in csv_files]
                df = pl.concat(df_list)
                df.write_parquet(parquet_path)
                print(f"processed {len(csv_files)} CSVs into {parquet_path}")

            # Stage 4: Cleanup
            shutil.rmtree(stage1_dir)
            shutil.rmtree(stage2_dir)
            temp_zip.unlink(missing_ok=True)

        return pl.read_parquet(parquet_path)

    def pull_comtrade(self, iso: str, trade_id, date, code) -> pl.DataFrame:
        df = comtradeapicall.previewFinalData(
            typeCode="C",
            freqCode="M",
            clCode="HS",
            period=date,
            reporterCode=None,
            cmdCode=code,
            flowCode=trade_id,
            partnerCode=iso,
            partner2Code=None,
            customsCode=None,
            motCode=None,
            maxRecords=500,
            format_output="JSON",
            aggregateBy=None,
            breakdownMode="classic",
            countOnly=None,
            includeDesc=True,
        )
        if df.empty:
            return pl.DataFrame(df)

        df = pl.from_pandas(df).cast(pl.String)
        return pl.DataFrame(df)

    def insert_comtrade(self, iso: str):
        if (
            "comtradetable"
            not in self.conn.sql("SHOW TABLES;").df().get("name").tolist()
        ):
            init_com_trade_data_table(self.data_file)

        codes = (
            self.insert_int_org()
            .select(pl.col("hts_code").str.slice(0, 2).unique())
            .to_series()
            .to_list()
        )
        for year in range(2010, datetime.date.today().year + 1):
            for month in range(1, 13):
                if (
                    year == datetime.date.today().year
                    and month >= datetime.date.today().month
                ):
                    continue
                for code in codes:
                    if (
                        not self.conn.sql(
                            f"SELECT * FROM 'comtradetable' WHERE refYear={year} AND refMonth={month} AND cmdCode={code} AND partnerCode={iso} LIMIT(1);"
                        )
                        .df()
                        .empty
                    ):
                        continue
                    df = self.pull_comtrade(
                        iso, "X", f"{year}{str(month).zfill(2)}", code
                    )
                    if df.is_empty():
                        dummy_df = pl.DataFrame(
                            [
                                pl.Series("typeCode", [""], dtype=pl.String),
                                pl.Series("freqCode", [""], dtype=pl.String),
                                pl.Series("refPeriodId", [""], dtype=pl.String),
                                pl.Series("refYear", [str(year)], dtype=pl.String),
                                pl.Series("refMonth", [str(month)], dtype=pl.String),
                                pl.Series("period", [""], dtype=pl.String),
                                pl.Series("reporterCode", [""], dtype=pl.String),
                                pl.Series("reporterISO", [""], dtype=pl.String),
                                pl.Series("reporterDesc", [""], dtype=pl.String),
                                pl.Series("flowCode", [""], dtype=pl.String),
                                pl.Series("flowDesc", [""], dtype=pl.String),
                                pl.Series("partnerCode", [iso], dtype=pl.String),
                                pl.Series("partnerISO", [""], dtype=pl.String),
                                pl.Series("partnerDesc", [""], dtype=pl.String),
                                pl.Series("partner2Code", [""], dtype=pl.String),
                                pl.Series("partner2ISO", [""], dtype=pl.String),
                                pl.Series("partner2Desc", [""], dtype=pl.String),
                                pl.Series("classificationCode", [""], dtype=pl.String),
                                pl.Series(
                                    "classificationSearchCode", [""], dtype=pl.String
                                ),
                                pl.Series(
                                    "isOriginalClassification",
                                    [""],
                                    dtype=pl.String,
                                ),
                                pl.Series("cmdCode", [code], dtype=pl.String),
                                pl.Series("cmdDesc", [""], dtype=pl.String),
                                pl.Series("aggrLevel", [""], dtype=pl.String),
                                pl.Series("isLeaf", [""], dtype=pl.String),
                                pl.Series("customsCode", [""], dtype=pl.String),
                                pl.Series("customsDesc", [""], dtype=pl.String),
                                pl.Series("mosCode", [""], dtype=pl.String),
                                pl.Series("motCode", [""], dtype=pl.String),
                                pl.Series("motDesc", [""], dtype=pl.String),
                                pl.Series("qtyUnitCode", [""], dtype=pl.String),
                                pl.Series("qtyUnitAbbr", [""], dtype=pl.String),
                                pl.Series("qty", [0.0], dtype=pl.Float64),
                                pl.Series("isQtyEstimated", [""], dtype=pl.String),
                                pl.Series("altQtyUnitCode", [""], dtype=pl.String),
                                pl.Series("altQtyUnitAbbr", [""], dtype=pl.String),
                                pl.Series("altQty", [0.0], dtype=pl.Float64),
                                pl.Series("isAltQtyEstimated", [""], dtype=pl.String),
                                pl.Series("netWgt", [0.0], dtype=pl.Float64),
                                pl.Series("isNetWgtEstimated", [""], dtype=pl.String),
                                pl.Series("grossWgt", [0.0], dtype=pl.Float64),
                                pl.Series("isGrossWgtEstimated", [""], dtype=pl.String),
                                pl.Series("cifvalue", [0.0], dtype=pl.Float64),
                                pl.Series("fobvalue", [0.0], dtype=pl.Float64),
                                pl.Series("primaryValue", [0.0], dtype=pl.Float64),
                                pl.Series(
                                    "legacyEstimationFlag", [""], dtype=pl.String
                                ),
                                pl.Series("isReported", [""], dtype=pl.String),
                                pl.Series("isAggregate", [""], dtype=pl.String),
                            ]
                        )
                        self.conn.sql(
                            "INSERT INTO 'comtradetable' BY NAME SELECT * FROM dummy_df;"
                        )

                        logging.warning(
                            f"Returned None for {year}-{month} for {code} Inserted dummy records for iso {iso}"
                        )
                        continue
                    elif len(df) == 500:
                        logging.critical(
                            f"Error: {year}-{month} {code} and iso {iso} returned 500 rows."
                        )

                    self.conn.sql(
                        "INSERT INTO 'comtradetable' BY NAME SELECT * FROM df;"
                    )
                    logging.info(
                        f"Succesfully inserted {len(df)} records for {year}-{month} for {code} for iso {iso}"
                    )
        return self.conn.sql("SELECT * FROM 'comtradetable';").pl()

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
