import importlib.resources as resources
from datetime import datetime as dt
from typing import Literal
import polars as pl

from pr_imports import TradeUtils

LEVEL_GROUPS = {
    "total": [],
    "naics": ["naics"],
    "hts": ["hts_code"],
    "country": ["country"],
}

TIME_GROUPS = {
    "yearly": ["year"],
    "fiscal": ["fiscal_year"],
    "qtr": ["year", "qtr"],
    "monthly": ["year", "month"],
}


class JPTrade(TradeUtils):
    """
    Data processing class for the various data sources in DataPull.
    Optimized to dynamically aggregate metrics using a configuration-driven design.
    """

    def __init__(
        self,
        saving_dir: str = "data/",
        log_file: str = "data.log",
    ):
        """
        Initialize the DataProcess class.
        """
        super().__init__(saving_dir, log_file)
        self.agr_file = str(
            resources.files("jp_imports").joinpath("resources/code_agr.json")
        )

    def process_int_jp(
        self,
        level: Literal["hts", "naics", "country", "total"],
        time_frame: Literal["yearly", "fiscal", "qtr", "monthly"],
        datetime: str = "",
        agriculture_filter: bool = False,
        corrections: bool = False,
        source: Literal["jp", "org"] = "org",
        level_filter: str = "",
    ) -> pl.DataFrame:
        """
        Processes international trade data from the Puerto Rico Institute of
        Statistics for JP.

        The method loads data from either the organizational source or the JP-specific
        source, optionally filters agricultural products, applies taxonomy-level
        filtering by HTS code, NAICS code, or country, and filters the data by year or
        date range. The resulting data is converted to the standardized format and
        aggregated according to the requested time frame and level.

        Args:
            level (Literal["hts", "naics", "country", "total"]): The aggregation
                level for the processed data. Supported levels are HTS code, NAICS
                code, country, and total.
            time_frame (Literal["yearly", "fiscal", "qtr", "monthly"]): The time
                period used to aggregate the data. Supported options are yearly,
                fiscal, quarterly, and monthly.
            datetime (str): Optional date filter. A single year can be provided
                (e.g., ``"2024"``), or a date range in the format
                ``"YYYY-MM-DD+YYYY-MM-DD"``. Defaults to an empty string, which
                applies no date filter.
            agriculture_filter (bool): If True, limits the data to agricultural
                products where ``agri_prod`` equals 1. Defaults to False.
            source (Literal["jp", "org"]): The source of the international trade
                data. Use ``"org"`` for the organizational source or ``"jp"`` for
                the JP-specific source. Defaults to ``"org"``.
            level_filter (str): Optional prefix used to filter the selected
                taxonomy level. For example, when ``level="hts"``, this filters
                records whose HTS code starts with the provided value. Defaults to
                an empty string.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the filtered, converted,
                and aggregated international trade data.

        Raises:
            ValueError: If ``level_filter`` does not match any records for the
                selected taxonomy level, or if ``datetime`` contains an invalid
                number of date components.
        """

        if source == "org":
            df = self.pull_int_org()
        else:
            df = self.pull_int_jp()

        if agriculture_filter:
            df = df.filter(pl.col("agri_prod") == 1)

        if corrections:
            df = self.corrections(df=df)

        # Unified taxonomy filtering
        if level in ["hts", "naics", "country"]:
            level_map = {"hts": "hts_code", "naics": "naics", "country": "country"}
            filter_col = level_map[level]

            df = df.filter(pl.col(filter_col).str.starts_with(level_filter))
            if df.is_empty():
                raise ValueError(f"Invalid {level.upper()} code: {level_filter}")

        # Streamlined date routing
        if datetime:
            times = datetime.split("+")
            if len(times) == 2:
                start_date = dt.strptime(times[0], "%Y-%m-%d")
                end_date = dt.strptime(times[1], "%Y-%m-%d")
                df = df.filter(pl.col("date").is_between(start_date, end_date))
            elif len(times) == 1:
                df = df.filter(pl.col("date").dt.year() == int(datetime))
            else:
                raise ValueError(
                    'Invalid time format. Use "date" or "start_date+end_date"'
                )

        df = self.conversion(df)

        return self.process_data(time_frame=time_frame, level=level, base=df)

    def process_data(
        self, time_frame: str, level: str, base: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Processes and aggregates trade data according to the requested time frame
        and classification level.

        The method validates the requested grouping configuration, dynamically
        determines the required time and classification columns, and delegates
        filtering and aggregation to ``filter_data``. It then resolves duplicate
        columns created during joins by coalescing null values, removes temporary
        ``_right`` columns, fills missing trade metrics with zeros, and calculates
        net import and export values for both monetary and quantity measures.

        Args:
            time_frame (str): The time-based grouping to apply. Must be a key
                defined in ``TIME_GROUPS``.
            level (str): The classification level to apply. Must be a key defined
                in ``LEVEL_GROUPS``.
            base (pl.DataFrame): The base Polars DataFrame containing the trade
                data to process.

        Returns:
            pl.DataFrame: A Polars DataFrame grouped and sorted according to the
                requested time frame and classification level, with missing trade
                metrics filled with zero and net import/export metrics calculated.

        Raises:
            ValueError: If ``time_frame`` or ``level`` is not defined in the
                corresponding grouping configuration.
        """
        if time_frame not in TIME_GROUPS or level not in LEVEL_GROUPS:
            raise ValueError(
                f"Invalid combination layout requested: {time_frame=}, {level=}"
            )

        group_by_keys = TIME_GROUPS[time_frame] + LEVEL_GROUPS[level]

        df = self.filter_data(base, group_by_keys)

        coalesce_exprs = []
        for col in group_by_keys:
            right_col = f"{col}_right"
            if right_col in df.columns:
                coalesce_exprs.append(
                    pl.when(pl.col(col).is_null())
                    .then(pl.col(right_col))
                    .otherwise(pl.col(col))
                    .alias(col)
                )

        if coalesce_exprs:
            df = df.with_columns(coalesce_exprs)

        df = df.drop([c for c in df.columns if c.endswith("_right")])

        target_metrics = ["imports", "exports", "imports_qty", "exports_qty"]
        df = (
            df.with_columns(pl.col(target_metrics).fill_null(0))
            .sort(group_by_keys)
            .with_columns(
                net_exports=pl.col("exports") - pl.col("imports"),
                net_imports=pl.col("imports") - pl.col("exports"),
                net_exports_qty=pl.col("exports_qty") - pl.col("imports_qty"),
                net_imports_qty=pl.col("imports_qty") - pl.col("exports_qty"),
            )
        )

        return df

    def process_price(self, agriculture_filter: bool = False) -> pl.DataFrame:
        """
        Calculates rolling price statistics and year-over-year price changes for
        monthly international trade data.

        The method processes monthly trade data at the HTS level, optionally limits
        the data to agricultural products, and aggregates trade values and
        quantities to the HS4 classification. Import and export unit prices are
        calculated from the aggregated values and quantities. The method then
        computes three-month rolling averages and standard deviations, price bands,
        monthly rankings, and year-over-year changes in both prices and rankings.

        Args:
            agriculture_filter (bool): If True, limits the underlying trade data to
                agricultural products where ``agri_prod`` equals 1. Defaults to
                False.

        Returns:
            pl.DataFrame: A Polars DataFrame containing monthly HS4-level import and
                export prices, rolling price statistics, price bands, rankings,
                prior-year values, and year-over-year price and ranking changes.

        Note:
            Rolling statistics are calculated using a three-month window. Price
            changes and rankings are also compared against values from the prior
            year when sufficient historical data is available.
        """
        df = self.process_int_jp(
            time_frame="monthly", level="hts", agriculture_filter=agriculture_filter
        )
        df = df.with_columns(pl.col("imports_qty", "exports_qty").replace(0, 1))
        df = df.with_columns(hs4=pl.col("hts_code").str.slice(0, 4))

        df = df.group_by(pl.col("hs4", "month", "year")).agg(
            pl.col("imports").sum().alias("imports"),
            pl.col("exports").sum().alias("exports"),
            pl.col("imports_qty").sum().alias("imports_qty"),
            pl.col("exports_qty").sum().alias("exports_qty"),
        )

        df = df.with_columns(
            price_imports=pl.col("imports") / pl.col("imports_qty"),
            price_exports=pl.col("exports") / pl.col("exports_qty"),
        )

        df = df.with_columns(date=pl.datetime(pl.col("year"), pl.col("month"), 1)).sort(
            "date"
        )

        # Rolling Statistical Engine
        results = df.with_columns(
            pl.col("price_imports")
            .rolling_mean(window_size=3, min_samples=1)
            .over("hs4")
            .alias("moving_price_imports"),
            pl.col("price_exports")
            .rolling_mean(window_size=3, min_samples=1)
            .over("hs4")
            .alias("moving_price_exports"),
            pl.col("price_imports")
            .rolling_std(window_size=3, min_samples=1)
            .over("hs4")
            .alias("moving_price_imports_std"),
            pl.col("price_exports")
            .rolling_std(window_size=3, min_samples=1)
            .over("hs4")
            .alias("moving_price_exports_std"),
        ).with_columns(
            pl.col("moving_price_imports")
            .rank("ordinal")
            .over("date")
            .alias("rank_imports")
            .cast(pl.Int64),
            pl.col("moving_price_exports")
            .rank("ordinal")
            .over("date")
            .alias("rank_exports")
            .cast(pl.Int64),
            upper_band_imports=pl.col("moving_price_imports")
            + 2 * pl.col("moving_price_imports_std"),
            lower_band_imports=pl.col("moving_price_imports")
            - 2 * pl.col("moving_price_imports_std"),
            upper_band_exports=pl.col("moving_price_exports")
            + 2 * pl.col("moving_price_exports_std"),
            lower_band_exports=pl.col("moving_price_exports")
            - 2 * pl.col("moving_price_exports_std"),
        )

        results = df.join(results, on=["date", "hs4"], how="left", validate="1:1")

        # Clean up overlap column names after explicit join validation
        results = results.with_columns(
            year=pl.when(pl.col("year").is_null())
            .then(pl.col("year_right"))
            .otherwise(pl.col("year")),
            month=pl.when(pl.col("month").is_null())
            .then(pl.col("month_right"))
            .otherwise(pl.col("month")),
            imports=pl.when(pl.col("imports").is_null())
            .then(pl.col("imports_right"))
            .otherwise(pl.col("imports")),
            exports=pl.when(pl.col("exports").is_null())
            .then(pl.col("exports_right"))
            .otherwise(pl.col("exports")),
            price_imports=pl.when(pl.col("price_imports").is_null())
            .then(pl.col("price_imports_right"))
            .otherwise(pl.col("price_imports")),
            price_exports=pl.when(pl.col("price_exports").is_null())
            .then(pl.col("price_exports_right"))
            .otherwise(pl.col("price_exports")),
            imports_qty=pl.when(pl.col("imports_qty").is_null())
            .then(pl.col("exports_qty_right"))
            .otherwise(pl.col("imports_qty")),
            exports_qty=pl.when(pl.col("exports_qty").is_null())
            .then(pl.col("exports_qty"))
            .otherwise(pl.col("exports_qty")),
        ).drop([c for c in results.columns if c.endswith("_right")])

        # Track month shifts for sequential year-over-year deltas
        results = results.with_columns(
            pl.col("moving_price_imports")
            .pct_change()
            .over("date", "hs4")
            .alias("pct_change_imports")
        ).sort(by=["date", "hs4"])

        results = results.with_columns(
            pl.when(pl.col("date").dt.year() > 1)
            .then(pl.col("moving_price_imports").shift(12))
            .otherwise(None)
            .alias("prev_year_imports"),
            pl.when(pl.col("date").dt.year() > 1)
            .then(pl.col("moving_price_exports").shift(12))
            .otherwise(None)
            .alias("prev_year_exports"),
            pl.when(pl.col("date").dt.year() > 1)
            .then(pl.col("rank_imports").shift(12))
            .otherwise(None)
            .alias("prev_year_rank_imports"),
            pl.when(pl.col("date").dt.year() > 1)
            .then(pl.col("rank_exports").shift(12))
            .otherwise(None)
            .alias("prev_year_rank_exports"),
        )

        results = results.with_columns(
            (
                (pl.col("moving_price_imports") - pl.col("prev_year_imports"))
                / pl.col("prev_year_imports")
            ).alias("pct_change_imports_year_over_year"),
            (
                (pl.col("moving_price_exports") - pl.col("prev_year_exports"))
                / pl.col("prev_year_exports")
            ).alias("pct_change_exports_year_over_year"),
            (pl.col("rank_imports") - pl.col("prev_year_rank_imports")).alias(
                "rank_imports_change_year_over_year"
            ),
            (
                pl.col("rank_exports").cast(pl.Int64)
                - pl.col("prev_year_rank_exports").cast(pl.Int64)
            ).alias("rank_exports_change_year_over_year"),
        ).sort(by=["date", "hs4"])

        return results

    def conversion(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Converts trade quantity data to standardized units and derives date-based
        fields for downstream aggregation.

        The method normalizes quantity measurements from the primary and secondary
        quantity fields into a common kilogram-based representation using the
        corresponding unit codes. It handles supported units such as kilograms,
        liters, dozens, cubic meters, metric tons, grams, and other source-specific
        units. It also derives quarterly, fiscal-year, monthly, and calendar-year
        fields from the source date and combines the converted quantities into a
        single ``qty`` field.

        Args:
            df (pl.DataFrame): A Polars DataFrame containing the raw trade data.
                The DataFrame must include quantity, unit, HTS code, and date
                columns required for the conversion and date-field calculations.

        Returns:
            pl.DataFrame: A Polars DataFrame with standardized quantity fields,
                derived date dimensions, and a combined ``qty`` column representing
                the converted quantity.
        """
        # Precompute lowercase units and fill nulls efficiently
        df = df.with_columns(
            [
                pl.col("qty_1", "qty_2").fill_null(0),
                pl.col("unit_1").str.to_lowercase().alias("u1"),
                pl.col("unit_2").str.to_lowercase().alias("u2"),
            ]
        )

        return df.with_columns(
            qty=pl.when(pl.col("u1") == "kg")
            .then(pl.col("qty_1"))
            .when(pl.col("u2") == "kg")
            .then(pl.col("qty_2"))
            .when(pl.col("u1") == "gm")
            .then(pl.col("qty_1") / 1000)
            .when(pl.col("u2") == "gm")
            .then(pl.col("qty_2") / 1000)
            .when(pl.col("u1") == "t")
            .then(pl.col("qty_1") * 1000)
            .when(pl.col("u2") == "t")
            .then(pl.col("qty_2") * 1000)
            .when(pl.col("u1") == "l")
            .then(pl.col("qty_1") * 1)
            .when(pl.col("u2") == "l")
            .then(pl.col("qty_2") * 1)
            .when(pl.col("u1") == "doz")
            .then(pl.col("qty_1") * 0.70874)
            .when(pl.col("u2") == "doz")
            .then(pl.col("qty_2") * 0.70874)
            .when(pl.col("u1") == "m3")
            .then(pl.col("qty_1") * 353.8322)
            .when(pl.col("u2") == "m3")
            .then(pl.col("qty_2") * 353.8322)
            .when(
                (pl.col("u1") == "pfl")
                & (pl.col("hts_code").str.slice(0, 6) == "220710")
            )
            .then(pl.col("qty_1") * 0.5556)
            .when(
                (pl.col("u2") == "pfl")
                & (pl.col("hts_code").str.slice(0, 6) == "220710")
            )
            .then(pl.col("qty_2") * 0.5556)
            .when(
                (pl.col("u1") == "pfl")
                & (pl.col("hts_code").str.slice(0, 6) == "220870")
            )
            .then(pl.col("qty_1") * 2)
            .when(
                (pl.col("u2") == "pfl")
                & (pl.col("hts_code").str.slice(0, 6) == "220870")
            )
            .then(pl.col("qty_2") * 2)
            .when(pl.col("u1") == "pfl")
            .then(pl.col("qty_1") * 1.25)
            .when(pl.col("u2") == "pfl")
            .then(pl.col("qty_2") * 1.25)
            .otherwise(None),
            qtr=pl.col("date").dt.quarter(),
            fiscal_year=pl.when(pl.col("date").dt.month() > 6)
            .then(pl.col("date").dt.year() + 1)
            .otherwise(pl.col("date").dt.year()),
            month=pl.col("date").dt.month(),
            year=pl.col("date").dt.year(),
        ).drop(["u1", "u2"])

    def filter_data(self, df: pl.DataFrame, filter: list) -> pl.DataFrame:
        """
        Aggregates import and export data according to the specified grouping
        fields and combines the resulting trade measures into a single DataFrame.

        The method first excludes records without an HTS code, then separates the
        data into imports and exports using the ``trade_id`` field. Each trade type
        is grouped by the provided filter columns and aggregated for both trade
        value and quantity. The resulting import and export DataFrames are then
        joined using the grouping fields, preserving records that exist in either
        trade type.

        Args:
            df (pl.DataFrame): A Polars DataFrame containing the trade data. The
                DataFrame must include ``hts_code``, ``trade_id``, ``data``, and
                ``qty`` columns.
            filter (list): A list of column names used to group and aggregate the
                trade data.

        Returns:
            pl.DataFrame: A Polars DataFrame containing aggregated import and export
                values and quantities, grouped according to the provided filter
                columns.
        """
        df = df.filter(pl.col("hts_code").is_not_null())
        imports = (
            df.filter(pl.col("trade_id") == 1)
            .group_by(filter)
            .agg(pl.sum("data", "qty"))
            .sort(filter)
            .rename({"data": "imports", "qty": "imports_qty"})
        )
        exports = (
            df.filter(pl.col("trade_id") == 2)
            .group_by(filter)
            .agg(pl.sum("data", "qty"))
            .sort(filter)
            .rename({"data": "exports", "qty": "exports_qty"})
        )
        return imports.join(exports, on=filter, how="full")

    def corrections(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
            qty_1=pl.when(
                (pl.col("date").dt.year() == 2007)  # 2007 SOYBEAN OILCAKE Correction
                & (pl.col("date").dt.month() == 3)
                & (pl.col("qty_1") == 7540542599)
                & (pl.col("hts_code") == "2304000000")
            )
            .then(pl.lit(10648031))
            .when(
                (pl.col("date").dt.year() == 2008)
                & (pl.col("date").dt.month() == 6)
                & (pl.col("qty_1") == 5000000)
                & (pl.col("hts_code") == "2714900000")
            )
            .then(pl.lit(50000))
            .otherwise(pl.col("qty_1")),
            unit_1=pl.when(
                (pl.col("date").dt.year() >= 2012)
                & (pl.col("date").dt.year() <= 2017)
                & (pl.col("unit_1") == "t")
                & (pl.col("country") != "united states")
                & (pl.col("hts_code") == "1004900000")
            )
            .then(pl.lit("kg"))
            .otherwise(pl.col("unit_1")),
        )
        return df
