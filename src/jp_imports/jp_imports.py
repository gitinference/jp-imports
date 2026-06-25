import importlib.resources as resources
from datetime import datetime as dt

import polars as pl

from .utils import TradeUtils

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

    def __init__(self, saving_dir: str = "data/", database_file: str = "data.ddb"):
        """
        Initialize the DataProcess class.
        """
        super().__init__(saving_dir, database_file)
        self.agr_file = str(
            resources.files("jp_imports").joinpath("resources/code_agr.json")
        )

    def process_int_jp(
        self,
        level: str,
        time_frame: str,
        datetime: str = "",
        agriculture_filter: bool = False,
        level_filter: str = "",
    ) -> pl.DataFrame:
        """
        Process the data for Puerto Rico Statistics Institute provided to JP.
        """
        df = self.pull_int_jp()

        if agriculture_filter:
            df = df.filter(pl.col("agri_prod") == 1)

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
        Process the data based on dynamic groupings without logic duplication.
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
                net_qty=pl.col("exports_qty") - pl.col("imports_qty"),
            )
        )

        return df

    def process_price(self, agriculture_filter: bool = False) -> pl.DataFrame:
        """
        Calculate rolling price statistics on top of monthly item structures.
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
        Convert the data to the correct units (kg).

        Parameters
        ----------
        df: pl.LazyFrame
            Data to convert.

        Returns
        -------
        pl.LazyFrame
            Converted data.
        """

        df = df.with_columns(pl.col("qty_1", "qty_2").fill_null(strategy="zero"))
        df = df.with_columns(
            conv_1=pl.when(pl.col("unit_1").str.to_lowercase() == "kg")
            .then(pl.col("qty_1") * 1)
            .when(pl.col("unit_1").str.to_lowercase() == "l")
            .then(pl.col("qty_1") * 1)
            .when(pl.col("unit_1").str.to_lowercase() == "doz")
            .then(pl.col("qty_1") / 0.756)
            .when(pl.col("unit_1").str.to_lowercase() == "m3")
            .then(pl.col("qty_1") * 1560)
            .when(pl.col("unit_1").str.to_lowercase() == "t")
            .then(pl.col("qty_1") * 907.185)
            .when(pl.col("unit_1").str.to_lowercase() == "kts")
            .then(pl.col("qty_1") * 1)
            .when(pl.col("unit_1").str.to_lowercase() == "pfl")
            .then(pl.col("qty_1") * 0.789)
            .when(pl.col("unit_1").str.to_lowercase() == "gm")
            .then(pl.col("qty_1") * 1000)
            .otherwise(pl.col("qty_1")),
            conv_2=pl.when(pl.col("unit_2").str.to_lowercase() == "kg")
            .then(pl.col("qty_2") * 1)
            .when(pl.col("unit_2").str.to_lowercase() == "l")
            .then(pl.col("qty_2") * 1)
            .when(pl.col("unit_2").str.to_lowercase() == "doz")
            .then(pl.col("qty_2") / 0.756)
            .when(pl.col("unit_2").str.to_lowercase() == "m3")
            .then(pl.col("qty_2") * 1560)
            .when(pl.col("unit_2").str.to_lowercase() == "t")
            .then(pl.col("qty_2") * 907.185)
            .when(pl.col("unit_2").str.to_lowercase() == "kts")
            .then(pl.col("qty_2") * 1)
            .when(pl.col("unit_2").str.to_lowercase() == "pfl")
            .then(pl.col("qty_2") * 0.789)
            .when(pl.col("unit_2").str.to_lowercase() == "gm")
            .then(pl.col("qty_2") * 1000)
            .otherwise(pl.col("qty_2")),
            qtr=pl.when(
                (pl.col("date").dt.month() >= 1) & (pl.col("date").dt.month() <= 3)
            )
            .then(1)
            .when((pl.col("date").dt.month() >= 4) & (pl.col("date").dt.month() <= 6))
            .then(2)
            .when((pl.col("date").dt.month() >= 7) & (pl.col("date").dt.month() <= 9))
            .then(3)
            .when((pl.col("date").dt.month() >= 10) & (pl.col("date").dt.month() <= 12))
            .then(4),
            fiscal_year=pl.when(pl.col("date").dt.month() > 6)
            .then(pl.col("date").dt.year() + 1)
            .otherwise(pl.col("date").dt.year())
            .alias("fiscal_year"),
            month=pl.col("date").dt.month(),
            year=pl.col("date").dt.year(),
        ).with_columns(qty=pl.col("conv_1") + pl.col("conv_2"))
        return df

    def filter_data(self, df: pl.DataFrame, filter: list) -> pl.DataFrame:
        """
        Filter the data based on the filter list.
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
