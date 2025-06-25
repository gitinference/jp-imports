import os
from datetime import datetime as dt

import polars as pl

from .data_pull import DataPull


class DataTrade(DataPull):
    """
    Data processing class for the various data sources in DataPull.
    """

    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        """
        Initialize the DataProcess class.

        Parameters
        ----------
        saving_dir: str
            Directory to save the data.
        debug: bool
            Will print debug information in the console if True.

        Returns
        -------
        None
        """
        super().__init__(saving_dir, database_file, log_file)
        self.jp_data = os.path.join(self.saving_dir, "raw/jp_data.parquet")
        self.org_data = os.path.join(self.saving_dir, "raw/org_data.parquet")
        self.agr_file = os.path.join(self.saving_dir, "external/code_agr.json")

    def process_int_jp(
        self,
        level: str,
        time_frame: str,
        datetime: str = "",
        agriculture_filter: bool = False,
        group: bool = False,
        level_filter: str = "",
    ) -> pl.DataFrame:
        """
        Process the data for Puerto Rico Statistics Institute provided to JP.

        Parameters
        ----------
        time_frame: str
            Time period to process the data. The options are "yearly", "qrt", and "monthly".
        level: str
            Type of data to process. The options are "total", "naics", "hs", and "country".
        group: bool
            Group the data by the classification. (Not implemented yet)
        level_filter:
            search and filter for the data for the given level

        Returns
        -------
        ibis.expr.types.relations.Table
            Returns a lazy ibis table that can be further process. See the Ibis documentations
            to see available outputs
        """

        switch = [time_frame, level]

        df = self.insert_int_jp()

        if agriculture_filter:
            df = df.filter(pl.col("agri_prod") == 1)

        if level == "hts":
            df = df.filter(pl.col("hts_code").str.starts_with(level_filter))
            if df.is_empty():
                raise ValueError(f"Invalid HTS code: {level_filter}")
        elif level == "naics":
            df = df.filter(pl.col("naics").str.starts_with(level_filter))
            if df.is_empty():
                raise ValueError(f"Invalid NAICS code: {level_filter}")
        elif level == "country":
            df = df.filter(pl.col("hts_code").str.starts_with(level_filter))
            if df.is_empty():
                raise ValueError(f"Invalid Name code: {level_filter}")

        if datetime == "":
            df = df
        elif len(datetime.split("+")) == 2:
            times = datetime.split("+")
            start = times[0]
            end = times[1]

            start_date = dt.strptime(start, "%Y-%m-%d")
            end_date = dt.strptime(end, "%Y-%m-%d")

            df = df.filter(
                (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
            )
        elif len(datetime.split("+")) == 1:
            df = df.filter(pl.col("date").dt.year() == int(datetime))
        else:
            raise ValueError('Invalid time format. Use "date" or "start_date+end_date"')

        df = self.conversion(df)

        if group:
            # return self.process_cat(switch=switch)
            raise NotImplementedError("Grouping not implemented yet")
        else:
            return self.process_data(switch=switch, base=df)

    def process_int_org(
        self,
        level: str,
        time_frame: str,
        datetime: str = "",
        agriculture_filter: bool = False,
        group: bool = False,
        level_filter: str = "",
    ) -> pl.DataFrame:
        """
        Process the data from Puerto Rico Statistics Institute.

        Parameters
        ----------
        time: str
            Time period to process the data. The options are "yearly", "qrt", and "monthly".
            ex. "2020-01-01+2021-01-01" - for yearly data
                "2020-01-01+2020-03-01" - for quarterly data
                "2020-01-01" - for monthly data
        types: str
            The type of data to process. The options are "total", "hts", and "country".
        agg: str
            Aggregation of the data. The options are "monthly", "yearly", "fiscal", "total" and "qtr".
        group: bool
            Group the data by the classification. (Not implemented yet)
        update: bool
            Update the data from the source.
        filter: str
            Filter the data based on the type. ex. "NAICS code" or "HTS code".

        Returns
        -------
        pl.LazyFrame
            Processed data. Requires df.collect() to view the data.
        """
        switch = [time_frame, level]

        if time_frame == "naics":
            raise ValueError(
                "NAICS data is not available for Puerto Rico Statistics Institute."
            )
        if datetime == "":
            df = self.insert_int_org()
        elif len(datetime.split("+")) == 2:
            times = datetime.split("+")
            start = times[0]
            end = times[1]
            df = self.insert_int_org()
            df = df.filter((pl.col("date") >= start) & (pl.col("date") <= end))
        elif len(datetime.split("+")) == 1:
            df = self.insert_int_org()
            df = df.filter(pl.col("date").dt.year() == int(datetime))
        else:
            raise ValueError('Invalid time format. Use "date" or "start_date+end_date"')

        if agriculture_filter:
            df = df.filter(pl.col("agri_prod") == 1)

        if level == "hts":
            df = df.filter(pl.col("hts_code").str.starts_with(level_filter))
            if df.is_empty():
                raise ValueError(f"Invalid HTS code: {level_filter}")
        elif level == "country":
            df = df.filter(pl.col("hts_code").str.starts_with(level_filter))
            if df.is_empty():
                raise ValueError(f"Invalid Country code: {level_filter}")
        df = self.conversion(df)

        if group:
            # return self.process_cat(switch=switch)
            raise NotImplementedError("Grouping not implemented yet")
        else:
            return self.process_data(switch=switch, base=df)

    def process_data(self, switch: list, base: pl.DataFrame) -> pl.DataFrame:
        """
        Process the data based on the switch. Used for the process_int_jp and process_int_org methods
            to determine the aggregation of the data.

        Parameters
        ----------
        switch: list
            List of strings to determine the aggregation of the data based on the time and type from
            the process_int_jp and process_int_org methods.
        base: pl.lazyframe
            The pre-procesed and staderized data to process. This data comes from the process_int_jp and process_int_org methods.

        Returns
        -------
        pl.LazyFrame
            Processed data. Requires df.collect() to view the data.
        """

        match switch:
            case ["yearly", "total"]:
                df = self.filter_data(base, ["year"])
                df = df.with_columns(
                    year=pl.when(pl.col("year").is_null())
                    .then(pl.col("year_right"))
                    .otherwise(pl.col("year"))
                )
                df = df.select(pl.col("*").exclude("year_right"))
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("year")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["yearly", "naics"]:
                df = self.filter_data(base, ["year", "naics"])
                df = df.with_columns(
                    year=pl.when(pl.col("year").is_null())
                    .then(pl.col("year_right"))
                    .otherwise(pl.col("year")),
                    naics=pl.when(pl.col("naics").is_null())
                    .then(pl.col("naics_right"))
                    .otherwise(pl.col("naics")),
                )
                df = df.select(pl.col("*").exclude("year_right", "naics_right"))
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("year", "naics")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["yearly", "hts"]:
                df = self.filter_data(base, ["year", "hts_code"])
                df = df.with_columns(
                    year=pl.when(pl.col("year").is_null())
                    .then(pl.col("year_right"))
                    .otherwise(pl.col("year")),
                    hts_code=pl.when(pl.col("hts_code").is_null())
                    .then(pl.col("hts_code_right"))
                    .otherwise(pl.col("hts_code")),
                )
                df = df.select(pl.col("*").exclude("year_right", "hts_code_right"))
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("year", "hts_code")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["yearly", "country"]:
                df = self.filter_data(base, ["year", "country", 'hts_code'])
                df = df.with_columns(
                    year=pl.when(pl.col("year").is_null())
                    .then(pl.col("year_right"))
                    .otherwise(pl.col("year")),
                    country=pl.when(pl.col("country").is_null())
                    .then(pl.col("country_right"))
                    .otherwise(pl.col("country")),
                    hts_code=pl.when(pl.col("hts_code").is_null())
                    .then(pl.col("hts_code_right"))
                    .otherwise(pl.col("hts_code")),
                )
                df = df.select(pl.col("*").exclude("year_right", "country_right", "hts_code_right"))
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("year", "country")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["fiscal", "total"]:
                df = self.filter_data(base, ["fiscal_year"])
                df = df.with_columns(
                    fiscal_year=pl.when(pl.col("fiscal_year").is_null())
                    .then(pl.col("fiscal_year_right"))
                    .otherwise(pl.col("fiscal_year"))
                )
                df = df.select(pl.col("*").exclude("fiscal_year_right"))
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("fiscal_year")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["fiscal", "naics"]:
                df = self.filter_data(base, ["fiscal_year", "naics"])
                df = df.with_columns(
                    fiscal_year=pl.when(pl.col("fiscal_year").is_null())
                    .then(pl.col("fiscal_year_right"))
                    .otherwise(pl.col("fiscal_year")),
                    naics=pl.when(pl.col("naics").is_null())
                    .then(pl.col("naics_right"))
                    .otherwise(pl.col("naics")),
                )
                df = df.select(pl.col("*").exclude("fiscal_year_right", "naics_right"))
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("fiscal_year", "naics")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["fiscal", "hts"]:
                df = self.filter_data(base, ["fiscal_year", "hts_code"])
                df = df.with_columns(
                    fiscal_year=pl.when(pl.col("fiscal_year").is_null())
                    .then(pl.col("fiscal_year_right"))
                    .otherwise(pl.col("fiscal_year")),
                    hts_code=pl.when(pl.col("hts_code").is_null())
                    .then(pl.col("hts_code_right"))
                    .otherwise(pl.col("hts_code")),
                )
                df = df.select(
                    pl.col("*").exclude("fiscal_year_right", "hts_code_right")
                )
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("fiscal_year", "hts_code")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["fiscal", "country"]:
                df = self.filter_data(base, ["fiscal_year", "country", 'hts_code'])
                df = df.with_columns(
                    fiscal_year=pl.when(pl.col("fiscal_year").is_null())
                    .then(pl.col("fiscal_year_right"))
                    .otherwise(pl.col("fiscal_year")),
                    country=pl.when(pl.col("country").is_null())
                    .then(pl.col("country_right"))
                    .otherwise(pl.col("country")),
                    hts_code=pl.when(pl.col("hts_code").is_null())
                    .then(pl.col("hts_code_right"))
                    .otherwise(pl.col("hts_code")),
                )
                df = df.select(
                    pl.col("*").exclude("fiscal_year_right", "country_right", 'hts_code_right')
                )
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("fiscal_year", "country")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["qrt", "total"]:
                df = self.filter_data(base, ["year", "qrt"])
                df = df.with_columns(
                    year=pl.when(pl.col("year").is_null())
                    .then(pl.col("year_right"))
                    .otherwise(pl.col("year")),
                    qrt=pl.when(pl.col("qrt").is_null())
                    .then(pl.col("qrt_right"))
                    .otherwise(pl.col("qrt")),
                )
                df = df.select(pl.col("*").exclude("year_right", "qrt_right"))
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("year", "qrt")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["qrt", "naics"]:
                df = self.filter_data(base, ["year", "qrt", "naics"])
                df = df.with_columns(
                    year=pl.when(pl.col("year").is_null())
                    .then(pl.col("year_right"))
                    .otherwise(pl.col("year")),
                    qrt=pl.when(pl.col("qrt").is_null())
                    .then(pl.col("qrt_right"))
                    .otherwise(pl.col("qrt")),
                    naics=pl.when(pl.col("naics").is_null())
                    .then(pl.col("naics_right"))
                    .otherwise(pl.col("naics")),
                )
                df = df.select(
                    pl.col("*").exclude("year_right", "qrt_right", "naics_right")
                )
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("year", "qrt", "naics")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["qrt", "hts"]:
                df = self.filter_data(base, ["year", "qrt", "hts_code"])
                df = df.with_columns(
                    year=pl.when(pl.col("year").is_null())
                    .then(pl.col("year_right"))
                    .otherwise(pl.col("year")),
                    qrt=pl.when(pl.col("qrt").is_null())
                    .then(pl.col("qrt_right"))
                    .otherwise(pl.col("qrt")),
                    hts_code=pl.when(pl.col("hts_code").is_null())
                    .then(pl.col("hts_code_right"))
                    .otherwise(pl.col("hts_code")),
                )
                df = df.select(
                    pl.col("*").exclude("year_right", "qrt_right", "hts_code_right")
                )
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("year", "qrt", "hts_code")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["qrt", "country"]:
                df = self.filter_data(base, ["year", "qrt", "country", 'hts_code'])
                df = df.with_columns(
                    year=pl.when(pl.col("year").is_null())
                    .then(pl.col("year_right"))
                    .otherwise(pl.col("year")),
                    qrt=pl.when(pl.col("qrt").is_null())
                    .then(pl.col("qrt_right"))
                    .otherwise(pl.col("qrt")),
                    country=pl.when(pl.col("country").is_null())
                    .then(pl.col("country_right"))
                    .otherwise(pl.col("country")),
                    hts_code=pl.when(pl.col("hts_code").is_null())
                    .then(pl.col("hts_code_right"))
                    .otherwise(pl.col("hts_code")),
                )
                df = df.select(
                    pl.col("*").exclude("year_right", "qrt_right", "country_right", 'hts_code_right')
                )
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("year", "qrt", "country")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["monthly", "total"]:
                df = self.filter_data(base, ["year", "month"])
                df = df.with_columns(
                    year=pl.when(pl.col("year").is_null())
                    .then(pl.col("year_right"))
                    .otherwise(pl.col("year")),
                    month=pl.when(pl.col("month").is_null())
                    .then(pl.col("month_right"))
                    .otherwise(pl.col("month")),
                )
                df = df.select(pl.col("*").exclude("year_right", "month_right"))
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("year", "month")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["monthly", "naics"]:
                df = self.filter_data(base, ["year", "month", "naics"])
                df = df.with_columns(
                    year=pl.when(pl.col("year").is_null())
                    .then(pl.col("year_right"))
                    .otherwise(pl.col("year")),
                    month=pl.when(pl.col("month").is_null())
                    .then(pl.col("month_right"))
                    .otherwise(pl.col("month")),
                    naics=pl.when(pl.col("naics").is_null())
                    .then(pl.col("naics_right"))
                    .otherwise(pl.col("naics")),
                )
                df = df.select(
                    pl.col("*").exclude("year_right", "month_right", "naics_right")
                )
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("year", "month", "naics")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["monthly", "hts"]:
                df = self.filter_data(base, ["year", "month", "hts_code"])
                df = df.with_columns(
                    year=pl.when(pl.col("year").is_null())
                    .then(pl.col("year_right"))
                    .otherwise(pl.col("year")),
                    month=pl.when(pl.col("month").is_null())
                    .then(pl.col("month_right"))
                    .otherwise(pl.col("month")),
                    hts_code=pl.when(pl.col("hts_code").is_null())
                    .then(pl.col("hts_code_right"))
                    .otherwise(pl.col("hts_code")),
                )
                df = df.select(
                    pl.col("*").exclude("year_right", "month_right", "hts_code_right")
                )
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("year", "month", "hts_code")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case ["monthly", "country"]:
                df = self.filter_data(base, ["year", "month", "country", 'hts_code'])
                df = df.with_columns(
                    year=pl.when(pl.col("year").is_null())
                    .then(pl.col("year_right"))
                    .otherwise(pl.col("year")),
                    month=pl.when(pl.col("month").is_null())
                    .then(pl.col("month_right"))
                    .otherwise(pl.col("month")),
                    country=pl.when(pl.col("country").is_null())
                    .then(pl.col("country_right"))
                    .otherwise(pl.col("country")),
                    hts_code=pl.when(pl.col("hts_code").is_null())
                    .then(pl.col("hts_code_right"))
                    .otherwise(pl.col("hts_code")),
                )
                df = df.select(
                    pl.col("*").exclude("year_right", "month_right", "country_right", 'hts_code_right')
                )
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "imports_qty", "exports_qty"
                    ).fill_null(strategy="zero")
                ).sort("year", "month", "country")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))
                df = df.with_columns(
                    net_qty=pl.col("exports_qty") - pl.col("imports_qty")
                )
                return df

            case _:
                raise ValueError(f"Invalid switch: {switch}")

    def process_price(self, agriculture_filter: bool = False) -> pl.DataFrame:
        df = self.process_int_org(
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

        df = df.with_columns(date=pl.datetime(pl.col("year"), pl.col("month"), 1))

        # Sort the DataFrame by the date column
        df = df.sort("date")

        # Now you can safely use group_by_dynamic
        result = df.with_columns(
            pl.col("price_imports")
            .rolling_mean(window_size=3, min_periods=1)
            .over("hs4")
            .alias("moving_price_imports"),
            pl.col("price_exports")
            .rolling_mean(window_size=3, min_periods=1)
            .over("hs4")
            .alias("moving_price_exports"),
            pl.col("price_imports")
            .rolling_std(window_size=3, min_periods=1)
            .over("hs4")
            .alias("moving_price_imports_std"),
            pl.col("price_exports")
            .rolling_std(window_size=3, min_periods=1)
            .over("hs4")
            .alias("moving_price_exports_std"),
        )
        results = result.with_columns(
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
        )

        results = results.select(
            pl.col("*").exclude(
                "year_right",
                "month_right",
                "imports_right",
                "exports_right",
                "price_imports_right",
                "price_exports_right",
                "exports_qty_right",
                "imports_qty_right",
            )
        )

        # Assuming 'results' already has the necessary columns and is sorted by date and hs4
        results = results.with_columns(
            pl.col("moving_price_imports")
            .pct_change()
            .over("date", "hs4")
            .alias("pct_change_imports")
        ).sort(by=["date", "hs4"])

        # To get the percentage change for the same month of the previous year
        # First, create a column for the previous year's value
        results = results.with_columns(
            pl.when(
                pl.col("date").dt.year() > 1
            )  # Ensure there's a previous year to compare
            .then(pl.col("moving_price_imports").shift(12))  # Shift by 12 months
            .otherwise(None)
            .alias("prev_year_imports"),
        )
        results = results.with_columns(
            pl.when(
                pl.col("date").dt.year() > 1
            )  # Ensure there's a previous year to compare
            .then(pl.col("moving_price_exports").shift(12))  # Shift by 12 months
            .otherwise(None)
            .alias("prev_year_exports"),
        )
        results = results.with_columns(
            pl.when(
                pl.col("date").dt.year() > 1
            )  # Ensure there's a previous year to compare
            .then(pl.col("rank_imports").shift(12))  # Shift by 12 months
            .otherwise(None)
            .alias("prev_year_rank_imports"),
        )
        results = results.with_columns(
            pl.when(
                pl.col("date").dt.year() > 1
            )  # Ensure there's a previous year to compare
            .then(pl.col("rank_exports").shift(12))  # Shift by 12 months
            .otherwise(None)
            .alias("prev_year_rank_exports")
        )

        # Now calculate the percentage change
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

    def process_cat(self, df: pl.DataFrame, switch: list):
        match switch:
            case ["yearly", "total"]:
                df = self.filter_data(df, ["year", "naics"])
                df = df.with_columns(
                    year=pl.when(pl.col("year").is_null())
                    .then(pl.col("year_right"))
                    .otherwise(pl.col("year")),
                    naics=pl.when(pl.col("naics").is_null())
                    .then(pl.col("naics_right"))
                    .otherwise(pl.col("naics")),
                )
                df = df.select(pl.col("*").exclude("year_right", "naics_right"))
                df = df.with_columns(
                    pl.col(
                        "imports", "exports", "qty_imports", "qty_exports"
                    ).fill_null(strategy="zero")
                ).sort("year", "naics")
                df = df.with_columns(net_exports=pl.col("exports") - pl.col("imports"))

    def filter_data(self, df: pl.DataFrame, filter: list) -> pl.DataFrame:
        """
        Filter the data based on the filter list.

        Parameters
        ----------
        df: pl.DataFrame
            Data to filter.
        filter: List
            List of columns to filter the data.

        Returns
        -------
        pl.DataFrame
            data to be filtered.
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
        return imports.join(exports, on=filter, how="full", validate="1:1")

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
            qrt=pl.when(
                (pl.col("date").dt.month() >= 1) & (pl.col("date").dt.month() <= 3)
            )
            .then(1)
            .when((pl.col("date").dt.month() >= 4) & (pl.col("date").dt.month() <= 8))
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

    def process_imports_exports(self, df: pl.DataFrame, graph_type: str):
        df = (
            df.group_by("country")
            .agg(pl.col(graph_type).sum().alias(graph_type))
            .with_columns(rank=pl.col(graph_type).rank("dense", descending=True))
            .with_columns(
                pl.when(pl.col("rank") <= 20)
                .then(pl.col("country"))
                .otherwise(pl.lit("Others"))
                .alias("country")
            )
            .group_by("country")
            .agg(pl.col(graph_type).sum())
            .sort(graph_type, descending=True)
        )

        df = df.with_columns(
            (pl.col(graph_type) / df[graph_type].sum() * 100)
            .round(1)
            .alias("percent_num"),
        )
        df = df.with_columns(pl.format("{}%", pl.col("percent_num")).alias("percent"))
        return df

    def process_hts_data(
        self,
        data: pl.DataFrame,
        hts_codes: pl.DataFrame,
        new_frequency: str,
        trade_type: str,
    ):
        hts_codes = hts_codes.with_columns(
            hts_code_first4=pl.col("hts_code").str.slice(0, 4)
        )
        hts_codes = (
            hts_codes.select(pl.col("hts_code_first4").unique()).to_series().to_list()
        )
        hts_codes = sorted(hts_codes)

        data = data.sort(new_frequency)

        if new_frequency == 'qrt':
            data = data.with_columns(
                (pl.col("year").cast(pl.String) + '-q' + pl.col("qrt").cast(pl.String)).alias("time_period")
            )
        elif new_frequency == 'month':
            data = data.with_columns(
                (pl.col("year").cast(pl.String) + '-' + pl.col("month").cast(pl.String)).alias("time_period")
            )
        else:
            data = data.with_columns(
                pl.col(new_frequency).cast(pl.String).alias("time_period")
            )

        data = data.sort(['time_period'])
        data = data.group_by(['time_period']).agg(pl.col(trade_type).sum().alias(trade_type))
        
        data = data[['time_period', trade_type]]
        return data, hts_codes

    def process_hts_ranking_data(self, df: pl.DataFrame):
        df = df.fill_null(0).fill_nan(0)
        last_month = df.select(pl.col("date").max()).item()

        hts_desc = self.process_hts_desc()
        hts_desc_clean = hts_desc.unique(subset=["hs4"], keep="first")
        df = df.join(hts_desc_clean, on="hs4", how="left", validate="m:m")

        df_last_month_imports = df.filter(
            (pl.col("date") == last_month)
            & (pl.col("rank_imports_change_year_over_year") != 0)
        )
        df_last_month_exports = df.filter(
            (pl.col("date") == last_month)
            & (pl.col("rank_exports_change_year_over_year") != 0)
        )

        df_imports_sorted = df_last_month_imports.sort(
            "rank_imports_change_year_over_year", descending=False
        )
        df_exports_sorted = df_last_month_exports.sort(
            "rank_exports_change_year_over_year", descending=False
        )

        top_imports = df_imports_sorted.head(20)
        last_imports = df_imports_sorted.tail(20)

        top_exports = df_exports_sorted.head(20)
        last_exports = df_exports_sorted.tail(20)

        return top_imports, last_imports, top_exports, last_exports
    
    def process_hts_desc(self, ) -> pl.DataFrame:
        df = pl.read_excel(f"{self.saving_dir}raw/hts_4_cats.xlsx", sheet_id=1).rename({
            "HTS_4": "hs4",
            "HTS_desc": "hts_desc"
        })
        df = df.group_by(pl.col("hs4", "hts_desc")).agg([])
        return df
