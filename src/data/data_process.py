from .data_pull import DataPull
from dateutil.relativedelta import relativedelta
import polars as pl
import ibis
import os


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
    ) -> ibis.expr.types.relations.Table:
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

        if "jptradedata" not in self.conn.list_tables():
            self.insert_int_jp(self.jp_data, self.agr_file)
        if int(self.conn.table("jptradedata").count().execute()) == 0:
            self.insert_int_jp(self.jp_data, self.agr_file)
        if datetime == "":
            df = self.conn.table("jptradedata")
        elif len(datetime.split("+")) == 2:
            times = datetime.split("+")
            start = times[0]
            end = times[1]
            df = self.conn.table("jptradedata")
            df = df.filter((df.date >= start) & (df.date <= end))
        elif len(datetime.split("+")) == 1:
            df = self.conn.table("jptradedata")
            df = df.filter(df.date == datetime)
        else:
            raise ValueError('Invalid time format. Use "date" or "start_date+end_date"')

        if agriculture_filter:
            hts = (
                self.conn.table("htstable")
                .select("id", "agri_prod")
                .rename(agr_id="id")
            )
            df = df.join(hts, df.hts_id == hts.agr_id)
            df = df.filter(df.agri_prod)

        if level == "hts":
            hts_table = self.conn.table("htstable")
            df_hts = hts_table.filter(hts_table.hts_code.startswith(level_filter))
            if df_hts.execute().empty:
                raise ValueError(f"Invalid HTS code: {level_filter}")
            hts_ids = df_hts["id"]

            df = df.filter(df["hts_id"].isin(hts_ids))
        elif level == "naics":
            naics_table = self.conn.table("naicstable")
            df_naics = naics_table.filter(
                naics_table.naics_code.startswith(level_filter)
            )
            if df_naics.execute().empty:
                raise ValueError(f"Invalid NAICS code: {level_filter}")
            naics_ids = df_naics["id"]

            df = df.filter(df["naics_id"].isin(naics_ids))
        elif level == "country":
            country_table = self.conn.table("countrytable")
            df_country = country_table.filter(
                country_table.cty_code.startswith(level_filter)
            )
            if df_country.execute().empty:
                raise ValueError(f"Invalid Country code: {level_filter}")
            country_ids = df_country["id"]

            df = df.filter(df["country_id"].isin(country_ids))

        units = self.conn.table("unittable")
        df = self.conversion(df, units)

        if group:
            # return self.process_cat(switch=switch)
            raise NotImplementedError("Grouping not implemented yet")
        else:
            return self.process_data(switch=switch, df=df)

    def process_int_org(
        self,
        level: str,
        time_frame: str,
        datetime: str = "",
        agriculture_filter: bool = False,
        group: bool = False,
        level_filter: str = "",
    ) -> ibis.expr.types.relations.Table:
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
        if "inttradedata" not in self.conn.list_tables():
            self.insert_int_org(self.org_data)
        if int(self.conn.table("inttradedata").count().execute()) == 0:
            self.insert_int_org(self.org_data)
        if datetime == "":
            df = self.conn.table("inttradedata")
        elif len(datetime.split("+")) == 2:
            times = datetime.split("+")
            start = times[0]
            end = times[1]
            df = self.conn.table("inttradedata")
            df = df.filter((df.date >= start) & (df.date <= end))
        elif len(datetime.split("+")) == 1:
            df = self.conn.table("inttradedata")
            df = df.filter(df.date == datetime)
        else:
            raise ValueError('Invalid time format. Use "date" or "start_date+end_date"')

        if agriculture_filter:
            hts = (
                self.conn.table("htstable")
                .select("id", "agri_prod")
                .rename(agr_id="id")
            )
            df = df.join(hts, df.hts_id == hts.agr_id)
            df = df.filter(df.agri_prod)

        if level == "hts":
            hts_table = self.conn.table("htstable")
            df_hts = hts_table.filter(hts_table.hts_code.startswith(level_filter))
            if df_hts.execute().empty:
                raise ValueError(f"Invalid HTS code: {level_filter}")
            hts_ids = df_hts["id"]

            df = df.filter(df["hts_id"].isin(hts_ids))
        elif level == "country":
            country_table = self.conn.table("countrytable")
            df_country = country_table.filter(
                country_table.cty_code.startswith(level_filter)
            )
            if df_country.execute().empty:
                raise ValueError(f"Invalid Country code: {level_filter}")
            country_ids = df_country["id"]

            df = df[df["country_id"].isin(country_ids)]

        units = self.conn.table("unittable")
        df = self.conversion(df, units)

        if group:
            # return self.process_cat(switch=switch)
            raise NotImplementedError("Grouping not implemented yet")
        else:
            return self.process_data(switch=switch, df=df)

    def process_data(
        self, switch: list, df: ibis.expr.types.relations.Table
    ) -> ibis.expr.types.relations.Table:
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
                df = self.filter_data(df, ["year"])
                df = df.mutate(year=df.year.fill_null(df.year_right))
                return df.select(
                    ["year", "imports", "exports", "qty_imports", "qty_exports"]
                )

            case ["yearly", "naics"]:
                df = self.filter_data(df, ["year", "naics_id"])
                df = df.mutate(
                    year=df.year.fill_null(df.year_right),
                    naics_id=df.naics_id.fill_null(df.naics_id_right),
                )
                naics = self.conn.table("naicstable")
                df = df.join(naics, df.naics_id == naics.id)
                return df.select(
                    [
                        "year",
                        "naics_id",
                        "naics_code",
                        "imports",
                        "exports",
                        "qty_imports",
                        "qty_exports",
                    ]
                )

            case ["yearly", "hts"]:
                df = self.filter_data(df, ["year", "hts_id"])
                df = df.mutate(
                    year=df.year.fill_null(df.year_right),
                    hts_id=df.hts_id.fill_null(df.hts_id_right),
                )
                hts = self.conn.table("htstable")
                df = df.join(hts, df.hts_id == hts.id)
                return df.select(
                    [
                        "year",
                        "hts_id",
                        "hts_code",
                        "imports",
                        "exports",
                        "qty_imports",
                        "qty_exports",
                    ]
                )

            case ["yearly", "country"]:
                df = self.filter_data(df, ["year", "country_id"])
                df = df.mutate(
                    year=df.year.fill_null(df.year_right),
                    country_id=df.country_id.fill_null(df.country_id_right),
                )
                countries = self.conn.table("countrytable")
                df = df.join(countries, df.country_id == countries.id)
                return df.select(
                    [
                        "year",
                        "country_id",
                        "cty_code",
                        "country_name",
                        "imports",
                        "exports",
                        "qty_imports",
                        "qty_exports",
                    ]
                )

            case ["fiscal", "total"]:
                df = self.filter_data(df, ["fiscal_year"])
                df = df.mutate(
                    fiscal_year=df.fiscal_year.fill_null(df.fiscal_year_right)
                )
                return df.select(
                    ["fiscal_year", "imports", "exports", "qty_imports", "qty_exports"]
                )

            case ["fiscal", "naics"]:
                df = self.filter_data(df, ["fiscal_year", "naics_id"])
                df = df.mutate(
                    fiscal_year=df.fiscal_year.fill_null(df.fiscal_year_right),
                    naics_id=df.naics_id.fill_null(df.naics_id_right),
                )
                naics = self.conn.table("naicstable")
                df = df.join(naics, df.naics_id == naics.id)
                return df.select(
                    [
                        "fiscal_year",
                        "naics_id",
                        "naics_code",
                        "imports",
                        "exports",
                        "qty_imports",
                        "qty_exports",
                    ]
                )

            case ["fiscal", "hts"]:
                df = self.filter_data(df, ["fiscal_year", "hts_id"])
                df = df.mutate(
                    fiscal_year=df.fiscal_year.fill_null(df.fiscal_year_right),
                    hts_id=df.hts_id.fill_null(df.hts_id_right),
                )
                hts = self.conn.table("htstable")
                df = df.join(hts, df.hts_id == hts.id)
                return df.select(
                    [
                        "fiscal_year",
                        "hts_id",
                        "hts_code",
                        "imports",
                        "exports",
                        "qty_imports",
                        "qty_exports",
                    ]
                )

            case ["fiscal", "country"]:
                df = self.filter_data(df, ["fiscal_year", "country_id"])
                df = df.mutate(
                    fiscal_year=df.fiscal_year.fill_null(df.fiscal_year_right),
                    country_id=df.country_id.fill_null(df.country_id_right),
                )
                countries = self.conn.table("countrytable")
                df = df.join(countries, df.country_id == countries.id)
                return df.select(
                    [
                        "fiscal_year",
                        "country_id",
                        "cty_code",
                        "country_name",
                        "imports",
                        "exports",
                        "qty_imports",
                        "qty_exports",
                    ]
                )

            case ["qrt", "total"]:
                df = self.filter_data(df, ["year", "qrt"])
                df = df.mutate(
                    year=df.year.fill_null(df.year_right),
                    qrt=df.qrt.fill_null(df.qrt_right),
                )
                return df.select(
                    ["year", "qrt", "imports", "exports", "qty_imports", "qty_exports"]
                )

            case ["qrt", "naics"]:
                df = self.filter_data(df, ["year", "qrt", "naics_id"])
                df = df.mutate(
                    year=df.year.fill_null(df.year_right),
                    qrt=df.qrt.fill_null(df.qrt_right),
                    naics_id=df.naics_id.fill_null(df.naics_id_right),
                )
                naics = self.conn.table("naicstable")
                df = df.join(naics, df.naics_id == naics.id)
                return df.select(
                    [
                        "year",
                        "qrt",
                        "naics_id",
                        "naics_code",
                        "imports",
                        "exports",
                        "qty_imports",
                        "qty_exports",
                    ]
                )

            case ["qrt", "hts"]:
                df = self.filter_data(df, ["year", "qrt", "hts_id"])
                df = df.mutate(
                    year=df.year.fill_null(df.year_right),
                    qrt=df.qrt.fill_null(df.qrt_right),
                    hts_id=df.hts_id.fill_null(df.hts_id_right),
                )
                hts = self.conn.table("htstable")
                df = df.join(hts, df.hts_id == hts.id)
                return df.select(
                    [
                        "year",
                        "qrt",
                        "hts_id",
                        "hts_code",
                        "imports",
                        "exports",
                        "qty_imports",
                        "qty_exports",
                    ]
                )

            case ["qrt", "country"]:
                df = self.filter_data(df, ["year", "qrt", "country_id"])
                df = df.mutate(
                    year=df.year.fill_null(df.year_right),
                    qrt=df.qrt.fill_null(df.qrt_right),
                    country_id=df.country_id.fill_null(df.country_id_right),
                )
                countries = self.conn.table("countrytable")
                df = df.join(countries, df.country_id == countries.id)
                return df.select(
                    [
                        "year",
                        "qrt",
                        "country_id",
                        "cty_code",
                        "country_name",
                        "imports",
                        "exports",
                        "qty_imports",
                        "qty_exports",
                    ]
                )

            case ["monthly", "total"]:
                df = self.filter_data(df, ["year", "month"])
                df = df.mutate(
                    year=df.year.fill_null(df.year_right),
                    month=df.month.fill_null(df.month_right),
                )
                return df.select(
                    [
                        "year",
                        "month",
                        "imports",
                        "exports",
                        "qty_imports",
                        "qty_exports",
                    ]
                )

            case ["monthly", "naics"]:
                df = self.filter_data(df, ["year", "month", "naics_id"])
                df = df.mutate(
                    year=df.year.fill_null(df.year_right),
                    month=df.month.fill_null(df.month_right),
                    naics_id=df.naics_id.fill_null(df.naics_id_right),
                )
                naics = self.conn.table("naicstable")
                df = df.join(naics, df.naics_id == naics.id)
                return df.select(
                    [
                        "year",
                        "month",
                        "naics_id",
                        "naics_code",
                        "imports",
                        "exports",
                        "qty_imports",
                        "qty_exports",
                    ]
                )

            case ["monthly", "hts"]:
                df = self.filter_data(df, ["year", "month", "hts_id"])
                df = df.mutate(
                    year=df.year.fill_null(df.year_right),
                    month=df.month.fill_null(df.month_right),
                    hts_id=df.hts_id.fill_null(df.hts_id_right),
                )
                hts = self.conn.table("htstable")
                df = df.join(hts, df.hts_id == hts.id)
                return df.select(
                    [
                        "year",
                        "month",
                        "hts_id",
                        "hts_code",
                        "imports",
                        "exports",
                        "qty_imports",
                        "qty_exports",
                    ]
                )

            case ["monthly", "country"]:
                df = self.filter_data(df, ["year", "month", "country_id"])
                df = df.mutate(
                    year=df.year.fill_null(df.year_right),
                    month=df.month.fill_null(df.month_right),
                    country_id=df.country_id.fill_null(df.country_id_right),
                )
                countries = self.conn.table("countrytable")
                df = df.join(countries, df.country_id == countries.id)
                return df.select(
                    [
                        "year",
                        "month",
                        "country_id",
                        "cty_code",
                        "country_name",
                        "imports",
                        "exports",
                        "qty_imports",
                        "qty_exports",
                    ]
                )

            case _:
                raise ValueError(f"Invalid switch: {switch}")

    def process_price(
        self, agriculture_filter: bool = False
    ) -> ibis.expr.types.relations.Table:
        max_date = self.conn.table("inttradedata").date.max().execute()
        start_date = max_date - relativedelta(years=1, months=1)
        df = self.process_int_org(
            time_frame="monthly",
            level="hts",
            agriculture_filter=agriculture_filter,
            datetime=f"{start_date}+{max_date}",
        )
        hts = self.conn.table("htstable").select("id", "hts_code").rename(hts_id="id")
        df = df.join(hts, "hts_id", how="left")
        df = df.mutate(
            date=ibis.date(
                df.year.cast("string") + "-" + df.month.cast("string") + "-01"
            ),
            qty_imports=df.qty_imports.substitute(0, 1),
            qty_exports=df.qty_exports.substitute(0, 1),
            hs4=df.hts_code[0:4],
        )
        df = df.group_by(["date", "hs4"]).aggregate(
            [
                df.imports.sum().name("imports"),
                df.exports.sum().name("exports"),
                df.qty_imports.sum().name("qty_imports"),
                df.qty_exports.sum().name("qty_exports"),
            ]
        )
        df = df.mutate(
            price_imports=df.imports / df.qty_imports,
            price_exports=df.exports / df.qty_exports,
        )
        df = df.mutate(
            moving_price_imports=df.price_imports.mean().over(
                range=(-ibis.interval(months=2), 0),
                group_by=df.hs4,
                order_by=df.date,
            ),
            moving_price_exports=df.price_exports.mean().over(
                range=(-ibis.interval(months=2), 0),
                group_by=df.hs4,
                order_by=df.date,
            ),
        )
        df = df.group_by("hs4").mutate(
            prev_year_imports=df.moving_price_imports.lag(12),
            prev_year_exports=df.moving_price_exports.lag(12),
        )
        df = df.mutate(
            pct_change_imports=ibis.cases(
                (
                    df.prev_year_imports != 0,
                    (df.moving_price_imports - df.prev_year_imports)
                    / df.prev_year_imports,
                ),
                else_=ibis.null(),
            ),
            pct_change_exports=ibis.cases(
                (
                    df.prev_year_exports != 0,
                    (df.moving_price_exports - df.prev_year_exports)
                    / df.prev_year_exports,
                ),
                else_=(ibis.null()),
            ),
        )
        df = df.mutate(
            moving_import_rank=ibis.cases(
                (
                    df.moving_price_imports.notnull(),
                    ibis.dense_rank().over(
                        order_by=df.moving_price_imports, group_by=df.date
                    ),
                ),
                else_=(ibis.null()),
            ),
            moving_export_rank=ibis.cases(
                (
                    df.moving_price_exports.notnull(),
                    ibis.dense_rank().over(
                        order_by=df.moving_price_exports, group_by=df.date
                    ),
                ),
                else_=(ibis.null()),
            ),
            pct_imports_rank=ibis.cases(
                (
                    df.pct_change_imports.notnull(),
                    ibis.dense_rank().over(
                        order_by=df.pct_change_imports, group_by=df.date
                    ),
                ),
                else_=(ibis.null()),
            ),
            pct_exports_rank=ibis.cases(
                (
                    df.pct_change_exports.notnull(),
                    ibis.dense_rank().over(
                        order_by=df.pct_change_exports, group_by=df.date
                    ),
                ),
                else_=(ibis.null()),
            ),
        )
        return df

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

    def filter_data(
        self, df: ibis.expr.types.relations.Table, filters: list
    ) -> ibis.expr.types.relations.Table:
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

        imports = (
            df.filter(df.trade_id == 1)
            .group_by(filters)
            .aggregate(
                [df.data.sum().name("imports"), df.qty.sum().name("qty_imports")]
            )
        )
        exports = (
            df.filter(df.trade_id == 2)
            .group_by(filters)
            .aggregate(
                [df.data.sum().name("exports"), df.qty.sum().name("qty_exports")]
            )
        )
        df = imports.join(exports, predicates=filters, how="outer")
        df = df.mutate(
            imports=df.imports.fill_null(0),
            exports=df.exports.fill_null(0),
            qty_imports=df.qty_imports.fill_null(0).round(2),
            qty_exports=df.qty_exports.fill_null(0).round(2),
        )
        df = df.mutate(
            net_exports=df.exports - df.imports, net_qty=df.qty_exports - df.qty_imports
        )
        return df

    def conversion(
        self,
        df: ibis.expr.types.relations.Table,
        units: ibis.expr.types.relations.Table,
    ) -> ibis.expr.types.relations.Table:
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

        df = df.join(units, df.unit1_id == units.id).rename(unit_1="unit_code")
        df = df.mutate(unit2_id=df.unit2_id.fill_null(df.unit1_id))
        df = df.join(units, df.unit2_id == units.id).rename(unit_2="unit_code")
        df = df.mutate(
            conv_1=ibis.cases(
                (df.unit_1 == "kg", df.qty_1 * 1),
                (df.unit_1 == "l", df.qty_1 * 1),
                (df.unit_1 == "doz", df.qty_1 / 0.756),
                (df.unit_1 == "m3", df.qty_1 * 907.1847),
                (df.unit_1 == "t", df.qty_1 * 907.1847),
                (df.unit_1 == "kts", df.qty_1 * 1),
                (df.unit_1 == "pfl", df.qty_1 * 0.789),
                (df.unit_1 == "gm", df.qty_1 * 0.001),
                else_=(df.qty_1),
            ),
            conv_2=ibis.cases(
                (df.unit_2 == "kg", df.qty_2 * 1),
                (df.unit_2 == "l", df.qty_2 * 1),
                (df.unit_2 == "doz", df.qty_2 / 0.756),
                (df.unit_2 == "m3", df.qty_2 * 1000),
                (df.unit_2 == "t", df.qty_2 * 1000),
                (df.unit_2 == "kts", df.qty_2 * 1),
                (df.unit_2 == "pfl", df.qty_2 * 0.789),
                (df.unit_2 == "gm", df.qty_2 * 0.001),
                else_=(df.qty_2),
            ),
            qrt=ibis.cases(
                ((df.date.month() >= 1) & (df.date.month() <= 3), 1),
                ((df.date.month() >= 4) & (df.date.month() <= 8), 2),
                ((df.date.month() >= 7) & (df.date.month() <= 9), 3),
                ((df.date.month() >= 10) & (df.date.month() <= 12), 4),
                else_=(0),
            ),
            fiscal_year=ibis.cases(
                (df.date.month() > 6, df.date.year() + 1), else_=(df.date.year())
            ),
        )
        df = df.mutate(
            qty=df.conv_1,
            year=df.date.year(),
            month=df.date.month(),
        )
        return df
