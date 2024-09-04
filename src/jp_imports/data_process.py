from .data_pull import DataPull
import polars as pl
import pandas as pd
import numpy as np

class DataProcess(DataPull):

    def __init__(self, saving_dir:str, instance:str, state_code:str="PR", debug:bool=False):
        self.saving_dir = saving_dir
        self.state_code = state_code
        self.debug = debug
        self.instance = instance

        super().__init__(saving_dir=self.saving_dir, state_code=self.state_code, instance=self.instance, debug=self.debug)

    def process_int_jp(self, time:str, types:str) -> pl.DataFrame:

        df = pl.read_parquet(self.saving_dir + "raw/jp_instance.parquet")
        switch = [time, types]

        df = df.with_columns(conv_1=pl.when(pl.col("unit_1").str.to_lowercase() == "kg").then(pl.col("qty_1") * 1)
                                        .when(pl.col("unit_1").str.to_lowercase() == "l").then(pl.col("qty_1") * 1)
                                        .when(pl.col("unit_1").str.to_lowercase() == "doz").then(pl.col("qty_1") / 0.756)
                                        .when(pl.col("unit_1").str.to_lowercase() == "m3").then(pl.col("qty_1") * 1560)
                                        .when(pl.col("unit_2").str.to_lowercase() == "t").then(pl.col("qty_1") * 907.185)
                                        .when(pl.col("unit_1").str.to_lowercase() == "kts").then(pl.col("qty_1") * 1)
                                        .when(pl.col("unit_1").str.to_lowercase() == "pfl").then(pl.col("qty_1") * 0.789)
                                        .when(pl.col("unit_1").str.to_lowercase() == "gm").then(pl.col("qty_1") * 1000).otherwise(None),

                            conv_2=pl.when(pl.col("unit_2").str.to_lowercase() == "kg").then(pl.col("qty_2") * 1)
                                        .when(pl.col("unit_2").str.to_lowercase() == "l").then(pl.col("qty_2") * 1)
                                        .when(pl.col("unit_2").str.to_lowercase() == "doz").then(pl.col("qty_2") / 0.756)
                                        .when(pl.col("unit_2").str.to_lowercase() == "m3").then(pl.col("qty_2") * 1560)
                                        .when(pl.col("unit_2").str.to_lowercase() == "t").then(pl.col("qty_2") * 907.185)
                                        .when(pl.col("unit_2").str.to_lowercase() == "kts").then(pl.col("qty_2") * 1)
                                        .when(pl.col("unit_2").str.to_lowercase() == "pfl").then(pl.col("qty_2") * 0.789)
                                        .when(pl.col("unit_2").str.to_lowercase() == "gm").then(pl.col("qty_2") * 1000)
                                        .otherwise(None).alias("converted_qty_2"),

                            qrt=pl.when((pl.col("Month") >= 1) & (pl.col("Month") <= 3)).then(1)
                                        .when((pl.col("Month") >= 4) & (pl.col("Month") <= 8)).then(2)
                                        .when((pl.col("Month") >= 7) & (pl.col("Month") <= 9)).then(3)
                                        .when((pl.col("Month") >= 10) & (pl.col("Month") <= 12)).then(4))

        df = df.rename({"Year": "year", "Month": "month", "Country": "country", "Commodity_Code": "hs"})
        df = df.with_columns(hs=pl.col("hs").cast(pl.String).str.zfill(10))
        df = df.filter(pl.col("naics") != "RETURN")
        return self.process_data(df, switch)


    def process_data(self, df:pl.DataFrame, switch:list) -> pl.DataFrame:

        match switch:
            case ["yearly", "naics"]:
                df = self.filter_data(df, ["year", "naics"])

                df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                    naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics")))

                df = df.select(pl.col("*").exclude("year_right", "naics_right"))

                df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "naics")
            case ["yearly", "hs"]:
                df = self.filter_data(df, ["year", "hs"])

                df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                            hs=pl.when(pl.col("hs").is_null()).then(pl.col("hs_right")).otherwise(pl.col("hs")))
                df = df.select(pl.col("*").exclude("year_right", "hs_right"))

                df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "hs")
            case ["qrt", "naics"]:
                df = self.filter_data(df, ["year", "qrt", "naics"])

                df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                    qrt=pl.when(pl.col("qrt").is_null()).then(pl.col("qrt_right")).otherwise(pl.col("qrt")),
                                    naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics"))
                                            )
                df = df.select(pl.col("*").exclude("year_right", "qrt_right", "naics_right"))

                df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "qrt", "naics")

            case ["qrt", "hs"]:
                df = self.filter_data(df, ["year", "qrt", "hs"])

                df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                    qrt=pl.when(pl.col("qrt").is_null()).then(pl.col("qrt_right")).otherwise(pl.col("qrt")),
                                    hs=pl.when(pl.col("hs").is_null()).then(pl.col("hs_right")).otherwise(pl.col("hs"))
                                    )
                df = df.select(pl.col("*").exclude("year_right", "qrt_right", "hs_right"))
                df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "qrt", "hs")

            case ["monthly", "naics"]:
                df = self.filter_data(df, ["year", "month", "naics"])

                df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                    month=pl.when(pl.col("month").is_null()).then(pl.col("month_right")).otherwise(pl.col("month")),
                                    naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics"))
                                    )
                df = df.select(pl.col("*").exclude("year_right", "month_right", "naics_right"))
                df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "month", "naics")

            case ["monthly", "hs"]:
                df = self.filter_data(df, ["year", "month", "hs"])

                df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                    month=pl.when(pl.col("month").is_null()).then(pl.col("month_right")).otherwise(pl.col("month")),
                                    hs=pl.when(pl.col("hs").is_null()).then(pl.col("hs_right")).otherwise(pl.col("hs"))
                                    )
                df = df.select(pl.col("*").exclude("year_right", "month_right", "hs_right"))
                df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "month", "hs")
            case ["yearly", "country"]:
                df = self.filter_data(df, ["year", "country"])

                df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                    country=pl.when(pl.col("country").is_null()).then(pl.col("country_right")).otherwise(pl.col("country"))
                                    )
                df = df.select(pl.col("*").exclude("year_right", "country_right"))

                df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "country")

            case ["qrt", "country"]:
                df = self.filter_data(df, ["year", "qrt", "country"])

                df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                    qrt=pl.when(pl.col("qrt").is_null()).then(pl.col("qrt_right")).otherwise(pl.col("qrt")),
                                    country=pl.when(pl.col("country").is_null()).then(pl.col("country_right")).otherwise(pl.col("country"))
                                    )
                df = df.select(pl.col("*").exclude("year_right", "qrt_right", "country_right"))

                return df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "qrt", "country")

            case ["monthly", "country"]:
                df = self.filter_data(df, ["year", "month", "country"])

                df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                    month=pl.when(pl.col("month").is_null()).then(pl.col("month_right")).otherwise(pl.col("month")),
                                    country=pl.when(pl.col("country").is_null()).then(pl.col("country_right")).otherwise(pl.col("country"))
                                    )
                df = df.select(pl.col("*").exclude("year_right", "month_right", "country_right"))

                df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "month", "country")

        return df

    def filter_data(self, df:pl.DataFrame, filter:list) -> pl.DataFrame:
        imports = df.filter(pl.col("Trade") == "i").group_by(filter).agg(
            pl.sum("data").alias("exports")).sort(filter)
        exports = df.filter(pl.col("Trade") == "e").group_by(filter).agg(
            pl.sum("data").alias("imports")).sort(filter)

        return imports.join(exports, on=filter, how="full", validate="1:1")


    def convertions(self, row:pd.Series) -> float:
            if row['unit_1'] == 'kg':
                return row['qty'] * 1
            elif row['unit_1'] == 'l':
                return row['qty'] * 1
            elif row['unit_1'] == 'doz':
                return row['qty'] / 0.756
            elif row['unit_1'] =='m3':
                return row['qty'] * 1560
            elif row['unit_1'] == 't':
                return row['qty'] * 907.185
            elif row['unit_1'] == 'kts':
                return row['qty'] * 1
            elif row['unit_1'] == 'pfl':
                return row['qty'] * 0.789
            elif row['unit_1'] == 'gm':
                return row['qty'] * 1000
            else:
                return np.nan
