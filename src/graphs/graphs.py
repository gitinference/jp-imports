import altair as alt
from ..data.data_process import DataTrade
import logging
import os
import webbrowser
import polars as pl

class DataGraph(DataTrade):
    def __init__(self):
        super().__init__()

        self.color_palette = [
            "#0051a9", "#048da8", "#7463e3", "#fb8072", "#80b1d3",
            "#fb9f36", "#FD4B3E", "#395902", "#d35094", "#bc80bd",
            "#74d85d", "#1793f8", "#9f40a9", "#957DAD", "#9F383F",
            "#F595B2", "#054DD3", "#FB8C63", "#05CC83", "#592D74",
            "#B76F3F", "#c2b71c", "#F31528", "#64A101", "#2DAB7D",
            "#F9A024", "#61A3BB", "#A7A72D", "#CD9300", "#181873"
        ]
    
    def gen_imports_chart(
        self, 
        level: str,
        time_frame: str,
        agriculture_filter: bool = False,
        group: bool = False,
        level_filter: str = "",
        datetime: str = "",
        frequency: str = "",
        second_dropdown: str = "",
        third_dropdown: str = "",
    ):
        if frequency == "Yearly":
            time_frame = 'yearly'
            datetime = second_dropdown
        elif frequency == "Monthly":
            time_frame = 'monthly'
            datetime = second_dropdown-third_dropdown
        elif frequency == "Quarterly":
            time_frame = 'qrt'
            datetime = second_dropdown-third_dropdown

        if not frequency and not second_dropdown:
            frequency = "Yearly"
            second_dropdown = 2009
        
        df1_imports = DataTrade.process_int_jp(
            self,
            level=level,
            time_frame=time_frame,
            agriculture_filter=agriculture_filter,
            group=group,
            level_filter=level_filter,
            datetime=datetime
        )

        df1_imports = df1_imports.group_by("country").agg(
            pl.col("imports").sum().alias("imports")
        ).with_columns(
            rank = pl.col("imports").rank('dense', descending=True)
        ).with_columns(
            pl.when(pl.col("rank") <= 20)
            .then(pl.col("country"))
            .otherwise(pl.lit("Others"))
            .alias("country")
        ).group_by("country").agg(
            pl.col("imports").sum()
        ).sort("imports", descending=True)

        df1_imports = df1_imports.to_pandas()
        df1_imports["percent_num"] = df1_imports["imports"] / df1_imports["imports"].sum() * 100
        df1_imports["percent"] = df1_imports["percent_num"].round(1).astype(str) + '%'

        base = alt.Chart(df1_imports).encode(
            theta=alt.Theta(field="imports", type="quantitative").stack(True),
            color=alt.Color(
                scale=alt.Scale(
                    domain=df1_imports["country"].to_list(),
                    range=self.color_palette
                ),
                field="country", 
                type="nominal",
                legend=alt.Legend(
                    title="Country",
                    labelFontSize=12,
                    titleFontSize=14,
                    symbolType="square",
                    symbolSize=100
                )
            ),
            tooltip=["country", "imports"],
            order=alt.Order(field="imports", sort='descending'),
        )

        pie = base.mark_arc(outerRadius=200).encode()
        text = base.mark_text(radius=220, size=10).encode(
            text="percent:N",
        ).transform_filter( alt.datum.percent_num >= 1.0 )

        pie = (pie + text).properties( width=700, height=500, )

        if not third_dropdown:
            pie = pie.properties(
                title=f"Time: {frequency} / {second_dropdown}",
            ).configure_title( anchor='middle', color='black', )
        else:
            pie = pie.properties(
                title=f"Time: {frequency} / {second_dropdown} / {third_dropdown}",
            ).configure_title( anchor='middle', color='black', )
        return pie

    def gen_exports_chart(
        self, 
        level: str,
        time_frame: str,
        agriculture_filter: bool = False,
        group: bool = False,
        level_filter: str = "",
        datetime: str = "",
        frequency: str = "",
        second_dropdown: str = "",
        third_dropdown: str = "",
    ):
        if frequency == "Yearly":
            time_frame = 'yearly'
            datetime = second_dropdown
        elif frequency == "Monthly":
            time_frame = 'monthly'
            datetime = second_dropdown-third_dropdown
        elif frequency == "Quarterly":
            time_frame = 'qrt'
            datetime = second_dropdown-third_dropdown

        if not frequency and not second_dropdown:
            frequency = "Yearly"
            second_dropdown = 2009
        
        df1_exports = DataTrade.process_int_jp(
            self,
            level=level,
            time_frame=time_frame,
            agriculture_filter=agriculture_filter,
            group=group,
            level_filter=level_filter,
            datetime=datetime
        )

        df1_exports = df1_exports.group_by("country").agg(
            pl.col("exports").sum().alias("exports")
        ).with_columns(
            rank = pl.col("exports").rank('dense', descending=True)
        ).with_columns(
            pl.when(pl.col("rank") <= 30)
            .then(pl.col("country"))
            .otherwise(pl.lit("Others"))
            .alias("country")
        ).group_by("country").agg(
            pl.col("exports").sum()
        ).sort("exports", descending=True)

        df1_exports = df1_exports.to_pandas()
        df1_exports["percent_num"] = df1_exports["exports"] / df1_exports["exports"].sum() * 100
        df1_exports["percent"] = df1_exports["percent_num"].round(1).astype(str) + '%'

        base = alt.Chart(df1_exports).encode(
            theta=alt.Theta(field="exports", type="quantitative").stack(True),
            color=alt.Color(
                scale=alt.Scale(
                    domain=df1_exports["country"].to_list(),
                    range=self.color_palette
                ),
                field="country", 
                type="nominal",
                legend=alt.Legend(
                    title="Country",
                    labelFontSize=12,
                    titleFontSize=14,
                    symbolType="square",
                    symbolSize=100
                )
            ),
            tooltip=["country", "exports"],
            order=alt.Order(field="exports", sort='descending'),
        )

        pie = base.mark_arc(outerRadius=200).encode()
        text = base.mark_text(radius=220, size=10).encode(
            text="percent:N"
        ).transform_filter( alt.datum.percent_num >= 1.0 )

        pie = (pie + text).properties( width=700, height=500, )

        if not third_dropdown:
            pie = pie.properties(
                title=f"Time: {frequency} / {second_dropdown}",
            ).configure_title( anchor='middle', color='black',)
        else:
            pie = pie.properties(
                title=f"Time: {frequency} / {second_dropdown} / {third_dropdown}",
            ).configure_title( anchor='middle', color='black',)
        return pie