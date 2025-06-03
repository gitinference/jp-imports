import altair as alt
from ..data.data_process import DataTrade
import logging
import os
import webbrowser
import polars as pl
import pandas as pd

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
        df1_imports = DataTrade.process_imports_exports(self, df1_imports, "imports").to_pandas()

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
        df1_exports = DataTrade.process_imports_exports(self, df1_exports, "exports").to_pandas()

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
    
    def gen_hts_graph(
        self,
        level: str = "",
        agriculture_filter: bool = False,
        group: bool = False,
        level_filter: str = "",
        frequency: str = "",
        trade_type: str = "",
    ):
        hts_data = DataTrade.process_int_jp(
            self,
            level=level,
            time_frame=frequency,
            agriculture_filter=agriculture_filter,
            group=group,
            level_filter=level_filter,
        )
        if frequency == "yearly":
            new_frequency = "year"
        elif frequency == "monthly":
            new_frequency = "month"
        elif frequency == "fiscal":
            new_frequency = "fiscal_year"
        else:
            new_frequency = frequency

        hts_codes = DataTrade.process_int_jp(self, level_filter="", level="hts", time_frame="yearly")
        data, hts_codes = DataTrade.process_hts_data(self, hts_data, hts_codes, new_frequency, trade_type)

        x_axis = data[new_frequency]
        y_axis = data[trade_type]

        context = { "hts_codes": hts_codes, }

        frequency = frequency.capitalize()
        trade_type = trade_type.capitalize()
        title = f"Frequency: {frequency} | HTS Code: {level_filter} | Trade Type: {trade_type}"

        df = pd.DataFrame({ 'x': x_axis, 'y': y_axis })

        chart = alt.Chart(df).mark_line(point=True).encode(
            x='x',
            y='y'
        ).properties( title=title )

        return chart, context