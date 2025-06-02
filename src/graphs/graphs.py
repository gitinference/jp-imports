import altair as alt
from ..data.data_process import DataTrade
import pandas as pd
import logging
import os
import webbrowser
import numpy as np

class DataGraph(DataTrade):
    def __init__(self):
        super().__init__()
    
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
        df1_imports = df1_imports.to_pandas()

        df1_imports["percent"] = (df1_imports["imports"] / df1_imports["imports"].sum() * 100).round(1)
        df1_imports["label"] = df1_imports["country"] + ": " + df1_imports["percent"].astype(str) + "%"

        base = alt.Chart(df1_imports).encode(
            theta=alt.Theta(field="imports", type="quantitative"),
            color=alt.Color(field="country", type="nominal"),
            tooltip=["country", "imports"],
            order=alt.Order(field="imports", sort='descending'),
        )

        pie = base.mark_arc().encode()

        pie = (pie).properties(
            width=700,
            height=500
        )

        if not third_dropdown:
            pie = pie.properties(
                title=f"Time: {frequency} / {second_dropdown}",
            ).configure_title(
                anchor='middle',
                color='black',
            )
        else:
            pie = pie.properties(
                title=f"Time: {frequency} / {second_dropdown} / {third_dropdown}",
            ).configure_title(
                anchor='middle',
                color='black',
            )

        pie.save('chart_date.html')
        webbrowser.open_new_tab('file://' + os.path.abspath('chart_date.html'))