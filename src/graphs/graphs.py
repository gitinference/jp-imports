import altair as alt
from ..data.data_process import DataTrade
import logging
import os
import webbrowser
import polars as pl
import pandas as pd
import numpy as np
import calendar

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

            month = int(second_dropdown)
            year = int(third_dropdown)
            
            start_of_month = f"{year}-{month:02d}-01"
            end_of_month = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}"
            
            datetime = f"{start_of_month}+{end_of_month}"
            
        elif frequency == "Quarterly":
            time_frame = 'qrt'

            if second_dropdown == '1':
                start_month = 1
                end_month = 3 
            elif second_dropdown == '2':
                start_month = 4
                end_month = 6
            elif second_dropdown == '3':
                start_month = 7
                end_month = 9
            elif second_dropdown == '4':
                start_month = 10
                end_month = 12

            year = int(third_dropdown)
            
            start_of_month = f"{year}-{start_month:02d}-01"
            end_of_month = f"{year}-{end_month:02d}-{calendar.monthrange(year, end_month)[1]}"
            
            datetime = f"{start_of_month}+{end_of_month}"

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

            month = int(second_dropdown)
            year = int(third_dropdown)
            
            start_of_month = f"{year}-{month:02d}-01"
            end_of_month = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}"
            
            datetime = f"{start_of_month}+{end_of_month}"

        elif frequency == "Quarterly":
            time_frame = 'qrt'

            if second_dropdown == 'Qrt 1':
                start_month = 1
                end_month = 3 
            elif second_dropdown == 'Qrt 2':
                start_month = 4
                end_month = 6
            elif second_dropdown == 'Qrt 3':
                start_month = 7
                end_month = 9
            elif second_dropdown == 'Qrt 4':
                start_month = 10
                end_month = 12

            year = int(third_dropdown)
            
            start_of_month = f"{year}-{start_month:02d}-01"
            end_of_month = f"{year}-{end_month:02d}-{calendar.monthrange(year, end_month)[1]}"
            
            datetime = f"{start_of_month}+{end_of_month}"

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
    
    def gen_hts_chart(
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
        elif frequency == "quarterly":
            new_frequency = frequency
            frequency = 'qrt'
        else:
            new_frequency = frequency

        hts_codes = DataTrade.process_int_jp(self, level_filter="", level="hts", time_frame=frequency)
        data, hts_codes = DataTrade.process_hts_data(self, hts_data, hts_codes, new_frequency, trade_type)

        x_axis = data[new_frequency]
        y_axis = data[trade_type]

        context = { "hts_codes": hts_codes, }

        frequency = frequency.capitalize()
        trade_type = trade_type.capitalize()
        title = f"Frequency: {frequency} | HTS Code: {level_filter} | Trade Type: {trade_type}"

        df = pd.DataFrame({ 'x': x_axis, 'y': y_axis })

        chart = alt.Chart(df).mark_line(point=True).encode(
            x=alt.X('x', axis=alt.Axis(title=None, format="~d", ), ),
            y=alt.Y('y', axis=alt.Axis(title=None, format='~s', )),
            tooltip=[
                alt.Tooltip('x', ),
                alt.Tooltip('y', )
            ]
        ).properties( title=title ).configure_view(
            fill='#e6f7ff'
        ).configure_axis(
            gridColor='white',
            grid=True
        ).properties( 
            width='container', height=200, 
        ).configure_title(
            anchor='start',     
            fontSize=16,         
            color='#333333',      
            offset=30           
        )
        return chart, context
    
    def gen_hts_ranking_chart(self, ):
        df = DataTrade.process_price(self)
        df.write_parquet("data/processed/moving.parquet")

        top_imports, last_imports, top_exports, last_exports = DataTrade.process_hts_ranking_data(self, df)

        # Create the top 20 exports chart
        export_top = alt.Chart(top_exports).mark_bar().encode(
            x=alt.X("moving_price_exports:Q", axis=alt.Axis(title="", format="~s")),
            y=alt.Y("hts_desc:N", sort="-x", title=""),
            color=alt.condition(
                alt.datum.moving_price_exports < 0,
                alt.value("red"),      
                alt.value("#504dff")       
            ),
            tooltip=["hs4:N", "moving_price_exports:Q"],
        ).properties(
            title="Top 20 Items by Export Rank",
            width='container',
            height=300
        ).configure_view(
            fill='#e6f7ff'
        ).configure_axis(
            gridColor='white',
            grid=True
        ).configure_title(
            anchor='start',     
            fontSize=16,         
            color='#333333',      
            offset=30           
        )

        # Create the last 20 exports chart
        export_bottom = alt.Chart(last_exports).mark_bar().encode(
            x=alt.X("moving_price_exports:Q", axis=alt.Axis(title="", format="~s")),
            y=alt.Y("hts_desc:N", sort="-x", title=""),
            color=alt.condition(
                alt.datum.moving_price_exports < 0,
                alt.value("red"),      
                alt.value("#504dff")       
            ),
            tooltip=["hs4:N", "moving_price_exports:Q"],
        ).properties(
            title="Bottom 20 Items by Export Rank",
            width='container',
            height=300
        ).configure_view(
            fill='#e6f7ff'
        ).configure_axis(
            gridColor='white',
            grid=True
        ).configure_title(
            anchor='start',     
            fontSize=16,         
            color='#333333',      
            offset=30           
        )

        # Create the top 20 imports chart
        import_top = alt.Chart(top_imports).mark_bar().encode(
            x=alt.X("moving_price_imports:Q", axis=alt.Axis(title="", format="~s")),
            y=alt.Y("hts_desc:N", sort="-x", title=""),
            color=alt.condition(
                alt.datum.moving_price_imports < 0,
                alt.value("red"),      
                alt.value("#504dff")       
            ),
            tooltip=["hs4:N", "moving_price_imports:Q"],
        ).properties(
            title="Top 20 Items by Import Rank",
            width='container',
            height=300
        ).configure_view(
            fill='#e6f7ff'
        ).configure_axis(
            gridColor='white',
            grid=True
        ).configure_title(
            anchor='start',     
            fontSize=16,         
            color='#333333',      
            offset=30           
        )

        # Create the bottom 20 imports chart
        import_bottom = alt.Chart(last_imports).mark_bar().encode(
            x=alt.X("moving_price_imports:Q", axis=alt.Axis(title="", format="~s")),
            y=alt.Y("hts_desc:N", sort="-x", title=""),
            color=alt.condition(
                alt.datum.moving_price_imports < 0,
                alt.value("red"),      
                alt.value("#504dff")       
            ),
            tooltip=["hs4:N", "moving_price_imports:Q"],
        ).properties(
            title="Bottom 20 Items by Import Rank",
            width='container',
            height=300
        ).configure_view(
            fill='#e6f7ff'
        ).configure_axis(
            gridColor='white',
            grid=True
        ).configure_title(
            anchor='start',     
            fontSize=16,         
            color='#333333',      
            offset=30           
        )

        return {
            "export_top": export_top,
            "export_bottom": export_bottom,
            "import_top": import_top,
            "import_bottom": import_bottom
        }
