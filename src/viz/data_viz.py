from ..data.data_process import DataTrade
import altair as alt


class DataViz(DataTrade):
    def __init__(
        self, saving_dir: str = "data/", database_url: str = "duckdb:///data.ddb"
    ):
        super().__init__(saving_dir, database_url)

    def gen_pie_chart(self, time_frame: str):
        df = self.process_int_jp(level="country", time_frame=time_frame)
        return df
