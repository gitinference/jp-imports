from ..data.data_process import DataTrade
import altair as alt


class DataViz(DataTrade):
    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        super().__init__(saving_dir, database_file, log_file)

    def gen_pie_chart(self, time_frame: str):
        """
        Crea un gráfico de pastel basado en el período de tiempo seleccionado.

        Parámetros:
            time_frame (str): El período de tiempo ('monthly', 'qrt', 'yearly').
            data (pd.DataFrame): Datos con información de países y exportaciones/importaciones.

        Retorna:
            alt.Chart: Gráfico de pastel de Altair.
        """

        # Verifica que el time_frame sea válido
        if time_frame not in ["monthly", "qrt", "yearly"]:
            raise ValueError(
                "El parámetro time_frame debe ser 'monthly', 'qrt' o 'yearly'."
            )

        df = self.process_int_jp(level="country", time_frame=time_frame)
        # Crear el gráfico de pastel
        pie_chart = (
            alt.Chart(df)
            .mark_arc()
            .encode(theta="value:Q", color="country:N", tooltip=["country", "value"])
            .properties(title=f"Distribución por país ({time_frame.capitalize()})")
        )

        return pie_chart

