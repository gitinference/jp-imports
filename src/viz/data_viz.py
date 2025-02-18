from ..data.data_process import DataTrade
import altair as alt


class DataViz(DataTrade):
    def __init__(
        self, saving_dir: str = "data/", database_url: str = "duckdb:///data.ddb"
    ):
        super().__init__(saving_dir, database_url)


    def gen_pie_chart(time_frame: str, data: pd.DataFrame):
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
        raise ValueError("El parámetro time_frame debe ser 'monthly', 'qrt' o 'yearly'.")

    # Filtra los datos según el período de tiempo seleccionado
    if time_frame == "monthly":
        filtered_data = data[data["month"] == 1]  # Puedes cambiar el mes aquí
    elif time_frame == "qrt":
        filtered_data = data[data["quarter"] == 1]  # Cambia el trimestre si es necesario
    else:
        filtered_data = data[data["year"] == 2024]  # Ajusta el año fiscal

    # Si después de filtrar no hay datos, mostrar un mensaje en la terminal
    if filtered_data.empty:
        print("No hay datos disponibles para el período seleccionado.")
        return None

    # Crear el gráfico de pastel
    pie_chart = alt.Chart(filtered_data).mark_arc().encode(
        theta="value:Q",
        color="country:N",
        tooltip=["country", "value"]
    ).properties(title=f"Distribución por país ({time_frame.capitalize()})")

    return pie_chart