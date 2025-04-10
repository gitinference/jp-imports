from .data_process import DataTrade
import pandas as pd
import altair as alt


def gen_pie_chart(
    time_frame: str,
    year: str,
    month: str,
    qrt: str,
    graph_type: str = "imports",
):
    """
    Crea un gráfico de pastel basado en el período de tiempo seleccionado.

    Parámetros:
        time_frame (str): El período de tiempo ('monthly', 'qrt', 'yearly').
        data (pd.DataFrame): Datos con información de países y exportaciones/importaciones.

    Retorna:
        alt.Chart: Gráfico de pastel de Altair.
    """

    if time_frame not in ["monthly", "qrt", "yearly"]:
        raise ValueError(
            "El parámetro time_frame debe ser 'monthly', 'qrt' o 'yearly'."
        )

    dt = DataTrade()
    df = dt.process_int_jp(time_frame=time_frame, level="country")
    df = df.to_pandas()
    filtered_data = df[df["year"] == year]
    if time_frame == "monthly" and month:
        filtered_data = df[df["month"] == month]
    if time_frame == "qrt" and qrt:
        filtered_data = df[df["qrt"] == qrt]

    # Filter by year and sort by imports

    filtered_data = filtered_data.sort_values(by=[graph_type], ascending=False)
    # Take top 10 countries with most imports
    df_20 = filtered_data.iloc[:10]
    df_other = filtered_data.iloc[10:]
    # Sum the imports of the rest other countries for graphing purposes
    new_row = {"country_name": "Others", graph_type: df_other[graph_type].sum()}
    df_20 = pd.concat([df_20, pd.DataFrame([new_row])], ignore_index=True)

    pie_chart = (
        alt.Chart(df_20, title="Country %s in %d / %d" % (graph_type, 7, 11))
        .mark_arc()
        .encode(
            theta=alt.Theta(field=graph_type, type="quantitative"),
            color=alt.Color(field="country_name", title="Countries", type="nominal"),
        )
        .properties(width="container")
    )

    return pie_chart

