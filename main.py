from src.data.data_process import DataTrade
from src.data.data_viz import gen_pie_chart

def main() -> None:
    d = DataTrade()
    print(d.process_int_jp(level="naics", time_frame="yearly").execute())
    # gen_pie_chart(time_frame="monthly", year=2011, month=7)



if __name__ == "__main__":
    main()
