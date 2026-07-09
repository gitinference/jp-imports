from jp_imports import JPTrade


def main() -> None:
    print(JPTrade().process_int_jp(time_frame="monthly", level="hts"))


if __name__ == "__main__":
    main()
