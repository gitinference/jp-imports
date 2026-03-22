from jp_imports import TradeUtils


def main() -> None:
    tu = TradeUtils()
    fips = ["PR", "VI", "HI"]
    for fip in fips:
        tu.pull_census_hts(exports=True, state=fip)
        tu.pull_census_naics(exports=True, state=fip)


if __name__ == "__main__":
    main()
