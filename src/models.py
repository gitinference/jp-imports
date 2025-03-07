import duckdb


def get_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def init_int_trade_data_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)

    # Create IntTradeData table
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "inttradedata" (
            trade_id INTEGER,
            hts_code TEXT,
            hts_desc TEXT,
            agri_prod BOOLEAN,
            country TEXT,
            data BIGINT DEFAULT 0,
            unit_1 TEXT,
            qty_1 BIGINT DEFAULT 0,
            unit_2 TEXT,
            qty_2 BIGINT DEFAULT 0,
            date TIMESTAMP
        );
        """
    )


def init_jp_trade_data_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)

    # Create JPTradeData table
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "jptradedata" (
            date TIMESTAMP,
            country TEXT,
            trade_id INTEGER,
            agri_prod BOOLEAN,
            hts_code TEXT,
            hts_desc TEXT,
            data BIGINT DEFAULT 0,
            sitc TEXT,
            naics TEXT,
            unit_1 TEXT,
            qty_1 BIGINT DEFAULT 0,
            unit_2 TEXT,
            qty_2 BIGINT DEFAULT 0,
        );
        """
    )
