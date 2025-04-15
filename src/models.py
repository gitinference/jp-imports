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
            agri_prod INTEGER,
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
            agri_prod INTEGER,
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


def init_com_trade_data_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "comtradetable" (
            refYear VARCHAR(255),
            refMonth VARCHAR(255),
            reporterCode VARCHAR(255),
            reporterDesc VARCHAR(255),
            flowCode VARCHAR(255),
            flowDesc VARCHAR(255),
            partnerDesc VARCHAR(255),
            classificationCode VARCHAR(255),
            cmdCode VARCHAR(255),
            cmdDesc VARCHAR(255),
            cifvalue FLOAT,
            fobvalue FLOAT,
            primaryValue FLOAT,
            netWgt FLOAT
            );
        """
    )
