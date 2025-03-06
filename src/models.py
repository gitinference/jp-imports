import duckdb


def get_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def init_int_trade_data_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)

    # Create sequence for primary keys
    conn.sql("DROP SEQUENCE IF EXISTS int_trade_data_sequence;")
    conn.sql("CREATE SEQUENCE int_trade_data_sequence START 1;")

    # Create IntTradeData table
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "inttradedata" (
            id INTEGER PRIMARY KEY DEFAULT nextval('int_trade_data_sequence'),
            trade_id INTEGER,
            hts_code TEXT,
            hts_short_desc TEXT,
            hts_long_desc TEXT,
            agri_prod BOOLEAN
            cty_code TEXT,
            country_name TEXT,
            data BIGINT DEFAULT 0,
            unit1_id INTEGER REFERENCES unittable(id),
            qty_1 BIGINT DEFAULT 0,
            unit2_id INTEGER REFERENCES unittable(id),
            qty_2 BIGINT DEFAULT 0,
            date TIMESTAMP
        );
        """
    )


def init_jp_trade_data_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)

    # Create sequence for primary key
    conn.sql("DROP SEQUENCE IF EXISTS jp_trade_data_sequence;")
    conn.sql("CREATE SEQUENCE jp_trade_data_sequence START 1;")

    # Create JPTradeData table
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "jptradedata" (
            id INTEGER PRIMARY KEY DEFAULT nextval('jp_trade_data_sequence'),
            trade_id INTEGER,
            hts_code TEXT,
            hts_short_desc TEXT,
            hts_long_desc TEXT,
            agri_prod BOOLEAN,
            cty_code TEXT,
            country_name TEXT,
            district_code TEXT,
            district_desc TEXT
            sitc_code TEXT,
            sitc_short_desc TEXT,
            sitc_long_desc TEXT
            naics_code TEXT,
            naics_description TEXT
            data INTEGER DEFAULT 0,
            end_use_i INTEGER,
            end_use_e INTEGER,
            unit_code1 TEXT
            qty_1 BIGINT DEFAULT 0,
            unit_code2 TEXT
            qty_2 BIGINT DEFAULT 0,
            date TIMESTAMP
        );
        """
    )