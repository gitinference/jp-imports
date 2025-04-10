from sqlmodel import Field, Session, SQLModel
from sqlalchemy import BigInteger, Column
from datetime import datetime
from typing import Optional

class JPTradeData(SQLModel, table=True):
    id: int = Field(primary_key=True)
    date: datetime = Field(default_factory=datetime.now)
    trade_id: int | None = Field(default=None, foreign_key="tradetable.id")
    hts_id: int | None = Field(default=1, foreign_key="htstable.id")
    country_id: int | None = Field(default=None, foreign_key="countrytable.id")
    district_id: int | None = Field(default=None, foreign_key="districttable.id")
    sitc_id: int | None = Field(default=None, foreign_key="sitctable.id")
    naics_id: int | None = Field(default=None, foreign_key="naicstable.id")
    data: int = Field(default=0)
    end_use_i: Optional[int] = Field(default=None)
    end_use_e: Optional[int] = Field(default=None)
    unit1_id: int | None = Field(default=None, foreign_key="unittable.id")
    qty_1: Optional[int] = Field(default=0)
    unit2_id: int | None = Field(default=None, foreign_key="unittable.id")
    qty_2: Optional[int] = Field(default=0)

class IntTradeData(SQLModel, table=True):
    id: int = Field(primary_key=True)
    date: datetime = Field(default_factory=datetime.now)
    trade_id: Optional[int] = Field(default=None, foreign_key="tradetable.id")
    hts_id: Optional[int] = Field(default=None, foreign_key="htstable.id")
    country_id: Optional[int] = Field(default=None, foreign_key="countrytable.id")
    data: int = Field(default=0, sa_column=Column(BigInteger))  # Use BigInteger for the data field
    unit1_id: Optional[int] = Field(default=None, foreign_key="unittable.id")
    qty_1: Optional[int] = Field(default=0, sa_column=Column(BigInteger))
    unit2_id: Optional[int] = Field(default=None, foreign_key="unittable.id")
    qty_2: Optional[int] = Field(default=0, sa_column=Column(BigInteger))

class CountryTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    cty_code : str
    country_name: str

class HTSTable(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    hts_code: str
    hts_short_desc: str
    hts_long_desc: str
    agri_prod: bool

class SITCTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    sitc_code: str
    sitc_short_desc: str
    sitc_long_desc: str

class NAICSTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    naics_code: str
    naics_description: str

class TradeTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    trade: str

class DistrictTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    district_code: str
    district_desc: str

class UnitTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    unit_code: str

def create_trade_tables(engine):
    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)
    create_trade(engine)

def create_trade(engine):
    imports = TradeTable(id=1, trade="Imports")
    exports = TradeTable(id=2, trade="Exports")
    hts = HTSTable(id=1, hts_code="N/A", hts_short_desc="N/A", hts_long_desc="N/A", agri_prod=True)
    with Session(engine) as session:
        session.add_all([imports, exports])
        session.commit()