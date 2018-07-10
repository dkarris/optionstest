
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.types import Numeric, DateTime, Float


Base = declarative_base()

class Volatility_storage(Base):
    ''' 
    Class is used to store the volatility calc data retrieved from csv files of MOEX
    Data structure is the same as in the source file except one additiona column:
    general_id_name which is added for simplicity - values from tickers like 'SI', "RI"
    will stored there without experation date parameter - easy for filtering later
    '''
    __tablename__ = 'volatility'

    id = Column(Integer, primary_key=True)
    name = Column(String(15))
    small_name = Column(String(15))
    timestamp = Column(DateTime)
    asset_price = Column(Numeric(precision=6))
    s = Column(Numeric(precision=6))
    a = Column(Numeric(precision=6))
    b = Column(Numeric(precision=6))
    c = Column(Numeric(precision=6))
    d = Column(Numeric(precision=6))
    e = Column(Numeric(precision=6))
    t = Column(Numeric(precision=6))
    general_id_name = Column(String(15))
    




