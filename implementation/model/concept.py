import implementation.database as db
from implementation.model.data import Data
from connarchitecture.decorators import overrides
from sqlalchemy import Column, String, Integer, Float, UniqueConstraint, ForeignKey
from sqlalchemy.orm import relationship


class Concept(Data):
    __tablename__ = db.TABLE_CONCEPTS
    id = Column(None, ForeignKey(f'{db.TABLE_DATA}.id'), primary_key=True)
    name = Column(String)
    year = Column(Integer)
    value = Column(Float)
    ticker_id = Column(Integer, ForeignKey(f'{db.TABLE_TICKERS}.id'))
    ticker = relationship('Ticker')

    __table_args__ = (UniqueConstraint('ticker_id', 'name', 'year', name='_ticker_id_name_year_uc'),)
    __mapper_args__ = {'polymorphic_identity': 'Concept'}

    def __init__(self, ticker, name: str, year: int, value: float):
        self.ticker = ticker
        self.name = name
        self.year = year
        self.value = value

    @overrides(Data)
    def get_ticker(self):
        return self.ticker

    def __repr__(self):
        return f'<Concept(name={self.name}, year={self.year}, value={self.value})>'
