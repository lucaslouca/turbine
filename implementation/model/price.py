import implementation.database as db
from implementation.model.data import Data
from connarchitecture.decorators import overrides
from sqlalchemy import Column, Date, Integer, Float, UniqueConstraint, ForeignKey
from sqlalchemy.orm import relationship


class Price(Data):
    __tablename__ = db.TABLE_PRICES
    id = Column(None, ForeignKey(f'{db.TABLE_DATA}.id'), primary_key=True)
    date = Column(Date)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    adj_close = Column(Float)
    volume = Column(Float)
    ticker_id = Column(Integer, ForeignKey(f'{db.TABLE_TICKERS}.id'))
    ticker = relationship('Ticker')
    __table_args__ = (UniqueConstraint('ticker_id', 'date', name='_ticker_id_date_uc'),)
    __mapper_args__ = {'polymorphic_identity': 'Price'}

    def __init__(self, ticker, date, open: float, high: float, low: float, close: float, adj_close: float, volume: float):
        self.ticker = ticker
        self.date = date
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.adj_close = adj_close
        self.volume = volume

    @overrides(Data)
    def get_ticker(self):
        return self.ticker

    def __repr__(self):
        return f'<Price(date={self.date}, open={self.open}, high={self.high}, low={self.low}, close={self.close}, adj_close={self.adj_close}, volume={self.volume})>'
