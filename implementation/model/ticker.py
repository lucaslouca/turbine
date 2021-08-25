import implementation.database as db
from implementation.model.concept import Concept
from sqlalchemy import Column, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship
import os


class Ticker(db.Base):
    __tablename__ = db.TABLE_TICKERS
    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    concepts = relationship('Concept', back_populates='ticker')

    __table_args__ = (UniqueConstraint('symbol', name='_symbol_uc'),)

    def __init__(self, symbol):
        self.symbol = symbol

    def __hash__(self):
        return hash(self.symbol)

    def __eq__(self, other):
        return isinstance(other, Ticker) and self.symbol == other.symbol

    def __str__(self):
        return f"{self.symbol}"

    def __repr__(self):
        return f'<Ticker(symbol={self.symbol})>'
