import implementation.database as db
from sqlalchemy import Column, String, Integer, Float, UniqueConstraint


class Data(db.Base):
    __tablename__ = db.TABLE_DATA
    id = Column(Integer, primary_key=True)
    ticker = Column(String)
    concept = Column(String)
    year = Column(Integer)
    value = Column(Float)

    __table_args__ = (UniqueConstraint('ticker', 'concept', 'year', name='_ticker_concept_year_uc'),)

    def __init__(self, ticker: str, concept: str, year: int, value: float):
        self.ticker = ticker
        self.concept = concept
        self.year = year
        self.value = value

    def __repr__(self):
        return f'<Data(ticker={self.ticker}, concept={self.concept}, year={self.year}, value={self.value})>'
