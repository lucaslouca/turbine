from implementation.model.concept import Concept
from implementation.model.price import Price
from implementation.model.data import Data
from implementation.model.ticker import Ticker
from connarchitecture.abstract_sender import AbstractSender
from connarchitecture.decorators import overrides
import implementation.database as db
from implementation.extractor_result import ExtractorResult
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from threading import Lock
from typing import List


class Sender(AbstractSender):
    _shared_db_conn = None
    _db_lock = Lock()
    _Session = None

    def __init__(self, name, **kwargs):
        AbstractSender.__init__(self, name, **kwargs)
        self._db_name = kwargs['db']

    def _init_database(self):
        engine = create_engine(f'sqlite:///{self._db_name}')
        Sender._Session = sessionmaker(bind=engine)
        db.Base.metadata.create_all(engine)

    @overrides(AbstractSender)
    def static_initialize(self):
        self.log("static init")
        self._init_database()

    @overrides(AbstractSender)
    def initialize(self):
        self.log("init")

    def _persist(self, data: List[Data]):
        session = Sender._Session()
        for d in data:
            ticker = d.get_ticker()
            db_ticker = session.query(Ticker.id).filter_by(symbol=ticker.symbol).first()
            if db_ticker:
                ticker.id = db_ticker.id

            if isinstance(d, Concept):
                db_data = session.query(Concept.id).filter_by(ticker_id=ticker.id, name=d.name, year=d.year).first()
                if db_data:
                    d.id = db_data.id

            elif isinstance(d, Price):
                db_data = session.query(Price.id).filter_by(ticker_id=ticker.id, date=d.date).first()
                if db_data:
                    d.id = db_data.id

            session.merge(d)
            session.commit()
        session.close()

    @overrides(AbstractSender)
    def process(self, result: List[ExtractorResult], poll_reference=None):
        for extractor_result in result:
            if extractor_result.result_list:
                c_width = 15 if poll_reference.concept else 0
                self.log(f"""Ticker: {poll_reference.ticker:<5} | Results: {len(extractor_result.result_list):<3} {('| Concept: {}'.format(poll_reference.concept) if poll_reference.concept else ''):<{c_width}} | Year: {poll_reference.year:<4}""")
                self._persist(extractor_result.result_list)

    @overrides(AbstractSender)
    def cleanup(self):
        self.log("cleanup")

    @overrides(AbstractSender)
    def static_cleanup(self):
        self.log("static cleanup")
