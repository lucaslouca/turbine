import implementation.database as db
from sqlalchemy import Column, Integer, String
from abc import abstractmethod


class Data(db.Base):
    __tablename__ = db.TABLE_DATA
    id = Column(Integer, primary_key=True)
    data_type = Column(String(32), nullable=False)
    __mapper_args__ = {'polymorphic_on': data_type}

    @abstractmethod
    def get_ticker(self):
        pass
