import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, Binary, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker

DBFILE = 'test_orm.sqlite'


class SessionContext:

    def __init__(self, connector_str=None):
        connector_str = connector_str or 'sqlite:///' + DBFILE
        self.engine = create_engine(connector_str, echo=True)
        self.session_fac = sessionmaker(bind=self.engine)

    @contextmanager
    def __call__(self, session=None):
        if session is not None:
            yield session
            return
        session = self.session_fac()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


make_session = SessionContext()


def base_test():
    if os.path.isfile(DBFILE):
        os.remove(DBFILE)
    Base = declarative_base()

    class TabTestA(Base):
        __tablename__ = 'test_a'

        id_field = Column(Integer, primary_key=True, autoincrement=True)
        tfield = Column(String)


    Base.metadata.create_all(make_session.engine)


def test_creation():
    if os.path.isfile(DBFILE):
        os.remove(DBFILE)
    Base = declarative_base()
    Base.id_column = None

    class TabTestA(Base):
        __tablename__ = 'test_a'
        id_column = 'id_field'

        id_field = Column(Integer, primary_key=True, autoincrement=True)
        tfield = Column(String)

    Base.metadata.create_all(make_session.engine)

    row = TabTestA()
    row.tfield = 'abc'

    with make_session() as session:
        session.add(row)
        session.flush()
        session.refresh(row)
        print(getattr(row, row.id_column), row.tfield)


def test_creation_2():
    if os.path.isfile(DBFILE):
        os.remove(DBFILE)

    class SubBase:
        id_column = None

        @classmethod
        def get_id(cls, id, session):
            return session.query(cls).filter(getattr(cls, cls.id_column) == id).first()

    Base = declarative_base(cls=SubBase)

    class TabTestA(Base):
        __tablename__ = 'test_a'
        id_column = 'id_field'

        id_field = Column(Integer, primary_key=True, autoincrement=True)
        tfield = Column(String)

    Base.metadata.create_all(make_session.engine)

    row = TabTestA()
    row.tfield = 'abc'

    with make_session() as session:
        session.add(row)
        session.flush()
        session.refresh(row)
        print(getattr(row, row.id_column), row.tfield)

    with make_session() as session:
        row = TabTestA.get_id(1, session)
        print(row)
        print(getattr(row, TabTestA.id_column), row.tfield)
        row = TabTestA.get_id(2, session)
        print(row is None)


def test_creation_3():
    if os.path.isfile(DBFILE):
        os.remove(DBFILE)

    class SubBase:
        id_column = None

        @classmethod
        def get_id(cls, id, session):
            return session.query(cls).filter(getattr(cls, cls.id_column) == id).first()

    Base = declarative_base(cls=SubBase)

    class TabTestA(Base):
        __tablename__ = 'test_a'
        id_column = 'id_field'

        id_field = Column(Integer, primary_key=True, autoincrement=True)
        tfield = Column(String)

    Base.metadata.create_all(make_session.engine)

    row = TabTestA(tfield='abc')

    with make_session() as session:
        session.add(row)
        session.flush()
        session.refresh(row)
        print(getattr(row, row.id_column), row.tfield)

    with make_session() as session:
        row = TabTestA.get_id(1, session)
        print(row)
        print(getattr(row, TabTestA.id_column), row.tfield)
        row = TabTestA.get_id(2, session)
        print(row is None)


def test_fk_1():
    if os.path.isfile(DBFILE):
        os.remove(DBFILE)

    Base = declarative_base()

    class TabTestA(Base):
        __tablename__ = 'test_a'
        id_field = Column(Integer, primary_key=True, autoincrement=True)
        tfield = Column(String)

    class TabTestB(Base):
        __tablename__ = 'test_b'
        id_field = Column(Integer, primary_key=True, autoincrement=True)
        fk_id = Column(Integer, ForeignKey('test_a.id_field'))
        test_a = relationship('TabTestA', back_populates='test_b')

    TabTestA.test_b = relationship('TabTestB', order_by=TabTestB.id_field, back_populates="test_a")

    Base.metadata.create_all(make_session.engine)

    row = TabTestA(tfield='abc')
    row_b_1 = TabTestB(fk_id=1)
    row_b_2 = TabTestB(fk_id=1)

    with make_session() as session:
        session.add(row)
        session.add(row_b_1)
        session.add(row_b_2)
        session.flush()
        session.refresh(row)
        session.refresh(row_b_1)
        session.refresh(row_b_2)
        print(row.id_field, row.tfield, row.test_b)
        print(row_b_1.id_field, row_b_1.test_a)
        print(row_b_2.id_field, row_b_2.test_a)


if __name__ == '__main__':
    # base_test()
    # test_creation_2()
    test_fk_1()
