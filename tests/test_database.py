import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from src.database import add_and_update_records


Base = declarative_base()


class MockTable(Base):
    __tablename__ = 'mock_table'
    id = Column(Integer, primary_key=True)
    name = Column(String)


def test_add_update_records(mocker):
    # Create a mock database engine
    mock_engine = create_engine('sqlite:///:memory:')

    # Create a mock schema
    Base.metadata.create_all(mock_engine)

    # Create a mock session
    mock_session = sessionmaker(bind=mock_engine)()

    # Patch the sessionmaker to return the mock session
    mocker.patch('sqlalchemy.orm.session.sessionmaker', return_value=mock_session)

    # Mock the query result
    existing_data = [MockTable(id=1, name='Test1'), MockTable(id=2, name='Test2'),
                     MockTable(id=3, name='Test3'), MockTable(id=4, name='Test4')]
    mock_session.add_all(existing_data)
    mock_session.commit()

    new_data = pd.DataFrame(columns=['id', 'name'], data=[[1, 'Banana'], [2, 'Test2'],
                                                          [5, 'angel'], [6, 'fallen']])
    add_and_update_records(mock_engine, MockTable, 'id', new_data)
    expected_result = [MockTable(id=1, name='Banana'), MockTable(id=2, name='Test2'),
                       MockTable(id=3, name='Test3'), MockTable(id=4, name='Test4'),
                       MockTable(id=5, name='angel'), MockTable(id=6, name='fallen')]

    # Perform the test query
    result = mock_session.query(MockTable).all()
    print(result)

    # Assert the result
    assert result == expected_result

    # Close the mock session
    mock_session.close()
    mock_engine.dispose()
