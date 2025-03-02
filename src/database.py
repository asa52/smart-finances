from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, \
    Engine, inspect, Float, DateTime, Date
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
from sqlalchemy.sql import text
import pandas as pd
from contextlib import contextmanager

database_path = 'test.db'
engine = create_engine(f'sqlite:///{database_path}', echo=True)
Base = declarative_base()

MAX_CATEGORY_LENGTH = 25
INCOME_CATEGORIES = ['Salary', 'Passive', 'Gift', 'Reward', 'Other Active',
                     'Bonus']
EXPENSE_CATEGORIES = [['Music', 'Entertainment'], ['Movies', 'Entertainment'],
                      ['Sports', 'Entertainment'], ['Games', 'Entertainment'],
                      ['Dining out', 'Food & drink'], ['Groceries', 'Food & drink'],
                      ['Liquor', 'Food & drink'], ['Services', 'Home'],
                      ['Furniture', 'Home'], ['Household supplies', 'Home'],
                      ['Maintenance', 'Home'], ['Mortgage', 'Home'],
                      ['Pets', 'Home'], ['Rent', 'Home'],
                      ['Electronics', 'Home'], ['Gifts', 'Life'],
                      ['Clothing', 'Life'], ['Taxes', 'Life'],
                      ['Insurance', 'Life'], ['Medical expenses', 'Life'],
                      ['Parking', 'Transportation'], ['Bus/train', 'Transportation'],
                      ['Bicycle', 'Transportation'], ['Plane', 'Transportation'],
                      ['Taxi', 'Transportation'], ['Car', 'Transportation'],
                      ['Gas/fuel', 'Transportation'], ['Hotel', 'Transportation'],
                      ['Cleaning', 'Utilities'], ['Heat/gas', 'Utilities'],
                      ['Trash', 'Utilities'], ['TV/Phone/Internet', 'Utilities'],
                      ['Electricity', 'Utilities'], ['Water', 'Utilities'],
                      ['Other', 'General'], ['General', 'General'],
                      ['Education', 'Life'], ['Life - Other', 'Life'],
                      ['Home - Other', 'Home'],
                      ['Entertainment - Other', 'Entertainment'],
                      ['Food and drink - Other', 'Food & drink'],
                      ['Transportation - Other', 'Transportation']]
EXPENSE_GROUPS = ['Entertainment', 'Food & drink', 'Home', 'Life',
                  'Transportation', 'Utilities', 'General']


class Category(Base):
    __abstract__ = True
    category = Column(String(MAX_CATEGORY_LENGTH), primary_key=True,
                      nullable=False, unique=True)


class IncomeCategory(Category):
    """Static income categories data."""
    __tablename__ = 'income_categories'


class ExpenseGroup(Category):
    __tablename__ = 'expense_groups'


class ExpenseCategory(Category):
    __tablename__ = 'expense_categories'
    expense_group = Column(String(MAX_CATEGORY_LENGTH), ForeignKey(
        ExpenseGroup.category))


class Accounts(Base):
    __tablename__ = 'accounts'
    name = Column(String, primary_key=True, nullable=False)
    account_type = Column(String, nullable=False) # todo platform or standard
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)


class Transactions(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)
    date = Column(Integer, nullable=False)
    amount = Column(Float, nullable=False)
    description = Column(String, nullable=False)
    to_account = Column(String, nullable=False)


class Transfers(Transactions):
    __tablename__ = 'transfers'
    from_account = Column(String, ForeignKey(Accounts.name), nullable=False)


class Income(Transactions):
    __tablename__ = 'income'
    category = Column(String, ForeignKey(IncomeCategory.category),
                      nullable=False)


class Expenses(Transactions):
    __tablename__ = 'expenses'
    currency_code = Column(String(3), nullable=False)
    details = Column(String)
    group_id = Column(Integer)
    amount_owed = Column(Float, nullable=False)
    amount_paid = Column(Float, nullable=False)
    subcategory = Column(String, ForeignKey(ExpenseCategory.category))
    #category = calculated field


class Investments(Base):
    __tablename__ = 'investments_list'
    ticker = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    source = Column(String(3), nullable=False) # todo constraint to YF or EODHD
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime)
    account = Column(String, ForeignKey(Accounts.name)) # todo must be a
    # platform account


class ValueTimeSeries(Base):
    __abstract__ = True
    date = Column(Date, primary_key=True)
    value = Column(Float, nullable=False)


class InflationValueTimeSeries(ValueTimeSeries):
    __tablename__ = 'inflation'


class InvestmentValueTimeSeries(ValueTimeSeries):
    # todo need to create multiple of these for each investment
    __tablename__ = 'investment_value'


def add_and_update_records(sql_engine: Engine, table_class: Base, prim_key: str,
                           new_and_updated: pd.DataFrame | pd.Series):
    """Identify unchanged, new and modified records between existing SQL
    database table and new_and_updated. Write to database."""
    existing_records = pd.read_sql_table(table_class.__tablename__, sql_engine)
    new_records = new_and_updated[~new_and_updated[prim_key].isin(
        existing_records[prim_key])].dropna(subset=[prim_key])
    # Pandas to_sql method has the caveat that if_exists='replace',
    # table schema will be erased. So, append the new data and use sqlalchemy
    # methods to modify existing data, rather than pandas methods.
    new_records.to_sql(table_class.__tablename__, sql_engine,
                       if_exists='append', index=False, method='multi')

    modified_records = new_and_updated.loc[new_and_updated[prim_key].isin(
        existing_records[prim_key])].dropna(subset=[prim_key])
    with session_scope(sql_engine) as session:
        for row in modified_records.iterrows():
            result = session.query(table_class.__tablename__).filter_by(
                getattr(table_class, prim_key) == row[prim_key]).first()
            print("GRBTGHNRREHGER" + result)



@contextmanager
def session_scope(sql_engine: Engine):
    Session = sessionmaker(bind=sql_engine)
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def get_primary_key_name(sql_engine: Engine, table_class: Base):
    """Return the name of the table's primary key. DOES NOT WORK FOR
    COMPOSITE KEYS."""
    columns = inspect(sql_engine).get_columns(table_class.__tablename__)
    for column in columns:
        if column['primary_key']:
            return column['name']


def populate_hardcoded(sql_engine: Engine):
    #income_primary_key = get_primary_key_name(sql_engine, IncomeCategory)
    add_and_update_records(sql_engine, IncomeCategory, 'category',
                           pd.Series(name='category', data=INCOME_CATEGORIES))

    #expense_primary_key = get_primary_key_name(sql_engine, ExpenseGroup)
    add_and_update_records(sql_engine, ExpenseGroup, 'category',
                           pd.Series(name='category', data=EXPENSE_GROUPS))

    #expense_subcat_primary_key = get_primary_key_name(sql_engine,
    # ExpenseCategory)
    add_and_update_records(sql_engine, ExpenseCategory, 'category',
                           pd.DataFrame(columns=['category', 'expense_groups'],
                                        data=EXPENSE_CATEGORIES))


Base.metadata.create_all(engine, checkfirst=True)
#populate_hardcoded(engine)

engine.dispose()
