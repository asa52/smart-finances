"""Define accounts and account groups as classes with their value histories."""
import datetime
from abc import abstractmethod

import pandas as pd


class ValueTimeSeries:
    """Class to hold information about any time series vs value data."""

    def __init__(self, name: str, start_date: datetime.date, value_history: pd.DataFrame | None=None,
                 end_date: datetime.date | None=None):
        self.name = name
        self.start_date = start_date
        self.value_history = value_history  # todo construct this properly
        self.end_date = end_date

    @abstractmethod
    def update(self):
        """Looks at all tables and updates value history accordingly. Define
        in child classes."""
        pass

    @property
    def latest_value(self):
        return self.value_history.Total.loc[
            self.value_history.Date == self.latest_date]

    @property
    def latest_date(self):
        return self.value_history.Date.max()


class StandardAccount(ValueTimeSeries):
    """General account class, usable for all standard accounts but not
    platforms."""

    def update(self):
        """Looks at all tables and updates value history accordingly."""
        pass     # todo define this specifically for standard accounts


class Platform(ValueTimeSeries):
    """Subset of accounts which track price history of a number of investment
    assets such as funds."""

    def __init__(self, name, start_date, end_date=None):
        super().__init__(name, start_date, value_history=None, end_date=end_date)
        # create the owned investments
        self.assets = dict()

    def update(self):
        pass


class InvestmentValueTimeSeries(ValueTimeSeries):
    """Tracks investment value against date."""

    def __init__(self, name, start_date, value_history, ticker,
                 source, end_date=None):
        super().__init__(name, start_date, value_history, end_date=end_date)
        self.ticker = ticker
        self.source = source

    def update(self):
        """Update investment value time series from appropriate source."""


class OwnedInvestment: # todo maybe change this to a decorator that acts on
    # InvestmentValueTimeSeries
    """Wrapper around InvestmentValueTimeSeries object to add data for buy
    and sell of shares and total value owned history."""

    def __init__(self, investment: InvestmentValueTimeSeries,
                 transaction_info: pd.DataFrame):
        self.name = investment.name
        self.start_date = investment.start_date
        self.value_history = investment.value_history  # todo construct this properly
        self.end_date = investment.end_date
        self.ticker = investment.ticker
        self.source = investment.source

    def





