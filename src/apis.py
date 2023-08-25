"""Functions to call various APIs."""

from datetime import timezone
from io import StringIO

import pandas as pd
import requests

from src import DEFAULT_START_DATE
from src import helpers as h


class APICallError(Exception):
    """Raise if APICall class fails for any reason."""


class APICall:
    """Class to hold information about an API call."""

    NUM_API_CALLS = 0
    DEFAULT_HEADER = {
        "User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) "
        "Gecko/2009021910 Firefox/3.0.7"
    }

    def __init__(self, url, header=None, params=None):
        header, params = self.empty_dict(header, params)
        self.url = url
        self.header = self.__class__.DEFAULT_HEADER | header
        self.params = params

    @staticmethod
    def empty_dict(*args):
        """Create an empty dict for every arg in args that is None.
        Otherwise, return the arg unchanged."""
        return [{} if arg is None else arg for arg in args[1:]]

    def make_api_call(self):
        response = requests.request(
            "GET", self.url, headers=self.header, params=self.params
        )
        self.__class__.NUM_API_CALLS += 1
        if response.status_code != 200:
            raise APICallError(
                f"API call failed with the following error: " f"{response.reason}"
            )
        return response.text

    def response_to_df(self):
        response_text = self.make_api_call()
        return pd.read_csv(StringIO(response_text))


def get_monthly_inflation(output_file, min_date=DEFAULT_START_DATE):
    """Get monthly inflation rate as a dataframe for all dates >= min_date."""
    url = (
        "https://www.ons.gov.uk/generator?format=csv&uri=/economy/inflation"
        "andpriceindices/timeseries/l55o/mm23"
    )
    inflation_api_caller = APICall(url)
    raw_df = inflation_api_caller.response_to_df()

    year_month_regex = r"2\d{3} [A-Z]{3}"
    filtered = (
        raw_df.loc[raw_df[raw_df.columns[0]].str.contains(year_month_regex)]
        .rename(
            columns={raw_df.columns[0]: "date", raw_df.columns[1]: "inflation_rate"}
        )
        .astype({"date": "datetime64[ns]"})
        .query(f'date >= "{min_date}"')
    )

    filtered.to_csv(
        output_file,
        header=True,
        mode="w",
        index=False,
        lineterminator="\n",
        date_format="%Y-%m",
    )
    return filtered


def get_ticker_values(source, ticker, min_date, max_date=None, api_token=None):
    assert source == "YF" or source == "EODHD", f"Invalid source: {source}"

    def get_ticker_eodhd_no_token(tick, start_date, end_date=None):
        return get_ticker_values_eodhd(api_token, tick, start_date, end_date)

    get_ticker = {"YF": get_ticker_values_yfinance, "EODHD": get_ticker_eodhd_no_token}

    return get_ticker[source](ticker, min_date, max_date)


def get_ticker_values_yfinance(ticker, min_date, max_date=None):
    """Gets the Yahoo Finance historical data for the value of a ticker from
    min_date to max_date."""
    min_date = h.get_midnight_datetime(min_date)
    max_date = h.get_midnight_datetime(max_date)
    date1 = int(min_date.replace(tzinfo=timezone.utc).timestamp())
    date2 = int(max_date.replace(tzinfo=timezone.utc).timestamp())

    url = (
        f"https://query1.finance.yahoo.com/v7/finance/download/"
        f"{ticker}?period1={date1}&period2={date2}&interval=1d&events"
        f"=history"
    )
    yfinance_api_caller = APICall(url)
    df = yfinance_api_caller.response_to_df()
    df.Date = pd.to_datetime(df.Date).dt.date
    df.drop(columns=["Open", "Close", "High", "Low", "Volume"], inplace=True)
    return df


def get_ticker_values_eodhd(api_token, ticker, min_date, max_date=None):
    """Gets the end of day historical data for the value of a ticker from
    min_date to max_date."""
    min_date, max_date = map(h.make_default_datestr_format, [min_date, max_date])

    url = (
        f"https://eodhistoricaldata.com/api/eod/{ticker}?api_token"
        f"={api_token}&fmt=csv&period=d&from={min_date}&to={max_date}"
    )
    eodhd_api_caller = APICall(url)
    df = eodhd_api_caller.response_to_df()

    if df.size > 0:
        df.Date = pd.to_datetime(df.Date).dt.date
        df.drop(columns=["Open", "Close", "High", "Low", "Volume"], inplace=True)
        df.rename(columns={"Adjusted_close": "Adj Close"}, inplace=True)
    return df
