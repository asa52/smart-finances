"""Functions to call various APIs."""

import json
from datetime import timezone
from io import StringIO

import pandas as pd
import requests
from src import helpers as h


class APICallError(Exception):
    pass


class APICall(object):
    """Class to hold information about an API call."""
    NUM_API_CALLS = 0
    DEFAULT_HEADER = {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) '
                                    'Gecko/2009021910 Firefox/3.0.7'}

    def __init__(self, url, header=None, params=None):
        header, params = self.empty_dict(header, params)
        self.url = url
        self.header = self.__class__.DEFAULT_HEADER | header
        self.params = params

    def empty_dict(*args):
        """Create an empty dict for every arg in args that is None.
        Otherwise, return the arg unchanged."""
        return [dict() if arg is None else arg for arg in args[1:]]

    def make_api_call(self):
        response = requests.request("GET", self.url, headers=self.header,
                                    params=self.params)
        self.__class__.NUM_API_CALLS += 1
        if response.status_code != 200:
            raise APICallError(f"API call failed with the following error: "
                               f"{response.reason}")
        return response.text

    def response_to_df(self):
        response_text = self.make_api_call()
        return pd.read_csv(StringIO(response_text))


def get_exchange_rates(symbols, date_str, token, base=h.DEFAULT_CURRENCY):
    """Get the exchange rate to convert from the currency given by 'symbol' to
    the base currency. If a date dt is specified, find the rate at the latest
    available date before or equal to the given date."""

    symbols = '%2C'.join(symbols)
    url = f"https://api.apilayer.com/exchangerates_data/{date_str}?symbols" \
          f"={symbols}&base={base}"
    header = {"apikey": token}
    exchange_rates_api_caller = APICall(url, header=header)
    rates = json.loads(exchange_rates_api_caller.make_api_call())['rates']
    return [[date_str, curr, rates[curr]] for curr in rates.keys()]


def get_monthly_inflation(output_file, min_date=h.DEFAULT_START_DATE):
    """Get monthly inflation rate as a dataframe for all dates >= min_date."""
    url = 'https://www.ons.gov.uk/generator?format=csv&uri=/economy/inflation' \
          'andpriceindices/timeseries/l55o/mm23'
    inflation_api_caller = APICall(url)
    df = inflation_api_caller.response_to_df()
    df.rename(columns={df.columns[0]: 'Date', df.columns[1]: 'InflationRate'},
              inplace=True)
    filtered_df_1 = df.loc[df.Date.str.contains('2\d{3} [A-Z]{3}')]
    filtered_df_1.Date = pd.to_datetime(filtered_df_1.Date, format='%Y %b')
    filtered_df_2 = filtered_df_1.loc[filtered_df_1.Date >= min_date]
    filtered_df_2.to_csv(output_file, index=False)
    return filtered_df_2


def get_ticker_values_yfinance(ticker, min_date, max_date=None):
    """Gets the Yahoo Finance historical data for the value of a ticker from
    min_date to max_date."""
    min_date = h.get_midnight_datetime(min_date)
    max_date = h.get_midnight_datetime(max_date)
    date1 = int(min_date.replace(tzinfo=timezone.utc).timestamp())
    date2 = int(max_date.replace(tzinfo=timezone.utc).timestamp())

    url = f'https://query1.finance.yahoo.com/v7/finance/download/' \
          f'{ticker}?period1={date1}&period2={date2}&interval=1d&events' \
          f'=history'
    yfinance_api_caller = APICall(url)
    df = yfinance_api_caller.response_to_df()
    df.Date = pd.to_datetime(df.Date).dt.date
    df.drop(columns=['Open', 'Close', 'High', 'Low', 'Volume'], inplace=True)
    return df


def get_ticker_values_eodhd(api_token, ticker, min_date, max_date=None):
    """Gets the end of day historical data for the value of a ticker from
    min_date to max_date."""
    min_date = h.make_default_datestr_format(min_date)
    max_date = h.make_default_datestr_format(max_date)

    url = f'https://eodhistoricaldata.com/api/eod/{ticker}?api_token' \
          f'={api_token}&fmt=csv&period=d&from={min_date}&to={max_date}'
    eodhd_api_caller = APICall(url)
    df = eodhd_api_caller.response_to_df()

    if df.size > 0:
        df.Date = pd.to_datetime(df.Date).dt.date
        df.drop(columns=['Open', 'Close', 'High', 'Low', 'Volume'], inplace=True)
        df.rename(columns={'Adjusted_close': 'Adj Close'}, inplace=True)
    return df


def get_raw_expenses_splitwise(token, min_date, max_date):
    """Get a list of expenses from Splitwise API after a certain date,
    for the user specified by token. Obtain token from 'API keys' in
    https://secure.splitwise.com/oauth_clients/1459."""

    url = "https://www.splitwise.com/api/v3.0/get_expenses"
    params = {"dated_after": min_date, "dated_before": max_date, "limit": "0"}
    headers = {'Authorization': f"Bearer {token}",
               'Accept': "*/*",
               'Cache-Control': "no-cache",
               'Host': "www.splitwise.com",
               'accept-encoding': "gzip, deflate",
               'Connection': "keep-alive"}

    splitwise_api_caller = APICall(url, header=headers, params=params)
    expenses_list = json.loads(splitwise_api_caller.make_api_call())['expenses']
    return expenses_list
