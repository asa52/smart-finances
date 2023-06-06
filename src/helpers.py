import codecs
import json
import re
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd
import yaml

DEFAULT_DATESTR_FORMAT = "%Y-%m-%d"
DEFAULT_START_DATE = "2017-09-01"
DEFAULT_CURRENCY = "GBP"


class NoDataWarning(Warning):
    pass


def get_excel_table(excel_file_path, sheet_name, header=2):
    excel_table = pd.read_excel(excel_file_path, sheet_name=sheet_name,
                                header=header)
    if len(excel_table.index) == 0:
        raise NoDataWarning(f'No data in excel workbook {excel_file_path}, '
                            f'sheet {sheet_name}')
    excel_table['Date'] = excel_table['Date'].dt.date
    excel_table.sort_values('Date', inplace=True)
    return excel_table


def combine_dataframes(income, expenses, pension):
    """
    :param income:
    :param expenses:
    :param pension:
    :return:
    """

    pension_reordered = reformat_pension_df(pension)

    # Reformat expenses correctly.
    expenses['Owed'] = expenses['Owed'].astype(np.float)
    expenses['Paid'] = expenses['Paid'].astype(np.float)
    #expenses = currency_convert(expenses)
    expenses.rename(columns={'Owed': 'Amount', 'Details': 'To account'},
                    inplace=True)
    expenses.loc[:, 'To account'].replace(to_replace=re.compile('.*Debit.*'),
                                          value='Current', inplace=True)
    expenses.loc[:, 'To account'].replace(to_replace=re.compile('.*Amex.*'),
                                          value='Current', inplace=True)
    expenses.loc[:, 'To account'].replace(to_replace=re.compile('.*PayPal.*'),
                                          value='PayPal', inplace=True)
    expenses_reordered = expenses.loc[:, ['Description', 'Date', 'Amount',
                                          'To account']]
    expenses_reordered.loc[:, 'Type'] = 'Expense'
    expenses_reordered.loc[:, 'From account'] = np.nan
    expenses.to_excel('existing_categories.xlsx')
    # Combine and sort
    combined = pd.concat([income, expenses_reordered, pension_reordered],
                         ignore_index=True, sort=False)
    combined.loc[:, 'Date'] = pd.to_datetime(combined.loc[:, 'Date'],
                                             format=DEFAULT_DATESTR_FORMAT)
    combined.sort_values('Date', inplace=True)
    combined = combined.reset_index(drop=True)
    combined.loc[:, 'Description'] = combined.loc[:, 'Description'].str.strip()
    combined.to_excel('Combined.xlsx')
    return combined


def reformat_pension_df(pension):
    # Reformat pension data to fit into incomes.
    pension.loc[:, 'Amount in'] = pension.loc[:, 'Amount in'].fillna(0)
    pension.loc[:, 'Amount out'] = pension.loc[:, 'Amount out'].fillna(0)
    pension.loc[:, 'Amount'] = pension.loc[:, 'Amount in'] + \
        pension.loc[:, 'Amount out']
    pension_reordered = pension.loc[:, ['Description', 'Date', 'Amount',
                                        'Type']]
    pension_reordered.loc[:, 'To account'] = 'Pension'
    pension_reordered.loc[:, 'From account'] = np.nan
    return pension_reordered


def get_midnight_datetime(dt=None):
    """For a date in string or date format, return a datetime at midnight
    on that day. If dt is None, defaults to today's date."""
    midnight_time = datetime.min.time()
    if dt is None:
        return datetime.combine(date.today(), midnight_time)
    elif isinstance(dt, str):
        return datetime.strptime(dt, DEFAULT_DATESTR_FORMAT)
    elif isinstance(dt, datetime):
        return datetime.combine(dt.date(), midnight_time)
    elif isinstance(dt, date):
        return datetime.combine(dt, midnight_time)
    else:
        raise AssertionError('dt is not of a valid type.')


def make_default_datestr_format(dt=None):
    """Make dt into DEFAULT_DATESTR_FORMAT string. Default to today's date
    if dt is not specified."""
    if dt is None:
        return date.today().strftime(DEFAULT_DATESTR_FORMAT)
    elif isinstance(dt, str):
        try:
            date.fromisoformat(dt)
        except ValueError:
            raise ValueError('Date string dt must be in the format YYYY-MM-DD')
        return dt
    elif isinstance(dt, datetime):
        return dt.date().strftime(DEFAULT_DATESTR_FORMAT)
    elif isinstance(dt, date):
        return dt.strftime(DEFAULT_DATESTR_FORMAT)
    else:
        raise AssertionError('dt is not of a valid type.')


def find_account_in_text(details):
    """Read the account the payment was taken from by searching within
    details."""
    if re.search(re.compile('paypal', flags=re.IGNORECASE), details):
        details = 'PayPal'
    else:
        details = 'Current'
    return details


def get_year_bound_dates(start_date_str):
    """Returns start and end dates for the year between start_date and
    today's date."""
    today = date.today()
    start_date = datetime.strptime(start_date_str, DEFAULT_DATESTR_FORMAT)
    years_since_start = [i for i in range(start_date.year + 1, today.year)]

    boundary_dates = [(start_date_str, f'{start_date.year}-12-31')]
    for year in years_since_start:
        boundary_dates.append((f'{year}-01-01', f'{year}-12-31'))
    boundary_dates.append((f'{today.year}-01-01', datetime.strftime(
        today + timedelta(days=1), DEFAULT_DATESTR_FORMAT)))
    return boundary_dates


def read_expenses(json_file_path):
    with codecs.open(json_file_path, 'r', encoding='utf-8-sig') as f:
        content = json.load(f)
    return content


def extra_df_entries(df1_path, df2, col):
    """Read in the csv from df1_path as a dataframe df1. Compare the 2 columns
    compare_cols_pair in df1 to df2 and return the rows of df2 whose column
    pairs do not appear in df2."""
    df1 = pd.read_csv(df1_path, index_col=col)
    new_rows = df2.loc[df2.index.difference(df1.index)]
    return new_rows, df1


def load_yaml(file_path):
    """Load yaml file and return resulting dictionary."""
    with open(file_path, "r") as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            raise exc
    return data


def lookup_unit_price(value_date: datetime.date, fund_name: str, funds_dict: dict):
    """Look up from funds_dict the unit price in GBP of the fund_name on the
    date closest to the value_date."""
    fund_value_df = funds_dict[fund_name].unit_price_history_df
    closest_date = fund_value_df.loc[fund_value_df['Date'] <= value_date][
        'Date'].max()
    unit_price = fund_value_df.loc[fund_value_df.Date == closest_date]['Adj Close'] /100
    return float(unit_price)
