"""Import, process and save expenses from Splitwise."""
import json
import re
from datetime import date, datetime, timedelta
from os.path import isfile
from typing import List

import pandas as pd

from src import DEFAULT_DATESTR_FORMAT, DEFAULT_CURRENCY, DEFAULT_START_DATE
from apis import APICall


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


def get_exchange_rates(symbols, date_str, token, base=DEFAULT_CURRENCY):
    """Get the exchange rate to convert from the currency given by 'symbol' to
    the base currency. If a date dt is specified, find the rate at the latest
    available date before or equal to the given date."""

    symbols = '%2C'.join(symbols)
    url = f"https://api.apilayer.com/exchangerates_data/{date_str}?symbols" \
          f"={symbols}&base={base}"
    header = {"apikey": token}
    exchange_rates_api_caller = APICall(url, header=header)
    rates = json.loads(exchange_rates_api_caller.make_api_call())['rates']
    return [[f'{date_str}_{curr}', date_str, curr, rates[curr]] for curr in
            rates.keys()]


def get_owed_paid_shares_for_user(users: pd.Series, target_user_id: int,
                                  which_share: str) -> pd.Series:
    """Get the owed and paid shares from the users series, which contains a list
    of dictionaries with user details per row."""

    def get_correct_user_share(users_details: List[dict]) -> float:
        """From users_details list, identify the correct user_id and
        determine their specified share."""
        users_df = pd.json_normalize(users_details)
        record = users_df.loc[users_df['user.id'] == target_user_id]
        return record[which_share].values[0] if len(record) != 0 else 0.

    return users.map(get_correct_user_share)


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


def determine_account_from_details(details: pd.Series) -> pd.Series:
    """Read the account the payment was taken from by searching within
    details."""
    account_map = {True: 'PayPal', False: 'Current', None: 'Current'}
    return details.str.contains('paypal', flags=re.IGNORECASE).map(account_map)


def currency_convert(transactions, token, exchange_rate_file,
                     default_curr=DEFAULT_CURRENCY):
    """Convert the foreign transactions into the default currency, updating
    the exchange rate file with new date-currency conversions."""

    date_curr_idx_name = 'date_curr'
    exchange_rate_column_names = [date_curr_idx_name, 'date', 'currency_code',
                                  'rate_per_base']
    if not isfile(exchange_rate_file):
        stored_exchange_rates = pd.DataFrame(columns=exchange_rate_column_names,
                                             ).set_index(date_curr_idx_name)
    else:
        stored_exchange_rates = pd.read_csv(exchange_rate_file,
                                            index_col=date_curr_idx_name)
        assert (stored_exchange_rates.columns == exchange_rate_column_names[1:]).all(), \
            f'{exchange_rate_file} has incorrect format: ' \
            f'{stored_exchange_rates.columns} not {exchange_rate_column_names}.'

    type_conversions = {exchange_rate_column_names[1]: 'datetime64[ns]',
                        exchange_rate_column_names[2]: 'category'}
    stored_exchange_rates = stored_exchange_rates.astype(type_conversions)

    transactions[date_curr_idx_name] = transactions.date.dt.date.astype(str) + \
                                       '_' + transactions.currency_code.astype(str)
    date_currencies = (
        transactions
        .drop(transactions[transactions.currency_code == default_curr].index)
        .loc[:, exchange_rate_column_names[:-1]]
        .drop_duplicates(subset=[date_curr_idx_name])
        .set_index(date_curr_idx_name)
    )
    new_date_currencies = (
        date_currencies
        .loc[date_currencies.index.difference(stored_exchange_rates.index)])

    if new_date_currencies.size > 0:
        forex_requests = (new_date_currencies
                          .groupby(new_date_currencies.date.dt.date.astype(str))
                          .currency_code.apply(list).reset_index())
        rates_list = forex_requests.apply(
            lambda x: get_exchange_rates(x.currency_code, x.date, token,
                                         base=default_curr), axis=1)
        new_exchange_rates = (pd.DataFrame.from_records(
            rates_list.explode(), index=date_curr_idx_name,
            columns=exchange_rate_column_names)
            .astype(type_conversions))

        all_exchange_rates = pd.concat([stored_exchange_rates,
                                        new_exchange_rates]).sort_index()

        # Sort by date and currency once all data is put together,
        # then overwrite existing exchange file with it to prevent needing to
        # sort later.
        all_exchange_rates.to_csv(exchange_rate_file, mode='w',
                                  date_format=DEFAULT_DATESTR_FORMAT,
                                  header=True, lineterminator='\n')
    else:
        all_exchange_rates = stored_exchange_rates

    # Convert transactions to base currency by dividing by the corresponding
    # exchange rate. If no exchange rate found (i.e. transaction in base
    # currency anyway, use 1 as the conversion rate).
    converted_transactions = (
        transactions
        .assign(amount=transactions.owed/transactions[date_curr_idx_name].map(
            all_exchange_rates.rate_per_base).fillna(1))
        .drop(columns=[date_curr_idx_name])
    )
    return converted_transactions


def expenses_to_df(user_id, forex_token, splitwise_token, output_file,
                   exchange_rate_file, start_date=DEFAULT_START_DATE):
    """Get all transactions for a specific person, checking that they
    have not been deleted, as a DataFrame."""
    dates = get_year_bound_dates(start_date)
    raw_expenses = []
    for boundary in dates:
        raw_expenses += get_raw_expenses_splitwise(splitwise_token, *boundary)

    raw_expenses_df = pd.json_normalize(raw_expenses).set_index('id')
    filtered_expenses = (
        raw_expenses_df
        .loc[raw_expenses_df.deleted_at.isna() & ~raw_expenses_df.payment]
        .loc[:, ['date', 'description', 'category.name', 'currency_code',
                 'users', 'group_id', 'details']])

    expense_categories = pd.read_csv('../data/expenses_categories.csv', index_col='sub_subcategory', header=0).astype(
        {'subcategory': 'category'})
    converted_expenses = (
        filtered_expenses
        .assign(date=pd.to_datetime(filtered_expenses.date, format='ISO8601').dt.tz_localize(None),
                account=determine_account_from_details(filtered_expenses.details),
                category='Expense',
                group_id=filtered_expenses.group_id.fillna(0),
                owed=get_owed_paid_shares_for_user(filtered_expenses.users, user_id, 'owed_share'),
                paid=get_owed_paid_shares_for_user(filtered_expenses.users, user_id, 'paid_share'),
                details=filtered_expenses.details.str.replace('\n', ' ', regex=True))
        .drop(columns=['users'])
        .rename(columns={'category.name': 'sub_subcategory'})
        .astype({'category': 'category', 'sub_subcategory': 'category',
                 'group_id': 'int',
                 'currency_code': 'category', 'account': 'category',
                 'owed': 'float', 'paid': 'float'})
        .query("`owed` > 0")
        .sort_values('date')
        .join(expense_categories, on='sub_subcategory', how='left')
        .pipe((currency_convert, 'transactions'), token=forex_token,
              exchange_rate_file=exchange_rate_file)
    )
    converted_expenses.to_csv(output_file)
    return converted_expenses
