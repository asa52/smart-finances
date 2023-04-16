from datetime import datetime, timedelta, date

from collections import OrderedDict, namedtuple
import pandas as pd
import numpy as np
from os.path import isfile

import apis
import helpers as h


def expenses_to_df(user_id, forex_token, splitwise_token, output_file,
                   exchange_rate_file, start_date=h.DEFAULT_START_DATE):
    """Get all transactions for a specific person, checking that they
    have not been deleted, as a DataFrame."""
    dates = h.get_year_bound_dates(start_date)
    raw_expenses = []
    for boundary in dates:
        raw_expenses += apis.get_raw_expenses_splitwise(splitwise_token,
                                                        *boundary)

    my_expenses = []
    for expense in raw_expenses:
        if expense['deleted_at'] is None and not expense['payment']:
            # Ensure the transaction has not been deleted and is not a payback.
            for users in expense['users']:
                if int(users['user']['id']) == user_id:
                    my_expenses.append(
                        [str(expense['id']),
                         datetime.fromisoformat(expense['date'][:-1]),
                         expense['description'], expense['category']['name'],
                         h.find_account_in_text(str(expense['details'])),
                         expense['currency_code'], users['owed_share'],
                         users['paid_share'], str(expense['group_id']),
                         'Expense', str(expense['details'])
                         ])
                    break

    expenses_df = pd.DataFrame(my_expenses, columns=[
        'ExpenseID', 'Date', 'Description', 'Sub-subcategory', 'To account',
        'Currency Code', 'Owed', 'Paid', 'GroupID', 'Category', 'Details'])
    expenses_df.set_index('ExpenseID', inplace=True)

    # Convert currencies
    expenses_df = currency_convert(expenses_df, forex_token, exchange_rate_file)
    expenses_df.sort_values('Date', inplace=True)

    expenses_df.to_csv(output_file)
    return expenses_df


def currency_convert(transactions, token, exchange_rate_file,
                     default_curr=h.DEFAULT_CURRENCY):
    """Convert the foreign transactions into the default currency."""

    # Get the foreign transactions, and those with unique date-currency pairs.
    # Compare the number of foreign expenses to what currencies.csv says.
    # get all foreign transactions, and group into unique date-currency pairs.
    # todo only put new transactions through this system. put old
    #  transactions into a csv to read from.
    transactions.loc[:, 'Owed'] = pd.to_numeric(transactions.loc[:, 'Owed'])
    transactions.loc[:, 'Date'] = pd.to_datetime(
        transactions.loc[:, 'Date']).dt.date
    transactions['Date_Curr'] = transactions['Date'].astype(str) + '_' + \
                                transactions['Currency Code']

    foreign_trns = transactions.loc[transactions['Currency Code'] !=
                                    default_curr, :]
    unique_date_currs = foreign_trns.drop_duplicates('Date_Curr')[
        ['Date_Curr', 'Date', 'Currency Code']]
    unique_date_currs.set_index('Date_Curr', inplace=True)
    new_date_currs, old_rates = h.extra_df_entries(
        exchange_rate_file, unique_date_currs, 'Date_Curr')
    unique_dates = new_date_currs.Date.unique()
    rates = []
    for d in unique_dates:
        filter_by_date = new_date_currs.loc[new_date_currs.Date == d]
        date_curr = filter_by_date.index[0]
        symbols = list(filter_by_date['Currency Code'])
        rates.append([date_curr, *apis.get_exchange_rates(
            symbols, d, token, base=default_curr)[0]])

    new_rates = pd.DataFrame(rates, columns=[
        'Date_Curr', 'Date', 'Currency Code', 'Rate/Base'])
    new_rates.set_index('Date_Curr', inplace=True)
    new_rates.to_csv(exchange_rate_file, mode='a', header=False)

    all_rates = pd.concat([old_rates, new_rates])

    # Do currency conversions to GBP on the foreign transactions.
    foreign_trns = foreign_trns.merge(all_rates['Rate/Base'],
                                      on=None, how='left', left_on='Date_Curr',
                                      right_index=True)
    foreign_trns['Amount'] = foreign_trns['Owed'] / foreign_trns['Rate/Base']
    foreign_trns.drop(columns=['Rate/Base'], inplace=True)

    local_trns = transactions.loc[transactions['Currency Code'] ==
                                  default_curr, :]
    local_trns['Amount'] = local_trns['Owed']

    transactions = pd.concat([local_trns, foreign_trns], sort=True)
    transactions.drop(columns=['Date_Curr'], inplace=True)
    return transactions


Fund = namedtuple('Fund', ['ticker', 'unit_price_history_df'])


def update_investment_values(eodhd_api_token, save_loc_path,
                             investments_file, force_read_old_data=False):
    """Update daily adjusted close data for each investment listed in the
    investments_file and save as csv."""
    investments = pd.read_csv(investments_file)

    # Create currified function so both ticker functions have the same input
    # argument format with no api token needed.
    get_ticker_eodhd_no_token = lambda ticker, min_date, max_date=None: apis.get_ticker_values_eodhd(
        eodhd_api_token, ticker, min_date, max_date)

    # Data can come from two sources - different function required for each.
    get_ticker = {'YF': apis.get_ticker_values_yfinance,
                  'EODHD': get_ticker_eodhd_no_token}
    all_updated_investments = OrderedDict()
    for _, investment in investments.iterrows():
        fname = f'{save_loc_path}{investment.Ticker}-{investment.Name}.csv'
        if isfile(fname):
            old_data = pd.read_csv(fname)
            old_data.Date = pd.to_datetime(old_data.Date, format=h.DEFAULT_DATESTR_FORMAT).dt.date
            latest_date = old_data.Date.max() + timedelta(days=1)
            if latest_date < date.today() and not force_read_old_data:
                new_data = get_ticker[investment.Source](
                    investment.Ticker, latest_date)
                new_data.to_csv(fname, mode='a', header=False, index=False)
                updated_investment_data = pd.concat([old_data, new_data],
                                                    axis=0, ignore_index=True)
            else:
                updated_investment_data = old_data
        else:
            updated_investment_data = get_ticker[investment.Source](
                investment.Ticker, investment.Start_date)
            updated_investment_data.to_csv(fname, index=False)

        all_updated_investments[f'{investment.Name}'] = Fund(
            ticker=investment.Ticker, unit_price_history_df=updated_investment_data)

    return all_updated_investments


def get_dates_union(inputs_table, funds):
    """Return list of union of sorted dates from inputs_table and all funds."""
    inputs_dates = inputs_table.Date
    unique_input_dates = set(inputs_dates)
    unique_funds_dates = set()
    for _, fund in funds.items():
        unique_funds_dates = unique_funds_dates.union(fund.unit_price_history_df.Date)
    extra_fund_dates = unique_funds_dates - unique_input_dates
    return sorted(inputs_dates)


def buy_fund(fund_df: pd.DataFrame, last_trans_date, transaction_date,
             num_shares_bought, buy_cost, unit_price):
    """Alter the fund dataframe for a buy operation happening on
    transaction_date. The last_chg_idx is the df index when the last
    transaction occurred. Return the modified fund_df and new last_chg_idx."""

    if transaction_date not in fund_df['Date'].values:
        fund_df.loc[len(fund_df)] = [transaction_date, unit_price, 0, 0, 0, 0]
        fund_df.sort_values('Date', inplace=True)

    # Find the indices for points between the current and last transaction.
    current_trans_date_idx = fund_df.index[fund_df['Date'] == transaction_date]
    last_trans_date_idx = fund_df.index[fund_df['Date'] == last_trans_date]
    between_dates_idxs = fund_df.index[(last_trans_date < fund_df.Date) &
                                       (fund_df.Date < transaction_date)]
    incl_current_idx = fund_df.index[(last_trans_date < fund_df.Date) &
                                     (fund_df.Date <= transaction_date)]

    fund_df.iloc[between_dates_idxs, 2] = fund_df.values[last_trans_date_idx, 2]
    fund_df.loc['Shares owned'].iloc[current_trans_date_idx] = fund_df['Shares owned'].iloc[current_trans_date_idx - 1] + num_shares_bought
    fund_df.loc['Value'].iloc[incl_current_idx] = fund_df['Shares owned'].iloc[incl_current_idx] * fund_df['Unit price'].iloc[incl_current_idx]

    fund_df.loc['Amount invested'].iloc[between_dates_idxs] = fund_df['Amount invested'].iloc[last_trans_date_idx]
    fund_df.loc['Amount invested'].iloc[current_trans_date_idx] = fund_df['Amount invested'].iloc[current_trans_date_idx - 1] + buy_cost

    gain = fund_df['Value'].iloc[incl_current_idx] - fund_df['Amount invested'].iloc[incl_current_idx]
    fund_df.loc['% return'].iloc[incl_current_idx] = gain / fund_df['Amount invested'].iloc[incl_current_idx]

    return fund_df, current_trans_date_idx


def calculate_platform_history(inputs_table, funds_dict):
    last_transaction_date = inputs_table.Date.min()
    day_before_start = last_transaction_date - timedelta(days=1)
    funds_history = {'Cash': pd.DataFrame(columns=['Date', 'Value'],
                                          data=[[day_before_start, 0.]])}
    last_trans_dates = {}
    for name, details in funds_dict.items():
        fund_value_transactions = details.unit_price_history_df.copy()
        fund_value_transactions[['Shares owned', 'Amount invested', 'Value',
                                 '% return']] = 0.
        fund_value_transactions.rename(columns={'Adj Close': 'Unit price'},
                                       inplace=True)
        fund_value_transactions['Unit price'] /= 100

        last_trans_dates[name] = last_transaction_date
        funds_history[name] = fund_value_transactions
    for _, row in inputs_table.iterrows():

        cash_df_length = len(funds_history['Cash'])
        last_cash_value = funds_history['Cash']['Value'].values[-1]
        fund_name = row['Fund']
        trans_date = row['Date']

        if row['Category'] == 'Transfer in':
            # Transfer in - add to cash, (subtract from income)
            funds_history['Cash'].loc[cash_df_length] = [
                trans_date, last_cash_value + row['Price']]
        elif row['Category'] == 'Transfer out' or row['Category'] == 'Fee - service' or row['Category'] == 'Fee - advisor':
            # transfer out - subtract from cash (add to income table),
            # Fee - service, subtract from cash
            # Fee - advisor, subtract from cash
            funds_history['Cash'].loc[cash_df_length] = [
                trans_date, last_cash_value - row['Price']]
        elif row['Category'] == 'Buy':
            # Buy - subtract from cash, add to fund at current fund unit price
            # to calculate number of shares
            funds_history['Cash'].loc[cash_df_length] = [
                trans_date, last_cash_value - row['Price']]

            unit_price = h.lookup_unit_price(trans_date, fund_name, funds_dict)
            if np.isnan(row['Corrected shares']):
                shares_transferred = row['Price'] / unit_price
            else:
                shares_transferred = row['Corrected shares']

            funds_history[fund_name], last_trans_dates[fund_name] = buy_fund(
                funds_history[fund_name], last_trans_dates[fund_name],
                trans_date, shares_transferred, row['Price'], unit_price)






    # Sell - subtract from fund at current unit price to calculate shares
    # transferred (or use input from inputs table)
    # Dividend - add to cash if income otherwise add as two lines, one for
    # add to cash and then buy with the cash
        pass