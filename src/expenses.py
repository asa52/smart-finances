"""Import, process and save expenses from Splitwise."""

import json
import re
from datetime import date, datetime, timedelta
from os.path import isfile
from typing import Dict, List, Tuple
import argparse

import pandas as pd

from src import DEFAULT_DATESTR_FORMAT, DEFAULT_CURRENCY, DEFAULT_START_DATE
from src.apis import APICall
from src.helpers import load_yaml


def get_raw_expenses_splitwise(token: str, min_date: str, max_date: str) -> list:
    """Get a list of expenses from Splitwise API after a certain date,
    for the user specified by token. Obtain token from 'API keys' in
    https://secure.splitwise.com/oauth_clients/1459."""

    url = "https://www.splitwise.com/api/v3.0/get_expenses"
    params = {"dated_after": min_date, "dated_before": max_date, "limit": "0"}
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "*/*",
        "Cache-Control": "no-cache",
        "Host": "www.splitwise.com",
        "accept-encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    splitwise_api_caller = APICall(url, header=headers, params=params)
    raw_expenses = json.loads(splitwise_api_caller.make_api_call())["expenses"]
    return raw_expenses


def get_expense_groups(token: str, expense_groups_file: str) -> pd.DataFrame:
    """Get a list of expenses from Splitwise API after a certain date,
    for the user specified by token. Obtain token from 'API keys' in
    https://secure.splitwise.com/oauth_clients/1459."""

    url = "https://www.splitwise.com/api/v3.0/get_groups"
    params: Dict[str, str] = {}
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "*/*",
        "Cache-Control": "no-cache",
        "Host": "www.splitwise.com",
        "accept-encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    splitwise_api_caller = APICall(url, header=headers, params=params)
    groups = json.loads(splitwise_api_caller.make_api_call())["groups"]
    groups_df = (
        pd.DataFrame(groups)
        .loc[:, ["id", "name", "group_type"]]
        .fillna("other")
        .set_index("name")
    )
    groups_df.to_csv(expense_groups_file)
    return groups_df


def get_exchange_rates(
    symbols: List[str], conversion_date: str, token: str, base: str = DEFAULT_CURRENCY
) -> List[list]:
    """Get the exchange rate to convert from the currency given by 'symbol' to
    the base currency. If a date is specified, find the rate at the latest
    available date before or equal to the given date."""

    joined_symbols = "%2C".join(symbols)
    url = (
        f"https://api.apilayer.com/exchangerates_data/{conversion_date}?symbols"
        f"={joined_symbols}&base={base}"
    )
    header = {"apikey": token}
    exchange_rates_api_caller = APICall(url, header=header)
    rates = json.loads(exchange_rates_api_caller.make_api_call())["rates"]
    return [
        [f"{conversion_date}_{curr}", conversion_date, curr, rates[curr]]
        for curr in rates.keys()
    ]


def get_owed_paid_shares_for_user(
    users: pd.Series, target_user_id: int, which_share: str
) -> pd.Series:
    """Get the owed and paid shares from the users series, which contains a list
    of dictionaries with user details per row."""

    def get_correct_user_share(users_details: List[dict]) -> float:
        """From users_details list, identify the correct user_id and
        determine their specified share."""
        users_df = pd.json_normalize(users_details)
        record = users_df.loc[users_df["user.id"] == target_user_id]
        return record[which_share].values[0] if len(record) != 0 else 0.0

    return users.map(get_correct_user_share)


def get_year_bound_dates(start_date_str: str) -> List[Tuple[str, str]]:
    """Returns start and end dates for the year between start_date and
    today's date."""
    today = date.today()
    start_date = datetime.strptime(start_date_str, DEFAULT_DATESTR_FORMAT)
    years_since_start = list(range(start_date.year + 1, today.year))

    boundary_dates = [(start_date_str, f"{start_date.year}-12-31")]
    for year in years_since_start:
        boundary_dates.append((f"{year}-01-01", f"{year}-12-31"))
    boundary_dates.append(
        (
            f"{today.year}-01-01",
            datetime.strftime(today + timedelta(days=1), DEFAULT_DATESTR_FORMAT),
        )
    )
    return boundary_dates


def determine_account_from_details(details: pd.Series) -> pd.Series:
    """Read the account the payment was taken from by searching within
    details."""
    account_map = {True: "PayPal", False: "Current", None: "Current"}
    return details.str.contains("paypal", flags=re.IGNORECASE).map(account_map)


def convert_foreign_transactions(
    transactions: pd.DataFrame,
    forex_api_token: str,
    exchange_rate_file: str,
    default_curr: str = DEFAULT_CURRENCY,
) -> pd.DataFrame:
    """Convert the foreign transactions into the default currency, updating
    the exchange rate file with new date-currency conversions."""

    # Obtain stored exchange rates
    index_column_name = "date_curr"
    updated_exchange_rates = update_exchange_rate_records(
        transactions,
        forex_api_token,
        exchange_rate_file,
        index_column_name,
        default_curr=default_curr,
    )

    # Convert transactions to base currency by dividing by the corresponding
    # exchange rate. If no exchange rate found (i.e. transaction in base
    # currency anyway), use 1 as the conversion rate.
    converted_transactions = transactions.assign(
        amount=transactions.owed
        / transactions[index_column_name]
        .map(updated_exchange_rates.rate_per_base)
        .fillna(1)
    ).drop(columns=[index_column_name])
    return converted_transactions


def update_exchange_rate_records(
    all_transactions: pd.DataFrame,
    forex_api_token: str,
    exchange_rate_file_path: str,
    index_column_name: str,
    default_curr: str = DEFAULT_CURRENCY,
):
    """Update stored exchange rates to reflect additional foreign currency
    transactions that have been made.
    """

    # Obtain stored exchange rates
    exchange_rate_column_names = [
        index_column_name,
        "date",
        "currency_code",
        "rate_per_base",
    ]
    if not isfile(exchange_rate_file_path):
        # Create an empty dataframe if there is no existing exchange rates file.
        existing_exchange_rates = pd.DataFrame(
            columns=exchange_rate_column_names,
        ).set_index(index_column_name)
    else:
        existing_exchange_rates = pd.read_csv(
            exchange_rate_file_path, index_col=index_column_name
        )
        assert (
            existing_exchange_rates.columns == exchange_rate_column_names[1:]
        ).all(), (
            f"{exchange_rate_file_path} has incorrect format: "
            f"{existing_exchange_rates.columns} not {exchange_rate_column_names}."
        )

    # Convert exchange rate dtypes to match transactions.
    type_conversions = {
        exchange_rate_column_names[1]: "datetime64[ns]",
        exchange_rate_column_names[2]: "category",
    }
    existing_exchange_rates = existing_exchange_rates.astype(type_conversions)

    # Determine the new date-currency pairs whose forex values need to be queried,
    # by constructing a date-currency column for all transactions, dropping rows
    # whose currency is the default (so doesn't need conversion) and dropping
    # duplicate date-currency pairs. Find the difference between this and the
    # existing exchange rates rows.
    all_transactions[index_column_name] = (
        all_transactions.date.dt.date.astype(str)
        + "_"
        + all_transactions.currency_code.astype(str)
    )
    date_currencies = (
        all_transactions.drop(
            all_transactions[all_transactions.currency_code == default_curr].index
        )
        .loc[:, exchange_rate_column_names[:-1]]
        .drop_duplicates(subset=[index_column_name])
        .set_index(index_column_name)
    )
    new_date_currencies = date_currencies.loc[
        date_currencies.index.difference(existing_exchange_rates.index)
    ]

    if new_date_currencies.size > 0:
        # Group requests by date because the API can return multiple currencies
        # in a single query.
        forex_requests = (
            new_date_currencies.groupby(new_date_currencies.date.dt.date.astype(str))
            .currency_code.apply(list)
            .reset_index()
        )
        # Obtain a list of rates relative to the default for each date. Note
        # that row.currency_code is a *list* of currencies.
        rates_list = forex_requests.apply(
            lambda row: get_exchange_rates(
                row.currency_code, row.date, forex_api_token, base=default_curr
            ),
            axis=1,
        )

        # Store new exchange rates with the existing ones to avoid having to run
        # the query again.
        new_exchange_rates = pd.DataFrame.from_records(
            rates_list.explode(),
            index=index_column_name,
            columns=exchange_rate_column_names,
        ).astype(type_conversions)

        all_exchange_rates = pd.concat(
            [existing_exchange_rates, new_exchange_rates]
        ).sort_index()

        # Sort by date and currency once all data is put together,
        # then overwrite existing exchange file with it to prevent needing to
        # sort later.
        all_exchange_rates.to_csv(
            exchange_rate_file_path,
            mode="w",
            date_format=DEFAULT_DATESTR_FORMAT,
            header=True,
            lineterminator="\n",
        )
    else:
        all_exchange_rates = existing_exchange_rates

    return all_exchange_rates


def expenses_to_csv(
    user_id,
    forex_api_token,
    splitwise_api_token,
    output_file,
    exchange_rate_file,
    expense_categories_file,
    start_date=DEFAULT_START_DATE,
):
    """Get all transactions for a specific person, checking that they
    have not been deleted, and save them to CSV.
    """

    # Split API calls by year to avoid saturating data being sent in a single
    # call.
    dates = get_year_bound_dates(start_date)
    raw_expenses = []
    for boundary in dates:
        raw_expenses += get_raw_expenses_splitwise(splitwise_api_token, *boundary)
    raw_expenses_df = pd.json_normalize(raw_expenses).set_index("id")

    # Filter expenses to ensure they are not deleted and are not records of
    # payments between people
    filtered_expenses = raw_expenses_df.loc[
        raw_expenses_df.deleted_at.isna() & ~raw_expenses_df.payment
    ].loc[
        :,
        [
            "date",
            "description",
            "category.name",
            "currency_code",
            "users",
            "group_id",
            "details",
        ],
    ]

    expense_categories = pd.read_csv(
        expense_categories_file, index_col="sub_subcategory", header=0
    ).astype({"subcategory": "category"})

    # Format the dates, record which account to debit from, change n/a group IDs
    # (non group expenses) to 0, determine how much the user in question owes
    # and paid, and change dtypes. Add a higher-level category column.
    converted_expenses = (
        filtered_expenses.assign(
            date=pd.to_datetime(
                filtered_expenses.date, format="ISO8601"
            ).dt.tz_localize(None),
            account=determine_account_from_details(filtered_expenses.details),
            category="Expense",
            group_id=filtered_expenses.group_id.fillna(0),
            owed=get_owed_paid_shares_for_user(
                filtered_expenses.users, user_id, "owed_share"
            ),
            paid=get_owed_paid_shares_for_user(
                filtered_expenses.users, user_id, "paid_share"
            ),
            details=filtered_expenses.details.str.replace("\n", " ", regex=True),
        )
        .drop(columns=["users"])
        .rename(columns={"category.name": "sub_subcategory"})
        .astype(
            {
                "category": "category",
                "sub_subcategory": "category",
                "group_id": "int",
                "currency_code": "category",
                "account": "category",
                "owed": "float",
                "paid": "float",
            }
        )
        .query("`owed` > 0")
        .sort_values("date")
        .join(expense_categories, on="sub_subcategory", how="left")
        .pipe(
            (convert_foreign_transactions, "transactions"),
            forex_api_token=forex_api_token,
            exchange_rate_file=exchange_rate_file,
        )
    )
    converted_expenses.to_csv(output_file)
    return converted_expenses


def main():
    """Get parameters file path and use this to download expenses from splitwise
    as CSVs.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "parameters_file", help="Path to YAML file specifying parameters"
    )
    params_file_path = parser.parse_args().parameters_file

    params = load_yaml(params_file_path)

    # Download and currency convert expenses from Splitwise.
    expenses_to_csv(
        params["user_id"],
        params["exchange_rates_token"],
        params["splitwise_token"],
        params["root_path"] + params["expenses_file"],
        params["root_path"] + params["exchange_rate_file"],
        params["root_path"] + params["expense_categories_file"],
        start_date=params["start_date"],
    )

    get_expense_groups(
        params["splitwise_token"], params["root_path"] + params["expense_groups_file"]
    )


if __name__ == "__main__":
    main()
