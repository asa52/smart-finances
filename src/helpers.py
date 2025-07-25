import codecs
import json
import re
from datetime import datetime, date

import numpy as np
import pandas as pd
import yaml

from src import DEFAULT_DATESTR_FORMAT, NoDataWarning


def get_excel_table(
    excel_file_path: str, sheet_name: str, header: int = 2
) -> pd.DataFrame:
    excel_table = pd.read_excel(excel_file_path, sheet_name=sheet_name, header=header)
    if len(excel_table.index) == 0:
        raise NoDataWarning(
            f"No data in excel workbook {excel_file_path}, sheet {sheet_name}"
        )
    excel_table["Date"] = excel_table["Date"].dt.date
    excel_table.sort_values("Date", inplace=True)
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
    expenses["Owed"] = expenses["Owed"].astype(np.float)
    expenses["Paid"] = expenses["Paid"].astype(np.float)
    # expenses = currency_convert(expenses)
    expenses.rename(columns={"Owed": "Amount", "Details": "To account"}, inplace=True)
    expenses.loc[:, "To account"].replace(
        to_replace=re.compile(".*Debit.*"), value="Current", inplace=True
    )
    expenses.loc[:, "To account"].replace(
        to_replace=re.compile(".*Amex.*"), value="Current", inplace=True
    )
    expenses.loc[:, "To account"].replace(
        to_replace=re.compile(".*PayPal.*"), value="PayPal", inplace=True
    )
    expenses_reordered = expenses.loc[
        :, ["Description", "Date", "Amount", "To account"]
    ]
    expenses_reordered.loc[:, "Type"] = "Expense"
    expenses_reordered.loc[:, "From account"] = np.nan
    expenses.to_excel("existing_categories.xlsx")
    # Combine and sort
    combined = pd.concat(
        [income, expenses_reordered, pension_reordered], ignore_index=True, sort=False
    )
    combined.loc[:, "Date"] = pd.to_datetime(
        combined.loc[:, "Date"], format=DEFAULT_DATESTR_FORMAT
    )
    combined.sort_values("Date", inplace=True)
    combined = combined.reset_index(drop=True)
    combined.loc[:, "Description"] = combined.loc[:, "Description"].str.strip()
    combined.to_excel("Combined.xlsx")
    return combined


def reformat_pension_df(pension):
    # Reformat pension data to fit into incomes.
    pension.loc[:, "Amount in"] = pension.loc[:, "Amount in"].fillna(0)
    pension.loc[:, "Amount out"] = pension.loc[:, "Amount out"].fillna(0)
    pension.loc[:, "Amount"] = (
        pension.loc[:, "Amount in"] + pension.loc[:, "Amount out"]
    )
    pension_reordered = pension.loc[:, ["Description", "Date", "Amount", "Type"]]
    pension_reordered.loc[:, "To account"] = "Pension"
    pension_reordered.loc[:, "From account"] = np.nan
    return pension_reordered


def get_midnight_datetime(dt=None):
    """For a date in string or date format, return a datetime at midnight
    on that day. If dt is None, defaults to today's date."""
    midnight_time = datetime.min.time()
    if dt is None:
        return datetime.combine(date.today(), midnight_time)
    if isinstance(dt, str):
        return datetime.strptime(dt, DEFAULT_DATESTR_FORMAT)
    if isinstance(dt, datetime):
        return datetime.combine(dt.date(), midnight_time)
    if isinstance(dt, date):
        return datetime.combine(dt, midnight_time)
    else:
        raise AssertionError("dt is not of a valid type.")


def make_default_datestr_format(dt=None):
    """Make dt into DEFAULT_DATESTR_FORMAT string. Default to today's date
    if dt is not specified."""
    if dt is None:
        return date.today().strftime(DEFAULT_DATESTR_FORMAT)
    if isinstance(dt, str):
        try:
            date.fromisoformat(dt)
        except ValueError as exc:
            raise ValueError("Date string dt must be in the format YYYY-MM-DD") from exc
        return dt
    elif isinstance(dt, datetime):
        return dt.date().strftime(DEFAULT_DATESTR_FORMAT)
    elif isinstance(dt, date):
        return dt.strftime(DEFAULT_DATESTR_FORMAT)
    else:
        raise AssertionError("dt is not of a valid type.")


def read_expenses(json_file_path):
    with codecs.open(json_file_path, "r", encoding="utf-8-sig") as json_file:
        content = json.load(json_file)
    return content


def load_yaml(file_path):
    """Load yaml file and return resulting dictionary."""
    with open(file_path, "r") as yaml_file:
        try:
            data = yaml.safe_load(yaml_file)
        except yaml.YAMLError as exc:
            raise exc
    return data


def lookup_unit_price(value_date: date, fund_name: str, funds_dict: dict):
    """Look up from funds_dict the unit price in GBP of the fund_name on the
    date closest to the value_date."""
    fund_value_df = funds_dict[fund_name].unit_price_history_df
    closest_date = fund_value_df.loc[fund_value_df["Date"] <= value_date]["Date"].max()
    unit_price = (
        fund_value_df.loc[fund_value_df.Date == closest_date]["Adj Close"] / 100
    )
    return float(unit_price)
