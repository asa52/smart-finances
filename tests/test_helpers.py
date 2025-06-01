"""Test functions in apis.py"""

import pytest

from src import helpers, expenses, NoDataWarning

TEST_DATA_PATH = "tests/test_data/"


@pytest.mark.parametrize(
    "input_details, expected_output",
    [
        ("", "Current"),
        ("paypal", "PayPal"),
        ("PAYPAL", "PayPal"),
        ("546%^%&* O*&  @M@JJ *(Y", "Current"),
        ("p a y p a l", "Current"),
    ],
)
def test_find_account_in_test(input_details, expected_output):
    """Check whether input details can be correctly interpreted as account.
    By default, account is Current unless 'PayPal' is explicitly specified."""
    assert expenses.determine_account_from_details(input_details) == expected_output


@pytest.mark.parametrize(
    "excel_file_path, sheet_name, output",
    [
        ("NoDatesInDateCol.xlsx", "Sheet2", AttributeError),
        ("EmptyTable.xlsx", "Sheet1", NoDataWarning),
    ],
)
def test_get_excel_table(excel_file_path, sheet_name, output):
    """Test if date column doesn't exist, date column doesn't contain dates."""
    with pytest.raises(output):
        helpers.get_excel_table(TEST_DATA_PATH + excel_file_path, sheet_name, header=0)


def test_get_excel_table_passes():
    """Read excel workbook with correct format, Date as first column."""
    excel_file_path = TEST_DATA_PATH + "NormalTable.xlsx"
    sheet_name = "Sheet1"
    helpers.get_excel_table(excel_file_path, sheet_name, header=0)
