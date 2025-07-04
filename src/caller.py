"""Main script to initiate data download, analysis and frontend."""

from src import expenses
from src import helpers as h


def main():
    params = h.load_yaml("parameters.yaml")

    # Download and currency convert expenses from Splitwise.
    expenses.expenses_to_csv(
        params["user_id"],
        params["exchange_rates_token"],
        params["splitwise_token"],
        params["root_path"] + params["expenses_file"],
        params["root_path"] + params["exchange_rate_file"],
        params["root_path"] + params["expense_categories_file"],
        start_date=params["start_date"],
    )

    # Update investment values.
    # investments_data = process.update_investment_values(
    #    params['eodhd_api_token'], params['root_path']+'investments/',
    #    params['root_path']+params['investments_file'],
    #    force_read_old_data=True) # #todo change this

    # Re-download monthly inflation table.
    # apis.get_monthly_inflation(params['root_path']+params['inflation_file'],
    #                           min_date=params['start_date'])

    # t = h.get_excel_table('../data/Master 8 - Copy.xlsm', 'Input - S&S ISA')
    # process.calculate_platform_history(t, investments_data)


if __name__ == "__main__":
    main()
