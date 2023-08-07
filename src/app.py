"""Define the dashboard app layout."""

from collections import namedtuple

import dash_auth
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Dash, html, dash_table, dcc, callback, Output, Input
from dash.dash_table.Format import Format, Group, Scheme, Symbol

JOINER_CHAR = '/'
DATE_COLUMN_TITLE = 'Date'

df = pd.read_csv('..\expenses.csv')
df = (df.assign(date=pd.to_datetime(df.date))
      .rename(columns={'date': DATE_COLUMN_TITLE}))

# Initialize the app
PLOTLY_LOGO = "https://images.plot.ly/logo/new-branding/plotly-logomark.png"
DBC_CSS_TEMPLATE = ("https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-"
                    "templates@V1.0.2/dbc.min.css")
app = Dash(__name__, external_stylesheets=[dbc.themes.MATERIA,
                                           DBC_CSS_TEMPLATE])

VALID_USERNAME_PASSWORD_PAIRS = {'admin': 'password'}
auth = dash_auth.BasicAuth(app, VALID_USERNAME_PASSWORD_PAIRS)

EXPENSE_CATEGORY_OPTIONS = pd.DataFrame(
    index=['by_category', 'by_subcategory'],
    columns=['option_name', 'df_column_name'],
    data=[['Category', 'subcategory'], ['Subcategory', 'sub_subcategory']])
TimeGroupFormats = namedtuple('TimeGroupFormats',
                              ['weekly', 'monthly', 'quarterly', 'yearly'])
TIME_MENU = TimeGroupFormats('Week', 'Month', 'Quarter', 'Year')

tab1_content = dbc.Card(dbc.CardBody([
    dbc.Row(html.H1('Expenses Breakdown',
                    className="text-primary text-center fs-3")),
    dbc.Row([dcc.Dropdown(TIME_MENU, TIME_MENU.monthly,
                          id='time-grouping-menu', clearable=False),
             dcc.Dropdown(EXPENSE_CATEGORY_OPTIONS.option_name,
                          EXPENSE_CATEGORY_OPTIONS.loc[
                              'by_category', 'option_name'],
                          id='expense-category-menu', clearable=False),
             dcc.Dropdown([*df.group_id.unique(), '-'], '-',
                          id='filter-menu', clearable=False)]),
    dbc.Row([dbc.Col([dcc.Graph(figure={}, id='absolute-expenses-graph'),
                      html.Div(id='expense-category-pivot',
                               className='dbc-row-selectable')]),
             dbc.Col([dcc.Graph(figure={}, id='absolute-expenses-graph-2')]),
             ]),
]), className="mt-3")

tab2_content = dbc.Card(dbc.CardBody([
    dbc.Row(html.H1("Income Breakdown",
                    className="text-primary text-center fs-3")),
]), className="mt-3")

tabs = dbc.Tabs([dbc.Tab(tab1_content, label="Expenses"),
                 dbc.Tab(tab2_content, label="Income")])

# App layout
app.layout = dbc.Container(tabs, fluid=True)


@callback(
    Output(component_id='expense-category-pivot',
           component_property='children'),
    Input(component_id='time-grouping-menu', component_property='value'),
    Input(component_id='expense-category-menu', component_property='value'),
    Input(component_id='filter-menu', component_property='value'))
def update_expense_pivottable(time_grouping_format: str,
                              expense_category_format: str,
                              filter_by_group: str) -> dash_table.DataTable:
    """Callback to update expense pivot table based on dropdowns.
    @param time_grouping_format: One of valid TIME_GROUP_FORMATS.
    @param expense_category_format: Either Category or Subcategory,
    indicating how to summarise expenses.
    @param filter_by_group: Which expense group to filter by.
    @return: Updated DataTable
    """
    date_format = {TIME_MENU.weekly: '%Y-W%W', TIME_MENU.monthly: '%Y-%b',
                   TIME_MENU.quarterly: '%Y-Q', TIME_MENU.yearly: '%Y'}

    groupings = [
        pd.Grouper(key=DATE_COLUMN_TITLE, freq=time_grouping_format[0]),
        EXPENSE_CATEGORY_OPTIONS.loc['by_category', 'df_column_name']]
    levels = [1]
    if expense_category_format == EXPENSE_CATEGORY_OPTIONS.loc['by_subcategory', 'option_name']:
        groupings.append(EXPENSE_CATEGORY_OPTIONS.loc['by_subcategory', 'df_column_name'])
        levels.append(2)

    filtered_expenses = df if filter_by_group == '-' else df.loc[
        df.group_id == filter_by_group]
    aggregated_expenses = (filtered_expenses
                           .groupby(groupings)['amount']
                           .sum()
                           .unstack(level=levels)
                           .fillna(0)
                           .sort_index(axis=1))
    add_row_totals = (aggregated_expenses
                      .assign(Total=aggregated_expenses.sum(axis=1))
                      .reset_index(level=0))

    if time_grouping_format == TIME_MENU.quarterly:
        # N.B. Quarter is not supported by dt.strftime so a different method
        # needs to be used for this specifically.
        add_row_totals[DATE_COLUMN_TITLE] = pd.PeriodIndex(
            add_row_totals.loc[:, DATE_COLUMN_TITLE], freq='Q').map(str)
    else:
        add_row_totals[DATE_COLUMN_TITLE] = (
            add_row_totals.loc[:, DATE_COLUMN_TITLE].dt.strftime(
            date_format[time_grouping_format]))

    column_formats = []

    for column_name in add_row_totals.columns:
        # If 'Subcategory' category format is chosen, column names will be
        # tuples of length 2. The 2nd element in the DATE_COLUMN_TITLE column
        # name will be empty.
        if expense_category_format == EXPENSE_CATEGORY_OPTIONS.loc['by_subcategory', 'option_name']:
            column_details = {"name": column_name,
                              "id": JOINER_CHAR.join(column_name)}
            if column_name[0] != DATE_COLUMN_TITLE:
                column_details['type'] = 'numeric'
        else:
            column_details = {"name": column_name, "id": column_name}
            if column_name != DATE_COLUMN_TITLE:
                column_details['type'] = 'numeric'

        column_details['format'] = Format(
            scheme=Scheme.fixed, precision=2, group=Group.yes, groups=3,
            group_delimiter=',', decimal_delimiter='.', symbol=Symbol.yes,
            symbol_prefix=u'£')
        column_formats.append(column_details)

    if expense_category_format == EXPENSE_CATEGORY_OPTIONS.loc['by_subcategory', 'option_name']:
        add_row_totals.columns = add_row_totals.columns.map(JOINER_CHAR.join)

    return dash_table.DataTable(
        columns=column_formats, data=add_row_totals.to_dict('records'),
        style_as_list_view=True, page_size=100, merge_duplicate_headers=True,
        style_table={'overflowX': 'auto', 'overflowY': 'auto'})


if __name__ == '__main__':
    app.run(debug=True)
