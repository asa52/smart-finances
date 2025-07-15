"""Define the dashboard app layout."""

import argparse
import os
import re
from collections import namedtuple
from typing import Tuple, Iterable

import dash_auth
import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import plotly.express as px
from dash import Dash, html, dash_table, callback, Output, Input, dcc
from dash.dash_table.Format import Format, Group, Scheme, Symbol
from plotly.graph_objects import Figure

JOINER_CHAR = "/"
DATE_COLUMN_TITLE = "Date"
PLOTLY_LOGO = "https://images.plot.ly/logo/new-branding/plotly-logomark.png"
DBC_CSS_TEMPLATE = (
    "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates@V1.0.2/dbc.min.css"
)
VALID_LOGIN = {os.environ.get('SMART_FINANCE_USERNAME'): os.environ.get('SMART_FINANCE_PASSWORD')}

TimeGroupFormats = namedtuple(
    "TimeGroupFormats", ["weekly", "monthly", "quarterly", "yearly", "all_time"]
)
TIME_MENU = TimeGroupFormats("Week", "Month", "Quarter", "Year", "All time")
EXPENSE_CATEGORY_OPTIONS = pd.DataFrame(
    index=["by_category", "by_subcategory"],
    columns=["option_name", "df_column_name"],
    data=[["Category", "subcategory"], ["Subcategory", "sub_subcategory"]],
)


def main(expense_groups: pd.DataFrame):
    app = Dash(__name__, external_stylesheets=[dbc.themes.MATERIA, DBC_CSS_TEMPLATE])
    _ = dash_auth.BasicAuth(app, VALID_LOGIN)

    expenses_tab_content = dbc.Card(
        dbc.CardBody(
            [
                dbc.Row(
                    html.H1(
                        "Expenses Breakdown", className="text-primary text-center fs-3"
                    )
                ),
                dbc.Row(
                    [
                        dcc.Dropdown(
                            TIME_MENU,
                            TIME_MENU.monthly,
                            id="time-grouping-menu",
                            clearable=False,
                        ),
                        dcc.Dropdown(
                            EXPENSE_CATEGORY_OPTIONS.option_name,
                            EXPENSE_CATEGORY_OPTIONS.loc["by_category", "option_name"],
                            id="expense-category-menu",
                            clearable=False,
                        ),
                        dcc.Dropdown(
                            [*sorted(expense_groups.name), "-"],
                            "-",
                            id="expense-group-menu",
                            clearable=False,
                        ),
                        dcc.Dropdown(
                            ["-", *['House', 'Delhi Trip 2025', 'Belfast 2025', 'Iceland 2025', 'Rome 2025']],
                            "-",
                            id="tag-menu",
                            clearable=False,
                        ),
                    ]
                ),
                dbc.Row(
                    [
                        dbc.Col(dcc.Graph(figure={}, id="absolute-expenses-graph")),
                        dbc.Col(dcc.Graph(figure={}, id="relative-expenses-graph")),
                    ]
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            html.Div(
                                id="expense-category-pivot",
                                className="dbc-row-selectable",
                            )
                        ),
                        dbc.Col(
                            html.Div(id="expense-list", className="dbc-row-selectable")
                        ),
                    ]
                ),
            ]
        ),
        className="mt-3",
    )

    income_tab_content = dbc.Card(
        dbc.CardBody(
            [
                dbc.Row(
                    html.H1(
                        "Income Breakdown", className="text-primary text-center fs-3"
                    )
                ),
            ]
        ),
        className="mt-3",
    )

    tabs = dbc.Tabs(
        [
            dbc.Tab(expenses_tab_content, label="Expenses"),
            dbc.Tab(income_tab_content, label="Income"),
        ]
    )

    # App layout
    app.layout = dbc.Container(tabs, fluid=True)
    app.run(host="0.0.0.0")


@callback(
    Output(component_id="absolute-expenses-graph", component_property="figure"),
    Output(component_id="relative-expenses-graph", component_property="figure"),
    Output(component_id="expense-category-pivot", component_property="children"),
    Output(component_id="expense-list", component_property="children"),
    Input(component_id="time-grouping-menu", component_property="value"),
    Input(component_id="expense-category-menu", component_property="value"),
    Input(component_id="expense-group-menu", component_property="value"),
    Input(component_id="tag-menu", component_property="value"),
)
def update_expense_pivottable(
        time_grouping_format: str, expense_category_format: str, filter_by_group: str, filter_by_tag: str
) -> Tuple[Figure, Figure, dash_table.DataTable, dash_table.DataTable]:
    """Callback to update expense graphs and tables based on dropdowns.
    @param time_grouping_format: One of valid TIME_GROUP_FORMATS.
    @param expense_category_format: Either Category or Subcategory,
    indicating how to summarise expenses.
    @param filter_by_group: Which expense group to filter by.
    @param filter_by_tag: Which tag to filter by.
    @return: Updated DataTable
    """
    date_string_format = {
        TIME_MENU.weekly: "%Y-W%W",
        TIME_MENU.monthly: "%Y-%b",
        TIME_MENU.quarterly: "%Y-Q",
        TIME_MENU.yearly: "%Y",
        TIME_MENU.all_time: "All time",
    }

    grouping_frequency_format = {
        TIME_MENU.weekly: 'W',
        TIME_MENU.monthly: 'ME',
        TIME_MENU.quarterly: 'QE',
        TIME_MENU.yearly: 'YE',
        TIME_MENU.all_time: '100YS',
    }

    groupings = [
        pd.Grouper(key=DATE_COLUMN_TITLE, freq=grouping_frequency_format[time_grouping_format]),
        EXPENSE_CATEGORY_OPTIONS.loc["by_category", "df_column_name"],
    ]
    if (
            expense_category_format
            == EXPENSE_CATEGORY_OPTIONS.loc["by_subcategory", "option_name"]
    ):
        groupings.append(
            EXPENSE_CATEGORY_OPTIONS.loc["by_subcategory", "df_column_name"]
        )
        levels = (1, 2)
    else:
        levels = (1,)

    if filter_by_group == "-" and filter_by_tag == "-":
        filtered_expenses = expenses
    elif filter_by_group != "-" and filter_by_tag == "-":
        selected_group_id = int(expense_groups.loc[expense_groups.name == filter_by_group, 'id'])
        filtered_expenses = expenses.loc[expenses.group_id == selected_group_id]
    elif filter_by_group == "-" and filter_by_tag != "-":
        filtered_expenses = expenses.loc[expenses.details.fillna("").str.contains(filter_by_tag, flags=re.IGNORECASE)]
    else:
        selected_group_id = int(expense_groups.loc[expense_groups.name == filter_by_group, 'id'])
        filtered_expenses = expenses.loc[
            np.logical_and(expenses.group_id == selected_group_id,
                           expenses.details.fillna("").str.contains(filter_by_tag, flags=re.IGNORECASE))
        ]

    aggregated_expenses = filtered_expenses.groupby(groupings)["amount"].sum()
    df_for_graphs = aggregated_expenses.reset_index()
    aggregated_expenses_pivoted = (
        aggregated_expenses.unstack(level=levels).fillna(0).sort_index(axis=1)
    )
    add_row_totals = aggregated_expenses_pivoted.assign(
        Total=aggregated_expenses_pivoted.sum(axis=1)
    ).reset_index(level=0)

    if time_grouping_format == TIME_MENU.quarterly:
        # N.B. Quarter is not supported by dt.strftime so a different method
        # needs to be used for this specifically.
        add_row_totals[DATE_COLUMN_TITLE] = pd.PeriodIndex(
            add_row_totals.loc[:, DATE_COLUMN_TITLE], freq="Q"
        ).map(str)
    else:
        add_row_totals[DATE_COLUMN_TITLE] = add_row_totals.loc[
                                            :, DATE_COLUMN_TITLE
                                            ].dt.strftime(date_string_format[time_grouping_format])

    def join_columns(column_names: Iterable[str]) -> str:
        return (
            column_names[0]
            if column_names[-1] == ""
            else JOINER_CHAR.join(column_names)
        )

    column_formats = []
    for column_name in add_row_totals.columns:
        # If 'Subcategory' category format is chosen, column names will be
        # tuples of length 2. The 2nd element in the DATE_COLUMN_TITLE column
        # name will be empty.
        if (
                expense_category_format
                == EXPENSE_CATEGORY_OPTIONS.loc["by_subcategory", "option_name"]
        ):
            column_details = {"name": column_name, "id": join_columns(column_name)}
            if column_name[0] != DATE_COLUMN_TITLE:
                column_details["type"] = "numeric"
        else:
            column_details = {"name": column_name, "id": column_name}
            if column_name != DATE_COLUMN_TITLE:
                column_details["type"] = "numeric"

        column_details["format"] = Format(
            scheme=Scheme.fixed,
            precision=2,
            group=Group.yes,
            groups=3,
            group_delimiter=",",
            decimal_delimiter=".",
            symbol=Symbol.yes,
            symbol_prefix="£",
        )
        column_formats.append(column_details)

    if (
            expense_category_format
            == EXPENSE_CATEGORY_OPTIONS.loc["by_subcategory", "option_name"]
    ):
        add_row_totals.columns = add_row_totals.columns.map(join_columns)

    expenses_df = (
        filtered_expenses.assign(
            date=filtered_expenses[DATE_COLUMN_TITLE].dt.strftime("%Y-%m-%d")
        )
        .loc[
        :,
        [
            "date",
            "description",
            "amount",
            "subcategory",
            "sub_subcategory",
            "group_id",
        ],
        ]
        .rename(
            columns=dict(
                zip(
                    EXPENSE_CATEGORY_OPTIONS.df_column_name,
                    EXPENSE_CATEGORY_OPTIONS.option_name,
                )
            )
        )
    )

    table_format = {
        "style_as_list_view": True,
        "page_size": 100,
        "merge_duplicate_headers": True,
        "style_table": {"overflowX": "auto", "overflowY": "auto"},
        "style_header": {"fontWeight": "bold"},
        "style_cell": {"font-family": "sans-serif"},
        "style_data_conditional": [
            {"if": {"row_index": "odd"}, "backgroundColor": "rgb(220, 220, 220)"}
        ],
    }
    expense_pivot = dash_table.DataTable(
        columns=column_formats, data=add_row_totals.to_dict("records"), **table_format
    )
    expense_list = dash_table.DataTable(
        data=expenses_df.to_dict("records"), **table_format
    )
    if (
            expense_category_format
            == EXPENSE_CATEGORY_OPTIONS.loc["by_subcategory", "option_name"]
    ):
        color = EXPENSE_CATEGORY_OPTIONS.loc["by_subcategory", "df_column_name"]
    else:
        color = EXPENSE_CATEGORY_OPTIONS.loc["by_category", "df_column_name"]
    graph_format = {
        "data_frame": df_for_graphs,
        "x": DATE_COLUMN_TITLE,
        "y": "amount",
        "line_group": "subcategory",
        "color": color,
        "hover_name": "Date",
        "hover_data": "amount",
    }
    absolute_fig = px.area(
        **graph_format,
        labels={
            "amount": "Expense / £",
            "subcategory": "Category",
            "sub_subcategory": "Subcategory",
        },
        range_y=[0, add_row_totals.Total],
    )
    relative_fig = px.area(
        **graph_format,
        groupnorm="percent",
        labels={
            "amount": "Expense / %",
            "subcategory": "Category",
            "sub_subcategory": "Subcategory",
        },
    )

    return absolute_fig, relative_fig, expense_pivot, expense_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("expenses_file", help="Path to CSV file specifying expenses")
    parser.add_argument("expense_groups_file", help="Path to CSV file specifying expense groups")
    parsed = parser.parse_args()

    expenses_file_path = parsed.expenses_file
    expense_groups_file_path = parsed.expense_groups_file

    expenses = pd.read_csv(expenses_file_path)
    expense_groups = pd.read_csv(expense_groups_file_path)
    expenses = expenses.assign(date=pd.to_datetime(expenses.date)).rename(
        columns={"date": DATE_COLUMN_TITLE}
    )
    main(expense_groups)
    # todo pie chart with sliding date scale
    # todo conditional formatting for pivot table
