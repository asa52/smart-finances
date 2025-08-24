"""Define the dashboard app layout."""

import argparse
import os
import re
from collections import namedtuple
from typing import Tuple, Iterable, Optional
from dotenv import load_dotenv

import dash_auth
from datetime import datetime, timedelta
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from src import DEFAULT_START_DATE, DEFAULT_DATESTR_FORMAT
from dash import Dash, html, dash_table, callback, Output, Input, dcc, State
from dash.dash_table.Format import Format, Group, Scheme, Symbol
from pathlib import Path

load_dotenv()
JOINER_CHAR = "/"
DATE_COLUMN_TITLE = "Date"
PLOTLY_LOGO = "https://images.plot.ly/logo/new-branding/plotly-logomark.png"
DBC_CSS_TEMPLATE = (
    "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates@V1.0.2/dbc.min.css"
)
SECRET_KEY = os.getenv("SMART_FINANCE_SECRET_KEY")
VALID_LOGIN = {os.getenv("SMART_FINANCE_USERNAME"): os.getenv("SMART_FINANCE_PASSWORD")}

TimeGroupFormats = namedtuple(
    "TimeGroupFormats", ["weekly", "monthly", "quarterly", "yearly", "all_time"]
)
TIME_MENU = TimeGroupFormats("Week", "Month", "Quarter", "Year", "All time")
EXPENSE_CATEGORY_OPTIONS = pd.DataFrame(
    index=["by_category", "by_subcategory"],
    columns=["option_name", "df_column_name"],
    data=[["Category", "subcategory"], ["Subcategory", "sub_subcategory"]],
)


def filter_expenses_by_date_range(
    expenses: pd.DataFrame, start_date_str: str, end_date_str: str
) -> pd.DataFrame:
    """Filter for expenses that occur between start_date and end_date inclusive."""
    # Filter by selected dates
    start_date = datetime.strptime(start_date_str, DEFAULT_DATESTR_FORMAT)
    end_date = datetime.strptime(end_date_str, DEFAULT_DATESTR_FORMAT)

    with_datetime_column = expenses.assign(date=pd.to_datetime(expenses.date)).rename(
        columns={"date": DATE_COLUMN_TITLE}
    )
    return with_datetime_column.loc[
        (start_date <= with_datetime_column[DATE_COLUMN_TITLE])
        & (with_datetime_column[DATE_COLUMN_TITLE] <= end_date),
        :,
    ]


def filter_expenses_by_groups(
    expenses: pd.DataFrame,
    expense_groups: pd.DataFrame,
    selected_group_name: Iterable[str],
) -> pd.DataFrame:
    """Filter expenses by the group they belong to."""
    selected_group_ids = set(
        expense_groups.loc[expense_groups.name == name, "id"].iloc[0]
        for name in selected_group_name
    )
    return expenses.loc[expenses.group_id.isin(selected_group_ids)]


def filter_expenses_by_tags(
    expenses: pd.DataFrame, selected_tags: Iterable[str]
) -> pd.DataFrame:
    """Filter expenses by any chosen tags. If no tags are selected, return all expenses."""
    if not selected_tags:
        return expenses
    else:
        tags_pattern = r"|".join(selected_tags)
        return expenses.loc[
            expenses.details.fillna("").str.contains(
                tags_pattern, regex=True, flags=re.IGNORECASE
            )
        ]


def filter_expenses_by_categorisation(
    expenses: pd.DataFrame, categorisation_format: str, selected_options: Iterable[str]
) -> pd.DataFrame:
    """Filter expenses by category or subcategory depending on the chosen categorisation_format."""
    assert categorisation_format in EXPENSE_CATEGORY_OPTIONS.option_name.values
    column_to_filter = EXPENSE_CATEGORY_OPTIONS.loc[
        EXPENSE_CATEGORY_OPTIONS.option_name == categorisation_format, "df_column_name"
    ].to_list()[0]
    return expenses.loc[expenses[column_to_filter].isin(selected_options)]


def area_plot(df: pd.DataFrame, color: str, groupnorm: Optional[str]) -> dcc.Graph:
    """Create area plot."""
    graph_format = {
        "data_frame": df,
        "x": DATE_COLUMN_TITLE,
        "y": "amount",
        "line_group": "subcategory",
        "color": color,
        "hover_name": "Date",
        "hover_data": "amount",
        "labels": {
            "amount": "Expense / %",
            "subcategory": "Category",
            "sub_subcategory": "Subcategory",
        },
    }
    area = px.area(
        **graph_format,
        groupnorm=groupnorm,
    )
    area.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        xaxis={"fixedrange": True},
    )
    area_graph = dcc.Graph(figure=area)
    return area_graph


@callback(
    Output(component_id="expense-category-multi-select", component_property="options"),
    Output(component_id="expense-category-multi-select", component_property="value"),
    Input(
        component_id="expense-categorisation-format-menu", component_property="value"
    ),
    State(component_id="file-paths", component_property="data"),
)
def update_selected_expense_categories(
    expense_category_format: str, file_paths: dict[str, Path]
) -> tuple[list[str], list[str]]:
    """When the expense categorisation format is changed, update the available expense categories selected
    and available to be selected."""

    expenses = pd.read_csv(file_paths["expenses_path"])
    if (
        expense_category_format
        == EXPENSE_CATEGORY_OPTIONS.loc["by_subcategory", "option_name"]
    ):
        expense_categories = sorted(
            expenses[
                EXPENSE_CATEGORY_OPTIONS.loc["by_subcategory", "df_column_name"]
            ].unique()
        )
    else:
        expense_categories = sorted(
            expenses[
                EXPENSE_CATEGORY_OPTIONS.loc["by_category", "df_column_name"]
            ].unique()
        )
    return expense_categories, expense_categories


@callback(
    Output(
        component_id="absolute-expenses-graph-container", component_property="children"
    ),
    Output(
        component_id="relative-expenses-graph-container", component_property="children"
    ),
    Output(component_id="expense-category-pivot", component_property="children"),
    Output(component_id="expense-list", component_property="children"),
    Output(component_id="graph-title", component_property="children"),
    Output(component_id="absolute-graph-title", component_property="children"),
    Output(component_id="relative-graph-title", component_property="children"),
    Output(component_id="expense-group-menu", component_property="options"),
    Output(component_id="tag-menu", component_property="options"),
    Input(component_id="time-grouping-menu", component_property="value"),
    Input(
        component_id="expense-categorisation-format-menu", component_property="value"
    ),
    Input(component_id="expense-group-menu", component_property="value"),
    Input(component_id="tag-menu", component_property="value"),
    Input(component_id="date-range", component_property="start_date"),
    Input(component_id="date-range", component_property="end_date"),
    Input(component_id="expense-category-multi-select", component_property="value"),
    State(component_id="file-paths", component_property="data"),
)
def update_expense_pivottable(
    time_grouping_format: str,
    expense_category_format: str,
    filter_by_group: list[str],
    filter_by_tag: list[str],
    start_date: str,
    end_date: str,
    selected_expense_categories: list[str],
    file_paths: dict[str, Path],
) -> tuple[
    Optional[dcc.Graph],
    Optional[dcc.Graph],
    dash_table.DataTable,
    dash_table.DataTable,
    Optional[html.H4],
    Optional[html.H6],
    Optional[html.H6],
    list[str],
    list[str],
]:
    """Callback to update expense graphs and tables based on dropdowns."""
    expenses = pd.read_csv(file_paths["expenses_path"])
    expense_groups = pd.read_csv(file_paths["expense_groups_path"]).astype({"id": int})
    tags = pd.read_csv(file_paths["tags_path"])['Tags'].tolist()

    date_filtered_expenses = filter_expenses_by_date_range(
        expenses, start_date, end_date
    )
    filtered_expenses = filter_expenses_by_groups(
        date_filtered_expenses, expense_groups, filter_by_group
    )
    filtered_expenses = filter_expenses_by_tags(filtered_expenses, filter_by_tag)
    filtered_expenses = filter_expenses_by_categorisation(
        filtered_expenses, expense_category_format, selected_expense_categories
    )

    expense_group_names = sorted(expense_groups.name)
    date_string_format = {
        TIME_MENU.weekly: "%Y-W%W",
        TIME_MENU.monthly: "%Y-%b",
        TIME_MENU.quarterly: "%Y-Q",
        TIME_MENU.yearly: "%Y",
        TIME_MENU.all_time: "All time",
    }

    grouping_frequency_format = {
        TIME_MENU.weekly: "W",
        TIME_MENU.monthly: "ME",
        TIME_MENU.quarterly: "QE",
        TIME_MENU.yearly: "YE",
        TIME_MENU.all_time: "100YS",
    }

    groupings = [
        pd.Grouper(
            key=DATE_COLUMN_TITLE, freq=grouping_frequency_format[time_grouping_format]
        ),
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

    def join_columns(column_names: list[str]) -> str:
        return (
            column_names[0]
            if column_names[-1] == ""
            else JOINER_CHAR.join(column_names)
        )

    money_format = Format(
        scheme=Scheme.fixed,
        precision=2,
        group=Group.yes,
        groups=3,
        group_delimiter=",",
        decimal_delimiter=".",
        symbol=Symbol.yes,
        symbol_prefix="£",
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
        column_details["format"] = money_format
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
        .merge(expense_groups, left_on="group_id", right_on="id")
        .loc[
            :,
            [
                "date",
                "description",
                "amount",
                "subcategory",
                "sub_subcategory",
                "name",
            ],
        ]
        .sort_values("date", ascending=False)
    )

    category_column_formats = [
        dict(id=key, name=value)
        for key, value in zip(
            EXPENSE_CATEGORY_OPTIONS.df_column_name,
            EXPENSE_CATEGORY_OPTIONS.option_name,
        )
    ]

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
        columns=[
            dict(id="date", name=DATE_COLUMN_TITLE),
            dict(id="description", name="Description"),
            dict(id="amount", name="Amount", type="numeric", format=money_format),
            *category_column_formats,
            dict(id="name", name="Group"),
        ],
        data=expenses_df.to_dict("records"),
        **table_format,
    )

    legend_series = EXPENSE_CATEGORY_OPTIONS.loc[
        EXPENSE_CATEGORY_OPTIONS.loc[:, "option_name"] == expense_category_format,
        "df_column_name",
    ].iloc[0]

    if time_grouping_format == TIME_MENU.all_time:
        graph_title = None
        absolute_graph_title = None
        absolute_fig = None
        relative_graph_title = None
        relative_fig = None
    else:
        graph_title = html.H4("Expenses against time")
        absolute_graph_title = html.H6("Absolute expenditure")
        absolute_fig = area_plot(df_for_graphs, color=legend_series, groupnorm=None)
        relative_graph_title = html.H6("Relative expenditure")
        relative_fig = area_plot(df_for_graphs, color=legend_series, groupnorm="percent")

    return (
        absolute_fig,
        relative_fig,
        expense_pivot,
        expense_list,
        graph_title,
        absolute_graph_title,
        relative_graph_title,
        expense_group_names,
        tags,
    )


def expense_options(expense_group_names: list[str]) -> list[dbc.Row]:
    """Return a list of options objects to control which expenses are displayed."""
    date_today = datetime.now()
    return [
        dbc.Row(html.H4("Options")),
        dbc.Row(html.H5("Filters")),
        dbc.Row(html.H6("Date range")),
        dbc.Row(
            dcc.DatePickerRange(
                id="date-range",
                min_date_allowed=DEFAULT_START_DATE,
                max_date_allowed=date_today,
                start_date=f"{date_today - timedelta(days=365.25 * 2):{DEFAULT_DATESTR_FORMAT}}",
                end_date=f"{date_today:{DEFAULT_DATESTR_FORMAT}}",
                display_format="DD-MMM-YYYY",
                first_day_of_week=1,
                show_outside_days=True,
            )
        ),
        dbc.Row(html.Br()),
        dbc.Row(html.H6("Expense categories / sub-categories")),
        dcc.Dropdown(
            options=[],
            value=[],
            id="expense-category-multi-select",
            multi=True,
            searchable=True,
            clearable=False,
        ),
        dbc.Row(html.Br()),
        dbc.Row(html.H6("Expense groups")),
        dbc.Row(
            dcc.Dropdown(
                options=expense_group_names,
                value=expense_group_names,
                id="expense-group-menu",
                clearable=False,
                multi=True,
            )
        ),
        dbc.Row(html.Br()),
        dbc.Row(html.H6("Tags")),
        dbc.Row(
            dcc.Dropdown(
                options=[], value=[], id="tag-menu", clearable=True, multi=True
            )
        ),
        dbc.Row(html.Br()),
        dbc.Row(html.H5("Groupings")),
        dbc.Row(html.H6("By time")),
        dbc.Row(
            dcc.Dropdown(
                options=TIME_MENU,
                value=TIME_MENU.monthly,
                id="time-grouping-menu",
                clearable=False,
            )
        ),
        dbc.Row(html.Br()),
        dbc.Row(html.H6("By expense categorisation")),
        dbc.Row(
            dcc.Dropdown(
                options=EXPENSE_CATEGORY_OPTIONS.option_name,
                value=EXPENSE_CATEGORY_OPTIONS.loc["by_category", "option_name"],
                id="expense-categorisation-format-menu",
                clearable=False,
            )
        ),
        dbc.Row(html.Br()),
    ]


def main(expenses_path: str, expense_groups_path: str, tags_path: str) -> None:
    app = Dash(__name__, external_stylesheets=[dbc.themes.MATERIA, DBC_CSS_TEMPLATE])
    _ = dash_auth.BasicAuth(
        app, username_password_list=VALID_LOGIN, secret_key=SECRET_KEY
    )
    expense_group_names = sorted(pd.read_csv(expense_groups_path).name)
    expenses_tab_content = dbc.Card(
        dbc.CardBody(
            [
                dcc.Store(
                    id="file-paths",
                    data={
                        "expenses_path": expenses_path,
                        "expense_groups_path": expense_groups_path,
                        "tags_path": tags_path,
                    },
                ),
                dbc.Row(
                    html.H1(
                        "Expenses Breakdown", className="text-primary text-center fs-3"
                    )
                ),
                *expense_options(expense_group_names),
                dbc.Row(html.Div(id="graph-title")),
                dbc.Row(html.Div(id="absolute-graph-title")),
                dbc.Row(html.Div(id="absolute-expenses-graph-container")),
                dbc.Row(html.Br()),
                dbc.Row(html.Div(id="relative-graph-title")),
                dbc.Row(html.Div(id="relative-expenses-graph-container")),
                dbc.Row(html.Br()),
                dbc.Row(html.H4("Categorised expense totals")),
                dbc.Row(
                    html.Div(
                        id="expense-category-pivot", className="dbc-row-selectable"
                    )
                ),
                dbc.Row(html.Br()),
                dbc.Row(html.H4("Full expense list")),
                dbc.Row(html.Div(id="expense-list", className="dbc-row-selectable")),
            ]
        ),
        className="mt-3",
    )

    tabs = dbc.Tabs(
        [
            dbc.Tab(expenses_tab_content, label="Expenses"),
        ]
    )

    # App layout
    app.layout = dbc.Container(tabs, fluid=True)
    app.run(host="0.0.0.0")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("expenses_file", help="Path to CSV file specifying expenses")
    parser.add_argument(
        "expense_groups_file", help="Path to CSV file specifying expense groups")
    parser.add_argument(
        "tags_file", help="Path to CSV file specifying tags"
    )
    parsed = parser.parse_args()

    expenses_file_path = parsed.expenses_file
    expense_groups_file_path = parsed.expense_groups_file
    tags_file_path = parsed.tags_file

    main(expenses_file_path, expense_groups_file_path, tags_file_path)
    # todo conditional formatting for pivot table
