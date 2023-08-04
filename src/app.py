import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import pandas as pd
import plotly.express as px
from dash import Dash, html, dash_table, dcc, callback, Output, Input

# Incorporate data
df = pd.read_csv(
    'https://raw.githubusercontent.com/plotly/datasets/master/gapminder2007.csv')

PLOTLY_LOGO = "https://images.plot.ly/logo/new-branding/plotly-logomark.png"

dbc_css = ("https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates@V1.0.2/dbc.min.css")

# Initialize the app - incorporate a Dash Bootstrap theme
app = Dash(__name__, external_stylesheets=[dbc.themes.MATERIA, dbc_css])



tab1_content = dbc.Card(dbc.CardBody([
        dbc.Row([html.H1('Expenses Breakdown',
                          className="text-primary text-center fs-3")]),


                dbc.Row(
                    [
                        dbc.RadioItems(options=[{"label": x, "value": x} for
                                                x in ['pop', 'lifeExp', 'gdpPercap']],
                                       value='lifeExp', inline=True,
                                       id='radio-buttons-final')
                    ]),
                dbc.Row(
                    [
                        dash_table.DataTable(
                            data=df.to_dict('records'), page_size=12,
                            style_table={'overflowX': 'auto'})
                    ]),



                dbc.Row(
                    [
                        dcc.Graph(figure={}, id='my-first-graph-final')
                    ])

    ]), className="mt-3")

tab2_content = dbc.Card(
    dbc.CardBody(
        [
            html.P("This is tab 2!", className="card-text"),
            dbc.Button("Don't click here", color="danger"),
        ]
    ),
    className="mt-3",
)

tabs = dbc.Tabs(
    [
        dbc.Tab(tab1_content, label="Expenses"),
        dbc.Tab(tab2_content, label="Income"),
    ]
)

navbar = dbc.Navbar(
    dbc.Container(
        [
            html.A(
                # Use row and col to control vertical alignment of logo / brand
                dbc.Row(
                    [
                        dbc.Col(html.Img(src=PLOTLY_LOGO, height="30px")),
                        dbc.Col(dbc.NavbarBrand("Smart Finance",
                                                className="ms-2")),
                    ],
                    align="left",
                    className="g-0",
                ),
                href="https://github.com/asa52/smart-finances",
                style={"textDecoration": "none"},
            ),
        ]
    ),
    color="light",
    dark=False,
)

# App layout
app.layout = dbc.Container([navbar, tabs], fluid=True)


@callback(
    Output(component_id='my-first-graph-final', component_property='figure'),
    Input(component_id='radio-buttons-final', component_property='value')
)
def update_graph(col_chosen):
    fig = px.histogram(df, x='continent', y=col_chosen, histfunc='avg',
                       template=load_figure_template('materia'))
    return fig


if __name__ == '__main__':
    app.run(debug=True)
