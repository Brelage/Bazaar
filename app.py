from flask import Flask
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import db_utils
from models import DailyData, DailyStatistics, Categories
import pandas as pd
from graphs import (
    create_price_trend_graph,
    create_price_distribution_graph,
    create_price_heatmap,
    create_price_statistics_dashboard,
    create_statistics_time_series,
    create_category_scatter_plot
)
import plotly.graph_objects as go

# Initialize Flask app
server = Flask(__name__)

# Initialize Dash app
app = dash.Dash(
    __name__,
    server=server,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="Bazaar: Supermarket Statistics"
)

# Define available statistics columns
STATISTICS_COLUMNS = [
    {'label': 'Price Statistics', 'value': 'price_stats'},
    {'label': 'Product Counts', 'value': 'product_counts'},
    {'label': 'Product Percentages', 'value': 'percentage_stats'}
]

# Get unique categories from database
with db_utils.session_query() as session:
    categories = [{'label': 'All Products', 'value': 'all'}] + [
        {'label': cat.category_name, 'value': str(cat.category_id)} for cat in 
        session.query(Categories).order_by(Categories.category_name).all()
    ]

# Define the layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("Bazaar Data Dashboard", className="text-center my-4"),
            html.Hr(),
        ])
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Select Visualization Type", className="card-title"),
                    dcc.Dropdown(
                        id='visualization-type',
                        options=[
                            {'label': 'General Information', 'value': 'dashboard'},
                            {'label': 'Statistics Time Series', 'value': 'statistics'},
                            {'label': 'Category Scatter Plot', 'value': 'category_scatter'}
                        ],
                        placeholder="Select a visualization type",
                        clearable=False
                    )
                ])
            ], className="mb-4")
        ], width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Select Statistic", className="card-title"),
                    dcc.Dropdown(
                        id='statistic-column',
                        options=STATISTICS_COLUMNS,
                        value='price_mean',
                        clearable=False
                    )
                ])
            ], className="mb-4", id='statistic-selector', style={'display': 'none'})
        ], width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Select Category", className="card-title"),
                    dcc.Dropdown(
                        id='category-selector',
                        options=categories,
                        placeholder="Select a category",
                        clearable=False
                    )
                ])
            ], className="mb-4", id='category-selector-container', style={'display': 'none'})
        ], width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Outlier Removal", className="card-title"),
                    dbc.Switch(
                        id='outlier-removal',
                        label="Remove outliers (3 standard deviations)",
                        value=False,
                        className="mb-3"
                    )
                ])
            ], className="mb-4", id='outlier-removal-container', style={'display': 'none'})
        ], width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-graph",
                type="circle",
                children=[
                    dcc.Graph(id='main-graph', style={'display': 'none'}),
                ]
            ),
        ], width=12)
    ]),
    
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # Update every minute
        n_intervals=0
    )
], fluid=True)

@app.callback(
    Output('statistic-selector', 'style'),
    Output('category-selector-container', 'style'),
    Output('outlier-removal-container', 'style'),
    [Input('visualization-type', 'value')]
)
def toggle_selectors(vis_type):
    if vis_type == 'statistics':
        return {'display': 'block'}, {'display': 'none'}, {'display': 'none'}
    elif vis_type == 'category_scatter':
        return {'display': 'none'}, {'display': 'block'}, {'display': 'block'}
    return {'display': 'none'}, {'display': 'none'}, {'display': 'none'}

@app.callback(
    Output('main-graph', 'figure'),
    Output('main-graph', 'style'),
    Input('visualization-type', 'value'),
    Input('statistic-column', 'value'),
    Input('category-selector', 'value'),
    Input('outlier-removal', 'value')
)
def update_graph(visualization_type, statistic_column, category, remove_outliers):
    if not visualization_type:
        # Return an empty figure and hide the graph when no type is selected
        return go.Figure(), {'display': 'none'}
    
    if visualization_type == 'trends':
        return create_price_trend_graph(), {'display': 'none'}
    elif visualization_type == 'heatmap':
        return create_price_heatmap(), {'display': 'none'}
    elif visualization_type == 'dashboard':
        return create_price_statistics_dashboard(), {'display': 'block'}
    elif visualization_type == 'statistics':
        if not statistic_column:
            # Return an empty figure and hide the graph when no statistic is selected
            return go.Figure(), {'display': 'none'}
        return create_statistics_time_series(statistic_column=statistic_column), {'display': 'block'}
    elif visualization_type == 'category_scatter':
        if not category:
            # Return an empty figure and hide the graph when no category is selected
            return go.Figure(), {'display': 'none'}
        return create_category_scatter_plot(category=category, remove_outliers=remove_outliers), {'display': 'block'}
    else:
        return go.Figure(), {'display': 'none'}

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050) 