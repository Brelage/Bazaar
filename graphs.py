import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import db_utils
from models import DailyData, DailyStatistics, Stores, Categories, Products
from sqlalchemy import func

def create_price_trend_graph(data=None):
    """
    Create a line graph showing price trends over time.
    """
    if data is None:
        with db_utils.session_query() as session:
            data = session.query(DailyData).all()
            df = pd.DataFrame([{
                'date': pd.to_datetime(d.date),
                'price': d.listed_price,
                'amount': d.listed_amount,
            } for d in data])
    else:
        df = data.copy()
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            print("Converting date column to datetime in price trend graph")
            df['date'] = pd.to_datetime(df['date'])

    print(f"Price trend data shape: {df.shape}")
    print(f"Price trend data types:\n{df.dtypes}")
    print(f"Price trend data sample:\n{df.head()}")

    # Group by date and calculate mean price
    df = df.groupby('date')['price'].mean().reset_index()

    fig = px.line(df, x='date', y='price',
                  title='Price Trends Over Time',
                  labels={'price': 'Price', 'date': 'Date'})
    
    fig.update_layout(
        hovermode='x unified',
        showlegend=False
    )
    
    return fig

def create_price_distribution_graph(data=None):
    """
    Create a box plot showing price distribution.
    """
    if data is None:
        with db_utils.session_query() as session:
            data = session.query(DailyData).all()
            df = pd.DataFrame([{
                'price': d.listed_price,
                'amount': d.listed_amount,
            } for d in data])
    else:
        df = data.copy()

    print(f"Price distribution data shape: {df.shape}")
    print(f"Price distribution data types:\n{df.dtypes}")
    print(f"Price distribution data sample:\n{df.head()}")

    fig = px.box(df, y='price',
                 title='Price Distribution',
                 labels={'price': 'Price'})
    
    fig.update_layout(
        showlegend=False,
        yaxis_title="Price"
    )
    
    return fig

def create_price_heatmap(data=None):
    """
    Create a heatmap showing price variations over time.
    """
    if data is None:
        with db_utils.session_query() as session:
            data = session.query(DailyData).all()
            df = pd.DataFrame([{
                'date': pd.to_datetime(d.date),
                'price': d.listed_price
            } for d in data])
    else:
        df = data.copy()
        # Ensure date column is datetime
        df['date'] = pd.to_datetime(df['date'])

    print(f"Heatmap data shape: {df.shape}")
    print(f"Heatmap data types:\n{df.dtypes}")
    print(f"Heatmap data sample:\n{df.head()}")

    # Group by date and calculate mean price
    df = df.groupby('date')['price'].mean().reset_index()

    # Reshape data for heatmap
    df['day'] = df['date'].dt.day
    df['month'] = df['date'].dt.month
    
    # Group by month and day to get average price
    grouped_df = df.groupby(['month', 'day'])['price'].mean().reset_index()
    pivot_df = grouped_df.pivot(index='month', columns='day', values='price')
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale='Viridis'
    ))
    
    fig.update_layout(
        title='Price Heatmap Over Time',
        xaxis_title="Day",
        yaxis_title="Month",
        height=600
    )
    
    return fig

def create_price_statistics_dashboard(data=None):
    """
    Create a dashboard showing general information about the products:
    1. Amount of products per category as a pie chart
    2. Most and least expensive products in a table
    """
    with db_utils.session_query() as session:
        # Get category counts
        category_counts = session.query(
            Categories.category_name,
            func.count(DailyData.product_id).label('count')
        ).join(DailyData, Categories.category_id == DailyData.category_id)\
         .group_by(Categories.category_name)\
         .all()
        
        # Get most and least expensive products
        price_data = session.query(
            Products.product_name,
            Categories.category_name,
            DailyData.listed_price,
            DailyData.listed_amount,
            DailyData.listed_unit
        ).join(DailyData, Products.product_id == DailyData.product_id)\
         .join(Categories, DailyData.category_id == Categories.category_id)\
         .order_by(DailyData.listed_price.desc())\
         .all()
        
        most_expensive = price_data[0]
        least_expensive = price_data[-1]
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=1,
        specs=[[{"type": "pie"}], [{"type": "table"}]],
        vertical_spacing=0.1,
        subplot_titles=('Products per Category', 'Price Extremes')
    )
    
    # Add pie chart
    fig.add_trace(
        go.Pie(
            labels=[c[0] for c in category_counts],
            values=[c[1] for c in category_counts],
            hole=0.3,
            textinfo='label+percent',
            insidetextorientation='radial'
        ),
        row=1, col=1
    )
    
    # Create table data
    table_data = [
        ['Product', 'Category', 'Price', 'Amount'],
        [most_expensive.product_name, most_expensive.category_name, 
         f"{float(most_expensive.listed_price):.2f}€", 
         f"{float(most_expensive.listed_amount)} {most_expensive.listed_unit}"],
        [least_expensive.product_name, least_expensive.category_name, 
         f"{float(least_expensive.listed_price):.2f}€", 
         f"{float(least_expensive.listed_amount)} {least_expensive.listed_unit}"]
    ]
    
    # Add table
    fig.add_trace(
        go.Table(
            header=dict(
                values=table_data[0],
                fill_color='paleturquoise',
                align='left'
            ),
            cells=dict(
                values=[[row[i] for row in table_data[1:]] for i in range(4)],
                fill_color='lavender',
                align='left'
            )
        ),
        row=2, col=1
    )
    
    # Update layout
    fig.update_layout(
        height=800,
        title_text="General Product Information",
        title_x=0.5,
        showlegend=False
    )
    
    return fig

def create_statistics_time_series(data=None, statistic_column='price_mean'):
    """
    Create a time series graph showing statistics over time.
    
    Args:
        data: DataFrame containing the statistics data
        statistic_column: The column name from DailyStatistics to plot
    """
    if data is None:
        with db_utils.session_query() as session:
            data = session.query(DailyStatistics).all()
            
            if statistic_column == 'price_stats':
                # Create DataFrame with all price statistics
                df = pd.DataFrame([{
                    'date': pd.to_datetime(d.date),
                    'Mean Price': float(d.price_mean),
                    'Median Price': float(d.price_median),
                    'Minimum Price': float(d.price_min),
                    'Maximum Price': float(d.price_max)
                } for d in data])
                
                # Create subplots
                fig = make_subplots(
                    rows=2, cols=2,
                    subplot_titles=('Mean Price', 'Median Price', 'Minimum Price', 'Maximum Price'),
                    vertical_spacing=0.12,
                    horizontal_spacing=0.1
                )
                
                # Add traces for each statistic
                fig.add_trace(
                    go.Scatter(x=df['date'], y=df['Mean Price'], name='Mean Price'),
                    row=1, col=1
                )
                fig.add_trace(
                    go.Scatter(x=df['date'], y=df['Median Price'], name='Median Price'),
                    row=1, col=2
                )
                fig.add_trace(
                    go.Scatter(x=df['date'], y=df['Minimum Price'], name='Minimum Price'),
                    row=2, col=1
                )
                fig.add_trace(
                    go.Scatter(x=df['date'], y=df['Maximum Price'], name='Maximum Price'),
                    row=2, col=2
                )
                
                # Update layout
                fig.update_layout(
                    height=800,
                    showlegend=False,
                    title_text="Price Statistics Over Time",
                    title_x=0.5,
                    title_y=0.95
                )
                
                return fig
            elif statistic_column == 'percentage_stats':
                # Create DataFrame with percentage statistics
                df = pd.DataFrame([{
                    'date': pd.to_datetime(d.date),
                    'Bio Products': float(d.percentage_bio_products),
                    'Reduced Products': float(d.percentage_reduced_products)
                } for d in data])
                
                # Create figure with two lines
                fig = go.Figure()
                
                # Add traces for each percentage
                fig.add_trace(
                    go.Scatter(
                        x=df['date'],
                        y=df['Bio Products'],
                        name='Bio Products',
                        line=dict(color='#2ca02c', width=2)  # Green
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=df['date'],
                        y=df['Reduced Products'],
                        name='Reduced Products',
                        line=dict(color='#ff7f0e', width=2)  # Orange
                    )
                )
                
                # Update layout
                fig.update_layout(
                    height=800,
                    title_text="Product Percentages Over Time",
                    title_x=0.5,
                    yaxis=dict(
                        title="Percentage"
                    ),
                    xaxis=dict(
                        title="Date"
                    ),
                    showlegend=True,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    ),
                    hovermode='x unified'  # Show all values for a given date
                )
                
                return fig
            elif statistic_column == 'product_counts':
                # Create DataFrame with product count statistics
                df = pd.DataFrame([{
                    'date': pd.to_datetime(d.date),
                    'Total Products': float(d.amount_total_products),
                    'Bio Products': float(d.amount_bio_products),
                    'Reduced Products': float(d.amount_reduced_products)
                } for d in data])
                
                # Create subplots
                fig = make_subplots(
                    rows=2, cols=1,
                    subplot_titles=('Total Products Over Time', 'Product Types Over Time'),
                    vertical_spacing=0.15
                )
                
                # Add total products trace
                fig.add_trace(
                    go.Scatter(
                        x=df['date'],
                        y=df['Total Products'],
                        name='Total Products',
                        line=dict(color='#1f77b4', width=2)  # Blue
                    ),
                    row=1, col=1
                )
                
                # Add bio and reduced products traces
                fig.add_trace(
                    go.Scatter(
                        x=df['date'],
                        y=df['Bio Products'],
                        name='Bio Products',
                        line=dict(color='#2ca02c', width=2)  # Green
                    ),
                    row=2, col=1
                )
                fig.add_trace(
                    go.Scatter(
                        x=df['date'],
                        y=df['Reduced Products'],
                        name='Reduced Products',
                        line=dict(color='#ff7f0e', width=2)  # Orange
                    ),
                    row=2, col=1
                )
                
                # Update layout
                fig.update_layout(
                    height=800,
                    title_text="Product Counts Over Time",
                    title_x=0.5,
                    showlegend=True,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    ),
                    hovermode='x unified'  # Show all values for a given date
                )
                
                # Update y-axes labels
                fig.update_yaxes(title_text="Number of Products", row=1, col=1)
                fig.update_yaxes(title_text="Number of Products", row=2, col=1)
                fig.update_xaxes(title_text="Date", row=1, col=1)
                fig.update_xaxes(title_text="Date", row=2, col=1)
                
                return fig
            else:
                df = pd.DataFrame([{
                    'date': pd.to_datetime(d.date),
                    'value': float(getattr(d, statistic_column))
                } for d in data])
    else:
        df = data.copy()
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])

    print(f"Statistics time series data shape: {df.shape}")
    print(f"Statistics time series data types:\n{df.dtypes}")
    print(f"Statistics time series data sample:\n{df.head()}")

    fig = px.line(df, x='date', y='value',
                  title=f'{statistic_column.replace("_", " ").title()} Over Time',
                  labels={'value': statistic_column.replace('_', ' ').title(),
                         'date': 'Date'})
    
    fig.update_layout(
        hovermode='x unified',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        yaxis=dict(
            rangemode='tozero'  # This ensures the y-axis starts at 0
        ),
        height=800
    )
    
    return fig

def create_category_scatter_plot(category=None, remove_outliers=False):
    """
    Create a scatter plot showing price vs amount for a specific category.
    Normalizes amounts by converting liters to milliliters and kilograms to grams.
    Optionally removes outliers (data points more than 3 standard deviations from the median).
    
    Args:
        category: The category_id to filter by, or 'all' for all categories
        remove_outliers: Whether to remove outliers (3 standard deviations from median)
    """
    with db_utils.session_query() as session:
        query = session.query(DailyData)
        
        if category and category != 'all':
            # Get category name
            category_name = session.query(Categories.category_name).filter(Categories.category_id == category).scalar()
            query = query.filter(DailyData.category_id == category)
        else:
            category_name = "All Categories"
            
        data = query.all()
        
        # Create DataFrame with category names and product names
        df = pd.DataFrame([{
            'price': float(d.listed_price),  # Convert Decimal to float
            'amount': float(d.listed_amount) * (1000 if d.listed_unit in ['l', 'kg'] else 1),  # Convert l/kg to ml/g
            'category_name': session.query(Categories.category_name)
                                  .filter(Categories.category_id == d.category_id)
                                  .scalar(),
            'product_name': session.query(Products.product_name)
                                 .filter(Products.product_id == d.product_id)
                                 .scalar()
        } for d in data])

    print(f"Category scatter plot data shape before outlier removal: {df.shape}")
    
    if remove_outliers:
        # Calculate price statistics
        price_median = df['price'].median()
        price_std = df['price'].std()
        price_lower_bound = price_median - (3 * price_std)
        price_upper_bound = price_median + (3 * price_std)
        
        # Calculate amount statistics
        amount_median = df['amount'].median()
        amount_std = df['amount'].std()
        amount_lower_bound = amount_median - (3 * amount_std)
        amount_upper_bound = amount_median + (3 * amount_std)
        
        # Filter out outliers
        df = df[
            (df['price'] >= price_lower_bound) & 
            (df['price'] <= price_upper_bound) &
            (df['amount'] >= amount_lower_bound) & 
            (df['amount'] <= amount_upper_bound)
        ]
        
        print(f"Category scatter plot data shape after outlier removal: {df.shape}")
    
    print(f"Category scatter plot data types:\n{df.dtypes}")
    print(f"Category scatter plot data sample:\n{df.head()}")

    fig = px.scatter(df, x='price', y='amount',
                     title=f'Price vs Amount for {category_name}',
                     labels={'price': 'Price in Euros', 'amount': 'Amount in grams/milliliters'},
                     hover_data=['category_name', 'product_name'])
    
    fig.update_layout(
        hovermode='closest',
        showlegend=False,
        xaxis=dict(
            rangemode='tozero'
        ),
        yaxis=dict(
            rangemode='tozero'
        ),
        height=800  # Set the height to 800 pixels
    )
    
    return fig 