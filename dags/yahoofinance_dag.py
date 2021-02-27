# Airflow imports
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Date, Yahoo Finance, and Pandas
from datetime import timedelta
import yfinance as yf
import pandas as pd

def download_market_data(ticker):
    """
    Downloads market data for given ticker between yesterday and today in 1m intervals
    
    Args: 
        ticker: ticker to download
    Returns: 
        None; Saves data as csv in working directory
    """

    # Download data for yesterday and save as CSV
    start_date = days_ago(1)
    end_date = start_date + timedelta(days = 1)
    df = yf.download(ticker, start = start_date, end = end_date, interval = '1m')
    df.to_csv(f'{ticker}_data.csv', header = False)

def query_data(**kwargs):
    """
    Queries data for given tickers and outputs daily low and high for those tickers

    Args: 
        **kwargs: contains 'tickers' list and Airflow context variables
    Returns: 
        None; writes files to date-specific folders
    """

    # Get variables from kwargs, create empty dataframe with columns
    ds = kwargs['ds']
    tickers = kwargs['tickers']
    combined_df = pd.DataFrame()

    for ticker in tickers:

        # Load data, add ticker column, append to combined_df
        df = pd.read_csv(f'~/data/{ds}/{ticker}_data.csv', header = None)
        df[len(df.columns)] = f'{ticker}'
        combined_df = combined_df.append(df, ignore_index = True)

    # Set column names, add date column, round to 2 decimals
    combined_df.columns = ['date_time', 'open', 'high', 'low', 'close', 'adj_close', 'volume', 'ticker']
    combined_df['date'] = combined_df['date_time'].str[:10]
    combined_df = combined_df.round(2)

    # Aggregate data by ticker, date and write to CSV
    daily_agg_df = combined_df.groupby(['ticker', 'date']).agg(daily_high = ('high', 'max'), daily_low = ('low', 'min'))
    daily_agg_df.to_csv(f'~/data/{ds}/output_data.csv')

# Set retries in default args
default_args = {
    'retry_delay': timedelta(minutes = 5),
    'retries': 2
}

# Build dag
dag = DAG(
    'yahoo_finance_pipeline',
    default_args = default_args,
    description = 'A simple DAG using Yahoo Finance data',
    #schedule_interval = '0 18 * * 1-5',
    schedule_interval = '0 17 * * *',
    start_date = days_ago(2)
)

# Initialize temp directory for data
t0 = BashOperator(
    task_id = 'init_temp_directory',
    bash_command = 'mkdir -p ~/data/{{ ds }}',
    dag = dag
)

# Download AAPL market data
t1 = PythonOperator(
    task_id = 'download_AAPL',
    python_callable = download_market_data,
    op_args = ['AAPL'],
    dag = dag
)

# Download TSLA market data
t2 = PythonOperator(
    task_id = 'download_TSLA',
    python_callable = download_market_data,
    op_args = ['TSLA'],
    dag = dag
)

# Move AAPL to directory
t3 = BashOperator(
    task_id = 'move_AAPL',
    bash_command = 'mv ~/AAPL_data.csv ~/data/{{ ds }}',
    dag = dag
)

# Move TSLA to directory
t4 = BashOperator(
    task_id = 'move_TSLA',
    bash_command = 'mv ~/TSLA_data.csv ~/data/{{ ds }}',
    dag = dag
)

# Query data and output new file
t5 = PythonOperator(
    task_id = 'query_data',
    python_callable = query_data,
    op_kwargs = {'tickers': ['AAPL', 'TSLA']},
    provide_context = True,
    dag = dag
)

# Set job dependencies
t0 >> [t1, t2]
t1 >> t3
t2 >> t4
[t3, t4] >> t5