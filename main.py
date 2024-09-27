from datetime import datetime, timedelta
import pandas as pd

from consolidated_fuel_data import consolidate_fuel_tables
from api_requests import fetch_currency_data, fetch_brent_data
from db_manager import SQL
from fuel_data import FuelDataAPI
import global_conf_variables
from transforms import results_to_dataframe
from update_logging import UpdateLogger

# Load parameters
params = global_conf_variables.ParamsDict()
user = params.get_value('user')
pwd = params.get_value('passwd')
host = params.get_value('host')
db = params.get_value('database')
token_curr = params.get_value('currency_key')
token_brent = params.get_value('brent_key')

logger = UpdateLogger(log_dir="logs", base_filename="update_main", max_size_mb=5, backup_count=5)


def update_fuel_prices(s, api):
    """Fetch and update fuel prices in the database."""
    logger.log_message("Starting update of fuel prices...")

    # Fetch recent fuel data
    data = api.fetch_all_data()
    frames = results_to_dataframe(data)

    # Consolidate data into the main table and update 'qld_fuel_prices_main'
    main_table_update = consolidate_fuel_tables(frames)

    # Append table 'qld_fuel_prices_main'
    if not main_table_update.empty:
        s.append(main_table_update, 'qld_fuel_prices_main')
        logger.log_message("Fuel prices updated in 'qld_fuel_prices_main'.")

    # Update other tables from frames
    for table_name, df in frames.items():
        if not df.empty:
            if table_name in ['site_prices', 'exchange_rates']:
                s.append(df, table_name)
                logger.log_message(f"Data appended to '{table_name}'.")
            else:
                s.replace(df, table_name)
                logger.log_message(f"Data replaced in '{table_name}'.")


def update_exchange_rate(s, t):
    """Check and update exchange rates in the database."""
    logger.log_message("Starting update of exchange rates...")

    today_date = datetime.now()
    start_date = (today_date - timedelta(days=1)).strftime('%Y-%m-%d')
    today_date = today_date.strftime('%Y-%m-%d')

    # Check if the start_date exists in the 'exchange_rates' table
    if not s.value_exists('exchange_rates', 'date', start_date):
        # Fetch recent exchange rate data
        ex_value = fetch_currency_data(t, start_date, today_date)
        most_recent_date = max(ex_value['response'].keys())
        aud_value = ex_value['response'][most_recent_date]['AUD']

        # Calculate USD from AUD
        usd_value = 1 / aud_value if aud_value != 0 else None

        # append rates
        s.append(pd.DataFrame({
            'date': [start_date],
            'aud': [aud_value],
            'usd': [usd_value],
            'updated': [datetime.now()]
        }), 'exchange_rates')
        logger.log_message("Exchange rate updated in 'exchange_rates'.")

    # Update qld_fuel_prices_main with exchange rates where AUDtoUSD is NULL
    s.update_main_with_exchange_rate()


def update_brent_crude(s, t):
    today_date = datetime.now()
    start_date = (today_date - timedelta(days=1)).strftime('%Y-%m-%d')
    today_date = today_date.strftime('%Y-%m-%d')

    data = fetch_brent_data(t, period=500)
    data = data.get("response", {}).get("data", [])
    df = pd.DataFrame(data)

    df = df[['period', 'value']]

    print(df)

    # if not s.value_exists('exchange_rates', 'date', start_date):
    #     # Fetch recent exchange rate
    #     ex_value = fetch_brent_data(t, start_date)
    #     most_recent_date = max(ex_value['response'].keys())
    #     aud_value = ex_value['response'][most_recent_date]['AUD']
    #     # Calculate USD from AUD
    #     usd_value = 1 / aud_value if aud_value != 0 else None
    #     s.insert_exchange_rate(start_date, aud_value, usd_value)


if __name__ == "__main__":
    sql = SQL(user=user, pwd=pwd, host=host, db=db)
    fuel_api = FuelDataAPI(token=params.get_value('token'))

    # Update exchange rate
    update_exchange_rate(sql, token_curr)

    # Update fuel prices
    #update_fuel_prices(sql, fuel_api)


    # Update Brent Crude
    # update_brent_crude(sql, token_brent)
