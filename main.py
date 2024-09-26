from datetime import datetime, timedelta
from consolidated_fuel_data import consolidate_fuel_tables
from api_requests import fetch_currency_data
from db_manager import SQL
from fuel_data import FuelDataAPI
import global_conf_variables
from transforms import results_to_dataframe

# Load parameters
params = global_conf_variables.ParamsDict()
user = params.get_value('user')
pwd = params.get_value('passwd')
host = params.get_value('host')
db = params.get_value('database')
token = params.get_value('currency_key')


def update_fuel_prices(sql, api):
    """Fetch and update fuel prices."""
    # Fetch recent fuel data
    data = api.fetch_all_data()
    frames = results_to_dataframe(data)

    # Consolidate data into the main table and update the 'qld_fuel_prices_main'
    main_table_update = consolidate_fuel_tables(frames)

    if not main_table_update.empty:
        sql.insert(main_table_update, 'qld_fuel_prices_main')

    # Update other related tables with new data
    for key, df in frames.items():
        if not df.empty:
            sql.insert(df, key)


def update_exchange_rate(s, t):
    """Check and update exchange rates in the database."""
    today_date = datetime.now()
    start_date = (today_date - timedelta(days=1)).strftime('%Y-%m-%d')
    today_date = today_date.strftime('%Y-%m-%d')

    # Check if the start_date exists in the 'exchange_rates' table
    if not s.value_exists('exchange_rates', 'date', start_date):
        # Fetch recent exchange rate
        ex_value = fetch_currency_data(t, start_date, today_date)
        most_recent_date = max(ex_value['response'].keys())
        aud_value = ex_value['response'][most_recent_date]['AUD']

        # Calculate USD from AUD
        usd_value = 1 / aud_value if aud_value != 0 else None
        s.insert_exchange_rate(start_date, aud_value, usd_value)


if __name__ == "__main__":
    sql = SQL(user=user, pwd=pwd, host=host, db=db)
    fuel_api = FuelDataAPI(token=params.get_value('token'))

    # Update fuel prices
    update_fuel_prices(sql, fuel_api)

    # Update exchange rate
    #update_exchange_rate(sql, token)
