from consolidated_fuel_data import consolidate_fuel_tables
from db_manager import SQL
from fuel_data import FuelDataAPI
import global_conf_variables
from transforms import results_to_dataframe

params = global_conf_variables.ParamsDict()

user = params.get_value('user')
pwd = params.get_value('passwd')
host = params.get_value('host')
db = params.get_value('database')

if __name__ == "__main__":
    sql = SQL(user=user, pwd=pwd, host=host, db=db)
    t = params.get_value('token')
    api = FuelDataAPI(token=t)
    data = api.fetch_all_data()

    frames = results_to_dataframe(data)

    # TODO push View logic to SQL
    # Consolidate data into a main table aka a View
    main_table_update = consolidate_fuel_tables(frames)

    # Insert 'View' into the 'qld_fuel_prices_main' table
    if not main_table_update.empty:
        sql.insert_dataframe(main_table_update, 'qld_fuel_prices_main')

    # Update the rest of the tables
    for key, df in frames.items():
        if not df.empty:
            sql.insert_dataframe(df, key)
