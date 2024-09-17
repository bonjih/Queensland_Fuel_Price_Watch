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

    for key, df in frames.items():
        if not df.empty:
            sql.insert_dataframe(df, key)
