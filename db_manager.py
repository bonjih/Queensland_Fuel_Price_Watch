from datetime import datetime
from sqlalchemy import create_engine


class SQL:
    def __init__(self, user, pwd, host, db):
        self.user = user
        self.pwd = pwd
        self.host = host
        self.db = db

        self.engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}/{db}?charset={'latin1'}")

    def insert_dataframe(self, df, table_name):
        """Insert a DataFrame into the specified table. Append for 'sites_prices' and replace for others."""
        try:
            # Add 'updated' column with the current datetime
            df['updated'] = datetime.now()

            # Special handling for 'sites_prices' table
            if table_name == 'sites_prices':
                if_exists = 'append'  # Append for sites_prices table
            else:
                if_exists = 'replace'  # Replace for other tables

            df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
            print(f"Data inserted into table '{table_name}' with {if_exists} mode.")
        except Exception as e:
            print(f"Failed to insert data into table '{table_name}': {str(e)}")
