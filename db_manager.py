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
        """Insert a DataFrame into the specified table. Append for 'sites_prices' and replace for other tables."""
        try:
            # Add 'updated' column with the current datetime
            df['updated'] = datetime.now()

            # Check if the table is 'sites_prices' and append, else replace
            if table_name == 'sites_prices':
                if_exists_option = 'append'
            else:
                if_exists_option = 'replace'  # This will delete all rows and replace with new data

            # Insert data into the table
            df.to_sql(table_name, con=self.engine, if_exists=if_exists_option, index=False)
            print(f"Data inserted into table '{table_name}' with {if_exists_option} mode.")
        except Exception as e:
            print(f"Failed to insert data into table '{table_name}': {str(e)}")