# db_manager.py

from datetime import datetime
from sqlalchemy import create_engine, text
import pandas as pd

from send_mail import MailSender
from update_logging import UpdateLogger

logger = UpdateLogger(log_dir="logs", base_filename="update_errors", max_size_mb=5, backup_count=5)


class SQL:
    def __init__(self, user, pwd, host, db):
        self.engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}/{db}?charset=latin1")

    def insert(self, df, table_name):
        """Insert a DataFrame into the specified table with conditional logic for different tables."""
        try:
            # Add 'updated' column with the current datetime for all tables except 'qld_fuel_prices_main'
            if table_name != 'qld_fuel_prices_main':
                df['updated'] = datetime.now()

            # Set the insert mode based on the table
            if_exists = 'append' if table_name in ['sites_prices', 'qld_fuel_prices_main'] else 'replace'

            # Insert the DataFrame into the SQL table
            df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)

            # Log success message
            message = f"Data inserted into table '{table_name}' with {if_exists} mode."
            logger.log_message(message)

        except Exception as e:
            # Log error and send email in case of failure
            error_message = f"Failed to insert data into table '{table_name}': {str(e)}"
            logger.log_error(error_message)

            mail_sender = MailSender()
            mail_sender.send_email_with_attachment()

    def insert_exchange_rate(self, date, aud, usd):
        """Insert exchange rate data into the exchange_rates table."""
        try:
            # DataFrame for the exchange rate values
            df = pd.DataFrame({
                'date': [date],
                'aud': [aud],
                'usd': [usd],
                'updated': [datetime.now()]
            })

            # Insert the exchange rate DataFrame into the 'exchange_rates' table
            self.insert(df, 'exchange_rates')

        except Exception as e:
            # Log error if insert fails
            error_message = f"Failed to insert exchange rate data: {str(e)}"
            logger.log_error(error_message)

    def value_exists(self, table_name, column_name, value):
        """Check if a specific value exists in a column of the specified table."""
        try:
            # Query to check if the value exists in the table
            query = text(f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE {column_name} = :value)")

            # Execute the query and return whether the value exists
            with self.engine.connect() as connection:
                result = connection.execute(query, {'value': value}).scalar()

            return bool(result)

        except Exception as e:
            # Log error if the check fails
            error_message = f"Error checking value in table '{table_name}': {str(e)}"
            logger.log_error(error_message)
            return False
