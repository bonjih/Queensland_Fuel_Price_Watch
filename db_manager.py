from datetime import datetime
from sqlalchemy import create_engine, text
from send_mail import MailSender
from update_logging import UpdateLogger

logger = UpdateLogger(log_dir="logs", base_filename="update_errors", max_size_mb=5, backup_count=5)


class SQL:
    def __init__(self, user, pwd, host, db):
        self.engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}/{db}?charset=latin1")

    def append(self, df, table_name):
        """Append rows to the specified table."""
        try:
            if table_name != 'qld_fuel_prices_main':
                df['updated'] = datetime.now()
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            logger.log_message(f"Data appended to table '{table_name}' successfully.")
        except Exception as e:
            logger.log_error(f"Failed to append data into table '{table_name}': {str(e)}")
            MailSender().send_email_with_attachment()

    def replace(self, df, table_name):
        """Replace the entire content of the specified table."""
        try:
            if table_name != 'qld_fuel_prices_main':
                df['updated'] = datetime.now()
            df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)
            logger.log_message(f"Data replaced in table '{table_name}' successfully.")
        except Exception as e:
            logger.log_error(f"Failed to replace data in table '{table_name}': {str(e)}")
            MailSender().send_email_with_attachment()

    def value_exists(self, table_name, column_name, value):
        """Check if a specific value exists in a column of the specified table."""
        try:
            query = text(f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE {column_name} = :value)")
            with self.engine.connect() as connection:
                result = connection.execute(query, {'value': value}).scalar()
            return bool(result)
        except Exception as e:
            error_message = f"Error checking value in table '{table_name}': {str(e)}"
            logger.log_error(error_message)
            return False
