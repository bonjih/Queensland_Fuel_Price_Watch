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

    def update_main_with_exchange_rate(self):
        """Update qld_fuel_prices_main with exchange rates where AUDtoUSD is NULL."""
        check_nulls_query = text("""
            SELECT COUNT(*) FROM qld_fuel_prices_main WHERE AUDtoUSD IS NULL;
        """)

        try:
            with self.engine.connect() as connection:
                null_count = connection.execute(check_nulls_query).scalar()
                if null_count > 0:
                    update_query = text("""
                        UPDATE qld_fuel_prices_main q
                        JOIN exchange_rates e
                        ON DATE(q.TransactionDateUtc) = e.date
                        SET q.AUDtoUSD = e.usd
                        WHERE q.AUDtoUSD IS NULL;
                    """)
                    result = connection.execute(update_query)
                    updated_count = result.rowcount
                    logger.log_message(
                        f"qld_fuel_prices_main updated with {updated_count} rows where AUDtoUSD was NULL.")
                else:
                    logger.log_message("No NULL values found in AUDtoUSD; skipping update.")
        except Exception as e:
            logger.log_error(f"Failed to update qld_fuel_prices_main with exchange rates: {str(e)}")

