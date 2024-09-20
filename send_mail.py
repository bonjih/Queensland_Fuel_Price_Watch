from __future__ import print_function
import os
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
import base64

import global_conf_variables
from update_logging import UpdateLogger


class MailSender:
    def __init__(self):
        self.params = global_conf_variables.ParamsDict()
        self.logger = UpdateLogger(log_dir="logs", base_filename="update_errors", max_size_mb=5, backup_count=5)

        api_key = self.params.get_value('send_mail_key')
        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key['api-key'] = api_key
        self.api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))

    def encode_file_to_base64(self, file_path):
        """
        Encode the file content in base64.
        """
        try:
            with open(file_path, "rb") as file:
                return base64.b64encode(file.read()).decode("utf-8")
        except Exception as e:
            self.logger.log_error(f"Error encoding file {file_path}: {e}")
            return None

    def find_latest_file(self, directory):
        """
        Find the latest .txt file in the provided directory.
        """
        try:
            txt_files = [f for f in os.listdir(directory) if f.endswith('.txt')]
            if not txt_files:
                return None
            # Get the latest file based on modification time
            latest_file = max(txt_files, key=lambda f: os.path.getmtime(os.path.join(directory, f)))
            return os.path.join(directory, latest_file)
        except Exception as e:
            self.logger.log_error(f"Error finding latest .txt file in {directory}: {e}")
            return None

    def send_email_with_attachment(self):
        """
        Send an email with the latest log file attached.
        """
        to_email = self.params.get_value('to_email')
        logs_directory = self.params.get_value('log_files')
        latest_file_path = self.find_latest_file(logs_directory)

        if latest_file_path:
            encoded_file = self.encode_file_to_base64(latest_file_path)
            if encoded_file:
                file_name = os.path.basename(latest_file_path)
                email = sib_api_v3_sdk.SendSmtpEmail(
                    to=[{"email": to_email, "name": "QLD Fuel Prices"}],
                    sender={"email": to_email, "name": "QLD Fuel Prices"},
                    subject="QLD Fuel Prices Log",
                    html_content="<html><body><h1>Latest log file</h1></body></html>",
                    attachment=[
                        {
                            "content": encoded_file,
                            "name": file_name,
                            "contentType": "text/plain"
                        }
                    ]
                )

                try:
                    # Send the email
                    response = self.api_instance.send_transac_email(email)
                    self.logger.log_message(f"Email sent successfully: {response}")
                except ApiException as e:
                    self.logger.log_error(f"Exception when calling SMTPApi->send_transac_email: {e}")
        else:
            self.logger.log_message("No .txt files found in the logs directory.")

