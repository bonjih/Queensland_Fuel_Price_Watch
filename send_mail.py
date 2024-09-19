from __future__ import print_function
import os
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
import base64

import global_conf_variables
from update_logging import UpdateLogger

params = global_conf_variables.ParamsDict()
logger = UpdateLogger(log_dir="logs", base_filename="update_errors", max_size_mb=5, backup_count=5)

api_key = params.get_value('send_mail_key')
configuration = sib_api_v3_sdk.Configuration()
configuration.api_key['api-key'] = api_key
api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))


# read the file and encode in base64
def encode_file_to_base64(file_path):
    with open(file_path, "rb") as file:
        return base64.b64encode(file.read()).decode("utf-8")


# find the latest .txt file in a directory
def find_latest_txt_file(directory):
    txt_files = [f for f in os.listdir(directory) if f.endswith('.txt')]
    if not txt_files:
        return None
    # Get the latest file based on modification time
    latest_file = max(txt_files, key=lambda f: os.path.getmtime(os.path.join(directory, f)))
    return os.path.join(directory, latest_file)


logs_directory = params.get_value('log_files')
latest_file_path = find_latest_txt_file(logs_directory)

if latest_file_path:
    encoded_file = encode_file_to_base64(latest_file_path)
    file_name = os.path.basename(latest_file_path)

    email = sib_api_v3_sdk.SendSmtpEmail(
        to=[{"email": "bonjih@gmail.com", "name": "QLD Fuel Prices"}],
        sender={"email": "bonjih@gmail.com", "name": "QLD Fuel Prices"},
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
        response = api_instance.send_transac_email(email)
    except ApiException as e:
        logger.log_error(f"Exception when calling SMTPApi->send_transac_email: {e}")
else:
    logger.log_message("No .txt files found in the logs directory.")
