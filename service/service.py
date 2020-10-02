"""
Simple service to download files from URL's extracted from Sesam data entities and upload
them to another URL
"""
import os
import tempfile
import logging
import json

import requests
from flask import Flask, Response, request
from sesamutils import VariablesConfig, sesam_logger
from sesamutils.flask import serve

from str_utils import str_to_bool

APP = Flask(__name__)

logger = sesam_logger("file-transfer-service", app=APP)

required_env_vars = ["UPLOAD_URL"]
optional_env_vars = [("FILE_URL", "file_url"),
                     ("FILE_NAME", "file_id"),
                     ("CONTENT_TYPE", "content_type"),
                     ("TARGET_PATH", "local_path"),
                     ("TARGET_PATH_IN_URL", "false"),
                     ("CHUNK_SIZE", 262144 * 4 * 10),  # chunk size 10Mb
                     ("FAIL_FAST_ON_ERROR", "false"),
                     ("LOG_LEVEL", "INFO")]
config = VariablesConfig(required_env_vars, optional_env_vars)

if not config.validate():
    exit(1)


@APP.route("/transfer", methods=['POST'])
def process():
    """
    transfer service entry point
    :return:
    """
    input_data = request.get_json()

    failures = False

    for input_entity in input_data:
        file_url = input_entity[config.FILE_URL]
        file_name = input_entity[config.FILE_NAME]
        target_path = input_entity[config.TARGET_PATH]
        content_type = input_entity[config.CONTENT_TYPE]

        logger.info(f"processing request for {file_name}")

        file_path = None

        try:
            res = requests.get(file_url, stream=True)
            res.raise_for_status()
            file_path = download_file(res)

            logger.debug(f"Starting upload file {file_name} to {config.UPLOAD_URL}")

            file_to_upload = open(file_path, 'rb')
            if content_type is not None:
                files = {file_name: (file_name, file_to_upload, content_type)}
            else:
                files = {file_name: (file_name, file_to_upload)}
            if str_to_bool(config.TARGET_PATH_IN_URL):
                upload_url = '/'.join(s.strip('/') for s in [config.UPLOAD_URL, target_path])
                send_resp = requests.post(upload_url, files=files)
            else:
                send_resp = requests.post(config.UPLOAD_URL,
                                          files=files,
                                          headers={"local_path": target_path})
            send_resp.raise_for_status()

            file_to_upload.close()
            input_entity['transfer_service'] = "TRANSFERRED"
        except Exception as exc:
            logger.error(f"Error when transferring file {file_name} to {config.UPLOAD_URL}: '{exc}'")
            input_entity['transfer_service'] = f"ERROR: {exc}"
            if str_to_bool(config.FAIL_FAST_ON_ERROR):
                raise exc
            failures = True
        finally:
            if file_path:
                logger.debug(f"Deleting temporary file {file_path}")
                os.remove(file_path)

    if failures:
        # Return error after proccessing all entities in batch if one of them resulted in error.
        return Response(status=500, response=json.dumps(input_data), content_type='application/json')
    return Response(json.dumps(input_data), content_type='application/json')


def download_file(res: requests.Response) -> str:
    """
    function that downloads a binary content of input response object and
    stores into NamedTemporaryFile in file system
    :param res: requests.Response instance
    :return: stored file name (path)
    """
    logger.debug("Chunked file download started")
    with tempfile.NamedTemporaryFile(delete=False) as file:
        for chunk in res.iter_content(chunk_size=int(config.CHUNK_SIZE)):
            if chunk:
                file.write(chunk)
        file.close()
    logger.debug(f"File stored as {file.name}")
    return file.name


if __name__ == "__main__":

    IS_DEBUG_ENABLED = logger.isEnabledFor(logging.DEBUG)

    if IS_DEBUG_ENABLED:
        APP.run(debug=IS_DEBUG_ENABLED, host='0.0.0.0', port=5000)
    else:
        serve(APP, config={'server.max_request_body_size': 0,
                           'server.socket_timeout': 60})
