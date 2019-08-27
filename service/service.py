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

from str_utils import str_to_bool

APP = Flask(__name__)
"""Property name in entities in incoming data that contains url to file to be downloaded"""
FILE_URL = os.environ.get('FILE_URL', 'file_url')
FILE_NAME = os.environ.get('FILE_NAME', 'file_id')
# chunk size 10Mb
CHUNK_SIZE = os.environ.get('CHUNK_SIZE', 262144 * 4 * 10)

UPLOAD_URL = os.environ.get('UPLOAD_URL')

FAIL_FAST_ON_ERROR = str_to_bool(os.environ.get('FAIL_FAST_ON_ERROR', 'false'))

if not UPLOAD_URL:
    logging.error("Target endpoint not defined. Check UPLOAD_URL env. variable  in your config.")
    exit(1)

logging.getLogger().setLevel(logging.DEBUG)


@APP.route("/transfer", methods=['POST'])
def process():
    """
    transfer service entry point
    :return:
    """
    input_data = request.get_json()

    for input_entity in input_data:
        file_url = input_entity[FILE_URL]
        file_name = input_entity[FILE_NAME]
        local_path = input_entity['local_path']

        logging.info(f"processing request for {file_name}")

        try:
            res = requests.get(file_url, stream=True)
            res.raise_for_status()
            file_path = download_file(res)

            logging.debug(f"Starting upload file {file_name} to {UPLOAD_URL}")

            file_to_upload = open(file_path, 'rb')
            requests.post(UPLOAD_URL, files={file_name: (file_name, file_to_upload)},
                          headers={"local_path": local_path})

            logging.debug(f"Deleting temporary file {file_path}")

            file_to_upload.close()
            input_entity['transfer_service'] = "TRANSFERRED"
        except Exception as exc:
            input_entity['transfer_service'] = f"ERROR: {exc}"
            if FAIL_FAST_ON_ERROR:
                raise exc
        finally:
            if file_path:
                os.remove(file_path)
    return Response(json.dumps(input_data), content_type='application/json')


def download_file(res: requests.Response) -> str:
    """
    function that downloads a binary content of input response object and
    stores into NamedTemporaryFile in file system
    :param res: requests.Response instance
    :return: stored file name (path)
    """
    logging.debug("Chunked file download started")
    with tempfile.NamedTemporaryFile(delete=False) as file:
        for chunk in res.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:
                file.write(chunk)
        file.close()
    logging.debug(f"File stored as {file.name}")
    return file.name


if __name__ == "__main__":
    APP.run(threaded=True, debug=True, host='0.0.0.0', port=5000)
