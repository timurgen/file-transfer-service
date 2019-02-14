import requests
import os
import tempfile
import logging
import json

from flask import Flask, Response, request

APP = Flask(__name__)
"""Property name in entities in incoming data that contains url to file to be downloaded"""
FILE_URL = os.environ.get('FILE_URL', 'file_url')
FILE_NAME = os.environ.get('FILE_NAME', 'file_id')
# chunk size 10Mb
CHUNK_SIZE = os.environ.get('CHUNK_SIZE', 262144 * 4 * 10)

UPLOAD_URL = os.environ.get('UPLOAD_URL')

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

        logging.info("processing request for {}".format(file_name))

        try:
            res = requests.get(file_url, stream=True)

            res.raise_for_status()

            file_path = download_file(res)

            logging.debug("Starting upload file {} to {}".format(file_name, UPLOAD_URL))

            file_to_upload = open(file_path, 'rb')
            requests.post(UPLOAD_URL, files={file_name: (file_name, file_to_upload)},
                          headers={"local_path": local_path})

            logging.debug("Deleting temporary file {}".format(file_path))

            file_to_upload.close()
            input_entity['transfer_service'] = "TRANSFERRED"
        except Exception as e:
            input_entity['transfer_service'] = "ERROR: {}".format(str(e))
        finally:
            os.remove(file_path)
    return Response(json.dumps(input_data), content_type='application/json')


def download_file(res):
    logging.debug("Chunked file download started")
    with tempfile.NamedTemporaryFile(delete=False) as file:
        for chunk in res.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:
                file.write(chunk)
        file.close()
    logging.debug("File stored as {}".format(file.name))
    return file.name


if __name__ == "__main__":
    APP.run(threaded=True, debug=True, host='0.0.0.0', port=5000)
