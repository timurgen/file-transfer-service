Simple file transfer service for Sesam.io powered applications

[![Build Status](https://travis-ci.org/sesam-community/file-transfer-service.svg?branch=master)](https://travis-ci.org/sesam-community/file-transfer-service)

## Usage
Service has only one endpoint `POST:/transfer` that takes list of json entities as input and return
(currently) nothing. Each entity must contain url to attachment and name of file.
Default properties `file_url`, `file_id` and `content_type`  (may be overridden with FILE_URL/FILE_ID/CONTENT_TYPE env variables)

Downloaded attachment will then be uploaded to URL from env var UPLOAD_URL (required)

**Required env vars:**
* UPLOAD_URL   # Base url for target system

**Optional env vars:**
* FILE_URL            default: "file_url"       # url to source file
* FILE_NAME           default: "file_id"        # source file name
* CONTENT_TYPE        default: "content_type"   # source content type
* TARGET_PATH         default: "local_path"     # Target file path on receiving system
* TARGET_PATH_IN_URL  default: "false"          # If target_path should be sent in url instead of as header
* CHUNK_SIZE          default: 262144 * 4 * 10  # chunk size 10Mb
* FAIL_FAST_ON_ERROR  default: false            # If the transfer should fail at first error or try the whole batch


