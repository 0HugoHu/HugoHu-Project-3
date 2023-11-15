"""
extract data from GitHub url
"""

import base64
import json
import os

import requests

server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/HugoHu_Project_3"
headers = {"Authorization": "Bearer %s" % access_token}
url = "https://" + server_h + "/api/2.0"


def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request(
        "POST", url + path, data=json.dumps(data), verify=True, headers=headers
    )
    return resp.json()


def mkdirs(path, headers):
    _data = {}
    _data["path"] = path
    return perform_query("/dbfs/mkdirs", headers=headers, data=_data)


def create(path, overwrite, headers):
    _data = {}
    _data["path"] = path
    _data["overwrite"] = overwrite
    return perform_query("/dbfs/create", headers=headers, data=_data)


def add_block(handle, data, headers):
    _data = {}
    _data["handle"] = handle
    _data["data"] = data
    return perform_query("/dbfs/add-block", headers=headers, data=_data)


def close(handle, headers):
    _data = {}
    _data["handle"] = handle
    return perform_query("/dbfs/close", headers=headers, data=_data)


def get_file_from_url(url, dbfs_path, overwrite, headers):
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        handle = create(dbfs_path, overwrite, headers=headers)["handle"]
        print("Getting file: " + dbfs_path)
        for i in range(0, len(content), 2**20):
            add_block(
                handle,
                base64.standard_b64encode(content[i : i + 2**20]).decode(),
                headers=headers,
            )
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print(
            f"Error downloading file from {url}. Status code: {response.status_code}"
        )


def extract(
    url="""https://github.com/nogibjj/HugoHu-Project-3/blob/main/shopping_trends_updated.csv?raw=true""",
    file_path=FILESTORE_PATH + "/shopping_trends.csv",
    directory=FILESTORE_PATH,
    overwrite=True,
):
    """Extract a url to a file path"""
    mkdirs(path=directory, headers=headers)
    get_file_from_url(url, file_path, overwrite, headers=headers)

    return file_path


if __name__ == "__main__":
    extract()
