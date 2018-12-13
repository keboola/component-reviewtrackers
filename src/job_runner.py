import os
from service.api_client import request_endpoint
from service.flattener import collection_flattener

DEFAULT_TABLE_SOURCE = "/data/in/tables/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"


def output(filename, data):

    dest = DEFAULT_TABLE_DESTINATION + filename + ".csv"

    if os.path.isfile(dest):
        with open(dest, 'a') as b:
            data.to_csv(b, index=False, header=False)
        b.close()
    else:
        with open(dest, 'w+') as b:
            data.to_csv(b, index=False, header=True)
        b.close()


def tester():
    username = "data_ca@keboola.com"
    token = "yO9UBwnYHdq3D0JYHrMHgkHaAns="
    account_id = "5c09425a297f4a2229509d2e"

    params = {
        'account_id': account_id
    }

    endpoints = [
        "profiles",
        "notes"
    ]

    for endpoint in endpoints:
        json_res = request_endpoint(username, token, endpoint, params)
        result_df = collection_flattener(json_res, endpoint)
        output(endpoint, result_df)
