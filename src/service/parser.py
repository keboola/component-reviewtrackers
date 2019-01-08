import os
import pandas as pd
import json
import sys


DEFAULT_TABLE_SOURCE = "/data/in/tables/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"


review_header = [
        "account_id",
        "author",
        "business_response_url",
        "content",
        "created_at",
        "extra_text",
        "id",
        "location_id",
        "metadata_blank", # In
        "name",
        "permalink",
        "published_at",
        "rating",
        "respondable",
        "source_code",
        "source_name",
        "url_metadata_google_serp" # In
    ]
location_header = [
        "account_id",
        "address",
        "city",
        "country",
        "country_id",
        "created_at",
        "deleted_at",
        "external_id",
        "feedback_url",
        "google_place_id",
        "has_issue",
        "id",
        "latitude",
        "longitude",
        "metadata_dealer_id", # in
        "metadata_import_id", # in
        "mute_issues",
        "name",
        "oid",
        "phone",
        "public_name",
        "request_page_id",
        "request_page_url",
        "resource",
        "state",
        "state_id",
        "updated_at",
        "url_id",
        "zipcode"
    ]
response_header = [
        "account_id",
        "content",
        "created_at",
        "created_by_user_id",
        "deleted_at",
        "id",
        "location_id",
        "published_at",
        "read_only",
        "reference_id",
        "resource",
        "response_template_id",
        "review_id",
        "source_id",
        "status",
        "updated_at"
    ]


def _review_parse(data_in):

    data_out = []

    for entity in data_in:
        temp = {}
        for header in review_header:
            # if header in ["metadata_blank", "url_metadata_google_serp"]:
            if header == "metadata_blank" and "blank" in entity["metadata"]:
                temp["metadata_blank"] = entity["metadata"]["blank"]
            elif header == "url_metadata_google_serp" and "google_serp" in entity["url_metadata"]:
                temp["url_metadata_google_serp"] = entity["url_metadata"]["google_serp"]
            elif header in entity:
                temp[header] = entity[header]
            else:
                temp[header] = ""
        data_out.append(temp)

    return data_out


def _location_parse(data_in):

    data_out = []

    for entity in data_in:
        temp = {}
        for header in location_header:
            if header == "metadata_dealer_id" and "dealer_id" in entity["metadata"]:
                temp["metadata_dealer_id"] = entity["metadata"]["dealer_id"]
            elif header == "metadata_import_id" and "import_id" in entity["metadata"]:
                temp["metadata_import_id"] = entity["metadata"]["import_id"]
            elif header in entity:
                temp[header] = entity[header]
            else:
                temp[header] = ""
        data_out.append(temp)

    return data_out


def _response_parse(data_in):

    data_out = []

    for entity in data_in:
        temp = {}
        for header in response_header:
            if header in entity:
                temp[header] = entity[header]
            else:
                temp[header] = ""
        data_out.append(temp)

    return data_out


def _output(filename, headers, data_in):
    dest = DEFAULT_TABLE_DESTINATION + filename + ".csv"

    data = pd.DataFrame(data_in)

    if os.path.isfile(dest):
        with open(dest, 'a') as b:
            data.to_csv(b, index=False, header=False, columns=headers)
        b.close()
    else:
        with open(dest, 'w+') as b:
            data.to_csv(b, index=False, header=True, columns=headers)
        b.close()


def parse(data_in, endpoint):

    if endpoint == "reviews":
        data_out = _review_parse(data_in)
        _output(endpoint, review_header, data_out)
    elif endpoint == "locations": 
        data_out = _location_parse(data_in)
        _output(endpoint, location_header, data_out)
    elif endpoint == "responses":
        data_out = _response_parse(data_in)
        _output(endpoint, response_header, data_out)

    return
