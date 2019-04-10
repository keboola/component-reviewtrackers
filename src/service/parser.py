import os
import json
import logging
import pandas as pd


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
    "metadata_blank",
    "name",
    "permalink",
    "published_at",
    "rating",
    "respondable",
    "source_code",
    "source_name",
    "url_metadata_google_serp"
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
    "metadata_dealer_id",
    "metadata_import_id",
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
    """
    Extracting Reviews from input data
    """

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

    return data_out, review_header


def _location_parse(data_in):
    """
    Extract Location Info from input data
    """

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

    return data_out, location_header


def _response_parse(data_in):
    """
    Parsing JSON responses
    """

    data_out = []

    for entity in data_in:
        temp = {}
        for header in response_header:
            if header in entity:
                temp[header] = entity[header]
            else:
                temp[header] = ""
        data_out.append(temp)

    return data_out, response_header


def _produce_manifest(file_name, primary_key):
    """
    Create manifest file
    """

    file = "/data/out/tables/" + str(file_name)+".csv.manifest"

    manifest = {
        "incremental": True,
        "primary_key": [primary_key]
    }
    logging.debug(manifest)
    try:
        with open(file, 'w') as file_out:
            json.dump(manifest, file_out)
            logging.info(
                "Output manifest file [{0}] produced.".format(file_name))
    except Exception as e:
        logging.error("Could not produce output file manifest.")
        logging.error(e)


def _output(filename, headers, data_in):

    dest = DEFAULT_TABLE_DESTINATION + filename + ".csv"
    data = pd.DataFrame(data_in)

    # If file is empty
    if len(data) == 0:
        logging.info("[{0}] contains no data.".format(filename))
        pass
    else:
        if os.path.isfile(dest):
            with open(dest, 'a') as b:
                data.to_csv(b, index=False, header=False, columns=headers)
            b.close()
        else:
            with open(dest, 'w+') as b:
                data.to_csv(b, index=False, header=True, columns=headers)
            b.close()

            # Output Manifest
            _produce_manifest(filename, "id")

    return


def parse(data_in, endpoint):
    """
    Different Parsing Method for different endpoint
    """

    if endpoint == "reviews":
        data_out, header = _review_parse(data_in)
        # _output(endpoint, review_header, data_out)
    elif endpoint == "locations":
        data_out, header = _location_parse(data_in)
        # _output(endpoint, location_header, data_out)
    elif endpoint == "responses":
        data_out, header = _response_parse(data_in)
        # _output(endpoint, response_header, data_out)

    # Output
    _output(endpoint, header, data_out)

    return
