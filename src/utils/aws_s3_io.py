import csv
import logging
from dataclasses import asdict, fields
from io import StringIO
from urllib.parse import urlparse

import boto3
import botocore

# from botocore.errorfactory import ClientError

csv.register_dialect("pipe", delimiter="|", quoting=csv.QUOTE_STRINGS)


def get_s3_client(s3_region: str = ""):
    if s3_region:
        client = boto3.client("s3", region_name=s3_region)
    else:
        client = boto3.client("s3")
    return client


def uf_read_delim_file_to_list_of_dict(
    s3_obj_uri: str, s3_region: str = "", delim: str = ",", s3_client=None
) -> list[dict]:
    if not s3_client:
        s3_client = get_s3_client(s3_region=s3_region)
    s3_bucket, s3_obj_key = parse_s3_uri(s3_obj_uri=s3_obj_uri)
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_obj_key)
    stream = StringIO(response["Body"].read().decode("utf-8"))

    if delim == "|":
        reader = csv.DictReader(stream, dialect="pipe")
    else:
        reader = csv.DictReader(stream)

    file_records: list[dict] = [row for row in reader]

    try:
        if file_records:
            return file_records
        else:
            raise ValueError("Error in reading the file.")
    except ValueError as error:
        logging.error(error)
        raise


def parse_s3_uri(s3_obj_uri: str) -> tuple:
    parsed = urlparse(s3_obj_uri)
    s3_bucket = parsed.netloc
    s3_obj_key = parsed.path.lstrip("/")
    return s3_bucket, s3_obj_key


def build_s3_uri(s3_bucket: str, s3_prefix: str):
    return f"s3://{s3_bucket}/{s3_prefix}"


def uf_merge_csv_files(
    out_s3_obj_uri: str,
    in_s3_bucket: str,
    in_s3_prefix: str,
    s3_client,
) -> None:
    logging.info(
        "Listing files in bucket %s with prefix %s", in_s3_bucket, in_s3_prefix
    )
    logging.warning("Listing is limited to 1000 objects.")
    in_csv_objects = s3_client.list_objects_v2(Bucket=in_s3_bucket)
    # in_csv_objects = s3_client.list_objects_v2(Bucket=in_s3_bucket, Prefix=in_s3_prefix, Delimiter='/')

    all_obj_records = []
    for obj in [
        obj for obj in in_csv_objects["Contents"] if obj["Key"].startswith(in_s3_prefix)
    ]:
        obj_records = uf_read_delim_file_to_list_of_dict(
            # s3_obj_uri=f"s3://{in_s3_bucket}/{obj["Key"]}", s3_client=s3_client
            s3_obj_uri=build_s3_uri(s3_bucket=in_s3_bucket, s3_prefix=obj["Key"]),
            s3_client=s3_client,
        )
        all_obj_records += obj_records

    uf_write_list_of_dict_to_delim_file(
        dict_list=all_obj_records, file_uri=out_s3_obj_uri, s3_client=s3_client
    )


def uf_write_list_of_data_cls_obj_to_delim_file(
    dataclass_obj_list: list,
    file_uri: str,
    delim: str = ",",
    s3_client=None,
):
    field_names = []
    try:
        if dataclass_obj_list:
            dataclass_obj = dataclass_obj_list[0]
            field_names = [fld.name for fld in fields(dataclass_obj)]
        else:
            raise RuntimeError("No data in the dataclass object list.")
    except RuntimeError as error:
        logging.error(error)

    stream = StringIO()

    if delim == "|":
        writer = csv.DictWriter(stream, dialect="pipe", fieldnames=field_names)
    else:
        writer = csv.DictWriter(stream, fieldnames=field_names)

    writer.writeheader()
    writer.writerows([asdict(obj) for obj in dataclass_obj_list])

    s3_bucket, s3_obj_key = parse_s3_uri(s3_obj_uri=file_uri)
    logging.debug("Bucket: %s, Key: %s", s3_bucket, s3_obj_key)
    csv_string_object = stream.getvalue()
    s3_client.put_object(Body=csv_string_object, Bucket=s3_bucket, Key=s3_obj_key)


def uf_write_list_of_dict_to_delim_file(
    dict_list: list,
    file_uri: str,
    delim: str = ",",
    s3_client=None,
):
    field_names = []
    try:
        if dict_list:
            dict_one = dict_list[0]
            field_names = [fld for fld in dict_one.keys()]
        else:
            raise RuntimeError("No data in the dict object list.")
    except RuntimeError as error:
        logging.error(error)

    stream = StringIO()

    if delim == "|":
        writer = csv.DictWriter(stream, dialect="pipe", fieldnames=field_names)
    else:
        writer = csv.DictWriter(stream, fieldnames=field_names)

    writer.writeheader()
    writer.writerows(dict_list)

    s3_bucket, s3_obj_key = parse_s3_uri(s3_obj_uri=file_uri)
    logging.debug("Bucket: %s, Key: %s", s3_bucket, s3_obj_key)
    csv_string_object = stream.getvalue()
    s3_client.put_object(Body=csv_string_object, Bucket=s3_bucket, Key=s3_obj_key)


def uf_write_image_file(
    image_content,
    file_uri: str,
    s3_client=None,
):
    s3_bucket, s3_obj_key = parse_s3_uri(s3_obj_uri=file_uri)
    logging.info("Bucket: %s, Key: %s", s3_bucket, s3_obj_key)
    s3_client.put_object(Body=image_content, Bucket=s3_bucket, Key=s3_obj_key)


def s3_obj_exists(s3_obj_uri: str, s3_client=None):
    try:
        s3_bucket, s3_obj_key = parse_s3_uri(s3_obj_uri=s3_obj_uri)
        logging.info("Bucket: %s, Key: %s", s3_bucket, s3_obj_key)
        s3_client.head_object(Bucket=s3_bucket, Key=s3_obj_key)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            # The key does not exist.
            logging.info("%s does not exist.", s3_obj_key)
            logging.info(e.response)
            return False
        elif e.response["Error"]["Code"] == "403":
            # Unauthorized, including invalid bucket
            logging.info("Un-authorized or Invalid bucket.")
            raise
        else:
            # Something else has gone wrong.
            logging.info("Unknown error.")
            raise
    return True
