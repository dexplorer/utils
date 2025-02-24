# import csvkit
# import os
import csv
import glob
from utils import file_io as uff
import logging
from dataclasses import fields, asdict

csv.register_dialect("pipe", delimiter="|", quoting=csv.QUOTE_STRINGS)


def uf_read_delim_file_to_list_of_dict(file_path: str, delim=",") -> list[dict]:
    with uff.uf_open_file(file_path=file_path, open_mode="r") as f:
        if delim == "|":
            reader = csv.DictReader(f, dialect="pipe")
        else:
            # reader = csv.DictReader(f, fieldnames=content_header) # omit fieldnames to use the first row as field names
            reader = csv.DictReader(f)

        file_records: list[dict] = [row for row in reader]

    try:
        if file_records:
            # print(file_records[:2])
            return file_records
        else:
            raise ValueError("Error in reading the file.")
    except ValueError as error:
        logging.error(error)
        raise


# remove this after replacing the references with the new func below
def merge_csv_files(in_file_dir_path: str, out_file: str) -> None:
    in_csv_files = glob.glob(f"{in_file_dir_path}/*.csv")

    with uff.uf_open_file(file_path=out_file, open_mode="w") as of:
        with uff.uf_open_file_list(files=in_csv_files) as fi:
            for line in fi:
                if fi.lineno() == 1 or fi.filelineno() > 1:
                    of.write(line)


def uf_merge_csv_files(
    in_file_dir_path: str, out_file: str, in_file_pattern: str = "*"
) -> None:
    in_csv_files = glob.glob(f"{in_file_dir_path}/{in_file_pattern}.csv")

    with uff.uf_open_file(file_path=out_file, open_mode="w") as of:
        with uff.uf_open_file_list(files=in_csv_files) as fi:
            for line in fi:
                if fi.lineno() == 1 or fi.filelineno() > 1:
                    of.write(line)


def uf_write_list_of_data_cls_obj_to_delim_file(
    dataclass_obj_list: list, file_path: str, delim=","
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

    with uff.uf_open_file(file_path=file_path, open_mode="w") as f:
        if delim == "|":
            writer = csv.DictWriter(f, dialect="pipe", fieldnames=field_names)
        else:
            writer = csv.DictWriter(f, fieldnames=field_names)

        writer.writeheader()
        writer.writerows([asdict(obj) for obj in dataclass_obj_list])
