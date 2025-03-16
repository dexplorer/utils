import json
import glob
import logging
from utils import file_io as uff


def uf_read_json_file_to_list_of_dict(file_path: str) -> list[dict]:
    with uff.uf_open_file(file_path=file_path, open_mode="r") as f:
        file_records: list[dict] = json.load(f)

    try:
        if file_records:
            # print(file_records[:2])
            return file_records
        else:
            raise ValueError("Error in reading the file.")
    except ValueError as error:
        logging.error(error)
        raise


def uf_write_list_of_data_cls_obj_to_json_file(obj_list: list, file_path: str):
    try:
        if obj_list:
            json_str = json.dumps(obj_list, default=lambda x: x.__dict__)
        else:
            raise RuntimeError("No data in the dataclass object list.")
    except RuntimeError as error:
        logging.error(error)

    with uff.uf_open_file(file_path=file_path, open_mode="w") as f:
        f.write(json_str)


def uf_write_data_cls_obj_to_json_file(obj, file_path: str):
    try:
        if obj:
            json_str = json.dumps(obj, default=lambda x: x.__dict__, indent=4)
        else:
            raise RuntimeError("No data in the dataclass object.")
    except RuntimeError as error:
        logging.error(error)

    with uff.uf_open_file(file_path=file_path, open_mode="w") as f:
        f.write(json_str)


def uf_merge_json_files(
    in_file_dir_path: str, out_file: str, in_file_pattern: str = "*"
) -> None:
    in_json_files = glob.glob(f"{in_file_dir_path}/{in_file_pattern}.json")
    merged_data = []
    for json_file in in_json_files:
        with uff.uf_open_file(file_path=json_file, open_mode="r") as fi:
            data = json.load(fi)
            merged_data.append(data)
    with uff.uf_open_file(file_path=out_file, open_mode="w") as f:
        json.dump(merged_data, f, indent=4)
