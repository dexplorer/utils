import json 
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
