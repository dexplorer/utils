def uf_write_list_of_data_cls_obj_to_json_file(obj_list: list, file_path: str):
    try:
        if dataclass_obj_list:
            json_str = json.dumps(obj_list, default=lambda x: x.__dict__)
        else:
            raise RuntimeError("No data in the dataclass object list.")
    except RuntimeError as error:
        logging.error(error)

    with uff.uf_open_file(file_path=file_path, open_mode="w") as f:
        f.write(json_str)
