import logging
import csv


def uf_open_file(file_path: str, open_mode: str):
    """
    Open a file with exception handling.

    Args:
        file_path: File path
        open_mode: File open mode

    Returns:
        A file object which is an instance of TextIOWrapper.
    """

    try:
        f = open(file_path, open_mode, encoding="utf-8")
    except FileNotFoundError as error:
        logging.error(error)
        raise  # re-raise error with stack trace
        # raise FileNotFoundError   # Don't do this, you'll lose the stack trace!
    else:
        return f


def uf_open_file_list(files: list[str]):
    """
    Open a list of files with exception handling.

    Args:
        files: List of files.

    Returns:
        An instance of FileInput.
    """

    try:
        if files:
            fi = fileinput.input(files=files, encoding="utf-8")
        else:
            raise ValueError("File list is empty.")

    except ValueError as error:
        logging.error(error)
        raise

    else:
        return fi


def uf_read_file_to_str(file_path: str) -> list[dict]:
    """
    Open a file and outputs its contents as a text string.

    Args:
        file_path: File path

    Returns:
        The contents of the file as a text string.
    """

    with uf_open_file(file_path=file_path, open_mode="r") as f:
        file_data = f.read()

    try:
        if file_data:
            # print(file_records[:2])
            return file_data
        else:
            raise ValueError("Error in reading the file.")
    except ValueError as error:
        logging.error(error)
        raise
