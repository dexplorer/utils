import io

from utils import file_io as uff


def uf_write_image_file(
    image_stream: io.BytesIO,
    file_path: str,
):
    with uff.uf_open_file(file_path=file_path, open_mode="wb") as f:
        f.write(image_stream.getvalue())
