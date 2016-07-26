from io import BytesIO
import requests

MAX_SIZE = 1024 * 1024 * 50  # 50 MB

def safe_download(url, size_limit=MAX_SIZE) -> BytesIO:
    r = requests.get(url, stream=True)
    size = 0
    content = BytesIO()

    for chunk in r.iter_content(2048):
        size += len(chunk)
        content.write(chunk)
        if size > size_limit:
            r.close()
            return None

    content.seek(0)

    return content
