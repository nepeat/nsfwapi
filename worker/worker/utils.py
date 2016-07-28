from io import BytesIO

import requests

MAX_SIZE = 1024 * 1024 * 50  # 50 MB

def safe_download(url, size_limit=MAX_SIZE) -> BytesIO:
    r = requests.get(url, stream=True, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.30 Safari/537.36"
    })
    size = 0
    content = BytesIO()

    if r.status_code not in (200, 302):
        return None

    for chunk in r.iter_content(2048):
        size += len(chunk)
        content.write(chunk)
        if size > size_limit:
            r.close()
            return None

    content.seek(0)

    return content
