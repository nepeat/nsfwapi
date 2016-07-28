import uuid

import imagehash
from PIL import Image
from rq.decorators import job

from worker.connections import create_redis
from worker.model import META_SCHEMA, Link
from worker.utils import safe_download

redis = create_redis()

@job("fetch", connection=redis, timeout=30, result_ttl=0)
def process(meta: dict):
    meta = META_SCHEMA.validate(meta)

    if redis.sismember("worker:done", meta["image"]):
        return

    print("Processing image %s" % (meta["image"]))

    content = safe_download(meta["image"])
    if not content:
        return

    meta.update({
        "id": uuid.uuid1(),
        "phash": str(imagehash.phash(Image.open(content)))
    })

    if "karma" in meta:
        meta["tags"].add("karma:%s" % (meta["karma"]))
        del meta["karma"]

    if "subreddit" in meta:
        meta["tags"].add("subreddit:%s" % (meta["subreddit"]))
        del meta["subreddit"]

    save.delay(meta)

@job("low", connection=redis, result_ttl=0)
def save(meta: dict):
    Link.create(**meta)
    redis.sadd("worker:done", meta["image"])
