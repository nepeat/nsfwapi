import json
import os
import time
import uuid

import imagehash
from cassandra import DriverException
from cassandra.cluster import Cluster
from classtools import reify
from PIL import Image
from redis import StrictRedis

from worker.model import INSERT_LINK_QUERY, ensure_init
from worker.utils import safe_download


class Worker(object):
    def __init__(self):
        self.cass_cluster = Cluster(
            os.environ.get("CASSANDRA_HOST", "localhost").split(","),
            port=os.environ.get("CASSANDRA_PORT", 9042)
        )

        ensure_init(self.cass_cluster)

    def run(self):
        print("Worker main loop started!")

        while True:
            try:
                data = self.redis.blpop("worker:imagequeue")[1]
                meta = json.loads(data)
            except json.JSONDecodeError:
                print("Invalid JSON.")
                print(data)
                print("-" * 30)

            try:
                self.process(meta)
            except DriverException as e:
                print("Cassandra error.")
                print(str(e))
                print("-" * 30)
            except Exception as e:
                print("Encountered error processing an image.")
                print(str(e))
                print(data)
                print("-" * 30)
                self.redis.rpush("worker:imagequeue", data)

            time.sleep(0.2)

    def process(self, meta: dict):
        if (
            "image" not in meta or
            "source" not in meta
        ):
            print("invalid meta")
            print(meta)
            return

        if self.redis.sismember("worker:done", meta["image"]):
            return

        if "tags" not in meta:
            meta["tags"] = set()

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

        self.commit(meta)

    def commit(self, meta):
        session = self.cass_cluster.connect("prondata")
        query = session.prepare(INSERT_LINK_QUERY)
        session.execute(query.bind(meta))
        self.redis.sadd("worker:done", meta["image"])

    @reify
    def redis(self) -> StrictRedis:
        return StrictRedis(
            host=os.environ.get("REDIS_HOST", "localhost"),
            port=os.environ.get("REDIS_PORT", 6379),
            decode_responses=True,
        )
