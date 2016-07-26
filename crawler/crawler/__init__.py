import json
import os
import time
from io import BytesIO
import uuid
import imagehash
import praw
import requests
from cassandra import DriverException
from cassandra.cluster import Cluster
from classtools import reify
from PIL import Image
from redis import StrictRedis

from crawler.model import ensure_init, INSERT_REDDIT_QUERY


class Crawler(object):
    def __init__(self):
        self.cass_cluster = Cluster(
            os.environ.get("CASSANDRA_HOST", "localhost").split(","),
            port=os.environ.get("CASSANDRA_PORT", 9042)
        )

        ensure_init(self.cass_cluster)

    def run(self):
        print("Crawler main loop started!")

        while True:
            try:
                data = self.redis.blpop("crawl:imagequeue")[1]
                meta = json.loads(data)
                self.process(meta)
            except json.JSONDecodeError:
                print("Invalid JSON.")
                print(data)
                print("-" * 30)
            except DriverException as e:
                print("Cassandra error.")
                print(str(e))
                print("-" * 30)
            except Exception as e:
                print("Encountered error processing an image.")
                print(str(e))
                print(data)
                print("-" * 30)
                self.redis.rpush("crawl:imagequeue", data)

            time.sleep(0.2)

    def process(self, meta: dict):
        MAX_SIZE = 1024 * 1024 * 50  # 50 MB

        if (
            "image" not in meta or
            "source" not in meta or
            "karma" not in meta or
            "subreddit" not in meta
        ):
            print("invalid meta")
            print(meta)
            return

        if self.redis.sismember("crawl:done", meta["image"]):
            return

        print("Processing image %s" % (meta["image"]))

        r = requests.get(meta["image"], stream=True)
        size = 0
        content = BytesIO()

        for chunk in r.iter_content(2048):
            size += len(chunk)
            content.write(chunk)
            if size > MAX_SIZE:
                r.close()
                return

        content.seek(0)
        meta.update({
            "id": uuid.uuid1(),
            "phash": str(imagehash.phash(Image.open(content)))
        })
        self.commit(meta)

    def commit(self, meta):
        session = self.cass_cluster.connect("crawler")
        query = session.prepare(INSERT_REDDIT_QUERY)
        session.execute(query.bind(meta))
        self.redis.sadd("crawl:done", meta["image"])

    @reify
    def redis(self) -> StrictRedis:
        return StrictRedis(
            host=os.environ.get("REDIS_HOST", "localhost"),
            port=os.environ.get("REDIS_PORT", 6379),
            decode_responses=True,
        )
