import os

from cassandra.cluster import Cluster
from redis import StrictRedis


def create_redis() -> StrictRedis:
    return StrictRedis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=os.environ.get("REDIS_PORT", 6379),
        decode_responses=True,
    )

def create_cassandra() -> Cluster:
    return Cluster(
        os.environ.get("CASSANDRA_HOST", "localhost").split(","),
        port=os.environ.get("CASSANDRA_PORT", 9042)
    )
