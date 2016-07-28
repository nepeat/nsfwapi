import logging
import os
import sys
import time

from cassandra.cluster import NoHostAvailable
from redis.exceptions import ConnectionError
from rq import Worker

from worker.connections import create_cassandra, create_redis

if "DEBUG" in os.environ:
    logging.basicConfig(format=logging.DEBUG)

def wait_for_redis():
    redis = create_redis()
    while True:
        try:
            redis.info()
            break
        except ConnectionError:
            time.sleep(1)

def wait_for_cassandra():
    while True:
        cass_cluster = create_cassandra()
        try:
            cass_cluster.connect()
            break
        except NoHostAvailable as e:
            print("Failed to connect to Cassandra, attempting reconnection in 5 seconds.")
            print(str(e))
            time.sleep(5)

if __name__ == "__main__":
    sentry_dsn = os.environ.get("SENTRY_DSN", None)

    wait_for_redis()
    wait_for_cassandra()

    redis = create_redis()

    queues = sys.argv[1:] or ["fetch", "low"]

    w = Worker(
        queues=queues,
        connection=redis,
        default_result_ttl=60
    )

    if sentry_dsn:
        from raven import Client
        from raven.transport.requests import RequestsHTTPTransport
        from rq.contrib.sentry import register_sentry
        client = Client(sentry_dsn, transport=RequestsHTTPTransport)
        register_sentry(client, w)

    w.work()
