import os
import sys

from rq import Connection, Worker

from worker.connections import create_cassandra
from worker.model import ensure_init

if __name__ == "__main__":
    sentry_dsn = os.environ.get("SENTRY_DSN", None)

    cass_cluster = create_cassandra()
    ensure_init(cass_cluster)

    with Connection():
        queues = sys.argv[1:] or ["fetch", "low"]

        w = Worker(queues)

        if sentry_dsn:
            from raven import Client
            from rq.contrib.sentry import register_sentry
            client = Client(sentry_dsn)
            register_sentry(client, w)

        w.work()
