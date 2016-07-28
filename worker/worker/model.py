import os

from cassandra.cqlengine import columns, connection
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from schema import Optional, Schema, Use


CREATE_KEYSPACE = ("""
    CREATE KEYSPACE IF NOT EXISTS prondata WITH replication = {
      'class': 'SimpleStrategy',
      'replication_factor': '1'
    };
""")

class Link(Model):
    id = columns.TimeUUID()
    phash = columns.Text(primary_key=True)
    image = columns.Text()
    source = columns.Text(primary_key=True)
    tags = columns.Set(columns.Text())
    nsfw = columns.Boolean()

class UnknownLink(Model):
    id = columns.TimeUUID(primary_key=True)
    image_url = columns.Text()
    source_url = columns.Text()

class Subreddit(Model):
    id = columns.TimeUUID()
    subreddit = columns.Text(primary_key=True)
    nsfw = columns.Boolean()

META_SCHEMA = Schema({
    "image": str,
    "source": str,
    Optional("nsfw", default=False): bool,
    Optional("tags", default=set()): set,
    Optional("karma"): Use(int),
    Optional("subreddit"): str
})

connection.setup(
    hosts=os.environ.get("CASSANDRA_HOST", "localhost").split(","),
    default_keyspace="prondata",
    protocol_version=4,
    retry_connect=True
)

sync_table(Link)
sync_table(UnknownLink)
