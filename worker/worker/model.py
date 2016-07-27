from cassandra import InvalidRequest

from schema import Optional, Schema, Use

CREATE_KEYSPACE = ("""
    CREATE KEYSPACE IF NOT EXISTS prondata WITH replication = {
      'class': 'SimpleStrategy',
      'replication_factor': '1'
    };
""")

CREATE_TABLE = ("""
    CREATE TABLE IF NOT EXISTS links (
    id timeuuid,
    image_url varchar,
    source_url varchar,
    tags set<text>,
    phash varchar,
    nsfw boolean,
    PRIMARY KEY (phash, source_url)
    )
""")

INSERT_LINK_QUERY = ("""
    INSERT INTO links (id, image_url, source_url, phash, tags, nsfw)
    VALUES (:id, :image, :source, :phash, :tags, :nsfw)
""")

META_SCHEMA = Schema({
    "image": str,
    "source": str,
    Optional("nsfw", default=False): bool,
    Optional("tags", default=set()): set,
    Optional("karma"): Use(int),
    Optional("subreddit"): str
})

def ensure_init(cass_cluster):
    session = cass_cluster.connect()

    try:
        session.set_keyspace("prondata")
    except InvalidRequest:
        session.execute(CREATE_KEYSPACE)
        session.set_keyspace("prondata")

    session.execute(CREATE_TABLE)
