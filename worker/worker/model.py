from cassandra import InvalidRequest

CREATE_KEYSPACE = ("""
    CREATE KEYSPACE IF NOT EXISTS worker WITH replication = {
      'class': 'SimpleStrategy',
      'replication_factor': '1'
    };
""")

CREATE_TABLE = ("""
    CREATE TABLE IF NOT EXISTS reddit (
    id timeuuid,
    subreddit varchar,
    image_url varchar,
    source_url varchar,
    karma int,
    phash varchar,
    PRIMARY KEY (phash, source_url)
    )
""")

INSERT_REDDIT_QUERY = ("""
    INSERT INTO reddit (id, image_url, source_url, phash, karma, subreddit)
    VALUES (:id, :image, :source, :phash, :karma, :subreddit)
""")

def ensure_init(cass_cluster):
    session = cass_cluster.connect()

    try:
        session.set_keyspace("worker")
    except InvalidRequest:
        session.execute(CREATE_KEYSPACE)
        session.set_keyspace("worker")

    session.execute(CREATE_TABLE)
