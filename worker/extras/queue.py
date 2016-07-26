import praw
import os
import json
from redis import StrictRedis
from urllib.parse import urlparse

reddit = praw.Reddit(user_agent="/u/nepeat image post hash scraper")

if "REDDIT_USERNAME" in os.environ and "REDDIT_PASSWORD" in os.environ:
    reddit.login(
        os.environ["REDDIT_USERNAME"],
        os.environ["REDDIT_PASSWORD"]
    )

redis = StrictRedis(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=os.environ.get("REDIS_PORT", 6379),
    decode_responses=True,
)


def fetch_reddit(subreddit):
    for post in reddit.get_subreddit(subreddit).get_hot():
        url = post.url.strip()
        if "imgur.com" in url.lower():
            if url.lower().endswith(".gifv"):
                url = url.replace(".gifv", ".gif")
        parsed = urlparse(url)
        if parsed.netloc == "imgur.com":
            if parsed.path.rindex("/") != 0:
                continue
            path_with_extension = parsed.path if "." in parsed.path else parsed.path + ".jpg"
            url = "https://i.imgur.com" + path_with_extension
        if (
            parsed.netloc.lower() not in ("imgur.com", "i.reddituploads.com", "imgur.com") and
            (
                not url.lower().endswith(".jpg") and
                not url.lower().endswith(".png") and
                not url.lower().endswith(".gif")
            )
        ):
            print("Ignoring URL %s." % (url))
            redis.sadd("worker:ignored", url)
            continue
        redis.lpush("crawl:imagequeue", json.dumps({
            "image": url,
            "source": post.permalink,
            "karma": post.score,
            "subreddit": subreddit
        }))
