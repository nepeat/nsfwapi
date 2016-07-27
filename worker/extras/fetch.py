import code
import os
from urllib.parse import urlparse

import praw

from worker.connections import create_redis
from worker.tasks import process

reddit = praw.Reddit(user_agent="/u/nepeat image post hash scraper")

if "REDDIT_USERNAME" in os.environ and "REDDIT_PASSWORD" in os.environ:
    reddit.login(
        os.environ["REDDIT_USERNAME"],
        os.environ["REDDIT_PASSWORD"]
    )

redis = create_redis()


def fetch_reddit(subreddit):
    nsfw = redis.sismember("nsfw", subreddit)

    for post in reddit.get_subreddit(subreddit).get_hot(limit=None):
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
        process.delay({
            "image": url,
            "source": post.permalink,
            "karma": post.score,
            "subreddit": subreddit,
            "nsfw": nsfw
        })

if __name__ == "__main__":
    code.interact(local=locals())
