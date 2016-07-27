import code
import os
from urllib.parse import urlparse

import praw
from imgurpython import ImgurClient

from worker.connections import create_redis
from worker.tasks import process

reddit = praw.Reddit(user_agent="/u/nepeat image post hash scraper")

if "REDDIT_USERNAME" in os.environ and "REDDIT_PASSWORD" in os.environ:
    reddit.login(
        os.environ["REDDIT_USERNAME"],
        os.environ["REDDIT_PASSWORD"]
    )

if "IMGUR_ID" not in os.environ or "IMGUR_SECRET" not in os.environ:
    raise Exception("Imgur API keys required.")

imgur = ImgurClient(
    os.environ["IMGUR_ID"],
    os.environ["IMGUR_SECRET"],
    mashape_key=os.environ.get("MASHAPE_KEY", None)
)

redis = create_redis()


def fetch_reddit(subreddit):
    sub = reddit.get_subreddit(subreddit)

    if sub.over18:
        redis.sadd("nsfw", subreddit)

    nsfw = redis.sismember("nsfw", subreddit) or sub.over18

    for post in sub.get_hot(limit=None):
        url = post.url.strip()
        parsed = urlparse(url)

        if "imgur.com" in url.lower():
            if url.lower().endswith(".gifv"):
                url = url.replace(".gifv", ".gif")

        if parsed.netloc == "imgur.com":
            if parsed.path.rindex("/") != 0:
                print("Ignoring album %s." % (url))
                redis.sadd("worker:ignored", url)
                continue
            path_with_extension = parsed.path if "." in parsed.path else parsed.path + ".jpg"
            url = "https://i.imgur.com" + path_with_extension

        if (
            parsed.netloc.lower() not in ("imgur.com", "i.reddituploads.com", "i.imgur.com") and
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
