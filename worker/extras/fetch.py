import code
import os
import re
import uuid
from urllib.parse import urlparse

import praw
from imgurpython import ImgurClient
from imgurpython.helpers.error import ImgurClientError

from worker.connections import create_redis
from worker.model import Subreddit, UnknownLink
from worker.tasks import process

# Connections

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

# Parsers

imgur_album_regex = re.compile(r"/(?:a|gallery)/(\w+)", re.IGNORECASE)

# CLI functions

def fetch_subreddit(subreddit, top=True, limit=None):
    sub = reddit.get_subreddit(subreddit)

    if sub.over18:
        Subreddit.create(
            id=uuid.uuid1(),
            subreddit=subreddit,
            nsfw=True
        )
    else:
        Subreddit.create(
            id=uuid.uuid1(),
            subreddit=subreddit,
            nsfw=False
        )

    for post in sub.get_hot(limit=limit):
        submit_reddit_post(post, subreddit, sub.over18)

    if top:
        topfuncs = [
            sub.get_top_from_all,
            sub.get_top_from_year,
            sub.get_top_from_month,
            sub.get_top_from_week,
            sub.get_top_from_day,
        ]
        for func in topfuncs:
            for post in func(limit=limit):
                submit_reddit_post(post, subreddit, sub.over18)

def fetch_imgur_album(album, source=None, nsfw=False, **kwargs):
    try:
        images = imgur.get_album_images(album)
    except ImgurClientError as e:
        print("ImgurClientError parsing album '%s'" % (album))
        return

    for image in images:
        if not source:
            source = "https://imgur.com/a/" + album

        process.delay({
            "image": image.link,
            "source": source,
            "nsfw": nsfw,
            **kwargs
        })

def submit_reddit_post(post, subreddit, nsfw=False):
    url = post.url.strip()
    parsed = urlparse(url)

    if "imgur.com" in url.lower():
        if url.lower().endswith(".gifv"):
            url = url.replace(".gifv", ".gif")

    if parsed.netloc in ("m.imgur.com", "imgur.com"):
        if parsed.path.rindex("/") != 0:
            albums = imgur_album_regex.findall(parsed.path)
            if albums:
                for album in albums:
                    fetch_imgur_album(
                        album,
                        source=post.permalink,
                        nsfw=nsfw,
                        subreddit=subreddit,
                        karma=post.score
                    )
            else:
                UnknownLink.create(
                    id=uuid.uuid1(),
                    image_url=url,
                    source_url=post.permalink
                )
                print("Ignoring unknown imgur URL %s." % (url))
            return

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
        UnknownLink.create(
            id=uuid.uuid1(),
            image_url=url,
            source_url=post.permalink
        )
        print("Ignoring URL %s." % (url))
        return

    process.delay({
        "image": url,
        "source": post.permalink,
        "karma": post.score,
        "subreddit": subreddit,
        "nsfw": nsfw
    })

if __name__ == "__main__":
    code.interact(local=locals())
