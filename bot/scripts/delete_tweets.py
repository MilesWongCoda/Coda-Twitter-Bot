#!/usr/bin/env python3
"""One-time script: delete specific low-quality trend alert tweets.

Usage:
    python -m bot.scripts.delete_tweets
"""
import os
import sys
import time
import tweepy
from dotenv import load_dotenv

load_dotenv()

# Tweets to delete — rank 200+ shitcoins that damage credibility
TWEET_IDS_TO_DELETE = [
    # $GIZA rank #967 — 8 views
    "2028622520379650382",
    # $FAI rank #686 — 44 views
    "2028622497197723898",
    # $FAI follow-up — 47 views
    "2028625730104389992",
]


def main():
    client = tweepy.Client(
        consumer_key=os.environ["TWITTER_API_KEY"],
        consumer_secret=os.environ["TWITTER_API_SECRET"],
        access_token=os.environ["TWITTER_ACCESS_TOKEN"],
        access_token_secret=os.environ["TWITTER_ACCESS_TOKEN_SECRET"],
    )

    for tweet_id in TWEET_IDS_TO_DELETE:
        try:
            resp = client.delete_tweet(tweet_id)
            print(f"Deleted {tweet_id}: {resp}")
        except Exception as exc:
            print(f"Failed to delete {tweet_id}: {exc}")
        time.sleep(2)  # Rate limit courtesy

    print(f"\nDone. Attempted to delete {len(TWEET_IDS_TO_DELETE)} tweets.")


if __name__ == "__main__":
    main()
