import asyncio
import json
import re
import emoji
from twscrape import API, gather
from twscrape.logger import set_log_level
from datetime import datetime, timezone, timedelta
import requests
from bs4 import BeautifulSoup
import boto3
import os

# ==============================
# CONFIGURATION
# ==============================

# S3 bucket name where results will be stored
S3_BUCKET = "reddit-crawler-bucket"

# max number of tweets to retrieve per hashtag
MAX_TWEETS_PER_HASHTAG = 125

# min likes for a tweet to have
MIN_LIKES = 5

# Boto3 S3 client
s3 = boto3.client("s3")

# ==============================
# HELPERS
# ==============================

def retrieve_top_hashtags():
    """
    Scrape the trending stock tickers from Finder.com
    """
    url = "https://www.finder.com/ca/stock-trading/top-trending-stocks-on-twitter"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/91.0.4472.124 Safari/537.36'
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    code_cells = soup.select('td[data-th="Code"], td[data-title="Code"]')

    # prepend "$" to make them usable as cashtags
    tickers = ['$' + cell.strong.text.strip() if cell.strong else cell.text.strip() 
               for cell in code_cells]
    return tickers[:30]


def process_tweet_content(tweet):
    """
    Clean up tweet display name and content (remove emojis, extra spaces).
    """
    tweet.user.displayname = emoji.replace_emoji(tweet.user.displayname, replace='')
    tweet.rawContent = emoji.replace_emoji(tweet.rawContent, replace='')
    tweet.rawContent = re.sub(r'[\r\n]+', ' ', tweet.rawContent)
    tweet.rawContent = re.sub(r'\s+', ' ', tweet.rawContent).strip()
    return tweet


def load_existing_results(file_name):
    """
    Try to load existing JSON data from S3, return [] if not found.
    """
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=f"tweets/{file_name}")
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return []

def load_accounts_from_s3():
    obj = s3.get_object(Bucket=S3_BUCKET, Key="config/accounts.json")
    return json.loads(obj["Body"].read().decode("utf-8"))

def save_results_to_s3(file_name, data):
    """
    Save JSON results to S3 in a daily partitioned folder.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"tweets/{file_name}/date={today}/data.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=4).encode("utf-8"),
        ContentType="application/json"
    )
    print(f"Saved {len(data)} tweets to s3://{S3_BUCKET}/{key}")



# ==============================
# MAIN SCRAPER
# ==============================

async def run_scraper():
    """
    Run the main scraping and tweet retrieval process.
    """
    # Ensure /tmp directory and accounts.db file exist
    db_path = "/tmp/accounts.db"
    if not os.path.exists("/tmp"):
        os.makedirs("/tmp")
    if not os.path.exists(db_path):
        open(db_path, "a").close()  # create empty file


    api = API("/tmp/accounts.db")

    # Try to load existing accounts from /tmp/accounts.db if it exists
    try:
        await api.pool.load("/tmp/accounts.db")
        print("Loaded accounts from /tmp/accounts.db")
    except Exception:
        print("No existing accounts.db found, creating new one")

    # Twitter accounts and cookies for authentication
    accounts = load_accounts_from_s3()

    # Add accounts with cookies
    for user, cookie in accounts.items():
        await api.pool.add_account(
            user,
            "dummy_pass",
            "dummy@mail.com",
            "dummy_pass",
            cookies=cookie
        )

    await api.pool.login_all()  # logs in with cookies
    set_log_level("INFO")

    # get tweets in the last 24 hours
    start_time = datetime.now(timezone.utc) - timedelta(hours=24)
    start_str = start_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")

    # scrape top hashtags
    hashtags = retrieve_top_hashtags()

    for tag in hashtags:
        file_name = f"{tag.strip('$').replace('#','').replace(' ','_')}.json"

        # Load existing tweets from S3
        results = load_existing_results(file_name)

        # Build query for tweets in English in the last 24h
        query = f"{tag} lang:en since:{start_str}"
        tweets = await gather(api.search(query, limit=MAX_TWEETS_PER_HASHTAG, kv={"product": "Top"}))

        # Filter by min likes
        top_tweets = sorted(
            [t for t in tweets if t.likeCount >= MIN_LIKES],
            key=lambda x: x.likeCount,
            reverse=True
        )

        # Append new tweets
        for t in top_tweets:
            cleanTweet = process_tweet_content(t)
            tweet_dict = {
                "id": cleanTweet.id,
                "ticker": tag,
                "username": cleanTweet.user.username,
                "display_name": cleanTweet.user.displayname,
                "content": cleanTweet.rawContent,
                "created_at": str(cleanTweet.date),
                "likes": cleanTweet.likeCount,
                "retweets": cleanTweet.retweetCount,
                "replies": cleanTweet.replyCount,
                "hashtags": cleanTweet.hashtags if cleanTweet.hashtags else [],
                "url": cleanTweet.url
            }
            if tweet_dict not in results:  # avoid duplicates
                results.append(tweet_dict)

        # Save updated results back to S3
        save_results_to_s3(file_name, results)
        print(f"Saved {len(top_tweets)} tweets for {tag} to S3: {file_name}")


# ==============================
# LAMBDA HANDLER
# ==============================

def lambda_handler(event, context):
    """
    AWS Lambda entry point.
    Triggered by EventBridge every 24 hours at 12 PM.
    """
    asyncio.run(run_scraper())
    return {"status": "success"}
