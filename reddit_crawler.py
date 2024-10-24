from reddit_client import RedditClient, RateLimitException
import logging
from pyfaktory import Client, Consumer, Job, Producer
import datetime
import psycopg2
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
register_adapter(dict, Json)
import random
from typing import Optional
from dotenv import load_dotenv
import os
import json

# Load environment variables
load_dotenv()

# Load configuration
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

CONFIG = load_config()

# Configuration
DATABASE_URL = os.getenv('DATABASE_URL')
FAKTORY_SERVER_URL = os.getenv('FAKTORY_SERVER_URL')

# Logger setup
logger = logging.getLogger("reddit crawler")
logger.setLevel(getattr(logging, CONFIG['logging']['level']))
sh = logging.StreamHandler()
formatter = logging.Formatter(CONFIG['logging']['format'])
sh.setFormatter(formatter)
logger.addHandler(sh)

def calculate_backoff(retry_count: int, base_delay: int = 60) -> int:
    """Calculate exponential backoff with jitter
    
    Args:
        retry_count: Number of retries so far
        base_delay: Base delay in seconds (default 60s = 1 min)
    
    Returns:
        Delay in seconds for next retry
    """
    # Exponential backoff: 1min, 2min, 4min, 8min, etc
    delay = base_delay * (2 ** retry_count)
    # Add jitter of ±25% to prevent thundering herd
    jitter = random.uniform(0.75, 1.25)
    return int(delay * jitter)

def reschedule_job(
    producer: Producer,
    jobtype: str,
    args: tuple,
    queue: str,
    retry_count: Optional[int] = None,
    max_retries: int = 5
) -> bool:
    """Reschedule a job with exponential backoff
    
    Args:
        producer: Faktory producer instance
        jobtype: Type of job to schedule
        args: Job arguments
        queue: Queue name
        retry_count: Current retry count (if None, assumed to be custom arg)
        max_retries: Maximum number of retries allowed
    
    Returns:
        bool: True if job was rescheduled, False if max retries exceeded
    """
    # If retry_count not provided, check if it's in the last argument
    if retry_count is None:
        retry_count = args[-1].get('retry_count', 0) if isinstance(args[-1], dict) else 0
    
    if retry_count >= max_retries:
        logger.error(f"[MAX_RETRIES] Job {jobtype} with args {args} exceeded maximum retries")
        return False
    
    # Calculate delay using exponential backoff
    delay = calculate_backoff(retry_count)
    next_try = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=delay)
    
    # Create metadata dict if it doesn't exist
    metadata = args[-1] if isinstance(args[-1], dict) else {}
    new_args = list(args[:-1]) if isinstance(args[-1], dict) else list(args)
    
    # Update metadata
    metadata.update({
        'retry_count': retry_count + 1,
        'previous_retries': metadata.get('previous_retries', []) + [datetime.datetime.now(datetime.UTC).isoformat()]
    })
    new_args.append(metadata)
    
    # Schedule new job
    job = Job(
        jobtype=jobtype,
        args=tuple(new_args),
        queue=queue,
        at=next_try.isoformat()
    )
    producer.push(job)
    
    logger.info(
        f"[RATE_LIMIT] Rescheduled {jobtype} for {next_try.isoformat()} "
        f"(retry {retry_count + 1}/{max_retries}, delay: {delay}s)"
    )
    return True

def get_subreddit_config(subreddit_name):
    """Get configuration for a specific subreddit"""
    for subreddit in CONFIG['subreddits']:
        if subreddit['name'] == subreddit_name:
            return subreddit
    return None

def post_ids_from_listing(listing):
    """Extract post IDs from a subreddit listing"""
    post_ids = []
    if listing and 'data' in listing and 'children' in listing['data']:
        for post in listing['data']['children']:
            post_ids.append(post['data']['id'])
    return post_ids

def find_new_posts(previous_post_ids, current_post_ids):
    """Find posts that weren't in the previous crawl"""
    return set(current_post_ids) - set(previous_post_ids)

def schedule_post_recrawls(subreddit, post_id):
    """Schedule fixed-time recrawls for a post"""
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        
        # Get recrawl delays from config
        recrawl_delays = CONFIG['crawl_settings']['recrawl_delays']
        
        for delay in recrawl_delays:
            next_crawl_time = datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=delay)
            run_at = next_crawl_time.isoformat()
            job = Job(jobtype="crawl-post",
                     args=(subreddit, post_id, True, {}),
                     queue="crawl-post",
                     at=run_at)
            producer.push(job)
            logger.info(f"[SCHEDULE] Post {post_id} from r/{subreddit} will be recrawled in {delay} days at {run_at}")

def crawl_post(subreddit: str, post_id: str, is_recrawl: bool = False, metadata: dict = None):
    """Crawl a single Reddit post and its comments"""
    try:
        if is_recrawl:
            logger.info(f"[START] Recrawling post {post_id} from r/{subreddit}")
        else:
            logger.info(f"[START] Crawling new post {post_id} from r/{subreddit}")

        reddit_client = RedditClient()
        try:
            post_data = reddit_client.get_post_comments(post_id)
        except RateLimitException:
            with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
                if not reschedule_job(
                    producer=Producer(client=client),
                    jobtype="crawl-post",
                    args=(subreddit, post_id, is_recrawl, metadata or {}),
                    queue="crawl-post"
                ):
                    logger.error(f"[FAILED] Post {post_id} from r/{subreddit} exceeded max retries")
            return
        
        if not post_data:
            logger.warning(f"[FAILED] Could not retrieve data for post {post_id} from r/{subreddit}")
            return

        with psycopg2.connect(dsn=DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Process main post
                main_post = post_data[0]['data']['children'][0]['data']
                
                q = """
                INSERT INTO reddit_posts (subreddit, post_id, title, author, created_utc, 
                                       score, data, last_updated, crawled_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (post_id) DO UPDATE
                SET title = EXCLUDED.title, score = EXCLUDED.score, 
                    data = EXCLUDED.data, last_updated = NOW(), crawled_at = NOW()
                """
                cur.execute(q, (subreddit, post_id, main_post['title'], 
                              main_post['author'],
                              datetime.datetime.fromtimestamp(main_post['created_utc'], 
                              tz=datetime.UTC),
                              main_post['score'], Json(main_post)))
                
                # Process comments
                comment_count = 0
                for comment in post_data[1]['data']['children']:
                    comment_data = comment['data']
                    q = """
                    INSERT INTO reddit_comments (post_id, comment_id, parent_id, author, 
                                              created_utc, score, body, data, last_updated, 
                                              crawled_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (comment_id) DO UPDATE
                    SET score = EXCLUDED.score, body = EXCLUDED.body, 
                        data = EXCLUDED.data, last_updated = NOW(), crawled_at = NOW()
                    """
                    cur.execute(q, (post_id, comment_data['id'], comment_data['parent_id'],
                                  comment_data['author'],
                                  datetime.datetime.fromtimestamp(comment_data['created_utc'], 
                                  tz=datetime.UTC),
                                  comment_data['score'], comment_data['body'], 
                                  Json(comment_data)))
                    comment_count += 1
                
                conn.commit()

        logger.info(f"[PROCESSED] Post {post_id} from r/{subreddit}: {comment_count} comments stored")

        # Schedule recrawls only for initial crawl of posts with positive score
        if not is_recrawl and main_post['score'] > 0:
            schedule_post_recrawls(subreddit, post_id)
        
        logger.info(f"[COMPLETE] {'Recrawl' if is_recrawl else 'Initial crawl'} of post {post_id} from r/{subreddit}")
                
    except psycopg2.Error as e:
        logger.error(f"[ERROR] Database error while crawling post {post_id}: {e}")
    except Exception as e:
        logger.error(f"[ERROR] Error while crawling post {post_id}: {e}")

def crawl_subreddit(subreddit_name: str, previous_post_ids: list = None, metadata: dict = None):
    """Crawl a subreddit and detect new posts"""
    try:
        logger.info(f"[START] Crawling subreddit r/{subreddit_name}")
        reddit_client = RedditClient()
        
        try:
            current_listing = reddit_client.get_subreddit_new(subreddit_name)
        except RateLimitException:
            with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
                if not reschedule_job(
                    producer=Producer(client=client),
                    jobtype="crawl-subreddit",
                    args=(subreddit_name, previous_post_ids or [], metadata or {}),
                    queue="crawl-subreddit"
                ):
                    logger.error(f"[FAILED] Subreddit r/{subreddit_name} exceeded max retries")
            return

        if not current_listing:
            logger.warning(f"[FAILED] Could not retrieve posts for r/{subreddit_name}")
            return

        current_post_ids = post_ids_from_listing(current_listing)
        new_posts = find_new_posts(previous_post_ids or [], current_post_ids)
        
        logger.info(f"[DETECTED] Found {len(new_posts)} new posts in r/{subreddit_name}")

        with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
            producer = Producer(client=client)
            
            # Queue jobs for new posts
            for post_id in new_posts:
                job = Job(jobtype="crawl-post",
                         args=(subreddit_name, post_id, False, {}),
                         queue="crawl-post")
                producer.push(job)
                logger.info(f"[SCHEDULE] Queued new post {post_id} from r/{subreddit_name} for crawling")

            # Schedule next subreddit crawl using UTC
            crawl_interval = CONFIG['crawl_settings']['crawl_interval']
            next_crawl_time = datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=crawl_interval)
            run_at = next_crawl_time.isoformat()
            
            next_job = Job(jobtype="crawl-subreddit",
                         args=(subreddit_name, current_post_ids, {}),
                         queue="crawl-subreddit",
                         at=run_at)
            producer.push(job)
            logger.info(f"[SCHEDULE] Next crawl for r/{subreddit_name} scheduled at {next_crawl_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        logger.info(f"[COMPLETE] Finished crawling r/{subreddit_name}")
            
    except Exception as e:
        logger.error(f"[ERROR] Error while crawling r/{subreddit_name}: {e}")

def schedule_initial_crawls():
    """Schedule initial crawls for all configured subreddits"""
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        for subreddit in CONFIG['subreddits']:
            job = Job(jobtype="crawl-subreddit",
                     args=(subreddit['name'], [], {}),
                     queue="crawl-subreddit")
            producer.push(job)
            logger.info(f"[SCHEDULE] Initial crawl scheduled for r/{subreddit['name']}")

if __name__ == "__main__":
    logger.info("[START] Starting Reddit crawler")
    schedule_initial_crawls()
    
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(client=client, 
                          queues=["crawl-subreddit", "crawl-post"],
                          concurrency=CONFIG['crawl_settings']['consumer_concurrency'])
        consumer.register("crawl-subreddit", crawl_subreddit)
        consumer.register("crawl-post", crawl_post)
        consumer.run()